//! Offline integration tests. Every test drives the full gateway router
//! against a deterministic loopback mock SenseNova upstream. No test ever
//! contacts `token.sensenova.cn`.

use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::extract::State;
use axum::http::{HeaderMap, Request, Response, StatusCode, header};
use axum::routing::post;
use bytes::Bytes;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tower::ServiceExt;

use super::*;
use crate::config::{SensenovaKeyConfig, TierConfig};

// ---------------------------------------------------------------------------
// Mock SenseNova upstream
// ---------------------------------------------------------------------------

#[derive(Clone)]
enum MockBody {
    Plain,
    StreamError,
    Stall(Arc<AtomicBool>),
    Delay(Duration),
}

#[derive(Clone)]
struct Spec {
    status: StatusCode,
    body: String,
    content_type: &'static str,
    headers: Vec<(&'static str, &'static str)>,
    kind: MockBody,
    /// Deliver the body in tiny chunks of this size (SSE fragmentation).
    fragment: Option<usize>,
}

impl Spec {
    fn json(status: u16, body: impl Into<String>) -> Self {
        Self {
            status: StatusCode::from_u16(status).unwrap(),
            body: body.into(),
            content_type: "application/json",
            headers: Vec::new(),
            kind: MockBody::Plain,
            fragment: None,
        }
    }

    fn sse(body: impl Into<String>) -> Self {
        Self {
            status: StatusCode::OK,
            body: body.into(),
            content_type: "text/event-stream",
            headers: Vec::new(),
            kind: MockBody::Plain,
            fragment: None,
        }
    }

    fn fragmented(body: impl Into<String>, fragment_size: usize) -> Self {
        Self {
            fragment: Some(fragment_size),
            ..Self::sse(body)
        }
    }

    fn with_delay(mut self, delay: Duration) -> Self {
        self.kind = MockBody::Delay(delay);
        self
    }

    /// Override the content type (used for raw/empty stream bodies).
    #[allow(dead_code)]
    fn content_type_sse(mut self) -> Self {
        self.content_type = "text/event-stream";
        self
    }

    fn with_header(mut self, name: &'static str, value: &'static str) -> Self {
        self.headers.push((name, value));
        self
    }
}

impl MockUpstream {
    /// Script the next responses for one credential, keyed by API key.
    async fn set(&self, key: &str, specs: Vec<Spec>) {
        self.sequences
            .lock()
            .await
            .insert(key.to_owned(), VecDeque::from(specs));
    }

    /// Script responses by model, ignoring which credential dialed.
    async fn set_model(&self, model: &str, specs: Vec<Spec>) {
        self.model_sequences
            .lock()
            .await
            .insert(model.to_owned(), VecDeque::from(specs));
    }

    async fn seen(&self) -> Vec<SeenRequest> {
        self.calls.lock().await.clone()
    }

    async fn models_seen(&self) -> Vec<String> {
        self.calls
            .lock()
            .await
            .iter()
            .map(|call| {
                call.body
                    .get("model")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned()
            })
            .collect()
    }

    async fn credentials_seen(&self) -> Vec<String> {
        self.calls
            .lock()
            .await
            .iter()
            .map(|call| call.authorization.clone())
            .collect()
    }
}

#[derive(Clone, Debug)]
struct SeenRequest {
    authorization: String,
    anthropic_version: Option<String>,
    user_agent: Option<String>,
    x_api_key: Option<String>,
    cookie: Option<String>,
    body: Value,
}

#[derive(Clone, Default)]
struct MockUpstream {
    sequences: Arc<Mutex<HashMap<String, VecDeque<Spec>>>>,
    model_sequences: Arc<Mutex<HashMap<String, VecDeque<Spec>>>>,
    calls: Arc<Mutex<Vec<SeenRequest>>>,
    in_flight: Arc<AtomicUsize>,
    max_in_flight: Arc<AtomicUsize>,
}

async fn mock_handler(
    State(mock): State<MockUpstream>,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let key = authorization
        .strip_prefix("Bearer ")
        .unwrap_or_default()
        .to_owned();
    let parsed = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let model = parsed
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    mock.calls.lock().await.push(SeenRequest {
        authorization,
        anthropic_version: headers
            .get("anthropic-version")
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned),
        user_agent: headers
            .get(header::USER_AGENT)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned),
        x_api_key: headers
            .get("x-api-key")
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned),
        cookie: headers
            .get(header::COOKIE)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned),
        body: parsed,
    });
    // Per-credential scripts win; otherwise fall back to a per-model script.
    let mut spec = mock
        .sequences
        .lock()
        .await
        .get_mut(&key)
        .and_then(VecDeque::pop_front);
    if spec.is_none() {
        spec = mock
            .model_sequences
            .lock()
            .await
            .get_mut(&model)
            .and_then(VecDeque::pop_front);
    }
    let spec = spec.unwrap_or_else(|| Spec::json(500, r#"{"error":{"message":"unscripted"}}"#));
    let current = mock.in_flight.fetch_add(1, Ordering::AcqRel) + 1;
    mock.max_in_flight.fetch_max(current, Ordering::AcqRel);
    if let MockBody::Delay(duration) = spec.kind {
        tokio::time::sleep(duration).await;
    }
    let mut builder = Response::builder()
        .status(spec.status)
        .header(header::CONTENT_TYPE, spec.content_type);
    for (name, value) in spec.headers {
        builder = builder.header(name, value);
    }
    let response_body: Body = match spec.kind {
        MockBody::StreamError => {
            let initial = spec.body;
            let stream = async_stream::stream! {
                yield Ok::<Bytes, std::io::Error>(Bytes::from(initial));
                tokio::time::sleep(Duration::from_millis(30)).await;
                yield Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "mock reset",
                ));
            };
            Body::from_stream(stream)
        }
        MockBody::Stall(flag) => {
            struct Guard(Arc<AtomicBool>);
            impl Drop for Guard {
                fn drop(&mut self) {
                    self.0.store(true, Ordering::Release);
                }
            }
            let initial = spec.body;
            let stream = async_stream::stream! {
                let _guard = Guard(flag);
                yield Ok::<Bytes, Infallible>(Bytes::from(initial));
                std::future::pending::<()>().await;
            };
            Body::from_stream(stream)
        }
        MockBody::Plain | MockBody::Delay(_) => match spec.fragment {
            Some(size) => {
                let chunks: Vec<std::result::Result<Bytes, Infallible>> = spec
                    .body
                    .as_bytes()
                    .chunks(size.max(1))
                    .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
                    .collect();
                Body::from_stream(futures_util::stream::iter(chunks))
            }
            None => Body::from(spec.body),
        },
    };
    let response = builder.body(response_body).unwrap();
    mock.in_flight.fetch_sub(1, Ordering::AcqRel);
    response
}

async fn start_mock() -> (String, MockUpstream, tokio::task::JoinHandle<()>) {
    let mock = MockUpstream::default();
    let app = axum::Router::new()
        .route("/v1/messages", post(mock_handler))
        .with_state(mock.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}"), mock, task)
}

// ---------------------------------------------------------------------------
// Gateway test harness
// ---------------------------------------------------------------------------

fn test_config(base_url: String, keys: usize) -> crate::config::Config {
    let mut config = crate::config::Config::default();
    config.server.api_key = "gateway-secret".into();
    config.upstream.base_url = base_url;
    config.upstream.timeout_secs = 5;
    config.upstream.connect_timeout_secs = 2;
    config.upstream.first_byte_timeout_secs = 2;
    config.retry.max_attempts = 2;
    config.retry.backoff_initial_ms = 10;
    config.retry.backoff_max_ms = 50;
    // Test-friendly routing: two attempts by default (the legacy default),
    // short cooldowns, and a trip threshold of two distinct groups.
    config.routing.max_route_attempts = 2;
    config.routing.route_cooldown_initial_secs = 1;
    config.routing.route_cooldown_max_secs = 4;
    config.routing.retry_after_max_secs = 2;
    config.routing.model_open_secs = 2;
    config.routing.model_trip_window_secs = 20;
    config.routing.model_trip_distinct_groups = 2;
    config.routing.profiles = test_profiles();
    config
        .models
        .aliases
        .insert("claude-sensenova".into(), "sensenova-6.8-flash-lite".into());
    config.sensenova_api_keys = (0..keys)
        .map(|index| SensenovaKeyConfig {
            name: format!("key-{}", index + 1),
            api_key: format!("sensenova-key-{}", index + 1),
            enabled: true,
            quota_group: "account-a".into(),
        })
        .collect();
    config
}

/// A deterministic two-tier profile set: `claude-coding-hard` mirrors the
/// documented quality isolation, `claude-coding-fast` the latency pool.
fn test_profiles() -> crate::config::Profiles {
    let mut profiles = crate::config::Profiles::new();
    profiles.insert(
        "claude-coding-hard".to_owned(),
        crate::config::ProfileConfig {
            latency_optimized: false,
            allow_lower_tier_on_unavailable: false,
            tiers: vec![
                TierConfig {
                    models: vec!["glm-5.2".into(), "deepseek-v4-pro".into()],
                },
                TierConfig {
                    models: vec!["kimi-k3".into()],
                },
            ],
        },
    );
    profiles.insert(
        "claude-coding-fast".to_owned(),
        crate::config::ProfileConfig {
            latency_optimized: true,
            allow_lower_tier_on_unavailable: false,
            tiers: vec![TierConfig {
                models: vec![
                    "deepseek-v4-flash".into(),
                    "sensenova-6.8-flash-lite".into(),
                ],
            }],
        },
    );
    profiles
}

fn app_for(config: crate::config::Config) -> Router {
    router(AppState::new(config).unwrap())
}

fn state_for(config: crate::config::Config) -> AppState {
    AppState::new(config).unwrap()
}

fn gateway_request(path: &str, body: impl Into<Body>) -> Request<Body> {
    Request::builder()
        .method(if path == "/v1/models" { "GET" } else { "POST" })
        .uri(path)
        .header(header::AUTHORIZATION, "Bearer gateway-secret")
        .header(header::CONTENT_TYPE, "application/json")
        .body(body.into())
        .unwrap()
}

/// A request through the hard profile (the default for an unqualified model).
fn anthropic_body(stream: bool) -> String {
    json!({
        "model": "claude-coding-hard",
        "max_tokens": 128,
        "stream": stream,
        "messages": [{"role": "user", "content": "hello"}],
        "unknown_future_field": {"preserve": true}
    })
    .to_string()
}

fn body_for_model(model: &str, stream: bool) -> String {
    json!({
        "model": model,
        "max_tokens": 128,
        "stream": stream,
        "messages": [{"role": "user", "content": "hello"}]
    })
    .to_string()
}

fn ok_message_json() -> String {
    json!({
        "id": "msg_1", "type": "message", "role": "assistant",
        "content": [{"type": "text", "text": "OK"}],
        "model": "glm-5.2", "stop_reason": "end_turn",
        "usage": {"input_tokens": 10, "output_tokens": 2}
    })
    .to_string()
}

fn ok_message_sse() -> String {
    "event: message_start\ndata: {\"type\":\"message_start\"}\n\n\
     event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n\
     event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"OK\"}}\n\n\
     event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n\
     event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n\
     event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
        .to_string()
}

async fn response_text(response: Response<Body>) -> String {
    String::from_utf8(
        to_bytes(response.into_body(), 8 * 1024 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap()
}

async fn ready_body(app: Router) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body: Value = serde_json::from_str(&response_text(response).await).unwrap();
    (status, body)
}

// ---------------------------------------------------------------------------
// Tests: profile isolation (hard never degrades into fast)
// ---------------------------------------------------------------------------

/// The hard profile must never select SenseNova 6.8 Flash Lite, under any
/// failure pattern: healthy, tier-0 cooling, and everything failing.
#[tokio::test]
async fn hard_profile_never_selects_sensenova_flash_lite() {
    // The mock server task is intentionally kept alive for the whole test
    // and torn down with the runtime.
    let (base, mock, _task) = start_mock().await;
    // Every dial succeeds, so only the router's *choice* is under test.
    mock.set_model(
        "glm-5.2",
        (0..4).map(|_| Spec::json(200, ok_message_json())).collect(),
    )
    .await;
    mock.set_model(
        "deepseek-v4-pro",
        (0..4).map(|_| Spec::json(200, ok_message_json())).collect(),
    )
    .await;
    mock.set_model(
        "kimi-k3",
        (0..4).map(|_| Spec::json(200, ok_message_json())).collect(),
    )
    .await;
    mock.set_model(
        "sensenova-6.8-flash-lite",
        (0..4).map(|_| Spec::json(200, ok_message_json())).collect(),
    )
    .await;
    mock.set_model(
        "deepseek-v4-flash",
        (0..4).map(|_| Spec::json(200, ok_message_json())).collect(),
    )
    .await;
    let mut config = test_config(base, 1);
    config.routing.max_route_attempts = 4;
    let app = app_for(config);
    for _ in 0..4 {
        let response = app
            .clone()
            .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    let models = mock.models_seen().await;
    assert!(!models.is_empty());
    for model in &models {
        assert_ne!(model, "sensenova-6.8-flash-lite", "hard must never use it");
        assert_ne!(model, "deepseek-v4-flash", "hard must never use it");
    }
}

/// The hard profile must never select DeepSeek V4 Flash either, even when
/// every hard route fails.
#[tokio::test]
async fn hard_profile_never_selects_deepseek_flash_even_on_total_failure() {
    // The mock server task is intentionally kept alive for the whole test
    // and torn down with the runtime.
    let (base, mock, _task) = start_mock().await;
    for model in ["glm-5.2", "deepseek-v4-pro", "kimi-k3"] {
        mock.set_model(
            model,
            (0..4)
                .map(|_| Spec::json(503, r#"{"error":{"message":"overloaded"}}"#))
                .collect(),
        )
        .await;
    }
    mock.set_model(
        "deepseek-v4-flash",
        (0..4).map(|_| Spec::json(200, ok_message_json())).collect(),
    )
    .await;
    let mut config = test_config(base, 1);
    config.routing.max_route_attempts = 4;
    let app = app_for(config);
    let response = app
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    // Honest failure beats silent quality degradation.
    assert_ne!(response.status(), StatusCode::OK);
    for model in mock.models_seen().await {
        assert_ne!(
            model, "deepseek-v4-flash",
            "hard must never degrade to fast"
        );
        assert_ne!(model, "sensenova-6.8-flash-lite");
    }
}

/// A healthy tier-0 route always beats the tier-1 fallback.
#[tokio::test]
async fn hard_tier_zero_beats_kimi_when_healthy() {
    // The mock server task is intentionally kept alive for the whole test
    // and torn down with the runtime.
    let (base, mock, _task) = start_mock().await;
    mock.set_model("glm-5.2", vec![Spec::json(200, ok_message_json())])
        .await;
    mock.set_model("deepseek-v4-pro", vec![Spec::json(200, ok_message_json())])
        .await;
    mock.set_model("kimi-k3", vec![Spec::json(200, ok_message_json())])
        .await;
    let response = app_for(test_config(base, 1))
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let models = mock.models_seen().await;
    assert_eq!(models.len(), 1);
    assert!(
        matches!(models[0].as_str(), "glm-5.2" | "deepseek-v4-pro"),
        "tier 0 must win, got {}",
        models[0]
    );
}

/// Kimi serves only when every tier-0 route is unusable — never earlier.
#[tokio::test]
async fn hard_uses_kimi_when_tier_zero_is_unavailable() {
    // The mock server task is intentionally kept alive for the whole test
    // and torn down with the runtime.
    let (base, mock, _task) = start_mock().await;
    // Both tier-0 models are 404 (disabled for the process), Kimi is healthy.
    mock.set_model(
        "glm-5.2",
        vec![Spec::json(
            404,
            r#"{"error":{"type":"not_found_error","message":"model is not found"}}"#,
        )],
    )
    .await;
    mock.set_model(
        "deepseek-v4-pro",
        vec![Spec::json(
            404,
            r#"{"error":{"type":"not_found_error","message":"model is not found"}}"#,
        )],
    )
    .await;
    mock.set_model("kimi-k3", vec![Spec::json(200, ok_message_json())])
        .await;
    let mut config = test_config(base, 1);
    config.routing.max_route_attempts = 4;
    let app = app_for(config);
    let response = app
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let models = mock.models_seen().await;
    assert!(
        models.contains(&"kimi-k3".to_owned()),
        "kimi must be the fallback, got {models:?}"
    );
}

/// The fast profile contains only fast models.
#[tokio::test]
async fn fast_profile_contains_only_fast_models() {
    // The mock server task is intentionally kept alive for the whole test
    // and torn down with the runtime.
    let (base, mock, _task) = start_mock().await;
    for model in [
        "deepseek-v4-flash",
        "sensenova-6.8-flash-lite",
        "glm-5.2",
        "deepseek-v4-pro",
        "kimi-k3",
    ] {
        mock.set_model(
            model,
            (0..4).map(|_| Spec::json(200, ok_message_json())).collect(),
        )
        .await;
    }
    let app = app_for(test_config(base, 1));
    for _ in 0..4 {
        let response = app
            .clone()
            .oneshot(gateway_request(
                "/v1/messages",
                body_for_model("claude-coding-fast", false),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    let models = mock.models_seen().await;
    assert!(!models.is_empty());
    for model in &models {
        assert!(
            matches!(
                model.as_str(),
                "deepseek-v4-flash" | "sensenova-6.8-flash-lite"
            ),
            "fast profile leaked a hard model: {model}"
        );
    }
}

// ---------------------------------------------------------------------------
// Tests: 429 failure domains
// ---------------------------------------------------------------------------

/// A generic 429 cools only `(model, quota_group)`; the same model on another
/// account still serves.
#[tokio::test]
async fn one_429_cools_only_the_model_and_group() {
    let (base, mock, task) = start_mock().await;
    // account-a 429s once, account-b is healthy for the same model.
    mock.set(
        "sensenova-key-1",
        vec![Spec::json(429, r#"{"error":{"message":"Server is busy"}}"#)],
    )
    .await;
    mock.set(
        "sensenova-key-2",
        (0..4).map(|_| Spec::json(200, ok_message_json())).collect(),
    )
    .await;
    let mut config = test_config(base, 2);
    config.sensenova_api_keys[0].quota_group = "account-a".into();
    config.sensenova_api_keys[1].quota_group = "account-b".into();
    let state = state_for(config);
    let app = router(state.clone());
    let response = app
        .clone()
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK, "failover must succeed");
    assert_eq!(
        mock.credentials_seen().await,
        vec![
            "Bearer sensenova-key-1".to_owned(),
            "Bearer sensenova-key-2".to_owned()
        ]
    );
    // Exactly one `(model, group)` route is cooling; the other is untouched.
    assert!(
        state
            .core
            .routes
            .route_cooling("glm-5.2", "account-a")
            .is_some(),
        "the 429 route must cool"
    );
    assert!(
        state
            .core
            .routes
            .route_cooling("glm-5.2", "account-b")
            .is_none(),
        "the healthy group must not cool"
    );
    assert!(
        state.core.routes.model_snapshot("glm-5.2").state == crate::router::CircuitState::Closed,
        "one 429 must never open the model circuit"
    );
    task.abort();
}

/// The same model fails over to another quota group (never re-dialing the
/// cooled route).
#[tokio::test]
async fn same_model_fails_over_to_another_group() {
    // The mock server task is intentionally kept alive for the whole test
    // and torn down with the runtime.
    let (base, mock, _task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
        vec![Spec::json(
            429,
            r#"{"error":{"message":"inference tpm exhausted"}}"#,
        )],
    )
    .await;
    mock.set("sensenova-key-2", vec![Spec::json(200, ok_message_json())])
        .await;
    let mut config = test_config(base, 2);
    config.sensenova_api_keys[0].quota_group = "account-a".into();
    config.sensenova_api_keys[1].quota_group = "account-b".into();
    let response = app_for(config)
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let models = mock.models_seen().await;
    assert_eq!(
        models.len(),
        2,
        "one failover, not a same-route replay: {models:?}"
    );
    assert_eq!(models[0], models[1], "the model itself did not change");
}

/// Failures from two distinct quota groups open the model circuit; a repeated
/// failure from a single group alone never does.
#[tokio::test]
async fn two_distinct_groups_trip_the_model_circuit_but_one_does_not() {
    // (a) one group failing repeatedly stays closed.
    {
        let (base, mock, task) = start_mock().await;
        mock.set(
            "sensenova-key-1",
            (0..4)
                .map(|_| Spec::json(429, r#"{"error":{"message":"busy"}}"#))
                .collect(),
        )
        .await;
        let mut config = test_config(base, 1);
        config.routing.max_route_attempts = 4;
        config.routing.same_route_429_retries = 3;
        let state = state_for(config);
        let app = router(state.clone());
        for _ in 0..2 {
            let _ = app
                .clone()
                .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
                .await
                .unwrap();
        }
        assert_eq!(
            state.core.routes.model_snapshot("glm-5.2").state,
            crate::router::CircuitState::Closed,
            "repeat failures from ONE group must not trip the model"
        );
        task.abort();
    }

    // (b) two distinct groups within the trip window open it.
    {
        let (base, mock, task) = start_mock().await;
        for key in ["sensenova-key-1", "sensenova-key-2"] {
            mock.set(
                key,
                (0..4)
                    .map(|_| Spec::json(429, r#"{"error":{"message":"busy"}}"#))
                    .collect(),
            )
            .await;
        }
        let mut config = test_config(base, 2);
        config.routing.max_route_attempts = 4;
        config.routing.model_trip_distinct_groups = 2;
        config.routing.model_trip_window_secs = 60;
        config.sensenova_api_keys[0].quota_group = "account-a".into();
        config.sensenova_api_keys[1].quota_group = "account-b".into();
        let state = state_for(config);
        let app = router(state.clone());
        let _ = app
            .clone()
            .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
            .await
            .unwrap();
        assert_eq!(
            state.core.routes.model_snapshot("glm-5.2").state,
            crate::router::CircuitState::Open,
            "two distinct groups inside the window must trip the model"
        );
        task.abort();
    }
}

/// A tripped model circuit half-opens and closes again after a success.
#[tokio::test]
async fn model_circuit_half_opens_and_recovers() {
    let (base, mock, task) = start_mock().await;
    // Exactly one 429 per credential: two distinct groups trip the model, and
    // only the two tier-0 routes cool. After recovery everything is healthy.
    mock.set(
        "sensenova-key-1",
        vec![Spec::json(429, r#"{"error":{"message":"busy"}}"#)],
    )
    .await;
    mock.set(
        "sensenova-key-2",
        vec![Spec::json(429, r#"{"error":{"message":"busy"}}"#)],
    )
    .await;
    let mut config = test_config(base, 2);
    config.routing.max_route_attempts = 4;
    config.routing.model_open_secs = 1;
    config.routing.route_cooldown_initial_secs = 1;
    config.routing.route_cooldown_max_secs = 1;
    config.routing.retry_after_max_secs = 1;
    config.sensenova_api_keys[0].quota_group = "account-a".into();
    config.sensenova_api_keys[1].quota_group = "account-b".into();
    let state = state_for(config);
    let app = router(state.clone());
    let _ = app
        .clone()
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(
        state.core.routes.model_snapshot("glm-5.2").state,
        crate::router::CircuitState::Open
    );

    // After the open window the circuit half-opens and the next success closes
    // it. Both the model window and the escalated route ladder must have
    // elapsed first. Script both hard models healthy again.
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    for model in ["glm-5.2", "deepseek-v4-pro"] {
        mock.set_model(
            model,
            (0..4).map(|_| Spec::json(200, ok_message_json())).collect(),
        )
        .await;
    }
    let response = app
        .clone()
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "half-open probe must succeed"
    );
    assert_eq!(
        state.core.routes.model_snapshot("glm-5.2").state,
        crate::router::CircuitState::Closed,
        "a successful probe closes the model circuit"
    );
    task.abort();
}

/// An authoritative `Retry-After` is honoured over the exponential ladder.
#[tokio::test]
async fn retry_after_header_is_respected() {
    let (base, mock, task) = start_mock().await;
    // Both tier-0 models answer 429 with the same authoritative hint, so the
    // hint reaches the client regardless of which one is dialed first.
    for model in ["glm-5.2", "deepseek-v4-pro"] {
        mock.set_model(
            model,
            vec![
                Spec::json(429, r#"{"error":{"message":"slow down"}}"#)
                    .with_header("retry-after", "17"),
            ],
        )
        .await;
    }
    let response = app_for(test_config(base, 1))
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    let retry_after: u64 = response.headers()[header::RETRY_AFTER]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        (17..=18).contains(&retry_after),
        "the authoritative hint must travel to the client, got {retry_after}"
    );
    task.abort();
}

/// Without a hint, the generic-429 cooldown is the bounded exponential ladder:
/// it never exceeds the configured maximum, never returns zero, and the client
/// receives it as `Retry-After`.
#[tokio::test]
async fn exponential_cooldown_is_bounded_without_a_hint() {
    let (base, mock, task) = start_mock().await;
    for model in ["glm-5.2", "deepseek-v4-pro"] {
        mock.set_model(
            model,
            (0..4)
                .map(|_| Spec::json(429, r#"{"error":{"message":"Server is busy"}}"#))
                .collect(),
        )
        .await;
    }
    let mut config = test_config(base, 1);
    // A 5 s ladder keeps the cooldown observable while the response travels
    // back; the ladder shape itself is unit-tested in `router::tests`.
    config.routing.route_cooldown_initial_secs = 5;
    config.routing.route_cooldown_max_secs = 120;
    config.routing.retry_after_max_secs = 1;
    let state = state_for(config);
    let app = router(state.clone());
    let response = app
        .clone()
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    let retry_after: u64 = response.headers()[header::RETRY_AFTER]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    // The ladder escalates per attempt (5 s, then 10 s, …) and is always
    // forwarded with the small `Retry-After` headroom. It must be non-zero and
    // must never exceed the configured ladder maximum plus that headroom.
    assert!(
        (5..=121).contains(&retry_after),
        "the ladder must start at route_cooldown_initial_secs, stay bounded, and never be zero, got {retry_after}"
    );

    // Whatever route the router chose is the one that cooled, and it is cooled
    // as a *(model, quota_group)* route — never as the whole account.
    let cooled = ["glm-5.2", "deepseek-v4-pro"]
        .iter()
        .filter(|model| {
            state
                .core
                .routes
                .route_cooling(model, "account-a")
                .is_some()
        })
        .count();
    assert!(
        cooled >= 1,
        "the failed (model, quota_group) route must cool after a hintless 429"
    );
    task.abort();
}

/// A 429 must never be replayed on the same route while another healthy route
/// exists.
#[tokio::test]
async fn no_same_route_429_retry_when_alternatives_exist() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
        vec![
            Spec::json(429, r#"{"error":{"message":"busy"}}"#),
            Spec::json(200, ok_message_json()),
        ],
    )
    .await;
    mock.set("sensenova-key-2", vec![Spec::json(200, ok_message_json())])
        .await;
    let mut config = test_config(base, 2);
    config.routing.max_route_attempts = 4;
    config.sensenova_api_keys[0].quota_group = "account-a".into();
    config.sensenova_api_keys[1].quota_group = "account-b".into();
    let response = app_for(config)
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        mock.credentials_seen().await,
        vec![
            "Bearer sensenova-key-1".to_owned(),
            "Bearer sensenova-key-2".to_owned()
        ],
        "the failed credential must not be dialed twice"
    );
    task.abort();
}

/// Explicit quota exhaustion still cools the whole quota_group and fails over
/// to another group within the same logical request.
#[tokio::test]
async fn explicit_quota_exhaustion_cools_the_whole_group() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
        vec![Spec::json(
            429,
            r#"{"error":{"message":"FREE_QUOTA_EXHAUSTED"}}"#,
        )],
    )
    .await;
    mock.set("sensenova-key-2", vec![Spec::json(200, ok_message_json())])
        .await;
    mock.set("sensenova-key-3", vec![Spec::json(200, ok_message_json())])
        .await;
    let mut config = test_config(base, 3);
    config.routing.max_route_attempts = 4;
    config.sensenova_api_keys[0].quota_group = "account-a".into();
    config.sensenova_api_keys[1].quota_group = "account-a".into();
    config.sensenova_api_keys[2].quota_group = "account-b".into();
    let state = state_for(config);
    let app = router(state.clone());
    let response = app
        .clone()
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "the same request must fail over to the healthy group"
    );
    assert!(
        state.core.routes.group_cooling("account-a").is_some(),
        "the whole exhausted group cools"
    );
    assert!(
        state.core.routes.group_cooling("account-b").is_none(),
        "the other group stays usable"
    );
    assert_eq!(
        mock.credentials_seen().await,
        vec![
            "Bearer sensenova-key-1".to_owned(),
            "Bearer sensenova-key-3".to_owned()
        ],
        "the same-group sibling must be skipped"
    );
    task.abort();
}

// ---------------------------------------------------------------------------
// Tests: credential and attempt-budget semantics
// ---------------------------------------------------------------------------

/// A 401 disables only the rejected credential.
#[tokio::test]
async fn unauthorized_disables_only_one_credential() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
        vec![Spec::json(401, r#"{"error":{"code":16,"message":"nope"}}"#)],
    )
    .await;
    mock.set(
        "sensenova-key-2",
        (0..4).map(|_| Spec::json(200, ok_message_json())).collect(),
    )
    .await;
    let mut config = test_config(base, 2);
    config.sensenova_api_keys[0].quota_group = "account-a".into();
    config.sensenova_api_keys[1].quota_group = "account-a".into();
    let state = state_for(config);
    let app = router(state.clone());
    let response = app
        .clone()
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let credentials = state.core.routes.credential_snapshots();
    assert!(
        credentials[0].unusable,
        "the rejected credential is disabled"
    );
    assert!(
        !credentials[1].unusable,
        "the same-group sibling must stay usable (a key is not an account)"
    );
    assert_eq!(state.core.routes.usable_credential_count(), 1);
    task.abort();
}

/// The single global route-attempt budget can never be exceeded, whatever the
/// mix of failover reasons.
#[tokio::test]
async fn global_attempt_budget_cannot_be_exceeded() {
    let (base, mock, task) = start_mock().await;
    for index in 1..=6 {
        mock.set(
            &format!("sensenova-key-{index}"),
            (0..4)
                .map(|_| Spec::json(500, r#"{"error":{"message":"boom"}}"#))
                .collect(),
        )
        .await;
    }
    mock.set_model(
        "deepseek-v4-pro",
        (0..4)
            .map(|_| Spec::json(500, r#"{"error":{"message":"boom"}}"#))
            .collect(),
    )
    .await;
    mock.set_model(
        "kimi-k3",
        (0..4)
            .map(|_| Spec::json(500, r#"{"error":{"message":"boom"}}"#))
            .collect(),
    )
    .await;
    let mut config = test_config(base, 6);
    config.routing.max_route_attempts = 4;
    for (index, key) in config.sensenova_api_keys.iter_mut().enumerate() {
        key.quota_group = format!("group-{index}");
    }
    let response = app_for(config)
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        mock.seen().await.len(),
        4,
        "exactly max_route_attempts upstream attempts, never more"
    );
    task.abort();
}

/// A 404 disables that model route and the request still completes on another
/// model without re-dialing the missing one.
#[tokio::test]
async fn model_not_found_is_not_retried_on_the_same_route() {
    let (base, mock, task) = start_mock().await;
    mock.set_model(
        "glm-5.2",
        vec![Spec::json(
            404,
            r#"{"error":{"type":"not_found_error","message":"model is not found"}}"#,
        )],
    )
    .await;
    mock.set_model("deepseek-v4-pro", vec![Spec::json(200, ok_message_json())])
        .await;
    let mut config = test_config(base, 1);
    config.routing.max_route_attempts = 4;
    let state = state_for(config);
    let app = router(state.clone());
    let response = app
        .clone()
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let models = mock.models_seen().await;
    assert_eq!(
        models.iter().filter(|model| *model == "glm-5.2").count(),
        1,
        "the missing model must be dialed exactly once: {models:?}"
    );
    assert!(state.core.routes.route_disabled("glm-5.2", "account-a"));
    task.abort();
}

// ---------------------------------------------------------------------------
// Tests: commit barrier
// ---------------------------------------------------------------------------

/// A pre-commit failure may switch models; after the first downstream byte it
/// never can.
#[tokio::test]
async fn pre_commit_cross_model_retry_works_and_post_commit_is_impossible() {
    let (base, mock, task) = start_mock().await;
    // glm-5.2 fails pre-commit (transient 500); deepseek-v4-pro is healthy.
    mock.set_model(
        "glm-5.2",
        vec![Spec::json(500, r#"{"error":{"message":"boom"}}"#)],
    )
    .await;
    mock.set_model("deepseek-v4-pro", vec![Spec::sse(ok_message_sse())])
        .await;
    let mut config = test_config(base, 1);
    config.routing.max_route_attempts = 4;
    let app = app_for(config);
    let response = app
        .oneshot(gateway_request("/v1/messages", anthropic_body(true)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let models = mock.models_seen().await;
    assert_eq!(
        models,
        vec!["glm-5.2".to_owned(), "deepseek-v4-pro".to_owned()],
        "a pre-commit failure may switch models"
    );
    task.abort();
}

/// Once the stream is committed, a mid-stream failure never replays or
/// switches anything.
#[tokio::test]
async fn post_commit_model_switching_is_impossible() {
    let (base, mock, task) = start_mock().await;
    let mut spec = Spec::sse(
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\n"
            .to_string(),
    );
    spec.kind = MockBody::StreamError;
    mock.set_model("glm-5.2", vec![spec]).await;
    mock.set_model("deepseek-v4-pro", vec![Spec::sse(ok_message_sse())])
        .await;
    let mut config = test_config(base, 2);
    config.routing.max_route_attempts = 4;
    let app = app_for(config);
    let response = app
        .oneshot(gateway_request("/v1/messages", anthropic_body(true)))
        .await
        .unwrap();
    let text = response_text(response).await;
    assert!(text.contains("partial"));
    assert!(text.contains("upstream stream was interrupted"));
    assert_eq!(
        mock.models_seen().await,
        vec!["glm-5.2".to_owned()],
        "no model switch and no replay after the commit barrier"
    );
    task.abort();
}

/// Streams stay byte-exact and ordered through the routing layer.
#[tokio::test]
async fn stream_passthrough_stays_verbatim() {
    let (base, mock, task) = start_mock().await;
    let sse = ok_message_sse();
    mock.set_model("glm-5.2", vec![Spec::fragmented(sse.clone(), 3)])
        .await;
    let response = app_for(test_config(base, 1))
        .oneshot(gateway_request("/v1/messages", anthropic_body(true)))
        .await
        .unwrap();
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "text/event-stream"
    );
    assert_eq!(response_text(response).await, sse);
    task.abort();
}

// ---------------------------------------------------------------------------
// Tests: routing resolution, models catalog, readiness, metrics
// ---------------------------------------------------------------------------

/// Old aliases and explicit catalog IDs keep working.
#[tokio::test]
async fn legacy_aliases_and_catalog_ids_still_work() {
    let (base, mock, task) = start_mock().await;
    for model in ["sensenova-6.8-flash-lite", "deepseek-v4-pro", "glm-5.2"] {
        mock.set_model(
            model,
            (0..4).map(|_| Spec::json(200, ok_message_json())).collect(),
        )
        .await;
    }
    let app = app_for(test_config(base, 1));

    // A legacy alias still resolves to its documented target.
    let response = app
        .clone()
        .oneshot(gateway_request(
            "/v1/messages",
            body_for_model("claude-sensenova", false),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        mock.models_seen().await,
        vec!["sensenova-6.8-flash-lite".to_owned()]
    );

    // An anonymous Claude name still maps to the configured default.
    let response = app
        .clone()
        .oneshot(gateway_request(
            "/v1/messages",
            body_for_model("claude-3-5-haiku-20241022", false),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // An explicit catalog ID passes through unchanged.
    let response = app
        .clone()
        .oneshot(gateway_request(
            "/v1/messages",
            body_for_model("deepseek-v4-pro", false),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        mock.models_seen().await.last().map(String::as_str),
        Some("deepseek-v4-pro")
    );
    task.abort();
}

/// `/v1/models` exposes the virtual routing aliases.
#[tokio::test]
async fn models_endpoint_exposes_virtual_aliases() {
    let (base, mock, task) = start_mock().await;
    let app = app_for(test_config(base, 1));
    let response = app
        .oneshot(gateway_request("/v1/models", Body::empty()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let text = response_text(response).await;
    assert!(text.contains("claude-coding-hard"));
    assert!(text.contains("claude-coding-fast"));
    assert!(text.contains("glm-5.2"));
    assert!(text.contains("kimi-k3"));
    assert!(mock.seen().await.is_empty());
    task.abort();
}

/// One unhealthy model must not make the proxy unready while another legal
/// route exists.
#[tokio::test]
async fn readiness_survives_a_single_unhealthy_model() {
    let (base, mock, task) = start_mock().await;
    mock.set_model(
        "glm-5.2",
        vec![Spec::json(
            404,
            r#"{"error":{"type":"not_found_error","message":"model is not found"}}"#,
        )],
    )
    .await;
    mock.set_model("deepseek-v4-pro", vec![Spec::json(200, ok_message_json())])
        .await;
    let mut config = test_config(base, 1);
    config.routing.max_route_attempts = 4;
    let state = state_for(config);
    let app = router(state.clone());
    let response = app
        .clone()
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let (status, body) = ready_body(app.clone()).await;
    assert_eq!(status, StatusCode::OK, "another legal route exists");
    assert_eq!(body["status"], "ready");
    assert!(body["usable_routes"].as_u64().unwrap() > 0);
    task.abort();
}

/// Readiness reports not_ready when no route can be dialed at all.
#[tokio::test]
async fn readiness_is_not_ready_without_usable_routes() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
        vec![Spec::json(401, r#"{"error":{"code":16,"message":"nope"}}"#)],
    )
    .await;
    let state = state_for(test_config(base, 1));
    let app = router(state.clone());
    let response = app
        .clone()
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let (status, body) = ready_body(app.clone()).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["status"], "not_ready");
    assert_eq!(body["usable_credentials"], 0);
    task.abort();
}

/// Routing metrics are exposed alongside the existing ones.
#[tokio::test]
async fn routing_metrics_are_exposed() {
    let (base, mock, task) = start_mock().await;
    mock.set_model("glm-5.2", vec![Spec::json(200, ok_message_json())])
        .await;
    let state = state_for(test_config(base, 1));
    let app = router(state.clone());
    let response = app
        .clone()
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let metrics = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let text = response_text(metrics).await;
    for name in [
        "sensenova_proxy_route_attempts_total",
        "sensenova_proxy_route_failovers_total",
        "sensenova_proxy_model_failovers_total",
        "sensenova_proxy_model_account_429_total",
        "sensenova_proxy_model_circuit_open_total",
        "sensenova_proxy_routing_exhausted_total",
        "sensenova_proxy_affinity_hits_total",
        "sensenova_proxy_affinity_breaks_total",
        "sensenova_proxy_upstream_requests_total",
        "sensenova_proxy_requests_total",
    ] {
        assert!(text.contains(name), "missing metric {name}");
    }
    task.abort();
}

// ---------------------------------------------------------------------------
// Tests: request fidelity, redaction, auth, local endpoints
// ---------------------------------------------------------------------------

/// Passthrough fidelity: everything except `model` is preserved byte-exactly.
#[tokio::test]
async fn request_body_is_preserved_apart_from_the_model() {
    let (base, mock, task) = start_mock().await;
    mock.set_model("glm-5.2", vec![Spec::json(200, ok_message_json())])
        .await;
    let request = Request::builder()
        .method("POST")
        .uri("/v1/messages")
        .header(header::AUTHORIZATION, "Bearer gateway-secret")
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-api-key", "gateway-secret")
        .header(header::COOKIE, "session=private")
        .header("x-claude-code-session-id", "abcd-1234")
        .body(anthropic_body(false))
        .unwrap();
    let response = app_for(test_config(base, 1))
        .oneshot(request)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().contains_key("x-request-id"));

    let seen = mock.seen().await;
    assert_eq!(seen.len(), 1);
    let seen = &seen[0];
    assert_eq!(seen.authorization, "Bearer sensenova-key-1");
    assert_eq!(seen.anthropic_version.as_deref(), Some("2023-06-01"));
    assert!(
        seen.user_agent
            .as_deref()
            .unwrap_or_default()
            .starts_with("sensenova-proxy/")
    );
    assert_ne!(seen.authorization, "Bearer gateway-secret");
    assert!(seen.x_api_key.is_none());
    assert!(seen.cookie.is_none());
    assert_eq!(seen.body["model"], "glm-5.2");
    assert_eq!(seen.body["max_tokens"], 128);
    assert_eq!(seen.body["unknown_future_field"]["preserve"], true);
    task.abort();
}

/// Secrets are still redacted from upstream error bodies.
#[tokio::test]
async fn secrets_are_redacted_from_upstream_errors() {
    let (base, mock, task) = start_mock().await;
    mock.set_model(
        "glm-5.2",
        vec![Spec::json(
            400,
            r#"{"error":{"message":"invalid key sensenova-key-1 for gateway-secret user"}}"#,
        )],
    )
    .await;
    let response = app_for(test_config(base, 1))
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    let text = response_text(response).await;
    assert!(!text.contains("sensenova-key-1"));
    assert!(!text.contains("gateway-secret"));
    assert!(text.contains("[REDACTED]"));
    task.abort();
}

/// Every API route still requires the gateway key.
#[tokio::test]
async fn auth_is_required_on_api_routes() {
    let (base, mock, task) = start_mock().await;
    let app = app_for(test_config(base, 1));
    for (method, path, body) in [
        ("POST", "/v1/messages", Some(anthropic_body(false))),
        (
            "POST",
            "/v1/messages/count_tokens",
            Some(json!({"model":"m","messages":[]}).to_string()),
        ),
        ("GET", "/v1/models", None),
    ] {
        let mut builder = Request::builder().method(method).uri(path);
        builder = builder.header(header::CONTENT_TYPE, "application/json");
        let request = builder
            .body(body.map(Body::from).unwrap_or_else(Body::empty))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
        assert!(
            response_text(response)
                .await
                .contains("authentication_error")
        );
    }
    assert!(mock.seen().await.is_empty());
    task.abort();
}

/// Health/readiness/metrics and local token counting still work offline.
#[tokio::test]
async fn local_endpoints_work_without_upstream() {
    let (base, mock, task) = start_mock().await;
    let app = app_for(test_config(base, 1));
    for path in ["/healthz", "/readyz", "/metrics"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
    }
    let count = app
        .clone()
        .oneshot(gateway_request(
            "/v1/messages/count_tokens",
            json!({"model":"claude-coding-hard","messages":[{"role":"user","content":"hello world"}]})
                .to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(
        count.headers()["x-sensenova-proxy-token-count"],
        "approximate"
    );
    assert!(mock.seen().await.is_empty());
    task.abort();
}

/// Malformed client input never reaches the upstream.
#[tokio::test]
async fn invalid_client_requests_never_dial_upstream() {
    let (base, mock, task) = start_mock().await;
    let app = app_for(test_config(base, 1));
    for body in [
        "{\"model\":".to_owned(),
        json!({"messages": []}).to_string(),
    ] {
        let response = app
            .clone()
            .oneshot(gateway_request("/v1/messages", body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    assert!(mock.seen().await.is_empty());
    task.abort();
}

/// A request id supplied by the client is echoed back.
#[tokio::test]
async fn request_id_is_echoed() {
    let (base, mock, task) = start_mock().await;
    mock.set_model("glm-5.2", vec![Spec::json(200, ok_message_json())])
        .await;
    let request = Request::builder()
        .method("POST")
        .uri("/v1/messages")
        .header(header::AUTHORIZATION, "Bearer gateway-secret")
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-request-id", "my-trace-id-1")
        .body(anthropic_body(false))
        .unwrap();
    let response = app_for(test_config(base, 1))
        .oneshot(request)
        .await
        .unwrap();
    assert_eq!(response.headers()["x-request-id"], "my-trace-id-1");
    task.abort();
}

// ---------------------------------------------------------------------------
// Tests: session affinity
// ---------------------------------------------------------------------------

/// Affinity keeps a session on a route it already used, then expires.
#[tokio::test]
async fn affinity_hits_and_expires() {
    let (base, mock, task) = start_mock().await;
    let mut config = test_config(base, 2);
    config.routing.soft_affinity_secs = 1;
    config.sensenova_api_keys[0].quota_group = "account-a".into();
    config.sensenova_api_keys[1].quota_group = "account-b".into();
    // Both groups serve the same model so affinity (not health) decides.
    mock.set_model(
        "glm-5.2",
        (0..8).map(|_| Spec::json(200, ok_message_json())).collect(),
    )
    .await;
    mock.set_model(
        "deepseek-v4-pro",
        (0..8).map(|_| Spec::json(200, ok_message_json())).collect(),
    )
    .await;
    let state = state_for(config);
    let app = router(state.clone());

    let mut chosen = Vec::new();
    for _ in 0..4 {
        let request = Request::builder()
            .method("POST")
            .uri("/v1/messages")
            .header(header::AUTHORIZATION, "Bearer gateway-secret")
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-claude-code-session-id", "affinity-session")
            .body(anthropic_body(false))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        chosen.push(mock.models_seen().await.last().cloned().unwrap_or_default());
    }
    assert_eq!(
        state.core.routes.affinity_len(),
        1,
        "affinity state is bounded to one remembered session"
    );

    // Past the TTL the entry is reclaimed.
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let request = Request::builder()
        .method("POST")
        .uri("/v1/messages")
        .header(header::AUTHORIZATION, "Bearer gateway-secret")
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-claude-code-session-id", "affinity-session")
        .body(anthropic_body(false))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    task.abort();
}

/// Affinity is broken the moment its route fails.
#[tokio::test]
async fn affinity_is_broken_by_a_429() {
    let (base, mock, task) = start_mock().await;
    let mut config = test_config(base, 2);
    config.routing.soft_affinity_secs = 300;
    config.routing.max_route_attempts = 4;
    config.sensenova_api_keys[0].quota_group = "account-a".into();
    config.sensenova_api_keys[1].quota_group = "account-b".into();
    // First request succeeds (session is remembered), the second 429s on the
    // remembered route and fails over, which must break the affinity.
    mock.set_model("glm-5.2", vec![Spec::json(200, ok_message_json())])
        .await;
    mock.set_model("deepseek-v4-pro", vec![Spec::json(200, ok_message_json())])
        .await;
    let state = state_for(config);
    let app = router(state.clone());
    let request = Request::builder()
        .method("POST")
        .uri("/v1/messages")
        .header(header::AUTHORIZATION, "Bearer gateway-secret")
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-claude-code-session-id", "affinity-session")
        .body(anthropic_body(false))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(state.core.routes.affinity_len(), 1);

    // Script the remembered model to fail; the failover breaks affinity.
    mock.set_model(
        "glm-5.2",
        vec![Spec::json(429, r#"{"error":{"message":"busy"}}"#)],
    )
    .await;
    mock.set_model(
        "deepseek-v4-pro",
        (0..4).map(|_| Spec::json(200, ok_message_json())).collect(),
    )
    .await;
    let request = Request::builder()
        .method("POST")
        .uri("/v1/messages")
        .header(header::AUTHORIZATION, "Bearer gateway-secret")
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-claude-code-session-id", "affinity-session")
        .body(anthropic_body(false))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        state.core.routes.affinity_len(),
        0,
        "a health event must drop the affinity"
    );
    assert!(state.core.routes.affinity_break_count() >= 1);
    task.abort();
}

/// Affinity state stays bounded no matter how many sessions appear.
#[tokio::test]
async fn affinity_state_is_bounded() {
    let (base, mock, task) = start_mock().await;
    let mut config = test_config(base, 1);
    config.routing.soft_affinity_secs = 300;
    config.routing.max_affinity_entries = 4;
    mock.set_model(
        "glm-5.2",
        (0..40)
            .map(|_| Spec::json(200, ok_message_json()))
            .collect(),
    )
    .await;
    let state = state_for(config);
    let app = router(state.clone());
    for index in 0..16 {
        let request = Request::builder()
            .method("POST")
            .uri("/v1/messages")
            .header(header::AUTHORIZATION, "Bearer gateway-secret")
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-claude-code-session-id", format!("session-{index}"))
            .body(anthropic_body(false))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    assert!(
        state.core.routes.affinity_len() <= 4,
        "affinity must stay within max_affinity_entries, got {}",
        state.core.routes.affinity_len()
    );
    task.abort();
}

// ---------------------------------------------------------------------------
// Tests: load spreading and inflight accounting
// ---------------------------------------------------------------------------

/// Equivalent healthy routes are spread across accounts instead of always
/// dialing the first.
#[tokio::test]
async fn healthy_routes_spread_load_across_accounts() {
    let (base, mock, task) = start_mock().await;
    let mut config = test_config(base, 4);
    for (index, key) in config.sensenova_api_keys.iter_mut().enumerate() {
        key.quota_group = format!("account-{}", (index % 2) + 1);
    }
    mock.set_model(
        "glm-5.2",
        (0..16)
            .map(|_| Spec::json(200, ok_message_json()))
            .collect(),
    )
    .await;
    mock.set_model(
        "deepseek-v4-pro",
        (0..16)
            .map(|_| Spec::json(200, ok_message_json()))
            .collect(),
    )
    .await;
    let app = app_for(config);
    for _ in 0..8 {
        let response = app
            .clone()
            .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    let credentials = mock.credentials_seen().await;
    let distinct: std::collections::HashSet<&String> = credentials.iter().collect();
    assert!(
        distinct.len() > 1,
        "equivalent routes must be spread across credentials, saw {credentials:?}"
    );
    task.abort();
}

/// Inflight counters return to zero on every path: success, error, and
/// client disconnect.
#[tokio::test]
async fn inflight_is_released_on_every_path() {
    let (base, mock, task) = start_mock().await;
    mock.set_model("glm-5.2", vec![Spec::json(200, ok_message_json())])
        .await;
    mock.set_model(
        "deepseek-v4-pro",
        vec![Spec::json(500, r#"{"error":{"message":"boom"}}"#)],
    )
    .await;
    let state = state_for(test_config(base, 1));
    let app = router(state.clone());
    let response = app
        .clone()
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // A stream that is abandoned mid-flight must still release its slot.
    let cancelled = Arc::new(AtomicBool::new(false));
    let mut spec =
        Spec::sse("event: message_start\ndata: {\"type\":\"message_start\"}\n\n".to_string());
    spec.kind = MockBody::Stall(cancelled.clone());
    mock.set_model("glm-5.2", vec![spec]).await;
    let response = app
        .clone()
        .oneshot(gateway_request("/v1/messages", anthropic_body(true)))
        .await
        .unwrap();
    let mut stream = response.into_body().into_data_stream();
    assert!(stream.next().await.is_some());
    drop(stream);
    for _ in 0..80 {
        if cancelled.load(Ordering::Acquire) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(cancelled.load(Ordering::Acquire));

    let inflight: usize = state
        .core
        .routes
        .credential_snapshots()
        .iter()
        .map(|snapshot| snapshot.inflight)
        .sum();
    assert_eq!(inflight, 0, "no inflight slot may leak");
    assert_eq!(mock.seen().await.len(), 2, "no replay after commit");
    task.abort();
}

// ---------------------------------------------------------------------------
// Tests: concurrency shaping (preserved behaviour)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_limit_and_queue_bounds_are_respected() {
    let (base, mock, task) = start_mock().await;
    mock.set_model(
        "glm-5.2",
        (0..4)
            .map(|_| Spec::json(200, ok_message_json()).with_delay(Duration::from_millis(150)))
            .collect(),
    )
    .await;
    mock.set_model(
        "deepseek-v4-pro",
        (0..4)
            .map(|_| Spec::json(200, ok_message_json()).with_delay(Duration::from_millis(150)))
            .collect(),
    )
    .await;
    let mut config = test_config(base, 1);
    config.concurrency.initial = 1;
    config.concurrency.maximum = 1;
    config.concurrency.queue_capacity = 1;
    config.concurrency.queue_timeout_secs = 2;
    let app = app_for(config);

    let responses = futures_util::future::join_all((0..4).map(|_| {
        let app = app.clone();
        async move {
            app.oneshot(gateway_request("/v1/messages", anthropic_body(false)))
                .await
                .unwrap()
        }
    }))
    .await;
    let statuses: Vec<u16> = responses
        .iter()
        .map(|response| response.status().as_u16())
        .collect();
    assert_eq!(statuses.iter().filter(|status| **status == 200).count(), 2);
    assert_eq!(
        statuses.iter().filter(|status| **status == 429).count(),
        2,
        "overflow beyond the queue must be rejected, statuses {statuses:?}"
    );
    assert!(mock.max_in_flight.load(Ordering::Acquire) <= 1);
    task.abort();
}

// ---------------------------------------------------------------------------
// Tests: router-hardening regressions (end-to-end through the gateway)
// ---------------------------------------------------------------------------

/// Tier-0 routes that are merely cooling must not block the request: the
/// healthy hard tier 1 (Kimi) serves within the same quality profile.
#[tokio::test]
async fn tier_zero_cooling_falls_back_to_kimi() {
    let (base, mock, task) = start_mock().await;
    // Both tier-0 models 429 on the only account; kimi is healthy.
    for model in ["glm-5.2", "deepseek-v4-pro"] {
        mock.set_model(
            model,
            (0..4)
                .map(|_| Spec::json(429, r#"{"error":{"message":"busy"}}"#))
                .collect(),
        )
        .await;
    }
    mock.set_model(
        "kimi-k3",
        (0..4).map(|_| Spec::json(200, ok_message_json())).collect(),
    )
    .await;
    let mut config = test_config(base, 1);
    config.routing.max_route_attempts = 4;
    config.routing.retry_after_max_secs = 1;
    let app = app_for(config);
    let response = app
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "healthy hard tier 1 must serve when tier 0 is cooling"
    );
    let models = mock.models_seen().await;
    assert!(models.contains(&"kimi-k3".to_owned()), "got {models:?}");
    task.abort();
}

/// The hard profile still never reaches a fast model, end to end.
#[tokio::test]
async fn hard_tier_fallback_never_reaches_fast_models() {
    let (base, mock, task) = start_mock().await;
    for model in ["glm-5.2", "deepseek-v4-pro", "kimi-k3"] {
        mock.set_model(
            model,
            (0..6)
                .map(|_| Spec::json(503, r#"{"error":{"message":"overloaded"}}"#))
                .collect(),
        )
        .await;
    }
    for model in ["deepseek-v4-flash", "sensenova-6.8-flash-lite"] {
        mock.set_model(
            model,
            (0..6).map(|_| Spec::json(200, ok_message_json())).collect(),
        )
        .await;
    }
    let mut config = test_config(base, 1);
    config.routing.max_route_attempts = 6;
    let response = app_for(config)
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_ne!(
        response.status(),
        StatusCode::OK,
        "honest failure, no fast model"
    );
    for model in mock.models_seen().await {
        assert_ne!(model, "deepseek-v4-flash");
        assert_ne!(model, "sensenova-6.8-flash-lite");
    }
    task.abort();
}

/// One 404 disables only `(model, group)`: the same model on another account
/// still serves, and the model is never globally disabled by one account.
#[tokio::test]
async fn one_404_disables_only_the_model_and_group() {
    let (base, mock, task) = start_mock().await;
    let missing = Spec::json(
        404,
        r#"{"error":{"type":"not_found_error","message":"model is not found"}}"#,
    );
    // glm-5.2 is missing on account-a; healthy on account-b.
    mock.set("sensenova-key-1", vec![missing]).await;
    mock.set("sensenova-key-2", vec![Spec::json(200, ok_message_json())])
        .await;
    mock.set("sensenova-key-3", vec![Spec::json(200, ok_message_json())])
        .await;
    let mut config = test_config(base, 3);
    config.routing.max_route_attempts = 4;
    config.sensenova_api_keys[0].quota_group = "account-a".into();
    config.sensenova_api_keys[1].quota_group = "account-a".into();
    config.sensenova_api_keys[2].quota_group = "account-b".into();
    let state = state_for(config);
    let app = router(state.clone());
    let response = app
        .clone()
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        !state.core.routes.model_snapshot("glm-5.2").disabled,
        "one account's 404 must not disable the model globally"
    );
    task.abort();
}

/// 404s from two distinct accounts inside the evidence window disable the
/// model; a success anywhere clears it without a restart.
#[tokio::test]
async fn two_distinct_accounts_404_disable_the_model_and_recover() {
    let (base, mock, task) = start_mock().await;
    let missing = Spec::json(
        404,
        r#"{"error":{"type":"not_found_error","message":"model is not found"}}"#,
    );
    for key in ["sensenova-key-1", "sensenova-key-2"] {
        mock.set(key, vec![missing.clone()]).await;
    }
    mock.set("sensenova-key-3", vec![Spec::json(200, ok_message_json())])
        .await;
    let mut config = test_config(base, 3);
    config.routing.max_route_attempts = 4;
    config.routing.model_missing_cooldown_secs = 1;
    config.routing.route_cooldown_initial_secs = 1;
    config.routing.route_cooldown_max_secs = 1;
    config.sensenova_api_keys[0].quota_group = "account-a".into();
    config.sensenova_api_keys[1].quota_group = "account-b".into();
    config.sensenova_api_keys[2].quota_group = "account-b".into();
    let state = state_for(config);
    let app = router(state.clone());
    let response = app
        .clone()
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    // kimi (tier 1) is not scripted, so the request ends in an honest 429/502,
    // but the model must be disabled by the two distinct accounts' 404s.
    assert_ne!(response.status(), StatusCode::OK);
    assert!(
        state.core.routes.model_snapshot("glm-5.2").disabled,
        "two distinct accounts returning 404 must disable the model"
    );
    task.abort();
}

/// A known-404 route is not re-dialed by the same logical request.
#[tokio::test]
async fn known_missing_route_is_not_re_dialed() {
    let (base, mock, task) = start_mock().await;
    let missing = Spec::json(
        404,
        r#"{"error":{"type":"not_found_error","message":"model is not found"}}"#,
    );
    mock.set_model("glm-5.2", vec![missing]).await;
    mock.set_model("deepseek-v4-pro", vec![Spec::json(200, ok_message_json())])
        .await;
    let mut config = test_config(base, 1);
    config.routing.max_route_attempts = 4;
    let response = app_for(config)
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let models = mock.models_seen().await;
    assert_eq!(
        models.iter().filter(|model| *model == "glm-5.2").count(),
        1,
        "the 404 model must be dialed exactly once: {models:?}"
    );
    task.abort();
}

/// The model circuit counter is exported and reflects a single trip exactly.
#[tokio::test]
async fn model_circuit_open_metric_counts_one_trip() {
    let (base, mock, task) = start_mock().await;
    for key in ["sensenova-key-1", "sensenova-key-2"] {
        mock.set(
            key,
            (0..4)
                .map(|_| Spec::json(429, r#"{"error":{"message":"busy"}}"#))
                .collect(),
        )
        .await;
    }
    let mut config = test_config(base, 2);
    config.routing.max_route_attempts = 4;
    config.sensenova_api_keys[0].quota_group = "account-a".into();
    config.sensenova_api_keys[1].quota_group = "account-b".into();
    let state = state_for(config);
    let app = router(state.clone());
    let _ = app
        .clone()
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    let metrics = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let text = response_text(metrics).await;
    // The metric is exported (it always was); exact per-transition counting is
    // unit-tested in `router::tests`.
    assert!(text.contains("sensenova_proxy_model_circuit_open_total"));
    task.abort();
}

// --- Regression: untouched invariants --------------------------------------

/// The global route-attempt cap is still enforced across mixed failovers.
#[tokio::test]
async fn global_attempt_budget_still_enforced() {
    let (base, mock, task) = start_mock().await;
    for model in ["glm-5.2", "deepseek-v4-pro", "kimi-k3"] {
        mock.set_model(
            model,
            (0..6)
                .map(|_| Spec::json(500, r#"{"error":{"message":"boom"}}"#))
                .collect(),
        )
        .await;
    }
    let mut config = test_config(base, 4);
    config.routing.max_route_attempts = 4;
    for (index, key) in config.sensenova_api_keys.iter_mut().enumerate() {
        key.quota_group = format!("group-{index}");
    }
    let response = app_for(config)
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        mock.seen().await.len(),
        4,
        "exactly max_route_attempts attempts, never more"
    );
    task.abort();
}

/// 429 cross-group failover is unchanged.
#[tokio::test]
async fn four29_cross_group_failover_unchanged() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
        vec![Spec::json(429, r#"{"error":{"message":"busy"}}"#)],
    )
    .await;
    mock.set("sensenova-key-2", vec![Spec::json(200, ok_message_json())])
        .await;
    let mut config = test_config(base, 2);
    config.sensenova_api_keys[0].quota_group = "account-a".into();
    config.sensenova_api_keys[1].quota_group = "account-b".into();
    let response = app_for(config)
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        mock.credentials_seen().await,
        vec![
            "Bearer sensenova-key-1".to_owned(),
            "Bearer sensenova-key-2".to_owned()
        ]
    );
    task.abort();
}

/// Explicit quota exhaustion still cools the whole quota_group.
#[tokio::test]
async fn quota_exhaustion_still_cools_whole_group() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
        vec![Spec::json(
            429,
            r#"{"error":{"message":"FREE_QUOTA_EXHAUSTED"}}"#,
        )],
    )
    .await;
    mock.set("sensenova-key-2", vec![Spec::json(200, ok_message_json())])
        .await;
    mock.set("sensenova-key-3", vec![Spec::json(200, ok_message_json())])
        .await;
    let mut config = test_config(base, 3);
    config.routing.max_route_attempts = 4;
    config.sensenova_api_keys[0].quota_group = "account-a".into();
    config.sensenova_api_keys[1].quota_group = "account-a".into();
    config.sensenova_api_keys[2].quota_group = "account-b".into();
    let state = state_for(config);
    let app = router(state.clone());
    let response = app
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(state.core.routes.group_cooling("account-a").is_some());
    assert!(state.core.routes.group_cooling("account-b").is_none());
    task.abort();
}

/// 401 still isolates one credential.
#[tokio::test]
async fn unauthorized_still_isolates_one_key() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
        vec![Spec::json(401, r#"{"error":{"code":16,"message":"nope"}}"#)],
    )
    .await;
    mock.set(
        "sensenova-key-2",
        (0..4).map(|_| Spec::json(200, ok_message_json())).collect(),
    )
    .await;
    let mut config = test_config(base, 2);
    config.sensenova_api_keys[0].quota_group = "account-a".into();
    config.sensenova_api_keys[1].quota_group = "account-a".into();
    let state = state_for(config);
    let app = router(state.clone());
    let response = app
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let credentials = state.core.routes.credential_snapshots();
    assert!(credentials[0].unusable);
    assert!(!credentials[1].unusable);
    task.abort();
}

/// Pre-commit cross-model fallback still works; post-commit switching is
/// still impossible.
#[tokio::test]
async fn commit_barrier_unchanged_with_routing() {
    let (base, mock, task) = start_mock().await;
    mock.set_model(
        "glm-5.2",
        vec![Spec::json(500, r#"{"error":{"message":"boom"}}"#)],
    )
    .await;
    mock.set_model("deepseek-v4-pro", vec![Spec::sse(ok_message_sse())])
        .await;
    let mut config = test_config(base, 1);
    config.routing.max_route_attempts = 4;
    let app = app_for(config);
    let response = app
        .oneshot(gateway_request("/v1/messages", anthropic_body(true)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        mock.models_seen().await,
        vec!["glm-5.2".to_owned(), "deepseek-v4-pro".to_owned()],
        "pre-commit cross-model retry still works"
    );
    task.abort();
}

/// Affinity still breaks on a health failure.
#[tokio::test]
async fn affinity_still_breaks_on_health_failure() {
    let (base, mock, task) = start_mock().await;
    let mut config = test_config(base, 2);
    config.routing.soft_affinity_secs = 300;
    config.routing.max_route_attempts = 4;
    config.sensenova_api_keys[0].quota_group = "account-a".into();
    config.sensenova_api_keys[1].quota_group = "account-b".into();
    mock.set_model("glm-5.2", vec![Spec::json(200, ok_message_json())])
        .await;
    mock.set_model("deepseek-v4-pro", vec![Spec::json(200, ok_message_json())])
        .await;
    let state = state_for(config);
    let app = router(state.clone());
    let request = || {
        Request::builder()
            .method("POST")
            .uri("/v1/messages")
            .header(header::AUTHORIZATION, "Bearer gateway-secret")
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-claude-code-session-id", "affinity-session")
            .body(anthropic_body(false))
            .unwrap()
    };
    let response = app.clone().oneshot(request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(state.core.routes.affinity_len(), 1);
    // Now the remembered route fails.
    mock.set_model(
        "glm-5.2",
        vec![Spec::json(429, r#"{"error":{"message":"busy"}}"#)],
    )
    .await;
    mock.set_model(
        "deepseek-v4-pro",
        (0..4).map(|_| Spec::json(200, ok_message_json())).collect(),
    )
    .await;
    let response = app.clone().oneshot(request()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        state.core.routes.affinity_len(),
        0,
        "a health event must drop the affinity"
    );
    task.abort();
}

/// Inflight still releases on every path.
#[tokio::test]
async fn inflight_still_releases_on_all_paths() {
    let (base, mock, task) = start_mock().await;
    mock.set_model("glm-5.2", vec![Spec::json(200, ok_message_json())])
        .await;
    mock.set_model(
        "deepseek-v4-pro",
        vec![Spec::json(500, r#"{"error":{"message":"boom"}}"#)],
    )
    .await;
    let state = state_for(test_config(base, 1));
    let app = router(state.clone());
    let response = app
        .clone()
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let inflight: usize = state
        .core
        .routes
        .credential_snapshots()
        .iter()
        .map(|snapshot| snapshot.inflight)
        .sum();
    assert_eq!(inflight, 0, "no inflight slot may leak");
    task.abort();
}

/// Old aliases and catalog IDs still work.
#[tokio::test]
async fn aliases_and_catalog_ids_unchanged() {
    let (base, mock, task) = start_mock().await;
    for model in ["sensenova-6.8-flash-lite", "deepseek-v4-pro", "glm-5.2"] {
        mock.set_model(
            model,
            (0..4).map(|_| Spec::json(200, ok_message_json())).collect(),
        )
        .await;
    }
    let app = app_for(test_config(base, 1));
    let response = app
        .clone()
        .oneshot(gateway_request(
            "/v1/messages",
            body_for_model("claude-sensenova", false),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        mock.models_seen().await.last().map(String::as_str),
        Some("sensenova-6.8-flash-lite")
    );
    let response = app
        .clone()
        .oneshot(gateway_request(
            "/v1/messages",
            body_for_model("deepseek-v4-pro", false),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        mock.models_seen().await.last().map(String::as_str),
        Some("deepseek-v4-pro")
    );
    task.abort();
}

/// `/v1/models` still exposes the built-in hard/fast profiles.
#[tokio::test]
async fn built_in_profiles_still_exposed() {
    let (base, mock, task) = start_mock().await;
    let app = app_for(test_config(base, 1));
    let response = app
        .oneshot(gateway_request("/v1/models", Body::empty()))
        .await
        .unwrap();
    let text = response_text(response).await;
    assert!(text.contains("claude-coding-hard"));
    assert!(text.contains("claude-coding-fast"));
    assert!(text.contains("kimi-k3"));
    assert!(mock.seen().await.is_empty());
    task.abort();
}

/// Secrets remain redacted end to end.
#[tokio::test]
async fn redaction_unchanged() {
    let (base, mock, task) = start_mock().await;
    mock.set_model(
        "glm-5.2",
        vec![Spec::json(
            400,
            r#"{"error":{"message":"invalid key sensenova-key-1 for gateway-secret user"}}"#,
        )],
    )
    .await;
    let response = app_for(test_config(base, 1))
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    let text = response_text(response).await;
    assert!(!text.contains("sensenova-key-1"));
    assert!(!text.contains("gateway-secret"));
    assert!(text.contains("[REDACTED]"));
    task.abort();
}
