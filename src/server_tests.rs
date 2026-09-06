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
use crate::config::SensenovaKeyConfig;

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
    fn content_type_sse(mut self) -> Self {
        self.content_type = "text/event-stream";
        self
    }
}

impl MockUpstream {
    async fn set(&self, key: &str, specs: Vec<Spec>) {
        self.sequences
            .lock()
            .await
            .insert(key.to_owned(), VecDeque::from(specs));
    }

    async fn seen(&self) -> Vec<SeenRequest> {
        self.calls.lock().await.clone()
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
    let spec = mock
        .sequences
        .lock()
        .await
        .get_mut(&key)
        .and_then(VecDeque::pop_front)
        .unwrap_or_else(|| Spec::json(500, r#"{"error":{"message":"unscripted"}}"#));
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
            // The delay lets hyper flush the initial bytes to the client
            // before the reset; an immediate RST would discard them.
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

fn app_for(config: crate::config::Config) -> Router {
    router(AppState::new(config).unwrap())
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

fn anthropic_body(stream: bool) -> String {
    json!({
        "model": "claude-sensenova",
        "max_tokens": 128,
        "stream": stream,
        "messages": [{"role": "user", "content": "hello"}],
        "unknown_future_field": {"preserve": true}
    })
    .to_string()
}

fn ok_message_json() -> String {
    json!({
        "id": "msg_1", "type": "message", "role": "assistant",
        "content": [{"type": "text", "text": "OK"}],
        "model": "sensenova-6.8-flash-lite", "stop_reason": "end_turn",
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

// ---------------------------------------------------------------------------
// Tests: passthrough and protocol fidelity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn nonstream_passthrough_rewrites_model_and_preserves_body() {
    let (base, mock, task) = start_mock().await;
    mock.set("sensenova-key-1", vec![Spec::json(200, ok_message_json())])
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
    // Gateway credential and client cookies never travel upstream.
    assert_ne!(seen.authorization, "Bearer gateway-secret");
    assert!(seen.x_api_key.is_none());
    assert!(seen.cookie.is_none());
    // Model rewritten, everything else preserved, max_tokens defaulted when
    // missing.
    assert_eq!(seen.body["model"], "sensenova-6.8-flash-lite");
    assert_eq!(seen.body["max_tokens"], 128);
    assert_eq!(seen.body["unknown_future_field"]["preserve"], true);

    let text = response_text(response).await;
    assert!(text.contains("\"stop_reason\":\"end_turn\""));
    task.abort();
}

#[tokio::test]
async fn unknown_models_map_to_default() {
    let (base, mock, task) = start_mock().await;
    mock.set("sensenova-key-1", vec![Spec::json(200, ok_message_json())])
        .await;
    let body = json!({
        "model": "claude-3-5-haiku-20241022",
        "max_tokens": 16,
        "messages": [{"role": "user", "content": "hi"}]
    })
    .to_string();
    let response = app_for(test_config(base, 1))
        .oneshot(gateway_request("/v1/messages", body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let seen = mock.seen().await;
    assert_eq!(seen[0].body["model"], "sensenova-6.8-flash-lite");
    task.abort();
}

#[tokio::test]
async fn missing_max_tokens_is_defaulted() {
    let (base, mock, task) = start_mock().await;
    mock.set("sensenova-key-1", vec![Spec::json(200, ok_message_json())])
        .await;
    let body = json!({
        "model": "claude-sensenova",
        "messages": [{"role": "user", "content": "hi"}]
    })
    .to_string();
    let response = app_for(test_config(base, 1))
        .oneshot(gateway_request("/v1/messages", body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let seen = mock.seen().await;
    assert_eq!(seen[0].body["max_tokens"], 8_192);
    task.abort();
}

#[tokio::test]
async fn stream_passthrough_is_verbatim_and_ordered() {
    let (base, mock, task) = start_mock().await;
    let sse = ok_message_sse();
    mock.set("sensenova-key-1", vec![Spec::sse(sse.clone())])
        .await;
    let response = app_for(test_config(base, 1))
        .oneshot(gateway_request("/v1/messages", anthropic_body(true)))
        .await
        .unwrap();
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "text/event-stream"
    );
    let text = response_text(response).await;
    assert_eq!(text, sse);
    assert!(text.ends_with("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"));
    task.abort();
}

#[tokio::test]
async fn fragmented_sse_passes_through_byte_exact() {
    let (base, mock, task) = start_mock().await;
    let sse = ok_message_sse();
    mock.set("sensenova-key-1", vec![Spec::fragmented(sse.clone(), 3)])
        .await;
    let response = app_for(test_config(base, 1))
        .oneshot(gateway_request("/v1/messages", anthropic_body(true)))
        .await
        .unwrap();
    let text = response_text(response).await;
    assert_eq!(text, sse);
    task.abort();
}

#[tokio::test]
async fn tool_use_stream_is_counted_and_forwarded() {
    let (base, mock, task) = start_mock().await;
    let sse = "event: message_start\ndata: {\"type\":\"message_start\"}\n\n\
               event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"call_x\",\"name\":\"Read\",\"input\":{}}}\n\n\
               event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"path\\\":\"}}\n\n\
               event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"\\\"a\\\"}\"}}\n\n\
               event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":1}\n\n\
               event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n\
               event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";
    mock.set("sensenova-key-1", vec![Spec::sse(sse)]).await;
    let response = app_for(test_config(base, 1))
        .oneshot(gateway_request("/v1/messages", anthropic_body(true)))
        .await
        .unwrap();
    let text = response_text(response).await;
    assert!(text.contains("input_json_delta"));
    assert!(text.contains("stop_reason\":\"tool_use"));
    task.abort();
}

// ---------------------------------------------------------------------------
// Tests: stream failures never replay
// ---------------------------------------------------------------------------

#[tokio::test]
async fn midstream_failure_emits_error_event_and_never_replays() {
    let (base, mock, task) = start_mock().await;
    let mut spec = Spec::sse(
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\n"
            .to_string(),
    );
    spec.kind = MockBody::StreamError;
    mock.set("sensenova-key-1", vec![spec]).await;
    mock.set("sensenova-key-2", vec![Spec::sse(ok_message_sse())])
        .await;
    let response = app_for(test_config(base, 2))
        .oneshot(gateway_request("/v1/messages", anthropic_body(true)))
        .await
        .unwrap();
    let text = response_text(response).await;
    assert!(text.contains("partial"));
    assert!(text.contains("upstream stream was interrupted"));
    // No replay: exactly one upstream call even though a second key exists.
    assert_eq!(mock.seen().await.len(), 1);
    task.abort();
}

#[tokio::test]
async fn malformed_sse_terminates_stream_without_replay() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
        vec![Spec::sse("data: this-is-not-json\n\n")],
    )
    .await;
    let response = app_for(test_config(base, 2))
        .oneshot(gateway_request("/v1/messages", anthropic_body(true)))
        .await
        .unwrap();
    let text = response_text(response).await;
    assert!(text.contains("this-is-not-json"));
    assert!(text.contains("upstream sent invalid SSE data"));
    assert_eq!(mock.seen().await.len(), 1);
    task.abort();
}

#[tokio::test]
async fn stream_ending_without_message_stop_gets_error_event() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
        vec![Spec::sse(
            "event: message_start\ndata: {\"type\":\"message_start\"}\n\n",
        )],
    )
    .await;
    let response = app_for(test_config(base, 1))
        .oneshot(gateway_request("/v1/messages", anthropic_body(true)))
        .await
        .unwrap();
    let text = response_text(response).await;
    assert!(text.contains("upstream stream ended before message_stop"));
    assert_eq!(mock.seen().await.len(), 1);
    task.abort();
}

#[tokio::test]
async fn client_disconnect_cancels_upstream_and_never_replays() {
    let (base, mock, task) = start_mock().await;
    let cancelled = Arc::new(AtomicBool::new(false));
    let mut spec =
        Spec::sse("event: message_start\ndata: {\"type\":\"message_start\"}\n\n".to_string());
    spec.kind = MockBody::Stall(cancelled.clone());
    mock.set("sensenova-key-1", vec![spec]).await;
    mock.set("sensenova-key-2", vec![Spec::sse(ok_message_sse())])
        .await;
    let response = app_for(test_config(base, 2))
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
    assert_eq!(mock.seen().await.len(), 1);
    task.abort();
}

// ---------------------------------------------------------------------------
// Tests: rate limits, quota, circuit breaker
// ---------------------------------------------------------------------------

#[tokio::test]
async fn direct_429_with_long_hint_returns_429_without_second_call() {
    let (base, mock, task) = start_mock().await;
    let mut limited = Spec::json(429, r#"{"error":{"message":"slow down"}}"#);
    limited.headers.push(("retry-after", "17"));
    mock.set("sensenova-key-1", vec![limited]).await;
    let state = AppState::new(test_config(base, 1)).unwrap();
    let response = router(state.clone())
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()[header::RETRY_AFTER], "17");
    let body: Value = serde_json::from_str(&response_text(response).await).unwrap();
    assert_eq!(body["error"]["type"], "rate_limit_error");
    assert_eq!(mock.seen().await.len(), 1);
    task.abort();
}

#[tokio::test]
async fn long_hint_is_parsed_from_human_text() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
        vec![Spec::json(
            429,
            r#"{"error":{"message":"Daily free limit reached. Try again in 2h 30m"}}"#,
        )],
    )
    .await;
    let response = app_for(test_config(base, 1))
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    let retry_after = response.headers()[header::RETRY_AFTER].to_str().unwrap();
    assert_eq!(retry_after, "9000");
    task.abort();
}

#[tokio::test]
async fn short_429_is_waited_out_on_the_same_key() {
    let (base, mock, task) = start_mock().await;
    let mut short = Spec::json(429, r#"{"error":{"message":"busy"}}"#);
    short.headers.push(("retry-after", "1"));
    mock.set(
        "sensenova-key-1",
        vec![short, Spec::json(200, ok_message_json())],
    )
    .await;
    let response = app_for(test_config(base, 1))
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "short 429 must be retried"
    );
    assert_eq!(mock.seen().await.len(), 2);
    task.abort();
}

#[tokio::test]
async fn quota_exhaustion_opens_circuit_and_blocks_followup_requests() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
        vec![Spec::json(
            429,
            r#"{"error":{"code":8,"message":"RESOURCE_EXHAUSTED: free quota exhausted"}}"#,
        )],
    )
    .await;
    let state = AppState::new(test_config(base, 1)).unwrap();
    let app = router(state.clone());
    let first = app
        .clone()
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::TOO_MANY_REQUESTS);
    let retry_after_present = first.headers().contains_key(header::RETRY_AFTER);
    let body: Value = serde_json::from_str(&response_text(first).await).unwrap();
    assert_eq!(body["error"]["type"], "rate_limit_error");
    assert!(retry_after_present);

    // Circuit is open: the second request must not reach the upstream.
    let second = app
        .clone()
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    let text = response_text(second).await;
    assert!(text.contains("circuit open"));
    assert_eq!(mock.seen().await.len(), 1);
    task.abort();
}

#[tokio::test]
async fn upstream_401_fails_over_to_next_key_and_sticks() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
        vec![Spec::json(
            401,
            r#"{"error":{"code":16,"message":"Authorization Not Found"}}"#,
        )],
    )
    .await;
    mock.set(
        "sensenova-key-2",
        vec![
            Spec::json(200, ok_message_json()),
            Spec::json(200, ok_message_json()),
        ],
    )
    .await;
    let app = app_for(test_config(base, 2));
    for _ in 0..2 {
        let response = app
            .clone()
            .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    let authorizations: Vec<String> = mock
        .seen()
        .await
        .into_iter()
        .map(|request| request.authorization)
        .collect();
    assert_eq!(
        authorizations,
        vec![
            "Bearer sensenova-key-1".to_string(),
            "Bearer sensenova-key-2".to_string(),
            "Bearer sensenova-key-2".to_string(),
        ]
    );
    task.abort();
}

#[tokio::test]
async fn upstream_401_with_single_key_marks_gateway_not_ready() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
        vec![Spec::json(401, r#"{"error":{"code":16,"message":"nope"}}"#)],
    )
    .await;
    let state = AppState::new(test_config(base, 1)).unwrap();
    let app = router(state);
    let response = app
        .clone()
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body: Value = serde_json::from_str(&response_text(response).await).unwrap();
    assert_eq!(body["error"]["type"], "authentication_error");

    let ready = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    let ready_body: Value = serde_json::from_str(&response_text(ready).await).unwrap();
    assert_eq!(ready_body["status"], "not_ready");
    task.abort();
}

// ---------------------------------------------------------------------------
// Tests: transient failures, retries, and error fidelity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transient_500_is_retried_within_budget() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
        vec![
            Spec::json(500, r#"{"error":{"message":"internal"}}"#),
            Spec::json(200, ok_message_json()),
        ],
    )
    .await;
    let response = app_for(test_config(base, 1))
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(mock.seen().await.len(), 2);
    task.abort();
}

#[tokio::test]
async fn persistent_502_exhausts_budget_and_returns_502() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
        vec![
            Spec::json(502, "bad gateway"),
            Spec::json(502, "bad gateway"),
        ],
    )
    .await;
    let response = app_for(test_config(base, 1))
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let retry_after_absent = !response.headers().contains_key(header::RETRY_AFTER);
    let body: Value = serde_json::from_str(&response_text(response).await).unwrap();
    assert_eq!(body["error"]["type"], "api_error");
    assert!(retry_after_absent);
    assert_eq!(mock.seen().await.len(), 2);
    task.abort();
}

#[tokio::test]
async fn false_positive_429_text_is_not_treated_as_rate_limit() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
        vec![
            Spec::json(500, "request 429123 referenced status 429 in a note"),
            Spec::json(500, "request 429123 referenced status 429 in a note"),
        ],
    )
    .await;
    let state = AppState::new(test_config(base, 1)).unwrap();
    let app = router(state.clone());
    let response = app
        .clone()
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    // Upstream 5xx statuses are preserved (Retry-After absent).
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(!response.headers().contains_key(header::RETRY_AFTER));
    // The key must not be cooling: a later request dials upstream again.
    let response = app
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(mock.seen().await.len(), 4);
    task.abort();
}

#[tokio::test]
async fn invalid_request_400_passes_through_without_retry() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
        vec![Spec::json(
            400,
            r#"{"type":"error","error":{"type":"invalid_request_error","message":"invalid arguments"}}"#,
        )],
    )
    .await;
    let response = app_for(test_config(base, 1))
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_str(&response_text(response).await).unwrap();
    assert_eq!(body["error"]["type"], "invalid_request_error");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("invalid arguments")
    );
    assert_eq!(mock.seen().await.len(), 1);
    task.abort();
}

#[tokio::test]
async fn model_not_found_404_passes_through() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
        vec![Spec::json(
            404,
            r#"{"type":"error","error":{"type":"not_found_error","message":"model is not found"}}"#,
        )],
    )
    .await;
    let response = app_for(test_config(base, 1))
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body: Value = serde_json::from_str(&response_text(response).await).unwrap();
    assert_eq!(body["error"]["type"], "not_found_error");
    assert_eq!(mock.seen().await.len(), 1);
    task.abort();
}

#[tokio::test]
async fn upstream_malformed_json_is_retried_then_502() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
        vec![
            Spec::json(200, "this is not json"),
            Spec::json(200, "still not"),
        ],
    )
    .await;
    let response = app_for(test_config(base, 1))
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(mock.seen().await.len(), 2);
    task.abort();
}

#[tokio::test]
async fn secrets_are_redacted_from_upstream_error_bodies() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
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

#[tokio::test]
async fn empty_stream_body_fails_fast_instead_of_hanging() {
    let (base, mock, task) = start_mock().await;
    // 200 with an empty body: EOF before any byte must be treated as a
    // protocol failure (retried, then 502), never as a busy wait.
    mock.set(
        "sensenova-key-1",
        vec![
            Spec::json(200, "").content_type_sse(),
            Spec::json(200, "").content_type_sse(),
        ],
    )
    .await;
    let mut config = test_config(base, 1);
    config.upstream.first_byte_timeout_secs = 2;
    let response = app_for(config)
        .oneshot(gateway_request("/v1/messages", anthropic_body(true)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(mock.seen().await.len(), 2);
    task.abort();
}

#[tokio::test]
async fn quota_cooldown_is_capped_by_configuration() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
        vec![Spec::json(
            429,
            r#"{"error":{"message":"free quota exhausted. Try again in 10d"}}"#,
        )],
    )
    .await;
    let mut config = test_config(base, 1);
    config.circuit.max_quota_cooldown_secs = 120;
    let response = app_for(config)
        .oneshot(gateway_request("/v1/messages", anthropic_body(false)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    // The 10-day human hint is capped to the configured 120 s.
    assert_eq!(response.headers()[header::RETRY_AFTER], "120");
    task.abort();
}

// ---------------------------------------------------------------------------
// Tests: request validation, local endpoints, auth
// ---------------------------------------------------------------------------

#[tokio::test]
async fn malformed_client_json_is_400_without_upstream_call() {
    let (base, mock, task) = start_mock().await;
    let response = app_for(test_config(base, 1))
        .oneshot(gateway_request("/v1/messages", "{\"model\":"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(mock.seen().await.is_empty());
    task.abort();
}

#[tokio::test]
async fn missing_model_is_400_without_upstream_call() {
    let (base, mock, task) = start_mock().await;
    let response = app_for(test_config(base, 1))
        .oneshot(gateway_request(
            "/v1/messages",
            json!({"messages": []}).to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(mock.seen().await.is_empty());
    task.abort();
}

#[tokio::test]
async fn auth_required_for_all_api_routes() {
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
        let text = response_text(response).await;
        assert!(text.contains("authentication_error"), "{path}");
    }
    // Wrong key is rejected too.
    let request = Request::builder()
        .method("GET")
        .uri("/v1/models")
        .header(header::AUTHORIZATION, "Bearer wrong")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(mock.seen().await.is_empty());
    task.abort();
}

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
    let models = app
        .clone()
        .oneshot(gateway_request("/v1/models", Body::empty()))
        .await
        .unwrap();
    let models_text = response_text(models).await;
    assert!(models_text.contains("claude-sensenova"));
    assert!(models_text.contains("sensenova-6.8-flash-lite"));

    // Anthropic-style model list when the client speaks Anthropic.
    let request = Request::builder()
        .method("GET")
        .uri("/v1/models")
        .header(header::AUTHORIZATION, "Bearer gateway-secret")
        .header("anthropic-version", "2023-06-01")
        .body(Body::empty())
        .unwrap();
    let models = app.clone().oneshot(request).await.unwrap();
    let anthropic_models: Value = serde_json::from_str(&response_text(models).await).unwrap();
    assert_eq!(anthropic_models["data"][0]["type"], "model");

    let count = app
        .clone()
        .oneshot(gateway_request(
            "/v1/messages/count_tokens",
            json!({"model":"claude-sensenova","messages":[{"role":"user","content":"hello world"}]})
                .to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(
        count.headers()["x-sensenova-proxy-token-count"],
        "approximate"
    );
    let count_value: Value = serde_json::from_str(&response_text(count).await).unwrap();
    assert!(count_value["input_tokens"].as_u64().unwrap() > 0);
    assert!(mock.seen().await.is_empty());
    task.abort();
}

#[tokio::test]
async fn request_id_is_echoed() {
    let (base, mock, task) = start_mock().await;
    mock.set("sensenova-key-1", vec![Spec::json(200, ok_message_json())])
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

#[tokio::test]
async fn metrics_expose_upstream_counters() {
    let (base, mock, task) = start_mock().await;
    mock.set("sensenova-key-1", vec![Spec::json(200, ok_message_json())])
        .await;
    let state = AppState::new(test_config(base, 1)).unwrap();
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
    assert!(text.contains("sensenova_proxy_requests_total 1"));
    assert!(text.contains("sensenova_proxy_upstream_requests_total 1"));
    let _ = state;
    task.abort();
}

// ---------------------------------------------------------------------------
// Tests: concurrency shaping
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_limit_and_queue_bounds_are_respected() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_normal_requests_share_the_pool_safely() {
    let (base, mock, task) = start_mock().await;
    mock.set(
        "sensenova-key-1",
        (0..8).map(|_| Spec::json(200, ok_message_json())).collect(),
    )
    .await;
    let app = app_for(test_config(base, 1));
    let responses = futures_util::future::join_all((0..8).map(|_| {
        let app = app.clone();
        async move {
            app.oneshot(gateway_request("/v1/messages", anthropic_body(false)))
                .await
        }
    }))
    .await;
    assert!(
        responses
            .into_iter()
            .all(|response| response.unwrap().status() == StatusCode::OK)
    );
    task.abort();
}
