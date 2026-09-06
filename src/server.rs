//! Axum routes, authentication, protocol dispatch, and graceful shutdown.
//!
//! Client-facing surface (all Anthropic-compatible):
//! - `POST /v1/messages` — native passthrough to SenseNova's Anthropic
//!   endpoint with model aliasing, bounded retry, and stream validation.
//! - `POST /v1/messages/count_tokens` — local conservative estimate.
//! - `GET /v1/models` — deterministic local catalog.
//! - `GET /healthz`, `GET /readyz`, `GET /metrics`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::Router;
use axum::body::Body;
use axum::extract::rejection::BytesRejection;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::{Value, json};

use crate::auth;
use crate::concurrency::{AdmissionController, AdmissionFailure, Permit};
use crate::error::{anthropic_error, error_type_for_status, insert_request_id, json_response};
use crate::metrics::Metrics;
use crate::models;
use crate::pool::SelectedKey;
use crate::sse::{AnthropicEventKind, SseDecoder, classify_event};
use crate::upstream::{Core, GatewayError, UpstreamOutcome, circuit_state_code};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<crate::config::Config>,
    pub core: Core,
    pub admission: Arc<AdmissionController>,
    pub metrics: Arc<Metrics>,
}

impl AppState {
    pub fn new(config: crate::config::Config) -> Result<Self> {
        config.validate()?;
        let admission = Arc::new(AdmissionController::new(
            config.concurrency.initial,
            config.concurrency.queue_capacity,
        ));
        let state = Self {
            core: Core::new(&config)?,
            admission,
            metrics: Arc::new(Metrics::new()),
            config: Arc::new(config),
        };
        state
            .metrics
            .set_concurrency_limit(state.config.concurrency.initial);
        Ok(state)
    }
}

pub fn router(state: AppState) -> Router {
    let max_request_bytes = state.config.runtime.max_request_bytes;
    let protected = Router::new()
        .route("/v1/messages", post(anthropic_messages))
        .route("/v1/messages/count_tokens", post(anthropic_count_tokens))
        .route("/v1/models", get(models_list))
        .layer(DefaultBodyLimit::max(max_request_bytes))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));
    // Health endpoints stay open on loopback binds; when bound non-loopback,
    // readiness and metrics require the gateway key too.
    let health = if state.config.binds_loopback() {
        Router::new()
            .route("/healthz", get(healthz))
            .route("/readyz", get(readyz))
            .route("/metrics", get(metrics_endpoint))
    } else {
        Router::new()
            .route("/healthz", get(healthz))
            .route("/readyz", get(readyz_protected))
            .route("/metrics", get(metrics_protected))
    };
    Router::new()
        .merge(health)
        .merge(protected)
        .layer(middleware::from_fn(response_log_middleware))
        .layer(middleware::from_fn(request_id_middleware))
        .with_state(state)
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok\n")
}

async fn readyz(State(state): State<AppState>) -> Response {
    readyz_impl(state)
}

async fn readyz_protected(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !auth::gateway_key_is_valid(&headers, &state.config.server.api_key) {
        return unauthorized(headers);
    }
    readyz_impl(state)
}

fn readyz_impl(state: AppState) -> Response {
    let usable = state.core.pool.usable_count();
    let circuit = state.core.circuit.state();
    state
        .metrics
        .set_circuit_state(circuit_code(&state, circuit));
    let ready = usable > 0;
    let body = json!({
        "status": if ready { "ready" } else { "not_ready" },
        "usable_credentials": usable,
        "circuit_state": circuit.as_str(),
        "circuit_open_remaining_secs": state.core.circuit.open_remaining()
            .map(|(remaining, _)| remaining.as_secs()),
        "concurrency_limit": state.admission.limit(),
        "queued_requests": state.admission.waiting(),
    });
    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        [(header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}

async fn metrics_endpoint(State(state): State<AppState>) -> Response {
    metrics_impl(state)
}

async fn metrics_protected(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !auth::gateway_key_is_valid(&headers, &state.config.server.api_key) {
        return unauthorized(headers);
    }
    metrics_impl(state)
}

fn metrics_impl(state: AppState) -> Response {
    state
        .metrics
        .set_circuit_state(circuit_code(&state, state.core.circuit.state()));
    state.metrics.set_queue_depth(state.admission.waiting());
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.metrics.render(),
    )
        .into_response()
}

async fn models_list(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let request_id = request_id(&headers);
    json_response(
        StatusCode::OK,
        models::models_response(&state.config, &headers),
        &request_id,
    )
}

async fn anthropic_count_tokens(
    State(_state): State<AppState>,
    headers: HeaderMap,
    body: std::result::Result<Bytes, BytesRejection>,
) -> Response {
    let request_id = request_id(&headers);
    let body = match body {
        Ok(body) => body,
        Err(_) => {
            return anthropic_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request_too_large",
                "request body exceeds the configured limit",
                &request_id,
            );
        }
    };
    match models::approximate_input_tokens(&body) {
        Ok(count) => {
            let mut response =
                json_response(StatusCode::OK, json!({"input_tokens": count}), &request_id);
            models::insert_approximate_header(response.headers_mut());
            response
        }
        Err(error) => anthropic_error(
            StatusCode::BAD_REQUEST,
            error.error_type,
            error.message,
            &request_id,
        ),
    }
}

async fn anthropic_messages(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: std::result::Result<Bytes, BytesRejection>,
) -> Response {
    let request_id = request_id(&headers);
    let raw_body = match body {
        Ok(body) => body,
        Err(_) => {
            return anthropic_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request_too_large",
                "request body exceeds the configured limit",
                &request_id,
            );
        }
    };

    let mut value: Value = match serde_json::from_slice(&raw_body) {
        Ok(value) => value,
        Err(error) => {
            return anthropic_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!("invalid JSON: {error}"),
                &request_id,
            );
        }
    };
    let Some((client_model, stream)) = models::request_summary(&value) else {
        return anthropic_error(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "model must be a non-empty string and messages must be an array",
            &request_id,
        );
    };
    let upstream_model = models::resolve_model(&state.config, &client_model);
    if let Err(error) = models::normalize_messages_request(&mut value, &upstream_model) {
        return anthropic_error(
            StatusCode::BAD_REQUEST,
            error.error_type,
            error.message,
            &request_id,
        );
    }
    let upstream_body = match serde_json::to_vec(&value) {
        Ok(body) => Bytes::from(body),
        Err(_) => {
            return anthropic_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "request could not be serialized",
                &request_id,
            );
        }
    };
    let session_tag = session_tag(&headers);
    // Serving-pressure observability only (never used to reject): large
    // input plus a large output budget is what trips upstream TPM limits.
    let approx_input_tokens = models::approximate_input_tokens(&upstream_body).unwrap_or(0);
    let requested_max_tokens = value.get("max_tokens").and_then(Value::as_u64);

    // Bounded local admission: no unbounded queue exists.
    let queue_timeout = Duration::from_secs(state.config.concurrency.queue_timeout_secs);
    let permit = match state.admission.acquire(queue_timeout).await {
        Ok(permit) => permit,
        Err(failure) => {
            state
                .metrics
                .queue_rejections_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let (message, kind) = match failure {
                AdmissionFailure::QueueFull => (
                    "local concurrency queue is full; retry shortly",
                    "queue_full",
                ),
                AdmissionFailure::QueueTimeout => (
                    "timed out waiting for a local concurrency slot",
                    "queue_timeout",
                ),
            };
            tracing::warn!(
                request_id,
                client_model,
                session_tag,
                failure = kind,
                "{message}"
            );
            let mut response = anthropic_error(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limit_error",
                message,
                &request_id,
            );
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("2"));
            return response;
        }
    };
    state.metrics.inc_requests();
    state.metrics.note_queue_wait(permit.queue_wait);
    state.metrics.set_queue_depth(state.admission.waiting());
    state
        .metrics
        .set_circuit_state(circuit_state_code(state.core.circuit.state()));
    let mut scope = RequestScope::new(state.metrics.clone());

    tracing::info!(
        request_id,
        protocol = "anthropic",
        client_model,
        upstream_model,
        session_tag,
        stream,
        request_bytes = upstream_body.len(),
        approx_input_tokens,
        requested_max_tokens,
        queue_wait_ms = permit.queue_wait.as_millis() as u64,
        "client request accepted"
    );

    let result = state
        .core
        .send_messages(
            &state.metrics,
            upstream_body,
            stream,
            &request_id,
            &client_model,
            &upstream_model,
            &session_tag,
        )
        .await;

    match result {
        Ok(UpstreamOutcome::Json { body, key, attempt }) => {
            let tool_calls = serde_json::from_slice::<Value>(&body)
                .map(|value| models::count_tool_use_blocks(&value))
                .unwrap_or(0);
            scope.finish_json(&key, attempt, tool_calls);
            let mut response = (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                body,
            )
                .into_response();
            insert_request_id(response.headers_mut(), &request_id);
            response
        }
        Ok(UpstreamOutcome::Stream {
            first_chunk,
            rest,
            key,
            attempt,
        }) => {
            tracing::info!(
                request_id,
                credential = %key.name,
                attempt,
                "stream committed to client; replay disabled from here on"
            );
            let mut response = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/event-stream")
                .header(header::CACHE_CONTROL, "no-cache")
                .header("x-accel-buffering", "no")
                .body(stream_body(StreamPump {
                    first_chunk: Some(first_chunk),
                    rest: Some(rest),
                    decoder: SseDecoder::default(),
                    request_id: request_id.clone(),
                    credential: key.name.to_string(),
                    session_tag,
                    ping_interval: ping_interval(state.config.runtime.stream_ping_secs),
                    permit,
                    scope,
                    state: state.clone(),
                    clean_stop: false,
                    tool_calls: 0,
                    committed: false,
                }))
                .unwrap_or_else(|_| Response::new(Body::empty()));
            insert_request_id(response.headers_mut(), &request_id);
            response
        }
        Err(error) => {
            scope.finish_error(error.class.as_str());
            gateway_error_response(&state, error, &request_id)
        }
    }
}

fn circuit_code(state: &AppState, state_enum: crate::circuit::CircuitState) -> i64 {
    let _ = state;
    circuit_state_code(state_enum)
}

fn gateway_error_response(state: &AppState, error: GatewayError, request_id: &str) -> Response {
    let status = error.status;
    let error_type = error_type_for_status(status);
    let mut response = anthropic_error(status, error_type, error.message, request_id);
    if let Some(duration) = error.retry_after {
        let seconds = duration
            .as_secs()
            .max(u64::from(duration.subsec_nanos() > 0));
        if let Ok(value) = HeaderValue::try_from(seconds.to_string()) {
            response.headers_mut().insert(header::RETRY_AFTER, value);
        }
    }
    if let Some(sanitized) = error.sanitized_body {
        tracing::debug!(
            request_id,
            upstream_error = %sanitized,
            "sanitized upstream error retained for diagnostics"
        );
    }
    let _ = state;
    response
}

// ---------------------------------------------------------------------------
// Stream pump: verbatim passthrough with parse-side validation.
// ---------------------------------------------------------------------------

struct StreamPump {
    first_chunk: Option<Bytes>,
    rest: Option<reqwest::Response>,
    decoder: SseDecoder,
    request_id: String,
    credential: String,
    session_tag: String,
    ping_interval: Option<Duration>,
    permit: Permit,
    scope: RequestScope,
    state: AppState,
    clean_stop: bool,
    tool_calls: usize,
    committed: bool,
}

#[derive(Default)]
struct EventOutcome {
    terminate: bool,
    error_frame: Option<Bytes>,
}

impl EventOutcome {
    fn continue_stream() -> Self {
        Self::default()
    }

    fn terminate_with(frame: Bytes) -> Self {
        Self {
            terminate: true,
            error_frame: Some(frame),
        }
    }
}

fn stream_body(pump: StreamPump) -> Body {
    let output = async_stream::stream! {
        let mut pump = pump;
        let rest = pump.rest.take().expect("stream body carries its upstream response");
        let mut upstream = rest.bytes_stream();
        let mut ping =
            tokio::time::interval(pump.ping_interval.unwrap_or(Duration::from_secs(3600)));
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ping.reset();
        // First chunk: commit barrier. Everything before this yield could
        // still have been retried inside send_messages; from the first yield
        // on, this stream is never replayed.
        if let Some(chunk) = pump.first_chunk.take() {
            // The first chunk feeds the validator too, so a stream that
            // starts and ends within it is still recognized.
            let events = match pump.decoder.push(&chunk) {
                Ok(events) => events,
                Err(()) => {
                    pump.state
                        .metrics
                        .stream_protocol_errors_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    pump.scope.finish_error("protocol_error");
                    pump.committed = true;
                    yield Ok::<Bytes, std::io::Error>(chunk);
                    yield Ok(sse_error_frame(
                        "upstream SSE event exceeded the gateway limit",
                    ));
                    return;
                }
            };
            pump.committed = true;
            yield Ok(chunk);
            let outcome = pump.process_events(events);
            if let Some(frame) = outcome.error_frame {
                yield Ok(frame);
            }
            if outcome.terminate {
                return;
            }
            if pump.clean_stop {
                let credential = pump.credential.clone();
                pump.finish_complete(&credential);
                return;
            }
        }
        loop {
            tokio::select! {
                biased;
                chunk = upstream.next() => match chunk {
                    Some(Ok(chunk)) => {
                        if chunk.is_empty() {
                            continue;
                        }
                        let events = match pump.decoder.push(&chunk) {
                            Ok(events) => events,
                            Err(()) => {
                                pump.state.metrics.stream_protocol_errors_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                pump.scope.finish_error("protocol_error");
                                yield Ok(sse_error_frame("upstream SSE event exceeded the gateway limit"));
                                return;
                            }
                        };
                        pump.committed = true;
                        yield Ok(chunk);
                        let outcome = pump.process_events(events);
                        if let Some(frame) = outcome.error_frame {
                            yield Ok(frame);
                        }
                        if outcome.terminate {
                            return;
                        }
                        if pump.clean_stop {
                            let credential = pump.credential.clone();
                            pump.finish_complete(&credential);
                            return;
                        }
                    }
                    Some(Err(error)) => {
                        pump.state.metrics.stream_interruptions_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tracing::warn!(
                            request_id = %pump.request_id,
                            session_tag = %pump.session_tag,
                            credential = %pump.credential,
                            error_class = if error.is_timeout() { "timeout" } else { "stream_transport" },
                            committed_to_client = pump.committed,
                            "upstream stream interrupted; request will not be replayed"
                        );
                        pump.scope.finish_error("stream_interrupted");
                        yield Ok(sse_error_frame("upstream stream was interrupted"));
                        return;
                    }
                    None => {
                        let trailing = pump.decoder.finish().unwrap_or_default();
                        let outcome = pump.process_events(trailing);
                        if let Some(frame) = outcome.error_frame {
                            yield Ok(frame);
                        }
                        if outcome.terminate {
                            return;
                        }
                        if pump.clean_stop {
                            let credential = pump.credential.clone();
                            pump.finish_complete(&credential);
                        } else {
                            pump.state.metrics.stream_interruptions_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            tracing::warn!(
                                request_id = %pump.request_id,
                                credential = %pump.credential,
                                committed_to_client = pump.committed,
                                "upstream stream ended before message_stop; not replaying"
                            );
                            pump.scope.finish_error("unexpected_eof");
                            yield Ok(sse_error_frame("upstream stream ended before message_stop"));
                        }
                        return;
                    }
                },
                _ = ping.tick(), if pump.ping_interval.is_some() && !pump.clean_stop => {
                    // Protocol-legal keepalive so long upstream silences do
                    // not look like dead connections.
                    yield Ok(Bytes::from_static(b"event: ping\ndata: {\"type\":\"ping\"}\n\n"));
                }
            }
        }
    };
    Body::from_stream(output)
}

impl StreamPump {
    fn finish_complete(&mut self, credential: &str) {
        tracing::info!(
            request_id = %self.request_id,
            session_tag = %self.session_tag,
            queue_wait_ms = self.permit.queue_wait.as_millis() as u64,
            tool_calls = self.tool_calls,
            "stream reached message_stop"
        );
        self.scope.finish_stream(
            &self.state,
            credential,
            self.tool_calls,
            "complete",
            self.committed,
        );
    }

    /// Process parse-side events. The raw chunk is forwarded before this runs,
    /// so any error frame here follows the offending bytes downstream.
    fn process_events(&mut self, events: Vec<crate::sse::SseEvent>) -> EventOutcome {
        for event in events {
            if event.event.as_deref() == Some("ping") {
                continue;
            }
            if event.data.trim() == "[DONE]" {
                // OpenAI-style terminator; harmless for Anthropic streams.
                continue;
            }
            if serde_json::from_str::<Value>(&event.data).is_err() {
                self.state
                    .metrics
                    .stream_protocol_errors_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(
                    request_id = %self.request_id,
                    credential = %self.credential,
                    committed_to_client = self.committed,
                    "upstream sent invalid SSE JSON; terminating the stream"
                );
                self.scope.finish_error("protocol_error");
                return EventOutcome::terminate_with(sse_error_frame(
                    "upstream sent invalid SSE data",
                ));
            }
            match classify_event(&event) {
                AnthropicEventKind::ContentBlockStart { block_type } => {
                    if block_type == "tool_use" {
                        self.tool_calls = self.tool_calls.saturating_add(1);
                    }
                }
                AnthropicEventKind::MessageStop => {
                    self.clean_stop = true;
                }
                AnthropicEventKind::Error => {
                    tracing::warn!(
                        request_id = %self.request_id,
                        credential = %self.credential,
                        "upstream emitted an error event; forwarding verbatim and terminating"
                    );
                    self.scope.finish_error("upstream_error_event");
                    // The upstream error frame itself was already forwarded
                    // verbatim inside its chunk; stop pumping.
                    return EventOutcome {
                        terminate: true,
                        error_frame: None,
                    };
                }
                _ => {}
            }
        }
        EventOutcome::continue_stream()
    }
}

fn sse_error_frame(message: &str) -> Bytes {
    Bytes::from(format!(
        "event: error\ndata: {}\n\n",
        json!({"type":"error", "error":{"type":"api_error", "message":message}})
    ))
}

fn ping_interval(stream_ping_secs: u64) -> Option<Duration> {
    if stream_ping_secs == 0 {
        None
    } else {
        Some(Duration::from_secs(stream_ping_secs.max(1)))
    }
}

// ---------------------------------------------------------------------------
// Request scope: metrics and completion logging for one accepted request.
// ---------------------------------------------------------------------------

struct RequestScope {
    metrics: Arc<Metrics>,
    started: Instant,
    finished: bool,
}

impl RequestScope {
    fn new(metrics: Arc<Metrics>) -> Self {
        Self {
            metrics,
            started: Instant::now(),
            finished: false,
        }
    }

    fn finish_json(&mut self, key: &SelectedKey, attempt: usize, tool_calls: usize) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.metrics.dec_requests();
        self.metrics.note_request_duration(self.started.elapsed());
        self.metrics
            .tool_calls_total
            .fetch_add(tool_calls as u64, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(
            credential = %key.name,
            attempt,
            duration_ms = self.started.elapsed().as_millis() as u64,
            tool_calls,
            "JSON exchange complete"
        );
    }

    fn finish_error(&mut self, class: &'static str) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.metrics.dec_requests();
        self.metrics.note_request_duration(self.started.elapsed());
        self.metrics.note_failure(class);
    }

    fn finish_stream(
        &mut self,
        state: &AppState,
        credential: &str,
        tool_calls: usize,
        outcome: &'static str,
        committed: bool,
    ) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.metrics.dec_requests();
        self.metrics.note_request_duration(self.started.elapsed());
        self.metrics
            .tool_calls_total
            .fetch_add(tool_calls as u64, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(
            credential = %credential,
            duration_ms = self.started.elapsed().as_millis() as u64,
            tool_calls,
            outcome,
            committed_to_client = committed,
            concurrency_limit = state.admission.limit(),
            "stream closed"
        );
    }
}

impl Drop for RequestScope {
    fn drop(&mut self) {
        // Covers client-disconnect cancellation of the pump generator.
        if !self.finished {
            self.finished = true;
            self.metrics.dec_requests();
            self.metrics.note_request_duration(self.started.elapsed());
            self.metrics.note_failure("client_disconnected");
            tracing::warn!(
                duration_ms = self.started.elapsed().as_millis() as u64,
                "request ended without completion (client disconnected)"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

async fn auth_middleware(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if auth::gateway_key_is_valid(request.headers(), &state.config.server.api_key) {
        next.run(request).await
    } else {
        unauthorized(request.headers().clone())
    }
}

fn unauthorized(headers: HeaderMap) -> Response {
    let request_id = request_id(&headers);
    anthropic_error(
        StatusCode::UNAUTHORIZED,
        "authentication_error",
        "invalid gateway API key",
        &request_id,
    )
}

async fn request_id_middleware(mut request: Request<Body>, next: Next) -> Response {
    let id = request
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 128
                && value.chars().all(|character| character.is_ascii_graphic())
        })
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("req_{}", uuid::Uuid::new_v4().simple()));
    if let Ok(value) = HeaderValue::try_from(id.as_str()) {
        request.headers_mut().insert("x-request-id", value);
    }
    let mut response = next.run(request).await;
    insert_request_id(response.headers_mut(), &id);
    response
}

async fn response_log_middleware(request: Request<Body>, next: Next) -> Response {
    let started = Instant::now();
    let request_id = request_id(request.headers());
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let session_tag = session_tag(request.headers());
    let response = next.run(request).await;
    let streaming = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("text/event-stream"));
    tracing::info!(
        request_id,
        session_tag,
        method = %method,
        path,
        status = response.status().as_u16(),
        duration_ms = started.elapsed().as_millis() as u64,
        streaming,
        "client response ready"
    );
    response
}

fn request_id(headers: &HeaderMap) -> String {
    headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("req_unknown")
        .to_owned()
}

/// A short non-reversible tag derived from Claude Code session headers so
/// logs can correlate turns without storing session identifiers verbatim.
fn session_tag(headers: &HeaderMap) -> String {
    for name in [
        "x-claude-code-session-id",
        "x-claude-code-agent-id",
        "x-claude-code-parent-agent-id",
    ] {
        if let Some(value) = headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.is_empty())
        {
            return format!("sess_{:016x}", fnv1a64(value.as_bytes()));
        }
    }
    "sess_none".into()
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

// ---------------------------------------------------------------------------
// Server lifecycle
// ---------------------------------------------------------------------------

pub async fn serve(state: AppState) -> Result<()> {
    let bind = state.config.server.bind.clone();
    let shutdown_timeout = Duration::from_secs(state.config.runtime.shutdown_timeout_secs);
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .with_context(|| format!("binding gateway to {bind}"))?;
    tracing::info!(bind, "sensenova-proxy listening");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let app = router(state);
    let mut task = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.await;
        })
        .await
    });
    tokio::select! {
        result = &mut task => {
            result.context("gateway task failed")?.context("serving HTTP")?;
        }
        signal = shutdown_signal() => {
            match &signal {
                Ok(()) => tracing::info!(
                    timeout_secs = shutdown_timeout.as_secs(),
                    "shutdown signal received; draining requests"
                ),
                Err(error) => tracing::warn!(
                    error = %error,
                    timeout_secs = shutdown_timeout.as_secs(),
                    "shutdown signal handler failed; stopping the gateway"
                ),
            }
            let _ = shutdown_tx.send(());
            match tokio::time::timeout(shutdown_timeout, &mut task).await {
                Ok(result) => {
                    result.context("gateway task failed during shutdown")?
                        .context("serving HTTP during shutdown")?;
                }
                Err(_) => {
                    tracing::warn!("graceful shutdown timed out; aborting remaining connections");
                    task.abort();
                    let _ = task.await;
                }
            }
            signal?;
        }
    }
    Ok(())
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .context("installing SIGTERM handler")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("installing SIGINT handler")?,
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c()
        .await
        .context("installing interrupt handler")?;
    Ok(())
}

#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;
