//! SenseNova upstream transport: one long-lived reqwest client, bounded body
//! reads, error classification, circuit-breaker integration, and a small
//! retry loop that is only ever reachable *before* any downstream commit.
//!
//! Commit-barrier invariant: `send_messages` resolves only after the outcome
//! is fully determined (JSON body buffered, or the first SSE chunk of a
//! healthy stream is in hand). Retries and failover happen exclusively inside
//! this function; the caller streams what it receives without any further
//! replay path.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use axum::http::{HeaderName, HeaderValue, StatusCode, header};
use bytes::Bytes;
use serde_json::Value;

use crate::circuit::{Admission, CircuitBreaker, CircuitState};
use crate::config::{Config, defaults};
use crate::error::error_type_for_status;
use crate::metrics::Metrics;
use crate::pool::{KeyPool, SelectedKey};
use crate::rate_limit::{UpstreamErrorClass, classify_upstream_error, transport_error_class};

#[derive(Clone)]
pub struct Core {
    pub http: reqwest::Client,
    pub messages_url: reqwest::Url,
    pub pool: KeyPool,
    pub circuit: Arc<CircuitBreaker>,
    pub secrets: Arc<Vec<String>>,
    retry: crate::config::RetryConfig,
    anthropic_version: HeaderValue,
    first_byte_timeout: Duration,
    max_quota_cooldown: Duration,
}

#[derive(Debug)]
pub enum UpstreamOutcome {
    /// Non-stream exchange completed; the body was read, bounded, and parsed.
    Json {
        body: Bytes,
        key: SelectedKey,
        attempt: usize,
    },
    /// Stream established; the first non-empty chunk is already buffered so
    /// every retry decision predates the first downstream byte.
    Stream {
        first_chunk: Bytes,
        rest: reqwest::Response,
        key: SelectedKey,
        attempt: usize,
    },
}

/// A fully classified failure that the server layer renders for the client.
#[derive(Debug)]
pub struct GatewayError {
    pub status: StatusCode,
    pub message: String,
    pub class: UpstreamErrorClass,
    pub retry_after: Option<Duration>,
    pub sanitized_body: Option<Value>,
}

impl Core {
    pub fn new(config: &Config) -> Result<Self> {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(config.upstream.connect_timeout_secs))
            // Inactivity timeout: a healthy SSE response may live longer than
            // this as long as chunks keep arriving.
            .read_timeout(Duration::from_secs(config.upstream.timeout_secs))
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(60))
            .http2_keep_alive_interval(Duration::from_secs(30))
            .http2_keep_alive_timeout(Duration::from_secs(20))
            .http2_keep_alive_while_idle(true)
            .gzip(true)
            .build()
            .context("building shared SenseNova HTTP client")?;
        let mut secrets: Vec<String> = config
            .sensenova_api_keys
            .iter()
            .map(|key| key.api_key.clone())
            .collect();
        secrets.push(config.server.api_key.clone());
        let anthropic_version = HeaderValue::from_str(&config.upstream.anthropic_version)
            .context("upstream.anthropic_version must be HTTP-header-safe")?;
        Ok(Self {
            http,
            messages_url: config.messages_url()?,
            pool: KeyPool::new(&config.sensenova_api_keys),
            circuit: Arc::new(CircuitBreaker::new(
                config.circuit.overload_threshold,
                config.circuit.overload_window_secs,
                config.circuit.overload_open_secs,
            )),
            secrets: Arc::new(secrets),
            retry: config.retry.clone(),
            anthropic_version,
            first_byte_timeout: Duration::from_secs(config.upstream.first_byte_timeout_secs),
            max_quota_cooldown: Duration::from_secs(config.circuit.max_quota_cooldown_secs),
        })
    }

    /// Send one logical `/v1/messages` request with bounded failover.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_messages(
        &self,
        metrics: &Metrics,
        body: Bytes,
        stream: bool,
        request_id: &str,
        client_model: &str,
        upstream_model: &str,
        session_tag: &str,
    ) -> std::result::Result<UpstreamOutcome, GatewayError> {
        let mut attempted: HashSet<usize> = HashSet::with_capacity(self.pool.len());
        let mut attempt = 0usize;
        loop {
            // Circuit check before every attempt.
            match self.circuit.admit() {
                Admission::Refused { remaining, reason } => {
                    tracing::warn!(
                        request_id,
                        circuit_reason = reason,
                        cooldown_ms = remaining.as_millis() as u64,
                        "circuit open; refusing upstream call without dialing SenseNova"
                    );
                    return Err(GatewayError {
                        status: StatusCode::TOO_MANY_REQUESTS,
                        message: format!(
                            "upstream temporarily unavailable (circuit open: {reason})"
                        ),
                        class: UpstreamErrorClass::RateLimited,
                        retry_after: Some(remaining),
                        sanitized_body: None,
                    });
                }
                Admission::Allowed => {}
            }

            let Some(selected) = self.pool.select(&attempted) else {
                for snapshot in self.pool.snapshots() {
                    tracing::debug!(
                        request_id,
                        credential = %snapshot.name,
                        quota_group = %snapshot.quota_group,
                        cooling_remaining_ms = snapshot
                            .cooling_remaining
                            .map(|duration| duration.as_millis() as u64)
                            .unwrap_or(0),
                        unusable = snapshot.unusable,
                        "credential state at exhaustion"
                    );
                }
                tracing::warn!(
                    request_id,
                    configured_keys = self.pool.len(),
                    "all credentials are cooling or unusable"
                );
                // No upstream call happened, but a HalfOpen probe slot
                // (if this request held one) must be released.
                self.circuit.record_neutral_failure();
                return Err(GatewayError {
                    status: StatusCode::TOO_MANY_REQUESTS,
                    message: "all SenseNova API keys are currently rate-limited or unavailable"
                        .into(),
                    class: UpstreamErrorClass::RateLimited,
                    retry_after: self.pool.earliest_retry_after(),
                    sanitized_body: None,
                });
            };
            attempted.insert(selected.index);
            attempt += 1;
            metrics
                .upstream_requests_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let started = Instant::now();

            let response = match self
                .send_once(&selected, body.clone(), stream, request_id, upstream_model)
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    let class = if error.is_timeout() {
                        UpstreamErrorClass::QueueTimeout
                    } else {
                        UpstreamErrorClass::TransportTransient
                    };
                    metrics
                        .upstream_transport_errors_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    self.circuit.record_overload();
                    if self.may_retry(&class, attempt) {
                        metrics
                            .retries_total
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if self.pool.select(&attempted).is_some() {
                            tracing::warn!(
                                request_id,
                                credential = %selected.name,
                                attempt,
                                error_class = transport_error_class(&error),
                                "upstream transport failure before commit; failing over"
                            );
                            continue;
                        }
                        tracing::warn!(
                            request_id,
                            credential = %selected.name,
                            attempt,
                            error_class = transport_error_class(&error),
                            "upstream transport failure before commit; retrying the same key"
                        );
                        attempted.remove(&selected.index);
                        sleep_backoff(&self.retry, attempt).await;
                        continue;
                    }
                    tracing::error!(
                        request_id,
                        credential = %selected.name,
                        attempt,
                        error_class = transport_error_class(&error),
                        "upstream transport failure; not retrying"
                    );
                    return Err(GatewayError {
                        status: StatusCode::BAD_GATEWAY,
                        message: "could not reach the SenseNova upstream".into(),
                        class,
                        retry_after: None,
                        sanitized_body: None,
                    });
                }
            };

            let status = response.status();
            if status.is_success() {
                if stream {
                    // Pre-commit: buffer the first non-empty chunk.
                    let mut rest = response;
                    let first = tokio::time::timeout(self.first_byte_timeout, async {
                        loop {
                            match rest.chunk().await {
                                Ok(Some(chunk)) if !chunk.is_empty() => {
                                    break Ok::<Bytes, FirstByteFailure>(chunk);
                                }
                                // Empty chunks are legal keepalives; EOF
                                // before any byte is a protocol failure.
                                Ok(Some(_)) => continue,
                                Ok(None) => break Err(FirstByteFailure::Eof),
                                Err(error) => break Err(FirstByteFailure::Transport(error)),
                            }
                        }
                    })
                    .await;
                    match first {
                        Ok(Ok(chunk)) => {
                            metrics.note_time_to_first_event(started.elapsed());
                            tracing::info!(
                                request_id,
                                client_model,
                                upstream_model,
                                session_tag,
                                credential = %selected.name,
                                attempt,
                                first_byte_ms = started.elapsed().as_millis() as u64,
                                "SenseNova stream established"
                            );
                            self.circuit.record_success();
                            return Ok(UpstreamOutcome::Stream {
                                first_chunk: chunk,
                                rest,
                                key: selected,
                                attempt,
                            });
                        }
                        Ok(Err(failure)) => {
                            metrics
                                .upstream_transport_errors_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            self.circuit.record_overload();
                            let class = match failure {
                                FirstByteFailure::Eof => UpstreamErrorClass::TransportTransient,
                                FirstByteFailure::Transport(ref error) if error.is_timeout() => {
                                    UpstreamErrorClass::QueueTimeout
                                }
                                FirstByteFailure::Transport(_) => {
                                    UpstreamErrorClass::TransportTransient
                                }
                            };
                            if self.may_retry(&class, attempt) {
                                metrics
                                    .retries_total
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                if self.pool.select(&attempted).is_some() {
                                    tracing::warn!(
                                        request_id,
                                        credential = %selected.name,
                                        attempt,
                                        "stream failed before first byte; failing over"
                                    );
                                    continue;
                                }
                                tracing::warn!(
                                    request_id,
                                    credential = %selected.name,
                                    attempt,
                                    "stream failed before first byte; retrying the same key"
                                );
                                attempted.remove(&selected.index);
                                sleep_backoff(&self.retry, attempt).await;
                                continue;
                            }
                            return Err(GatewayError {
                                status: StatusCode::BAD_GATEWAY,
                                message: "upstream stream ended before any output".into(),
                                class,
                                retry_after: None,
                                sanitized_body: None,
                            });
                        }
                        Err(_timeout) => {
                            self.circuit.record_overload();
                            let class = UpstreamErrorClass::QueueTimeout;
                            if self.may_retry(&class, attempt) {
                                metrics
                                    .retries_total
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                if self.pool.select(&attempted).is_some() {
                                    tracing::warn!(
                                        request_id,
                                        credential = %selected.name,
                                        attempt,
                                        "no first byte before the deadline; failing over"
                                    );
                                    continue;
                                }
                                tracing::warn!(
                                    request_id,
                                    credential = %selected.name,
                                    attempt,
                                    "no first byte before the deadline; retrying the same key"
                                );
                                attempted.remove(&selected.index);
                                sleep_backoff(&self.retry, attempt).await;
                                continue;
                            }
                            return Err(GatewayError {
                                status: StatusCode::GATEWAY_TIMEOUT,
                                message: "upstream produced no output before the deadline".into(),
                                class,
                                retry_after: None,
                                sanitized_body: None,
                            });
                        }
                    }
                }

                // Non-stream: buffer and validate the whole body.
                let bytes = read_limited(response, defaults::MAX_UPSTREAM_RESPONSE_BYTES).await;
                if serde_json::from_slice::<Value>(&bytes).is_ok() {
                    tracing::info!(
                        request_id,
                        client_model,
                        upstream_model,
                        session_tag,
                        credential = %selected.name,
                        attempt,
                        duration_ms = started.elapsed().as_millis() as u64,
                        response_bytes = bytes.len(),
                        "SenseNova JSON response buffered"
                    );
                    self.circuit.record_success();
                    return Ok(UpstreamOutcome::Json {
                        body: bytes,
                        key: selected,
                        attempt,
                    });
                }
                self.circuit.record_overload();
                let class = UpstreamErrorClass::ServerTransient;
                if self.may_retry(&class, attempt) {
                    metrics
                        .retries_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if self.pool.select(&attempted).is_some() {
                        tracing::warn!(
                            request_id,
                            credential = %selected.name,
                            attempt,
                            "upstream returned malformed JSON before commit; failing over"
                        );
                        continue;
                    }
                    tracing::warn!(
                        request_id,
                        credential = %selected.name,
                        attempt,
                        "upstream returned malformed JSON before commit; retrying the same key"
                    );
                    attempted.remove(&selected.index);
                    sleep_backoff(&self.retry, attempt).await;
                    continue;
                }
                return Err(GatewayError {
                    status: StatusCode::BAD_GATEWAY,
                    message: "upstream returned invalid JSON".into(),
                    class,
                    retry_after: None,
                    sanitized_body: None,
                });
            }

            // Error status: buffer, classify, and decide.
            let headers = response.headers().clone();
            let error_body = read_limited(response, defaults::MAX_ERROR_BODY_BYTES).await;
            let classification =
                classify_upstream_error(status, &headers, &error_body, Duration::from_secs(60));
            let class = classification.class;
            let sanitized = serde_json::from_slice::<Value>(&error_body)
                .map(|value| {
                    let secrets: Vec<&str> = self.secrets.iter().map(String::as_str).collect();
                    crate::redaction::sanitize_json(value, &secrets)
                })
                .unwrap_or_else(|_| {
                    let secrets: Vec<&str> = self.secrets.iter().map(String::as_str).collect();
                    serde_json::json!({"error": {"message": crate::redaction::sanitize_text(
                        &String::from_utf8_lossy(&error_body),
                        &secrets,
                    )}})
                });
            let hint = classification.retry_hint.clone();
            let message = sanitized_message(&sanitized, default_message_for_status(status));

            match class {
                UpstreamErrorClass::RateLimited => {
                    metrics
                        .upstream_429_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    self.circuit.record_overload();
                    let cooldown = hint.as_ref().map(|hint| hint.duration).unwrap_or_default();
                    self.pool.mark_key_cooling(selected.index, cooldown);
                    let hint_source = hint
                        .as_ref()
                        .map(|hint| hint.source.as_str())
                        .unwrap_or("none");
                    // Prefer immediate failover to another credential.
                    if attempt < self.retry.max_attempts && self.pool.select(&attempted).is_some() {
                        metrics
                            .retries_total
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tracing::warn!(
                            request_id,
                            credential = %selected.name,
                            attempt,
                            cooldown_ms = cooldown.as_millis() as u64,
                            hint_source,
                            "rate limited before commit; failing over to the next key"
                        );
                        continue;
                    }
                    // With no alternative key, a short Retry-After may be
                    // waited out within the attempt budget; the same key is
                    // retried after its cooldown expires.
                    let short = hint.as_ref().is_some_and(|hint| {
                        hint.duration
                            <= Duration::from_secs(self.retry.max_retry_after_secs_for_retry)
                    });
                    if self.retry.retry_429_with_short_retry_after
                        && short
                        && attempt < self.retry.max_attempts
                    {
                        metrics
                            .retries_total
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tracing::warn!(
                            request_id,
                            credential = %selected.name,
                            attempt,
                            cooldown_ms = cooldown.as_millis() as u64,
                            hint_source,
                            "short rate limit before commit; waiting out the hint on the same key"
                        );
                        tokio::time::sleep(
                            cooldown + backoff_duration(&self.retry, attempt, pseudo_jitter()),
                        )
                        .await;
                        attempted.remove(&selected.index);
                        continue;
                    }
                    tracing::warn!(
                        request_id,
                        credential = %selected.name,
                        attempt,
                        cooldown_ms = cooldown.as_millis() as u64,
                        hint_source,
                        "upstream rate limit; returning 429 to client"
                    );
                    return Err(GatewayError {
                        status: StatusCode::TOO_MANY_REQUESTS,
                        message,
                        class,
                        retry_after: hint
                            .map(|hint| hint.duration)
                            .or_else(|| self.pool.earliest_retry_after()),
                        sanitized_body: Some(sanitized),
                    });
                }
                UpstreamErrorClass::QuotaExhausted => {
                    metrics
                        .upstream_429_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let cooldown = hint
                        .as_ref()
                        .map(|hint| hint.duration)
                        .unwrap_or(Duration::from_secs(3_600))
                        .clamp(Duration::from_secs(1), self.max_quota_cooldown);
                    if classification.quota_group_exhausted {
                        self.pool
                            .mark_group_cooling(&selected.quota_group, cooldown);
                    } else {
                        self.pool.mark_key_cooling(selected.index, cooldown);
                    }
                    self.circuit.record_quota_exhaustion(cooldown);
                    tracing::error!(
                        request_id,
                        credential = %selected.name,
                        quota_group = %selected.quota_group,
                        attempt,
                        cooldown_ms = cooldown.as_millis() as u64,
                        "quota exhaustion; cooling the failure domain and opening the circuit"
                    );
                    return Err(GatewayError {
                        status: StatusCode::TOO_MANY_REQUESTS,
                        message,
                        class,
                        retry_after: Some(cooldown),
                        sanitized_body: Some(sanitized),
                    });
                }
                UpstreamErrorClass::Authentication | UpstreamErrorClass::Permission => {
                    if class == UpstreamErrorClass::Authentication {
                        // A rejected credential can never recover in-process.
                        self.pool.mark_unusable(selected.index);
                    }
                    self.circuit.record_neutral_failure();
                    // Credential problems are per-key: failover is bounded by
                    // the attempt budget and only when another key exists.
                    if attempt < self.retry.max_attempts && self.pool.select(&attempted).is_some() {
                        tracing::error!(
                            request_id,
                            credential = %selected.name,
                            attempt,
                            "credential rejected before commit; failing over to the next key"
                        );
                        continue;
                    }
                    return Err(GatewayError {
                        status,
                        message,
                        class,
                        retry_after: None,
                        sanitized_body: Some(sanitized),
                    });
                }
                UpstreamErrorClass::ServerTransient
                | UpstreamErrorClass::QueueTimeout
                | UpstreamErrorClass::TransportTransient => {
                    metrics
                        .upstream_5xx_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    self.circuit.record_overload();
                    if self.may_retry(&class, attempt) {
                        metrics
                            .retries_total
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if self.pool.select(&attempted).is_some() {
                            tracing::warn!(
                                request_id,
                                credential = %selected.name,
                                attempt,
                                upstream_status = status.as_u16(),
                                "transient upstream failure before commit; failing over"
                            );
                            continue;
                        }
                        tracing::warn!(
                            request_id,
                            credential = %selected.name,
                            attempt,
                            upstream_status = status.as_u16(),
                            "transient upstream failure before commit; retrying the same key"
                        );
                        attempted.remove(&selected.index);
                        sleep_backoff(&self.retry, attempt).await;
                        continue;
                    }
                    return Err(GatewayError {
                        status: if status == StatusCode::REQUEST_TIMEOUT {
                            StatusCode::GATEWAY_TIMEOUT
                        } else {
                            status
                        },
                        message,
                        class,
                        retry_after: None,
                        sanitized_body: Some(sanitized),
                    });
                }
                UpstreamErrorClass::InvalidRequest
                | UpstreamErrorClass::NotFound
                | UpstreamErrorClass::Unknown
                | UpstreamErrorClass::StreamInterrupted => {
                    self.circuit.record_neutral_failure();
                    return Err(GatewayError {
                        status,
                        message,
                        class,
                        retry_after: None,
                        sanitized_body: Some(sanitized),
                    });
                }
            }
        }
    }

    fn may_retry(&self, class: &UpstreamErrorClass, attempt: usize) -> bool {
        class.is_retryable_before_commit() && attempt < self.retry.max_attempts
    }

    async fn send_once(
        &self,
        selected: &SelectedKey,
        body: Bytes,
        stream: bool,
        request_id: &str,
        upstream_model: &str,
    ) -> std::result::Result<reqwest::Response, reqwest::Error> {
        let started = Instant::now();
        let accept = if stream {
            "text/event-stream"
        } else {
            "application/json"
        };
        // Construct authorization last and never copy caller headers.
        let authorization = format!("Bearer {}", selected.api_key());
        let mut request = self
            .http
            .post(self.messages_url.clone())
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT, accept)
            .header(
                header::USER_AGENT,
                concat!("sensenova-proxy/", env!("CARGO_PKG_VERSION")),
            )
            .header(header::ACCEPT_ENCODING, "gzip")
            .header(
                HeaderName::from_static("anthropic-version"),
                self.anthropic_version.clone(),
            )
            .body(body);
        if let Ok(value) = HeaderValue::try_from(request_id) {
            request = request.header(HeaderName::from_static("x-request-id"), value);
        }
        if let Ok(value) = HeaderValue::try_from(upstream_model) {
            request = request.header(HeaderName::from_static("x-sensenova-proxy-model"), value);
        }
        let result = request
            .header(header::AUTHORIZATION, authorization)
            .send()
            .await;
        match &result {
            Ok(response) => tracing::info!(
                request_id,
                upstream_model,
                credential = %selected.name,
                upstream_status = response.status().as_u16(),
                duration_ms = started.elapsed().as_millis() as u64,
                stream,
                "SenseNova upstream response"
            ),
            Err(error) => tracing::warn!(
                request_id,
                upstream_model,
                credential = %selected.name,
                duration_ms = started.elapsed().as_millis() as u64,
                error_class = transport_error_class(error),
                stream,
                "SenseNova upstream request failed"
            ),
        }
        result
    }
}

fn default_message_for_status(status: StatusCode) -> &'static str {
    match error_type_for_status(status) {
        "invalid_request_error" => "upstream rejected the request",
        "authentication_error" => "upstream rejected the credential",
        "permission_error" => "upstream denied access",
        "not_found_error" => "upstream resource not found",
        _ => "upstream request failed",
    }
}

fn sanitized_message(sanitized: &Value, fallback: &str) -> String {
    let message = sanitized
        .get("error")
        .and_then(|error| {
            error
                .get("message")
                .and_then(Value::as_str)
                .or_else(|| error.as_str())
        })
        .or_else(|| sanitized.get("message").and_then(Value::as_str))
        .unwrap_or(fallback);
    crate::redaction::truncate_public(message, 512)
}

async fn read_limited(mut response: reqwest::Response, limit: usize) -> Bytes {
    let mut output = Vec::new();
    while let Ok(Some(chunk)) = response.chunk().await {
        let remaining = limit.saturating_sub(output.len());
        if remaining == 0 {
            break;
        }
        output.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
        if output.len() >= limit {
            break;
        }
    }
    Bytes::from(output)
}

/// Bounded exponential backoff with jitter: `initial << (attempt-1)` scaled by
/// a jitter factor in [1.0, 1.5), capped at `backoff_max_ms`. Never sleeps
/// indefinitely.
pub fn backoff_duration(
    retry: &crate::config::RetryConfig,
    attempt: usize,
    jitter: f64,
) -> Duration {
    let step = attempt.saturating_sub(1).min(4) as u32;
    let initial = retry.backoff_initial_ms as u128;
    let max = retry.backoff_max_ms as u128;
    let base = initial.saturating_mul(1u128 << step).min(max);
    let jitter = jitter.clamp(0.0, 1.0);
    let value = base as f64 * (1.0 + 0.5 * jitter);
    Duration::from_millis((value as u128).min(max) as u64)
}

async fn sleep_backoff(retry: &crate::config::RetryConfig, attempt: usize) {
    let jitter = pseudo_jitter();
    tokio::time::sleep(backoff_duration(retry, attempt, jitter)).await;
}

fn pseudo_jitter() -> f64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|time| time.subsec_nanos())
        .unwrap_or(0);
    (nanos % 1000) as f64 / 1000.0
}

/// Why a stream produced no first byte.
enum FirstByteFailure {
    /// Clean EOF before any byte: a protocol failure.
    Eof,
    /// A transport-level read failure.
    Transport(reqwest::Error),
}

pub fn circuit_state_code(state: CircuitState) -> i64 {
    match state {
        CircuitState::Closed => 0,
        CircuitState::HalfOpen => 1,
        CircuitState::Open => 2,
    }
}
