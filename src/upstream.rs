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
use crate::pool::KeyPool;
use crate::rate_limit::{
    RetryHintSource, UpstreamErrorClass, classify_upstream_error, transport_error_class,
};
use crate::router::{Followup, RouteSpec, RouteTable, RouteTarget, cap_cooldown, retry_after_for};

#[derive(Clone)]
pub struct Core {
    pub http: reqwest::Client,
    pub messages_url: reqwest::Url,
    /// Retained for the credential surface (`/readyz`, `/metrics`) and for the
    /// legacy non-routing code paths; routing decisions use `routes`.
    pub pool: KeyPool,
    pub routes: RouteTable,
    pub circuit: Arc<CircuitBreaker>,
    pub secrets: Arc<Vec<String>>,
    anthropic_version: HeaderValue,
    first_byte_timeout: Duration,
    max_quota_cooldown: Duration,
    rate_limit_fallback_initial: Duration,
    #[allow(dead_code)]
    spec: RouteSpec,
    max_route_attempts: usize,
    same_route_429_retries: usize,
    retry_after_max: Duration,
}

#[derive(Debug)]
pub enum UpstreamOutcome {
    /// Non-stream exchange completed; the body was read, bounded, and parsed.
    Json {
        body: Bytes,
        target: RouteTarget,
        attempt: usize,
    },
    /// Stream established; the first non-empty chunk is already buffered so
    /// every retry decision predates the first downstream byte.
    Stream {
        first_chunk: Bytes,
        rest: reqwest::Response,
        target: RouteTarget,
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
        let pool = KeyPool::new(&config.sensenova_api_keys);
        let routes = RouteTable::new(config, pool.clone());
        let route = routes.route_config().clone();
        Ok(Self {
            http,
            messages_url: config.messages_url()?,
            pool,
            routes,
            circuit: Arc::new(CircuitBreaker::new(
                config.circuit.overload_threshold,
                config.circuit.overload_window_secs,
                config.circuit.overload_open_secs,
            )),
            secrets: Arc::new(secrets),
            anthropic_version,
            first_byte_timeout: Duration::from_secs(config.upstream.first_byte_timeout_secs),
            max_quota_cooldown: Duration::from_secs(config.circuit.max_quota_cooldown_secs),
            rate_limit_fallback_initial: Duration::from_secs(
                config.retry.rate_limit_fallback_initial_secs,
            ),
            spec: RouteSpec::Profile(config.default_profile()),
            max_route_attempts: route.max_route_attempts,
            same_route_429_retries: route.same_route_429_retries,
            retry_after_max: route.retry_after_max,
        })
    }

    /// Send one logical `/v1/messages` request through the routing table.
    ///
    /// Retry budget: exactly `routing.max_route_attempts` upstream attempts per
    /// logical request, whatever mix of model, quota group and credential they
    /// use. Every attempt draws from that one counter — there is no nested
    /// retry loop that can multiply it. Failover is always preferred over
    /// replaying the same `(model, group, key)` route.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_messages(
        &self,
        metrics: &Metrics,
        stream: bool,
        request_id: &str,
        client_model: &str,
        canonical: &Bytes,
        session_tag: &str,
        spec: &RouteSpec,
    ) -> std::result::Result<UpstreamOutcome, GatewayError> {
        let mut skipped: HashSet<(u64, u64, usize)> = HashSet::new();
        let mut same_route_retries = 0usize;
        let mut attempt = 0usize;
        let mut last_error: Option<GatewayError> = None;

        loop {
            if attempt >= self.max_route_attempts {
                metrics
                    .routing_exhausted_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(
                    request_id,
                    attempt,
                    budget = self.max_route_attempts,
                    "route-attempt budget exhausted"
                );
                return Err(last_error.unwrap_or_else(|| GatewayError {
                    status: StatusCode::TOO_MANY_REQUESTS,
                    message: "all routing options for this request were exhausted".into(),
                    class: UpstreamErrorClass::RateLimited,
                    retry_after: self.routes.earliest_retry_after(),
                    sanitized_body: None,
                }));
            }

            let plan = match self
                .routes
                .plan(spec, session_tag, attempt, &skipped, metrics)
            {
                Ok(plan) => plan,
                Err(waitable) => return Err(self.route_unavailable(waitable, request_id)),
            };

            // Pre-dial health gate: the global circuit still guards a
            // proxy-wide upstream outage, but a single model's circuit no
            // longer blocks healthy models.
            if let Admission::Refused { remaining, reason } = self.circuit.admit() {
                tracing::warn!(
                    request_id,
                    circuit_reason = reason,
                    cooldown_ms = remaining.as_millis() as u64,
                    "circuit open; refusing upstream call without dialing SenseNova"
                );
                return Err(GatewayError {
                    status: StatusCode::TOO_MANY_REQUESTS,
                    message: format!("upstream temporarily unavailable (circuit open: {reason})"),
                    class: UpstreamErrorClass::RateLimited,
                    retry_after: Some(remaining),
                    sanitized_body: None,
                });
            }

            let target = plan.target.clone();
            let stepped_down = plan.stepped_down;
            let gate = plan.gate.map(|gate| gate.as_str()).unwrap_or("");
            let cross_group = !skipped.is_empty()
                && skipped
                    .iter()
                    .any(|(_, group, _)| *group != plan.target.identity().1);
            attempt += 1;
            skipped.insert(target.identity());
            metrics
                .route_attempts_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            metrics
                .upstream_requests_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            // Per-attempt model rewrite: the canonical body is never mutated,
            // so a retry can safely switch models.
            let attempt_body = match rewrite_model(canonical, &target.model) {
                Ok(body) => body,
                Err(error) => return Err(error),
            };

            tracing::info!(
                request_id,
                client_model,
                model = target.model_str(),
                profile = spec.label(),
                tier = target.tier,
                stepped_down,
                gate,
                credential = %target.key.name,
                quota_group = target.quota_group_str(),
                session_tag,
                attempt,
                cross_group,
                max_attempts = self.max_route_attempts,
                "route attempt"
            );

            let started = Instant::now();
            let mut guard = self.routes.begin_attempt(&target);
            let response = match self
                .send_once(&target, attempt_body, stream, request_id)
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    metrics
                        .upstream_transport_errors_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    self.circuit.record_overload();
                    let cooldown = self.routes.note_transient_failure(
                        metrics,
                        &target,
                        attempt,
                        None,
                        pseudo_jitter(),
                    );
                    self.routes.break_affinity(metrics, session_tag);
                    let class = if error.is_timeout() {
                        UpstreamErrorClass::QueueTimeout
                    } else {
                        UpstreamErrorClass::TransportTransient
                    };
                    let failure = GatewayError {
                        status: StatusCode::BAD_GATEWAY,
                        message: "could not reach the SenseNova upstream".into(),
                        class,
                        retry_after: None,
                        sanitized_body: None,
                    };
                    drop(guard);
                    if let Some(next) = self.next_attempt(
                        spec,
                        metrics,
                        &target,
                        &mut skipped,
                        &mut same_route_retries,
                        attempt,
                        session_tag,
                        "transport_error",
                        error_class(&error),
                        cooldown,
                        request_id,
                    ) {
                        last_error = Some(failure);
                        if let Some(wait) = next {
                            tokio::time::sleep(wait).await;
                        }
                        continue;
                    }
                    return Err(failure);
                }
            };

            let status = response.status();
            if status.is_success() {
                if stream {
                    let mut rest = response;
                    let first = tokio::time::timeout(self.first_byte_timeout, async {
                        loop {
                            match rest.chunk().await {
                                Ok(Some(chunk)) if !chunk.is_empty() => {
                                    break Ok::<Bytes, FirstByteFailure>(chunk);
                                }
                                Ok(Some(_)) => continue,
                                Ok(None) => break Err(FirstByteFailure::Eof),
                                Err(error) => break Err(FirstByteFailure::Transport(error)),
                            }
                        }
                    })
                    .await;
                    match first {
                        Ok(Ok(chunk)) => {
                            let ttft = started.elapsed();
                            metrics.note_time_to_first_event(ttft);
                            self.routes.note_success(metrics, &target, Some(ttft), ttft);
                            if attempt == 1 {
                                // Only an uninterrupted first-try success
                                // establishes affinity: a request that had to
                                // fail over must not re-pin the session to
                                // whatever eventually answered.
                                self.routes
                                    .note_session_route(metrics, session_tag, &target);
                            }
                            self.circuit.record_success();
                            guard.release_probe();
                            tracing::info!(
                                request_id,
                                model = target.model_str(),
                                client_model,
                                session_tag,
                                credential = %target.key.name,
                                quota_group = target.quota_group_str(),
                                tier = target.tier,
                                attempt,
                                first_byte_ms = ttft.as_millis() as u64,
                                "stream established; commit barrier reached"
                            );
                            return Ok(UpstreamOutcome::Stream {
                                first_chunk: chunk,
                                rest,
                                target,
                                attempt,
                            });
                        }
                        Ok(Err(failure)) => {
                            metrics
                                .upstream_transport_errors_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            self.circuit.record_overload();
                            let first_byte_failure = match &failure {
                                FirstByteFailure::Eof => "eof",
                                FirstByteFailure::Transport(error) if error.is_timeout() => {
                                    "timeout"
                                }
                                FirstByteFailure::Transport(_) => "transport",
                            };
                            let class = match failure {
                                FirstByteFailure::Eof => UpstreamErrorClass::TransportTransient,
                                FirstByteFailure::Transport(ref error) if error.is_timeout() => {
                                    UpstreamErrorClass::QueueTimeout
                                }
                                FirstByteFailure::Transport(_) => {
                                    UpstreamErrorClass::TransportTransient
                                }
                            };
                            let cooldown = self.routes.note_transient_failure(
                                metrics,
                                &target,
                                attempt,
                                None,
                                pseudo_jitter(),
                            );
                            self.routes.break_affinity(metrics, session_tag);
                            let result = GatewayError {
                                status: StatusCode::BAD_GATEWAY,
                                message: "upstream stream ended before any output".into(),
                                class,
                                retry_after: None,
                                sanitized_body: None,
                            };
                            drop(guard);
                            if let Some(wait) = self.next_attempt(
                                spec,
                                metrics,
                                &target,
                                &mut skipped,
                                &mut same_route_retries,
                                attempt,
                                session_tag,
                                "stream_eof_before_first_byte",
                                first_byte_failure,
                                cooldown,
                                request_id,
                            ) {
                                last_error = Some(result);
                                if let Some(wait) = wait {
                                    tokio::time::sleep(wait).await;
                                }
                                continue;
                            }
                            return Err(result);
                        }
                        Err(_timeout) => {
                            self.circuit.record_overload();
                            let cooldown = self.routes.note_transient_failure(
                                metrics,
                                &target,
                                attempt,
                                None,
                                pseudo_jitter(),
                            );
                            self.routes.break_affinity(metrics, session_tag);
                            let result = GatewayError {
                                status: StatusCode::GATEWAY_TIMEOUT,
                                message: "upstream produced no output before the deadline".into(),
                                class: UpstreamErrorClass::QueueTimeout,
                                retry_after: None,
                                sanitized_body: None,
                            };
                            drop(guard);
                            if let Some(wait) = self.next_attempt(
                                spec,
                                metrics,
                                &target,
                                &mut skipped,
                                &mut same_route_retries,
                                attempt,
                                session_tag,
                                "first_byte_timeout",
                                "timeout",
                                cooldown,
                                request_id,
                            ) {
                                last_error = Some(result);
                                if let Some(wait) = wait {
                                    tokio::time::sleep(wait).await;
                                }
                                continue;
                            }
                            return Err(result);
                        }
                    }
                }

                let bytes = read_limited(response, defaults::MAX_UPSTREAM_RESPONSE_BYTES).await;
                if serde_json::from_slice::<Value>(&bytes).is_ok() {
                    let elapsed = started.elapsed();
                    self.routes.note_success(metrics, &target, None, elapsed);
                    if attempt == 1 {
                        self.routes
                            .note_session_route(metrics, session_tag, &target);
                    }
                    self.circuit.record_success();
                    guard.release_probe();
                    tracing::info!(
                        request_id,
                        client_model,
                        model = target.model_str(),
                        tier = target.tier,
                        credential = %target.key.name,
                        quota_group = target.quota_group_str(),
                        session_tag,
                        attempt,
                        duration_ms = elapsed.as_millis() as u64,
                        response_bytes = bytes.len(),
                        "JSON response buffered"
                    );
                    return Ok(UpstreamOutcome::Json {
                        body: bytes,
                        target,
                        attempt,
                    });
                }
                self.circuit.record_overload();
                let cooldown = self.routes.note_transient_failure(
                    metrics,
                    &target,
                    attempt,
                    None,
                    pseudo_jitter(),
                );
                self.routes.break_affinity(metrics, session_tag);
                let result = GatewayError {
                    status: StatusCode::BAD_GATEWAY,
                    message: "upstream returned invalid JSON".into(),
                    class: UpstreamErrorClass::ServerTransient,
                    retry_after: None,
                    sanitized_body: None,
                };
                drop(guard);
                if let Some(wait) = self.next_attempt(
                    spec,
                    metrics,
                    &target,
                    &mut skipped,
                    &mut same_route_retries,
                    attempt,
                    session_tag,
                    "malformed_upstream_json",
                    "invalid_json",
                    cooldown,
                    request_id,
                ) {
                    last_error = Some(result);
                    if let Some(wait) = wait {
                        tokio::time::sleep(wait).await;
                    }
                    continue;
                }
                return Err(result);
            }

            // Error status: buffer, classify, and decide.
            let headers = response.headers().clone();
            let error_body = read_limited(response, defaults::MAX_ERROR_BODY_BYTES).await;
            let classification = classify_upstream_error(
                status,
                &headers,
                &error_body,
                self.rate_limit_fallback_initial,
            );
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
            let classification_reason = classification.reason.as_str();
            let upstream_error_code = classification.numeric_code;
            let upstream_error_kind = classification.error_kind.as_deref().unwrap_or("");
            let hint_source = hint
                .as_ref()
                .map(|hint| hint.source.as_str())
                .unwrap_or("none");

            match class {
                UpstreamErrorClass::RateLimited => {
                    metrics
                        .upstream_429_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    metrics
                        .model_account_429_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // Deliberately NOT record_overload(): a per-route generic
                    // 429 says nothing about other quota groups, and the route
                    // cooldown plus failover already protects the upstream.
                    let fallback_hint = hint
                        .as_ref()
                        .is_some_and(|hint| hint.source == RetryHintSource::Fallback);
                    let streak = if fallback_hint {
                        self.routes.rate_limit_streak(&target)
                    } else {
                        0
                    };
                    let cooldown = self.routes.note_rate_limited(
                        metrics,
                        &target,
                        attempt,
                        streak,
                        if fallback_hint {
                            None
                        } else {
                            hint.as_ref().map(|hint| hint.duration)
                        },
                        pseudo_jitter(),
                    );
                    self.routes.break_affinity(metrics, session_tag);
                    let result = GatewayError {
                        status: StatusCode::TOO_MANY_REQUESTS,
                        message: message.clone(),
                        class,
                        retry_after: Some(retry_after_for(cooldown)),
                        sanitized_body: Some(sanitized.clone()),
                    };
                    drop(guard);
                    tracing::warn!(
                        request_id,
                        credential = %target.key.name,
                        quota_group = target.quota_group_str(),
                        model = target.model_str(),
                        tier = target.tier,
                        attempt,
                        classification = class.as_str(),
                        classification_reason,
                        upstream_error_code,
                        upstream_error_kind,
                        cooldown_ms = cooldown.as_millis() as u64,
                        hint_source,
                        "generic 429; cooling (model, quota_group) only"
                    );
                    if let Some(wait) = self.next_attempt(
                        spec,
                        metrics,
                        &target,
                        &mut skipped,
                        &mut same_route_retries,
                        attempt,
                        session_tag,
                        "rate_limited",
                        "rate_limited",
                        cooldown,
                        request_id,
                    ) {
                        last_error = Some(result);
                        if let Some(wait) = wait {
                            tokio::time::sleep(wait).await;
                        }
                        continue;
                    }
                    return Err(result);
                }
                UpstreamErrorClass::QuotaExhausted => {
                    metrics
                        .upstream_429_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    metrics
                        .quota_exhaustion_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let cooldown = hint
                        .as_ref()
                        .map(|hint| hint.duration)
                        .unwrap_or(Duration::from_secs(3_600));
                    let cooldown = cap_cooldown(cooldown, self.max_quota_cooldown);
                    self.routes.note_quota_exhausted(metrics, &target, cooldown);
                    self.routes.break_affinity(metrics, session_tag);
                    let circuit_opened = self.routes.usable_route_count() == 0;
                    if circuit_opened {
                        self.circuit.record_quota_exhaustion(cooldown);
                    }
                    tracing::error!(
                        request_id,
                        credential = %target.key.name,
                        quota_group = target.quota_group_str(),
                        model = target.model_str(),
                        attempt,
                        classification = class.as_str(),
                        classification_reason,
                        upstream_error_code,
                        upstream_error_kind,
                        circuit_opened,
                        cooldown_ms = cooldown.as_millis() as u64,
                        "quota exhaustion; cooling the whole quota_group"
                    );
                    let result = GatewayError {
                        status: StatusCode::TOO_MANY_REQUESTS,
                        message,
                        class,
                        retry_after: Some(retry_after_for(cooldown)),
                        sanitized_body: Some(sanitized),
                    };
                    drop(guard);
                    // The exhausted group is fully cooled; only a different
                    // group can serve, and never a same-route replay.
                    if let Some(wait) = self.next_attempt_group(
                        spec,
                        metrics,
                        &target,
                        &mut skipped,
                        attempt,
                        session_tag,
                        cooldown,
                        request_id,
                    ) {
                        last_error = Some(result);
                        if let Some(wait) = wait {
                            tokio::time::sleep(wait).await;
                        }
                        continue;
                    }
                    return Err(result);
                }
                UpstreamErrorClass::Authentication | UpstreamErrorClass::Permission => {
                    if class == UpstreamErrorClass::Authentication {
                        self.routes.note_authentication_failure(&target);
                    }
                    self.routes.break_affinity(metrics, session_tag);
                    self.circuit.record_neutral_failure();
                    let result = GatewayError {
                        status,
                        message,
                        class,
                        retry_after: None,
                        sanitized_body: Some(sanitized),
                    };
                    drop(guard);
                    tracing::error!(
                        request_id,
                        credential = %target.key.name,
                        quota_group = target.quota_group_str(),
                        model = target.model_str(),
                        attempt,
                        classification = class.as_str(),
                        "credential rejected; failing over without disabling the account"
                    );
                    if let Some(wait) = self.next_attempt(
                        spec,
                        metrics,
                        &target,
                        &mut skipped,
                        &mut same_route_retries,
                        attempt,
                        session_tag,
                        "credential_rejected",
                        "credential_rejected",
                        Duration::ZERO,
                        request_id,
                    ) {
                        last_error = Some(result);
                        if let Some(wait) = wait {
                            tokio::time::sleep(wait).await;
                        }
                        continue;
                    }
                    return Err(result);
                }
                UpstreamErrorClass::NotFound => {
                    self.routes.note_model_missing(metrics, &target);
                    self.routes.break_affinity(metrics, session_tag);
                    self.circuit.record_neutral_failure();
                    let result = GatewayError {
                        status,
                        message,
                        class,
                        retry_after: None,
                        sanitized_body: Some(sanitized),
                    };
                    drop(guard);
                    tracing::warn!(
                        request_id,
                        model = target.model_str(),
                        tier = target.tier,
                        attempt,
                        "model not served on this route; disabled and failing over"
                    );
                    if let Some(wait) = self.next_attempt_model(
                        spec,
                        metrics,
                        &target,
                        &mut skipped,
                        attempt,
                        session_tag,
                        Duration::from_secs(1),
                        request_id,
                    ) {
                        last_error = Some(result);
                        if let Some(wait) = wait {
                            tokio::time::sleep(wait).await;
                        }
                        continue;
                    }
                    return Err(result);
                }
                UpstreamErrorClass::ServerTransient
                | UpstreamErrorClass::QueueTimeout
                | UpstreamErrorClass::TransportTransient => {
                    metrics
                        .upstream_5xx_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    self.circuit.record_overload();
                    let cooldown = self.routes.note_transient_failure(
                        metrics,
                        &target,
                        attempt,
                        hint.as_ref().map(|hint| hint.duration),
                        pseudo_jitter(),
                    );
                    self.routes.break_affinity(metrics, session_tag);
                    let result = GatewayError {
                        status: if status == StatusCode::REQUEST_TIMEOUT {
                            StatusCode::GATEWAY_TIMEOUT
                        } else {
                            status
                        },
                        message,
                        class,
                        retry_after: None,
                        sanitized_body: Some(sanitized),
                    };
                    drop(guard);
                    tracing::warn!(
                        request_id,
                        credential = %target.key.name,
                        quota_group = target.quota_group_str(),
                        model = target.model_str(),
                        tier = target.tier,
                        attempt,
                        upstream_status = status.as_u16(),
                        cooldown_ms = cooldown.as_millis() as u64,
                        "transient upstream failure before commit; failing over"
                    );
                    if let Some(wait) = self.next_attempt(
                        spec,
                        metrics,
                        &target,
                        &mut skipped,
                        &mut same_route_retries,
                        attempt,
                        session_tag,
                        "http_5xx",
                        "http_5xx",
                        cooldown,
                        request_id,
                    ) {
                        last_error = Some(result);
                        if let Some(wait) = wait {
                            tokio::time::sleep(wait).await;
                        }
                        continue;
                    }
                    return Err(result);
                }
                UpstreamErrorClass::InvalidRequest
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

    /// Dial one route with its own model in the request body.
    async fn send_once(
        &self,
        target: &RouteTarget,
        body: Bytes,
        stream: bool,
        request_id: &str,
    ) -> std::result::Result<reqwest::Response, reqwest::Error> {
        let started = Instant::now();
        let accept = if stream {
            "text/event-stream"
        } else {
            "application/json"
        };
        // Construct authorization last and never copy caller headers.
        let authorization = format!("Bearer {}", target.key.api_key());
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
        if let Ok(value) = HeaderValue::try_from(target.model_str()) {
            request = request.header(HeaderName::from_static("x-sensenova-proxy-model"), value);
        }
        let result = request
            .header(header::AUTHORIZATION, authorization)
            .send()
            .await;
        match &result {
            Ok(response) => tracing::info!(
                request_id,
                model = target.model_str(),
                credential = %target.key.name,
                upstream_status = response.status().as_u16(),
                duration_ms = started.elapsed().as_millis() as u64,
                stream,
                "SenseNova upstream response"
            ),
            Err(error) => tracing::warn!(
                request_id,
                model = target.model_str(),
                credential = %target.key.name,
                duration_ms = started.elapsed().as_millis() as u64,
                error_class = transport_error_class(error),
                stream,
                "SenseNova upstream request failed"
            ),
        }
        result
    }

    /// Decide whether another route attempt is worth making.
    ///
    /// Returns `Some(wait)` to continue (after sleeping `wait`), or `None`
    /// when the budget is spent or nothing is dialable. A same-route replay is
    /// only ever allowed when the profile's `same_route_429_retries` budget
    /// permits *and* no other healthy route exists.
    #[allow(clippy::too_many_arguments)]
    fn next_attempt(
        &self,
        spec: &RouteSpec,
        metrics: &Metrics,
        failed: &RouteTarget,
        skipped: &mut HashSet<(u64, u64, usize)>,
        same_route_retries: &mut usize,
        attempt: usize,
        session_tag: &str,
        reason: &'static str,
        detail: &'static str,
        cooldown: Duration,
        request_id: &str,
    ) -> Option<Option<Duration>> {
        if attempt >= self.max_route_attempts {
            return None;
        }
        match self
            .routes
            .followup(spec, session_tag, failed, skipped, metrics)
        {
            Followup::Continue { wait, waitable } => {
                metrics
                    .retries_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                metrics
                    .route_failovers_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(
                    request_id,
                    credential = %failed.key.name,
                    quota_group = failed.quota_group_str(),
                    model = failed.model_str(),
                    attempt,
                    retry_reason = reason,
                    detail,
                    cooldown_ms = cooldown.as_millis() as u64,
                    wait_ms = wait.as_millis() as u64,
                    same_route_retry = false,
                    hint_waitable = waitable,
                    "pre-commit failure; failing over to the next route"
                );
                Some((wait > Duration::ZERO).then_some(wait))
            }
            Followup::Return => {
                // Nothing healthy is dialable. A same-route replay is the last
                // resort and is tightly bounded.
                if *same_route_retries >= self.same_route_429_retries
                    || cooldown > self.retry_after_max
                {
                    return None;
                }
                *same_route_retries += 1;
                metrics
                    .retries_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(
                    request_id,
                    credential = %failed.key.name,
                    quota_group = failed.quota_group_str(),
                    model = failed.model_str(),
                    attempt,
                    retry_reason = reason,
                    detail,
                    cooldown_ms = cooldown.as_millis() as u64,
                    same_route_retry = true,
                    same_route_budget = self.same_route_429_retries,
                    "no alternative route; waiting out the bounded cooldown on the same route"
                );
                Some(Some(cooldown))
            }
        }
    }

    /// Explicit quota exhaustion: only a different quota group may be tried.
    #[allow(clippy::too_many_arguments)]
    fn next_attempt_group(
        &self,
        spec: &RouteSpec,
        metrics: &Metrics,
        failed: &RouteTarget,
        skipped: &mut HashSet<(u64, u64, usize)>,
        attempt: usize,
        session_tag: &str,
        cooldown: Duration,
        request_id: &str,
    ) -> Option<Option<Duration>> {
        if attempt >= self.max_route_attempts {
            return None;
        }
        let mut probe = skipped.clone();
        probe.insert(failed.identity());
        // The failed group is fully cooled, so the planner will not return it;
        // any plan therefore implies a different group (or model).
        match self
            .routes
            .plan(spec, session_tag, attempt, &probe, metrics)
        {
            Ok(plan) if plan.target.quota_group_str() != failed.quota_group_str() => {
                metrics
                    .retries_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                metrics
                    .cross_group_failovers_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                metrics
                    .route_failovers_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(
                    request_id,
                    credential = %failed.key.name,
                    quota_group = failed.quota_group_str(),
                    model = failed.model_str(),
                    attempt,
                    next_model = plan.target.model_str(),
                    next_quota_group = plan.target.quota_group_str(),
                    cross_group_failover = true,
                    cooldown_ms = cooldown.as_millis() as u64,
                    "quota exhaustion; failing over to another quota group"
                );
                Some(None)
            }
            _ => None,
        }
    }

    /// 404: the failed `(model, group)` route is disabled, so the next
    /// attempt uses another route — the same model on another quota group
    /// (account entitlement may differ) or, failing that, another model.
    #[allow(clippy::too_many_arguments)]
    fn next_attempt_model(
        &self,
        spec: &RouteSpec,
        metrics: &Metrics,
        failed: &RouteTarget,
        skipped: &mut HashSet<(u64, u64, usize)>,
        attempt: usize,
        session_tag: &str,
        cooldown: Duration,
        request_id: &str,
    ) -> Option<Option<Duration>> {
        if attempt >= self.max_route_attempts {
            return None;
        }
        match self
            .routes
            .plan(spec, session_tag, attempt, skipped, metrics)
        {
            Ok(plan) if plan.target.identity() != failed.identity() => {
                metrics
                    .retries_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                metrics
                    .route_failovers_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let cross_model = plan.target.model_str() != failed.model_str();
                if cross_model {
                    metrics
                        .model_failovers_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    metrics
                        .cross_model_failovers_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                tracing::warn!(
                    request_id,
                    model = failed.model_str(),
                    quota_group = failed.quota_group_str(),
                    attempt,
                    next_model = plan.target.model_str(),
                    next_quota_group = plan.target.quota_group_str(),
                    next_tier = plan.target.tier,
                    cooldown_ms = cooldown.as_millis() as u64,
                    cross_model_failover = cross_model,
                    "model missing on this route; failing over to another route"
                );
                Some(None)
            }
            _ => None,
        }
    }

    /// Build the client-facing failure when no route can be dialed at all.
    fn route_unavailable(
        &self,
        waitable: crate::router::WaitablePlan,
        request_id: &str,
    ) -> GatewayError {
        let wait = waitable
            .wait
            .map(|wait| cap_cooldown(wait, self.retry_after_max));
        match wait {
            Some(wait) => {
                tracing::warn!(
                    request_id,
                    retry_after_ms = wait.as_millis() as u64,
                    routes_inactive = self.routes.usable_route_count(),
                    "no route is currently dialable; returning Retry-After"
                );
                GatewayError {
                    status: StatusCode::TOO_MANY_REQUESTS,
                    message: "all configured routes are cooling down; retry shortly".into(),
                    class: UpstreamErrorClass::RateLimited,
                    retry_after: Some(retry_after_for(wait)),
                    sanitized_body: None,
                }
            }
            None => {
                tracing::error!(
                    request_id,
                    credentials = self.routes.credential_count(),
                    "no usable route exists for the configured routing profiles"
                );
                GatewayError {
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    message: "no usable upstream route is configured for this model".into(),
                    class: UpstreamErrorClass::Unknown,
                    retry_after: None,
                    sanitized_body: None,
                }
            }
        }
    }
}

/// Rewrite only the `model` field of a canonical request body.
///
/// The canonical body is never mutated, so every attempt can carry a
/// different upstream model without risking a partial or double rewrite.
fn rewrite_model(canonical: &Bytes, model: &str) -> std::result::Result<Bytes, GatewayError> {
    let mut value: Value = serde_json::from_slice(canonical).map_err(|_| GatewayError {
        status: StatusCode::BAD_REQUEST,
        message: "request body could not be re-serialized for routing".into(),
        class: UpstreamErrorClass::InvalidRequest,
        retry_after: None,
        sanitized_body: None,
    })?;
    match value.as_object_mut() {
        Some(object) => {
            object.insert("model".into(), Value::String(model.to_owned()));
        }
        None => {
            return Err(GatewayError {
                status: StatusCode::BAD_REQUEST,
                message: "request body must be a JSON object".into(),
                class: UpstreamErrorClass::InvalidRequest,
                retry_after: None,
                sanitized_body: None,
            });
        }
    }
    serde_json::to_vec(&value)
        .map(Bytes::from)
        .map_err(|_| GatewayError {
            status: StatusCode::BAD_REQUEST,
            message: "request could not be serialized".into(),
            class: UpstreamErrorClass::InvalidRequest,
            retry_after: None,
            sanitized_body: None,
        })
}

/// Stable, bounded transport-error label for logs.
fn error_class(error: &reqwest::Error) -> &'static str {
    transport_error_class(error)
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
