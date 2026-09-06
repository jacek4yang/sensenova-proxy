//! Upstream error classification and safe retry-hint extraction.
//!
//! SenseNova error envelopes observed in the wild:
//! - OpenAI-style routes: `{"error":{"code":<int>,"message":"..."}}` with
//!   Google-API-style numeric codes (code 16 = UNAUTHENTICATED was observed;
//!   the control plane also answers with `error_key` / gRPC `ErrorInfo`
//!   details).
//! - Anthropic route: `{"type":"error","error":{"type":...,"message":...}}`.
//! - Generic rate limiting (observed in production): HTTP 429 with busy/retry
//!   wording while the account's Token Plan dashboard still shows substantial
//!   remaining credits. Official documentation therefore maps HTTP 429 to
//!   "rate limiting → back off and retry", and explicit markers such as
//!   `FREE_QUOTA_EXHAUSTED` to plan-quota exhaustion.
//!
//! Consequence for classification: HTTP 429 is **always** `RateLimited`
//! unless the body carries explicit, unambiguous quota-exhaustion evidence.
//! Google-style numeric code 8 (`RESOURCE_EXHAUSTED`) alone describes any
//! exhausted server-side resource (RPM/TPM/concurrency/capacity) and must
//! never by itself equate to account credit exhaustion. Body text containing
//! "429" alone never classifies anything.

use std::time::{Duration, SystemTime};

use axum::http::{HeaderMap, StatusCode};
use serde_json::Value;

/// Hard cap applied to every parsed cooldown so a hostile or buggy hint can
/// never park a credential forever.
pub const MAX_PARSED_COOLDOWN: Duration = Duration::from_secs(366 * 24 * 60 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamErrorClass {
    RateLimited,
    QuotaExhausted,
    Authentication,
    Permission,
    InvalidRequest,
    NotFound,
    QueueTimeout,
    ServerTransient,
    TransportTransient,
    /// Reserved for stream failures after the downstream commit; those are
    /// handled by the stream pump rather than the pre-commit classifier, but
    /// the classifier documents them as a distinct class.
    #[allow(dead_code)]
    StreamInterrupted,
    Unknown,
}

impl UpstreamErrorClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RateLimited => "rate_limited",
            Self::QuotaExhausted => "quota_exhausted",
            Self::Authentication => "authentication",
            Self::Permission => "permission",
            Self::InvalidRequest => "invalid_request",
            Self::NotFound => "not_found",
            Self::QueueTimeout => "queue_timeout",
            Self::ServerTransient => "server_transient",
            Self::TransportTransient => "transport_transient",
            Self::StreamInterrupted => "stream_interrupted",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryHintSource {
    RetryAfterHeader,
    StructuredJson,
    HumanText,
    Fallback,
}

impl RetryHintSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RetryAfterHeader => "retry_after_header",
            Self::StructuredJson => "structured_json",
            Self::HumanText => "human_text",
            Self::Fallback => "fallback",
        }
    }
}

/// Why the classifier produced its verdict. Observability only — the class
/// itself is what drives behavior — but logs must be able to answer "why was
/// this classified as X?".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClassificationReason {
    /// Plain HTTP 429 with no explicit quota evidence.
    Http429RateLimit,
    /// Rate-limited verdict where Google-style code 8 (RESOURCE_EXHAUSTED)
    /// was present but no explicit quota evidence: per official semantics
    /// code 8 covers any exhausted server-side resource (RPM, TPM,
    /// concurrency, capacity), not just account credits.
    ResourceExhaustedCode,
    /// The body carried explicit, unambiguous quota-exhaustion evidence.
    ExplicitQuotaEvidence,
    /// Explicit quota evidence under a non-429 status.
    ExplicitQuotaEvidenceOffStatus,
    /// Mapped purely from the HTTP status code (5xx, 401, 404, ...).
    HttpStatus,
    /// No specific signal matched.
    UnknownBody,
}

impl ClassificationReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Http429RateLimit => "http_429",
            Self::ResourceExhaustedCode => "resource_exhausted_code",
            Self::ExplicitQuotaEvidence => "explicit_quota_evidence",
            Self::ExplicitQuotaEvidenceOffStatus => "explicit_quota_evidence_off_429",
            Self::HttpStatus => "http_status",
            Self::UnknownBody => "unknown_body",
        }
    }
}

#[derive(Debug, Clone)]
pub struct RetryHint {
    pub duration: Duration,
    pub source: RetryHintSource,
}

#[derive(Debug, Clone)]
pub struct ClassifiedUpstreamError {
    pub class: UpstreamErrorClass,
    pub retry_hint: Option<RetryHint>,
    /// True when the failure looks account-level (shared quota group), as
    /// opposed to a per-key transient rate limit.
    pub quota_group_exhausted: bool,
    pub reason: ClassificationReason,
    /// Google-style numeric error code from the body, if any (observability).
    pub numeric_code: Option<i64>,
    /// Bounded, credential-free error kind marker for logs: the Anthropic
    /// `error.type`, or the control-plane `error_key`, if present.
    pub error_kind: Option<String>,
}

pub fn classify_upstream_error(
    status: StatusCode,
    headers: &HeaderMap,
    body: &[u8],
    fallback_cooldown: Duration,
) -> ClassifiedUpstreamError {
    let value = serde_json::from_slice::<Value>(body).ok();
    let message = value
        .as_ref()
        .and_then(extract_error_message)
        .or_else(|| {
            let text = String::from_utf8_lossy(body);
            (!text.trim().is_empty()).then(|| text.trim().to_owned())
        })
        .unwrap_or_default();
    let numeric_code = value.as_ref().and_then(numeric_error_code);
    let error_kind = value
        .as_ref()
        .and_then(error_kind_marker)
        .map(str::to_owned);
    let explicit_quota = is_explicit_quota_exhaustion(&message);

    let (class, quota_group_exhausted, reason) = match status.as_u16() {
        429 => {
            if explicit_quota {
                (
                    UpstreamErrorClass::QuotaExhausted,
                    true,
                    ClassificationReason::ExplicitQuotaEvidence,
                )
            } else if numeric_code == Some(8) {
                // RESOURCE_EXHAUSTED without quota evidence: any exhausted
                // server-side resource. Rate limiting, not account exhaustion.
                (
                    UpstreamErrorClass::RateLimited,
                    false,
                    ClassificationReason::ResourceExhaustedCode,
                )
            } else {
                (
                    UpstreamErrorClass::RateLimited,
                    false,
                    ClassificationReason::Http429RateLimit,
                )
            }
        }
        401 => (
            UpstreamErrorClass::Authentication,
            false,
            ClassificationReason::HttpStatus,
        ),
        403 => (
            UpstreamErrorClass::Permission,
            false,
            ClassificationReason::HttpStatus,
        ),
        400 | 405 | 413 | 422 => (
            UpstreamErrorClass::InvalidRequest,
            false,
            ClassificationReason::HttpStatus,
        ),
        404 => (
            UpstreamErrorClass::NotFound,
            false,
            ClassificationReason::HttpStatus,
        ),
        408 => (
            UpstreamErrorClass::QueueTimeout,
            false,
            ClassificationReason::HttpStatus,
        ),
        500 | 502 | 503 | 504 => (
            UpstreamErrorClass::ServerTransient,
            false,
            ClassificationReason::HttpStatus,
        ),
        _ => {
            // Account-level exhaustion may arrive under unexpected statuses;
            // require explicit quota wording before inferring it.
            if explicit_quota {
                (
                    UpstreamErrorClass::QuotaExhausted,
                    true,
                    ClassificationReason::ExplicitQuotaEvidenceOffStatus,
                )
            } else {
                (
                    UpstreamErrorClass::Unknown,
                    false,
                    ClassificationReason::UnknownBody,
                )
            }
        }
    };

    let retry_hint =
        match class {
            UpstreamErrorClass::RateLimited | UpstreamErrorClass::QuotaExhausted => Some(
                retry_hint(headers, &message, value.as_ref(), fallback_cooldown),
            ),
            _ => None,
        };

    ClassifiedUpstreamError {
        class,
        retry_hint,
        quota_group_exhausted,
        reason,
        numeric_code,
        error_kind,
    }
}

/// Build a retry hint with documented precedence:
/// 1. `Retry-After` delta-seconds or HTTP date.
/// 2. Structured JSON retry fields (`retry_after`, `retry_after_seconds`, ...).
/// 3. Human text (`Try again in 2h 30m`, `Retry in 3h`).
/// 4. Configured fallback.
pub fn retry_hint(
    headers: &HeaderMap,
    message: &str,
    body: Option<&Value>,
    fallback: Duration,
) -> RetryHint {
    if let Some(duration) = headers
        .get("retry-after")
        .and_then(|header| header.to_str().ok())
        .and_then(parse_retry_after_header)
    {
        return RetryHint {
            duration,
            source: RetryHintSource::RetryAfterHeader,
        };
    }
    if let Some(duration) = body.and_then(structured_retry_duration) {
        return RetryHint {
            duration,
            source: RetryHintSource::StructuredJson,
        };
    }
    if let Some(duration) = parse_retry_duration(message) {
        return RetryHint {
            duration,
            source: RetryHintSource::HumanText,
        };
    }
    RetryHint {
        duration: fallback.min(MAX_PARSED_COOLDOWN),
        source: RetryHintSource::Fallback,
    }
}

/// Structured JSON retry fields (used when the error body is valid JSON).
pub fn structured_retry_duration(value: &Value) -> Option<Duration> {
    match value {
        Value::Object(object) => {
            for (name, value) in object {
                let lower = name.to_ascii_lowercase().replace(['-', '_'], "");
                let milliseconds = matches!(lower.as_str(), "retryafterms" | "retryinms");
                if matches!(
                    lower.as_str(),
                    "retryafter"
                        | "retryafterseconds"
                        | "retryin"
                        | "retryinseconds"
                        | "retryafterms"
                        | "retryinms"
                        | "cooldown"
                        | "cooldownseconds"
                ) {
                    let parsed = match value {
                        Value::Number(number) => number.as_u64().and_then(|amount| {
                            if milliseconds {
                                duration_with_cap(Duration::from_millis(amount))
                            } else {
                                duration_with_cap(Duration::from_secs(amount))
                            }
                        }),
                        Value::String(text) => parse_retry_duration(text),
                        _ => None,
                    };
                    if parsed.is_some() {
                        return parsed;
                    }
                }
            }
            object.values().find_map(structured_retry_duration)
        }
        Value::Array(values) => values.iter().find_map(structured_retry_duration),
        _ => None,
    }
}

/// Progressive cooldown for a generic 429 that carries no authoritative
/// `Retry-After`: `initial × 2^streak` with multiplicative jitter in
/// [1.0, 1.5), capped at `max`. First generic 429 ≈ 5s, then ≈10s, 20s, 40s,
/// capped at 60s (with the documented defaults) — instead of the previous
/// fixed 60s that made one transient TPM limit look like a minute-long
/// outage.
pub fn fallback_rate_limit_cooldown(
    streak: u32,
    initial: Duration,
    max: Duration,
    jitter: f64,
) -> Duration {
    let step = streak.min(4);
    let base = initial
        .as_millis()
        .saturating_mul(1u128 << step)
        .min(max.as_millis());
    let jittered = base as f64 * (1.0 + 0.5 * jitter.clamp(0.0, 1.0));
    Duration::from_millis((jittered as u64).min(max.as_millis() as u64))
}

pub fn parse_retry_after_header(input: &str) -> Option<Duration> {
    let input = input.trim();
    if let Ok(seconds) = input.parse::<u64>() {
        return duration_with_cap(Duration::from_secs(seconds));
    }
    let when = httpdate::parse_http_date(input).ok()?;
    let duration = when.duration_since(SystemTime::now()).unwrap_or_default();
    duration_with_cap(duration)
}

/// Parse a duration embedded in common rate-limit text, e.g.
/// `Try again in 2h 30m`, `Retry in 3h`, `Retry after 500ms`. Accepts
/// `d/h/m/s/ms`, combines components, rejects negative or overflowing input,
/// and never panics.
pub fn parse_retry_duration(input: &str) -> Option<Duration> {
    let lower = input.to_ascii_lowercase();
    let mut candidate = lower.as_str();
    for marker in ["try again in", "retry after", "retry in"] {
        if let Some(position) = lower.find(marker) {
            candidate = &lower[position + marker.len()..];
            break;
        }
    }
    candidate = candidate.trim_start();
    if candidate.starts_with('-') {
        return None;
    }

    let bytes = candidate.as_bytes();
    let mut index = 0usize;
    let mut total_ms = 0u128;
    let mut components = 0usize;
    while index < bytes.len() {
        while index < bytes.len() && (bytes[index].is_ascii_whitespace() || bytes[index] == b',') {
            index += 1;
        }
        if index >= bytes.len() || !bytes[index].is_ascii_digit() {
            break;
        }
        let number_start = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
        let number = candidate[number_start..index].parse::<u128>().ok()?;
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        let (unit_ms, unit_len) = if candidate[index..].starts_with("ms") {
            (1u128, 2usize)
        } else if let Some(unit) = bytes.get(index).copied() {
            match unit {
                b'd' => (86_400_000, 1),
                b'h' => (3_600_000, 1),
                b'm' => (60_000, 1),
                b's' => (1_000, 1),
                _ => break,
            }
        } else {
            break;
        };
        let part = number.checked_mul(unit_ms)?;
        total_ms = total_ms.checked_add(part)?;
        components = components.saturating_add(1);
        index = index.saturating_add(unit_len);
    }
    if components == 0 {
        return None;
    }
    let total_ms = u64::try_from(total_ms).ok()?;
    duration_with_cap(Duration::from_millis(total_ms))
}

/// Strong, unambiguous quota-exhaustion evidence in an upstream error body.
///
/// Deliberately conservative: a false positive cools an entire quota group
/// and opens the circuit, while a false negative merely costs one extra
/// attempt on another credential. Only quota-scoped combinations qualify —
/// never a bare "exhausted", "balance", "quota", "配额", or Google-style
/// code 8 (`RESOURCE_EXHAUSTED`), all of which describe ordinary rate
/// limiting or any exhausted server-side resource just as often as billing
/// exhaustion. Chinese markers are accepted only as full explicit
/// combinations (额度/积分/余额 + 不足/耗尽), never bare nouns.
fn is_explicit_quota_exhaustion(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    [
        // The documented Token Plan marker (official FAQ distinguishes this
        // from ordinary 429 rate limiting).
        "free_quota_exhausted",
        "free quota exhausted",
        // Quota + exhausted/exceeded combinations.
        "quota exhausted",
        "quota_exhausted",
        "quota has been exhausted",
        "quota exceeded",
        "insufficient quota",
        // Explicit Chinese combinations (not bare 配额/余额).
        "额度已耗尽",
        "积分已耗尽",
        "积分不足",
        "余额不足",
    ]
    .iter()
    .any(|phrase| lower.contains(phrase))
}

/// Bounded, credential-free error-kind marker for logs: the Anthropic-style
/// `error.type`, or the control-plane `error_key`, if present.
fn error_kind_marker(value: &Value) -> Option<&str> {
    let object = value.as_object()?;
    if let Some(kind) = object
        .get("error")
        .and_then(|error| error.get("type"))
        .and_then(Value::as_str)
    {
        return Some(kind);
    }
    object.get("error_key").and_then(Value::as_str)
}

fn numeric_error_code(value: &Value) -> Option<i64> {
    let error = value.get("error")?;
    let code = error.get("code")?.as_i64()?;
    Some(code)
}

fn extract_error_message(value: &Value) -> Option<String> {
    if let Some(text) = value.as_str() {
        return Some(text.to_owned());
    }
    let object = value.as_object()?;
    for key in ["message", "detail", "error_description"] {
        if let Some(text) = object.get(key).and_then(Value::as_str) {
            return Some(text.to_owned());
        }
    }
    object.get("error").and_then(extract_error_message)
}

fn duration_with_cap(duration: Duration) -> Option<Duration> {
    (duration <= MAX_PARSED_COOLDOWN).then_some(duration)
}

pub fn transport_error_class(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connect"
    } else if error.is_body() || error.is_decode() {
        "body"
    } else if error.is_request() {
        "request"
    } else {
        "transport"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use proptest::prelude::*;

    fn classify_status(status: u16, body: &[u8]) -> ClassifiedUpstreamError {
        classify_upstream_error(
            StatusCode::from_u16(status).unwrap(),
            &HeaderMap::new(),
            body,
            Duration::from_secs(60),
        )
    }

    #[test]
    fn parses_required_human_formats() {
        assert_eq!(
            parse_retry_duration("Try again in 2h 30m"),
            Some(Duration::from_secs(9_000))
        );
        assert_eq!(
            parse_retry_duration("Try again in 23h 17m"),
            Some(Duration::from_secs(83_820))
        );
        assert_eq!(
            parse_retry_duration("Retry after 30ms"),
            Some(Duration::from_millis(30))
        );
        assert_eq!(
            parse_retry_duration("TRY AGAIN IN 1D 3H 59M 9S"),
            Some(Duration::from_secs(100_749))
        );
        assert_eq!(parse_retry_duration("Try again"), None);
        assert_eq!(parse_retry_duration("Try again in -1h"), None);
        assert_eq!(parse_retry_duration("Try again in 999999999999h"), None);
        assert_eq!(parse_retry_duration(""), None);
    }

    #[test]
    fn direct_429_is_rate_limited_and_uses_retry_after_first() {
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", HeaderValue::from_static("17"));
        let classified = classify_upstream_error(
            StatusCode::TOO_MANY_REQUESTS,
            &headers,
            br#"{"error":{"code":8,"message":"RESOURCE_EXHAUSTED","retry_after":99}}"#,
            Duration::from_secs(60),
        );
        let hint = classified.retry_hint.expect("429 needs a hint");
        assert_eq!(hint.duration, Duration::from_secs(17));
        assert_eq!(hint.source, RetryHintSource::RetryAfterHeader);
    }

    #[test]
    fn quota_wording_promotes_to_quota_exhausted() {
        let classified = classify_status(
            429,
            br#"{"error":{"message":"FREE_QUOTA_EXHAUSTED for today"}}"#,
        );
        assert_eq!(classified.class, UpstreamErrorClass::QuotaExhausted);
        assert!(classified.quota_group_exhausted);
        assert_eq!(
            classified.reason,
            ClassificationReason::ExplicitQuotaEvidence
        );
    }

    #[test]
    fn plain_429_is_only_rate_limited_not_quota() {
        let classified = classify_status(429, br#"{"error":{"message":"slow down"}}"#);
        assert_eq!(classified.class, UpstreamErrorClass::RateLimited);
        assert!(!classified.quota_group_exhausted);
    }

    /// §31 matrix: generic 429s — including Google-style code 8
    /// (RESOURCE_EXHAUSTED) without quota wording — must classify as
    /// RateLimited, never QuotaExhausted.
    #[test]
    fn generic_429s_are_rate_limited_not_quota() {
        for (label, body) in [
            ("empty body", ""),
            (
                "code 8 RESOURCE_EXHAUSTED",
                r#"{"error":{"code":8,"message":"RESOURCE_EXHAUSTED"}}"#,
            ),
            (
                "code 8 busy",
                r#"{"error":{"code":8,"message":"Server is busy"}}"#,
            ),
            ("too many requests", "Too many requests"),
            ("rate limit exceeded", "rate limit exceeded"),
            ("server resources exhausted", "server resources exhausted"),
            ("bare RESOURCE_EXHAUSTED", "RESOURCE_EXHAUSTED"),
            (
                "quota limit temporarily reached",
                "quota limit temporarily reached",
            ),
            (
                "body merely contains 429",
                r#"{"error":{"message":"request 429123 failed on worker 429"}}"#,
            ),
        ] {
            let classified = classify_status(429, body.as_bytes());
            assert_eq!(classified.class, UpstreamErrorClass::RateLimited, "{label}");
            assert!(!classified.quota_group_exhausted, "{label}");
            assert!(
                !matches!(
                    classified.reason,
                    ClassificationReason::ExplicitQuotaEvidence
                ),
                "{label}"
            );
        }
        // A bare RESOURCE_EXHAUSTED / code 8 gets the dedicated reason so
        // logs can explain why it stayed a rate limit.
        let code8 = classify_status(
            429,
            br#"{"error":{"code":8,"message":"RESOURCE_EXHAUSTED"}}"#,
        );
        assert_eq!(code8.reason, ClassificationReason::ResourceExhaustedCode);
        assert_eq!(code8.numeric_code, Some(8));
        let plain = classify_status(429, b"slow down");
        assert_eq!(plain.reason, ClassificationReason::Http429RateLimit);
    }

    /// §31 matrix: explicit quota-exhaustion evidence — and only such
    /// evidence — promotes a 429 to QuotaExhausted.
    #[test]
    fn explicit_quota_evidence_promotes_to_quota_exhausted() {
        for (label, body) in [
            (
                "FREE_QUOTA_EXHAUSTED",
                r#"{"error":{"message":"FREE_QUOTA_EXHAUSTED"}}"#,
            ),
            (
                "free_quota_exhausted",
                r#"{"error":{"message":"free_quota_exhausted"}}"#,
            ),
            (
                "quota exhausted",
                r#"{"error":{"message":"your quota has been exhausted"}}"#,
            ),
            (
                "quota exceeded",
                r#"{"error":{"message":"quota exceeded for this plan"}}"#,
            ),
            (
                "code 8 + FREE_QUOTA_EXHAUSTED",
                r#"{"error":{"code":8,"message":"FREE_QUOTA_EXHAUSTED"}}"#,
            ),
            (
                "insufficient quota",
                r#"{"error":{"message":"insufficient quota remaining"}}"#,
            ),
            ("中文 额度已耗尽", r#"{"error":{"message":"额度已耗尽"}}"#),
            ("中文 积分不足", r#"{"error":{"message":"积分不足"}}"#),
            ("中文 余额不足", r#"{"error":{"message":"余额不足"}}"#),
        ] {
            let classified = classify_status(429, body.as_bytes());
            assert_eq!(
                classified.class,
                UpstreamErrorClass::QuotaExhausted,
                "{label}"
            );
            assert!(classified.quota_group_exhausted, "{label}");
            assert_eq!(
                classified.reason,
                ClassificationReason::ExplicitQuotaEvidence,
                "{label}"
            );
        }
    }

    /// Bare nouns and ordinary rate-limit wording must never suffice.
    #[test]
    fn bare_nouns_never_mean_quota_exhaustion() {
        for body in ["exhausted", "balance", "配额", "余额", "quota"] {
            let classified = classify_status(429, body.as_bytes());
            assert_eq!(classified.class, UpstreamErrorClass::RateLimited, "{body}");
        }
    }

    /// Explicit quota evidence under an unexpected (non-5xx, non-mapped)
    /// status still promotes, with its own observability reason. Mapped
    /// statuses — including all 5xx — keep their status-based class: a
    /// spurious body on a transient 5xx must never cool an account.
    #[test]
    fn quota_evidence_off_429_status_is_recorded() {
        let classified = classify_status(505, br#"{"error":{"message":"FREE_QUOTA_EXHAUSTED"}}"#);
        assert_eq!(classified.class, UpstreamErrorClass::QuotaExhausted);
        assert_eq!(
            classified.reason,
            ClassificationReason::ExplicitQuotaEvidenceOffStatus
        );
        let transient = classify_status(500, br#"{"error":{"message":"FREE_QUOTA_EXHAUSTED"}}"#);
        assert_eq!(transient.class, UpstreamErrorClass::ServerTransient);
    }

    /// The bounded error-kind marker must be extracted for logs.
    #[test]
    fn error_kind_marker_is_extracted() {
        let anthropic = classify_status(
            429,
            br#"{"type":"error","error":{"type":"rate_limit_error","message":"busy"}}"#,
        );
        assert_eq!(anthropic.error_kind.as_deref(), Some("rate_limit_error"));
        let control_plane = classify_status(
            401,
            br#"{"code":16,"error_key":"auth_type_disabled","message":"Unauthenticated"}"#,
        );
        assert_eq!(
            control_plane.error_kind.as_deref(),
            Some("auth_type_disabled")
        );
    }

    #[test]
    fn text_containing_429_never_classifies_by_itself() {
        for (status, body) in [
            (500u16, "internal error referencing request 429123"),
            (502, "bad gateway (upstream said 429)"),
            (500, "error 429"),
        ] {
            let classified = classify_status(status, body.as_bytes());
            assert_eq!(
                classified.class,
                UpstreamErrorClass::ServerTransient,
                "{body}"
            );
            assert!(classified.retry_hint.is_none());
        }
    }

    #[test]
    fn non_retryable_classes_are_mapped() {
        assert_eq!(
            classify_status(
                401,
                br#"{"error":{"code":16,"message":"Authorization Not Found"}}"#
            )
            .class,
            UpstreamErrorClass::Authentication
        );
        assert_eq!(
            classify_status(400, br#"{"type":"error","error":{"type":"invalid_request_error","message":"invalid arguments"}}"#).class,
            UpstreamErrorClass::InvalidRequest
        );
        assert_eq!(
            classify_status(
                404,
                br#"{"error":{"type":"not_found_error","message":"model is not found"}}"#
            )
            .class,
            UpstreamErrorClass::NotFound
        );
        assert_eq!(
            classify_status(503, b"overloaded").class,
            UpstreamErrorClass::ServerTransient
        );
    }

    #[test]
    fn progressive_fallback_cooldown_ladder() {
        let initial = Duration::from_secs(5);
        let max = Duration::from_secs(60);
        // Deterministic jitter bounds: streak ladder is 5, 10, 20, 40, then
        // capped at 60 regardless of further streaks.
        assert_eq!(
            fallback_rate_limit_cooldown(0, initial, max, 0.0),
            Duration::from_secs(5)
        );
        assert_eq!(
            fallback_rate_limit_cooldown(1, initial, max, 0.0),
            Duration::from_secs(10)
        );
        assert_eq!(
            fallback_rate_limit_cooldown(2, initial, max, 0.0),
            Duration::from_secs(20)
        );
        assert_eq!(
            fallback_rate_limit_cooldown(3, initial, max, 0.0),
            Duration::from_secs(40)
        );
        assert_eq!(
            fallback_rate_limit_cooldown(4, initial, max, 0.0),
            Duration::from_secs(60)
        );
        assert_eq!(
            fallback_rate_limit_cooldown(9, initial, max, 0.0),
            Duration::from_secs(60)
        );
        // Jitter only ever scales up within [1.0, 1.5) and never past max.
        let jittered = fallback_rate_limit_cooldown(0, initial, max, 1.0);
        assert_eq!(jittered, Duration::from_millis(7_500));
        let capped = fallback_rate_limit_cooldown(4, initial, max, 1.0);
        assert_eq!(capped, Duration::from_secs(60));
        assert!(
            fallback_rate_limit_cooldown(0, initial, max, 0.4) >= Duration::from_secs(5)
                && fallback_rate_limit_cooldown(0, initial, max, 0.4) <= Duration::from_secs(8),
            "first fallback cooldown must stay in the ~5s band"
        );
    }

    #[test]
    fn retry_after_http_date_is_supported() {
        let when = httpdate::fmt_http_date(SystemTime::now() + Duration::from_secs(120));
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", HeaderValue::from_str(&when).unwrap());
        // (hint source assertion below)
        let hint = retry_hint(&headers, "", None, Duration::from_secs(1));
        assert_eq!(hint.source, RetryHintSource::RetryAfterHeader);
        assert!(hint.duration >= Duration::from_secs(100));
    }

    #[test]
    fn malformed_retry_hints_fall_back_safely() {
        let hint = retry_hint(&HeaderMap::new(), "nonsense", None, Duration::from_secs(9));
        assert_eq!(hint.duration, Duration::from_secs(9));
        assert_eq!(hint.source, RetryHintSource::Fallback);
    }

    proptest! {
        #[test]
        fn arbitrary_text_never_panics(input in any::<String>()) {
            let _ = parse_retry_duration(&input);
            let _ = classify_upstream_error(
                StatusCode::BAD_GATEWAY,
                &HeaderMap::new(),
                input.as_bytes(),
                Duration::from_secs(1),
            );
        }

        #[test]
        fn arbitrary_bytes_never_panic(input in proptest::collection::vec(any::<u8>(), 0..4096)) {
            let _ = classify_upstream_error(
                StatusCode::TOO_MANY_REQUESTS,
                &HeaderMap::new(),
                &input,
                Duration::from_secs(1),
            );
        }
    }
}
