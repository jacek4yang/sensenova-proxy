//! Anthropic-style error envelopes and HTTP response helpers.

use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

/// A locally detected protocol problem in a client request.
#[derive(Debug, Clone)]
pub struct ProtocolError {
    pub error_type: &'static str,
    pub message: String,
}

impl ProtocolError {
    pub fn invalid(message: impl Into<String>) -> Self {
        Self {
            error_type: "invalid_request_error",
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ProtocolError {}

pub fn error_envelope(error_type: &str, message: impl Into<String>, request_id: &str) -> Value {
    json!({
        "type": "error",
        "error": {"type": error_type, "message": message.into()},
        "request_id": request_id
    })
}

/// Map an HTTP status to the Anthropic error type used by this gateway.
pub fn error_type_for_status(status: StatusCode) -> &'static str {
    match status.as_u16() {
        400 | 405 | 413 | 422 => "invalid_request_error",
        404 => "not_found_error",
        401 => "authentication_error",
        403 => "permission_error",
        429 => "rate_limit_error",
        500 | 502 | 503 | 504 | 529 => "api_error",
        _ => "api_error",
    }
}

pub fn anthropic_error(
    status: StatusCode,
    error_type: &str,
    message: impl Into<String>,
    request_id: &str,
) -> Response {
    json_response(
        status,
        error_envelope(error_type, message, request_id),
        request_id,
    )
}

pub fn json_response(status: StatusCode, value: Value, request_id: &str) -> Response {
    let mut response = (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        value.to_string(),
    )
        .into_response();
    insert_request_id(response.headers_mut(), request_id);
    response
}

pub fn insert_request_id(headers: &mut axum::http::HeaderMap, request_id: &str) {
    if let Ok(value) = HeaderValue::try_from(request_id) {
        headers.insert("x-request-id", value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_contains_type_error_and_request_id() {
        let value = error_envelope("rate_limit_error", "slow down", "req_1");
        assert_eq!(value["type"], "error");
        assert_eq!(value["error"]["type"], "rate_limit_error");
        assert_eq!(value["error"]["message"], "slow down");
        assert_eq!(value["request_id"], "req_1");
    }

    #[test]
    fn status_mapping_covers_documented_cases() {
        assert_eq!(
            error_type_for_status(StatusCode::BAD_REQUEST),
            "invalid_request_error"
        );
        assert_eq!(
            error_type_for_status(StatusCode::UNAUTHORIZED),
            "authentication_error"
        );
        assert_eq!(
            error_type_for_status(StatusCode::TOO_MANY_REQUESTS),
            "rate_limit_error"
        );
        assert_eq!(error_type_for_status(StatusCode::BAD_GATEWAY), "api_error");
    }
}
