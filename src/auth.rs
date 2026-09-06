//! Local gateway authentication. The gateway key never travels upstream; the
//! SenseNova credential is attached exclusively by the upstream layer.

use axum::http::{HeaderMap, header};

/// Constant-time string equality so response timing cannot leak the key.
pub fn constant_time_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    let mut difference = left.len() ^ right.len();
    let length = left.len().max(right.len());
    for index in 0..length {
        difference |= usize::from(
            left.get(index).copied().unwrap_or_default()
                ^ right.get(index).copied().unwrap_or_default(),
        );
    }
    difference == 0
}

/// Extract the presented gateway credential from either accepted form:
/// `Authorization: Bearer <key>` or `x-api-key: <key>`.
pub fn presented_gateway_key(headers: &HeaderMap) -> Option<&str> {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    let x_api_key = headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok());
    bearer.or(x_api_key)
}

pub fn gateway_key_is_valid(headers: &HeaderMap, gateway_key: &str) -> bool {
    presented_gateway_key(headers).is_some_and(|candidate| constant_time_eq(candidate, gateway_key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn accepts_bearer_and_x_api_key() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer secret"),
        );
        assert!(gateway_key_is_valid(&headers, "secret"));

        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_static("secret"));
        assert!(gateway_key_is_valid(&headers, "secret"));

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer wrong"),
        );
        assert!(!gateway_key_is_valid(&headers, "secret"));
        assert!(!gateway_key_is_valid(&HeaderMap::new(), "secret"));
    }

    #[test]
    fn constant_time_eq_is_correct() {
        assert!(constant_time_eq("abc", "abc"));
        assert!(!constant_time_eq("abc", "abd"));
        assert!(!constant_time_eq("abc", "abcd"));
        assert!(!constant_time_eq("", "a"));
        assert!(constant_time_eq("", ""));
    }
}
