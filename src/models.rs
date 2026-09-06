//! Deterministic local model aliasing, `/v1/models`, and token counting.
//! None of these touch the network.

use axum::http::{HeaderMap, HeaderValue};
use serde_json::{Value, json};

use crate::config::Config;
use crate::error::ProtocolError;

/// Resolve a client-requested model name to the upstream model.
///
/// `map_unknown_to_default` protects against Claude Code's built-in model
/// names (`claude-*`, e.g. the small fast model) 404-ing upstream. Any other
/// unknown name — such as an explicit SenseNova catalog ID like
/// `deepseek-v4-pro` — passes through unchanged so it can be served (or
/// honestly 404) upstream instead of being silently rewritten to the default.
pub fn resolve_model(config: &Config, requested: &str) -> String {
    if let Some(target) = config.models.aliases.get(requested) {
        return target.clone();
    }
    if requested == config.models.default {
        return requested.to_owned();
    }
    if config.models.map_unknown_to_default && requested.starts_with("claude") {
        config.models.default.clone()
    } else {
        requested.to_owned()
    }
}

pub fn model_ids(config: &Config) -> Vec<String> {
    let mut models = vec![config.models.default.clone()];
    models.extend(config.models.aliases.keys().cloned());
    models.extend(config.models.aliases.values().cloned());
    models.sort();
    models.dedup();
    models
}

/// Anthropic-style model list when the client speaks Anthropic
/// (`anthropic-version` header present), OpenAI style otherwise.
pub fn models_response(config: &Config, headers: &HeaderMap) -> Value {
    let ids = model_ids(config);
    if headers.contains_key("anthropic-version") {
        let data = ids
            .iter()
            .map(|id| {
                json!({
                    "type": "model",
                    "id": id,
                    "display_name": id,
                    "created_at": "1970-01-01T00:00:00Z"
                })
            })
            .collect::<Vec<_>>();
        json!({
            "data": data,
            "has_more": false,
            "first_id": ids.first(),
            "last_id": ids.last()
        })
    } else {
        let data = ids
            .iter()
            .map(|id| json!({"id": id, "object": "model", "created": 0, "owned_by": "sensenova-proxy"}))
            .collect::<Vec<_>>();
        json!({"object": "list", "data": data})
    }
}

/// Local conservative token estimate: `ceil(serialized prompt bytes / 4)`,
/// over system, tools, tool_choice and messages. This is explicitly an
/// approximation (SenseNova exposes no count endpoint) and prefers
/// overestimating; the response marks itself with
/// `x-sensenova-proxy-token-count: approximate`.
pub fn approximate_input_tokens(bytes: &[u8]) -> Result<usize, ProtocolError> {
    let input: Value = serde_json::from_slice(bytes)
        .map_err(|error| ProtocolError::invalid(format!("invalid JSON: {error}")))?;
    let object = input
        .as_object()
        .ok_or_else(|| ProtocolError::invalid("request body must be a JSON object"))?;
    if !object
        .get("model")
        .and_then(Value::as_str)
        .is_some_and(|model| !model.is_empty())
    {
        return Err(ProtocolError::invalid("model must be a non-empty string"));
    }
    if !object.get("messages").is_some_and(Value::is_array) {
        return Err(ProtocolError::invalid("messages must be an array"));
    }
    let mut total = 0usize;
    for field in ["messages", "system", "tools", "tool_choice"] {
        if let Some(value) = object.get(field) {
            total = total.saturating_add(estimated_json_bytes(value));
        }
    }
    Ok(total.div_ceil(4))
}

fn estimated_json_bytes(value: &Value) -> usize {
    match value {
        Value::Null => 4,
        Value::Bool(true) => 4,
        Value::Bool(false) => 5,
        Value::Number(number) => number.to_string().len(),
        Value::String(text) => text.len().saturating_add(2),
        Value::Array(values) => values.iter().fold(2usize, |total, value| {
            total
                .saturating_add(1)
                .saturating_add(estimated_json_bytes(value))
        }),
        Value::Object(object) => object.iter().fold(2usize, |total, (name, value)| {
            total
                .saturating_add(name.len())
                .saturating_add(3)
                .saturating_add(estimated_json_bytes(value))
        }),
    }
}

/// Count `tool_use` blocks in a non-stream Anthropic response (metrics only).
pub fn count_tool_use_blocks(value: &Value) -> usize {
    value
        .get("content")
        .and_then(Value::as_array)
        .map(|blocks| {
            blocks
                .iter()
                .filter(|block| block.get("type").and_then(Value::as_str) == Some("tool_use"))
                .count()
        })
        .unwrap_or(0)
}

/// Mark a token-count response with the approximation disclosure header.
pub fn insert_approximate_header(headers: &mut HeaderMap) {
    headers.insert(
        "x-sensenova-proxy-token-count",
        HeaderValue::from_static("approximate"),
    );
}

/// Rewrite the model (and inject a default `max_tokens` when absent) on an
/// Anthropic Messages request body. This is the proxy's entire request
/// transformation: everything else passes through unchanged (observed to be
/// tolerated by SenseNova).
pub fn normalize_messages_request(
    body: &mut Value,
    upstream_model: &str,
) -> Result<(), ProtocolError> {
    let object = body
        .as_object_mut()
        .ok_or_else(|| ProtocolError::invalid("request body must be a JSON object"))?;
    match object.get("model").and_then(Value::as_str) {
        Some(model) if !model.is_empty() => {}
        _ => return Err(ProtocolError::invalid("model must be a non-empty string")),
    }
    object.insert("model".into(), Value::String(upstream_model.to_owned()));
    if !object.contains_key("max_tokens") {
        // SenseNova tolerates absence, but Claude Code always sends it; a
        // explicit default keeps non-CC clients working deterministically.
        object.insert("max_tokens".into(), Value::from(8_192));
    }
    if let Some(value) = object.get("max_tokens")
        && (!value.is_u64() || value.as_u64().unwrap_or(0) == 0)
    {
        return Err(ProtocolError::invalid(
            "max_tokens must be a positive integer",
        ));
    }
    if let Some(messages) = object.get("messages") {
        if !messages.is_array() {
            return Err(ProtocolError::invalid("messages must be an array"));
        }
    } else {
        return Err(ProtocolError::invalid("messages must be an array"));
    }
    if let Some(stream) = object.get("stream")
        && !stream.is_boolean()
    {
        return Err(ProtocolError::invalid("stream must be a boolean"));
    }
    Ok(())
}

/// Extract the requested model and stream flag for logging/routing.
pub fn request_summary(body: &Value) -> Option<(String, bool)> {
    let object = body.as_object()?;
    let model = object.get("model")?.as_str()?.to_owned();
    let stream = object
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Some((model, stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_config() -> Config {
        let mut config = Config::default();
        config.server.api_key = "gateway".into();
        config.sensenova_api_keys = vec![crate::config::SensenovaKeyConfig {
            name: "primary".into(),
            api_key: "sk-test".into(),
            enabled: true,
            quota_group: "account-a".into(),
        }];
        config
            .models
            .aliases
            .insert("claude-sensenova".into(), "sensenova-6.8-flash-lite".into());
        config
    }

    #[test]
    fn aliases_resolve_and_unknown_maps_to_default() {
        let config = test_config();
        assert_eq!(
            resolve_model(&config, "claude-sensenova"),
            "sensenova-6.8-flash-lite"
        );
        assert_eq!(
            resolve_model(&config, "claude-3-5-haiku-20241022"),
            "sensenova-6.8-flash-lite"
        );
        assert_eq!(
            resolve_model(&config, "sensenova-6.8-flash-lite"),
            "sensenova-6.8-flash-lite"
        );
        let mut config = test_config();
        config.models.map_unknown_to_default = false;
        assert_eq!(resolve_model(&config, "something-else"), "something-else");
    }

    #[test]
    fn explicit_sensenova_catalog_ids_pass_through_unchanged() {
        // A direct request for a non-default SenseNova catalog model must not
        // be silently rewritten to the default (multi-model support).
        let config = test_config();
        assert_eq!(resolve_model(&config, "deepseek-v4-pro"), "deepseek-v4-pro");
        assert_eq!(resolve_model(&config, "glm-5.2"), "glm-5.2");
        // Claude-family names are the ones protected by the default mapping.
        assert_eq!(
            resolve_model(&config, "claude-sonnet-4-6"),
            "sensenova-6.8-flash-lite"
        );
    }

    #[test]
    fn deepseek_alias_resolves_to_deepseek_catalog_id() {
        let mut config = test_config();
        config
            .models
            .aliases
            .insert("claude-deepseek".into(), "deepseek-v4-pro".into());
        assert_eq!(resolve_model(&config, "claude-deepseek"), "deepseek-v4-pro");
        // And the resolved target is itself stable under resolution.
        assert_eq!(resolve_model(&config, "deepseek-v4-pro"), "deepseek-v4-pro");
    }

    #[test]
    fn token_count_is_byte_based_and_positive() {
        let body = json!({
            "model": "claude-sensenova",
            "messages": [{"role": "user", "content": "hello"}]
        });
        let count = approximate_input_tokens(body.to_string().as_bytes()).unwrap();
        assert!(count > 0);
        assert!(approximate_input_tokens(b"not json").is_err());
        assert!(approximate_input_tokens(br#"{"messages":[]}"#).is_err());
    }

    #[test]
    fn normalization_rewrites_model_and_defaults_max_tokens() {
        let mut body = json!({
            "model": "claude-sensenova",
            "messages": [{"role": "user", "content": "hi"}],
            "unknown_future_field": {"preserve": true}
        });
        normalize_messages_request(&mut body, "sensenova-6.8-flash-lite").unwrap();
        assert_eq!(body["model"], "sensenova-6.8-flash-lite");
        assert_eq!(body["max_tokens"], 8_192);
        assert_eq!(body["unknown_future_field"]["preserve"], true);
    }

    #[test]
    fn normalization_rejects_bad_shapes() {
        let mut body = json!({"model": "", "messages": []});
        assert!(normalize_messages_request(&mut body, "m").is_err());
        let mut body = json!({"model": "x", "messages": "no"});
        assert!(normalize_messages_request(&mut body, "m").is_err());
        let mut body = json!({"model": "x", "messages": [], "stream": "yes"});
        assert!(normalize_messages_request(&mut body, "m").is_err());
        let mut body = json!({"model": "x", "messages": [], "max_tokens": 0});
        assert!(normalize_messages_request(&mut body, "m").is_err());
    }

    #[test]
    fn models_response_adapts_to_protocol() {
        let config = test_config();
        let mut headers = HeaderMap::new();
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        let anthropic = models_response(&config, &headers);
        assert_eq!(anthropic["data"][0]["type"], "model");
        let openai = models_response(&config, &HeaderMap::new());
        assert_eq!(openai["object"], "list");
        assert_eq!(openai["data"][0]["object"], "model");
    }

    #[test]
    fn tool_use_blocks_are_counted() {
        let value = json!({"content": [
            {"type": "text", "text": "a"},
            {"type": "tool_use", "id": "call_1", "name": "t", "input": {}},
            {"type": "tool_use", "id": "call_2", "name": "t", "input": {}}
        ]});
        assert_eq!(count_tool_use_blocks(&value), 2);
        assert_eq!(count_tool_use_blocks(&json!({"content": []})), 0);
    }
}
