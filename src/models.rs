//! Deterministic local model aliasing, `/v1/models`, and token counting.
//! None of these touch the network.

use axum::http::{HeaderMap, HeaderValue};
use serde_json::{Value, json};

use crate::config::Config;
use crate::error::ProtocolError;

pub fn model_ids(config: &Config) -> Vec<String> {
    let mut models = vec![config.models.default.clone()];
    // Virtual routing aliases first so Claude Code sees them in `/v1/models`.
    models.extend(config.profiles().keys().cloned());
    models.extend(config.models.aliases.keys().cloned());
    models.extend(config.models.aliases.values().cloned());
    for profile in config.profiles().values() {
        for tier in &profile.tiers {
            models.extend(tier.models.iter().cloned());
        }
    }
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
/// Anthropic Messages request body, then apply the observed upstream
/// compatibility fixups. Everything else passes through unchanged.
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
    if let Some(thinking) = object.get_mut("thinking") {
        adapt_thinking_for_model(thinking, upstream_model);
    }
    // Observed upstream quirk (2026-09-22): `output_config` with a structured
    // `format` (Claude Code's session-title call) is rejected with HTTP 400
    // "inference request is invalid" by the fast models. The proxy never
    // commits to a structured-output contract, so the whole field is a
    // client-side hint and is dropped.
    object.remove("output_config");
    fold_system_messages(object);
    Ok(())
}

/// Observed Claude Code 2.1.270 behaviour (2026-09-22): it can emit
/// `{"role":"system", "content":[...]}` *inside* the `messages` array. That
/// role is not part of the Anthropic Messages schema, and SenseNova rejects
/// such requests with HTTP 400 "inference request is invalid".
///
/// The system-role messages are folded into the top-level `system` field —
/// the canonical way to carry system context — preserving block order: the
/// existing system blocks come first, then each folded message's text blocks
/// (cache_control markers preserved as-is).
fn fold_system_messages(object: &mut serde_json::Map<String, Value>) {
    use serde_json::json;

    let has_system_message = object
        .get("messages")
        .and_then(Value::as_array)
        .is_some_and(|messages| {
            messages
                .iter()
                .any(|message| message.get("role").and_then(Value::as_str) == Some("system"))
        });
    if !has_system_message {
        return;
    }

    let mut system_blocks: Vec<Value> = match object.get_mut("system") {
        Some(Value::Array(blocks)) => blocks.clone(),
        Some(Value::String(text)) => {
            vec![json!({"type": "text", "text": text.clone()})]
        }
        _ => Vec::new(),
    };
    let Some(messages) = object.get_mut("messages").and_then(Value::as_array_mut) else {
        return;
    };
    let mut carried: Vec<Value> = Vec::with_capacity(messages.len());
    for message in messages.drain(..) {
        if message.get("role").and_then(Value::as_str) == Some("system") {
            match message.get("content") {
                Some(Value::Array(blocks)) => system_blocks.extend(blocks.iter().cloned()),
                Some(Value::String(text)) => {
                    system_blocks.push(json!({"type": "text", "text": text.clone()}))
                }
                _ => {}
            }
        } else {
            carried.push(message);
        }
    }
    *messages = carried;
    if !system_blocks.is_empty() {
        object.insert("system".into(), Value::Array(system_blocks));
    }
}

/// Observed upstream quirk (2026-09-22): `glm-5.2` rejects
/// `thinking: {"type":"adaptive"}` with HTTP 400 "inference request is
/// invalid", while `deepseek-v4-pro` / `kimi-k3` accept it and every model
/// accepts `{"type":"disabled"}` and `{"type":"enabled","budget_tokens":…}`.
///
/// Adaptive means "the model decides", so downgrading to explicit
/// `disabled` on `glm-5.2` is the safe, semantics-preserving fallback: the
/// request succeeds and no unsolicited thinking blocks are produced.
/// Everything else (including `display` and unknown extensions) is forwarded
/// untouched — the 400 comes from the `adaptive` type itself, not the extra
/// keys, and other models tolerate them.
fn adapt_thinking_for_model(thinking: &mut Value, upstream_model: &str) {
    if upstream_model != "glm-5.2" {
        return;
    }
    let is_adaptive = thinking
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "adaptive");
    if is_adaptive && let Some(object) = thinking.as_object_mut() {
        object.insert("type".into(), Value::String("disabled".into()));
    }
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
    fn glm_adaptive_thinking_is_downgraded_to_disabled() {
        let mut body = json!({
            "model": "claude-coding-hard",
            "messages": [{"role": "user", "content": "hi"}],
            "thinking": {"type": "adaptive", "display": "omitted"}
        });
        normalize_messages_request(&mut body, "glm-5.2").unwrap();
        assert_eq!(body["thinking"]["type"], "disabled");
        assert_eq!(
            body["thinking"]["display"], "omitted",
            "extra keys are preserved"
        );

        // Other models keep adaptive untouched.
        let mut body = json!({
            "model": "x",
            "messages": [{"role": "user", "content": "hi"}],
            "thinking": {"type": "adaptive"}
        });
        normalize_messages_request(&mut body, "deepseek-v4-pro").unwrap();
        assert_eq!(body["thinking"]["type"], "adaptive");

        // Explicit enabled passes through everywhere.
        let mut body = json!({
            "model": "x",
            "messages": [{"role": "user", "content": "hi"}],
            "thinking": {"type": "enabled", "budget_tokens": 1024}
        });
        normalize_messages_request(&mut body, "glm-5.2").unwrap();
        assert_eq!(body["thinking"]["type"], "enabled");

        // No thinking field: nothing is injected.
        let mut body = json!({"model": "x", "messages": [{"role": "user", "content": "hi"}]});
        normalize_messages_request(&mut body, "glm-5.2").unwrap();
        assert!(body.get("thinking").is_none());
    }

    #[test]
    fn system_role_messages_are_folded_into_the_system_field() {
        // Claude Code 2.1.270 can emit a system-role message inside `messages`;
        // SenseNova rejects it with 400. It must be folded into `system`.
        let mut body = json!({
            "model": "x",
            "system": [{"type": "text", "text": "base"}],
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "system", "content": [{"type": "text", "text": "extra context"}]},
                {"role": "assistant", "content": "hello"}
            ]
        });
        normalize_messages_request(&mut body, "glm-5.2").unwrap();
        let system = body["system"].as_array().unwrap();
        let texts: Vec<&str> = system
            .iter()
            .map(|block| block["text"].as_str().unwrap_or(""))
            .collect();
        assert_eq!(
            texts,
            vec!["base", "extra context"],
            "folded after existing"
        );
        // The system-role message is gone; user and assistant remain in order.
        let roles: Vec<&str> = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|message| message["role"].as_str().unwrap_or(""))
            .collect();
        assert_eq!(roles, vec!["user", "assistant"]);

        // String-content system message folds too.
        let mut body = json!({
            "model": "x",
            "messages": [
                {"role": "system", "content": "inline"},
                {"role": "user", "content": "hi"}
            ]
        });
        normalize_messages_request(&mut body, "glm-5.2").unwrap();
        assert_eq!(body["system"][0]["text"], "inline");
        assert_eq!(body["messages"].as_array().unwrap().len(), 1);

        // No system-role message: `system` untouched, messages unchanged.
        let mut body = json!({
            "model": "x",
            "system": "keep",
            "messages": [{"role": "user", "content": "hi"}]
        });
        normalize_messages_request(&mut body, "glm-5.2").unwrap();
        assert_eq!(body["system"], "keep");
        assert_eq!(body["messages"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn output_config_is_dropped() {
        // Claude Code's structured-output hint is rejected by the upstream
        // fast models; the proxy never honours it, so drop the field.
        let mut body = json!({
            "model": "x",
            "output_config": {"effort": "high", "format": {"type": "json_schema"}},
            "messages": [{"role": "user", "content": "hi"}]
        });
        normalize_messages_request(&mut body, "deepseek-v4-flash").unwrap();
        assert!(body.get("output_config").is_none());

        // Absent stays absent; no error on models without the quirk either.
        let mut body = json!({"model": "x", "messages": [{"role": "user", "content": "hi"}]});
        normalize_messages_request(&mut body, "glm-5.2").unwrap();
        assert!(body.get("output_config").is_none());
    }

    #[test]
    fn model_ids_include_profile_aliases_and_pool_models() {
        let config = test_config();
        let ids = model_ids(&config);
        // Virtual routing aliases are advertised to Claude Code.
        assert!(ids.contains(&"claude-coding-hard".to_owned()));
        assert!(ids.contains(&"claude-coding-fast".to_owned()));
        // Configured aliases and their targets remain present.
        assert!(ids.contains(&"claude-sensenova".to_owned()));
        assert!(ids.contains(&"sensenova-6.8-flash-lite".to_owned()));
        // Every model reachable through a profile is listed.
        for model in ["glm-5.2", "deepseek-v4-pro", "kimi-k3", "deepseek-v4-flash"] {
            assert!(
                ids.contains(&model.to_owned()),
                "missing {model} in {ids:?}"
            );
        }
        // The list is sorted and deduplicated.
        let mut sorted = ids.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(ids, sorted);
    }

    #[test]
    fn configured_profiles_are_reflected_in_the_catalog() {
        let mut config = test_config();
        let mut profiles = crate::config::Profiles::new();
        profiles.insert(
            "only-this".into(),
            crate::config::ProfileConfig {
                latency_optimized: true,
                allow_lower_tier_on_unavailable: false,
                tiers: vec![crate::config::TierConfig {
                    models: vec!["custom-model".into()],
                }],
            },
        );
        config.routing.profiles = profiles;
        let ids = model_ids(&config);
        assert!(ids.contains(&"only-this".to_owned()));
        assert!(ids.contains(&"custom-model".to_owned()));
        // A configured profile section replaces the built-in pair.
        assert!(!ids.contains(&"claude-coding-hard".to_owned()));
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
