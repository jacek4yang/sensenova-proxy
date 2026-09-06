//! File-backed configuration and startup validation.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

pub mod defaults {
    pub const BIND: &str = "127.0.0.1:8789";
    pub const BASE_URL: &str = "https://token.sensenova.cn";
    pub const MESSAGES_PATH: &str = "/v1/messages";
    pub const ANTHROPIC_VERSION: &str = "2023-06-01";
    pub const TIMEOUT_SECS: u64 = 600;
    pub const CONNECT_TIMEOUT_SECS: u64 = 20;
    pub const FIRST_BYTE_TIMEOUT_SECS: u64 = 120;
    pub const MAX_REQUEST_BYTES: usize = 32 * 1024 * 1024;
    pub const MAX_UPSTREAM_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
    pub const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;
    pub const SHUTDOWN_TIMEOUT_SECS: u64 = 30;
    pub const STREAM_PING_SECS: u64 = 15;
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub server: ServerConfig,
    pub upstream: UpstreamConfig,
    pub sensenova_api_keys: Vec<SensenovaKeyConfig>,
    pub models: ModelsConfig,
    pub retry: RetryConfig,
    pub concurrency: ConcurrencyConfig,
    pub circuit: CircuitConfig,
    pub runtime: RuntimeConfig,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    pub bind: String,
    pub api_key: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: defaults::BIND.into(),
            api_key: String::new(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct UpstreamConfig {
    pub base_url: String,
    pub messages_path: String,
    pub anthropic_version: String,
    pub timeout_secs: u64,
    pub connect_timeout_secs: u64,
    pub first_byte_timeout_secs: u64,
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            base_url: defaults::BASE_URL.into(),
            messages_path: defaults::MESSAGES_PATH.into(),
            anthropic_version: defaults::ANTHROPIC_VERSION.into(),
            timeout_secs: defaults::TIMEOUT_SECS,
            connect_timeout_secs: defaults::CONNECT_TIMEOUT_SECS,
            first_byte_timeout_secs: defaults::FIRST_BYTE_TIMEOUT_SECS,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SensenovaKeyConfig {
    pub name: String,
    pub api_key: String,
    pub enabled: bool,
    /// Keys within one quota group are assumed to share account-level quota:
    /// an account-level exhaustion condition cools down the whole group.
    pub quota_group: String,
}

impl Default for SensenovaKeyConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            api_key: String::new(),
            enabled: true,
            quota_group: "default".into(),
        }
    }
}

impl std::fmt::Debug for SensenovaKeyConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SensenovaKeyConfig")
            .field("name", &self.name)
            .field("api_key", &"[REDACTED]")
            .field("enabled", &self.enabled)
            .field("quota_group", &self.quota_group)
            .finish()
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ModelsConfig {
    pub default: String,
    /// Unknown model names are rewritten to the default so Claude Code's
    /// built-in model names never 404 against SenseNova.
    pub map_unknown_to_default: bool,
    pub aliases: std::collections::BTreeMap<String, String>,
}

impl Default for ModelsConfig {
    fn default() -> Self {
        Self {
            default: "sensenova-6.8-flash-lite".into(),
            map_unknown_to_default: true,
            aliases: std::collections::BTreeMap::new(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetryConfig {
    /// Maximum upstream attempts per logical request. Claude Code already
    /// retries; keep this small to avoid multiplying its loop.
    pub max_attempts: usize,
    pub backoff_initial_ms: u64,
    pub backoff_max_ms: u64,
    /// Whether a 429 with a short Retry-After may be retried within the
    /// attempt budget instead of being returned to the client immediately.
    pub retry_429_with_short_retry_after: bool,
    pub max_retry_after_secs_for_retry: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 2,
            backoff_initial_ms: 250,
            backoff_max_ms: 4_000,
            retry_429_with_short_retry_after: true,
            max_retry_after_secs_for_retry: 10,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConcurrencyConfig {
    pub initial: usize,
    pub minimum: usize,
    pub maximum: usize,
    pub queue_capacity: usize,
    pub queue_timeout_secs: u64,
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            initial: 2,
            minimum: 1,
            maximum: 8,
            queue_capacity: 32,
            queue_timeout_secs: 120,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CircuitConfig {
    pub overload_threshold: u32,
    pub overload_window_secs: u64,
    pub overload_open_secs: u64,
    pub max_quota_cooldown_secs: u64,
}

impl Default for CircuitConfig {
    fn default() -> Self {
        Self {
            overload_threshold: 5,
            overload_window_secs: 60,
            overload_open_secs: 30,
            max_quota_cooldown_secs: 86_400,
        }
    }
}

#[derive(Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    #[default]
    Pretty,
    Json,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeConfig {
    pub max_request_bytes: usize,
    pub log_level: String,
    pub log_format: LogFormat,
    /// Interval for protocol-legal `event: ping` frames injected into
    /// otherwise-silent Anthropic streams. 0 disables ping injection.
    pub stream_ping_secs: u64,
    pub shutdown_timeout_secs: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_request_bytes: defaults::MAX_REQUEST_BYTES,
            log_level: "info".into(),
            log_format: LogFormat::Pretty,
            stream_ping_secs: defaults::STREAM_PING_SECS,
            shutdown_timeout_secs: defaults::SHUTDOWN_TIMEOUT_SECS,
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading configuration {}", path.display()))?;
        let config: Self = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing configuration {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        let bind: SocketAddr =
            self.server.bind.parse().with_context(
                || "server.bind must be an IP socket address such as 127.0.0.1:8789",
            )?;
        let _ = bind;
        if self.server.api_key.trim().is_empty() {
            bail!("server.api_key must not be empty");
        }
        validate_header_safe(&self.server.api_key, "server.api_key")?;

        let url = reqwest::Url::parse(&self.upstream.base_url)
            .context("upstream.base_url must be a valid URL")?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            bail!("upstream.base_url must be an absolute HTTP(S) URL");
        }
        if url.query().is_some() || url.fragment().is_some() {
            bail!("upstream.base_url must not contain a query or fragment");
        }
        if !url.username().is_empty() || url.password().is_some() {
            bail!("upstream.base_url must not contain credentials");
        }
        if !self.upstream.messages_path.starts_with('/')
            || self.upstream.messages_path.contains('?')
            || self.upstream.messages_path.contains('#')
        {
            bail!("upstream.messages_path must be an absolute path without query or fragment");
        }
        if self.upstream.anthropic_version.trim().is_empty() {
            bail!("upstream.anthropic_version must not be empty");
        }
        if self.upstream.timeout_secs == 0
            || self.upstream.connect_timeout_secs == 0
            || self.upstream.first_byte_timeout_secs == 0
        {
            bail!("upstream timeout values must be greater than zero");
        }

        if self.sensenova_api_keys.is_empty() {
            bail!("at least one SenseNova API key must be configured");
        }
        let mut names = HashSet::new();
        for (index, key) in self.sensenova_api_keys.iter().enumerate() {
            if key.name.trim().is_empty() {
                bail!("sensenova_api_keys[{index}].name must not be empty");
            }
            if key.api_key.trim().is_empty() {
                bail!("SenseNova API key at index {index} must not be empty");
            }
            validate_header_safe(&key.api_key, "sensenova_api_keys entry")?;
            if key.quota_group.trim().is_empty() {
                bail!("sensenova_api_keys[{index}].quota_group must not be empty");
            }
            if !names.insert(key.name.as_str()) {
                bail!("SenseNova API key names must be unique; duplicate name at index {index}");
            }
        }
        if !self.sensenova_api_keys.iter().any(|key| key.enabled) {
            bail!("at least one SenseNova API key must be enabled");
        }

        if self.models.default.trim().is_empty() {
            bail!("models.default must not be empty");
        }
        for (alias, model) in &self.models.aliases {
            if alias.trim().is_empty() || model.trim().is_empty() {
                bail!("model aliases and targets must not be empty");
            }
        }

        if self.retry.max_attempts == 0 || self.retry.max_attempts > 4 {
            bail!("retry.max_attempts must be between 1 and 4 (Claude Code already retries)");
        }
        if self.retry.backoff_initial_ms == 0 || self.retry.backoff_max_ms == 0 {
            bail!("retry.backoff values must be greater than zero");
        }
        if self.retry.backoff_initial_ms > self.retry.backoff_max_ms {
            bail!("retry.backoff_initial_ms must not exceed retry.backoff_max_ms");
        }

        let concurrency = &self.concurrency;
        if concurrency.minimum == 0
            || concurrency.initial < concurrency.minimum
            || concurrency.initial > concurrency.maximum
            || concurrency.maximum > 64
        {
            bail!(
                "concurrency must satisfy 1 <= minimum <= initial <= maximum <= 64; \
                 got minimum {}, initial {}, maximum {}",
                concurrency.minimum,
                concurrency.initial,
                concurrency.maximum
            );
        }
        if concurrency.queue_capacity == 0 || concurrency.queue_timeout_secs == 0 {
            bail!("concurrency.queue_capacity and queue_timeout_secs must be greater than zero");
        }

        if self.circuit.overload_threshold == 0
            || self.circuit.overload_window_secs == 0
            || self.circuit.overload_open_secs == 0
            || self.circuit.max_quota_cooldown_secs == 0
        {
            bail!("circuit values must be greater than zero");
        }

        if self.runtime.max_request_bytes == 0 || self.runtime.shutdown_timeout_secs == 0 {
            bail!("runtime size and shutdown limits must be greater than zero");
        }
        tracing_subscriber::EnvFilter::try_new(&self.runtime.log_level)
            .context("runtime.log_level must be a valid tracing filter")?;
        Ok(())
    }

    pub fn messages_url(&self) -> Result<reqwest::Url> {
        reqwest::Url::parse(&format!(
            "{}{}",
            self.upstream.base_url.trim_end_matches('/'),
            self.upstream.messages_path.as_str()
        ))
        .context("building SenseNova messages URL")
    }

    pub fn binds_loopback(&self) -> bool {
        self.server
            .bind
            .parse::<SocketAddr>()
            .map(|address| address.ip().is_loopback())
            .unwrap_or(false)
    }
}

fn validate_header_safe(value: &str, label: &str) -> Result<()> {
    // The value is used inside a `Bearer <value>` Authorization header, so
    // reject control characters that could enable header smuggling.
    if value
        .chars()
        .any(|character| (character as u32) < 0x20 || character as u32 == 0x7f)
    {
        bail!("{label} must not contain control characters");
    }
    Ok(())
}

pub fn default_config_path() -> PathBuf {
    std::env::var_os("SENSENOVA_PROXY_CONFIG")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config.json"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> Config {
        let mut config = Config::default();
        config.server.api_key = "gateway-secret".into();
        config.sensenova_api_keys = vec![SensenovaKeyConfig {
            name: "one".into(),
            api_key: "sensenova-secret".into(),
            enabled: true,
            quota_group: "account-a".into(),
        }];
        config
    }

    #[test]
    fn empty_or_disabled_keys_are_rejected() {
        let mut config = valid_config();
        config.sensenova_api_keys.clear();
        assert!(config.validate().is_err());
        config.sensenova_api_keys.push(SensenovaKeyConfig {
            name: "one".into(),
            api_key: "secret".into(),
            enabled: false,
            quota_group: "account-a".into(),
        });
        assert!(config.validate().is_err());
    }

    #[test]
    fn empty_keys_are_rejected_without_leaking_values() {
        let mut config = valid_config();
        config.sensenova_api_keys[0].api_key.clear();
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("index 0"));
        assert!(!error.contains("sensenova-secret"));
        assert!(!error.contains("gateway-secret"));

        let mut config = valid_config();
        config.server.api_key = "bad\nheader".into();
        assert!(config.validate().is_err());
        assert!(
            !config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("bad\nheader")
        );
    }

    #[test]
    fn url_credentials_and_unknown_fields_are_rejected() {
        let mut config = valid_config();
        config.upstream.base_url = "https://user:password@token.sensenova.cn".into();
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("must not contain credentials"));
        assert!(!error.contains("password"));

        let parsed: std::result::Result<Config, _> =
            serde_json::from_slice(br#"{"no_such_section":1}"#);
        assert!(parsed.is_err());
    }

    #[test]
    fn concurrency_ordering_is_enforced() {
        let mut config = valid_config();
        config.concurrency.initial = 0;
        assert!(config.validate().is_err());
        let mut config = valid_config();
        config.concurrency.minimum = 4;
        config.concurrency.initial = 2;
        config.concurrency.maximum = 8;
        assert!(config.validate().is_err());
        let mut config = valid_config();
        config.retry.max_attempts = 9;
        assert!(config.validate().is_err());
    }

    #[test]
    fn key_debug_never_exposes_key_material() {
        let key = SensenovaKeyConfig {
            name: "one".into(),
            api_key: "top-secret".into(),
            enabled: true,
            quota_group: "a".into(),
        };
        let rendered = format!("{key:?}");
        assert!(!rendered.contains("top-secret"));
        assert!(rendered.contains("REDACTED"));
    }

    #[test]
    fn example_configuration_loads_and_validates() {
        let path = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/config.example.json"));
        let config = Config::load(path).expect("example configuration must remain valid");
        assert_eq!(
            config.models.aliases["claude-sensenova"],
            config.models.default
        );
    }

    #[test]
    fn messages_url_is_joined_without_double_v1() {
        let config = valid_config();
        let url = config.messages_url().unwrap().to_string();
        assert_eq!(url, "https://token.sensenova.cn/v1/messages");
        let mut config = valid_config();
        config.upstream.base_url = "https://token.sensenova.cn/".into();
        assert_eq!(
            config.messages_url().unwrap().to_string(),
            "https://token.sensenova.cn/v1/messages"
        );
    }
}
