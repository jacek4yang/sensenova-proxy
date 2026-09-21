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
    pub const MAX_ROUTE_ATTEMPTS: usize = 4;
    pub const SOFT_AFFINITY_SECS: u64 = 300;
    pub const MAX_AFFINITY_ENTRIES: usize = 4_096;
    pub const SAME_ROUTE_429_RETRIES: usize = 0;
    pub const ROUTE_COOLDOWN_INITIAL_SECS: u64 = 10;
    pub const ROUTE_COOLDOWN_MAX_SECS: u64 = 120;
    pub const MODEL_TRIP_DISTINCT_GROUPS: usize = 2;
    pub const MODEL_TRIP_WINDOW_SECS: u64 = 20;
    pub const MODEL_OPEN_SECS: u64 = 30;
    pub const RETRY_AFTER_MAX_SECS: u64 = 120;
    pub const MAX_MODEL_COOLDOWN_SECS: u64 = 86_400;
    pub const HARD_PROFILE: &str = "claude-coding-hard";
    pub const FAST_PROFILE: &str = "claude-coding-fast";
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub server: ServerConfig,
    pub upstream: UpstreamConfig,
    pub sensenova_api_keys: Vec<SensenovaKeyConfig>,
    pub models: ModelsConfig,
    pub routing: RoutingConfig,
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

/// Virtual-model routing configuration.
///
/// Routing chain: client model → profile → quality tier → upstream model →
/// quota group → API key. A profile without any tier disables routing for
/// that name and the request falls back to the legacy
/// `models.default` / `models.aliases` behaviour.
#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RoutingConfig {
    /// One global route-attempt budget per logical request. Quality-tier
    /// fallback, quota-group failover, key failover and same-route replay all
    /// draw from this single number.
    pub max_route_attempts: usize,
    /// TTL of the weak behavioural session affinity (never cache-driven).
    pub soft_affinity_secs: u64,
    /// Hard bound on remembered sessions so affinity cannot grow without
    /// limit.
    pub max_affinity_entries: usize,
    /// Replays of the exact same `(model, quota_group, key)` after a 429.
    /// Zero means: always try another healthy route first.
    pub same_route_429_retries: usize,
    pub route_cooldown_initial_secs: u64,
    pub route_cooldown_max_secs: u64,
    /// Distinct quota groups that must fail inside `model_trip_window_secs`
    /// before a model-wide circuit opens. `2` means one account can never
    /// disable a model by itself.
    pub model_trip_distinct_groups: usize,
    pub model_trip_window_secs: u64,
    /// How long a tripped model circuit stays open before a half-open probe.
    pub model_open_secs: u64,
    /// Longest cooldown the proxy will wait out inside one request before
    /// returning `Retry-After` to the client instead.
    pub retry_after_max_secs: u64,
    /// Upper bound for a route/model cooldown (also caps parsed hints).
    pub max_model_cooldown_secs: u64,
    pub profiles: Profiles,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            max_route_attempts: defaults::MAX_ROUTE_ATTEMPTS,
            soft_affinity_secs: defaults::SOFT_AFFINITY_SECS,
            max_affinity_entries: defaults::MAX_AFFINITY_ENTRIES,
            same_route_429_retries: defaults::SAME_ROUTE_429_RETRIES,
            route_cooldown_initial_secs: defaults::ROUTE_COOLDOWN_INITIAL_SECS,
            route_cooldown_max_secs: defaults::ROUTE_COOLDOWN_MAX_SECS,
            model_trip_distinct_groups: defaults::MODEL_TRIP_DISTINCT_GROUPS,
            model_trip_window_secs: defaults::MODEL_TRIP_WINDOW_SECS,
            model_open_secs: defaults::MODEL_OPEN_SECS,
            retry_after_max_secs: defaults::RETRY_AFTER_MAX_SECS,
            max_model_cooldown_secs: defaults::MAX_MODEL_COOLDOWN_SECS,
            profiles: builtin_profiles(),
        }
    }
}

/// The documented default profiles.
///
/// `claude-coding-hard` isolates quality: tier 0 (`glm-5.2`,
/// `deepseek-v4-pro`) always outranks tier 1 (`kimi-k3`), and neither
/// `deepseek-v4-flash` nor `sensenova-6.8-flash-lite` can ever be selected.
/// `claude-coding-fast` contains only latency-oriented models.
pub fn builtin_profiles() -> Profiles {
    let mut profiles = Profiles::new();
    profiles.insert(
        defaults::HARD_PROFILE.to_owned(),
        ProfileConfig {
            latency_optimized: false,
            allow_cross_tier_fallback: false,
            tiers: vec![
                TierConfig {
                    models: vec!["glm-5.2".into(), "deepseek-v4-pro".into()],
                },
                TierConfig {
                    models: vec!["kimi-k3".into()],
                },
            ],
        },
    );
    profiles.insert(
        defaults::FAST_PROFILE.to_owned(),
        ProfileConfig {
            latency_optimized: true,
            allow_cross_tier_fallback: false,
            tiers: vec![TierConfig {
                models: vec![
                    "deepseek-v4-flash".into(),
                    "sensenova-6.8-flash-lite".into(),
                ],
            }],
        },
    );
    profiles
}

pub type Profiles = std::collections::BTreeMap<String, ProfileConfig>;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileConfig {
    /// Fast profiles score candidates by observed TTFT/latency; hard profiles
    /// only use health and load inside the same quality tier.
    pub latency_optimized: bool,
    /// When false (the default, and mandatory for the hard profile) a failing
    /// tier never falls through to a weaker model: the proxy fails honestly.
    pub allow_cross_tier_fallback: bool,
    pub tiers: Vec<TierConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TierConfig {
    pub models: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetryConfig {
    /// Maximum upstream attempts per logical request across all credentials.
    /// Claude Code already retries; keep this small to avoid multiplying its
    /// loop. Prefer spending the budget on *different* credentials: one
    /// credential is attempted at most `1 + max_same_key_retries` times.
    pub max_attempts: usize,
    /// How often one credential may be replayed within one logical request
    /// after a pre-commit transient failure. `1` means: initial attempt plus
    /// at most one same-key replay — never A×4 on a single key.
    pub max_same_key_retries: usize,
    pub backoff_initial_ms: u64,
    pub backoff_max_ms: u64,
    /// Whether a 429 with a short Retry-After may be retried within the
    /// attempt budget instead of being returned to the client immediately.
    pub retry_429_with_short_retry_after: bool,
    pub max_retry_after_secs_for_retry: u64,
    /// First fallback cooldown for a generic 429 without any authoritative
    /// Retry-After. SenseNova TPM/serving-capacity limits are transient:
    /// ~5 s beats the previous fixed 60 s, which made one 429 look like a
    /// minute-long outage. Escalates per consecutive generic 429 up to
    /// `rate_limit_fallback_max_secs` (5s → 10s → 20s → 40s → 60s).
    pub rate_limit_fallback_initial_secs: u64,
    pub rate_limit_fallback_max_secs: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            max_same_key_retries: 1,
            backoff_initial_ms: 1_000,
            backoff_max_ms: 5_000,
            retry_429_with_short_retry_after: true,
            max_retry_after_secs_for_retry: 10,
            rate_limit_fallback_initial_secs: 5,
            rate_limit_fallback_max_secs: 60,
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
        if self.retry.max_same_key_retries > 3 {
            bail!(
                "retry.max_same_key_retries must be between 0 and 3 (protect the upstream from replay bursts)"
            );
        }
        if self.retry.rate_limit_fallback_initial_secs == 0
            || self.retry.rate_limit_fallback_max_secs == 0
            || self.retry.rate_limit_fallback_initial_secs > self.retry.rate_limit_fallback_max_secs
            || self.retry.rate_limit_fallback_max_secs > 3_600
        {
            bail!("retry.rate_limit_fallback_* must satisfy 1 <= initial <= max <= 3600 seconds");
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
        self.validate_routing()?;
        tracing_subscriber::EnvFilter::try_new(&self.runtime.log_level)
            .context("runtime.log_level must be a valid tracing filter")?;
        Ok(())
    }

    /// Validate the routing section, reporting problems by profile/tier index
    /// so no configuration value (and never a credential) is echoed.
    fn validate_routing(&self) -> Result<()> {
        let routing = &self.routing;
        if routing.max_route_attempts == 0 || routing.max_route_attempts > 8 {
            bail!("routing.max_route_attempts must be between 1 and 8");
        }
        if routing.soft_affinity_secs > 86_400 {
            bail!("routing.soft_affinity_secs must not exceed 86400");
        }
        if routing.max_affinity_entries == 0 || routing.max_affinity_entries > 1_000_000 {
            bail!("routing.max_affinity_entries must be between 1 and 1000000");
        }
        if routing.same_route_429_retries > 3 {
            bail!("routing.same_route_429_retries must be between 0 and 3");
        }
        if routing.route_cooldown_initial_secs == 0
            || routing.route_cooldown_max_secs == 0
            || routing.route_cooldown_initial_secs > routing.route_cooldown_max_secs
            || routing.route_cooldown_max_secs > 3_600
        {
            bail!("routing.route_cooldown_* must satisfy 1 <= initial <= max <= 3600 seconds");
        }
        if routing.model_trip_distinct_groups == 0 || routing.model_trip_distinct_groups > 16 {
            bail!(
                "routing.model_trip_distinct_groups must be between 1 and 16 \
                 (2 means one account can never disable a model)"
            );
        }
        if routing.model_trip_window_secs == 0 || routing.model_trip_window_secs > 3_600 {
            bail!("routing.model_trip_window_secs must be between 1 and 3600");
        }
        if routing.model_open_secs == 0 || routing.model_open_secs > 3_600 {
            bail!("routing.model_open_secs must be between 1 and 3600");
        }
        if routing.retry_after_max_secs == 0 || routing.retry_after_max_secs > 3_600 {
            bail!("routing.retry_after_max_secs must be between 1 and 3600");
        }
        if routing.max_model_cooldown_secs == 0
            || routing.max_model_cooldown_secs > 366 * 24 * 3_600
        {
            bail!("routing.max_model_cooldown_secs must be between 1 and one year");
        }

        if routing.profiles.is_empty() {
            // Absent profiles fall back to the built-in hard/fast pair.
            return Ok(());
        }
        for (name, profile) in &routing.profiles {
            if name.trim().is_empty() {
                bail!("routing.profiles keys must not be empty");
            }
            if profile.tiers.is_empty() {
                bail!("routing.profiles.{name}.tiers must not be empty");
            }
            if profile.allow_cross_tier_fallback && profile.tiers.len() < 2 {
                bail!(
                    "routing.profiles.{name}.allow_cross_tier_fallback is meaningless with a single tier"
                );
            }
            let mut seen: HashSet<&str> = HashSet::new();
            for (index, tier) in profile.tiers.iter().enumerate() {
                if tier.models.is_empty() {
                    bail!("routing.profiles.{name}.tiers[{index}].models must not be empty");
                }
                for model in &tier.models {
                    if model.trim().is_empty() {
                        bail!(
                            "routing.profiles.{name}.tiers[{index}].models entries must not be empty"
                        );
                    }
                    if !seen.insert(model.as_str()) {
                        bail!(
                            "routing.profiles.{name}: model '{model}' appears in more than one tier"
                        );
                    }
                }
            }
        }
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

    /// Routing profiles actually in force: the configured ones when present,
    /// otherwise the documented built-in hard/fast profiles.
    pub fn profiles(&self) -> Profiles {
        if self.routing.profiles.is_empty() {
            builtin_profiles()
        } else {
            self.routing.profiles.clone()
        }
    }

    /// Routing tunables as used by the router, with the documented defaults
    /// backfilled when the section (or a field) is absent.
    pub fn route_config(&self) -> crate::router::RouteConfig {
        crate::router::RouteConfig::from(&self.routing)
    }

    /// The routing profile a request that names no virtual model is routed
    /// through. The hard profile is deliberate: an unqualified Claude Code
    /// request gets the quality pool, never the fast one.
    pub fn default_profile(&self) -> String {
        defaults::HARD_PROFILE.to_owned()
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
    fn example_routing_section_is_valid_and_documented() {
        let path = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/config.example.json"));
        let config = Config::load(path).expect("example configuration must remain valid");
        let profiles = config.profiles();
        assert!(profiles.contains_key("claude-coding-hard"));
        assert!(profiles.contains_key("claude-coding-fast"));
        let hard = &profiles["claude-coding-hard"];
        assert_eq!(
            hard.tiers.len(),
            2,
            "hard has a quality tier and a fallback"
        );
        assert_eq!(hard.tiers[0].models, vec!["glm-5.2", "deepseek-v4-pro"]);
        assert_eq!(hard.tiers[1].models, vec!["kimi-k3"]);
        assert!(
            !hard.allow_cross_tier_fallback,
            "the hard profile must never degrade silently"
        );
        let fast = &profiles["claude-coding-fast"];
        assert!(fast.latency_optimized);
        assert_eq!(fast.tiers.len(), 1);
        assert_eq!(
            fast.tiers[0].models,
            vec!["deepseek-v4-flash", "sensenova-6.8-flash-lite"]
        );
        // The aliases route Claude Code's model names at the profiles.
        assert_eq!(
            config.models.aliases["claude-sonnet-4-5"],
            "claude-coding-hard"
        );
        assert_eq!(
            config.models.aliases["claude-haiku-4-5"],
            "claude-coding-fast"
        );
    }

    #[test]
    fn builtin_profiles_isolate_quality() {
        let profiles = builtin_profiles();
        let hard = &profiles[defaults::HARD_PROFILE];
        let fast = &profiles[defaults::FAST_PROFILE];
        let hard_models: Vec<&String> = hard
            .tiers
            .iter()
            .flat_map(|tier| tier.models.iter())
            .collect();
        let fast_models: Vec<&String> = fast
            .tiers
            .iter()
            .flat_map(|tier| tier.models.iter())
            .collect();
        // No overlap at all: a fast model can never appear in the hard pool.
        for model in &fast_models {
            assert!(
                !hard_models.contains(model),
                "{model} must not be in the hard pool"
            );
        }
        assert!(!hard.allow_cross_tier_fallback);
        assert!(fast.latency_optimized);
    }

    #[test]
    fn routing_bounds_are_validated_without_leaking_values() {
        for mutate in [
            |routing: &mut RoutingConfig| routing.max_route_attempts = 0,
            |routing: &mut RoutingConfig| routing.max_route_attempts = 99,
            |routing: &mut RoutingConfig| routing.route_cooldown_initial_secs = 0,
            |routing: &mut RoutingConfig| routing.route_cooldown_initial_secs = 600,
            |routing: &mut RoutingConfig| routing.route_cooldown_max_secs = 0,
            |routing: &mut RoutingConfig| routing.model_trip_distinct_groups = 0,
            |routing: &mut RoutingConfig| routing.model_trip_window_secs = 0,
            |routing: &mut RoutingConfig| routing.model_open_secs = 0,
            |routing: &mut RoutingConfig| routing.retry_after_max_secs = 0,
            |routing: &mut RoutingConfig| routing.max_model_cooldown_secs = 0,
            |routing: &mut RoutingConfig| routing.max_affinity_entries = 0,
            |routing: &mut RoutingConfig| routing.soft_affinity_secs = 999_999,
            |routing: &mut RoutingConfig| routing.same_route_429_retries = 9,
            |routing: &mut RoutingConfig| routing.model_trip_distinct_groups = 99,
        ] {
            let mut config = valid_config();
            mutate(&mut config.routing);
            let error = config.validate().unwrap_err().to_string();
            assert!(error.contains("routing."), "unexpected error: {error}");
            // Validation messages name the field, never a value.
            assert!(!error.contains("sensenova-secret"));
            assert!(!error.contains("gateway-secret"));
        }
    }

    #[test]
    fn routing_profiles_are_validated() {
        // Empty tier list.
        let mut config = valid_config();
        config.routing.profiles.insert(
            "broken".into(),
            ProfileConfig {
                latency_optimized: false,
                allow_cross_tier_fallback: false,
                tiers: Vec::new(),
            },
        );
        assert!(config.validate().is_err());

        // Empty model list inside a tier.
        let mut config = valid_config();
        config.routing.profiles.insert(
            "broken".into(),
            ProfileConfig {
                latency_optimized: false,
                allow_cross_tier_fallback: false,
                tiers: vec![TierConfig { models: Vec::new() }],
            },
        );
        assert!(config.validate().is_err());

        // The same model in two tiers is ambiguous and rejected.
        let mut config = valid_config();
        config.routing.profiles.insert(
            "broken".into(),
            ProfileConfig {
                latency_optimized: false,
                allow_cross_tier_fallback: true,
                tiers: vec![
                    TierConfig {
                        models: vec!["dupe".into()],
                    },
                    TierConfig {
                        models: vec!["dupe".into()],
                    },
                ],
            },
        );
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("more than one tier"));

        // Cross-tier fallback with a single tier is meaningless.
        let mut config = valid_config();
        config.routing.profiles.insert(
            "broken".into(),
            ProfileConfig {
                latency_optimized: false,
                allow_cross_tier_fallback: true,
                tiers: vec![TierConfig {
                    models: vec!["only".into()],
                }],
            },
        );
        assert!(config.validate().is_err());
    }

    #[test]
    fn routing_section_rejects_unknown_fields() {
        let parsed: std::result::Result<Config, _> =
            serde_json::from_str(r#"{"routing":{"no_such_field":1}}"#);
        assert!(parsed.is_err(), "unknown routing fields must be rejected");
        let parsed: std::result::Result<Config, _> =
            serde_json::from_str(r#"{"routing":{"profiles":{"p":{"tiers":[],"extra":1}}}}"#);
        assert!(parsed.is_err(), "unknown profile fields must be rejected");
    }

    #[test]
    fn absent_routing_section_falls_back_to_builtin_profiles() {
        // Backward compatibility: an old configuration with no `routing`
        // section keeps working and gets the documented defaults.
        let config = Config::default();
        assert_eq!(
            config.routing.max_route_attempts,
            defaults::MAX_ROUTE_ATTEMPTS
        );
        assert_eq!(
            config.routing.soft_affinity_secs,
            defaults::SOFT_AFFINITY_SECS
        );
        assert_eq!(
            config.routing.same_route_429_retries,
            defaults::SAME_ROUTE_429_RETRIES
        );
        let profiles = config.profiles();
        assert_eq!(profiles.len(), 2);
        let route = config.route_config();
        assert_eq!(route.max_route_attempts, defaults::MAX_ROUTE_ATTEMPTS);
        assert_eq!(
            route.route_cooldown_initial,
            std::time::Duration::from_secs(defaults::ROUTE_COOLDOWN_INITIAL_SECS)
        );
        assert_eq!(
            route.route_cooldown_max,
            std::time::Duration::from_secs(defaults::ROUTE_COOLDOWN_MAX_SECS)
        );
        assert_eq!(config.default_profile(), defaults::HARD_PROFILE);
    }

    #[test]
    fn explicit_profiles_replace_the_builtin_pair() {
        let mut config = valid_config();
        let mut profiles = Profiles::new();
        profiles.insert(
            "solo".into(),
            ProfileConfig {
                latency_optimized: true,
                allow_cross_tier_fallback: false,
                tiers: vec![TierConfig {
                    models: vec!["one-model".into()],
                }],
            },
        );
        config.routing.profiles = profiles;
        config.validate().unwrap();
        let resolved = config.profiles();
        assert_eq!(resolved.len(), 1);
        assert!(resolved.contains_key("solo"));
        assert!(!resolved.contains_key(defaults::HARD_PROFILE));
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
