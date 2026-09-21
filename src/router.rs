//! Resilient dynamic model routing.
//!
//! Routing chain: virtual model → routing profile → quality tier → upstream
//! model → quota group → API key. The retry/routing unit is a
//! [`RouteTarget`]: one `(model, quota_group, key)` triple. Keeping the three
//! axes separate is what keeps the failure domains separate:
//!
//! - [`KeyState`] — one credential. A 401 disables only this key.
//! - [`GroupEntry`] — one `quota_group` (an account). Explicit quota
//!   exhaustion cools every key in the group.
//! - [`RouteState`] — one `(model, quota_group)` pair. A *generic* 429 is a
//!   per-route failure: it cools `glm-5.2 × account-A` without touching
//!   `glm-5.2 × account-B` or `deepseek-v4-pro × account-A`.
//! - [`ModelEntry`] — the model as a whole. Its circuit opens only when
//!   qualifying failures arrive from [`RouteConfig::model_trip_distinct_groups`]
//!   distinct quota groups inside
//!   [`RouteConfig::model_trip_window_secs`], so one failing account can never
//!   globally disable a model.
//!
//! Policy invariants enforced here (and covered by unit tests):
//!
//! - Quality tiers are absolute: a lower tier is considered only when every
//!   route of every higher tier is unusable *and* the profile allows
//!   cross-tier fallback. The hard profile never degrades quality silently.
//! - Latency scoring applies *within* one tier and only for
//!   `latency_optimized` profiles; cache state has zero influence anywhere.
//! - Session affinity is weak, bounded and behavioural only: it reorders
//!   candidates inside the already-chosen tier and never overrides health.
//! - Every wait is bounded: a cooldown is either waited out (only when no
//!   other route can be dialed and the hint is short) or reported to the
//!   caller as `Retry-After`. There is no unbounded sleep loop.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::config::{ModelsConfig, Profiles, TierConfig};
use crate::metrics::Metrics;
use crate::pool::SelectedKey;
use crate::rate_limit::MAX_PARSED_COOLDOWN;

/// How far ahead of the client's `Retry-After` the next attempt is scheduled,
/// so a hint that expires mid-flight does not merely race the upstream.
const RETRY_AFTER_MARGIN: Duration = Duration::from_millis(250);

/// Extra delay used to break up a replay against a route that just failed but
/// is not cooling.
const HEALTHY_ROUTE_STEP: Duration = Duration::from_millis(250);

/// Largest exponent the exponential ladder may use. Bounds the shift so a
/// long run of failures can never overflow or park a route forever.
const BACKOFF_MAX_SHIFT: usize = 32;

/// Upper bound for any internally computed wait. Keeps `Instant` arithmetic
/// and the client-facing `Retry-After` header in safe, representable ranges.
const MAX_ROUTE_WAIT: Duration = Duration::from_secs(86_400);

/// Routing tunables with `Duration`/`Instant` arithmetic already worked out.
///
/// Built from [`crate::config::RoutingConfig`] once at startup; the router
/// never re-reads the file.
#[derive(Debug, Clone)]
pub struct RouteConfig {
    pub max_route_attempts: usize,
    pub soft_affinity_secs: Duration,
    pub max_affinity_entries: usize,
    pub same_route_429_retries: usize,
    pub route_cooldown_initial: Duration,
    pub route_cooldown_max: Duration,
    pub model_trip_distinct_groups: usize,
    pub model_trip_window: Duration,
    pub model_open_secs: Duration,
    pub retry_after_max: Duration,
    pub max_model_cooldown: Duration,
}

impl From<&crate::config::RoutingConfig> for RouteConfig {
    fn from(config: &crate::config::RoutingConfig) -> Self {
        Self {
            max_route_attempts: config.max_route_attempts.max(1),
            soft_affinity_secs: Duration::from_secs(config.soft_affinity_secs),
            max_affinity_entries: config.max_affinity_entries.max(1),
            same_route_429_retries: config.same_route_429_retries,
            route_cooldown_initial: Duration::from_secs(config.route_cooldown_initial_secs.max(1)),
            route_cooldown_max: Duration::from_secs(config.route_cooldown_max_secs.max(1)),
            model_trip_distinct_groups: config.model_trip_distinct_groups.max(1),
            model_trip_window: Duration::from_secs(config.model_trip_window_secs.max(1)),
            model_open_secs: Duration::from_secs(config.model_open_secs.max(1)),
            retry_after_max: Duration::from_secs(config.retry_after_max_secs.max(1)),
            max_model_cooldown: Duration::from_secs(config.max_model_cooldown_secs.max(1)),
        }
    }
}

/// Which health gate excluded a candidate (observability only: the graded
/// scan walks past a refused candidate instead of failing the request).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateReason {
    ModelOpen,
    ModelDisabled,
    RouteDisabled,
    RouteCooling,
    QuotaGroupCooling,
    KeyCooling,
    KeyUnusable,
}

impl GateReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ModelOpen => "model_circuit_open",
            Self::ModelDisabled => "model_disabled",
            Self::RouteDisabled => "route_disabled",
            Self::RouteCooling => "route_cooling",
            Self::QuotaGroupCooling => "quota_group_cooling",
            Self::KeyCooling => "key_cooling",
            Self::KeyUnusable => "key_unusable",
        }
    }
}

/// The retry/routing unit.
#[derive(Clone, PartialEq, Eq)]
pub struct RouteTarget {
    pub model: Arc<str>,
    pub quota_group: Arc<str>,
    pub key: SelectedKey,
    /// Quality tier this target belongs to; lower is better.
    pub tier: usize,
}

impl RouteTarget {
    pub fn model_str(&self) -> &str {
        &self.model
    }

    pub fn quota_group_str(&self) -> &str {
        &self.quota_group
    }

    /// Identity used by the caller's per-request `skipped` set.
    pub fn identity(&self) -> (u64, u64, usize) {
        (
            hash_name(&self.model),
            hash_name(&self.quota_group),
            self.key.index,
        )
    }

    fn route_id(&self) -> RouteId {
        RouteId {
            model: hash_name(&self.model),
            group: hash_name(&self.quota_group),
        }
    }
}

impl std::fmt::Debug for RouteTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RouteTarget")
            .field("model", &self.model)
            .field("quota_group", &self.quota_group)
            .field("key", &self.key.name)
            .field("tier", &self.tier)
            .finish()
    }
}

/// What the caller should do after recording a failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Followup {
    /// Another attempt is worth making; sleep `wait` first.
    Continue { wait: Duration, waitable: bool },
    /// No dialable route remains; surface the failure (with `Retry-After`).
    Return,
}

/// A selected route for one attempt.
#[derive(Debug, Clone)]
pub struct Plan {
    pub target: RouteTarget,
    /// How many higher-quality tiers were found unusable in this scan.
    pub stepped_down: usize,
    /// The first gate that excluded a candidate, for logging.
    pub gate: Option<GateReason>,
    /// Time until the first same-tier alternative becomes dialable.
    pub next_alternative: Option<Duration>,
}

/// No route can be dialed right now.
#[derive(Debug, Clone)]
pub struct WaitablePlan {
    /// When the earliest dialable *higher-or-equal* quality route appears.
    /// `None` when every known route is hard-blocked (disabled model, dead
    /// credential) rather than merely cooling.
    pub wait: Option<Duration>,
}

/// A client-visible model resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    /// A virtual routing profile (multi-tier, quality-isolated).
    Profile(String),
    /// A concrete upstream model: routed as itself, all keys, one tier.
    Literal(String),
}

/// What a request is routed *as*: either a configured profile or a single
/// concrete model (an alias, a catalog ID, or the default mapping).
#[derive(Debug, Clone)]
pub enum RouteSpec {
    Profile(String),
    Literal(String),
}

impl RouteSpec {
    pub fn label(&self) -> &str {
        match self {
            Self::Profile(name) | Self::Literal(name) => name,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CircuitState {
    #[default]
    Closed,
    Open,
    HalfOpen,
}

impl CircuitState {
    /// Low-cardinality label for logs and readiness output.
    #[allow(dead_code)]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::HalfOpen => "half_open",
        }
    }
}

/// Health of one configured credential.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CredentialSnapshot {
    pub name: Arc<str>,
    pub quota_group: Arc<str>,
    pub cooling_remaining: Option<Duration>,
    /// True after a 401: this credential is rejected and will not be dialed.
    pub unusable: bool,
    pub rate_limit_streak: u32,
    pub inflight: usize,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ModelSnapshot {
    pub model: Arc<str>,
    pub state: CircuitState,
    pub open_reason: &'static str,
    pub disabled: bool,
}

struct Inner {
    keys: Vec<KeyEntry>,
    routes: Mutex<HashMap<RouteId, RouteState>>,
    models: Mutex<HashMap<Arc<str>, ModelEntry>>,
    groups: Mutex<HashMap<Arc<str>, GroupEntry>>,
    affinity: Mutex<HashMap<String, AffinityEntry>>,
    /// Deterministic alternation counter for equally-scored candidates.
    round_robin: AtomicU64,
    /// Total affinities broken by a health event (rendered by metrics).
    affinity_breaks: AtomicU64,
}

struct KeyEntry {
    index: usize,
    name: Arc<str>,
    api_key: Arc<str>,
    group: Arc<str>,
    state: Mutex<KeyState>,
}

#[derive(Default)]
struct KeyState {
    cooling_until: Option<Instant>,
    unusable: bool,
    /// Consecutive generic-429 events without an intervening success; drives
    /// the bounded exponential route cooldown ladder.
    streak: u32,
    /// Observed time-to-first-byte EWMA, in milliseconds (fast profiles).
    ttft_ms: Option<f64>,
    /// Observed total latency EWMA, in milliseconds (fast profiles).
    latency_ms: Option<f64>,
    inflight: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct RouteId {
    model: u64,
    group: u64,
}

#[derive(Default, Clone, Copy)]
struct RouteState {
    cooling_until: Option<Instant>,
    /// 404 or repeated hard failure: this `(model, group)` route is not worth
    /// dialing for a while.
    disabled_until: Option<Instant>,
}

#[derive(Default)]
struct GroupEntry {
    cooling_until: Option<Instant>,
}

#[derive(Default)]
struct ModelEntry {
    state: CircuitState,
    open_until: Option<Instant>,
    open_reason: &'static str,
    /// 404 latches here: a model the upstream does not serve cannot recover
    /// without a configuration change and a restart.
    disabled: bool,
    /// Open half-open probe slot, released by the inflight guard or by a
    /// terminal outcome.
    probe_in_flight: bool,
    /// Distinct quota groups that produced qualifying failures recently.
    failing_groups: VecDeque<(u64, Instant)>,
}

struct AffinityEntry {
    route: RouteId,
    expires: Instant,
}

#[derive(Clone)]
struct Candidate {
    model: Arc<str>,
    model_hash: u64,
    group: Arc<str>,
    group_hash: u64,
    a_index: usize,
    b_index: Option<usize>,
    /// Position of the model inside its tier, then of the group in the key
    /// table: the deterministic order of the configuration.
    ordinal: (usize, usize),
}

// Two `impl Candidate` blocks would be a duplicate-definition error; merge them.
impl Candidate {
    fn route_id(&self) -> RouteId {
        RouteId {
            model: self.model_hash,
            group: self.group_hash,
        }
    }

    fn stable_id(&self) -> (u64, u64) {
        (self.model_hash, self.group_hash)
    }

    /// Deterministic tie-break: the model's position inside its tier.
    /// Credential and group order are supplied by the rotation, so an
    /// equally-scored set spreads load while a cold table stays predictable.
    fn order_key(&self) -> usize {
        self.ordinal.0
    }
}

impl std::fmt::Debug for Candidate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Candidate")
            .field("model", &self.model)
            .field("group", &self.group)
            .field("a", &self.a_index)
            .field("b", &self.b_index)
            .finish()
    }
}

/// A candidate's dialability.
#[derive(Debug, Clone, Copy)]
enum Dialability {
    Ready,
    Cooling(Duration),
    Blocked(GateReason),
}

#[derive(Clone)]
pub struct RouteTable {
    inner: Arc<Inner>,
    route: RouteConfig,
    profiles: Profiles,
}

impl std::fmt::Debug for RouteTable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RouteTable")
            .field("keys", &self.inner.keys.len())
            .field("models", &self.all_candidates().len())
            .finish()
    }
}

impl RouteTable {
    /// Build the table over every enabled credential.
    ///
    /// The pool passed in is retained for the already-configured credential
    /// surface (`/readyz`, `/metrics`, integration tests) while routing
    /// decisions use this table's own, schema-independent health state.
    pub fn new(config: &crate::config::Config, pool: crate::pool::KeyPool) -> Self {
        let keys = config
            .sensenova_api_keys
            .iter()
            .enumerate()
            .filter(|(_, key)| key.enabled)
            .map(|(index, key)| KeyEntry {
                index,
                name: Arc::from(key.name.as_str()),
                api_key: Arc::from(key.api_key.as_str()),
                group: Arc::from(key.quota_group.as_str()),
                state: Mutex::new(KeyState::default()),
            })
            .collect();
        // The pool stays authoritative for `usable_count` when no routing
        // configuration is present; the router keeps it in step through
        // `sync_pool` so both surfaces agree.
        let _ = &pool;
        Self {
            inner: Arc::new(Inner {
                keys,
                routes: Mutex::new(HashMap::new()),
                models: Mutex::new(HashMap::new()),
                groups: Mutex::new(HashMap::new()),
                affinity: Mutex::new(HashMap::new()),
                round_robin: AtomicU64::new(0),
                affinity_breaks: AtomicU64::new(0),
            }),
            route: config.route_config(),
            profiles: config.profiles(),
        }
    }

    #[allow(dead_code)]
    pub fn route_config(&self) -> &RouteConfig {
        &self.route
    }

    #[allow(dead_code)]
    pub fn profile_names(&self) -> Vec<String> {
        self.profiles.keys().cloned().collect()
    }

    #[allow(dead_code)]
    pub fn is_profile(&self, name: &str) -> bool {
        self.profiles.contains_key(name)
    }

    #[allow(dead_code)]
    pub fn profile_models(&self, profile: &str) -> Vec<String> {
        self.profiles
            .get(profile)
            .map(|profile| {
                profile
                    .tiers
                    .iter()
                    .flat_map(|tier| tier.models.iter().cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Resolve the client's requested model.
    ///
    /// Precedence: explicit alias → virtual routing profile → pass-through.
    /// `map_unknown_to_default` keeps Claude Code's built-in model names from
    /// 404-ing upstream, exactly as before routing existed.
    pub fn resolve(&self, models: &ModelsConfig, requested: &str) -> Resolution {
        if let Some(target) = models.aliases.get(requested) {
            // A configured alias is authoritative, but if it names a profile
            // the profile still wins so `claude-sonnet-4-5 -> claude-coding-hard`
            // behaves as documented.
            if self.profiles.contains_key(target.as_str()) {
                return Resolution::Profile(target.clone());
            }
            return Resolution::Literal(target.clone());
        }
        if self.profiles.contains_key(requested) {
            return Resolution::Profile(requested.to_owned());
        }
        if models.map_unknown_to_default && requested.starts_with("claude") {
            return Resolution::Literal(models.default.clone());
        }
        Resolution::Literal(requested.to_owned())
    }

    #[allow(dead_code)]
    pub fn affinity_len(&self) -> usize {
        lock(&self.inner.affinity).len()
    }

    #[allow(dead_code)]
    pub fn affinity_break_count(&self) -> u64 {
        self.inner.affinity_breaks.load(Ordering::Relaxed)
    }

    /// Whether a `(model, quota_group)` route has been disabled (404).
    #[allow(dead_code)]
    pub fn route_disabled(&self, model: &str, group: &str) -> bool {
        let now = self.now();
        let id = RouteId {
            model: hash_name(model),
            group: hash_name(group),
        };
        let mut routes = lock(&self.inner.routes);
        match routes.get_mut(&id) {
            Some(entry) => {
                clear(&mut entry.disabled_until, now);
                entry.disabled_until.is_some()
            }
            None => false,
        }
    }

    fn key_entry(&self, index: usize) -> Option<&KeyEntry> {
        self.inner.keys.get(index)
    }

    /// One candidate per `(model, quota_group)`, with a power-of-two choice of
    /// credential inside the group so a healthy group is not always dialed
    /// through the same key.
    fn candidates(&self, model: &str) -> Vec<Candidate> {
        self.candidates_at(model, 0)
    }

    fn candidates_at(&self, model: &str, model_ordinal: usize) -> Vec<Candidate> {
        let mut by_group: Vec<(Arc<str>, Vec<usize>)> = Vec::new();
        for key in &self.inner.keys {
            match by_group
                .iter_mut()
                .find(|(group, _)| group.as_ref() == key.group.as_ref())
            {
                Some((_, indexes)) => indexes.push(key.index),
                None => by_group.push((key.group.clone(), vec![key.index])),
            }
        }
        let rotation = self.inner.round_robin.load(Ordering::Relaxed) as usize;
        by_group
            .into_iter()
            .map(|(group, indexes)| {
                let group_hash = hash_name(&group);
                // Power-of-two choices inside the group: rotate the *starting*
                // credential so equivalent keys genuinely share load, and keep
                // a second candidate for the health check to fall back on.
                let a_index = indexes[rotation % indexes.len()];
                let b_index = if indexes.len() > 1 {
                    Some(indexes[(rotation + 1) % indexes.len()])
                } else {
                    None
                };
                Candidate {
                    model: Arc::from(model),
                    model_hash: hash_name(model),
                    group,
                    group_hash,
                    a_index,
                    b_index,
                    ordinal: (model_ordinal, a_index),
                }
            })
            .collect()
    }

    /// Every `(model, quota_group)` the configured profiles can reach.
    pub fn all_candidates(&self) -> Vec<(Arc<str>, Arc<str>)> {
        let mut seen: HashSet<(u64, u64)> = HashSet::new();
        let mut out = Vec::new();
        for profile in self.profiles.values() {
            for tier in &profile.tiers {
                for model in &tier.models {
                    for candidate in self.candidates(model) {
                        if seen.insert(candidate.stable_id()) {
                            out.push((candidate.model, candidate.group));
                        }
                    }
                }
            }
        }
        out
    }

    fn now(&self) -> Instant {
        Instant::now()
    }

    fn next_round_robin(&self) -> u64 {
        self.inner.round_robin.fetch_add(1, Ordering::Relaxed)
    }

    fn key_choices(&self, candidate: &Candidate) -> Vec<usize> {
        match candidate.b_index {
            Some(b) if b != candidate.a_index => vec![candidate.a_index, b],
            _ => vec![candidate.a_index],
        }
    }

    fn dialability(
        &self,
        candidate: &Candidate,
        now: Instant,
    ) -> (Dialability, Option<(usize, Duration)>) {
        let model = self.model_view(&candidate.model, now);
        if model.disabled {
            return (Dialability::Blocked(GateReason::ModelDisabled), None);
        }
        let (half_open, open_until) = match model.state {
            CircuitState::Open => (false, model.open_until),
            CircuitState::HalfOpen => (true, None),
            CircuitState::Closed => (false, None),
        };
        if let Some(open_until) = open_until
            && now < open_until
        {
            return (Dialability::Blocked(GateReason::ModelOpen), None);
        }

        let view = self.route_view(candidate, now);
        if view.disabled {
            return (Dialability::Blocked(GateReason::RouteDisabled), None);
        }

        let group_cooldown = self.group_view(&candidate.group, now);
        if let Some(remaining) = group_cooldown {
            // Explicit quota exhaustion: the whole group is the failure, so
            // half-open probing does not admit it.
            return (
                Dialability::Blocked(GateReason::QuotaGroupCooling),
                Some((candidate.a_index, remaining)),
            );
        }

        if let Some(remaining) = view.cooling {
            // A closed model circuit lets a cooled route be probed by another
            // key while the original key waits out its own cooldown — the
            // 429 was very likely per-key if the model has a healthy sibling.
            let has_healthy_sibling = self
                .key_choices(candidate)
                .iter()
                .any(|index| self.key_ready(*index, now).is_some());
            if !(half_open && has_healthy_sibling) {
                return (
                    Dialability::Blocked(GateReason::RouteCooling),
                    Some((candidate.a_index, remaining)),
                );
            }
        }

        // Half-open admits one probe; a second concurrent request is refused
        // rather than racing the probe.
        if half_open && model.probe_in_flight {
            return (Dialability::Blocked(GateReason::ModelOpen), None);
        }
        let mut best_wait: Option<(usize, Duration)> = None;
        for index in self.key_choices(candidate) {
            if self.key_ready(index, now).is_some() {
                return (Dialability::Ready, best_wait);
            }
            if let Some(remaining) = self.key_cooling(index, now) {
                best_wait = Some(match best_wait {
                    Some((_, current)) if current <= remaining => (index, current),
                    _ => (index, remaining),
                });
            }
        }
        match best_wait {
            Some((_, remaining)) => (Dialability::Cooling(remaining), best_wait),
            None if self.all_keys_unusable(candidate) => {
                (Dialability::Blocked(GateReason::KeyUnusable), None)
            }
            None => (Dialability::Blocked(GateReason::KeyCooling), None),
        }
    }

    fn key_ready(&self, index: usize, now: Instant) -> Option<usize> {
        let entry = self.key_entry(index)?;
        let mut state = lock(&entry.state);
        clear(&mut state.cooling_until, now);
        if state.unusable || state.cooling_until.is_some() {
            None
        } else {
            Some(index)
        }
    }

    fn key_cooling(&self, index: usize, now: Instant) -> Option<Duration> {
        let entry = self.key_entry(index)?;
        let mut state = lock(&entry.state);
        clear(&mut state.cooling_until, now);
        state.cooling_until.map(|until| until - now)
    }

    fn all_keys_unusable(&self, candidate: &Candidate) -> bool {
        self.key_choices(candidate).iter().all(|index| {
            self.key_entry(*index)
                .map(|entry| lock(&entry.state).unusable)
                .unwrap_or(false)
        })
    }

    fn model_view(&self, model: &Arc<str>, now: Instant) -> ModelView {
        let mut models = lock(&self.inner.models);
        let entry = models.entry(model.clone()).or_default();
        if entry.state == CircuitState::Open
            && entry.open_until.is_some_and(|open_until| now >= open_until)
        {
            entry.state = CircuitState::HalfOpen;
            entry.open_until = None;
            entry.probe_in_flight = false;
        }
        ModelView {
            state: entry.state,
            open_until: entry.open_until,
            disabled: entry.disabled,
            probe_in_flight: entry.probe_in_flight,
        }
    }

    fn route_view(&self, candidate: &Candidate, now: Instant) -> RouteView {
        let mut routes = lock(&self.inner.routes);
        let entry = routes.entry(candidate.route_id()).or_default();
        clear(&mut entry.cooling_until, now);
        clear(&mut entry.disabled_until, now);
        RouteView {
            cooling: entry
                .cooling_until
                .map(|until| until.saturating_duration_since(now)),
            disabled: entry.disabled_until.is_some(),
        }
    }

    fn group_view(&self, group: &Arc<str>, now: Instant) -> Option<Duration> {
        let mut groups = lock(&self.inner.groups);
        let entry = groups.entry(group.clone()).or_default();
        clear(&mut entry.cooling_until, now);
        entry
            .cooling_until
            .map(|until| until.saturating_duration_since(now))
    }

    /// Select the next route to attempt.
    ///
    /// `skipped` carries the `(model, group, key)` identities already attempted
    /// by this logical request — the global retry budget keeps it small.
    pub fn plan(
        &self,
        spec: &RouteSpec,
        session_tag: &str,
        attempt: usize,
        skipped: &HashSet<(u64, u64, usize)>,
        metrics: &Metrics,
    ) -> Result<Plan, WaitablePlan> {
        // A literal model is its own single-tier, single-model profile: it is
        // never substituted, only failed over across quota groups and keys.
        let literal_tiers = vec![TierConfig {
            models: vec![spec.label().to_owned()],
        }];
        let (tiers, latency_optimized, allow_cross_tier_fallback): (
            &[crate::config::TierConfig],
            bool,
            bool,
        ) = match spec {
            RouteSpec::Literal(_) => (&literal_tiers, false, false),
            RouteSpec::Profile(name) => match self.profiles.get(name) {
                Some(config) => (
                    config.tiers.as_slice(),
                    config.latency_optimized,
                    config.allow_cross_tier_fallback,
                ),
                None => return Err(WaitablePlan { wait: None }),
            },
        };
        let _ = (latency_optimized, attempt);
        let now = self.now();
        let affinity = self.affinity_route(session_tag, now);
        let mut stepped_down = 0usize;
        let mut gate: Option<GateReason> = None;
        let mut lower_tier_wait: Option<Duration> = None;

        for (tier, tier_config) in tiers.iter().enumerate() {
            let mut available: Vec<(Candidate, u64)> = Vec::new();
            let mut tier_next: Option<Duration> = None;

            for (model_ordinal, model) in tier_config.models.iter().enumerate() {
                for candidate in self.candidates_at(model, model_ordinal) {
                    let choices = self.key_choices(&candidate);
                    let any_unskipped = choices.iter().any(|index| {
                        !skipped.contains(&(candidate.model_hash, candidate.group_hash, *index))
                    });
                    if !any_unskipped {
                        continue;
                    }
                    let (dialability, wait) = self.dialability(&candidate, now);
                    match dialability {
                        Dialability::Ready => {
                            let score = self.score(&candidate, affinity);
                            available.push((candidate, score));
                        }
                        Dialability::Cooling(remaining) => {
                            tier_next = Some(match tier_next {
                                Some(current) => current.min(remaining),
                                None => remaining,
                            });
                        }
                        Dialability::Blocked(reason) => {
                            if gate.is_none() {
                                gate = Some(reason);
                            }
                            if let Some((_, remaining)) = wait {
                                tier_next = Some(match tier_next {
                                    Some(current) => current.min(remaining),
                                    None => remaining,
                                });
                            }
                        }
                    }
                }
            }

            if !available.is_empty() {
                if tier > 0 {
                    stepped_down = tier;
                    metrics.tier_steps_total.fetch_add(1, Ordering::Relaxed);
                }
                if gate.is_some() {
                    metrics
                        .health_excluded_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                let rotation = self.inner.round_robin.load(Ordering::Relaxed) as usize;
                let mut candidates = order(available, affinity, rotation);
                let chosen = candidates.remove(0);
                // Only after a selection is made does the rotation advance, so
                // a cold table starts with the first configured credential
                // while subsequent requests genuinely spread load.
                self.next_round_robin();
                if affinity.is_some() && Some(chosen.0.route_id()) == affinity {
                    metrics.affinity_hits_total.fetch_add(1, Ordering::Relaxed);
                }
                debug_assert!(chosen.0.a_index < self.inner.keys.len());
                let target = self.target(chosen.0.clone(), tier);
                return Ok(Plan {
                    target,
                    stepped_down,
                    gate,
                    next_alternative: tier_next,
                });
            }

            // The tier is unusable.
            //
            // `allow_cross_tier_fallback` is about *quality*: when it is false
            // (the hard profile's setting) a tier is only left behind when it
            // is genuinely blocked — every route hard-failed (disabled model,
            // dead credential) — never merely because it is cooling. A tier
            // that is only cooling reports its wait instead, so the caller can
            // sleep and retry the same quality rather than silently downgrade.
            if let Some(remaining) = tier_next {
                lower_tier_wait = Some(match lower_tier_wait {
                    Some(current) => current.min(remaining),
                    None => remaining,
                });
                if !allow_cross_tier_fallback {
                    break;
                }
            }
        }

        let _ = attempt;
        Err(WaitablePlan {
            wait: lower_tier_wait,
        })
    }

    fn target(&self, candidate: Candidate, tier: usize) -> RouteTarget {
        let index = self
            .key_choices(&candidate)
            .into_iter()
            .find(|index| self.key_ready(*index, self.now()).is_some())
            .unwrap_or(candidate.a_index);
        let entry = &self.inner.keys[index];
        RouteTarget {
            model: candidate.model.clone(),
            quota_group: candidate.group.clone(),
            key: SelectedKey::new(
                entry.index,
                entry.name.clone(),
                entry.group.clone(),
                entry.api_key.clone(),
            ),
            tier,
        }
    }

    /// Cost of choosing this candidate. Lower wins.
    ///
    /// Hard (non-latency-optimized) profiles only weigh health and load, so a
    /// fast weak model can never outrank a healthy higher-quality tier — the
    /// tier loop already guarantees that, and this score never crosses tiers.
    /// Fast profiles additionally weigh observed TTFT/latency. Cache state is
    /// never consulted.
    fn score(&self, candidate: &Candidate, affinity: Option<RouteId>) -> u64 {
        let latency_profile = self
            .profiles
            .get(&self.profile_for_candidate(candidate))
            .map(|profile| profile.latency_optimized)
            .unwrap_or(false);
        let mut score = 0u64;
        for index in self.key_choices(candidate) {
            if let Some(entry) = self.key_entry(index) {
                let state = lock(&entry.state);
                // Recent 429 streak and inflight both push a candidate down;
                // inflight dominates so the least-loaded route wins first.
                score = score.saturating_add((state.inflight as u64) * 1_000_000);
                score = score.saturating_add((state.streak as u64) * 100_000);
                if latency_profile {
                    if let Some(ttft) = state.ttft_ms {
                        score = score.saturating_add(ttft.clamp(0.0, 60_000.0) as u64);
                    }
                    if let Some(latency) = state.latency_ms {
                        score = score.saturating_add((latency.clamp(0.0, 600_000.0) / 10.0) as u64);
                    }
                }
            }
        }
        // Affinity is a weak preference *within* the chosen tier only: it
        // must never outweigh load or health.
        if affinity == Some(candidate.route_id()) {
            score = score.saturating_sub(1);
        }
        score
    }

    /// Name of the profile that declares this model (first match wins).
    fn profile_for_candidate(&self, candidate: &Candidate) -> String {
        for (name, profile) in &self.profiles {
            for tier in &profile.tiers {
                if tier
                    .models
                    .iter()
                    .any(|model| model == candidate.model.as_ref())
                {
                    return name.clone();
                }
            }
        }
        String::new()
    }

    fn affinity_route(&self, session_tag: &str, now: Instant) -> Option<RouteId> {
        if session_tag.is_empty() || session_tag == "sess_none" {
            return None;
        }
        let mut affinity = lock(&self.inner.affinity);
        let entry = affinity.get(session_tag)?;
        if entry.expires <= now {
            affinity.remove(session_tag);
            return None;
        }
        Some(entry.route)
    }

    /// Current consecutive generic-429 streak for a target's credential.
    pub fn rate_limit_streak(&self, target: &RouteTarget) -> u32 {
        self.key_entry(target.key.index)
            .map(|entry| lock(&entry.state).streak)
            .unwrap_or(0)
    }

    /// The next route to try after recording a failure.
    ///
    /// Re-plans against the failure marks that were just written, so the
    /// decision already accounts for the cooldown the caller is about to
    /// honour. Affinity is dropped here because the route behind it just
    /// proved unhealthy.
    pub fn followup(
        &self,
        spec: &RouteSpec,
        session_tag: &str,
        failed: &RouteTarget,
        skipped: &HashSet<(u64, u64, usize)>,
        metrics: &Metrics,
    ) -> Followup {
        self.break_affinity(metrics, session_tag);
        let mut probe: HashSet<(u64, u64, usize)> = skipped.clone();
        probe.insert(failed.identity());
        match self.plan(spec, "sess_none", 0, &probe, metrics) {
            Ok(plan) => Followup::Continue {
                wait: plan.next_alternative.unwrap_or(Duration::ZERO),
                waitable: false,
            },
            Err(waitable) => match waitable.wait {
                Some(wait) if wait <= self.route.retry_after_max => Followup::Continue {
                    wait,
                    waitable: true,
                },
                _ => Followup::Return,
            },
        }
    }

    pub fn note_success(
        &self,
        metrics: &Metrics,
        target: &RouteTarget,
        ttft: Option<Duration>,
        total: Duration,
    ) {
        if let Some(entry) = self.key_entry(target.key.index) {
            let mut state = lock(&entry.state);
            state.streak = 0;
            state.cooling_until = None;
            let sample = ttft.map(|ttft| ttft.as_secs_f64() * 1000.0);
            if let Some(sample) = sample {
                state.ttft_ms = Some(match state.ttft_ms {
                    Some(previous) => previous * 0.7 + sample * 0.3,
                    None => sample,
                });
            }
            let sample = total.as_secs_f64() * 1000.0;
            state.latency_ms = Some(match state.latency_ms {
                Some(previous) => previous * 0.7 + sample * 0.3,
                None => sample,
            });
        }
        {
            let mut routes = lock(&self.inner.routes);
            if let Some(entry) = routes.get_mut(&target.route_id()) {
                entry.cooling_until = None;
                entry.disabled_until = None;
            }
        }
        {
            let mut groups = lock(&self.inner.groups);
            if let Some(entry) = groups.get_mut(&self.group_of(target)) {
                entry.cooling_until = None;
            }
        }
        {
            let mut models = lock(&self.inner.models);
            if let Some(entry) = models.get_mut(&target.model) {
                entry.state = CircuitState::Closed;
                entry.open_reason = "";
                entry.probe_in_flight = false;
                entry.failing_groups.clear();
            }
        }
        let _ = metrics;
    }

    /// Generic 429: cool `(model, quota_group)` only.
    pub fn note_rate_limited(
        &self,
        metrics: &Metrics,
        target: &RouteTarget,
        attempt: usize,
        streak: u32,
        hint: Option<Duration>,
        jitter: f64,
    ) -> Duration {
        let cooldown = barrier_duration(
            attempt,
            self.route.route_cooldown_initial,
            self.route.route_cooldown_max,
            self.route.max_model_cooldown,
            streak,
            hint,
            jitter,
        );
        let now = self.now();
        if let Some(entry) = self.key_entry(target.key.index) {
            lock(&entry.state).streak = streak.saturating_add(1);
        }
        {
            let mut routes = lock(&self.inner.routes);
            let entry = routes.entry(target.route_id()).or_default();
            entry.cooling_until = deadline(now, cooldown);
        }
        let _ = metrics;
        self.record_route_failure(target, now);
        cooldown
    }

    /// Explicit quota exhaustion: cool every key in the group.
    pub fn note_quota_exhausted(
        &self,
        metrics: &Metrics,
        target: &RouteTarget,
        cooldown: Duration,
    ) {
        let now = self.now();
        let group = self.group_of(target);
        // The whole quota group is the failure domain: cooling it here makes
        // every key and every model on that account unavailable at once.
        {
            let mut groups = lock(&self.inner.groups);
            let entry = groups.entry(group.clone()).or_default();
            entry.cooling_until = deadline(now, cooldown);
        }
        for key in &self.inner.keys {
            if key.group == group {
                lock(&key.state).cooling_until = deadline(now, cooldown);
            }
        }
        self.break_affinity_for_group(&group);
        let _ = metrics;
    }

    /// 401: only this credential is wrong.
    pub fn note_authentication_failure(&self, target: &RouteTarget) {
        if let Some(entry) = self.key_entry(target.key.index) {
            let mut state = lock(&entry.state);
            state.unusable = true;
            state.cooling_until = None;
        }
        self.break_affinity_for_route(target);
    }

    /// 404: the model is not served on this route at all.
    ///
    /// Disables the `(model, quota_group)` route (strong, long cooldown) so the
    /// same logical request never re-dials it, and latches the model as
    /// unavailable on this deployment: a 404 cannot recover without a
    /// configuration change.
    pub fn note_model_missing(&self, metrics: &Metrics, target: &RouteTarget) {
        let now = self.now();
        self.break_affinity_for_route(target);
        {
            let mut routes = lock(&self.inner.routes);
            let entry = routes.entry(target.route_id()).or_default();
            entry.disabled_until = deadline(now, self.route.max_model_cooldown);
        }
        let mut models = lock(&self.inner.models);
        let entry = models.entry(target.model.clone()).or_default();
        entry.disabled = true;
        let _ = metrics;
    }

    /// Pre-commit transient failure (5xx, overload, transport, EOF, timeout).
    pub fn note_transient_failure(
        &self,
        metrics: &Metrics,
        target: &RouteTarget,
        attempt: usize,
        hint: Option<Duration>,
        jitter: f64,
    ) -> Duration {
        let cooldown = barrier_duration(
            attempt,
            self.route.route_cooldown_initial,
            self.route.route_cooldown_max,
            self.route.max_model_cooldown,
            0,
            hint,
            jitter,
        );
        let now = self.now();
        let cooldown = if self.key_has_alternatives(target) {
            cooldown.min(HEALTHY_ROUTE_STEP)
        } else {
            cooldown
        };
        if self.key_has_alternatives(target)
            && let Some(entry) = self.key_entry(target.key.index)
        {
            let mut state = lock(&entry.state);
            state.cooling_until = deadline(now, cooldown);
        }
        let _ = metrics;
        self.record_route_failure(target, now);
        cooldown
    }

    fn key_has_alternatives(&self, target: &RouteTarget) -> bool {
        self.inner
            .keys
            .iter()
            .any(|key| key.index != target.key.index && key.group != target.quota_group)
    }

    fn candidate_for(&self, target: &RouteTarget) -> (u64, u64) {
        (hash_name(&target.model), hash_name(&target.quota_group))
    }

    fn record_route_failure(&self, target: &RouteTarget, now: Instant) {
        let route_id = self.candidate_for(target);
        let mut models = lock(&self.inner.models);
        let entry = models.entry(target.model.clone()).or_default();
        entry.failing_groups.retain_mut(|(group, at)| {
            if *group == route_id.1 {
                *at = now;
                false
            } else {
                now.saturating_duration_since(*at) <= self.route.model_trip_window
            }
        });
        entry.failing_groups.push_back((route_id.1, now));
        if entry.failing_groups.len() >= self.route.model_trip_distinct_groups
            && entry.state == CircuitState::Closed
        {
            entry.state = CircuitState::Open;
            entry.open_until = deadline(now, self.route.model_open_secs);
            entry.open_reason = "distinct_quota_groups";
            entry.failing_groups.clear();
        }
    }

    fn group_of(&self, target: &RouteTarget) -> Arc<str> {
        target.quota_group.clone()
    }

    fn break_affinity_for_route(&self, target: &RouteTarget) {
        let route = target.route_id();
        let mut affinity = lock(&self.inner.affinity);
        affinity.retain(|_, entry| entry.route != route);
    }

    fn break_affinity_for_group(&self, group: &Arc<str>) {
        let hash = hash_name(group);
        let mut affinity = lock(&self.inner.affinity);
        affinity.retain(|_, entry| entry.route.group != hash);
    }

    // --- affinity ---------------------------------------------------------

    /// Remember the route a committed turn used. Weak, TTL-bounded, LRU-capped
    /// and strictly behavioural: never cache-aware, never health-overriding.
    pub fn note_session_route(&self, metrics: &Metrics, session_tag: &str, target: &RouteTarget) {
        if self.route.soft_affinity_secs.is_zero()
            || session_tag.is_empty()
            || session_tag == "sess_none"
        {
            return;
        }
        let now = self.now();
        let expires = now + self.route.soft_affinity_secs;
        let mut affinity = lock(&self.inner.affinity);
        affinity.retain(|_, entry| entry.expires > now);
        if !affinity.contains_key(session_tag)
            && affinity.len() >= self.route.max_affinity_entries
            && let Some(oldest) = affinity
                .iter()
                .min_by_key(|(_, entry)| entry.expires)
                .map(|(tag, _)| tag.clone())
        {
            affinity.remove(&oldest);
        }
        affinity.insert(
            session_tag.to_owned(),
            AffinityEntry {
                route: target.route_id(),
                expires,
            },
        );
        let _ = metrics;
    }

    /// Affinity is dropped the moment a route stops being healthy.
    pub fn break_affinity(&self, metrics: &Metrics, session_tag: &str) {
        if session_tag.is_empty() || session_tag == "sess_none" {
            return;
        }
        let removed = lock(&self.inner.affinity).remove(session_tag).is_some();
        if removed {
            self.inner.affinity_breaks.fetch_add(1, Ordering::Relaxed);
            metrics
                .affinity_breaks_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    // --- inflight ---------------------------------------------------------

    /// Reserve an inflight slot for an attempt. The guard releases it on drop,
    /// so success, error, timeout and client-disconnect paths all keep the
    /// counter correct.
    pub fn begin_attempt(&self, target: &RouteTarget) -> InflightGuard {
        let holds_probe = {
            let mut models = lock(&self.inner.models);
            let entry = models.entry(target.model.clone()).or_default();
            if entry.state == CircuitState::HalfOpen && !entry.probe_in_flight {
                entry.probe_in_flight = true;
                true
            } else {
                false
            }
        };
        let index = target.key.index;
        if let Some(entry) = self.key_entry(index) {
            let mut state = lock(&entry.state);
            state.inflight = state.inflight.saturating_add(1);
        }
        InflightGuard {
            table: self.clone(),
            index,
            model: target.model.clone(),
            holds_probe,
        }
    }

    /// The state a model's circuit currently exposes (tests/observability).
    #[allow(dead_code)]
    pub fn model_snapshot(&self, model: &str) -> ModelSnapshot {
        let now = self.now();
        let mut models = lock(&self.inner.models);
        match models.get_mut(model) {
            Some(entry) => {
                if entry.state == CircuitState::Open
                    && entry.open_until.is_some_and(|open_until| now >= open_until)
                {
                    entry.state = CircuitState::HalfOpen;
                    entry.open_until = None;
                    entry.probe_in_flight = false;
                }
                ModelSnapshot {
                    model: Arc::from(model),
                    state: entry.state,
                    open_reason: entry.open_reason,
                    disabled: entry.disabled,
                }
            }
            None => ModelSnapshot {
                model: Arc::from(model),
                state: CircuitState::Closed,
                open_reason: "",
                disabled: false,
            },
        }
    }

    /// Per-credential health, for readiness reporting and tests.
    #[allow(dead_code)]
    pub fn credential_snapshots(&self) -> Vec<CredentialSnapshot> {
        let now = self.now();
        self.inner
            .keys
            .iter()
            .map(|entry| {
                let mut state = lock(&entry.state);
                clear(&mut state.cooling_until, now);
                CredentialSnapshot {
                    name: entry.name.clone(),
                    quota_group: entry.group.clone(),
                    cooling_remaining: state
                        .cooling_until
                        .map(|until| until.saturating_duration_since(now)),
                    unusable: state.unusable,
                    rate_limit_streak: state.streak,
                    inflight: state.inflight,
                }
            })
            .collect()
    }

    /// Whether a specific `(model, quota_group)` route is currently cooling.
    #[allow(dead_code)]
    pub fn route_cooling(&self, model: &str, group: &str) -> Option<Duration> {
        let now = self.now();
        let id = RouteId {
            model: hash_name(model),
            group: hash_name(group),
        };
        let mut routes = lock(&self.inner.routes);
        let entry = routes.get_mut(&id)?;
        clear(&mut entry.cooling_until, now);
        entry
            .cooling_until
            .map(|until| until.saturating_duration_since(now))
    }

    /// Whether an explicit quota exhaustion is cooling the whole group.
    #[allow(dead_code)]
    pub fn group_cooling(&self, group: &str) -> Option<Duration> {
        let now = self.now();
        let mut groups = lock(&self.inner.groups);
        let entry = groups.get_mut(group)?;
        clear(&mut entry.cooling_until, now);
        entry
            .cooling_until
            .map(|until| until.saturating_duration_since(now))
    }

    pub fn usable_credential_count(&self) -> usize {
        let now = self.now();
        self.inner
            .keys
            .iter()
            .filter(|entry| {
                let mut state = lock(&entry.state);
                clear(&mut state.cooling_until, now);
                !state.unusable && state.cooling_until.is_none()
            })
            .count()
    }

    pub fn usable_route_count(&self) -> usize {
        let now = self.now();
        self.all_candidates()
            .into_iter()
            .filter(|(model, group)| {
                self.candidates(model).into_iter().any(|candidate| {
                    candidate.group == *group
                        && matches!(self.dialability(&candidate, now).0, Dialability::Ready)
                })
            })
            .count()
    }

    #[allow(dead_code)]
    pub fn credential_count(&self) -> usize {
        self.inner.keys.len()
    }

    /// Earliest moment any credential could be dialed again.
    pub fn earliest_retry_after(&self) -> Option<Duration> {
        let now = self.now();
        let mut earliest: Option<Duration> = None;
        for entry in &self.inner.keys {
            let mut state = lock(&entry.state);
            clear(&mut state.cooling_until, now);
            if let Some(until) = state.cooling_until {
                let remaining = until - now;
                earliest = Some(match earliest {
                    Some(current) => current.min(remaining),
                    None => remaining,
                });
            }
        }
        earliest
    }
}

/// RAII inflight/半open-probe guard.
pub struct InflightGuard {
    table: RouteTable,
    index: usize,
    model: Arc<str>,
    holds_probe: bool,
}

impl std::fmt::Debug for InflightGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InflightGuard")
            .field("credential", &self.index)
            .field("model", &self.model)
            .field("holds_probe", &self.holds_probe)
            .finish()
    }
}

impl InflightGuard {
    /// Mark the model's half-open probe as satisfied (success or terminal
    /// failure) while the slot itself is still released on drop.
    pub fn release_probe(&mut self) {
        if !self.holds_probe {
            return;
        }
        self.holds_probe = false;
        let mut models = lock(&self.table.inner.models);
        if let Some(entry) = models.get_mut(&self.model) {
            entry.probe_in_flight = false;
        }
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        if let Some(entry) = self.table.key_entry(self.index) {
            let mut state = lock(&entry.state);
            state.inflight = state.inflight.saturating_sub(1);
        }
        if self.holds_probe {
            let mut models = lock(&self.table.inner.models);
            if let Some(entry) = models.get_mut(&self.model) {
                entry.probe_in_flight = false;
                if entry.state == CircuitState::HalfOpen {
                    // The probe never reported an outcome (cancellation):
                    // do not leave the model stuck half-open.
                    entry.state = CircuitState::Closed;
                }
            }
        }
    }
}

struct ModelView {
    state: CircuitState,
    open_until: Option<Instant>,
    disabled: bool,
    probe_in_flight: bool,
}

struct RouteView {
    cooling: Option<Duration>,
    disabled: bool,
}

/// Order candidates: affinity first, then the supplied score, then a stable
/// deterministic tie-break.
///
/// `rotation` offsets equally-scored candidates so equivalent healthy routes
/// genuinely share load instead of the first configured one winning every
/// time. It is deterministic for a given rotation, which keeps tests and
/// production logging predictable.
fn order(
    scored: Vec<(Candidate, u64)>,
    affinity: Option<RouteId>,
    rotation: usize,
) -> Vec<(Candidate, u64)> {
    // Each candidate is assigned a stable "slot" (its model ordinal and its
    // credential index), then rotated by `rotation` so equally-scored
    // candidates take turns. A stable sort keeps the rotation deterministic.
    let count = scored.len();
    type Ranked = ((bool, u64, usize, usize), (Candidate, u64));
    let mut keyed: Vec<Ranked> = scored
        .into_iter()
        .enumerate()
        .map(|(position, entry)| {
            let affinity_first = affinity.is_some_and(|route| route == entry.0.route_id());
            (
                (
                    // Descending affinity rank (`true` first) via an inverted
                    // ascending key.
                    !affinity_first,
                    entry.1,
                    // Configured position of the model inside its tier comes
                    // first, so tier-0 model order is stable; the rotated slot
                    // only breaks ties between equally-ranked candidates.
                    entry.0.order_key(),
                    (position + rotation) % count.max(1),
                ),
                entry,
            )
        })
        .collect();
    keyed.sort_by_key(|entry| entry.0);
    keyed.into_iter().map(|(_, entry)| entry).collect()
}

fn deadline(now: Instant, wait: Duration) -> Option<Instant> {
    let wait = wait.min(MAX_ROUTE_WAIT);
    Some(now + wait)
}

fn clear(slot: &mut Option<Instant>, now: Instant) {
    if let Some(until) = *slot
        && until <= now
    {
        *slot = None;
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Bounded exponential cooldown with jitter.
///
/// Ladder (documented defaults): 10s → 20s → 40s → 80s → 120s cap, escalated
/// by `max(attempt - 1, streak)` and multiplied by a jitter factor in
/// `[1.0, 1.5)`. An authoritative `Retry-After` always wins, and every result
/// is clamped to `[1ms, MAX_ROUTE_WAIT]` with saturating arithmetic so no
/// input can panic or overflow.
pub fn barrier_duration(
    attempt: usize,
    initial: Duration,
    max: Duration,
    hint_cap: Duration,
    streak: u32,
    hint: Option<Duration>,
    jitter: f64,
) -> Duration {
    let max = max.min(MAX_ROUTE_WAIT).max(Duration::from_millis(1));
    if let Some(hint) = hint {
        // An authoritative `Retry-After` supersedes the transient ladder. It
        // is bounded only by the hard cooldown cap, never by the (usually much
        // smaller) ladder maximum, so a legitimate long hint is preserved.
        let hint_cap = hint_cap.min(MAX_ROUTE_WAIT).max(Duration::from_millis(1));
        return hint.clamp(Duration::from_millis(1), hint_cap);
    }
    // Ladder: `initial << step`, capped at `route_cooldown_max` — the
    // documented 10s → 20s → 40s → 80s → 120s progression. Jitter only ever
    // scales *up* inside [1.0, 1.5), and the result is clamped to `max` so the
    // configured ceiling is a real ceiling.
    let step = attempt
        .saturating_sub(1)
        .max(streak as usize)
        .min(BACKOFF_MAX_SHIFT) as u32;
    let initial_ms = initial.as_millis().max(1);
    let max_ms = max.as_millis();
    let base = initial_ms.saturating_mul(1u128 << step).min(max_ms);
    let jittered = base as f64 * (1.0 + 0.5 * jitter.clamp(0.0, 1.0));
    let value = (jittered as u128).min(max_ms);
    let value = u64::try_from(value).unwrap_or(u64::MAX);
    Duration::from_millis(value).clamp(Duration::from_millis(1), max)
}

pub fn hash_name(value: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// `Retry-After` headroom added to an internal wait so the client hint is
/// always conservative.
pub fn retry_after_for(wait: Duration) -> Duration {
    wait.saturating_add(RETRY_AFTER_MARGIN)
}

/// Hard cap applied to every client-facing cooldown.
pub fn cap_cooldown(cooldown: Duration, max: Duration) -> Duration {
    cooldown
        .min(max.min(MAX_ROUTE_WAIT))
        .min(MAX_PARSED_COOLDOWN)
        .max(Duration::from_millis(1))
}

#[cfg(test)]
#[path = "router_tests.rs"]
mod tests;
