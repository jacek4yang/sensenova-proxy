#![cfg(test)]
//! Deterministic routing tests. They drive `RouteTable` directly (no network)
//! and pin the policy invariants documented in `router.rs`.

use crate::config::{ProfileConfig, SensenovaKeyConfig};
use crate::router::*;

fn key(name: &str, group: &str) -> SensenovaKeyConfig {
    SensenovaKeyConfig {
        name: name.into(),
        api_key: format!("secret-{name}"),
        enabled: true,
        quota_group: group.into(),
    }
}

fn table(keys: Vec<SensenovaKeyConfig>) -> RouteTable {
    table_with(keys, |_| {})
}

fn table_with(
    keys: Vec<SensenovaKeyConfig>,
    configure: impl FnOnce(&mut crate::config::RoutingConfig),
) -> RouteTable {
    let mut config = crate::config::Config::default();
    config.server.api_key = "gateway".into();
    config.routing.profiles = crate::config::builtin_profiles();
    config.sensenova_api_keys = keys;
    configure(&mut config.routing);
    let pool = crate::pool::KeyPool::new(&config.sensenova_api_keys);
    RouteTable::new(&config, pool)
}

fn hard() -> RouteSpec {
    RouteSpec::Profile(crate::config::defaults::HARD_PROFILE.to_owned())
}

fn fast() -> RouteSpec {
    RouteSpec::Profile(crate::config::defaults::FAST_PROFILE.to_owned())
}

fn pick(
    table: &RouteTable,
    spec: &RouteSpec,
    skipped: &HashSet<(u64, u64, usize)>,
) -> Option<RouteTarget> {
    table
        .plan(spec, "sess_none", 0, skipped, &Metrics::new())
        .ok()
        .map(|plan| plan.target)
}

fn empty() -> HashSet<(u64, u64, usize)> {
    HashSet::new()
}

#[test]
fn hard_profile_pool_is_the_documented_quality_isolation() {
    let table = table(vec![key("k1", "account-a")]);
    let first = pick(&table, &hard(), &empty()).unwrap();
    assert!(
        matches!(first.model_str(), "glm-5.2" | "deepseek-v4-pro"),
        "tier 0 must win, got {}",
        first.model_str()
    );
    assert_eq!(first.tier, 0);

    // Neither fast model is in the hard pool at all.
    let hard_models = table.profile_models("claude-coding-hard");
    assert!(!hard_models.iter().any(|model| model == "deepseek-v4-flash"));
    assert!(
        !hard_models
            .iter()
            .any(|model| model == "sensenova-6.8-flash-lite")
    );
    assert!(hard_models.iter().any(|model| model == "kimi-k3"));
}

#[test]
fn fast_profile_pool_contains_only_fast_models() {
    let table = table(vec![key("k1", "account-a")]);
    let first = pick(&table, &fast(), &empty()).unwrap();
    assert!(
        matches!(
            first.model_str(),
            "deepseek-v4-flash" | "sensenova-6.8-flash-lite"
        ),
        "fast profile must only use fast models, got {}",
        first.model_str()
    );
    let models = table.profile_models("claude-coding-fast");
    assert!(!models.iter().any(|model| model == "glm-5.2"));
    assert!(!models.iter().any(|model| model == "kimi-k3"));
}

#[test]
fn hard_fails_honestly_instead_of_degrading_into_fast() {
    let table = table(vec![key("k1", "account-a")]);
    let metrics = Metrics::new();
    let spec = hard();
    let mut skipped = empty();
    let mut seen: Vec<String> = Vec::new();
    for attempt in 0..8 {
        match table.plan(&spec, "sess_none", attempt, &skipped, &metrics) {
            Ok(plan) => {
                seen.push(plan.target.model_str().to_owned());
                // Every route hard-fails: nothing healthy remains, and the
                // profile still refuses to reach for a weak model.
                table.note_model_missing(&metrics, &plan.target);
                skipped.insert(plan.target.identity());
            }
            Err(_) => break,
        }
    }
    assert!(!seen.is_empty());
    for model in &seen {
        assert!(
            matches!(model.as_str(), "glm-5.2" | "deepseek-v4-pro" | "kimi-k3"),
            "hard profile leaked {model}"
        );
    }
}

#[test]
fn one_429_cools_only_the_model_and_group() {
    let table = table(vec![key("k1", "account-a"), key("k2", "account-b")]);
    let metrics = Metrics::new();
    let spec = hard();
    let first = table
        .plan(&spec, "sess_none", 0, &empty(), &metrics)
        .unwrap();
    let model = first.target.model_str().to_owned();
    let group = first.target.quota_group_str().to_owned();
    let other = if group == "account-a" {
        "account-b"
    } else {
        "account-a"
    };
    table.note_rate_limited(&metrics, &first.target, 1, 0, None, 0.0);
    assert!(
        table.route_cooling(&model, &group).is_some(),
        "the failing (model, group) route must cool"
    );
    assert!(
        table.route_cooling(&model, other).is_none(),
        "the other account must stay warm"
    );
    assert_eq!(
        table.model_snapshot(&model).state,
        CircuitState::Closed,
        "one account must not globally disable a model"
    );
}

#[test]
fn repeated_failures_from_one_group_never_trip_the_model() {
    let table = table_with(vec![key("k1", "account-a")], |routing| {
        routing.model_trip_distinct_groups = 2;
    });
    let metrics = Metrics::new();
    let spec = hard();
    let mut skipped = empty();
    for attempt in 0..6 {
        if let Ok(plan) = table.plan(&spec, "sess_none", attempt, &skipped, &metrics) {
            table.note_rate_limited(
                &metrics,
                &plan.target,
                attempt + 1,
                attempt as u32,
                None,
                0.0,
            );
            skipped.insert(plan.target.identity());
        }
    }
    assert_eq!(
        table.model_snapshot("glm-5.2").state,
        CircuitState::Closed,
        "one account can never trip a model by itself"
    );
}

#[test]
fn two_distinct_groups_inside_the_window_trip_the_model() {
    let table = table_with(
        vec![key("k1", "account-a"), key("k2", "account-b")],
        |routing| {
            routing.model_trip_distinct_groups = 2;
            routing.model_trip_window_secs = 60;
        },
    );
    let metrics = Metrics::new();
    let spec = hard();
    let mut skipped = empty();
    for attempt in 0..2 {
        let plan = table
            .plan(&spec, "sess_none", attempt, &skipped, &metrics)
            .unwrap();
        table.note_rate_limited(&metrics, &plan.target, attempt + 1, 0, None, 0.0);
        skipped.insert(plan.target.identity());
    }
    assert_eq!(
        table.model_snapshot("glm-5.2").state,
        CircuitState::Open,
        "two distinct groups must trip the model"
    );
}

#[test]
fn a_missing_model_is_disabled_for_that_route_only() {
    let table = table(vec![key("k1", "account-a")]);
    let metrics = Metrics::new();
    let plan = table
        .plan(&hard(), "sess_none", 0, &empty(), &metrics)
        .unwrap();
    let failed = plan.target.model_str().to_owned();
    table.note_model_missing(&metrics, &plan.target);
    assert!(table.route_disabled(&failed, "account-a"));

    let mut skipped = empty();
    skipped.insert(plan.target.identity());
    let next = pick(&table, &hard(), &skipped).unwrap();
    assert_ne!(
        next.model_str(),
        failed,
        "the disabled model must be skipped"
    );
}

#[test]
fn quota_exhaustion_cools_the_whole_group_and_spares_others() {
    let table = table(vec![key("k1", "account-a"), key("k2", "account-b")]);
    let metrics = Metrics::new();
    let spec = hard();
    let first = table
        .plan(&spec, "sess_none", 0, &empty(), &metrics)
        .unwrap();
    assert_eq!(
        first.target.quota_group_str(),
        "account-a",
        "selection starts in configured order"
    );
    table.note_quota_exhausted(&metrics, &first.target, Duration::from_secs(60));
    assert!(table.group_cooling("account-a").is_some());
    assert!(table.group_cooling("account-b").is_none());

    let mut skipped = empty();
    skipped.insert(first.target.identity());
    let next = pick(&table, &spec, &skipped).unwrap();
    assert_eq!(
        next.quota_group_str(),
        "account-b",
        "only another quota group may serve"
    );
}

#[test]
fn unauthorized_disables_only_the_rejected_credential() {
    let table = table(vec![key("k1", "account-a"), key("k2", "account-a")]);
    let metrics = Metrics::new();
    let spec = hard();
    let first = table
        .plan(&spec, "sess_none", 0, &empty(), &metrics)
        .unwrap();
    let rejected = first.target.key.name.clone();
    table.note_authentication_failure(&first.target);
    let disabled: Vec<_> = table
        .credential_snapshots()
        .iter()
        .filter(|snapshot| snapshot.unusable)
        .map(|snapshot| snapshot.name.clone())
        .collect();
    assert_eq!(disabled, vec![rejected]);
    assert_eq!(table.usable_credential_count(), 1);
}

#[test]
fn exponential_cooldown_ladder_is_bounded_and_jittered() {
    let initial = Duration::from_secs(10);
    let max = Duration::from_secs(120);
    let hint_cap = Duration::from_secs(86_400);
    let ladder = |attempt: usize| barrier_duration(attempt, initial, max, hint_cap, 0, None, 0.0);
    assert_eq!(ladder(1), Duration::from_secs(10));
    assert_eq!(ladder(2), Duration::from_secs(20));
    assert_eq!(ladder(3), Duration::from_secs(40));
    assert_eq!(ladder(4), Duration::from_secs(80));
    assert_eq!(ladder(5), Duration::from_secs(120), "capped at the max");
    assert_eq!(ladder(50), Duration::from_secs(120), "never overflows");
    assert_eq!(
        barrier_duration(1, initial, max, hint_cap, 0, None, 1.0),
        Duration::from_millis(15_000),
        "jitter scales up within [1.0, 1.5)"
    );
    assert_eq!(
        barrier_duration(9, initial, max, hint_cap, 0, None, 1.0),
        Duration::from_secs(120),
        "jitter never exceeds the cap"
    );
    assert_eq!(
        barrier_duration(1, initial, max, hint_cap, 3, None, 0.0),
        Duration::from_secs(80),
        "a streak escalates the ladder too"
    );
}

#[test]
fn authoritative_retry_after_wins_over_the_ladder() {
    let initial = Duration::from_secs(10);
    let max = Duration::from_secs(120);
    let hint_cap = Duration::from_secs(86_400);
    assert_eq!(
        barrier_duration(
            1,
            initial,
            max,
            hint_cap,
            0,
            Some(Duration::from_secs(900)),
            0.0
        ),
        Duration::from_secs(900),
        "a long authoritative hint is preserved, not clamped to the ladder"
    );
    assert_eq!(
        barrier_duration(
            9,
            initial,
            max,
            hint_cap,
            9,
            Some(Duration::from_millis(500)),
            0.0
        ),
        Duration::from_millis(500)
    );
    assert_eq!(
        barrier_duration(
            1,
            initial,
            max,
            Duration::from_secs(30),
            0,
            Some(Duration::from_secs(9_999)),
            0.0
        ),
        Duration::from_secs(30),
        "a hostile hint is clamped to the hard cap, not the ladder max"
    );
    assert!(
        barrier_duration(1, initial, max, hint_cap, 0, Some(Duration::ZERO), 0.0)
            >= Duration::from_millis(1),
        "a zero cooldown would spin"
    );
}

#[test]
fn equivalent_routes_spread_load_instead_of_always_dialing_the_first() {
    let table = table(vec![
        key("k1", "account-a"),
        key("k2", "account-b"),
        key("k3", "account-c"),
    ]);
    let metrics = Metrics::new();
    let spec = hard();
    let mut chosen = Vec::new();
    for _ in 0..6 {
        let plan = table
            .plan(&spec, "sess_none", 0, &empty(), &metrics)
            .unwrap();
        chosen.push(plan.target.key.name.to_string());
    }
    let distinct: HashSet<&String> = chosen.iter().collect();
    assert!(
        distinct.len() > 1,
        "equivalent routes must share load, saw {chosen:?}"
    );
}

#[test]
fn inflight_is_tracked_and_released_through_the_guard() {
    let table = table(vec![key("k1", "account-a")]);
    let metrics = Metrics::new();
    let plan = table
        .plan(&hard(), "sess_none", 0, &empty(), &metrics)
        .unwrap();
    let guard = table.begin_attempt(&plan.target);
    let inflight = |table: &RouteTable| -> usize {
        table
            .credential_snapshots()
            .iter()
            .map(|snapshot| snapshot.inflight)
            .sum()
    };
    assert_eq!(inflight(&table), 1, "the guard must hold the slot");
    drop(guard);
    assert_eq!(inflight(&table), 0, "the guard must release the slot");
}

#[test]
fn affinity_hits_breaks_and_is_bounded() {
    let table = table_with(
        vec![key("k1", "account-a"), key("k2", "account-b")],
        |routing| {
            routing.soft_affinity_secs = 300;
            routing.max_affinity_entries = 2;
        },
    );
    let metrics = Metrics::new();
    let spec = hard();
    let plan = table
        .plan(&spec, "sess_one", 0, &empty(), &metrics)
        .unwrap();
    table.note_session_route(&metrics, "sess_one", &plan.target);
    assert_eq!(table.affinity_len(), 1);

    let again = table
        .plan(&spec, "sess_one", 0, &empty(), &metrics)
        .unwrap();
    assert_eq!(
        (again.target.model_str(), again.target.quota_group_str()),
        (plan.target.model_str(), plan.target.quota_group_str()),
        "the remembered session resolves back to the same route"
    );

    for tag in ["sess_two", "sess_three"] {
        let plan = table.plan(&spec, tag, 0, &empty(), &metrics).unwrap();
        table.note_session_route(&metrics, tag, &plan.target);
    }
    assert!(
        table.affinity_len() <= 2,
        "affinity must stay within max_affinity_entries, got {}",
        table.affinity_len()
    );

    table.break_affinity(&metrics, "sess_three");
    assert!(table.affinity_break_count() >= 1);
    assert_eq!(table.affinity_len(), 1);
    table.break_affinity(&metrics, "sess_none");
    table.break_affinity(&metrics, "");
    assert_eq!(table.affinity_len(), 1, "empty tags are never tracked");
}

#[test]
fn affinity_never_overrides_health() {
    let table = table_with(
        vec![key("k1", "account-a"), key("k2", "account-b")],
        |routing| routing.soft_affinity_secs = 300,
    );
    let metrics = Metrics::new();
    let spec = hard();
    let plan = table
        .plan(&spec, "sess_one", 0, &empty(), &metrics)
        .unwrap();
    table.note_session_route(&metrics, "sess_one", &plan.target);
    table.note_quota_exhausted(&metrics, &plan.target, Duration::from_secs(60));
    let next = table
        .plan(&spec, "sess_one", 0, &empty(), &metrics)
        .unwrap();
    assert_ne!(
        next.target.quota_group_str(),
        plan.target.quota_group_str(),
        "a cooled route must not be selected through affinity"
    );
}

#[test]
fn hard_quality_tier_outranks_latency() {
    let table = table(vec![key("k1", "account-a")]);
    let metrics = Metrics::new();
    let plan = table
        .plan(&hard(), "sess_none", 0, &empty(), &metrics)
        .unwrap();
    assert_eq!(
        plan.target.tier, 0,
        "hard serves tier 0 while it is dialable"
    );
}

#[test]
fn fast_scoring_prefers_the_lower_latency_route() {
    let table = table(vec![key("k1", "account-a"), key("k2", "account-b")]);
    let metrics = Metrics::new();
    let spec = fast();
    for attempt in 0..12 {
        let plan = table
            .plan(&spec, "sess_none", attempt, &empty(), &metrics)
            .unwrap();
        let slow = plan.target.quota_group_str() == "account-a";
        let ttft = if slow {
            Duration::from_millis(900)
        } else {
            Duration::from_millis(20)
        };
        table.note_success(&metrics, &plan.target, Some(ttft), ttft);
    }
    let chosen = table
        .plan(&spec, "sess_none", 0, &empty(), &metrics)
        .unwrap();
    assert_eq!(
        chosen.target.quota_group_str(),
        "account-b",
        "the latency-optimized profile must prefer the faster account"
    );
}

#[test]
fn literal_models_are_routed_as_themselves() {
    let table = table(vec![key("k1", "account-a"), key("k2", "account-b")]);
    let metrics = Metrics::new();
    let spec = RouteSpec::Literal("deepseek-v4-pro".into());
    let plan = table
        .plan(&spec, "sess_none", 0, &empty(), &metrics)
        .unwrap();
    assert_eq!(plan.target.model_str(), "deepseek-v4-pro");
    let next = table.plan(&spec, "sess_none", 1, &empty(), &metrics);
    assert!(next.is_ok(), "quota-group failover applies to literals too");
}

#[test]
fn resolution_prefers_alias_then_profile_then_default_mapping() {
    let table = table(vec![key("k1", "account-a")]);
    let mut models = ModelsConfig {
        default: "sensenova-6.8-flash-lite".into(),
        map_unknown_to_default: true,
        ..ModelsConfig::default()
    };
    models
        .aliases
        .insert("claude-sensenova".into(), "sensenova-6.8-flash-lite".into());
    models
        .aliases
        .insert("claude-sonnet-4-5".into(), "claude-coding-hard".into());

    assert_eq!(
        table.resolve(&models, "claude-sonnet-4-5"),
        Resolution::Profile("claude-coding-hard".into())
    );
    assert_eq!(
        table.resolve(&models, "claude-coding-fast"),
        Resolution::Profile("claude-coding-fast".into())
    );
    assert_eq!(
        table.resolve(&models, "claude-sensenova"),
        Resolution::Literal("sensenova-6.8-flash-lite".into())
    );
    assert_eq!(
        table.resolve(&models, "deepseek-v4-pro"),
        Resolution::Literal("deepseek-v4-pro".into())
    );
    assert_eq!(
        table.resolve(&models, "claude-3-5-haiku-20241022"),
        Resolution::Literal("sensenova-6.8-flash-lite".into())
    );
    assert_eq!(
        table.resolve(&models, "some-future-model"),
        Resolution::Literal("some-future-model".into())
    );
}

#[test]
fn readiness_counts_routes_not_credentials() {
    let table = table(vec![key("k1", "account-a"), key("k2", "account-b")]);
    let metrics = Metrics::new();
    assert!(table.usable_route_count() > 0);
    let plan = table
        .plan(&hard(), "sess_none", 0, &empty(), &metrics)
        .unwrap();
    table.note_model_missing(&metrics, &plan.target);
    assert!(
        table.usable_route_count() > 0,
        "a single dead model must not make the proxy unready"
    );
}

#[test]
fn route_target_debug_never_exposes_credentials() {
    let table = table(vec![key("k1", "account-a")]);
    let plan = table
        .plan(&hard(), "sess_none", 0, &empty(), &Metrics::new())
        .unwrap();
    let rendered = format!("{:?}", plan.target);
    assert!(!rendered.contains("secret-k1"));
    assert!(rendered.contains("k1"));
}

#[test]
fn profiles_keep_their_pools_disjoint() {
    let table = table(vec![key("k1", "account-a")]);
    let metrics = Metrics::new();
    let hard_plan = table
        .plan(&hard(), "sess_none", 0, &empty(), &metrics)
        .unwrap();
    let fast_plan = table
        .plan(&fast(), "sess_none", 0, &empty(), &metrics)
        .unwrap();
    assert_eq!(hard_plan.target.tier, 0);
    assert_eq!(fast_plan.target.tier, 0);
    assert_ne!(
        hard_plan.target.model_str(),
        fast_plan.target.model_str(),
        "the hard and fast pools never overlap"
    );
}

#[test]
fn unknown_profile_is_not_routable() {
    let table = table(vec![key("k1", "account-a")]);
    let metrics = Metrics::new();
    let spec = RouteSpec::Profile("no-such-profile".into());
    assert!(
        table
            .plan(&spec, "sess_none", 0, &empty(), &metrics)
            .is_err()
    );
}

#[test]
fn affinity_is_never_created_for_anonymous_sessions() {
    let table = table(vec![key("k1", "account-a")]);
    let metrics = Metrics::new();
    let plan = table
        .plan(&hard(), "sess_none", 0, &empty(), &metrics)
        .unwrap();
    table.note_session_route(&metrics, "sess_none", &plan.target);
    assert_eq!(
        table.affinity_len(),
        0,
        "a request without a session tag must not create affinity state"
    );
}

#[test]
fn profile_labels_and_models_are_exposed() {
    let table = table(vec![key("k1", "account-a")]);
    let mut names = table.profile_names();
    names.sort();
    assert_eq!(names, vec!["claude-coding-fast", "claude-coding-hard"]);
    assert!(table.is_profile("claude-coding-hard"));
    assert!(!table.is_profile("nope"));
    assert_eq!(hard().label(), "claude-coding-hard");
    assert!(matches!(hard(), RouteSpec::Profile(_)));
}

#[test]
fn custom_profiles_with_cross_tier_fallback_are_honoured() {
    let mut config = crate::config::Config::default();
    config.server.api_key = "gateway".into();
    let mut profiles = crate::config::Profiles::new();
    profiles.insert(
        "custom".into(),
        ProfileConfig {
            latency_optimized: false,
            allow_lower_tier_on_unavailable: true,
            tiers: vec![
                TierConfig {
                    models: vec!["glm-5.2".into()],
                },
                TierConfig {
                    models: vec!["kimi-k3".into()],
                },
            ],
        },
    );
    config.routing.profiles = profiles;
    config.sensenova_api_keys = vec![key("k1", "account-a")];
    let pool = crate::pool::KeyPool::new(&config.sensenova_api_keys);
    let table = RouteTable::new(&config, pool);
    let metrics = Metrics::new();
    let spec = RouteSpec::Profile("custom".into());
    let plan = table
        .plan(&spec, "sess_none", 0, &empty(), &metrics)
        .unwrap();
    assert_eq!(plan.target.model_str(), "glm-5.2");
    // Disable tier 0 entirely; with cross-tier fallback the lower tier
    // may now serve.
    table.note_model_missing(&metrics, &plan.target);
    let next = table
        .plan(&spec, "sess_none", 0, &empty(), &metrics)
        .unwrap();
    assert_eq!(next.target.model_str(), "kimi-k3");
    assert_eq!(next.target.tier, 1);
}

#[test]
fn cooldown_caps_are_update_safe() {
    assert_eq!(
        cap_cooldown(Duration::ZERO, Duration::from_secs(10)),
        Duration::from_millis(1)
    );
    assert_eq!(
        cap_cooldown(Duration::from_secs(100), Duration::from_secs(10)),
        Duration::from_secs(10)
    );
    // Even an absurd configured cap cannot exceed the internal ceiling.
    assert_eq!(
        cap_cooldown(MAX_PARSED_COOLDOWN, Duration::from_secs(u64::MAX)),
        MAX_ROUTE_WAIT
    );
    assert!(retry_after_for(Duration::from_secs(1)) > Duration::from_secs(1));
    assert!(MAX_ROUTE_WAIT <= MAX_PARSED_COOLDOWN);
    // The ladder shift must stay well inside a u128 so no attempt count can
    // overflow it.
    assert!(
        u32::try_from(BACKOFF_MAX_SHIFT).is_ok_and(|shift| shift < 64),
        "the exponential ladder must not overflow its shift"
    );
}

#[test]
fn barrier_is_deterministic_for_frozen_input() {
    let initial = Duration::from_secs(10);
    let max = Duration::from_secs(120);
    for attempt in 0..12usize {
        let first = barrier_duration(
            attempt,
            initial,
            max,
            Duration::from_secs(86_400),
            0,
            None,
            0.25,
        );
        let second = barrier_duration(
            attempt,
            initial,
            max,
            Duration::from_secs(86_400),
            0,
            None,
            0.25,
        );
        assert_eq!(first, second);
        assert!(first >= Duration::from_millis(1));
        assert!(first <= max);
    }
}

// ---------------------------------------------------------------------------
// Production-hardening regressions (fix/router-hardening)
// ---------------------------------------------------------------------------

/// A hard profile with `allow_lower_tier_on_unavailable` disabled, for tests
/// that need the old conservative behaviour.
fn hard_conservative() -> RouteSpec {
    RouteSpec::Profile("hard-conservative".into())
}

fn table_with_profiles(
    keys: Vec<SensenovaKeyConfig>,
    profiles: crate::config::Profiles,
    configure: impl FnOnce(&mut crate::config::RoutingConfig),
) -> RouteTable {
    let mut config = crate::config::Config::default();
    config.server.api_key = "gateway".into();
    config.routing.profiles = profiles;
    config.sensenova_api_keys = keys;
    configure(&mut config.routing);
    let pool = crate::pool::KeyPool::new(&config.sensenova_api_keys);
    RouteTable::new(&config, pool)
}

/// Two-tier hard-conservative profile: tier 0 glm/deepseek, tier 1 kimi, with
/// lower-tier fallback explicitly disabled.
fn conservative_profiles() -> crate::config::Profiles {
    let mut profiles = crate::config::Profiles::new();
    profiles.insert(
        "hard-conservative".into(),
        ProfileConfig {
            latency_optimized: false,
            allow_lower_tier_on_unavailable: false,
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
    profiles
}

// --- Tier fallback ---------------------------------------------------------

/// Healthy tier 0 still beats tier 1 — the fallback flag changes nothing when
/// a dialable higher-quality route exists.
#[test]
fn healthy_tier_zero_beats_kimi_even_with_fallback_enabled() {
    let table = table(vec![key("k1", "account-a")]);
    let plan = pick(&table, &hard(), &empty()).unwrap();
    assert_eq!(plan.tier, 0);
    assert_ne!(plan.model_str(), "kimi-k3");
}

/// The headline fix: tier-0 routes merely *cooling* must not block tier 1.
#[test]
fn tier_zero_cooling_allows_healthy_kimi() {
    let table = table(vec![key("k1", "account-a")]);
    let metrics = Metrics::new();
    let mut skipped = empty();
    // Cool both tier-0 routes with a generic 429 each.
    for attempt in 0..2 {
        let plan = table
            .plan(&hard(), "sess_none", attempt, &skipped, &metrics)
            .unwrap();
        table.note_rate_limited(&metrics, &plan.target, attempt + 1, 0, None, 0.0);
        skipped.insert(plan.target.identity());
    }
    let plan = table
        .plan(&hard(), "sess_none", 0, &skipped, &metrics)
        .unwrap();
    assert_eq!(
        plan.target.model_str(),
        "kimi-k3",
        "cooling tier 0 must yield to the healthy hard tier 1"
    );
    assert_eq!(plan.target.tier, 1);
    assert_eq!(plan.stepped_down, 1);
}

/// An open model circuit is a *temporary* unavailability: tier 1 serves.
#[test]
fn tier_zero_model_circuit_open_allows_healthy_kimi() {
    let table = table(vec![key("k1", "account-a"), key("k2", "account-b")]);
    let metrics = Metrics::new();
    // Trip the glm-5.2 circuit with two distinct groups.
    let mut skipped = empty();
    for attempt in 0..2 {
        let plan = table
            .plan(&hard(), "sess_none", attempt, &skipped, &metrics)
            .unwrap();
        table.note_rate_limited(&metrics, &plan.target, attempt + 1, 0, None, 0.0);
        skipped.insert(plan.target.identity());
    }
    assert_eq!(
        table.model_snapshot("glm-5.2").state,
        CircuitState::Open,
        "setup: the tier-0 model circuit must be open"
    );
    // The other tier-0 model is still healthy; cool both of its routes too.
    let mut skipped2 = empty();
    for attempt in 0..2 {
        let plan = table
            .plan(&hard(), "sess_none", attempt, &skipped2, &metrics)
            .unwrap();
        assert_eq!(plan.target.model_str(), "deepseek-v4-pro");
        table.note_rate_limited(&metrics, &plan.target, attempt + 1, 0, None, 0.0);
        skipped2.insert(plan.target.identity());
    }
    let plan = table
        .plan(&hard(), "sess_none", 0, &skipped2, &metrics)
        .unwrap();
    assert_eq!(
        plan.target.model_str(),
        "kimi-k3",
        "an open tier-0 circuit plus cooled routes must yield to tier 1"
    );
}

/// A cooled quota group is a *temporary* unavailability: tier 1 serves.
#[test]
fn tier_zero_group_cooldown_allows_healthy_kimi() {
    let table = table(vec![key("k1", "account-a"), key("k2", "account-b")]);
    let metrics = Metrics::new();
    // Explicit quota exhaustion cools the whole of group A.
    let target_a = pick(&table, &hard(), &empty()).unwrap();
    assert_eq!(target_a.quota_group_str(), "account-a");
    table.note_quota_exhausted(&metrics, &target_a, Duration::from_secs(60));
    // Group B still has healthy tier-0 routes; cool them both so only tier 1
    // (kimi on group B) remains.
    let mut skipped = empty();
    for attempt in 0..2 {
        let plan = table
            .plan(&hard(), "sess_none", attempt, &skipped, &metrics)
            .unwrap();
        assert_eq!(plan.target.quota_group_str(), "account-b");
        assert_ne!(plan.target.model_str(), "kimi-k3");
        table.note_rate_limited(&metrics, &plan.target, attempt + 1, 0, None, 0.0);
        skipped.insert(plan.target.identity());
    }
    let next = table
        .plan(&hard(), "sess_none", 0, &skipped, &metrics)
        .unwrap();
    assert_eq!(
        next.target.model_str(),
        "kimi-k3",
        "a group cooldown plus cooled tier-0 routes must yield to tier 1"
    );
    assert_eq!(next.target.tier, 1);
}

/// With fallback disabled (the old conservative semantics), a cooling tier 0
/// still reports its wait instead of degrading.
#[test]
fn conservative_profile_still_waits_for_cooling_tier_zero() {
    let table = table_with_profiles(
        vec![key("k1", "account-a")],
        conservative_profiles(),
        |_| {},
    );
    let metrics = Metrics::new();
    let target = pick(&table, &hard_conservative(), &empty()).unwrap();
    table.note_quota_exhausted(&metrics, &target, Duration::from_secs(60));
    let outcome = table.plan(&hard_conservative(), "sess_none", 0, &empty(), &metrics);
    assert!(
        outcome.is_err(),
        "without the fallback flag a cooling tier must not degrade"
    );
    assert!(
        outcome.unwrap_err().wait.is_some(),
        "the wait for the cooling tier must be reported"
    );
}

/// Hard fallback never reaches DeepSeek V4 Flash — not through cooling, not
/// through circuits, not through any combination.
#[test]
fn hard_fallback_never_reaches_deepseek_flash() {
    let table = table(vec![key("k1", "account-a")]);
    let metrics = Metrics::new();
    let mut skipped = empty();
    let mut seen: Vec<String> = Vec::new();
    for attempt in 0..8 {
        match table.plan(&hard(), "sess_none", attempt, &skipped, &metrics) {
            Ok(plan) => {
                seen.push(plan.target.model_str().to_owned());
                // Fail every route as it is visited, mixing failure kinds.
                if attempt % 2 == 0 {
                    table.note_model_missing(&metrics, &plan.target);
                } else {
                    table.note_rate_limited(&metrics, &plan.target, attempt, 0, None, 0.0);
                }
                skipped.insert(plan.target.identity());
            }
            Err(_) => break,
        }
    }
    assert!(!seen.is_empty());
    for model in &seen {
        assert_ne!(model, "deepseek-v4-flash", "hard leaked into the fast pool");
        assert_ne!(model, "sensenova-6.8-flash-lite");
    }
    // The fast pool is unreachable through the hard spec even with everything
    // else gone.
    assert!(
        table
            .plan(&hard(), "sess_none", 0, &skipped, &metrics)
            .is_err()
    );
}

/// Same invariant through the other fast model, exercised by the disjoint-pool
/// assertion as well.
#[test]
fn hard_fallback_never_reaches_sensenova_flash_lite() {
    let table = table(vec![key("k1", "account-a")]);
    let hard_models = table.profile_models("claude-coding-hard");
    assert!(
        !hard_models
            .iter()
            .any(|model| model == "sensenova-6.8-flash-lite")
    );
    // And a literal request for the fast model never inherits the hard pool.
    let literal = pick(
        &table,
        &RouteSpec::Literal("sensenova-6.8-flash-lite".into()),
        &empty(),
    )
    .unwrap();
    assert_eq!(literal.model_str(), "sensenova-6.8-flash-lite");
    assert_eq!(literal.tier, 0, "a literal model is its own single tier");
}

/// Latency must never let tier 1 beat a *healthy* tier 0.
#[test]
fn latency_never_lets_tier_one_beat_healthy_tier_zero() {
    let table = table(vec![key("k1", "account-a")]);
    let metrics = Metrics::new();
    // Give every credential excellent latency history; the hard profile is
    // not latency-optimized anyway, and tier order is structural.
    for _ in 0..6 {
        let target = pick(&table, &hard(), &empty()).unwrap();
        table.note_success(
            &metrics,
            &target,
            Some(Duration::from_millis(1)),
            Duration::from_millis(1),
        );
    }
    let target = pick(&table, &hard(), &empty()).unwrap();
    assert_eq!(
        target.tier, 0,
        "tier order is structural, never latency-driven"
    );
}

// --- 404 handling ----------------------------------------------------------

/// One 404 disables only `(model, group)`.
#[test]
fn one_404_disables_only_the_model_and_group() {
    let table = table(vec![key("k1", "account-a"), key("k2", "account-b")]);
    let metrics = Metrics::new();
    let target = pick(&table, &hard(), &empty()).unwrap();
    assert_eq!(target.quota_group_str(), "account-a");
    table.note_model_missing(&metrics, &target);
    assert!(table.route_disabled("glm-5.2", "account-a"));
    assert!(
        !table.model_snapshot("glm-5.2").disabled,
        "one account's 404 must not disable the model globally"
    );
    // The same model on another group stays dialable.
    let mut skipped = empty();
    skipped.insert(target.identity());
    let next = pick(&table, &hard(), &skipped).unwrap();
    assert_eq!(next.model_str(), "glm-5.2");
    assert_eq!(next.quota_group_str(), "account-b");
}

/// A success on another group is the strongest counter-evidence.
#[test]
fn success_on_another_group_prevents_global_model_disable() {
    let table = table(vec![key("k1", "account-a"), key("k2", "account-b")]);
    let metrics = Metrics::new();
    let first = pick(&table, &hard(), &empty()).unwrap();
    table.note_model_missing(&metrics, &first);
    // Now group B serves the model: plan with the first route skipped.
    let mut skipped = empty();
    skipped.insert(first.identity());
    let second = pick(&table, &hard(), &skipped).unwrap();
    assert_eq!(second.quota_group_str(), "account-b");
    table.note_success(&metrics, &second, None, Duration::from_millis(5));
    assert!(
        !table.model_snapshot("glm-5.2").disabled,
        "a success must clear missing evidence"
    );
}

/// Two distinct groups returning 404 inside the window disable the model.
#[test]
fn two_distinct_groups_404_disable_the_model() {
    let table = table(vec![key("k1", "account-a"), key("k2", "account-b")]);
    let metrics = Metrics::new();
    let first = pick(&table, &hard(), &empty()).unwrap();
    table.note_model_missing(&metrics, &first);
    // Second distinct group: plan with the first route skipped.
    let mut skipped = empty();
    skipped.insert(first.identity());
    let plan_b = table
        .plan(&hard(), "sess_none", 0, &skipped, &metrics)
        .expect("group B route must still be planned for the 404 evidence");
    assert_eq!(plan_b.target.quota_group_str(), "account-b");
    table.note_model_missing(&metrics, &plan_b.target);
    assert!(
        table.model_snapshot("glm-5.2").disabled,
        "two distinct accounts returning 404 must disable the model"
    );
}

/// Repeated 404s from ONE group never count as distinct evidence.
#[test]
fn repeated_404_from_one_group_is_not_distinct_evidence() {
    let table = table(vec![key("k1", "account-a")]);
    let metrics = Metrics::new();
    let target = pick(&table, &hard(), &empty()).unwrap();
    for _ in 0..6 {
        table.note_model_missing(&metrics, &target);
    }
    assert!(
        !table.model_snapshot("glm-5.2").disabled,
        "one account's repeated 404s must never fabricate cross-group evidence"
    );
}

/// A known-missing route is not re-dialed by the same logical request.
#[test]
fn known_missing_route_is_not_immediately_redialed() {
    let table = table(vec![key("k1", "account-a"), key("k2", "account-b")]);
    let metrics = Metrics::new();
    let first = pick(&table, &hard(), &empty()).unwrap();
    table.note_model_missing(&metrics, &first);
    let mut skipped = empty();
    skipped.insert(first.identity());
    let next = pick(&table, &hard(), &skipped).unwrap();
    assert_ne!(
        (next.model_str(), next.quota_group_str()),
        (first.model_str(), first.quota_group_str()),
        "a 404 route must not be redialed within the same request"
    );
    // And across requests, while the cooldown holds, the route stays disabled.
    assert!(table.route_disabled("glm-5.2", "account-a"));
}

/// Single-group deployment: one 404 means the model cannot currently be served
/// through any known account, and the response is honest — but the model is
/// NOT marked globally disabled (no fabricated cross-group evidence), and the
/// route recovers after its bounded cooldown.
#[test]
fn single_group_404_is_honest_without_fabricated_evidence() {
    let table = table(vec![key("k1", "account-a")]);
    let metrics = Metrics::new();
    let target = pick(&table, &hard(), &empty()).unwrap();
    table.note_model_missing(&metrics, &target);
    assert!(
        !table.model_snapshot("glm-5.2").disabled,
        "a single group cannot produce distinct-group evidence"
    );
    assert!(table.route_disabled("glm-5.2", "account-a"));
    // The 404'd route is gone, but the sibling tier-0 model on the same
    // account is untouched: it is the correct next pick.
    let next = table
        .plan(&hard(), "sess_none", 0, &empty(), &metrics)
        .unwrap();
    assert_eq!(
        next.target.model_str(),
        "deepseek-v4-pro",
        "a 404 on one route must leave the sibling tier-0 route dialable"
    );
    assert_eq!(next.target.quota_group_str(), "account-a");
    // And a literal request for the 404'd model is an honest failure.
    assert!(
        table
            .plan(
                &RouteSpec::Literal("glm-5.2".into()),
                "sess_none",
                0,
                &empty(),
                &metrics
            )
            .is_err(),
        "with the only route 404'd, a literal request must fail honestly"
    );
}

/// Model-wide missing state recovers after `model_missing_cooldown`, and a
/// success clears it immediately — no process restart required.
#[test]
fn model_global_missing_recovers_without_restart() {
    let table = table(vec![key("k1", "account-a"), key("k2", "account-b")]);
    let metrics = Metrics::new();
    let first = pick(&table, &hard(), &empty()).unwrap();
    table.note_model_missing(&metrics, &first);
    let mut skipped = empty();
    skipped.insert(first.identity());
    let plan_b = table
        .plan(&hard(), "sess_none", 0, &skipped, &metrics)
        .unwrap();
    table.note_model_missing(&metrics, &plan_b.target);
    assert!(table.model_snapshot("glm-5.2").disabled);
    // A success anywhere clears it.
    table.note_success(&metrics, &plan_b.target, None, Duration::from_millis(5));
    assert!(
        !table.model_snapshot("glm-5.2").disabled,
        "recovery must not require a restart"
    );
}

// --- Circuit transition metrics -------------------------------------------

/// Closed → Open increments `model_circuit_open_total` exactly once.
#[test]
fn circuit_open_increments_exactly_once() {
    let table = table(vec![key("k1", "account-a"), key("k2", "account-b")]);
    let metrics = Metrics::new();
    let mut skipped = empty();
    for attempt in 0..2 {
        let plan = table
            .plan(&hard(), "sess_none", attempt, &skipped, &metrics)
            .unwrap();
        table.note_rate_limited(&metrics, &plan.target, attempt + 1, 0, None, 0.0);
        skipped.insert(plan.target.identity());
    }
    assert_eq!(
        metrics.model_circuit_open_total.load(Ordering::Relaxed),
        1,
        "exactly one Closed -> Open transition"
    );
}

/// Repeated failures while already Open do not increment again.
#[test]
fn repeated_failure_while_open_does_not_increment() {
    let table = table(vec![key("k1", "account-a"), key("k2", "account-b")]);
    let metrics = Metrics::new();
    // Capture the glm routes *before* tripping, while everything is plannable.
    let glm_routes: Vec<RouteTarget> = ["account-a", "account-b"]
        .iter()
        .map(|group| pick_for_group_spec(&table, "glm-5.2", group, &metrics))
        .collect();
    assert_eq!(glm_routes.len(), 2, "setup: two glm routes expected");
    // Trip the circuit with one 429 per distinct group.
    let mut skipped = empty();
    for (attempt, target) in glm_routes.iter().enumerate() {
        table.note_rate_limited(&metrics, target, attempt + 1, 0, None, 0.0);
        skipped.insert(target.identity());
    }
    assert_eq!(
        table.model_snapshot("glm-5.2").state,
        CircuitState::Open,
        "setup: the circuit must be open"
    );
    assert_eq!(metrics.model_circuit_open_total.load(Ordering::Relaxed), 1);
    // Re-record 429s on the exact glm routes that tripped the circuit: the
    // model is already Open, so the counter must not move.
    let mut recorded = 0usize;
    for target in &glm_routes {
        table.note_rate_limited(&metrics, target, 1, 0, None, 0.0);
        recorded += 1;
    }
    assert_eq!(recorded, 2, "failures were re-recorded while Open");
    assert_eq!(
        metrics.model_circuit_open_total.load(Ordering::Relaxed),
        1,
        "failures while Open must not double count"
    );
}

/// Open → HalfOpen does not increment; HalfOpen → Closed does not increment;
/// HalfOpen → Open increments once (reopen).
#[tokio::test]
async fn half_open_transitions_count_exactly() {
    let table = table_with(
        vec![key("k1", "account-a"), key("k2", "account-b")],
        |routing| {
            routing.model_open_secs = 1;
            // Short route cooldowns so they elapse alongside the model window and
            // the second trip is not blocked by stale cooldown state.
            routing.route_cooldown_initial_secs = 1;
            routing.route_cooldown_max_secs = 1;
        },
    );
    let metrics = Metrics::new();
    let mut skipped = empty();
    for attempt in 0..2 {
        let plan = table
            .plan(&hard(), "sess_none", attempt, &skipped, &metrics)
            .unwrap();
        table.note_rate_limited(&metrics, &plan.target, attempt + 1, 0, None, 0.0);
        skipped.insert(plan.target.identity());
    }
    assert_eq!(metrics.model_circuit_open_total.load(Ordering::Relaxed), 1);
    // HalfOpen is reached lazily by the model view; the counter must not move.
    let snapshot = table.model_snapshot("glm-5.2");
    assert_eq!(snapshot.state, CircuitState::Open);
    assert_eq!(
        metrics.model_circuit_open_total.load(Ordering::Relaxed),
        1,
        "Open -> HalfOpen must not increment"
    );
    // A successful probe closes the circuit; the counter must not move.
    // Let the open window elapse so the circuit half-opens, then plan the
    // literal glm spec so the probe provably targets glm-5.2.
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let probe = table
        .plan(
            &RouteSpec::Literal("glm-5.2".into()),
            "sess_none",
            0,
            &empty(),
            &metrics,
        )
        .expect("the half-open circuit must admit one probe");
    table.note_success(&metrics, &probe.target, None, Duration::from_millis(5));
    assert_eq!(table.model_snapshot("glm-5.2").state, CircuitState::Closed);
    assert_eq!(
        metrics.model_circuit_open_total.load(Ordering::Relaxed),
        1,
        "HalfOpen -> Closed must not increment"
    );
    // Trip again: exactly one more increment (Closed -> Open). First clear
    // the route cooldowns left over from the first trip (a success on each
    // glm route does exactly that), then record one 429 per distinct group
    // through the literal glm spec so both failures provably hit glm-5.2.
    let route_a = pick_for_group_spec(&table, "glm-5.2", "account-a", &metrics);
    let route_b = pick_for_group_spec(&table, "glm-5.2", "account-b", &metrics);
    table.note_success(&metrics, &route_a, None, Duration::from_millis(5));
    table.note_success(&metrics, &route_b, None, Duration::from_millis(5));
    assert_eq!(table.model_snapshot("glm-5.2").state, CircuitState::Closed);
    let mut skipped2 = empty();
    for target in [&route_a, &route_b] {
        table.note_rate_limited(&metrics, target, 1, 0, None, 0.0);
        skipped2.insert(target.identity());
    }
    assert_eq!(
        metrics.model_circuit_open_total.load(Ordering::Relaxed),
        2,
        "a second distinct trip increments once more"
    );
}

/// 429 failover paths do not double-increment the circuit counter.
#[test]
fn no_double_increment_across_429_failover_paths() {
    let table = table(vec![
        key("k1", "account-a"),
        key("k2", "account-b"),
        key("k3", "account-c"),
    ]);
    let metrics = Metrics::new();
    let mut skipped = empty();
    for attempt in 0..3 {
        if let Ok(plan) = table.plan(&hard(), "sess_none", attempt, &skipped, &metrics) {
            table.note_rate_limited(&metrics, &plan.target, attempt + 1, 0, None, 0.0);
            skipped.insert(plan.target.identity());
        }
    }
    let total = metrics.model_circuit_open_total.load(Ordering::Relaxed);
    assert!(
        total <= 1,
        "three 429s must not produce {total} circuit openings"
    );
}

// --- Profile-scoped latency scoring ---------------------------------------

/// Plan through `profile` until the given quota group is chosen, skipping the
/// others. Yields a real [`RouteTarget`] with a live credential.
fn pick_for_group(
    table: &RouteTable,
    profile: &str,
    group: &str,
    metrics: &Metrics,
) -> RouteTarget {
    pick_for_group_spec(
        table,
        table
            .profile_models(profile)
            .first()
            .map(String::as_str)
            .unwrap_or_default(),
        group,
        metrics,
    )
}

/// Plan the literal `model` spec until the given quota group is chosen.
fn pick_for_group_spec(
    table: &RouteTable,
    model: &str,
    group: &str,
    metrics: &Metrics,
) -> RouteTarget {
    let mut skipped = empty();
    for _ in 0..8 {
        let plan = table
            .plan(
                &RouteSpec::Literal(model.to_owned()),
                "sess_none",
                0,
                &skipped,
                metrics,
            )
            .expect("a route for the requested group must be plannable");
        if plan.target.quota_group_str() == group {
            return plan.target;
        }
        skipped.insert(plan.target.identity());
    }
    panic!("no plan reached quota group {group}");
}
/// The same model in two profiles with different latency policies is scored by
/// each profile's own rules — never by "whichever profile is found first".
#[test]
fn same_model_is_scored_by_the_active_profile() {
    let mut profiles = crate::config::Profiles::new();
    profiles.insert(
        "batch".into(),
        ProfileConfig {
            latency_optimized: false,
            allow_lower_tier_on_unavailable: false,
            tiers: vec![TierConfig {
                models: vec!["glm-5.2".into()],
            }],
        },
    );
    profiles.insert(
        "turbo".into(),
        ProfileConfig {
            latency_optimized: true,
            allow_lower_tier_on_unavailable: false,
            tiers: vec![TierConfig {
                models: vec!["glm-5.2".into()],
            }],
        },
    );
    let table = table_with_profiles(
        vec![key("k1", "account-a"), key("k2", "account-b")],
        profiles,
        |_| {},
    );
    let metrics = Metrics::new();
    // Give account-a a poor latency history, account-b an excellent one.
    for (group, ttft) in [("account-a", 900u64), ("account-b", 5u64)] {
        let target = pick_for_group(&table, "turbo", group, &metrics);
        table.note_success(
            &metrics,
            &target,
            Some(Duration::from_millis(ttft)),
            Duration::from_millis(ttft),
        );
    }
    // The latency-optimized profile must prefer the fast account.
    let turbo = table
        .plan(
            &RouteSpec::Profile("turbo".into()),
            "sess_none",
            0,
            &empty(),
            &metrics,
        )
        .unwrap();
    assert_eq!(turbo.target.quota_group_str(), "account-b");
    // The batch profile ignores latency entirely: both accounts are equivalent
    // on load, so the choice must differ from the latency-driven one at least
    // across the rotation — but crucially it must NOT systematically prefer
    // account-b the way the turbo profile does.
    let batch_choices: std::collections::HashSet<String> = (0..6)
        .filter_map(|_| {
            table
                .plan(
                    &RouteSpec::Profile("batch".into()),
                    "sess_none",
                    0,
                    &empty(),
                    &metrics,
                )
                .ok()
                .map(|plan| plan.target.quota_group_str().to_owned())
        })
        .collect();
    assert!(
        batch_choices.contains("account-a"),
        "the non-latency profile must not inherit the latency preference, saw {batch_choices:?}"
    );
}

/// Profile selection for scoring never depends on map iteration order or the
/// first matching profile.
#[test]
fn scoring_does_not_depend_on_first_matching_profile() {
    let mut profiles = crate::config::Profiles::new();
    // "aaa" sorts first; it is NOT latency optimized. "zzz" is.
    profiles.insert(
        "aaa".into(),
        ProfileConfig {
            latency_optimized: false,
            allow_lower_tier_on_unavailable: false,
            tiers: vec![TierConfig {
                models: vec!["glm-5.2".into()],
            }],
        },
    );
    profiles.insert(
        "zzz".into(),
        ProfileConfig {
            latency_optimized: true,
            allow_lower_tier_on_unavailable: false,
            tiers: vec![TierConfig {
                models: vec!["glm-5.2".into()],
            }],
        },
    );
    let table = table_with_profiles(
        vec![key("k1", "account-a"), key("k2", "account-b")],
        profiles,
        |_| {},
    );
    let metrics = Metrics::new();
    for (group, ttft) in [("account-a", 900u64), ("account-b", 5u64)] {
        let target = pick_for_group(&table, "zzz", group, &metrics);
        table.note_success(
            &metrics,
            &target,
            Some(Duration::from_millis(ttft)),
            Duration::from_millis(ttft),
        );
    }
    // "zzz" (latency-optimized, sorts last) must still score by latency.
    let zzz = table
        .plan(
            &RouteSpec::Profile("zzz".into()),
            "sess_none",
            0,
            &empty(),
            &metrics,
        )
        .unwrap();
    assert_eq!(
        zzz.target.quota_group_str(),
        "account-b",
        "the latency-optimized profile scores by latency even though it sorts last"
    );
}
