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
            allow_cross_tier_fallback: true,
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
