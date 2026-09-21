# Dynamic model routing

This document specifies how `sensenova-proxy` decides *which* upstream request
to make. Everything here is **proxy policy** — our own behaviour — unless a
statement is explicitly labelled *observed*, in which case it describes
measured SenseNova behaviour (see
[`sensenova-compatibility.md`](sensenova-compatibility.md) for the evidence
levels and the measurements behind them).

## The chain

```
client model → routing profile → quality tier → upstream model → quota group → API key
```

Each attempt dials one **`RouteTarget`**:

```rust
RouteTarget { model, quota_group, key }
```

Keeping the three axes separate is what keeps failure domains separate: a
`(model, quota_group)` pair can cool without touching the same model on another
account, or another model on the same account.

## Resolution

`POST /v1/messages` carries a client model. It resolves in this order:

1. **An explicit alias** in `models.aliases`. If the alias points at a routing
   profile, that profile is used; otherwise the alias's literal model is used.
2. **A routing profile name** (`claude-coding-hard`, `claude-coding-fast`, or
   any configured profile).
3. **Pass-through**: an explicit SenseNova catalog ID (for example
   `deepseek-v4-pro`, `glm-5.2`) is routed *as itself* — one tier, all quota
   groups, all keys. It is never substituted for another model.
4. **`models.map_unknown_to_default`**: an unknown `claude*` name is mapped to
   `models.default`, exactly as before routing existed.

A literal model still gets full quota-group and key failover; it simply has a
one-model pool.

## Profiles and quality tiers

A profile is an ordered list of tiers, each a list of models. Tiers are
**absolute**: tier *n+1* is only considered when every route of tiers *0..=n*
is unusable, and only when the profile sets `allow_cross_tier_fallback`.

The built-in pair:

| Profile | Tier 0 | Tier 1 | `latency_optimized` | `allow_lower_tier_on_unavailable` |
| ------- | ------ | ------ | ------------------- | -------------------------------- |
| `claude-coding-hard` | `glm-5.2`, `deepseek-v4-pro` | `kimi-k3` | false | **true** |
| `claude-coding-fast` | `deepseek-v4-flash`, `sensenova-6.8-flash-lite` | — | true | n/a (single tier) |

Consequences, all covered by tests:

- The hard pool **cannot** reach `deepseek-v4-flash` or
  `sensenova-6.8-flash-lite`; they are not in the profile.
- The fast pool **cannot** reach `glm-5.2`, `deepseek-v4-pro`, or `kimi-k3`.
- A healthy tier-0 route always beats a tier-1 route — never on latency, never
  on load.
- When every tier-0 route is **temporarily unavailable** (route cooldown, open
  model circuit, cooled quota group, waiting-out key), the hard profile falls
  through to tier 1: `kimi-k3` is part of the hard pool and is a legitimate
  resilience fallback. No sleeping while a healthy hard route exists.
- When every hard route *at every tier* is unusable, the client gets an honest
  error. There is no silent quality degradation into the fast pool.

### `allow_lower_tier_on_unavailable`

This profile-level flag (renamed from the deprecated `allow_cross_tier_fallback`,
which is still accepted with the same meaning) governs exactly one thing:
whether a *temporarily unavailable* tier may fall through to the next tier of
the **same profile**.

- It does **not** weaken profile boundaries. Tier fallback only ever walks the
  tier list of the profile being routed; the hard profile can never reach a
  fast model because the fast models are not in its tier list, whatever the
  flag says.
- `false` means the old conservative behaviour: a cooling tier reports its
  wait and the request waits (bounded) rather than degrade.
- Setting both names in one profile is rejected at startup.

## Failure domains

| Signal | Cools | Scope |
| ------ | ----- | ----- |
| Generic 429 (TPM / concurrency / capacity) | one `(model, quota_group)` route | the failing route only |
| Explicit quota exhaustion (`FREE_QUOTA_EXHAUSTED` and equivalent quota wording, *observed*) | the whole `quota_group` | every key and every model on that account |
| 401 | that credential | that key only |
| 403 | nothing; failover only | 403 may be model-level, so no evidence-based condemnation |
| 404 model not found | that `(model, quota_group)` route, long bounded cooldown | the same model on other accounts |
| 5xx / transport / EOF before the first byte / first-byte timeout | the `(model, quota_group)` route, briefly | the failing route |

`quota_group` is the account-level failure domain: keys inside one group are
*not* assumed to have independent quota (no evidence supports that), so an
explicit exhaustion signal cools the whole group.

### The model circuit

A model-wide circuit opens only when qualifying failures arrive from
`model_trip_distinct_groups` (**default 2**) distinct quota groups inside
`model_trip_window_secs` (**default 20 s**). One failing account can therefore
never globally disable a model — that is the entire point of the threshold.

The circuit is Closed → Open → HalfOpen → Closed:

- **Open** for `model_open_secs` (**default 30 s**).
- Then **HalfOpen**: one probe attempt is admitted. It is only admitted when
  the model's keys are all cooling or already attempted, so a retry can still
  explore a *different* quota group instead of duelling with the circuit.
- A successful probe closes the circuit and clears the failure history.
- A cancelled probe (client disconnect) releases the slot and closes the
  circuit rather than leaving the model stuck half-open.

The proxy-wide circuit in `src/circuit.rs` is separate and much blunter: it
exists for a proxy-wide upstream outage and for quota exhaustion with no usable
route left. A single model's circuit never blocks healthy models.

### Model-missing (404) evidence

Model availability and entitlement may differ **per account**, so one 404 is
only route-level evidence. The model-wide missing state requires 404s from
`model_missing_distinct_groups` (**default 2**) distinct quota groups inside
`model_missing_window_secs` (**default 300 s**):

```
glm-5.2 / account-A -> 404  => route (glm-5.2, A) disabled
glm-5.2 / account-B -> 200  => model stays globally healthy
glm-5.2 / A -> 404, glm-5.2 / B -> 404 (inside the window)
                            => model disabled
```

- Repeated 404s from **one** group never count as distinct evidence: they
  refresh a single bounded entry.
- The route-level disable means a known-missing route is never re-dialed.
- The model-wide state **recovers** after `model_missing_cooldown_secs`
  (**default 1 h**) and is cleared immediately by any success — a transient
  catalog change never requires a process restart.
- A single-group deployment cannot fabricate cross-group evidence: one 404
  disables the only route, the request falls back to the next tier or fails
  honestly, and the model is *not* marked globally missing.

### `model_circuit_open_total` semantics

The counter increments **exactly once per transition into Open**:

| Transition | Counter |
| ---------- | ------- |
| Closed → Open | +1 |
| more failures while Open | unchanged |
| Open → HalfOpen | unchanged |
| HalfOpen → Closed (successful probe) | unchanged |
| HalfOpen → Open (failed probe) | +1 |

The transition logic lives in one place (`RouteTable`'s circuit transition
helper), so double counting across failover paths is structurally impossible.

## 429 behaviour

A 429 means "this route is busy", so the proxy moves **horizontally**:

```
glm-5.2 / account-A  →  429  →  cool (glm-5.2, account-A)
glm-5.2 / account-B  →  429  →  cool (glm-5.2, account-B)
                              →  two distinct groups: open the glm-5.2 circuit
deepseek-v4-pro      →  healthy  →  served
```

It never performs `429 → sleep → retry the identical route` while another
healthy route exists. `same_route_429_retries` defaults to **0**.

### Cooldowns

- **Authoritative `Retry-After`** (header, structured JSON field, or human
  text such as `Try again in 2h 30m`) always wins. It is used verbatim for the
  cooldown and forwarded to the client, bounded only by
  `max_model_cooldown_secs` so a hostile or buggy hint cannot park a route
  forever.
- **Without a hint**, a bounded exponential ladder with jitter in
  `[1.0, 1.5)`:

  | attempt / streak | cooldown |
  | ---------------- | -------- |
  | 1 | 10 s |
  | 2 | 20 s |
  | 3 | 40 s |
  | 4 | 80 s |
  | 5+ | 120 s (cap) |

  Controlled by `route_cooldown_initial_secs` and `route_cooldown_max_secs`.
  All arithmetic is saturating and the shift is capped, so no input overflows
  and no cooldown is ever zero.

### Waiting vs. returning

A cooldown is either **waited out** in-request or **reported** to the client:

- If some route in the selected tier is dialable, the attempt proceeds
  immediately (zero wait) — that is the common failover case.
- If nothing in the tier is dialable but something will become dialable soon,
  the wait is capped by `retry_after_max_secs` (**default 120 s**). Beyond
  that, the client receives `429` with `Retry-After` instead, so Claude Code's
  own backoff takes over. There is no unbounded sleep loop.
- If nothing is dialable *at all* (every model 404'd, every credential
  rejected), the client gets `503` with an honest message and `/readyz`
  reports `not_ready`.

## Retry budget

`max_route_attempts` (**default 4**) is a single global counter per logical
request. Quality-tier fallback, quota-group failover, key failover and any
same-route replay all draw from it, and no nested path can exceed it.

Good, and what the proxy produces:

```
1. glm-5.2         / account-A
2. glm-5.2         / account-B
3. deepseek-v4-pro / account-A
4. kimi-k3         / account-C
```

Never:

```
1. glm-5.2 / A
2. glm-5.2 / A   ← same-route replay while an alternative exists
...
```

## Request rewriting

The canonical request body is built once, then **not** mutated. Each attempt
receives a fresh clone with only its `model` field rewritten. A retry can
therefore safely switch models, and a partially rewritten body is impossible by
construction.

## Load distribution

Equivalent healthy routes take turns rather than always dialing the first key:
candidates are ranked by affinity, then by score, then by a rotation over the
configured order. The score prefers:

1. fewer in-flight requests on the credential;
2. a lower recent-failure penalty (the consecutive-429 streak);
3. for `latency_optimized` profiles only, lower observed time-to-first-byte and
   total-latency EWMAs.

In-flight counts live behind an RAII guard, so success, error, timeout and
client-disconnect paths all release them — a leak is not possible.

Hard profiles score only health and load, and only *within* a tier, so a
faster weak model can never outrank a higher-quality tier.

## Session affinity

Weak, bounded, behavioural — and **not** a cache.

- TTL: `soft_affinity_secs` (**default 300 s**).
- Keyed on the existing hashed, non-reversible `session_tag` derived from
  Claude Code's session headers. The raw identifier is never stored or logged.
- Bounded by `max_affinity_entries` (**default 4096**), with expired entries
  reclaimed on write.
- Broken immediately on a 429, a cooldown, an open circuit, a relevant 5xx, or
  any unhealthy route. Only an uninterrupted first-try success (re)establishes
  it, so a request that had to fail over never re-pins the session to whatever
  eventually answered.
- It reorders candidates **inside the already-chosen tier only**. It can never
  override health, and it can never override quality-tier rules.

### Cache has zero influence

Prompt caching, cache hits, cache tokens and cache affinity are **not read, not
scored, and never influence a routing decision**. The affinity above exists so
a conversation keeps talking to a consistent backend, not to keep a cache warm.

## Streaming safety

Every routing decision happens strictly before the first downstream byte.

1. The upstream response is inspected before the client sees anything: a
   non-stream body is buffered and validated; a stream's first non-empty chunk
   is buffered.
2. Only then is the response handed to the client — the **commit barrier**.
3. After that byte, the proxy never retries, never switches account, never
   switches model, and never splices two upstream streams. A mid-stream failure
   emits exactly one Anthropic `event: error` frame and terminates.

Duplicate tool calls, shell commands, or file edits are structurally
impossible.

## Readiness

`GET /readyz` reports:

```json
{
  "status": "ready",
  "usable_routes": 5,
  "total_routes": 6,
  "usable_credentials": 2,
  "circuit_state": "closed",
  "concurrency_limit": 2,
  "queued_requests": 0
}
```

Readiness is **route**-based, not credential-based: one unhealthy model, or one
bad key, does not take the proxy out of service while any legal route remains.
`not_ready` (HTTP 503) means no usable route exists for the configured routing
setup.

## Observability

Metrics (all prefixed `sensenova_proxy_`):

| Metric | Meaning |
| ------ | ------- |
| `route_attempts_total` | Route attempts dialed under a profile. |
| `route_failovers_total` | Failovers to a different `(model, quota_group)` route. |
| `model_failovers_total` / `cross_model_failovers_total` | Failovers that changed the upstream model. |
| `model_account_429_total` | Generic 429s attributed to one `(model, quota_group)` route. |
| `model_circuit_open_total` | Model circuits opened by distinct quota groups. |
| `routing_exhausted_total` | Requests whose route-attempt budget was spent. |
| `affinity_hits_total` / `affinity_breaks_total` | Affinity reuse and health-driven breaks. |
| `tier_steps_total` | Requests that had to step down a quality tier. |
| `health_excluded_total` | Candidates excluded by a cooldown or open circuit. |

Structured logs carry `request_id`, `session_tag`, the routing profile, `tier`,
`stepped_down`, `gate`, `model`, credential name, `quota_group`, `attempt`,
`cross_group`, the failover reason, the cooldown, and the circuit state. Keys
and raw session identifiers are never logged. Existing metrics
(`upstream_requests_total`, `upstream_429_total`, `quota_exhaustion_total`,
`retries_total`, `cross_group_failovers_total`, …) are preserved.

## Configuration reference

```json
"routing": {
  "max_route_attempts": 4,
  "soft_affinity_secs": 300,
  "max_affinity_entries": 4096,
  "same_route_429_retries": 0,
  "route_cooldown_initial_secs": 10,
  "route_cooldown_max_secs": 120,
  "model_trip_distinct_groups": 2,
  "model_trip_window_secs": 20,
  "model_open_secs": 30,
  "model_missing_distinct_groups": 2,
  "model_missing_window_secs": 300,
  "model_missing_cooldown_secs": 3600,
  "retry_after_max_secs": 120,
  "max_model_cooldown_secs": 86400,
  "profiles": {
    "claude-coding-hard": {
      "latency_optimized": false,
      "allow_cross_tier_fallback": false,
      "tiers": [
        { "models": ["glm-5.2", "deepseek-v4-pro"] },
        { "models": ["kimi-k3"] }
      ]
    },
    "claude-coding-fast": {
      "latency_optimized": true,
      "allow_cross_tier_fallback": false,
      "tiers": [{ "models": ["deepseek-v4-flash", "sensenova-6.8-flash-lite"] }]
    }
  }
}
```

| Field | Default | Constraint |
| ----- | ------- | ---------- |
| `max_route_attempts` | 4 | 1–8 |
| `soft_affinity_secs` | 300 | ≤ 86400 |
| `max_affinity_entries` | 4096 | 1–1000000 |
| `same_route_429_retries` | 0 | 0–3 |
| `route_cooldown_initial_secs` | 10 | 1 ≤ initial ≤ max ≤ 3600 |
| `route_cooldown_max_secs` | 120 | see above |
| `model_trip_distinct_groups` | 2 | 1–16 |
| `model_trip_window_secs` | 20 | 1–3600 |
| `model_open_secs` | 30 | 1–3600 |
| `model_missing_distinct_groups` | 2 | 1–16 |
| `model_missing_window_secs` | 300 | 1–86400 |
| `model_missing_cooldown_secs` | 3600 | 1–one year |
| `retry_after_max_secs` | 120 | 1–3600 |
| `max_model_cooldown_secs` | 86400 | 1–one year |

Validation rejects unknown fields anywhere, empty profile/tier/model lists, a
model listed in two tiers of one profile, `allow_lower_tier_on_unavailable` on
a single-tier profile, zero or impossible durations, and out-of-range bounds.
The deprecated `allow_cross_tier_fallback` name is still accepted (same
meaning); setting both names in one profile is rejected. Errors name the field
(and profile/tier index) only and never echo a value.

`routing` itself is optional; omitting it selects the built-in hard/fast pair,
so existing configurations keep working unchanged.

## Invariants

These are the rules the implementation must never break, and each has a test:

1. No retry, failover, account switch or model switch after the downstream
   commit.
2. The hard profile never silently falls into a fast model (tier fallback
   within the hard pool is fine and expected; profile boundaries are absolute).
3. One 429 cannot globally disable a model.
4. A model-wide circuit requires failures from distinct quota groups.
5. One account's failure cannot stop other accounts.
6. One model's failure cannot stop other models.
7. Exactly one bounded route-attempt budget per logical request.
8. A generic 429 prefers another healthy route over replaying the failed one.
9. Cache state has zero routing influence.
10. Secrets and raw session identifiers never leak into logs or responses.
11. Explicit quota exhaustion still cools the whole `quota_group`.
12. Existing streaming safety is preserved.
