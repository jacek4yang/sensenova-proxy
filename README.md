[![CI](https://github.com/jacek4yang/sensenova-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/jacek4yang/sensenova-proxy/actions/workflows/ci.yml)

# sensenova-proxy

`sensenova-proxy` is a small Rust gateway that makes SenseTime **SenseNova
Token Plan** APIs work reliably as an **Anthropic-compatible backend for
Claude Code**.

It exposes the Anthropic Messages API locally and forwards requests to
SenseNova's native Anthropic endpoint (`https://token.sensenova.cn/v1/messages`)
over one or more configured SenseNova API keys, adding the pieces the upstream
does not provide:

- gateway authentication and model aliasing,
- **dynamic virtual-model routing** with quality-tier isolation
  (`claude-coding-hard` / `claude-coding-fast`) across multiple accounts,
- bounded, provably safe retries with **one global route-attempt budget**,
- robust 429 / quota / overload handling with per-route cooldowns and
  hierarchical circuits,
- local concurrency shaping (bounded queue, no unbounded growth),
- secret redaction in every error path,
- streaming validation and observability.

This is not a generic multi-provider gateway. It is dedicated to SenseNova's
observed real-world behavior; every protocol decision is recorded with its
evidence level in [`docs/sensenova-compatibility.md`](docs/sensenova-compatibility.md).

## Architecture

The binary is a single Axum service with deliberately small subsystems:

```
Claude Code (Anthropic Messages)
    │  Authorization: Bearer <gateway key>
    ▼
sensenova-proxy  (Axum, 127.0.0.1:8789)
    │  · auth middleware (constant-time)     · request IDs
    │  · model aliasing + normalization      · bounded body limits
    │  · admission control (semaphore + bounded queue)
    │  · virtual-model routing profiles       · secret redaction
    │  · bounded route-attempt loop (pre-commit only)
    │  · hierarchical circuits (route → model)
    ▼
https://token.sensenova.cn/v1/messages   (native Anthropic compatibility)
```

Routing chain:

```
client model → routing profile → quality tier → upstream model → quota group → API key
```

- `src/config.rs` — file-backed configuration and startup validation.
- `src/router.rs` — routing profiles, quality tiers, and the hierarchical
  failure domains (`key` / `quota_group` / `(model, quota_group)` / `model`).
- `src/server.rs` — routes, middleware, the stream pump, graceful shutdown.
- `src/upstream.rs` — one long-lived rustls `reqwest::Client`; classification,
  the route-attempt loop, and the pre-commit buffering that makes retries safe.
- `src/rate_limit.rs` — error classification and `Retry-After` parsing.
- `src/pool.rs` — the configured credential set (identity + quota group).
- `src/circuit.rs` — the proxy-wide Closed / Open / HalfOpen circuit breaker.
- `src/concurrency.rs` — semaphore + bounded wait queue.
- `src/sse.rs` — bounded incremental SSE parser (stream validation only; the
  passthrough bytes are never rewritten).
- `src/redaction.rs` — structural and exact-secret redaction.
- `src/metrics.rs` — lightweight counters exposed at `GET /metrics`.

There is no OAuth, credential refresh, database, web UI, or admin API.
Configuration is read once from JSON at startup.

### Why native passthrough (architecture decision)

The compatibility investigation (see
[`docs/sensenova-compatibility.md`](docs/sensenova-compatibility.md)) found
that SenseNova's `/v1/messages` genuinely implements the Anthropic Messages
protocol — text, streaming lifecycle, tools, tool results, images, thinking
blocks — so a conversion layer would add risk without benefit. The proxy
therefore passes through and concentrates on reliability, safety, and
observability. SenseNova's OpenAI-compatible `/v1/chat/completions` endpoint
was also characterized in the compatibility document (including its
reasoning quirks) but is intentionally not used in v1.

## Endpoints

| Endpoint                        | Auth required | Notes                                          |
| ------------------------------- | ------------- | ---------------------------------------------- |
| `POST /v1/messages`             | yes           | Anthropic Messages passthrough.                |
| `POST /v1/messages/count_tokens`| yes           | Local conservative estimate (no upstream call).|
| `GET /v1/models`                | yes           | Deterministic local catalog.                   |
| `GET /healthz`                  | no            | Liveness.                                      |
| `GET /readyz`                   | loopback only | Readiness: usable routes, credentials, circuit state. |
| `GET /metrics`                  | loopback only | Prometheus text format.                        |

When bound to a non-loopback address, `/readyz` and `/metrics` require the
gateway key as well.

## Build and install

Rust 1.88 or newer.

```bash
cargo build --release
install -m 0755 target/release/sensenova-proxy /usr/local/bin/sensenova-proxy
```

## Configuration

```bash
cp config.example.json config.json
chmod 600 config.json   # it contains a credential
```

The default path is `./config.json`; override with `--config PATH` or
`SENSENOVA_PROXY_CONFIG=PATH`. All fields have documented defaults; unknown
fields are rejected at startup.

```json
{
  "server":      { "bind": "127.0.0.1:8789", "api_key": "change-me..." },
  "upstream":    { "base_url": "https://token.sensenova.cn",
                   "messages_path": "/v1/messages",
                   "anthropic_version": "2023-06-01",
                   "timeout_secs": 600, "connect_timeout_secs": 20,
                   "first_byte_timeout_secs": 120 },
  "sensenova_api_keys": [
    { "name": "primary", "api_key": "YOUR_SENSENOVA_API_KEY",
      "enabled": true, "quota_group": "account-a" }
  ],
  "models":      { "default": "sensenova-6.8-flash-lite",
                   "map_unknown_to_default": true,
                   "aliases": { "claude-sensenova": "sensenova-6.8-flash-lite" } },
  "routing":     {
    "max_route_attempts": 4, "soft_affinity_secs": 300,
    "max_affinity_entries": 4096, "same_route_429_retries": 0,
    "route_cooldown_initial_secs": 10, "route_cooldown_max_secs": 120,
    "model_trip_distinct_groups": 2, "model_trip_window_secs": 20,
    "model_open_secs": 30, "retry_after_max_secs": 120,
    "max_model_cooldown_secs": 86400,
    "profiles": {
      "claude-coding-hard": {
        "latency_optimized": false, "allow_cross_tier_fallback": false,
        "tiers": [ { "models": ["glm-5.2", "deepseek-v4-pro"] },
                   { "models": ["kimi-k3"] } ]
      },
      "claude-coding-fast": {
        "latency_optimized": true, "allow_cross_tier_fallback": false,
        "tiers": [ { "models": ["deepseek-v4-flash", "sensenova-6.8-flash-lite"] } ]
      }
    }
  },
  "retry":       { "max_attempts": 2 },
  "concurrency": { "initial": 2, "minimum": 1, "maximum": 8,
                   "queue_capacity": 32, "queue_timeout_secs": 120 },
  "circuit":     { "overload_threshold": 5, "overload_window_secs": 60,
                   "overload_open_secs": 30, "max_quota_cooldown_secs": 86400 },
  "runtime":     { "max_request_bytes": 33554432, "log_level": "info",
                   "log_format": "pretty", "stream_ping_secs": 15,
                   "shutdown_timeout_secs": 30 }
}
```

Startup rejects: empty gateway/SenseNova keys, control characters in keys,
duplicate or zero enabled credentials, invalid bind address or URL, embedded
URL credentials, zero timeouts, inconsistent concurrency bounds
(`1 ≤ minimum ≤ initial ≤ maximum ≤ 64`), `retry.max_attempts` outside 1–4,
and invalid tracing filters.

The `routing` section is validated just as strictly: unknown fields anywhere
(including inside a profile), empty profile/tier/model lists, a model listed in
two tiers of the same profile, `allow_cross_tier_fallback` on a single-tier
profile, zero or impossible durations, `route_cooldown_initial_secs >
route_cooldown_max_secs`, and out-of-range bounds are all rejected at startup.
Validation errors identify the offending field (and profile/tier index) only,
and never print key values.

`routing` is optional: omit it and the built-in `claude-coding-hard` /
`claude-coding-fast` pair is used, so existing configurations keep working
unchanged.

#### Using DeepSeek V4 Pro (and other catalog models)

SenseNova catalog model IDs (for example `deepseek-v4-pro`, `glm-5.2`) work
natively on the same Anthropic endpoint and are passed through unchanged —
they are never rewritten to the default model. Point Claude Code at one via
an alias:

```json
"models": {
  "default": "deepseek-v4-pro",
  "map_unknown_to_default": true,
  "aliases": { "claude-deepseek": "deepseek-v4-pro" }
}
```

```bash
export ANTHROPIC_MODEL=claude-deepseek
```

DeepSeek V4 Pro differences (observed — see
[`docs/sensenova-compatibility.md`](docs/sensenova-compatibility.md)):
reasoning is **on by default** (signature-less `thinking` blocks are emitted
even when Claude Code does not ask for thinking; `thinking:
{"type":"disabled"}` suppresses them), upstream responses report a dated
snapshot such as `deepseek-v4-pro-0813`, and its advertised 1M context is
upstream metadata (not independently verified here).

## Dynamic model routing

Routing is the proxy's core feature: a client asks for a **virtual model**, and
the proxy decides which upstream model, on which account, through which key to
use — per attempt.

```
client model → routing profile → quality tier → upstream model → quota group → API key
```

### The two built-in profiles

| Profile | Tier 0 | Tier 1 | Purpose |
| ------- | ------ | ------ | ------- |
| `claude-coding-hard` | `glm-5.2`, `deepseek-v4-pro` | `kimi-k3` | Agent-quality work. |
| `claude-coding-fast` | `deepseek-v4-flash`, `sensenova-6.8-flash-lite` | — | Latency/health work. |

Isolation is absolute:

- **`claude-coding-hard` never selects `deepseek-v4-flash` or
  `sensenova-6.8-flash-lite`.** They are not in the profile, so no failure
  pattern can reach them.
- **Quality outranks latency, always.** Tier 0 is exhausted (all routes
  hard-failed) before tier 1 is considered — a faster model is never chosen
  over a higher-quality one that is merely slower.
- **If every hard route fails, you get an honest failure**, never a silent
  downgrade to a weak model.
- `claude-coding-fast` contains only fast models; it can never reach a
  quality model.

### Claude Code setup

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:8789
export ANTHROPIC_AUTH_TOKEN="$SENSENOVA_PROXY_GATEWAY_KEY"

export ANTHROPIC_DEFAULT_OPUS_MODEL=claude-coding-hard
export ANTHROPIC_DEFAULT_SONNET_MODEL=claude-coding-hard
export ANTHROPIC_DEFAULT_HAIKU_MODEL=claude-coding-fast
export ANTHROPIC_MODEL=claude-coding-hard

export API_TIMEOUT_MS=600000
claude
```

Both virtual names and every concrete model in the profiles are listed by
`GET /v1/models`, so Claude Code's model picker shows them. Old aliases
(`claude-sensenova`, `claude-deepseek`, …) and explicit SenseNova catalog IDs
still work exactly as before.

### Hierarchical failure domains

Every failure cools exactly the thing that failed — never more:

| Failure | Cools | Does *not* affect |
| ------- | ----- | ----------------- |
| Generic 429 (TPM/capacity) | one `(model, quota_group)` route | the same model on another account; another model on the same account |
| Explicit quota exhaustion (`FREE_QUOTA_EXHAUSTED`) | the whole `quota_group` | other quota groups and other models |
| 401 | that one credential | siblings in the same quota group |
| 404 model not found | that model (latched) | other models |
| 5xx / transport / EOF before first byte | the `(model, quota_group)` route, briefly | everything else |

A **model-wide circuit** opens only when qualifying failures arrive from
`routing.model_trip_distinct_groups` (default **2**) distinct quota groups
inside `routing.model_trip_window_secs` (default **20 s**). One failing account
can therefore never globally disable a model; the model is also half-open
probed after `routing.model_open_secs` (default **30 s**) and closes again on a
successful probe.

### 429 behaviour

A 429 means "this route is busy right now", so the proxy moves horizontally:

```
glm-5.2 / account-A  →  429  →  cool (glm-5.2, account-A)
glm-5.2 / account-B  →  429  →  cool (glm-5.2, account-B)
                              →  2 distinct groups: open the glm-5.2 model circuit
deepseek-v4-pro      →  healthy  →  served
```

It never does `429 → sleep → retry the identical route` while another healthy
route exists. `routing.same_route_429_retries` defaults to **0** for that
reason; it only becomes reachable when literally nothing else is dialable, and
the wait is then bounded by `routing.retry_after_max_secs`.

An authoritative `Retry-After` always wins: it is used verbatim for the
cooldown and forwarded to the client (bounded only by
`routing.max_model_cooldown_secs`, so a hostile hint cannot park a route
forever). Without one, the cooldown ladder is bounded exponential with jitter:
**10 s → 20 s → 40 s → 80 s → 120 s cap** (`route_cooldown_initial_secs` →
`route_cooldown_max_secs`, capped there).

### One global retry budget

`routing.max_route_attempts` (default **4**) is the *only* attempt counter, and
it covers every kind of failover.

Good, and what the proxy does:

```
1. glm-5.2       / account-A
2. glm-5.2       / account-B
3. deepseek-v4-pro / account-A
4. kimi-k3       / account-C
```

Never (no nested retry loop can multiply the budget):

```
glm-5.2/A → glm-5.2/A again → glm-5.2/B → glm-5.2/B again → deepseek/A → ...
```

### Load distribution

Equivalent healthy routes take turns (a rotation over the configured
candidates) instead of always dialing the first key, and the choice prefers the
route with **fewer in-flight requests** and a **lower recent-failure penalty**.
In-flight counts are held by an RAII guard, so success, error, timeout and
client-disconnect paths all release them.

For `latency_optimized` profiles (the fast one) the score also weighs observed
time-to-first-byte and total latency EWMAs. The hard profile scores only health
and load, and only *inside* a tier — latency can never promote a weaker model.

### Session affinity is a hint, not a cache

Weak, bounded, behavioural affinity keeps one Claude Code session on the route
it already used, for at most `routing.soft_affinity_secs` (default **300 s**).

- It uses the existing hashed, non-reversible session tag; no session data is
  stored, and the raw identifier is never logged.
- State is bounded by `routing.max_affinity_entries` (default **4096**);
  expired entries are reclaimed.
- It is broken immediately on a 429, a cooldown, an open circuit, a relevant
  5xx, or any unhealthy route — and only an uninterrupted first-try success
  re-establishes it.
- It reorders candidates **inside the already-chosen tier only**: it can never
  override health, and it can never override quality-tier rules.

**It has nothing to do with prompt caching.** Cache state is not read, not
scored, and never influences a routing decision.

### Streaming safety is unchanged

The commit barrier still governs everything: a streaming request is committed
to the client with its first SSE byte, and that byte is buffered *before* the
response is handed over. Every routing, cooldown and failover decision
therefore happens strictly before any observable output. After commit the proxy
never retries, never switches account, never switches model, and never splices
two upstream streams.

## Multiple API keys and quota groups

Multiple keys are supported, but keys from one SenseNova account are **not
assumed to have independent quota**. `quota_group` is the failure domain, and
failover within one logical request is quota-group-aware:

```json
"sensenova_api_keys": [
  { "name": "account-a-1", "api_key": "...", "enabled": true, "quota_group": "account-a" },
  { "name": "account-a-2", "api_key": "...", "enabled": true, "quota_group": "account-a" },
  { "name": "account-b-1", "api_key": "...", "enabled": true, "quota_group": "account-b" }
]
```

- **Explicit quota exhaustion** (`FREE_QUOTA_EXHAUSTED`, observed wording) →
  the entire `account-a` group cools and the same logical request fails over
  to another group (`account-b`) — the client never sees the 429 as long as
  some group remains within the attempt budget.
- **Generic 429** (TPM / serving limits) → a credential from a *different*
  quota group is preferred, since same-group keys likely share the account's
  serving capacity; same-group siblings are the fallback when no other group
  exists.
- **401/403** → credential-specific: a sibling key in the same group remains
  fully usable (a bad key does not condemn the account).

Failover is always bounded by `retry.max_attempts`, and one credential is
replayed at most `1 + retry.max_same_key_retries` times per logical request.
A 401 permanently disables only the rejected credential. If a single key
serves you well, use one key — the pool adds no value unless the keys
genuinely fail independently.

### Model aliasing

`models.default` plus `models.aliases` form the local `/v1/models` catalog.
With `map_unknown_to_default: true` (default), any unknown client model — for
example Claude Code's built-in small-model names — is rewritten to
`sensenova-6.8-flash-lite` instead of failing upstream with a 404. Explicit
SenseNova model IDs always pass through unchanged.

## Claude Code

```bash
export SENSENOVA_PROXY_GATEWAY_KEY="$(choose a long random secret)"
# must equal server.api_key in config.json

export ANTHROPIC_BASE_URL=http://127.0.0.1:8789
export ANTHROPIC_AUTH_TOKEN="$SENSENOVA_PROXY_GATEWAY_KEY"

# Virtual routing profiles (see "Dynamic model routing" above).
export ANTHROPIC_MODEL=claude-coding-hard
export ANTHROPIC_DEFAULT_OPUS_MODEL=claude-coding-hard
export ANTHROPIC_DEFAULT_SONNET_MODEL=claude-coding-hard
export ANTHROPIC_DEFAULT_HAIKU_MODEL=claude-coding-fast

export API_TIMEOUT_MS=600000

claude
```

`claude-sensenova` and friends keep working as aliases if you prefer the older
single-model behaviour.

Both `Authorization: Bearer <key>` and `x-api-key: <key>` are accepted. The
gateway key never travels upstream; the SenseNova credential is attached only
by the upstream layer. Claude Code session/agent headers
(`x-claude-code-session-id` and friends) are used locally for log correlation
(as a short non-reversible tag) and are not forwarded upstream.

## Rate-limit behavior

Every upstream error is classified (`UpstreamErrorClass`) from the HTTP
status, `Retry-After`, and the error body — including SenseNova's Google-style
numeric codes (`{"error":{"code":16,...}}` was observed for 401). Observed
real-world 429 text ("Server is busy, please try again later") is handled.
Classification verdicts are logged with a machine-readable reason
(`http_429`, `resource_exhausted_code`, `explicit_quota_evidence`, ...) so
every cooldown can be audited after the fact.

**Token Plan credits and serving limits are different things.** A generic 429
has been observed in production while the dashboard still showed substantial
remaining credits; it means transient rate limiting (TPM / concurrency /
capacity), not credit exhaustion, and is handled as such.

### Retry budget (anti-amplification)

`routing.max_route_attempts` (default **4**) is a single, global budget per
logical request: quality-tier fallback, quota-group failover, key failover and
any same-route replay all draw from it, and nothing nests. When another usable
route exists, **failover is always preferred over replaying the same route** —
immediately re-submitting a large prompt against the route that just failed
only amplifies the burst. "HTTP 200 whose stream ends before the first byte" is
treated as potentially having consumed upstream scheduler work: it fails over
rather than being replayed.

### Generic 429 cooldown ladder

A generic 429 with an authoritative `Retry-After` always uses it (the same
value is forwarded to the client). Without one, the route cooldown escalates
per consecutive generic 429 — 10 s → 20 s → 40 s → 80 s → capped at 120 s
(`routing.route_cooldown_*`, with jitter). A success resets the ladder.

- **429 with another usable route** → immediate failover, preferring a
  different quota group (same account, same model → different account).
- **429 with nothing else dialable** → the proxy returns HTTP 429 with a
  `Retry-After` header so Claude Code's own backoff can take over. It only
  waits a cooldown out in-request when `routing.same_route_429_retries > 0`
  and the wait is within `routing.retry_after_max_secs`.
- **Generic 429s never open the proxy-wide circuit** — one account's TPM limit
  says nothing about other accounts. They cool exactly one
  `(model, quota_group)` route; the model-wide circuit needs failures from
  distinct quota groups.
- **Quota exhaustion** → only *explicit* evidence promotes a 429 to quota
  exhaustion (`FREE_QUOTA_EXHAUSTED` and equivalent quota-scoped wording;
  never a plain 429 and never Google-style code 8 alone, which usually means
  rate limiting). The exhausted quota group cools; other quota groups keep
  serving; the global circuit opens only when no credential remains usable.
  No hammering.
- **Sustained 5xx/transport failures** → after `overload_threshold`
  failures within `overload_window_secs`, the circuit opens for
  `overload_open_secs` and requests fail fast without dialing SenseNova.
- **401** → that credential is marked unusable (readiness reflects it);
  remaining keys still serve. **403** fails over but does not disable the key
  (it may be model-level). **404** disables that model route and fails over to
  another model — it is never dialed twice for the same request.
- **400/422** → sanitized passthrough, never retried, never fail over.

Body text merely containing "429" never classifies as a rate limit.

## Retry guarantees

- At most `routing.max_route_attempts` (default **4**) upstream attempts per
  logical request. Claude Code already retries; the proxy never multiplies its
  loop, and no nested retry path can exceed this single budget.
- Retries happen only for provably safe classes (transport failures, 5xx,
  queue timeouts, 429 per above) and only with bounded, jittered cooldowns.
- **The commit barrier:** a streaming request is committed to the client with
  its first SSE byte. The first upstream chunk is buffered *before* the
  response is handed to the client, so every retry decision strictly precedes
  any observable output. After commit there is no replay path at all — a
  mid-stream upstream failure emits exactly one Anthropic `event: error`
  frame and terminates the stream. Duplicated tool calls, shell commands, or
  file edits are structurally impossible.

## Streaming guarantees

- Upstream SSE bytes are forwarded **verbatim**; the proxy never rewrites
  model output.
- A bounded incremental parser validates the stream in parallel: malformed
  SSE JSON terminates the stream with a clean error frame (counted in
  metrics), and an abrupt EOF without `message_stop` is reported instead of
  being silently swallowed.
- Protocol-legal `event: ping` keepalives (default every 15 s,
  `runtime.stream_ping_secs: 0` disables) keep long reasoning pauses from
  looking like dead connections.
- Dropping the downstream body promptly cancels the upstream request and
  releases the concurrency permit.

## Quota visibility

sensenova-proxy does **not** claim to know your remaining credits. No stable
API-key-authenticated quota endpoint was found: the Token Plan dashboard's
pool data comes from control-plane endpoints that explicitly reject API-key
authentication (`401 auth_type_disabled`, observed) and require a browser
console session, which this proxy will not automate or persist. Estimated
local usage is therefore not exposed as "remaining quota".

What the proxy does guarantee: HTTP 429 is treated as **rate limiting**
(per-key cooldown, bounded retry, failover) unless the upstream body carries
explicit quota-exhaustion evidence such as `FREE_QUOTA_EXHAUSTED` — so a
transient 429 can no longer trip a false account-wide "quota exhausted"
circuit while your dashboard still shows remaining credits.

## Known SenseNova quirks (observed 2026-09-06)

- `/v1/messages/count_tokens` does not exist (404); token counts here are a
  local byte-based estimate marked with
  `x-sensenova-proxy-token-count: approximate`.
- Thinking blocks are returned **without a `signature` field** (real Anthropic
  includes one); unsigned thinking blocks round-trip cleanly.
- `tool_choice` enforcement is silently ignored upstream.
- Tool IDs are OpenAI-style `call_…`, not Anthropic `toolu_…` (Claude Code
  round-trips them fine).
- The OpenAI-compat endpoint emits `reasoning` fields and has a non-stream
  parallel-tool-call `index: 0` collision; none of this affects the native
  Anthropic path used here (documented for future maintainers).
- Under modest concurrency the upstream serializes work server-side (~8 s for
  four parallel requests); the default concurrency limit of 2 exists for this
  reason.

## Security model

- The gateway key protects all API routes; it is never forwarded upstream and
  never logged.
- All credentials are removed from upstream error bodies (exact matching plus
  structural redaction of `authorization`, `*api_key`, tokens, cookies,
  Bearer values, and JWT-like strings).
- Client headers are never blindly forwarded; the proxy owns `Authorization`,
  `Host`, `Content-Length`, `Transfer-Encoding`, `Connection`, `Content-Type`,
  `Accept`, `Accept-Encoding`, and `Cookie`.
- Logs contain request IDs, sanitized error classes, and hashed session tags
  — never prompts, tool arguments, headers, or key material.
- Prompts and responses are not logged.

## Troubleshooting

- **Startup: `server.api_key must not be empty`** — choose a local gateway key
  separate from the SenseNova key.
- **HTTP 429 `all configured routes are cooling down; retry shortly`** — every
  route in the profile is cooling. Wait for the `Retry-After` header, add
  another account (`quota_group`) or another model to the profile, and restart.
- **HTTP 429 `circuit open (quota_exhausted)`** — the account's quota signal
  opened the circuit; it will half-open automatically. Check your Token Plan
  balance at https://token.sensenova.cn.
- **HTTP 503 `no usable upstream route is configured for this model`** — no
  route in the selected profile can be dialed at all (every model 404'd, every
  credential rejected). `readyz` reports `usable_routes` and
  `usable_credentials`.
- **HTTP 401 `authentication_error`** — a configured SenseNova key was
  rejected and disabled; correct it in `config.json` and restart. Other
  credentials in other quota groups keep serving.
- **`could not reach the SenseNova upstream`** — DNS/TLS/firewall problem;
  the request was not retried on another credential.
- **`upstream stream ended before message_stop`** — the provider closed the
  stream early; the proxy intentionally did not replay generation.
- **Token counts differ from billing** — expected; the count endpoint is a
  documented approximation.

## Testing

The offline suite never contacts SenseNova; it drives the full gateway
against deterministic loopback mock upstreams:

```bash
cargo fmt --all -- --check
cargo check --all-targets
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
```

Coverage includes: passthrough fidelity (byte-exact fragmented SSE), model
aliasing and literal pass-through, tool-use streaming, mid-stream failure
without replay, malformed SSE, client-disconnect cancellation, direct 429 with
`Retry-After` (seconds and HTTP-date parsing, property-tested), quota
exhaustion with cross-group failover, 401 isolation, transient-retry budgets,
false-positive "429" text rejection, secret redaction, header ownership,
concurrency limits with bounded-queue rejection, and metrics — plus the routing
suite: hard/fast pool isolation (hard can never reach a fast model), tier
ordering, one-429-cools-one-route, distinct-group model tripping, half-open
recovery, cooldown bounds and jitter, the global attempt cap, affinity
hit/expiry/break/bounding, load spreading, in-flight accounting on every path,
latency scoring, and cross-model pre-commit retry.

Live behavior was additionally verified against the real endpoint with a real
key during development (text, streaming, tool calls, a genuine 429 episode,
and readiness transitions); those probes were minimal and are not part of the
test suite.

## Attribution

Design ideas and some defensive code patterns (redaction, retry-duration
parsing, sticky pool structure) were adapted from the MIT-licensed
[`cline-proxy`](https://github.com/jacek4yang/cline-proxy) project. All
SenseNova-specific behavior was independently researched and is documented in
[`docs/sensenova-compatibility.md`](docs/sensenova-compatibility.md).
