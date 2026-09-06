# sensenova-proxy

`sensenova-proxy` is a small Rust gateway that makes SenseTime **SenseNova
Token Plan** APIs work reliably as an **Anthropic-compatible backend for
Claude Code**.

It exposes the Anthropic Messages API locally and forwards requests to
SenseNova's native Anthropic endpoint (`https://token.sensenova.cn/v1/messages`)
over one or more configured SenseNova API keys, adding the pieces the upstream
does not provide:

- gateway authentication and model aliasing,
- bounded, provably safe retries,
- robust 429 / quota / overload handling with cooldowns and a circuit breaker,
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
    │  · retry loop (pre-commit only)        · circuit breaker
    │  · credential pool with quota groups   · secret redaction
    ▼
https://token.sensenova.cn/v1/messages   (native Anthropic compatibility)
```

- `src/config.rs` — file-backed configuration and startup validation.
- `src/server.rs` — routes, middleware, the stream pump, graceful shutdown.
- `src/upstream.rs` — one long-lived rustls `reqwest::Client`; classification,
  retry, failover, and the pre-commit buffering that makes retries safe.
- `src/rate_limit.rs` — error classification and `Retry-After` parsing.
- `src/pool.rs` — sticky credential selection with quota-group semantics.
- `src/circuit.rs` — Closed / Open / HalfOpen circuit breaker.
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
| `GET /readyz`                   | loopback only | Readiness: usable credentials, circuit state.  |
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
and invalid tracing filters. Validation errors identify keys by index only
and never print key values.

### Multiple API keys and quota groups

Multiple keys are supported, but keys from one SenseNova account are **not
assumed to have independent quota**. `quota_group` is the failure domain: an
account-level exhaustion signal cools the whole group, while a per-key rate
limit cools only that key. A 401 permanently disables only the rejected
credential. If a single key serves you well, use one key — the pool adds no
value unless the keys genuinely fail independently.

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

export ANTHROPIC_MODEL=claude-sensenova
export ANTHROPIC_DEFAULT_OPUS_MODEL=claude-sensenova
export ANTHROPIC_DEFAULT_SONNET_MODEL=claude-sensenova
export ANTHROPIC_DEFAULT_HAIKU_MODEL=claude-sensenova

export API_TIMEOUT_MS=600000

claude
```

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

- **429 with another usable key** → immediate failover (bounded by key count).
- **429 with a short `Retry-After` (≤ 10 s) and no other key** → the proxy
  waits out the hint once within its attempt budget, then retries the same
  key.
- **429 otherwise** → the credential cools down (hint duration, or the
  configured fallback) and the client receives HTTP 429 with a `Retry-After`
  header so Claude Code's own backoff can take over.
- **Quota exhaustion** (quota wording or numeric code 8) → the entire quota
  group cools and the circuit opens until the reset hint. No hammering.
- **Sustained 429/5xx/transport failures** → after `overload_threshold`
  failures within `overload_window_secs`, the circuit opens for
  `overload_open_secs` and requests fail fast without dialing SenseNova.
- **401** → that credential is marked unusable (readiness reflects it);
  remaining keys still serve. **403** fails over but does not disable the key
  (it may be model-level).
- **400/404/422** → sanitized passthrough, never retried, never fail over.

Body text merely containing "429" never classifies as a rate limit.

## Retry guarantees

- At most `retry.max_attempts` (default **2**) upstream attempts per logical
  request. Claude Code already retries; the proxy never multiplies its loop.
- Retries happen only for provably safe classes (transport failures, 5xx,
  queue timeouts, 429 per above) and only with bounded, jittered backoff
  (250 ms → 4 s, capped).
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
- **HTTP 429 `all SenseNova API keys are currently rate-limited`** — wait for
  the earliest cooldown (see the `Retry-After` header) or add another enabled
  key and restart.
- **HTTP 429 `circuit open (quota_exhausted)`** — the account's quota signal
  opened the circuit; it will half-open automatically. Check your Token Plan
  balance at https://token.sensenova.cn.
- **HTTP 401 `authentication_error`** — a configured SenseNova key was
  rejected and disabled; correct it in `config.json` and restart. `readyz`
  reports `usable_credentials`.
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
aliasing, tool-use streaming, mid-stream failure without replay, malformed
SSE, client-disconnect cancellation, direct 429 with `Retry-After` (seconds
and HTTP-date parsing, property-tested), quota exhaustion with circuit
opening, 401 failover and readiness, transient-retry budgets, false-positive
"429" text rejection, secret redaction, header ownership, concurrency limits
with bounded-queue rejection, and metrics.

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
