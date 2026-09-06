# SenseNova Token Plan API compatibility matrix

Findings that drove the `sensenova-proxy` architecture. Evidence levels:

- **documented** — stated by official SenseNova documentation or repositories.
- **observed** — measured against `https://token.sensenova.cn` with a real API
  key on 2026-09-06 using minimal, low-cost requests (small `max_tokens`, no
  abusive concurrency).
- **inferred** — deduced from observed behavior without a direct test.
- **unknown** — not measured; the proxy does not rely on it.

The API key used during the investigation was never stored in this repository
and was removed from the probe environment afterwards.

## Endpoints

| Endpoint                            | Status      | Evidence   | Notes                                                    |
| ----------------------------------- | ----------- | ---------- | -------------------------------------------------------- |
| `POST /v1/chat/completions`         | works       | observed   | OpenAI-compatible chat endpoint.                         |
| `POST /v1/messages`                 | works       | observed   | Anthropic Messages-compatible endpoint.                  |
| `POST /v1/messages/count_tokens`    | 404         | observed   | Plain-text `404 page not found` body, not JSON.          |
| `GET  /v1/models`                   | works       | observed   | Rich OpenAI-style model catalog.                         |
| Unauthenticated request             | 401         | observed   | `{"error":{"code":16,"message":"Authorization Not Found"}}` (Google-style numeric code). |

Every observed response carries `X-Request-Id` (also echoed as `request_id`
inside JSON bodies).

## Model catalog (observed 2026-09-06)

`GET /v1/models` returned: `sensenova-6.7-flash-lite`, `sensenova-6.8-flash-lite`,
`deepseek-v4-flash`, `deepseek-v4-pro`, `glm-5.2`, `kimi-k3`,
`sensenova-u1-fast` (image output), `sensenova-u1.5-lite` (image output).

`sensenova-6.8-flash-lite` metadata: text+image input, 262 144 context,
65 536 max output, `supported_sampling_parameters: ["temperature","stop"]`,
`supported_features: ["tools","json_mode","reasoning"]`.

Note: only `temperature` and `stop` are advertised sampling parameters, but
`top_p` and unknown top-level fields were **accepted without error** on
`/v1/chat/completions` (observed).

## Anthropic endpoint (`POST /v1/messages`)

| Capability                    | Result | Evidence | Notes                                                                        |
| ----------------------------- | ------ | -------- | ---------------------------------------------------------------------------- |
| plain text (non-stream)       | works  | observed | Proper `type:"message"` envelope, `stop_reason:"end_turn"`.                  |
| streaming text                | works  | observed | `message_start` → `content_block_start/delta/stop` → `message_delta` → `message_stop`. |
| `system` (string)             | works  | observed | (string form tested on OpenAI endpoint; array form below)                    |
| `system` (array + cache_control) | tolerated | observed | Array-of-text-blocks with `cache_control` accepted.                     |
| tools (`input_schema`)        | works  | observed | `tool_use` blocks with `stop_reason:"tool_use"`.                             |
| `tool_result` round-trip      | works  | observed | Assistant `tool_use` + user `tool_result` history accepted.                  |
| parallel tools (stream)       | works  | observed | Sequential correct block indexes 0/1/2.                                      |
| parallel tools (non-stream)   | inferred | observed on OpenAI endpoint only | Anthropic non-stream parallel calls not directly probed. |
| `tool_choice` enforcement     | IGNORED | observed | `{"type":"any"}` accepted (200) but the model freely replied with text.      |
| images (base64)               | works  | observed | 1×1 PNG correctly identified.                                                |
| `thinking` enabled + budget   | works  | observed | Emits `{"type":"thinking","thinking":"..."}` blocks.                         |
| thinking block `signature`    | **absent** | observed | SenseNova thinking blocks carry **no signature** field.                  |
| unsigned thinking round-trip  | tolerated | observed | Assistant history containing signature-less thinking blocks accepted.      |
| `thinking` disabled/adaptive  | tolerated | observed | `{"type":"disabled"}` and `{"type":"adaptive"}` accepted (200).              |
| `max_tokens` optional         | tolerated | observed | Missing `max_tokens` still succeeded (default applied upstream).            |
| `anthropic-beta` headers      | tolerated | observed | Multiple beta labels accepted without error.                                |
| `metadata`                    | tolerated | observed | Arbitrary metadata object accepted.                                         |
| usage                         | works  | observed | `input_tokens`, `output_tokens`, `cache_read_input_tokens`, `service_tier`, `server_tool_use`. |
| usage `cache_creation_input_tokens` | **absent** | observed | Not present in usage payloads.                                        |
| `stop_sequence` field         | **absent** | observed | Omitted in non-stream responses; `null` in `message_delta`.                  |
| tool IDs                      | `call_*` | observed | OpenAI-style `call_…` IDs, **not** Anthropic `toolu_…`.                     |
| invalid model                 | 404    | observed | `{"type":"error","error":{"type":"not_found_error","message":"model is not found"}}`. |
| malformed JSON                | 400    | observed | `{"type":"error","error":{"type":"invalid_request_error","message":"invalid arguments"}}`. |
| ping events                   | **absent** | observed | No `event: ping` frames observed during streaming.                       |
| count_tokens                  | absent | observed | 404; proxy must estimate locally.                                           |

## OpenAI endpoint (`POST /v1/chat/completions`)

Probed to characterize SenseNova's overall behavior (relevant if a translation
fallback is ever added; sensenova-proxy v1 does not use this path).

| Behavior                        | Result | Evidence | Notes                                                                        |
| ------------------------------- | ------ | -------- | ---------------------------------------------------------------------------- |
| reasoning by default            | yes    | observed | `message.reasoning` / `delta.reasoning` (NOT `reasoning_content`).           |
| missing `content`               | yes    | observed | Reasoning can consume the whole budget: message has no `content` field, `finish_reason:"length"`. |
| disable reasoning               | `thinking:{"type":"disabled"}` | observed | Works; `reasoning_tokens:0`, direct answer.      |
| `enable_thinking:false`         | ignored | observed | Model still reasoned; silently discarded.                                   |
| SSE `finish_reason` mid-stream  | `""`    | observed | Empty string (not `null`) while streaming deltas.                            |
| usage chunk (stream)            | separate | observed | Requires `stream_options:{"include_usage":true}`; arrives as chunk with `choices:[]`, then `data: [DONE]`. |
| tool calls (non-stream)         | works  | observed | `finish_reason:"tool_calls"`.                                                |
| **parallel tool call indexes**  | **all `index:0`** | observed | Non-stream parallel `tool_calls` share `index:0`; IDs remain unique.   |
| tool call streaming             | works  | observed | First delta carries `id`+`name`+empty args; continuation deltas repeat with **empty-string** `id`/`name`. |
| argument fragmentation          | yes    | observed | Long `arguments` JSON split across ≥5 deltas, split mid-string.             |
| `response_format: json_object`  | accepted | observed | Model wrapped output in markdown fences (model behavior, not API).          |
| system messages / content arrays | works | observed | Multi-part text arrays accepted.                                            |
| unknown top-level fields        | tolerated | observed | No validation error.                                                        |

## Rate limiting / errors

- 4 concurrent requests (twice) produced no 429, but all four first-round
  requests took ~8 s while a second burst took ~1.2 s — evidence of **server-side
  queueing/serialization** under modest concurrency (observed). This motivates a
  conservative local concurrency limit.
- **Generic 429 observed in production** during real Claude Code usage: the
  request received HTTP 429 while the Token Plan dashboard still showed
  ~97.7 % of the 5-hour general credit pool remaining. The exact upstream 429
  body was not retained in the earlier log. Consequence: **generic 429 cannot
  be treated as proof of credit exhaustion.** Official semantics distinguish
  the two: HTTP 429 = rate limiting (back off and retry);
  `FREE_QUOTA_EXHAUSTED` = explicit plan-quota exhaustion. Community reports
  describe transient limits such as "inference tpm exhausted" — i.e. serving
  capacity, not credits (no official TPM numbers exist; none are claimed).
- **HTTP 200 + stream EOF before the first byte observed in production**: a
  large (72 KB) streaming request returned HTTP 200 and then ended with no
  SSE bytes at all; replaying it re-submits the full inference, and a client
  disconnect is typically followed by Claude Code re-sending the request
  itself (often as non-streaming). The proxy therefore caps same-key replays
  (`retry.max_same_key_retries`, default 1) and prefers failover — retry
  amplification is bounded by construction.
- Quota-exhaustion classification (updated after the production incident):
  HTTP 429 is `RateLimited` unless the body carries explicit, unambiguous
  quota evidence (`free_quota_exhausted`, quota+exhausted/exceeded,
  `insufficient quota`, or explicit Chinese combinations such as
  额度已耗尽/积分已耗尽/积分不足/余额不足). Google-style numeric code 8
  (`RESOURCE_EXHAUSTED`) without such evidence is **rate limiting** — it can
  describe RPM/TPM/concurrency/capacity exhaustion just as well. Bare
  `exhausted` / `balance` / `quota` / `配额` never suffice. Classification
  logs record a machine-readable reason (`http_429`,
  `resource_exhausted_code`, `explicit_quota_evidence`, ...) plus the bounded
  `error.type` / `error_key` marker and numeric code, so verdicts are
  auditable.
- Authoritative quota querying: **intentionally not implemented.** The
  logged-in dashboard reads live pool data from control-plane endpoints
  discovered in the console JavaScript bundle
  (`/lite/console/v1/tokenplan/pool-usage`, `.../credit-usage-trend` on
  `platform.sensenova.cn`). Both explicitly reject API-key authentication —
  an API-key request answers `401` with `error_key: auth_type_disabled`
  ("Authentication type 'apikey' is not enabled", observed) — and the same
  paths do not exist on the `token.sensenova.cn` data plane (404, code 5).
  They require a browser console session, which this proxy will not
  automate or persist. Dashboard quota data is control-plane only.
- Error envelope duality (observed): OpenAI-style routes return
  `{"error":{"code":N,"message":…}}`; the Anthropic route returns
  `{"type":"error","error":{"type":…,"message":…}}`.

## DeepSeek V4 Pro (`deepseek-v4-pro`)

Probed 2026-09-06 on both endpoints. Evidence level: **observed** except
where noted. Catalog metadata (`GET /v1/models`): text-only I/O, 1 048 576
context, 65 536 max output, features `tools`/`json_mode`/`reasoning`
(**upstream metadata**, not independently load-tested).

### Model identifier resolution (observed)

| Candidate on `/v1/messages` | Result                                                                 |
| --------------------------- | ---------------------------------------------------------------------- |
| `deepseek-v4-pro`           | **200 OK** — the correct identifier.                                    |
| `deepseek/deepseek-v4-pro`  | 404 `not_found_error` "model is not found".                              |
| `sensenova/deepseek-v4-pro` | 404 `not_found_error` "model is not found".                              |

A previously reported `400 invalid model format. Expected format:
modelType/model` for the plain ID **did not reproduce**; the endpoint
currently accepts the plain catalog ID. Upstream resolves the model to a
dated snapshot: responses report `"model": "deepseek-v4-pro-0813"`.

### Anthropic endpoint capabilities (all observed)

| Capability                        | Result | Notes                                                                        |
| --------------------------------- | ------ | ---------------------------------------------------------------------------- |
| plain text (non-stream)           | works  |                                                                              |
| reasoning                         | works  | **On by default**: a signature-less `thinking` block is emitted even without a `thinking` parameter. |
| `thinking: {"type":"disabled"}`   | works  | Suppresses reasoning entirely (no thinking block).                            |
| `thinking: {"type":"enabled","budget_tokens":N}` | works | Accepted; thinking block returned.                    |
| tools + `tool_use`                | works  | `call_*` IDs, `stop_reason:"tool_use"`.                                      |
| `tool_result` round-trip          | works  | Including unsigned thinking blocks in assistant history.                     |
| streaming text                    | works  | Standard Anthropic lifecycle.                                                |
| streaming tools                   | works  | `input_json_delta` fragmented across many deltas (observed split mid-string). |
| `system` (array) + `temperature`  | works  |                                                                              |
| images                            | not tested | Catalog says text-only input.                                            |
| 1M context                        | not tested | Advertised upstream metadata; no large-context load test performed.      |

### OpenAI endpoint (`/v1/chat/completions`, observed)

Also serves `deepseek-v4-pro` (text with `reasoning_content` field,
`reasoning_tokens` in usage). Not needed by this proxy: the native Anthropic
path works, so no OpenAI translation layer exists.

## Claude Code compatibility assessment

- Claude Code's Anthropic Messages workload (system+tools+tool loop, streaming,
  parallel tool calls, images) is served **natively** by `/v1/messages`.
- The unsigned thinking blocks are compatible with Claude Code's pass-back
  behavior because the upstream itself tolerates signature-less thinking blocks
  (observed).
- `tool_choice` cannot be enforced upstream; Claude Code primarily uses
  `auto`, so impact is limited (documented as a known quirk).

## Architecture decision

**Strategy A — native Anthropic passthrough** was selected (this also
covers `deepseek-v4-pro`, which was later confirmed to work natively on
`/v1/messages` — no model-specific adapter is needed):

1. `/v1/messages` is genuinely compatible with the full Claude Code workload
   (observed), including streaming lifecycle and tool use.
2. Protocol conversion (Strategy B) would add a large, risk-carrying SSE state
   machine for no measured benefit.
3. The proxy instead concentrates on what the upstream lacks: gateway auth,
   model aliasing, local token counting, bounded retries, 429/quota handling
   with cooldown + circuit breaking, concurrency shaping, secret redaction,
   stream validation, and observability.

The OpenAI endpoint remains documented above as a future fallback path, but no
translation layer is implemented in v1 (deliberate, per the project's
"avoid unnecessary complexity" mandate).
