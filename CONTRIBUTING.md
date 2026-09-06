# Contributing

Thanks for considering a contribution. This is a small, focused project —
correctness and auditability are valued over feature count.

## Toolchain

- Rust **1.88 or newer** (stable). The MSRV is declared in `Cargo.toml`.
- A committed `Cargo.lock` pins the dependency graph; keep it updated with
  your changes (do not delete it).

## Build, test, lint

```bash
cargo fmt --all -- --check
cargo check --all-targets --all-features --locked
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-targets --all-features --locked
```

All four must pass for CI to accept a PR. Fix warnings rather than
suppressing them; any `#[allow]` needs a written justification.

## Tests

The normal test suite is fully **offline**: it drives the gateway against
deterministic loopback mock upstreams and never contacts
`token.sensenova.cn`. New behavior needs tests, especially:

- request/response protocol conversion and normalization rules;
- SSE fragmentation and stream-failure paths;
- retry, rate-limit classification, and circuit-breaker behavior;
- secret redaction and header ownership.

Live SenseNova probes are **opt-in and manual** — they are not part of CI and
must never be run with a real key inside any CI environment.

## Pull requests

- Keep PRs focused; one logical change per PR.
- `main` is protected: changes land via PR, CI (`CI / Required`), and squash
  merge. The history is linear.
- If your change affects retry, streaming, tool-use, redaction, or
  credential-handling behavior, say so explicitly in the PR description and
  update `docs/sensenova-compatibility.md` if upstream behavior is involved.

## Security

- **Never commit API keys** (SenseNova keys, gateway keys, or any other
  credential). `config.json` is git-ignored for this reason.
- Never paste API keys or `Authorization` headers into issues or PRs.
- Report vulnerabilities privately — see [SECURITY.md](SECURITY.md).
