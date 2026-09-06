# Security Policy

## Supported versions

Security fixes are applied to the latest `main` branch and released as new
tags. Older tags do not receive backported fixes.

## Reporting a vulnerability

**Do not open a public issue for security problems.**

- Preferred: use GitHub **Private Vulnerability Reporting** on this
  repository (Security → Report a vulnerability). This keeps the report,
  discussion, and fix coordinated privately.
- If private reporting is unavailable, contact the maintainer directly through
  a private channel and wait for a response before any public disclosure.

Never include real API keys, gateway keys, or `Authorization` header values in
any report. A credential that appears in any disclosure must be treated as
compromised and revoked immediately at its provider (SenseNova platform or
your local gateway configuration).

## Especially sensitive areas

This project is a credential-holding proxy for Claude Code. Reports involving
any of the following are particularly valuable:

- credential leakage into logs, error bodies, or upstream requests;
- authentication bypass of the local gateway;
- **request replay after streaming output has been committed** (duplicated
  tool calls, shell commands, or file edits);
- SSE protocol violations that could corrupt or duplicate model output;
- rate-limit or retry amplification against the upstream;
- bypass of the local admission/concurrency controls.

## Safe handling of secrets

- Never commit `config.json`; it contains real credentials by design.
- CI is fully offline with respect to SenseNova and requires no secrets.
- There are intentionally **no GitHub Actions secrets** in this repository.
