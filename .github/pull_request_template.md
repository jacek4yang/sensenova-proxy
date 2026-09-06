## What changed?

<!-- One or two sentences. -->

## Why?

<!-- Problem being solved or improvement being made. -->

## Tests

- [ ] Tests added or updated for the changed behavior
- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --all-targets --all-features --locked -- -D warnings`
- [ ] `cargo test --all-targets --all-features --locked`

## Protocol / security implications

- [ ] No change to retry, streaming, tool-use, or credential handling
      (if changed, describe the effect on the commit barrier and error
      classification)

## Checklist

- [ ] No API keys or credentials added anywhere (including tests and logs)
- [ ] Documentation updated if user-visible behavior changed
