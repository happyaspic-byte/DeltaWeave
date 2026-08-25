# Contributing

DeltaWeave welcomes focused issues and pull requests. Because synchronization
bugs can destroy data, changes are held to correctness-first standards.

## Development

Use the pinned Rust toolchain, then run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
cargo doc --workspace --no-deps
```

New behavior needs tests at the lowest useful layer. Storage and protocol changes
also need a failure-path or adversarial test. Avoid time-based sleeps in tests;
prefer explicit readiness and bounded timeouts.

## Pull requests

- Keep a pull request to one coherent change.
- Explain the invariant being added or preserved, not only the implementation.
- Update protocol/architecture/threat-model documents when their claims change.
- Call out on-disk or wire compatibility explicitly.
- Never commit node keys, real user data, or generated state directories.

Unsafe Rust is forbidden in the current crates. A future OS integration requiring
FFI must live in a narrowly scoped platform crate with documented safety
invariants and dedicated review.
