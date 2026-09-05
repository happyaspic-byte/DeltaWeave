# Security Review Implementation Plan

> **For agentic workers:** Apply systematic debugging and test-driven development to each confirmed defect; independent crate audits run in parallel and receive integration review.

**Goal:** Audit the current security boundaries, fix confirmed issues within compatibility constraints, and verify the resulting worktree.

**Architecture:** Preserve the authenticated iroh protocols, portable record formats, and verified storage design. Investigate network, storage, and synchronization independently, then validate the complete workspace and local P2P flows.

**Tech Stack:** Rust 1.91, Cargo workspace, iroh/QUIC, postcard, redb, BLAKE3, GitHub Actions, Docker Compose.

**Spec:** User's attached security review instructions, read on 2026-09-05; repository SECURITY.md, CONTRIBUTING.md, and docs/THREAT_MODEL.md.

## Constraints

- Preserve the uncommitted work found on continuation; compare against `75ffab7`
  without resetting or replacing the user's working tree.
- Only confirmed security defects justify implementation changes; preserve normal operation and wire/on-disk compatibility.
- Validate dependency findings against current official RustSec/upstream advisories.
- Do not print secrets, modify external systems, suppress new security warnings, or claim unrun checks passed.
- Record severity, confidence, exploit conditions, evidence, disposition, and remaining limitations.

## Work items

- [x] Read project instructions, manifests, runtime/deployment configuration, and threat model.
- [x] Capture pre-fix regression failures and unaffected tests in temporary HEAD copies, plus an unfiltered Cargo.lock security audit. Separate baseline Cargo targets to prevent stale artifact reuse.
- [x] Review and test network identity, authorization, framing, and remote input in `crates/deltaweave-net/src/lib.rs`.
- [x] Review and test paths, manifests, local scans, CAS, and materialization in `crates/deltaweave-{core,store,index}/src/lib.rs`.
- [x] Review and test causal records, reconciliation, CLI validation, and log boundaries in `crates/deltaweave-{reconcile,sync,cli}/src/{lib,main}.rs`.
- [x] Review dependency reachability, CI/release permissions, container configuration, and tracked secret candidates.
- [x] For every confirmed issue, run an adversarial regression against the pre-fix implementation, implement/review the narrow root-cause fix, then run its positive and negative tests.
- [x] Update only necessary dependency manifests/lockfile; retain and explain residual advisories if compatible remediation is unavailable.
- [x] Review integrated diffs and rerun formatting, Clippy, workspace tests, rustdoc, release build, CLI self-test, fault/loopback checks, dependency audit, and available Compose checks. Final workspace: 127 tests, no failures/ignored; release script all 9 gates pass; Windows cross-check passes (runtime remains unverified).
- [x] Write `docs/SECURITY_REVIEW_2026-09-05.md` with finding dispositions, actual commands/results, version changes, and explicit unverified risks; deliver a concise Korean report.

## Validation commands

```sh
cargo audit --json
cargo audit --no-fetch --deny warnings # expected failure: documented paste warning
cargo audit --no-fetch --deny warnings --ignore RUSTSEC-2024-0436
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --all-features
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --all-features
cargo build --locked --workspace --release --all-features
cargo check --locked --workspace --all-targets --all-features --target x86_64-pc-windows-gnu
cargo run --locked -p deltaweave -- self-test
RUST_LOG=warn,netwatch=error target/release/deltaweave self-test
bash scripts/tests/test-p2p-loopback.sh
DELTAWEAVE_BIN="$PWD/target/debug/deltaweave" bash scripts/test-p2p-loopback.sh
bash scripts/verify-release.sh
git diff --check
```
