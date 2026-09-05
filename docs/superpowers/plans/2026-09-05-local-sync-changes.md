# Local changes during synchronization

> **For agentic workers:** Execute the regression, minimal fix, and verification steps in order. The user authorized autonomous investigation and implementation in this worktree.

**Goal:** Preserve local changes made after the initial sync snapshot and before application of its reconciliation plan.

**Architecture:** Reuse the authoritative index scan, scan-health gate, and canonical Merkle tree. Before applying the plan, require that the local snapshot still matches the snapshot used to compute it. A mismatch must fail the pass before either peer is mutated; the next pass reconciles the new local version.

**Tech stack:** Rust 1.91, Tokio, redb, iroh, existing workspace tests.

**Contract:** `docs/ARCHITECTURE.md` distributed-state gates and `docs/THREAT_MODEL.md` causal-write and content-preservation guarantees. The attached user task requires minimal changes, a failing reproduction, and local verification.

## Constraints and scope

- Preserve existing work, public APIs, dependencies, disk formats, and wire formats.
- Inspect direct push and one-shot folder synchronization; prioritize the local mutation candidate because it can remove a user's fresh edit from both synchronized roots.
- Use synthetic data and direct loopback peers only. No deployment or production changes.
- This check does not provide an OS snapshot or close filesystem races after validation.

## Implementation and verification

- [x] Record baseline workspace tests, formatting, and CLI self-test.
- [x] Add deterministic tests in `crates/deltaweave-sync/src/lib.rs`: cover edited and newly created targets, deletion without partial application, remote-only plans, incomplete scans, and the existing `sync_with_session` continuation. Assert failure and preservation of both peers' actual file contents. Retry normally and check convergence with the fresh contents preserved.
- [x] Run `cargo test --locked -p deltaweave-sync --lib -- --nocapture` against unchanged production code and confirm failures originate from stale plan application.
- [x] At the start of `apply_local`, scan and validate the local index, rebuild its Merkle tree, and compare its root and record count with `current`. Reject a mismatch before applying deletions, type transitions, directories, or files. Use the existing asynchronous scan/read wrappers so the additional scan runs in the blocking task pool.
- [x] Re-run the focused regressions and existing sync integration test.
- [x] Document the gate in `docs/ARCHITECTURE.md`; record the investigated candidates and final results here.
- [x] Run workspace tests with all features, Clippy with warnings denied, formatting, rustdoc, a workspace build, CLI self-test, and patch hygiene. Review the final diff independently.

## Evidence

- Initial worktree: clean at `75ffab7` (`release: prepare DeltaWeave v0.4.0`); no `AGENTS.md` found in the repository or ancestor directories.
- Environment: Linux x86_64, `rustc 1.91.0`, `cargo 1.91.0`. Existing Cargo lockfile and dependencies retained.
- Before production changes, `cargo fmt --all -- --check` and `git diff --check` exited 0.
- Before production changes, `RUST_LOG=warn,netwatch=error cargo run --locked -p deltaweave -- self-test` exited 0 with `status: pass`, bidirectional and deletion verification true, one preserved conflict, and zero restart actions. Direct transfer sent 4,194,304 bytes initially and 257,800 bytes after insertion, reusing 16 extents.
- Baseline `cargo test --workspace --all-targets` exited 0: 103 tests passed, none failed or ignored. The fault-injection integration tests completed in 212.78 seconds and verified forced process termination and durable restart.
- Baseline `DELTAWEAVE_BIN="$PWD/target/debug/deltaweave" RUST_LOG=warn,netwatch=error ./scripts/test-p2p-loopback.sh` exited 0: 104,857,600 bytes transferred; source and received SHA-256 digests matched. No direct-push defect was confirmed in this scope; that investigation is closed.
- First regression run, before production changes: `cargo test -p deltaweave-sync apply_local_ -- --nocapture` exited 101. All four new tests failed because `apply_local` returned success for an existing file edited after the snapshot, a target created after the snapshot, deletion of a freshly edited file, and a changed outgoing source with no local actions. This confirms the missing check rather than a compile or environment failure.
- Complete regression run, before production changes: `cargo test --locked -p deltaweave-sync --lib -- --nocapture` exited 101. All six new regressions failed at their intended rejection assertions; the existing bidirectional/conflict/delete/restart/type-transition integration test passed. The loopback test confirmed that the stale continuation could report success instead of aborting.
- After the minimal fix, the same `cargo test --locked -p deltaweave-sync --lib -- --nocapture` exited 0: all seven tests passed. The loopback regression verifies that abort leaves each peer's own fresh bytes intact, then a normal retry converges and preserves both edits in the canonical file and conflict copy on both peers.
- Independent read-only review found no blocking issues: the guard precedes local and remote application, covers zero local actions, uses authoritative hashing, and retains public compatibility. The reviewer also checked all six regressions and the documented cost/race limitation.

## Final verification

All commands below completed successfully against the fixed source. No existing test was deleted, skipped, or weakened. There were no failures in the baseline and no new regressions in these checks.

| Command | Result |
| --- | --- |
| `cargo test --locked -p deltaweave-sync --lib -- --nocapture` | 7 passed, including all 6 new regressions |
| `cargo test --locked --workspace --all-targets --all-features` | 109 passed; 0 failed; 0 ignored; fault injection and durable restart also passed |
| `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` | Exit 0, no warnings |
| `cargo fmt --all -- --check` | Exit 0 |
| `cargo build --locked --workspace --all-features` | Exit 0 |
| `RUST_LOG=warn,netwatch=error cargo run --locked -p deltaweave -- self-test` | Exit 0, `status: pass`; bidirectional/delete verified; 1 conflict preserved; 0 restart actions |
| `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --all-features` | Exit 0; documentation generated for all 8 crates |
| `git diff --check` | Exit 0 |

The fixed code, six regressions, architecture update, and this investigation record are left as uncommitted worktree changes. There were no dependency, public API, on-disk schema, or wire-format changes. No deployment, publication, or production data modification was performed.

## Candidate conclusions and limitations

- Direct push, delta reuse, authorization, ordinary bidirectional sync, deletion, restart, and sequential type transition: no defect confirmed by the selected baseline tests and CLI runs.
- Local changes after the planning snapshot: reproduced and fixed. The previous implementation could return success after replacing or deleting a fresh edit; existing private trash retained the replaced bytes, but the fresh edit disappeared from the synchronized namespace. The added gate rejects the outdated plan before either namespace is changed.
- Concurrent parent directory-to-file transition and child edit: static follow-up candidate only, not reproduced in this task. Per-path reconciliation may leave a live descendant or conflict copy under a file parent and fail namespace validation. A separate deterministic two-peer reproduction is needed before classifying or fixing it; it is outside the selected local-freshness change.
- A possible tombstone-under-file-ancestor edge was also considered, but no reproducer was established. Do not treat it as a confirmed defect.
- Verification here runs on Linux x86_64 with synthetic local data and direct peers. Windows/Synology hardware behavior and internet/relay operation remain unverified; use the repository's Windows CI and cross-device guide for those checks.
- The new gate performs one additional full local scan/hash per pass. Mutation after that check remains subject to the project's documented lack of filesystem snapshots; this change does not claim to eliminate every filesystem race.
