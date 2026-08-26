# Roadmap

Roadmap phases are acceptance-gated. Dates are intentionally omitted until the
preceding phase passes correctness, crash-recovery, and cross-platform tests.

## v0.1 — Transfer foundation (implemented in this repository)

- Streaming FastCDC + BLAKE3 manifest construction
- Verified content-addressed chunks and redb metadata
- Authenticated iroh direct/relay transport
- Allow-listed one-file push with missing-chunk transfer
- Journaled, idempotent file materialization
- Unit, corruption, traversal, restart, delta-reuse, and local P2P tests

Exit gate: Linux and Windows CI pass format, lint, docs, unit, and integration
tests from a clean checkout.

## v0.2 — Authoritative local index (implemented; hardware validation ongoing)

- Initial scanner with stable file identity where each OS exposes it
- Normalized Unicode/case collision keys and explicit collision records
- Native watcher ingestion with adaptive debounce and overflow recovery
- Rename correlation, tombstones, ignored paths, and retry/backoff queue
- Mutation-during-read detection and bounded hashing workers

Exit gate: randomized filesystem-operation tests converge after watcher loss,
rename storms, process restart, locked files, and case-collision fixtures.

The implementation and deterministic operation-storm model are in the main
branch. Linux and Windows CI exercise native watching; Windows additionally
holds an exclusive file handle to verify durable retry and recovery. Packaged
Synology binaries run the index restart self-test under both supported CPU
architectures. A long-running Windows/Synology hardware soak remains a field
validation gate, not a claim of distributed synchronization.

## v0.3 — Distributed reconciliation (implemented; hardware soak ongoing)

- Deterministic Merkle search tree over versioned path records
- Partial-subtree query protocol, persistent replica IDs, version vectors, and durable tombstones
- Bidirectional create/update/delete/rename propagation
- Deterministic conflict copies without using wall clocks for causality
- Partition/reconnect and multi-peer convergence simulation
- Final local/remote root verification and retry-safe continuous CLI orchestration
- Native local-change wakeups with 750 ms debounce and polling fallback; periodic
  remote polling remains the bounded discovery path for NAS-side changes

The deterministic three-peer partition model, two-peer create/update/delete,
file/directory transition, conflict, restart, stale-write rejection, and
zero-transfer retry tests run in CI. Long-running Windows/Synology hardware
soak, randomized message interruption, and safe tombstone garbage collection
remain field/hardening gates rather than completed claims.

## v0.4 — Operations and hardening

- Pairing, device revocation, key rotation, bandwidth/concurrency quotas
- Handle-relative path operations, disk-space admission control, observability
- Upgrade/migration tooling, fault injection, compatibility test vectors
- Signed releases and reproducible packaging for Windows, Linux, and Synology

Exit gate: threat-model review, fuzzing, long-running soak tests, and documented
recovery from every injected commit boundary.

## v0.5+ — On-demand files

- Windows Cloud Files API provider and Explorer placeholders
- Linux FUSE3 mount with hydration/dehydration policy
- Range-aware chunk hydration, cancellation, offline behavior, and cache eviction

VFS work begins only after the non-virtualized state engine demonstrates reliable
convergence. CFAPI and FUSE adapters remain separate platform crates so unsafe FFI
can be isolated and audited rather than weakening safe core crates.
