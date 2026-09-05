# Sparse synchronization performance implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development for bounded implementation and independent review. The user authorizes autonomous investigation, implementation and measurement in this existing worktree.

**Goal:** Measure and remove repeated full-record scans when reconstructing a mostly unchanged remote Merkle snapshot, and verify a complete sparse synchronization round under identical local conditions.

**Architecture:** Preserve the immutable Merkle tree, validation, hashes and protocol. Use the existing ordered record map to seek directly to an exact record and its descendants. No cache or concurrency change.

**Tech stack:** Rust 1.91.0, existing Cargo.lock, BTreeMap, redb, BLAKE3, authenticated iroh QUIC.

**Spec:** User attachment `pasted-text-1.txt`: reproducible baseline, root cause, minimal improvement, identical remeasurement, correctness gates, Korean report. No deployment or unrelated features.

## Constraints

- Preserve existing changes; initial HEAD is `75ffab7`, worktree initially clean.
- Preserve path validation, canonical ordering, exact records, implicit directories, tombstones, Unicode, maximum path lengths and all security/integrity checks.
- Benchmark release profile (thin LTO, one codegen unit), pinned toolchain and lockfile; one measured run at a time with no parallel build/test load.
- Distinguish setup, first synchronization and warmed runs. Record raw samples, median and min/max; do not infer percentiles from small samples.
- Compare time, CPU and peak RSS; require zero errors, identical verified roots, correct action/query counts and file bytes. No-op and nested layout are controls.
- Retain only optimizations with improvements beyond observed noise. Do not promise an arbitrary percentage.

## Tasks

- [x] Add `crates/deltaweave-sync/examples/sparse_sync.rs`: deterministic isolated two-root, allowlisted loopback full-sync benchmark with distinct identities; shared causal history seeded with two authoritative scans and a validated fixture-only bulk version transaction outside timing; one changed remote file among 1,000 / 4,000 files, plus no-op and nested controls; one warmup and seven measured repetitions. CSV output and explicit correctness assertions.
- [x] Build unchanged production code plus harness; save executable and baseline samples with commands/environment. Supplement with deterministic record-query cost evidence and profiler if available.
- [x] Add regression coverage in `crates/deltaweave-reconcile/src/lib.rs` before implementation: exact/subtree canonical results, punctuation siblings between exact key and descendants, implicit/missing paths, Unicode/tombstones, invalid and maximum-length prefixes. Test performance through baseline harness rather than flaky timing assertions.
- [x] Add `Borrow<str>` to `WirePath` in `crates/deltaweave-core/src/lib.rs`; implement exact lookup followed by `range::<str, _>((Included(child_prefix.as_str()), Unbounded)).take_while(...)` in `records_under`. Validate prefix first; preserve empty-root fast path. Run core/reconcile tests.
- [x] Rebuild identical release harness; run the same datasets/samples. Run full workspace formatting, clippy, tests, rustdoc and release build, packaged self-test and existing fault tests. Record unavailable platform checks accurately.
- [x] Independently review code and measurement methodology. Record results, absolute differences, percentages, ranges, sample sizes, reproducible commands and limits in `docs/performance/2026-09-05-sparse-sync.md`; update relevant complexity documentation.

## Investigation evidence

`MerkleTree::records_under` scans every record at each nonempty prefix. `SyncClient::fetch_snapshot_connected` calls it once per matching child. For N flat records and one changed file, N-1 lookups inspect N records each despite only two network node queries. The index's full authoritative hashing is intentional and remains unchanged. Per-file hash buffer allocations and per-action receiver rescans are deferred candidates, outside this change.

## Execution record

- Benchmark methodology reviewed independently before runs; no blockers.
- Added behavior-preservation tests passed against baseline: 11 core and 15 reconcile tests. The measured lookup baseline, rather than a flaky wall-time unit assertion, demonstrates the performance defect.
- Production range implementation independently reviewed; no blockers, awaiting post-change execution.
- Baseline binaries are preserved in `target/performance/*-before`; first full benchmark collection runs sequentially without own compilation/test load.

- Measurement adjustment: per-record fixture adoption took over four minutes for half of 4,000 records under shared load. Interrupted that pilot explicitly; incomplete data excluded. Revised benchmark prepares only synthetic causal history in one transaction after both full scans, then reopens and fully rescans to verify exact records. Existing locked redb/postcard are direct dev dependencies only. Both baseline and optimized binaries are rebuilt with this identical harness.

- Storage check found default `/tmp` on tmpfs. Final paired measurements set `TMPDIR` to `target/performance/fixtures` on the worktree ext4 filesystem for both binaries; tmpfs measurements remain labeled exploratory. Cache state stays warm; no shared OS cache flush.

## Final verification

- Same ext4 fixture location, four-CPU affinity, release profile, harness, deterministic data and seven warm samples per scenario/version. Saved original baseline re-run immediately before optimized binary for each configuration.
- Flat 4,000-file edit: 1606.886 ms median [1560.616, 1721.091] to 1373.449 ms [1309.944, 1393.486], -233.437 ms / -14.53%. No-op control -0.74%; all non-resource outputs identical. Smaller/nested changes are within variation and are not claimed as confirmed gains.
- 84 measured sync calls (102 with initial/warmup calls), zero errors; six independent final filesystem checks agree exactly. Subtree lookup 4,000-record median 306.507 ms to 4.201 ms supports the root-cause attribution.
- Full release build, 107 workspace tests, strict Clippy, strict rustdoc, formatting, release CLI self-test all passed. The full test suite includes fault injection and unauthorized-peer rejection.
- Process high-water memory includes preparation and is not improved consistently; final report records exact maxima. No cache or concurrency behavior changed. Windows/NAS/WAN/cold-disk and unavailable perf counters remain explicitly outside measured claims.
- Production algorithm/tests and revised fixture methodology independently reviewed with no blockers. Raw final evidence is saved in the worktree under docs/performance/data/2026-09-05-sparse-sync/.

- Final independent completion audit approved: numerical tables match archived CSVs, peak RSS increase disclosed, source and harness provenance verified, all required gates passed, and claims remain limited to the measured workload.
