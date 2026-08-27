# Changelog

All notable changes will be documented here. The project follows Semantic
Versioning after its first stable release.

## [Unreleased]

### Added

- Add the authenticated CAS-only `deltaweave/sync/3` protocol with exact chunk
  availability, verified local-CAS serving, bounded multi-source filling, and an
  experimental `swarm-fill` command.
- Add a rarest-first scheduler with bounded 1/10/100/1,000-peer overlay tests.
- Connect `sync-once` to optional authorized V3 swarm sources: the v2 peer
  remains state authority, V3 sources fill missing CAS hashes, and a v2 content
  pull is the fallback when swarm filling cannot complete.

### Performance

- Overlap durable receiver chunk writes while preserving per-file fsync and
  draining every submitted writer on error. A 64 MiB DirectOnly hardware run
  improved from 4.13 s to a 2.75 s mean; two V3 sources filled the same payload
  in 1.51 s (1.82× single-source throughput).
- Reject unrequested or oversized V3 chunk payloads before allocation, persist
  each fill round before requesting more, and keep duplicate endpoint IDs from
  collapsing distinct source addresses.
- Reuse one sync endpoint and one persistent QUIC connection per V3 source,
  query source availability concurrently, and schedule up to 16 chunks per
  active source in each fill pass.

## [0.3.0] - 2026-08-26

### Added

- Add canonical Merkle path tries, partial-node snapshot queries, portable sync
  records, exact causal apply receipts, and deterministic two-snapshot merge.
- Add `deltaweave-sync` with verified two-way create/update/delete/rename,
  conflict-copy, file/directory transition, restart, and final-root convergence.
- Add `sync-once` and continuous `sync` commands with native debounced local
  wakeups, bounded remote polling, watcher fallback, and capped exponential retry.
- Expand `self-test` to cover bidirectional exchange, concurrent edits, deletion,
  and zero-action durable restart.
- Add actual before/during/after/result synchronization frames and GIF.

### Security

- Reject stale, unmerged-concurrent, and equal-clock/different-state writes.
- Abort synchronization on incomplete scans, queued retries, or path collisions.
- Stage every required conflict content hash before namespace mutation and retain
  remotely replaced/deleted files in private trash.

### Fixed

- Normalize volatile directory mtimes and non-portable readonly semantics to
  prevent child changes from causing perpetual version-vector churn.
- Count actual unique transferred bytes when a manifest repeats chunk hashes.
- Verify readonly state before adopting an exact remote causal record.
- Treat a bound direct UDP socket as ready when restricted containers cannot use
  netlink address discovery.
- Reuse one local iroh endpoint across a complete reconciliation pass.

## [0.2.1] - 2026-08-26

### Added

- Add a GitHub hero image and two compact animated usage demonstrations.
- Add a verified usage gallery with Windows, Synology ARM64, scan, and native
  watcher screens organized as before, during, after, and result stages.
- Add a visual Windows-to-Portainer-to-Synology deployment flow to the AI
  operator runbook.
- Package all usage images, the gallery, and the AI Portainer runbook with every
  Windows and Synology release archive.
- Validate required documentation media in Linux CI.

## [0.2.0] - 2026-08-26

### Added

- Add the `deltaweave-index` crate with persistent redb path records, stable
  file identity, version vectors, tombstones, Unicode/case collision groups,
  and bounded concurrent BLAKE3 hashing.
- Use Unicode 16 full case folding plus NFKC normalization for conservative
  cross-platform collision keys.
- Add `scan` and `watch` commands with native watcher hints, adaptive debounce,
  periodic authoritative scans, and a polling fallback after watcher loss.
- Add opt-in `scan --include-records` output for operational record/retry
  inspection without making large listings the default.
- Extend packaged `self-test` with local indexing, rename correlation, deletion,
  and restart recovery checks.
- Add deterministic operation-storm, native watcher, restart, collision,
  non-portable-name, retry, and Windows sharing-violation coverage.

### Fixed

- Reject additional Windows device-name aliases, including stems with spaces,
  `CLOCK$`, and COM/LPT superscript-digit forms.
- Propagate stdout write failures instead of panicking while printing JSON.
- Preserve prior records beneath unreadable or incompletely enumerated
  directories instead of converting uncertain observations into deletions.
- Exclude a root-level index database without accidentally excluding the entire
  synchronization root.
- Preserve records when a previously indexed subtree becomes ignored, and reject
  ignore rules that contain the entire synchronization root.
- Reject symlink roots and symlinked index databases before opening them.
- Treat every Windows reparse point as a non-traversable link so junctions and
  similar directory redirects cannot escape the indexed root.
- Bind every index database to one canonical root and replica identity so a
  configuration mistake cannot reinterpret another tree as mass deletion.
- Require both stable identity and an unchanged content/metadata fingerprint
  before correlating a rename, preventing inode/file-index reuse from joining
  unrelated files.
- Remove retry records for new files that disappear before their first
  successful hash, while retaining retries beneath genuinely uncertain trees.
- Keep full reconciliation on a fixed schedule even during continuous watcher
  activity, while reusing hashes for untouched files between full scans.
- Avoid rewriting byte-identical path and retry records during no-change scans,
  reducing redb write amplification on large trees.

## [0.1.2] - 2026-08-26

### Fixed

- Normalize Synology archive entries to numeric UID/GID 0 instead of leaking
  the GitHub runner's UID/GID 1001 into release packages.
- Extract Synology packages with `--no-same-owner` in the operator guide so
  installation never attempts to restore archive ownership.

## [0.1.1] - 2026-08-26

### Security

- Reject unauthorized endpoint IDs before waiting for a transfer stream.
- Reject overlapping destination/state roots and receiver identities stored beneath the destination.
- Reject manifests that reuse one chunk hash with conflicting lengths.
- Verify every field in the receiver's transfer receipt.

### Changed

- Move chunk-inventory disk verification off Tokio worker threads.
- Upgrade iroh from 1.0.3 to 1.1.0 while retaining Rust 1.91 compatibility.
- Eliminate a direct-mode startup race with a safe loopback fallback for local self-tests.
- Handle SIGTERM for graceful Portainer/Docker shutdown.
- Disable ANSI log escapes so Windows consoles render clean output.
- Run RustSec audits in CI and execute both Synology release architectures before publishing.
- Apply a container PID limit and suppress invalid future Rust-toolchain Dependabot updates.

## [0.1.0] - 2026-08-26

### Added

- Initial Rust workspace and `deltaweave` CLI
- Streaming FastCDC/BLAKE3 manifests and delta planning
- Verified content-addressed chunk storage and redb metadata
- Authenticated iroh transfer protocol with peer allow-listing
- Journaled file materialization and end-to-end local P2P tests
- Native Windows x86-64 and static Synology x86-64/ARM64 release packages
- `deltaweave self-test` for local QUIC, storage, integrity, and delta checks
- Windows-to-Synology release validation guide and SHA-256 checksums
