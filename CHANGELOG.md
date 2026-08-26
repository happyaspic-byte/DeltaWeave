# Changelog

All notable changes will be documented here. The project follows Semantic
Versioning after its first stable release.

## [Unreleased]

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
