# Changelog

All notable changes will be documented here. The project follows Semantic
Versioning after its first stable release.

## [Unreleased]

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
