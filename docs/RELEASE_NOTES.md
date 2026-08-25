# DeltaWeave v0.1.0

This pre-alpha release is a verified one-file P2P delta-transfer foundation.
It is intended for controlled Windows PC and Synology DSM evaluation, not as
the only copy of important data.

## Included

- Streaming FastCDC manifests and BLAKE3 chunk/file verification
- Durable redb metadata and content-addressed chunk storage
- Authenticated iroh/QUIC transfer with deny-by-default peer allow-listing
- Missing-chunk-only retransmission and safe materialization
- `init`, `manifest`, `serve`, `push`, and isolated `self-test` CLI commands
- Windows x86-64 and static Synology x86-64/ARM64 packages
- SHA-256 checksums for every downloadable archive

Run `deltaweave self-test` (or `deltaweave.exe self-test` on Windows) immediately
after extraction. It validates local QUIC transport, storage, reconstruction,
integrity checks, and delta reuse without touching user data.

Follow the complete
[Windows PC ↔ Synology guide](https://github.com/happyaspic-byte/DeltaWeave/blob/main/docs/TESTING_WINDOWS_SYNOLOGY.md)
for architecture selection, checksums, firewall notes, cross-device transfer,
and hash verification.

## Scope warning

Continuous watching, directory reconciliation, bidirectional conflict handling,
DSM SPK installation, Windows installers/services, and on-demand VFS are planned
but are not part of v0.1.0.
