# DeltaWeave v0.2.0

This pre-alpha release adds an authoritative local filesystem index to the
verified one-file P2P delta-transfer foundation. It is intended for controlled
Windows PC and Synology DSM evaluation, not as the only copy of important data.

## New local-state foundation

- Persistent redb records for files, directories, symlinks, stable OS identity,
  complete-file BLAKE3 hashes, version vectors, generations, and tombstones
- `scan` for an authoritative full directory scan
- `watch` for native event ingestion with adaptive debounce, touched-path
  rehashing, fixed periodic full verification, and polling fallback
- Unambiguous rename correlation using NTFS file index or Unix device/inode
- Unicode-normalized case-collision groups that preserve every local name
- Persistent capped exponential backoff for locked or mutating files
- Conservative incomplete-scan handling that never treats an unreadable subtree
  as confirmed deletion

## Packaged verification

`deltaweave self-test` now validates both major vertical slices:

- two encrypted local transfers, chunk/file integrity, materialization, and
  delta reuse;
- initial local indexing, stable-identity rename correlation, deletion
  tombstones, and index restart recovery.

CI also exercises the native watcher on Linux and Windows. The Windows suite
holds an exclusive file handle to verify sharing-violation retry and recovery.
Container and release jobs execute packaged binaries on linux/amd64 and
linux/arm64.

## Packages

- Windows x86-64 ZIP
- Static Synology/Linux x86-64 tarball
- Static Synology/Linux ARM64 tarball
- Multi-architecture OCI image on GitHub Container Registry
- SHA-256 checksums for every downloadable archive

Run `deltaweave self-test` (or `deltaweave.exe self-test` on Windows) immediately
after extraction. Then follow the
[Windows PC ↔ Synology guide](https://github.com/happyaspic-byte/DeltaWeave/blob/main/docs/TESTING_WINDOWS_SYNOLOGY.md)
and the
[local-index test guide](https://github.com/happyaspic-byte/DeltaWeave/blob/main/docs/TESTING_LOCAL_INDEX.md).

## Scope warning

The local index is not yet connected to distributed Merkle reconciliation.
Automatic bidirectional propagation, deterministic conflict copies, tombstone
garbage collection, DSM SPK installation, Windows services/installers, and
on-demand VFS remain future phases.
