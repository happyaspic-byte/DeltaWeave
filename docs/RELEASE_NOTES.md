# DeltaWeave v0.3.0

This pre-alpha field preview turns the v0.2 index and verified chunk transport
into an end-to-end, bidirectional Windows/Synology folder synchronizer. It is
for controlled testing with independent backups, not as the only copy of data.
The reproducible gates, explicit deductions, and v0.3-scoped score are recorded
in `QUALITY_REPORT_V0.3.md`, included with every release package.

## Distributed reconciliation

- Canonical path-trie Merkle roots and partial-subtree network queries
- Portable causal records with persistent version vectors and tombstones
- Deterministic, orientation-independent conflict decisions without wall-clock
  causality
- Portable `.conflict-<hash>` copies that preserve losing live-file content
- Safe concurrent file-versus-directory handling that retains the file and a
  materializable directory subtree
- Causal rejection of stale, unmerged-concurrent, and equal-clock divergent
  remote writes

## Bidirectional product path

- `sync-once` scans, merges, stages content, applies both peers, and returns
  success only after independent local/remote Merkle verification
- `sync` wakes on debounced native local filesystem events, polls for remote-only
  changes at a bounded interval, falls back cleanly when watcher startup fails,
  and uses capped exponential backoff after transient failures
- Create, content update, deletion, empty directory, and rename-as-delete/create
  propagation with FastCDC chunk reuse
- Content is staged in the durable CAS before conflict paths are overwritten
- Files replaced or deleted by remote state are retained in private trash;
  non-empty unknown directories are never recursively removed
- One reusable iroh endpoint per reconciliation pass

## Correctness and operations hardening

- Remote and local synchronization abort on incomplete scans, queued retries,
  and case/Unicode collisions
- Directory mtime/readonly normalization prevents child updates from creating
  endless causal churn across Windows and Linux
- Direct-only readiness accepts a successfully bound UDP socket even when a
  restricted Portainer container cannot use netlink discovery
- Exact remote versions are adopted only after filesystem kind, size, content
  hash, and readonly state have been reverified
- Transfer accounting counts actual unique payload bytes, including repeated
  manifest hashes correctly

## Expanded packaged self-test

`deltaweave self-test` now validates:

- encrypted full and delta transfer, BLAKE3 integrity, and extent reuse;
- persistent indexing, rename correlation, tombstones, and restart recovery;
- bidirectional exchange, concurrent conflict-copy preservation, deletion
  propagation, equal final Merkle roots, and zero-action restart idempotence.

CI runs the workspace on Linux and Windows, executes the packaged self-test,
builds static Synology x86-64/ARM64 archives, and exercises the same self-test in
both multi-architecture container images.

## Verified usage visuals

The repository and release archives include reproducible before/during/after/
result terminal frames and a GIF made from an actual v0.3 `sync-once` scenario:
initial two-way exchange, simultaneous edits, conflict copy, deletion, and a
zero-byte no-change retry.

## Windows GUI installer

The first GUI bundle is an unsigned current-user NSIS installer plus the
existing portable ZIP. Windows SmartScreen will warn until a trusted
code-signing certificate is used. Unsigned artifacts are allowed only for
controlled hardware soak.

## Known limits

- Long-running physical Windows/Synology soak remains a field validation gate.
- Symlinks/reparse points and special files are indexed but not materialized.
- Safe tombstone acknowledgement/garbage collection, multi-peer membership,
  pairing/revocation, service installers, DSM SPK, Windows CFAPI, and Linux FUSE
  remain future work.
- Keep an independent backup and read `docs/THREAT_MODEL.md` before exposing a
  receiver.
