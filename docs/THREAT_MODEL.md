# Threat model

## Protected assets

- File contents and names beneath the configured destination root
- The node secret key and authenticated endpoint identity
- Metadata integrity, chunk-store integrity, and replacement history
- Host memory, disk, file descriptors, and network capacity

## Assumptions

- iroh's endpoint authentication and encrypted QUIC implementation are trusted.
- BLAKE3 collision resistance is trusted.
- The operating system, administrator, and DeltaWeave process are trusted.
- A remote peer may be malicious even when it can establish an encrypted session.
- Files already present under the destination may be untrusted.

## Defenses in v0.1

- Deny-by-default endpoint allow-list; accepting any authenticated peer requires
  an explicit flag. Unauthorized endpoint IDs are closed before stream intake.
- Strict frame, manifest, chunk-count, file-size, path, and chunk-size limits.
- Wire-path validation also runs during deserialization, preventing constructor
  bypasses.
- Parent symlinks are rejected before materialization.
- Chunk payloads and complete reconstructed files are hash-verified.
- Existing files are moved to private state trash rather than deleted.
- Destination and private state roots may not overlap, and the CLI rejects a
  receiver identity stored beneath the writable destination root.
- Secret keys are created with owner-only permissions on Unix and insecure
  existing Unix key permissions are rejected.

## Known gaps before production

- Path lookup is not yet handle-relative (`openat2`, directory handles, or Windows
  equivalents), so a hostile local process may race an ancestor after validation.
- There is no pairing UX, key rotation, revocation distribution, or rate limiting.
- State and chunks are not encrypted at rest.
- Metadata, permissions, ACLs, alternate streams, sparse extents, xattrs, hard
  links, and symlinks are not synchronized.
- Disk quotas and per-peer concurrency limits are not implemented.
- Local file mutation while the sender chunks and later reads it can cause a
  verified transfer failure; snapshot/identity checks are still required.
- Unicode normalization and case-fold collision handling are not implemented.
- Continuous reconciliation, signed device membership, tombstones, and rollback
  protection belong to later protocol phases.

Operate the receiver with a dedicated unprivileged account and a dedicated empty
destination. Keep separate backups. Do not expose `--allow-any-authenticated` on
an untrusted network.
