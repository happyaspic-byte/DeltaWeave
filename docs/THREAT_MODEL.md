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

## Defenses in v0.3

- Deny-by-default endpoint allow-list; accepting any authenticated peer requires
  an explicit flag. Unauthorized endpoint IDs are closed before stream intake.
- Strict frame, manifest, chunk-count, file-size, path, and chunk-size limits.
- Wire-path validation also runs during deserialization, preventing constructor
  bypasses.
- Parent symlinks are rejected before materialization.
- Chunk payloads and complete reconstructed files are hash-verified.
- Existing files are moved to private state trash rather than deleted.
- Recovery directories are reserved atomically so process restarts cannot reuse
  and overwrite prior trash entries; dangling symlinks are preserved on replacement.
- Existing files are reused only after their size, chunk digests, and complete hash
  match the manifest. Multiply linked files are replaced with a new inode before
  applying readonly changes, avoiding permission changes outside the destination.
- Destination and private state roots may not overlap, and the CLI rejects a
  receiver identity stored beneath the writable destination root.
- Secret keys are created with owner-only permissions on Unix and insecure
  existing Unix key permissions are rejected.
- CLI server/sync identity locations are checked before a new secret is created.
- Newly created Unix state directories and index, metadata, and sender-cache DB
  files are owner-only. Existing operator-managed file/directory permissions and
  Windows ACLs are not rewritten automatically.
- Protocol errors and receiver failure logs use bounded recovery guidance instead
  of serializing internal filesystem paths and database errors to peers.
- Local scans never follow symlinks, verify metadata stability around hashing,
  and retain prior records when enumeration or reads are uncertain.
- Cross-platform Unicode/case name collisions are reported without collapsing
  or overwriting either local record.
- Watcher events are only optimization hints; periodic scans and polling fallback
  prevent event loss from becoming authoritative state loss.
- Each index DB is bound to one canonical root and replica identity, preventing
  accidental reuse from being interpreted as mass deletion.
- Remote snapshots are accepted only after rebuilding and matching their Merkle
  root and record count; unhealthy local or remote scans abort reconciliation.
- Merkle child names, cardinalities, prefixes, and parent commitments are checked
  before scheduling further work; queued and issued queries share a bounded budget.
- The merged live namespace is checked for case/Unicode collisions before staging,
  and local state is rescanned before applying a previously computed action plan.
- Version vectors reject stale, unmerged-concurrent, and equal-clock divergent
  writes before namespace replacement.
- Required conflict contents enter verified CAS before either peer is mutated,
  and non-empty unknown directories block remote deletion.
- A post-apply local rescan and fresh remote Merkle snapshot must both equal the
  desired root before `sync-once` reports success.

## Known gaps before production

- Path lookup is not yet handle-relative (`openat2`, directory handles, or Windows
  equivalents), so a hostile local process may race an ancestor after validation.
- There is no pairing UX, key rotation, revocation distribution, or rate limiting.
- State and chunks are not encrypted at rest.
- Metadata, permissions, ACLs, alternate streams, sparse extents, xattrs, hard
  links, and symlinks are not synchronized.
- Disk quotas and per-peer concurrency limits are not implemented.
- Local file mutation while the sender chunks and later reads it causes a safe
  verified transfer failure and retry, but OS snapshot integration is absent.
- Tombstones participate in distributed Merkle reconciliation, but safe
  acknowledgement-based retention/GC, signed device membership, and rollback
  protection across removed devices are not implemented.
- Name collisions are detected but there is not yet a cross-device operator UX
  or automatic resolution policy.
- Live-path/tombstone aliases on case-insensitive filesystems need platform tests;
  live namespace collision rejection does not prove that an aliased deletion is safe.
- Causal state is implemented for two-peer orchestration; membership changes,
  malicious history amplification, and multi-peer admission policy are not.

Operate the receiver with a dedicated unprivileged account and a dedicated empty
destination. Keep separate backups. Do not expose `--allow-any-authenticated` on
an untrusted network.

See [the 2026-09-05 security review](SECURITY_REVIEW_2026-09-05.md) for synthetic
reproductions, dependency advisory dispositions, verification, and remaining limits.
