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
- An allow-listed peer is authorized for the entire configured receiver root,
  including both one-file push and reconciliation; there are no per-path or
  read-only peer permissions.

## Defenses in the current implementation

- Deny-by-default endpoint allow-list; accepting any authenticated peer requires
  an explicit flag. Unauthorized endpoint IDs are closed before stream intake.
- A 16 MiB control-frame limit, portable-path and manifest validation, and
  profile-bounded chunk lengths. Incoming v1/v2 pushes additionally enforce
  250,000 chunks and 16 TiB per file; v2 pulls do not enforce these two
  push-specific limits. See [protocol limits](PROTOCOL.md#resource-limits).
- Wire-path validation also runs during deserialization, preventing constructor
  bypasses.
- Parent symlinks are rejected before materialization.
- Chunk payloads and complete reconstructed files are hash-verified.
- Verified network chunks are persisted through a bounded per-transfer write
  pipeline (eight tasks, eight chunks per batch, and a 32 MiB queued-byte
  budget). This is not a bound on total process memory or concurrent peers.
- Existing files are moved to private state trash rather than deleted.
- Destination and private state roots may not overlap, and the CLI rejects a
  receiver identity stored beneath the writable destination root.
- Secret keys are created with owner-only permissions on Unix and insecure
  existing Unix key permissions are rejected.
- Local scans never follow symlinks, verify metadata stability around hashing,
  and retain prior records when enumeration or reads are uncertain.
- Cross-platform Unicode/case name collisions are reported without collapsing
  or overwriting either local record.
- Watcher events are only optimization hints; periodic scans and polling fallback
  prevent event loss from becoming authoritative state loss.
- Each index DB stores its canonical-root-path/OS hash and replica identity,
  rejecting accidental reuse with a different path or replica. This does not
  detect replacement of the directory at the same path or malicious edits to
  trusted private state.
- Remote snapshots are accepted only after rebuilding and matching their Merkle
  root and record count; unhealthy local or remote scans abort reconciliation.
- V2 receiver mutations use a fresh scan and version-vector precondition to
  reject stale, unmerged-concurrent, and equal-clock divergent writes before
  namespace replacement. V1 push is an authorized overwrite operation without
  that causal check, and is adopted as a receiver-local event.
- Required conflict contents enter verified CAS before either peer is mutated,
  and non-empty unknown directories block remote deletion.
- A post-apply local rescan and fresh remote Merkle snapshot must both equal the
  desired root before `sync-once` reports success.

## Known gaps before production

- Path lookup is not yet handle-relative (`openat2`, directory handles, or Windows
  equivalents), so a hostile local process may race an ancestor after validation.
- There is no pairing UX, key rotation, revocation distribution, or rate limiting.
- State and chunks are not encrypted at rest.
- Only regular-file readonly state is synchronized from filesystem permissions.
  File timestamps, ownership, full permission modes, ACLs, alternate streams,
  sparse extents, xattrs, hard-link relationships, and symlinks are not
  synchronized. Directory readonly state is normalized to writable.
- Disk quotas and per-peer concurrency limits are not implemented.
- Frame limits do not bound total histories or namespace work. V2 snapshots
  allow up to 1,000,000 records and the client permits 1,000,000 node queries;
  server query sessions have no separate query-count cap. Full snapshots and
  version vectors remain in memory, and there are no application-level transfer
  deadlines or per-peer work quotas.
- Sender metadata checks and requested-chunk verification detect source drift
  when observed. They do not provide an OS snapshot or guarantee that a transfer
  contains the source's latest state after manifest preparation; reused chunks
  are not reread from the source.
- Push handlers do not require upload EOF or reject trailing data after all
  requested chunks have arrived. An error response is best-effort and may not
  reach the client after a transport or storage failure.
- A Merkle root proves consistency with the authenticated peer's advertised
  snapshot, not truthfulness of its filesystem or version history. Records and
  receipts have no independent signature or membership proof.
- Apply locks serialize receiver mutations within that server, but do not
  exclude local writers. The client applies its initial local plan without a
  fresh causal check per action. Filesystem, journal, and index updates are
  separate; final verification does not roll back completed actions or guarantee
  that every racing local edit is retained in the live namespace.
- Store retries can complete an already installed file, but there is no startup
  journal replay or automatic trash restoration. Moving an old file to trash
  and installing its replacement are separate renames. Destination and trash
  must share a filesystem, and directory synchronization is implemented only
  on Unix. Crash recovery at every commit boundary remains a validation gate.
- Tombstones participate in distributed Merkle reconciliation, but safe
  acknowledgement-based retention/GC, signed device membership, and rollback
  protection across removed devices are not implemented.
- Name collisions are detected but there is not yet a cross-device operator UX
  or automatic resolution policy.
- Causal state is implemented for two-peer orchestration; membership changes,
  malicious history amplification, and multi-peer admission policy are not.

These defenses and gaps follow the current
[`net`](../crates/deltaweave-net/src/lib.rs),
[`store`](../crates/deltaweave-store/src/lib.rs),
[`index`](../crates/deltaweave-index/src/lib.rs), and
[`sync`](../crates/deltaweave-sync/src/lib.rs) implementations; they are not a
claim of production validation. Follow the [security policy](../SECURITY.md):
use test data, a dedicated unprivileged account, an explicit allow-list, and
independent backups. Do not use `--allow-any-authenticated` on an untrusted network.
