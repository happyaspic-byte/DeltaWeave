# Architecture

DeltaWeave is split so storage correctness does not depend on the network and
network input is never materialized before content verification.

## v0.1 data path

1. The sender streams a source through FastCDC using the manifest's versioned
   chunking profile.
2. It computes a BLAKE3 digest for each chunk and for the complete file.
3. The receiver validates the portable path and manifest structure, then checks
   its content-addressed store.
4. The receiver requests each missing hash exactly once.
5. Every payload is length- and hash-verified before it enters the chunk store.
6. A temporary file is reconstructed from verified chunks and whole-file hashed.
7. Existing destination content is moved to private trash, the temporary file is
   atomically renamed, and redb metadata is committed.
8. Only then does the receiver return a completion receipt.

## Core invariants

| Area | Invariant |
| --- | --- |
| Manifest | Chunks are non-empty, ordered, contiguous, bounded, and cover the declared size |
| Content | A chunk filename is its BLAKE3 digest; reads are reverified before use |
| Paths | All wire paths are relative, portable, validated during deserialization, and scoped beneath a destination root |
| Authorization | Encryption is not authorization; peer IDs are checked against policy after transport authentication |
| Commit | A destination is never published until every chunk and the complete file hash match |
| Recovery | Prepared and committed operation states make an interrupted apply safe to retry |
| Delta | A repeated hash is transferred at most once, even when it appears in several extents |

## Concurrency and durability

Materialization is serialized within a process. redb provides ACID metadata
transactions, chunk writes use create-new temporary files plus verified rename,
and file/parent directories are synchronized where the platform supports it.
If metadata commit fails after file installation, a later identical transfer
recognizes the whole-file hash and completes the journal idempotently.

The current symlink-ancestor check blocks ordinary path escapes, but is not an
`openat2`/handle-relative defense against a hostile local process racing path
components. That hardening is a pre-production gate.

## v0.2 local index

`deltaweave-index` stores one versioned record per portable path in redb. A
record carries entry type, best-effort stable OS identity, size and modification
fingerprint, complete-file BLAKE3 hash, normalized collision key, version
vector, generation, and tombstone state.

Collision keys apply per-component NFKC normalization and Unicode 16 full case
folding. This deliberately prefers a false-positive operator warning over
silently collapsing two names on a less expressive peer filesystem.

The scanner follows these safety rules:

1. Symbolic links and Windows reparse points are indexed but never traversed.
2. Regular files are hashed only when their metadata remains unchanged before
   and after the read.
3. Locked or mutating files retain their prior record and enter a persistent,
   capped exponential-backoff queue.
4. A directory that cannot be enumerated completely is uncertain. Existing
   records beneath it are preserved rather than inferred as deletions.
5. Stable identities correlate unambiguous renames. Ambiguous identities fall
   back to independent create/delete records.
6. All safe observations, tombstones, retries, generation, and replica counter
   commit in one redb transaction.
7. A database is cryptographically bound to one canonical root and replica ID;
   opening it with a different root or node identity fails before scanning.

Native watcher events are hints, never the source of truth. Normal batches
trigger a complete namespace walk but only rehash touched paths; fixed-interval
authoritative scans rehash every file. Ambiguous events or watcher errors force
an immediate full scan and activate a five-second polling fallback. This keeps
correctness independent of inotify/ReadDirectoryChangesW event loss.
Byte-identical path and retry records are not rewritten during no-change scans,
limiting database write amplification.

Continuous `sync` also attaches a recursive native watcher to the client root.
After a successful pass, a normal local event batch wakes reconciliation after
the default 750 ms quiet period (bounded by a five-second storm deadline).
Remote-only changes are still discovered by the configured periodic poll. If
the watcher cannot start, the same poll remains a correctness-preserving fallback;
native events only reduce latency and never replace authoritative scanning.

## v0.3 distributed state engine

`deltaweave-reconcile` projects host-specific index rows into portable
`SyncRecord` values. A canonical component trie hashes the record at each node,
ordered child names, child hashes, and cardinalities. Peers compare the root and
request only mismatched node summaries; a one-node query completes an unchanged
pass.

One `sync-once` pass follows these gates:

1. Authoritatively scan the local root; abort on read issues, retry-queued files,
   or cross-platform collisions.
2. Reconstruct and verify the remote tree through `deltaweave/sync/2` partial
   queries. The receiver applies the same scan-health gate.
3. Merge each path by version-vector causality. Concurrent identical state
   merges knowledge; divergent state selects a deterministic winner and retains
   losing file bytes under a portable `.conflict-<hash>` name.
4. In a concurrent file-versus-directory conflict, the live directory wins so
   descendants remain materializable and the file becomes a sibling conflict
   copy. Causal directory-to-file transitions still win normally.
5. Stage every required content hash in the local CAS before changing either
   namespace. This prevents conflict data from being lost when a canonical path
   is overwritten.
6. Apply tombstones deepest-first, directories parent-first, and files in path
   order. Existing non-directory content moves to private trash; non-empty
   unknown directories block deletion.
7. Push exact causal records to the remote. The receiver rejects stale,
   concurrent, or equal-clock/different-state records that skipped merge.
8. Rescan local state and reconstruct a fresh remote snapshot. Success is
   reported only when both roots and record counts equal the desired tree.

Directory mtime and read-only flags are deliberately normalized away: child
updates mutate directory timestamps implicitly and directory write semantics
are not portable. Regular-file readonly state is retained. Symlinks/reparse
points and special files remain indexed but are rejected before materialization.

The current orchestrator is two-peer. The merge model is orientation-independent
for records/conflicts and includes a deterministic three-peer partition test,
but production multi-peer membership, tombstone acknowledgement/GC, device
revocation, and protocol migration remain hardening work.
