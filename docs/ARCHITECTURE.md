# Architecture

DeltaWeave is split so storage correctness does not depend on the network and
network input is never materialized before content verification.

The workspace contains nine crates: `deltaweave-core` defines shared types,
`deltaweave-cdc` builds manifests, `deltaweave-store` owns verified chunks and
file materialization, `deltaweave-index` scans local state,
`deltaweave-reconcile` computes deterministic merges, `deltaweave-net` serves
both protocol versions, `deltaweave-sync` orchestrates two-peer passes, and
`deltaweave-cli` provides commands and continuous watcher/poll loops.
`deltaweave-web` embeds a local browser UI and serializes scan, receive, and sync
operations against the same engine. Its HTTP listener requires a loopback address,
a process-specific bearer token, and matching Host/Origin headers. See
[WEB_UI.md](WEB_UI.md) for the separate HTTP management interface.
The v0.x headings below identify roadmap milestones; the workspace package
version is defined separately in [`Cargo.toml`](../Cargo.toml).

## v0.1 data path

1. The sender streams a source through FastCDC using the manifest's versioned
   chunking profile. Path-based chunking selects 512 KiB / 1 MiB / 4 MiB for
   files at least 8 GiB when the default 64 KiB / 256 KiB / 1 MiB profile was
   requested; an explicitly different profile is retained.
2. It computes a BLAKE3 digest for each chunk and for the complete file.
3. The receiver validates the portable path and manifest structure, then checks
   its content-addressed store.
4. The receiver requests each missing hash exactly once.
5. Every payload is length- and hash-verified before it enters the chunk store.
6. A temporary file is reconstructed from verified chunks and whole-file hashed.
7. Existing destination content is moved to private trash, the temporary file is
   renamed into place, and the manifest and committed journal state are written
   to redb in separate transactions.
8. The receiver adopts the installed file into its local index, then returns a
   completion receipt. V1 creates a receiver-local causal event; v2 adopts the
   supplied causal record after its precondition succeeds.

V1 `push` can cache sender manifests in `sender-manifests.redb` when a sender
state root is supplied. Cache eligibility requires stable file identity and
change time, currently available through the Unix implementation. Schema,
generator, profile, size, mtime, change time, identity, and readonly state must
match, with before/after metadata checked on the opened source handle. Cache failures fall
back to manifest construction; requested payload chunks are still reverified.
V2 push/pull and local staging construct manifests without this persistent cache.

## Core invariants

| Area | Invariant |
| --- | --- |
| Manifest | Chunks are non-empty, ordered, contiguous, bounded, and cover the declared size |
| Content | A chunk's sharded path is derived from its BLAKE3 digest; reads are reverified before use |
| Paths | All wire paths are relative, portable, validated during deserialization, and scoped beneath a destination root |
| Authorization | Encryption is not authorization; peer IDs are checked against policy after transport authentication |
| Commit | A destination is never published until every chunk and the complete file hash match |
| Recovery | Prepared and committed file-operation states support identical-content retries; they do not form a transaction across the filesystem and both databases |
| Delta | A repeated hash is transferred at most once per transfer, even when it appears in several extents |

## Concurrency and durability

Each `Store` serializes file, directory, and removal operations with its own
mutex. The receiver also shares an apply lock between v1 and v2 mutations;
v2 holds that lock across the fresh causal-precondition scan and index adoption.
It does not lock out external filesystem writers or make a whole synchronization
pass atomic.

Network receives validate each payload once into `VerifiedChunk`, then submit
batches of up to eight chunks to blocking store tasks. The pipeline allows up to
eight tasks and budgets 32 MiB across pending and in-flight chunk bytes per
transfer. It waits for outstanding writes even after a receive/write failure.
These limits exclude the current receive buffer, manifests, and other transfers.
Each new chunk file is synchronized, and batch writes synchronize each affected
chunk-parent directory once.

redb provides ACID transactions within each database. The receiver places
`metadata.redb`, `chunks/`, `tmp/`, `trash/`, and `index.redb` beneath its state
root. A sync client uses `index.redb` plus a `store/` subdirectory containing its
CAS, metadata, and trash. Public and private roots must not overlap. Replacements
and removals rename old content into private trash, so the destination and trash
must be on the same filesystem; there is no cross-filesystem copy fallback.

Materialization uses a create-new temporary file beside the destination. Its
final rename is atomic, but moving the old destination to trash and installing
the new file are separate operations with an interval when the path is absent.
Manifest, journal, and index commits are also separate. If metadata commit fails
after installation, an identical retry can recognize the whole-file hash and
complete the store journal without rewriting the file. There is no startup
journal replay or automatic trash restoration. File synchronization is explicit;
parent-directory synchronization is implemented on Unix and is a no-op on other platforms.

After installation, a `MaterializationObservation` carries the verified digest
and metadata fingerprint into index adoption. Matching change time, identity,
size, mtime, and readonly state can avoid another hash pass. When the fingerprint
cannot be trusted (including missing change time on Windows), adoption falls
back to hashing the file stably. This optimization does not exclude a concurrent
writer around the readonly permission update.

The existing-file fast path also verifies the manifest's size and each chunk
digest in the same read before committing metadata. A file with other hard links
is rebuilt into a separate inode before readonly changes. Trash directory names
are atomically reserved, preserving prior recovery entries across process restarts.

The current symlink-ancestor check blocks ordinary path escapes, but is not an
`openat2`/handle-relative defense against a hostile local process racing path
components. That hardening is a pre-production gate.

## v0.2 local index

`deltaweave-index` stores one versioned record per portable path in redb. A
record carries entry type, best-effort stable OS identity, size and modification
fingerprint, complete-file BLAKE3 hash, normalized collision key, version
vector, generation, and tombstone state.

Collision keys apply per-component NFKC normalization, ICU full case folding,
then NFKC again. Unicode data comes from the normalization and ICU dependencies
pinned in `Cargo.lock`. This deliberately prefers a false-positive operator
warning over silently collapsing two names on a less expressive peer filesystem.

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
7. A database stores a hash of its canonical root path and OS plus its replica
   ID; opening it with a different root path or replica fails before scanning.
   This prevents accidental reuse, not malicious edits to trusted private state
   or replacement of the directory at the same path.

Native watcher events are hints, never the source of truth. Normal batches
trigger a complete namespace walk, reusing hashes only for metadata-stable files
outside touched paths; fixed-interval authoritative scans attempt to rehash
every file, subject to read failures and retry backoff. In `watch`,
ambiguous events or watcher errors force a full scan on the next loop tick and
activate the default five-second polling fallback. This keeps
correctness independent of inotify/ReadDirectoryChangesW event loss.
Byte-identical path and retry records are not rewritten during no-change scans,
limiting database write amplification.

Continuous `sync` also attaches a recursive native watcher to the client root.
After a successful pass, a normal local event batch wakes reconciliation after
the default 750 ms quiet period (bounded by the default five-second storm deadline).
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
   losing live file bytes when their content differs or the winner is a
   tombstone. Conflict copies are siblings such as `report.conflict-<hash>.txt`;
   metadata-only conflicts over identical file bytes do not create extra copies.
4. In a concurrent file-versus-directory conflict, the live directory wins so
   descendants remain materializable and the file becomes a sibling conflict
   copy. Causal directory-to-file transitions still win normally.
5. Stage every required content hash in the local CAS before changing either
   namespace. This prevents conflict data from being lost when a canonical path
   is overwritten.
   Before staging, validate the merged live namespace for case/Unicode collisions.
6. Authoritatively rescan the local root and require its Merkle root and record
   count to match the initial local snapshot before applying the plan, including
   passes with only remote actions. Incomplete scans or local changes abort the
   pass so a retry reconciles the fresh state. This adds a full local rehash; it
   does not provide an OS snapshot or prevent filesystem races after the check.
7. Apply local tombstones deepest-first, remove objects requiring a kind change
   deepest-first, create directories parent-first, and apply files in path
   order. Existing non-directory content moves to private trash; any non-empty
   directory blocks removal.
8. Push exact causal records to the remote. The receiver rejects stale,
   concurrent, or equal-clock/different-state records that skipped merge.
9. Rescan local state and reconstruct a fresh remote snapshot. Success is
   reported only when both roots and record counts equal the desired tree.

File mtimes and OS identities remain local index metadata and are not transmitted
in `SyncRecord`; full permission modes, ownership, ACLs, and xattrs are absent.
Directory mtime and read-only flags are normalized away: child updates mutate
directory timestamps implicitly and directory write semantics are not portable.
Regular-file readonly state is retained. Symlinks/reparse points and special
files remain indexed but are rejected before live materialization.

Snapshot and merge records are fully held in memory. Merkle queries reduce
record transfer, but visited nodes include all immediate-child summaries and
the client rebuilds the complete remote tree. Each v2 mutation also performs a
fresh receiver scan, so many actions can incur repeated full-file hashing.

Local apply first rescans and requires the initial snapshot root and record count
to remain unchanged, including passes with no local actions. It does not repeat
the receiver's per-action causal precondition. Remote checks and final rescans detect many
intervening changes, but the pass is not a distributed transaction or filesystem
snapshot: completed actions can remain after failure, and external writes can
race an apply. A retry reconciles the state that now exists. Conflict-name
allocation can also fail when a portable name cannot be formed or available
candidate names are exhausted; see [the reconciliation analysis](RECONCILE_ANALYSIS.md).

The current orchestrator is two-peer. The merge model is orientation-independent
for records/conflicts and includes a deterministic three-peer partition test,
but production multi-peer membership, tombstone acknowledgement/GC, device
revocation, and protocol migration remain hardening work.

Implementation references: [`deltaweave-net`](../crates/deltaweave-net/src/lib.rs)
(`prepare_sender_manifest`, `ChunkWritePipeline`, v1/v2 handlers),
[`deltaweave-store`](../crates/deltaweave-store/src/lib.rs) (`Store::materialize`,
`remove_path`, `MaterializationObservation`),
[`deltaweave-index`](../crates/deltaweave-index/src/lib.rs) (`LocalIndex`,
`observation_trusts_no_rehash`), and
[`deltaweave-sync`](../crates/deltaweave-sync/src/lib.rs) (`sync_with_session`).
