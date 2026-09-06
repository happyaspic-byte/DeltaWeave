# Task 2 report: owner-mediated share authorization

## Status and scope

Implemented and verified in `share-key-sync-update`; independent root-controller
review is the next step. No push, main integration, preserved-worktree mutation,
or live-user-data/registry reset was performed. The retained external migration
fixture was not modified; migration tests create independent temporary v2 fixtures.

The implementation is divided into ticket/proof, durable registry, runtime and
revocation, endpoint/session service, and wire modules under `net/src/share/`.
Common root admission lives in `net/src/root_admission.rs`. Existing transfer
algorithms were adapted with focused connection/authentication hooks, not replaced
from the preserved branch. The root approved the additional bounded index metadata
slot and atomic adoption APIs needed to commit trusted ceilings with records.

## Implemented behavior

- One persistent `device.key` and one iroh endpoint per `ShareService`; its private
  redb catalog prevents concurrent service opens against the same device state.
  Member sessions clone that endpoint. Only `deltaweave/share/3` is registered.
  A device can own A, join B, and retain a separate legacy endpoint for C.
- Version-3 canonical postcard invitations, domain-separated maintained iroh
  Ed25519 signatures, separate OS-random 32-byte invitation IDs and bearers,
  bounded parsing, signed owner/share/name/role/expiry/address hints, and strict
  re-encoding checks. `Debug` redacts ticket credentials. Issuance stores a digest
  of the entire canonical signed body, including its random bearer, and uses
  maintained `subtle::ConstantTimeEq`; stored role/scope/expiry must also match.
  No device or administrator private key appears in a ticket.
- Offline signature preview, online authoritative validation without enrollment,
  and actual enrollment are distinct calls. Only the authenticated QUIC peer ID
  determines the member endpoint. Existing members retain their durable role;
  presenting a different permission key does not upgrade them.
- Key revocation stops new enrollment, independently of existing memberships.
  Member revocation writes a durable denial tombstone and advances its epoch,
  closes tracked connections, waits on the share gate, and drains retained
  handlers and blocking/chunk work. The same identity cannot re-enroll. A fresh
  identity can still enroll using another active invitation; revoking/rotating
  all distributed invitations is needed to stop that path.
- Only locally owned runtimes serve snapshots. Connections establish one share
  before operation streams; subsequent Merkle/chunk/completion frames cannot
  select a different root. Members open sessions only from their persisted,
  authenticated issuing-owner relationship. RW grants no management RPCs.
- RO file pushes, tombstones, and directory mutations fail on the owner. Durable
  membership/epoch checks occur in query loops, during content sending, before
  chunk persistence, after reception, under the mutation gate before namespace
  changes, before readonly changes, and before index adoption.
- Fresh members receive an owner-generated reserved logical replica. Historical
  IDs remain reserved. Retained-key import requires a signed, version/domain-bound
  proof naming invitation, owner, share, new authenticated endpoint, old endpoint,
  and its hash-derived replica. Claims cannot take owner/resolver IDs or any
  active/tombstoned member binding. A proof may import a never-authored legacy ID
  absent from owner vectors. Competing claims serialize; retries retain the same
  assignment across restart. Old transport secrets remain local proof keys.
- Migrated owner roots use the actual verified DB-bound logical replica, including
  when `create_owned_share` receives `None`. Root/schema/replica checks remain in
  normal index opening. Neither migration nor missing-state recovery resets vectors.
- Trusted causal ceilings are initialized/refreshed from retained owner records.
  Incoming vectors are bounded to 4,096 known IDs and 256 KiB; only the member's
  own replica can exceed its ceiling. The resolver can advance by at most one,
  with checked arithmetic. Ordinary large global-counter jumps remain valid.
  Ceilings and accepted immediate-peer/epoch/path/operation/record-hash provenance
  commit atomically with index adoption. The audit retains up to 512 recent
  accepted remote mutations; it is not an indefinite audit archive. Vectors are
  causal labels, not author signatures or proofs of an honest Byzantine merge.
- Common admission uses the fixed user profile's `.deltaweave/root-admission`,
  independent of `--state` and web data directories. One file lock serializes
  canonical component-wise exact/ancestor/descendant comparison and registration.
  Each operation holds an OS lifetime lease. Managed registrations persist and
  have a deterministic `.deltaweave-<binding>.managed` sidecar in the root's parent;
  preparing entries reconcile fail-closed. Missing/corrupt active markers reject
  admission. Only demonstrably unlocked transient legacy rows are reaped.
- Admission is internal to legacy servers, legacy `SyncEngine::open*`, and direct
  `push_file`; the control worker delegates to those leases. Legacy handler tasks
  also survive Router cancellation until blocking work drains, and public server
  shutdown waits for them. Local index scans and manifests remain available.
- Owner catalog creation intent is durable before admission/runtime construction.
  A successful runtime is marked ready before publication. Interrupted creation
  can resume, but a ready share missing its index/store metadata or trusted share
  metadata fails closed instead of recreating history. `ShareService::open` does
  not automatically register catalog rows; callers explicitly load owned runtimes.

## Public Rust API frozen for Tasks 3 and 4

Unless shown otherwise, `Result<T>` below is `anyhow::Result<T>`. Exported models
have serde serialization/deserialization and safe Debug, except `ShareTicket`
which deliberately redacts its Debug. The internal registry and authorization
capability types are not public application APIs.

```rust
pub const ALPN_V3: &[u8] = b"deltaweave/share/3";
pub struct ShareId(pub [u8; 32]);
pub struct InvitationId(pub [u8; 32]);
pub enum Permission { ReadOnly, ReadWrite }

pub struct TicketPreview {
    pub share_id: ShareId,
    pub owner: EndpointId,
    pub name: String,
    pub permission: Permission,
    pub invitation_id: InvitationId,
    pub expires_at: Option<u64>,
}
impl ShareTicket {
    pub fn encode(&self) -> String;
    pub fn parse(encoded: &str) -> Result<Self, ShareError>;
    pub fn parse_at(encoded: &str, now: u64) -> Result<Self, ShareError>;
    pub fn preview(&self) -> TicketPreview;
    pub fn address(&self) -> EndpointAddr;
}
impl LegacyProof {
    pub fn create(ticket: &ShareTicket, old_key: &SecretKey,
        new_endpoint: EndpointId, replica: ReplicaId) -> Result<Self, ShareError>;
}

pub struct OwnedShareConfig {
    pub share_id: ShareId,
    pub owner: EndpointId,
    pub name: String,
    pub root: PathBuf,
    pub state_root: PathBuf,
    pub replica: ReplicaId,
    pub min_free_space_bytes: u64,
}
pub struct Membership {
    pub share_id: ShareId,
    pub owner: EndpointId,
    pub endpoint: EndpointId,
    pub permission: Permission,
    pub replica: ReplicaId,
    pub enrolled_at: u64,
    pub revoked_at: Option<u64>,
    pub epoch: u64,
}
pub struct Invitation {
    pub id: InvitationId,
    pub share_id: ShareId,
    pub permission: Permission,
    pub expires_at: Option<u64>,
    pub revoked_at: Option<u64>,
    // Private issuance digest; no stored bearer or encoded key.
}
pub struct MemberRelationship {
    pub membership: Membership,
    pub address: EndpointAddr,
}

impl ShareService {
    pub async fn open(state: impl AsRef<Path>, mode: NetworkMode,
        bind: Option<SocketAddr>) -> Result<Self>;
    pub fn endpoint_id(&self) -> EndpointId;
    pub fn endpoint_addr(&self) -> EndpointAddr;
    pub async fn wait_online(&self, timeout: Duration) -> bool;
    pub fn owned_configs(&self) -> Result<Vec<OwnedShareConfig>>;
    pub async fn create_owned_share(&self, name: String, root: PathBuf,
        state_root: PathBuf, replica: Option<ReplicaId>,
        min_free_space_bytes: u64) -> Result<OwnerShare>;
    pub async fn load_owned_share(&self, share: ShareId) -> Result<OwnerShare>;
    pub fn relationships(&self) -> Result<Vec<MemberRelationship>>;
    pub async fn validate_ticket(&self, ticket: &ShareTicket) -> Result<TicketPreview>;
    pub async fn enroll(&self, ticket: &ShareTicket,
        proof: Option<LegacyProof>) -> Result<Membership>;
    pub fn open_session(&self, owner: EndpointId, share: ShareId) -> Result<ShareSession>;
    pub fn admit_member_root(&self, owner: EndpointId, share: ShareId,
        root: impl AsRef<Path>) -> Result<root_admission::RootLease>;
    pub async fn shutdown(self) -> Result<()>;
}

impl OwnerShare { // Clone is supported; this is a local owner capability.
    pub fn config(&self) -> &OwnedShareConfig;
    pub fn issue_key(&self, permission: Permission, expires_at: Option<u64>,
        address: EndpointAddr) -> Result<ShareTicket>;
    pub fn keys(&self) -> Result<Vec<Invitation>>;
    pub fn revoke_key(&self, id: InvitationId) -> Result<()>;
    pub fn rotate_key(&self, id: InvitationId, expires_at: Option<u64>,
        address: EndpointAddr) -> Result<ShareTicket>;
    pub fn members(&self) -> Result<Vec<Membership>>;
    pub async fn revoke_member(&self, peer: EndpointId) -> Result<()>;
    pub async fn pause(&self);
    pub fn resume(&self);
    pub fn set_observer(&self, observer: Option<TransferObserver>);
    pub fn inventory(&self) -> Result<Inventory>;
    pub fn provenance(&self) -> Result<Vec<MutationProvenance>>;
}
pub struct MutationProvenance {
    pub peer: EndpointId,
    pub membership_epoch: u64,
    pub path: WirePath,
    pub operation: String,
    pub record_hash: Hash32,
    pub accepted_at: u64,
}

impl ShareSession {
    pub fn membership(&self) -> &Membership;
    pub async fn fetch_snapshot(&self, local: &MerkleTree) -> Result<RemoteSnapshot>;
    pub async fn pull_record(&self, record: SyncRecord,
        store: Arc<Store>) -> Result<PullReceipt>;
    pub async fn pull_record_to(&self, record: SyncRecord,
        store: Arc<Store>, destination_root: PathBuf) -> Result<PullReceipt>;
    pub async fn pull_record_to_with_budget(&self, record: SyncRecord,
        store: Arc<Store>, destination_root: PathBuf, min_free_space_bytes: u64,
        pending_destination_bytes: u64) -> Result<PullReceipt>;
    pub async fn push_record(&self, source: impl AsRef<Path>, record: SyncRecord,
        profile: ChunkingProfile) -> Result<SyncApplyReceipt>;
    pub async fn apply_metadata(&self, record: SyncRecord) -> Result<SyncApplyReceipt>;
    pub async fn close(self); // Does not close the device-wide endpoint.
}

pub enum ShareError {
    InvalidTicket, UnsupportedVersion, Expired, InvitationRevoked, MemberRevoked,
    PermissionDenied, UnknownShare, NotMember, ReplicaClaimRejected, InvalidRecord,
    Busy, Offline, StateUnavailable, Protocol, OwnerMismatch, TransferFailed,
}
impl ShareError {
    pub fn classify(error: &anyhow::Error) -> Self;
}
// Serde error names are snake_case; Display is static and credential/path-free.

// deltaweave_net::root_admission:
pub enum RootUse { Legacy, Managed { share: [u8; 32], owner: [u8; 32] } }
pub fn acquire(path: impl AsRef<Path>, kind: RootUse) -> Result<RootLease>;
impl RootLease { pub fn root(&self) -> &Path; }

// Focused LocalIndex extension; old APIs retain their signatures and checks.
impl LocalIndex {
    pub fn read_bound_replica(root: impl AsRef<Path>,
        database_path: impl AsRef<Path>) -> Result<Option<ReplicaId>>;
    pub fn share_metadata(&self) -> Result<Option<Vec<u8>>>;
    pub fn set_share_metadata(&self, metadata: &[u8]) -> Result<()>;
    pub fn adopt_verified_record_with_share_metadata(&self,
        record: &SyncRecord, metadata: &[u8]) -> Result<()>;
    pub fn adopt_materialized_record_with_share_metadata(&self,
        record: &SyncRecord, observation: &MaterializationObservation,
        metadata: &[u8]) -> Result<()>;
}
```

### Caller obligations and restart state

1. Retain one `ShareService` for the device. Do not extract/rebind its secret for
   each member session. Keep legacy identities separate. `open_session` uses
   stored owner/share scope, not a requested role or mutable peer redirect.
2. After selecting a member root, retain `admit_member_root`'s lease throughout the
   managed engine lifetime. Use the returned membership replica when opening a
   fresh index. For an existing participant, read its bound replica and provide
   a retained-key proof; never reset an index to fit a fresh assignment.
3. Use `pull_record_to_with_budget` with the entire pending destination byte
   budget and configured reserve. The low-level `pull_record` zero-reserve
   default exists for compatibility; Task 3 must use budget-aware staging and
   preserve its own final materialization/rescan/recovery safeguards.
4. Ticket `preview()` means offline signature metadata only. Only a successful
   `validate_ticket` is current online issuance validation, and only successful
   enrollment stores a relationship. Cached membership is not a current online
   status. Reconnect after an interrupted transfer to learn durable revocation;
   use `ShareError::classify`, never raw error chains, for web/API errors.
5. Keys are display-once. List APIs do not recover their bearer. Rotation revokes
   the old key first, then issues a replacement; a failure leaves the old key
   revoked. Task 4 should show/copy only the newly returned encoded key.
6. The catalog retains owner creation intents, active configurations, keys,
   members/tombstones, and member relationships. Open does not auto-serve them.
   Load selected owned configurations explicitly and preserve control-layer
   enabled/removed state. A paused/removed UI entry must not erase common managed
   ownership. Serialize pause/resume commands in the controlling worker.
7. The lock order is share gate -> short registry transaction -> index adoption.
   Revocation commits the registry denial and releases that transaction before
   closing connections/waiting for the gate. Pending writers and scans are owned
   by retained handlers, not abandoned join handles. Already-started CAS writes
   may leave unreachable chunks; completed revocation does not recall prior bytes.
8. The opaque 2 MiB index slot is only for trusted share metadata. It cannot
   address root/replica configuration. Remote adoption must use the atomic
   variants. Owner-runtime code serializes scans/adoption with the share gate.
   Task 3 should not independently mutate the owned runtime's index.
9. Offline pending tickets, RO preservation/checkpoints, automatic workers,
   credential migration, and UI actions are downstream Tasks 3–5. Store any
   pending bearer only in private state and redact it from ordinary snapshots.

## TDD and attack evidence

New API tests were introduced before the implementations (initial compile RED
for missing admission, ticket, registry, and bounded index APIs). Behavioral RED
and subsequent GREEN runs included:

- `cargo test --locked -p deltaweave-net --test admission`: before the hook,
  `legacy_server_rejects_managed_root_with_alternate_state` failed with
  `legacy server reopened a managed root`; afterward it denies admission while
  preserving exact protected bytes. Direct legacy publication from a managed
  source is also denied with no destination file.
- `cargo test --locked -p deltaweave-net --test shares`: before the remote write
  checks, the actual v3 RO deletion test failed because
  `session.apply_metadata(deleted).await.is_err()` was false. The final test sends
  file, tombstone, and directory attempts, checks exact owner root hash/bytes,
  and successfully pulls content with a budget-aware RO session.
- `cargo test --locked -p deltaweave-net --test shares revocation_`: three actual
  disk/network barriers run the revoke/assertion tasks on one LocalSet thread,
  so the assertion cannot race the revoker between its synchronous denial and
  its first drain await. Temporarily removing only the gate/handler drains made
  all three fail: `revocation returned with a blocked disk operation` and
  `revocation returned before tracked transfer drained`. Restoring the drains
  makes all pass. Mutation RED log:
  `/tmp/deltaweave-task2-revocation-red.log`.
- `cargo test --locked -p deltaweave-net denied_session_does_not --lib`: RED
  showed `an already-denied idle connection kept revocation waiting` when a
  denied connection arrived after the close snapshot. GREEN registers only
  accepted sessions and rechecks after registration, excluding denied idle
  connections from the drain without allowing new operations.
- `cargo test --locked -p deltaweave-net --test shares active_share_missing_index`:
  RED showed `active managed index was silently recreated`; GREEN refuses load
  and asserts the deleted disposable index is still absent. A separate real
  interrupted-store-creation test proves pending intents resume successfully.
- `cargo test --locked -p deltaweave-index share_metadata_commits --lib`: failed
  verification and oversized metadata leave both old records and old metadata
  intact; successful adoption/reopen retains both together.
- `cargo test --locked -p deltaweave-index actual_crash_never --lib`: actual child
  processes are killed immediately before and after the redb commit. Restart
  sees the old record+ceiling pair or new record+ceiling pair, never a split pair.
- `cargo test --locked -p deltaweave-net legacy_shutdown_drains --lib`: a real
  v2 blocking application is held after iroh Router cancellation; polling the
  public shutdown remains pending until disk work finishes, and managed root
  admission remains denied until then.

Additional actual QUIC tests cover raw mismatched-share enrollment/session
requests; v1/v2 ALPN refusal; valid sender counter 1,000,000; forged victim,
unknown, resolver and oversized vectors; normal resolver advancement; fresh IDs;
key/member restart; old-v2 two-replica migration and subsequent retained-replica
editing; a never-authored nonempty participant; and mixed own/join/legacy devices.
Unit proof tests change every claim-context field and signature. Concurrent old
key claims produce one durable binding; response-loss-style retries after reopen
return the same grant. Admission tests cover aliases, exact/ancestor/descendant
pairs in both start orders, separate-process races, stale leases, corrupt/lost
markers, preparing recovery, siblings, alternate state and protected push sources.

The first all-net run exposed an empty result-file read in the process test
harness: creation and data write were separate observable steps. The test now
publishes its result by atomic rename. All eight admission tests and the complete
suite pass afterward; no protection assertion was removed or weakened.

## Final verification

Evidence directory: `/tmp/deltaweave-task2-verification-_3wy9uzb`.
Tests run with a temporary child-process user profile; Cargo and rustup retain
explicit existing toolchain/cache paths. No real user's root catalog is reset,
deleted, or used for these full-suite admission writes.

```text
cargo test --locked -p deltaweave-net --all-targets --all-features
PASS: 59 tests (45 unit, 2 admission integration, 12 share integration), 0 failures
Log: net-tests-final.log

cargo test --locked -p deltaweave-index -p deltaweave-sync \
  -p deltaweave-control --all-targets --all-features
PASS: 63 tests, 0 failures
Log: scoped-tests-final.log

cargo clippy --locked -p deltaweave-net -p deltaweave-index \
  -p deltaweave-sync -p deltaweave-control --all-targets --all-features -- -D warnings
PASS
Log: clippy-final.log

cargo check --locked --target x86_64-pc-windows-gnu \
  -p deltaweave-net -p deltaweave-index -p deltaweave-sync -p deltaweave-control
PASS
Log: windows-check.log
```

The final clippy run is clean. Its earlier large-enum finding was fixed by
boxing the optional wire proof; postcard encoding remains unchanged. The Windows
check is compilation evidence, not Windows runtime/CI evidence.

## Self-review and limitations

Reviewed each authorization boundary, retained handler lifetime, causal adoption,
connection scope, registry transition, and the small legacy hooks. Self-review
found and fixed the denied-connection drain race, active-versus-preparing restart
state, bounded metadata preflight, and legacy Router-cancellation lease lifetime.

- External internet v3/NAT/relay behavior is not yet claimed. The service reuses
  the verified N0/Minimal builders and direct hints. The root's earlier actual N0
  changed-port probe was a same-host legacy transport probe; v3 external validation
  remains a later gate. DirectOnly persisted hints can become stale on port change.
- Windows runtime CI is still required at the final integration gate.
- Cross-filesystem replacement/deletion behavior was independently shown by root
  to fail safely on the immutable baseline and is assigned to Task 3; this task
  preserves those store operations and does not claim that issue is repaired.
- Root admission is current-binary coordination for one OS user. Explicit OS
  exports, removing private ownership state, changing the user profile, or using
  an old binary remain outside the remote model. Admission/state corruption
  fails closed; local administrative recovery must not silently reset history.
- The recent mutation audit is bounded to 512 entries and 2 MiB combined share
  metadata. It records authenticated immediate peers, not cryptographic authorship
  of every causal vector or an unlimited provenance history.
- The common network file is already large; edits there are limited to verified
  connection reuse, optional authorization hooks, lease/task lifetimes, and tests.

## Fix round 1 — independent review C1 / I2 / I3

Reviewed `.superpowers/sdd/2026-09-06-folder-share-keys/task-2-review.md` in full.
This round addresses all three findings and the adjacent existing-member RO/RW
invitation regression. The review base is `83697d8`; root's intervening commits
through `19847c1` contain documentation only. No index, store, control, CLI, UI,
or worker implementation was changed in this round.

### Private/public admission and unchanged denial

The common fixed admission registry now has a dedicated durable `private_roots_v1`
table. Every managed service reserves its device/catalog directory before writing
its identity. Owner creation and loading atomically admit the public root together
with its private state directory. The underlying legacy server and SyncEngine
also use public/private admission for their already-external state directories.
Every public admission, including member roots and legacy publication, checks
private reservations in both hierarchy directions. Private/private nesting is
allowed. Reservations persist after service/worker shutdown and are shared across
all service instances using the fixed user profile registry.

The registry itself is an implicit private root. Public requests equal to,
containing, or beneath it are rejected before initial registry bootstrap creates
anything. The protected directory is independent of service state or web data-dir.

Admission resolves the existing canonical ancestor and missing components without
mkdir, then repeats resolution under the global file lock. It completes all
public/public, public/private, and paired-root overlap checks before creating
requested directories or invoking the owner catalog writer. Canonical roots are
revalidated after creation and before public registration. Exact roots, ancestors,
missing descendants, aliases, alternate state paths, and separate-process races
are covered. Clean ordinary rejection opens the admission catalog read-only;
this avoids redb header/checkpoint writes on rejected requests as well as avoiding
logical catalog changes.

Existing preparing managed intents continue to recover their outside-root marker.
A redb catalog left dirty by process death is repaired only after read-only open
specifically reports `RepairAborted`, while holding the global admission lock;
other open errors fail closed. Recovery never clears either reservation table.
Recovery of already-committed intent may update the existing registry, independently
of the requested admission; it does not create the denied requested namespace.

### Public API additions and caller obligations

Existing public signatures remain unchanged. Two public APIs were added in
`deltaweave_net::root_admission`:

```rust
pub fn reserve_private(path: impl AsRef<Path>) -> anyhow::Result<PathBuf>;

pub fn acquire_with_private(
    path: impl AsRef<Path>,
    kind: RootUse,
    private: &[PathBuf],
) -> anyhow::Result<RootLease>;
```

`reserve_private` creates and returns the canonical private directory after
successful preflight, then permanently excludes it from current-binary public
admission. Task 3 must call this before writing member private state or an
external recovery vault. Private nesting is permitted; no release/reset API is
provided. It does not grant publication rights. `acquire_with_private` performs
one serialized preflight for a public root and all requested external private
directories and returns the public lifetime lease. Keep that lease through every
active disk handler, as with `acquire`. Existing `acquire` delegates with an empty
private list. Existing member-root admission therefore inherits protection of
all registered device, catalog, state, and vault namespaces.

A crate-private `admit_with_private<T>` accepts a synchronous preparation callback
so owner catalog intent can be committed after preflight and before managed
marker registration. Lock order is service lifecycle -> global admission OS lock
-> short catalog callback. The callback checks retained index binding and commits
catalog intent; it must not await, recurse into admission, acquire a runtime gate,
or run disk handlers. Runtime construction and scanning happen after admission
returns and use the returned lease without reopening it.

Successful preflight may create the requested directories. Private reservations
are committed before the callback can write sensitive state. If valid preparation
then fails, those private reservations and created directories deliberately remain;
no later caller may publish the failed preparation's private data. If the callback
fails before committing owner intent, no public managed marker is registered. If
owner intent committed and later marker/runtime preparation fails, the existing
preparing owner catalog entry and durable reservation permit explicit load/retry.
Tests cover a failed callback retaining private exclusion and the original
incomplete-runtime preparation/restart path. Namespace preflight rejection invokes
no callback and creates neither requested public nor alternate private directories.

### Bounded enrollment and existing-member permission

Retained-key enrollment now checks whether selecting its replica would add a new
ID at `MAX_REPLICAS`, before inserting either a reservation or membership. The
fresh path retains its existing limit. At capacity, an unbound valid historical
ID already in the known set succeeds; an unknown old-key proof fails atomically.
The test seeds 4096 durable known IDs, includes an already-enrolled writer, and
uses actual QUIC enrollment. It compares the exact owner catalog bytes before
and after rejection, reopens the owner, verifies the denied claim is absent,
enrolls a known historical proof, and proves both the incumbent and admitted
historical writer can adopt a valid directory record. Original file bytes remain
unchanged. Existing RO membership is also re-enrolled using a distinct active RW
invitation: both the returned and persisted grant stay RO, and raw mutation
requests still fail remotely in the existing QUIC test.

### RED/GREEN evidence

All tests use fresh temporary roots. Service/network tests re-exec in an isolated
profile; full verification used a dedicated temporary HOME/USERPROFILE while
retaining only the Rust toolchain/cache directories. No live registry, retained
migration fixture, main/preserve worktree, or user process was modified.

- C1 RED: `cargo test --locked -p deltaweave-net --test shares
  private_state_and_denied_descendants -- --nocapture` failed with
  `device namespace admitted`. Log `/tmp/dw-fix1-private-red.log`.
  GREEN log `/tmp/dw-fix1-private-green.log`, then final suite.
  The full test compares protected device/public/other-share trees and owner
  catalog bytes on denial; tries both creation orders, aliases, another service,
  and member admission; a real RO session pulls the public file but cannot pull
  forged device-key or other-share-private paths. Secret bytes are never printed.
- I2 RED: `cargo test --locked -p deltaweave-net --test admission
  legacy_server_rejects -- --nocapture` failed with
  `denied admission mutated managed root`. Log `/tmp/dw-fix1-desc-red.log`.
  GREEN final net and sync suites cover the original exact-root rejection plus
  missing descendants through direct and alias paths; complete single-file tree
  shape/bytes stay unchanged and every alternate state path remains absent.
- I3 RED: `cargo test --locked -p deltaweave-net --lib
  proof_at_replica_capacity -- --nocapture` failed with
  `unknown retained replica exceeded capacity`.
  Logs `/tmp/dw-fix1-capacity-red.log` and `/tmp/dw-fix1-capacity-green.log`;
  final suite includes the expanded incumbent-writer checks.
- Common admission tests compare exact admission-catalog bytes on clean denial,
  test private nesting and persistence, test initial catalog containment without
  bootstrap mkdir, and race private/public equal/ancestor/descendant registration
  in separate processes. Exactly one process succeeds.
- Self-review RED: a real killed redb writer made read-only preflight fail to
  recover an unrelated valid root (`dirty admission catalog prevented recovery`).
  Logs `/tmp/dw-fix1-dirty-red.log` and `/tmp/dw-fix1-dirty-green.log`.
  The fix preserves the committed private reservation through allocator recovery.
- The initial impacted suites passed. Initial clippy identified only a complex
  tuple return and collapsible conditional; an `AdmissionCatalog` struct and the
  suggested conditional form resolved both without suppressions.

### Final verification

Final evidence directory: `/tmp/deltaweave-task2-fix1-r0exr828`.
Runner: `/tmp/dw-fix1-verify.py`. Counts below exclude repeated child-process
harness output. All commands exited zero:

```text
cargo test --locked -p deltaweave-net --all-targets --all-features
PASS: 52 unit + 2 admission + 13 share integration = 67 tests
Log: net-tests.log

cargo test --locked -p deltaweave-sync -p deltaweave-control --all-targets --all-features
PASS: 13 sync + 15 control = 28 tests
Log: sync-control-tests.log

cargo clippy --locked -p deltaweave-net -p deltaweave-sync -p deltaweave-control \
  --all-targets --all-features -- -D warnings
PASS
Log: clippy.log

cargo check --locked --target x86_64-pc-windows-gnu \
  -p deltaweave-net -p deltaweave-sync -p deltaweave-control
PASS
Log: windows.log

cargo fmt --all -- --check
PASS

git diff --check
PASS
```

No remaining concern is known for C1, I2, I3, or the re-enrollment case. The existing
Task 3 cross-filesystem storage work and required Windows runtime CI remain later
gates. This round's Windows result is compilation only. Root separately recorded
an actual v3 N0 ID-only/changed-port probe in its documentation; that evidence is
not attributed to this fix round. Local administrative deletion/replacement of
private registry files remains outside the remote authorization model.
