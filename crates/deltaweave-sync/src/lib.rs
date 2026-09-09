//! Deterministic, retry-safe bidirectional folder reconciliation.

#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Context, Result, bail, ensure};
use deltaweave_core::{
    ChunkingProfile, FileManifest, Hash32, ReplicaId, SyncEntryKind, SyncRecord, WirePath,
};
use deltaweave_index::{IndexOptions, LocalIndex, ScanReport, collision_key};
use deltaweave_net::share::{
    ApplyPermit, ApplyStateView, AuthoritativeSnapshot, ClientIntentPhase, ManifestAttestation,
    ShareError, ShareGrant, ShareId, ShareSession, SnapshotToken, SwarmTransferReceipt,
};
use deltaweave_net::{
    DiskAdmission, Inventory, PullManifestReceipt, PullReceipt, SwarmSources, SyncApplyReceipt,
    SyncClient, SyncSession, TransferEvent, TransferObserver, is_swarm_local_storage_error,
    swarm_partial_fill,
};
use deltaweave_reconcile::{
    ApplyAction, ConflictRecord, MerkleTree, actions_to_reach, merge_snapshots,
};
use deltaweave_store::Store;
use iroh::{EndpointAddr, EndpointId};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod read_only;
mod shared;
mod transport;
pub use read_only::{PreservedLocalChange, ReadOnlyReport};
pub use shared::{ManagedSyncConfig, ManagedSyncEngine, ManagedSyncFailure, ManagedSyncReport};
use transport::ReconcileTransport;

/// Durable local inputs for one reconciliation engine.
#[derive(Clone, Debug)]
pub struct SyncConfig {
    /// Synchronized namespace root.
    pub root: PathBuf,
    /// Private index, chunk, journal, and recovery directory outside `root`.
    pub state_root: PathBuf,
    /// Stable logical-clock identity derived from the local endpoint identity.
    pub replica: ReplicaId,
    /// Authenticated remote peer configuration.
    pub client: SyncClient,
    /// Optional authorized V3 swarm sources used to fill missing CAS chunks.
    /// The authoritative peer is excluded so its connection budget remains
    /// available for snapshots, record operations, and fallback transfers.
    pub swarm_sources: Vec<EndpointAddr>,
    /// Content-defined chunking profile.
    pub profile: ChunkingProfile,
    /// Additional local paths excluded from indexing.
    pub ignored_paths: Vec<PathBuf>,
}

/// Reusable local half of a bidirectional reconciliation relationship.
#[derive(Debug)]
pub struct SyncEngine {
    local: Arc<ReplicaState>,
    client: SyncClient,
}

#[derive(Debug)]
#[doc(hidden)]
pub struct ReplicaState {
    _root_lease: Arc<deltaweave_net::root_admission::RootLease>,
    root: PathBuf,
    index: Arc<LocalIndex>,
    store: Arc<Store>,
    swarm_sources: Vec<EndpointAddr>,
    profile: ChunkingProfile,
    min_free_space_bytes: u64,
    peer: String,
}

impl std::ops::Deref for SyncEngine {
    type Target = ReplicaState;
    fn deref(&self) -> &Self::Target {
        &self.local
    }
}

/// Auditable outcome after both peers have been re-read and proven converged.
#[derive(Clone, Debug, Serialize)]
pub struct SyncReport {
    /// Status is emitted only after both verified roots equal the desired root.
    pub status: &'static str,
    /// Local root before the merge.
    pub local_before_root: Hash32,
    /// Remote root before the merge.
    pub remote_before_root: Hash32,
    /// Deterministic canonical root selected by reconciliation.
    pub desired_root: Hash32,
    /// Local root after an authoritative rescan.
    pub verified_local_root: Hash32,
    /// Remote root after a fresh network snapshot.
    pub verified_remote_root: Hash32,
    /// Merkle nodes queried while reconstructing the initial remote snapshot.
    pub merkle_queries: usize,
    /// Local filesystem/index actions performed.
    pub local_actions: usize,
    /// Remote causal actions performed.
    pub remote_actions: usize,
    /// Unique desired file contents staged from existing local paths.
    pub staged_local_files: usize,
    /// Unique desired file contents pulled from the remote peer.
    pub pulled_remote_files: usize,
    /// Payload bytes pulled into the local CAS.
    pub pulled_bytes: u64,
    /// Payload bytes pushed into the remote CAS.
    pub pushed_bytes: u64,
    /// Manifest extents reused across pull and push operations.
    pub reused_extents: usize,
    /// Number of distinct V3 swarm sources that delivered at least one CAS chunk.
    pub swarm_sources_used: usize,
    /// Deterministic conflict decisions, including preserved conflict-copy paths.
    pub conflicts: Vec<ConflictRecord>,
}

#[derive(Default)]
struct StageStats {
    local_files: usize,
    remote_files: usize,
    pulled_bytes: u64,
    reused_extents: usize,
    swarm_source_ids: BTreeSet<EndpointId>,
}

#[derive(Default)]
struct RemoteStats {
    pushed_bytes: u64,
    reused_extents: usize,
}

/// One bounded, grant-specific swarm assignment.  The grant is retained by
/// the parent while the fetch task runs so a panic or transport failure still
/// has an exact receipt/intent binding available for recovery.
struct ManagedSwarmAssignment {
    provider: EndpointId,
    grant: ShareGrant,
    operation_id: [u8; 16],
    hashes: Vec<Hash32>,
}

struct ManagedSwarmOutcome {
    assignment: usize,
    result: Result<SwarmTransferReceipt>,
}

/// Filesystem identity captured while a stage directory is owned by this
/// process.  If a persisted stage has no captured identity (for example, an
/// older journal format), cleanup deliberately preserves the directory rather
/// than risking deletion after a same-name replacement.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ManagedStageIdentity {
    volume: u64,
    file: u64,
}

type ManagedStageMarker = dyn Fn(&Path, Option<ManagedStageIdentity>) -> Result<()> + Send + Sync;

fn managed_stage_identity(path: &Path) -> Option<ManagedStageIdentity> {
    let metadata = fs::symlink_metadata(path).ok()?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return None;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Some(ManagedStageIdentity {
            volume: metadata.dev(),
            file: metadata.ino(),
        })
    }
    #[cfg(windows)]
    {
        // `Handle::from_path_any` opens with backup semantics and follows a
        // junction/reparse point.  Reject the reparse attribute from the
        // no-follow metadata probe before asking the OS for a stable identity;
        // otherwise a replaced stage could inherit the target's identity and
        // become eligible for cleanup.
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return None;
        }
        // Keep the same stable handle identity used by Store.  The nightly
        // std::os::windows::fs::MetadataExt file-index methods are not
        // available on the repository's Windows toolchain, and a path-only
        // check would permit same-name replacement deletion.
        let handle = winapi_util::Handle::from_path_any(path).ok()?;
        let information = winapi_util::file::information(&handle).ok()?;
        (information.file_index() != 0).then_some(ManagedStageIdentity {
            volume: information.volume_serial_number(),
            file: information.file_index(),
        })
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = metadata;
        None
    }
}

/// The owner permit is an admission window, not a cancellation deadline for
/// filesystem calls that have already started.  Keep this value aligned with
/// the D authority contract; a later check stops new work and leaves the
/// operation pending when the owner cannot accept the drain acknowledgement.
const MANAGED_APPLY_TTL: Duration = Duration::from_secs(10);
const MAX_MANAGED_OWNER_ROUNDS: usize = 2;
const MAX_MANAGED_SWARM_PROVIDERS: usize = 8;
const MAX_MANAGED_SWARM_HASHES: usize = 64;
static MANAGED_STAGE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Exact owner admission retained across a process restart.  This is a
/// local journal record; the signed permit remains the authority and the
/// operation id prevents a retry from creating a second owner row.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ManagedApplyJournal {
    pub(crate) permit: deltaweave_net::share::ApplyPermit,
    pub(crate) operation_id: [u8; 16],
    /// Intended owner-side result.  This is written before sending the drain
    /// acknowledgement so a response loss cannot turn a committed apply into
    /// a later negative replay.
    #[serde(default)]
    pub(crate) committed: bool,
}

/// RW engines use the index share-metadata slot for this versioned envelope.
/// RO engines have a different envelope in `read_only.rs`; neither path
/// silently overwrites an unknown metadata value.
const MANAGED_RW_JOURNAL_VERSION: u8 = 2;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ManagedRwJournal {
    version: u8,
    owner: [u8; 32],
    share: [u8; 32],
    #[serde(default)]
    stage_roots: Vec<PathBuf>,
    /// Identity captured before a stage is used. `None` denotes a legacy
    /// entry whose path is retained but cannot be safely deleted after restart.
    #[serde(default)]
    stage_identities: Vec<Option<ManagedStageIdentity>>,
    #[serde(default)]
    apply: Option<ManagedApplyJournal>,
}

/// The first E3 checkpoint stored one optional stage root.  Postcard is
/// positional, so decode both prior shapes explicitly before accepting the
/// identity-bound v2 envelope. A missing/invalid envelope remains
/// StateUnavailable.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct LegacyManagedRwJournal {
    version: u8,
    owner: [u8; 32],
    share: [u8; 32],
    stage_root: Option<PathBuf>,
    apply: Option<ManagedApplyJournal>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LegacyManagedRwJournalV1 {
    version: u8,
    owner: [u8; 32],
    share: [u8; 32],
    stage_roots: Vec<PathBuf>,
    apply: Option<ManagedApplyJournal>,
}

impl ManagedRwJournal {
    fn new(owner: EndpointId, share: ShareId) -> Self {
        Self {
            version: MANAGED_RW_JOURNAL_VERSION,
            owner: *owner.as_bytes(),
            share: share.0,
            stage_roots: Vec::new(),
            stage_identities: Vec::new(),
            apply: None,
        }
    }
}

fn decode_managed_rw_journal(bytes: &[u8]) -> Result<ManagedRwJournal> {
    if let Ok(journal) = decode_exact_postcard::<ManagedRwJournal>(bytes)
        && journal.version == MANAGED_RW_JOURNAL_VERSION
    {
        return Ok(journal);
    }
    if let Ok(legacy) = decode_exact_postcard::<LegacyManagedRwJournalV1>(bytes)
        && legacy.version == 1
    {
        return Ok(ManagedRwJournal {
            version: MANAGED_RW_JOURNAL_VERSION,
            owner: legacy.owner,
            share: legacy.share,
            stage_identities: vec![None; legacy.stage_roots.len()],
            stage_roots: legacy.stage_roots,
            apply: legacy.apply,
        });
    }
    if let Ok(legacy) = decode_exact_postcard::<LegacyManagedRwJournal>(bytes)
        && legacy.version == 1
    {
        let stage_roots = legacy.stage_root.into_iter().collect::<Vec<_>>();
        return Ok(ManagedRwJournal {
            version: MANAGED_RW_JOURNAL_VERSION,
            owner: legacy.owner,
            share: legacy.share,
            stage_identities: vec![None; stage_roots.len()],
            stage_roots,
            apply: legacy.apply,
        });
    }
    Err(ShareError::StateUnavailable.into())
}

/// Postcard's ordinary `from_bytes` intentionally accepts a valid value with
/// trailing bytes. Managed journal variants are positional migrations, so a
/// trailing field must never be silently interpreted as a different version.
fn decode_exact_postcard<T>(bytes: &[u8]) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let (value, remaining) = postcard::take_from_bytes(bytes)
        .map_err(|_| anyhow::Error::new(ShareError::StateUnavailable))?;
    ensure!(remaining.is_empty(), ShareError::StateUnavailable);
    Ok(value)
}

#[derive(Default)]
struct ManagedStages {
    roots: Vec<PathBuf>,
    identities: BTreeMap<PathBuf, ManagedStageIdentity>,
    manifests: BTreeMap<Hash32, FileManifest>,
    sources: BTreeMap<Hash32, PathBuf>,
    /// While a round is in flight, dropping this value cleans only its
    /// run-owned stage roots.  The explicit success cleanup disarms it before
    /// an attacker can replace a just-removed name.
    cleanup_state_root: Option<PathBuf>,
    cleanup_armed: bool,
}

/// Cleans a newly reserved stage if staging is interrupted before its caller
/// can persist the stage root in the managed journal.  The cleanup delegates
/// to the same no-follow, exact-parent checks used after a successful round;
/// an uncertain replacement is intentionally retained for recovery review.
struct ManagedStageGuard {
    root: PathBuf,
    state_root: PathBuf,
    identity: Option<ManagedStageIdentity>,
    armed: bool,
}

impl ManagedStageGuard {
    fn new(root: PathBuf, state_root: &Path) -> Self {
        let identity = managed_stage_identity(&root);
        Self {
            root,
            state_root: state_root.to_path_buf(),
            identity,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ManagedStageGuard {
    fn drop(&mut self) {
        if self.armed {
            let mut stages = ManagedStages::for_root_with_identity(
                self.root.clone(),
                self.identity,
                &self.state_root,
            );
            stages.cleanup_owned(&self.state_root);
        }
    }
}

/// Chooses a fresh child of the already-admitted private state root.  The
/// state root is covered by the engine's `RootLease`, so registering every
/// short-lived stage in the host-wide permanent private catalog would only
/// grow that catalog without adding a new exclusion boundary.  A stage gets a
/// process/sequence-qualified name and an existing name is never reused.
fn managed_stage_path(state_root: &Path, stage_name: &str) -> Result<PathBuf> {
    // Do not canonicalize first: canonicalize follows a replaced symlink or
    // junction.  The caller's lexical parent must itself be a real directory
    // before we resolve it, otherwise the stage could escape the RootLease's
    // admitted namespace during a same-name replacement race.
    let metadata = fs::symlink_metadata(state_root)?;
    ensure!(metadata.is_dir() && !metadata.file_type().is_symlink());
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
        ensure!(
            metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT == 0,
            ShareError::StateUnavailable
        );
    }
    let canonical_state_root = fs::canonicalize(state_root)?;
    // Store::state_root is derived from the canonical managed root.  Reject a
    // spelling that resolves elsewhere rather than accepting an unadmitted
    // alias.  This also keeps the exact-parent check in cleanup meaningful.
    ensure!(
        canonical_state_root == state_root,
        ShareError::StateUnavailable
    );
    let state_root = canonical_state_root;
    for _ in 0..64 {
        let sequence = MANAGED_STAGE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let candidate = state_root.join(format!(
            ".managed-stage-{stage_name}-{}-{sequence}",
            std::process::id()
        ));
        match fs::symlink_metadata(&candidate) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(candidate);
            }
            Ok(_) => continue,
            Err(error) => return Err(error.into()),
        }
    }
    bail!(ShareError::StateUnavailable)
}

/// Creates one selected stage child without a second host-wide reservation.
/// The caller durably records the selected path before invoking this helper;
/// if a crash occurs after creation, the retained path is still a conservative
/// recovery reference rather than an untracked directory.
fn create_managed_stage_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    let builder = {
        let mut builder = fs::DirBuilder::new();
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
        builder
    };
    #[cfg(not(unix))]
    let builder = fs::DirBuilder::new();
    builder.create(path)?;
    let metadata = fs::symlink_metadata(path)?;
    ensure!(metadata.is_dir() && !metadata.file_type().is_symlink());
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
        ensure!(
            metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT == 0,
            ShareError::StateUnavailable
        );
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        ensure!(
            metadata.permissions().mode() & 0o077 == 0,
            ShareError::StateUnavailable
        );
    }
    Ok(())
}

impl ManagedStages {
    fn for_root(root: PathBuf, state_root: &Path) -> Self {
        Self::for_root_with_identity(root.clone(), managed_stage_identity(&root), state_root)
    }

    fn for_root_with_identity(
        root: PathBuf,
        identity: Option<ManagedStageIdentity>,
        state_root: &Path,
    ) -> Self {
        let mut stages = Self::default();
        if let Some(identity) = identity {
            stages.identities.insert(root.clone(), identity);
        }
        stages.roots.push(root);
        stages.cleanup_state_root = Some(state_root.to_path_buf());
        stages.cleanup_armed = true;
        stages
    }

    /// Reconstructs a path from a persisted journal without claiming current
    /// filesystem ownership.  A post-restart same-name replacement is left in
    /// place until a future journal with an identity can prove it is ours.
    fn for_recovery_root_with_identity(
        root: PathBuf,
        identity: Option<ManagedStageIdentity>,
        state_root: &Path,
    ) -> Self {
        let mut stages = Self::default();
        if let Some(identity) = identity {
            stages.identities.insert(root.clone(), identity);
        }
        stages.roots.push(root);
        stages.cleanup_state_root = Some(state_root.to_path_buf());
        stages.cleanup_armed = true;
        stages
    }

    fn merge(&mut self, mut other: ManagedStages) {
        self.roots.append(&mut other.roots);
        self.identities
            .extend(std::mem::take(&mut other.identities));
        self.manifests.extend(std::mem::take(&mut other.manifests));
        self.sources.extend(std::mem::take(&mut other.sources));
        if self.cleanup_state_root.is_none() {
            self.cleanup_state_root = other.cleanup_state_root.take();
        }
        self.cleanup_armed |= other.cleanup_armed;
        other.cleanup_armed = false;
    }

    fn cleanup(&self, state_root: &Path) {
        let Ok(state_root) = fs::canonicalize(state_root) else {
            return;
        };
        for root in &self.roots {
            let Some(expected_identity) = self.identities.get(root) else {
                // A path reconstructed from an old/restarted journal has no
                // ownership proof.  Keep it for explicit recovery rather than
                // deleting a directory that may have replaced the old name.
                continue;
            };
            let Ok(metadata) = fs::symlink_metadata(root) else {
                continue;
            };
            // Never canonicalize a caller-owned path during cleanup: an
            // attacker replacing the stage directory with a symlink must not
            // redirect deletion into another private namespace.  Stages are
            // created directly below the canonical state root and carry a
            // unique prefix, so this lexical + no-follow check is sufficient.
            let Ok(canonical_root) = fs::canonicalize(root) else {
                continue;
            };
            if metadata.is_dir()
                && !metadata.file_type().is_symlink()
                // A Windows junction can report as a directory without the
                // portable symlink bit.  Refuse cleanup unless the resolved
                // directory is exactly the recorded lexical directory; a
                // harmless leaked stage is preferable to deleting a foreign
                // namespace after replacement.
                && canonical_root == *root
                && root.parent() == Some(state_root.as_path())
                && root
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with(".managed-stage-"))
                && managed_stage_identity(root).is_some_and(|actual| actual == *expected_identity)
            {
                let _ = fs::remove_dir_all(root);
            }
        }
    }

    fn cleanup_owned(&mut self, state_root: &Path) {
        if self.cleanup_armed {
            self.cleanup(state_root);
            self.cleanup_armed = false;
            self.roots.clear();
        }
    }
}

/// Removes only journaled stage paths that are now absent. Existing paths are
/// retained when cleanup could not prove ownership, including old journals
/// without a persisted identity. This makes a successful drain unable to
/// erase the only recovery reference to an uncertain replacement.
fn retain_existing_stage_roots(journal: &mut ManagedRwJournal) {
    let mut roots = Vec::with_capacity(journal.stage_roots.len());
    let mut identities = Vec::with_capacity(journal.stage_identities.len());
    for (root, identity) in journal
        .stage_roots
        .drain(..)
        .zip(journal.stage_identities.drain(..))
    {
        match fs::symlink_metadata(&root) {
            Ok(_) => {
                roots.push(root);
                identities.push(identity);
            }
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                // PermissionDenied, NotADirectory, and transient filesystem
                // failures do not prove that the run-owned directory is gone.
                // Dropping the journal reference here would make an uncertain
                // replacement uncollectable and could permit a later retry to
                // treat it as fresh local state.
                roots.push(root);
                identities.push(identity);
            }
            Err(_) => {}
        }
    }
    journal.stage_roots = roots;
    journal.stage_identities = identities;
}

/// Adds a stage root to the durable managed journal before any CAS transfer
/// or private materialization begins. Existing uncertain roots remain part of
/// the bounded recovery set; a new round may never silently replace that
/// reference with only its newest stage.
fn record_managed_stage_root(
    journal: &mut ManagedRwJournal,
    root: &Path,
    identity: Option<ManagedStageIdentity>,
) -> Result<()> {
    ensure!(
        journal.stage_roots.len() == journal.stage_identities.len(),
        ShareError::StateUnavailable
    );
    if let Some(index) = journal
        .stage_roots
        .iter()
        .position(|existing| existing == root)
    {
        if journal.stage_identities[index].is_none() {
            journal.stage_identities[index] = identity;
        }
    } else {
        ensure!(
            journal.stage_roots.len() < MAX_MANAGED_OWNER_ROUNDS.saturating_add(1),
            ShareError::StateUnavailable
        );
        journal.stage_roots.push(root.to_path_buf());
        journal.stage_identities.push(identity);
    }
    Ok(())
}

fn merge_managed_stage_roots(journal: &mut ManagedRwJournal, stages: &ManagedStages) -> Result<()> {
    ensure!(
        journal.stage_roots.len() == journal.stage_identities.len(),
        ShareError::StateUnavailable
    );
    for root in &stages.roots {
        record_managed_stage_root(journal, root, stages.identities.get(root).copied())?;
    }
    Ok(())
}

impl Drop for ManagedStages {
    fn drop(&mut self) {
        if self.cleanup_armed {
            if let Some(state_root) = self.cleanup_state_root.clone() {
                self.cleanup(&state_root);
            }
            self.cleanup_armed = false;
        }
    }
}

fn managed_operation_id(prefix: &[u8], snapshot: &SnapshotToken, round: usize) -> [u8; 16] {
    let mut bytes = prefix.to_vec();
    bytes.extend_from_slice(&snapshot.snapshot);
    bytes.extend_from_slice(snapshot.root_hash.as_bytes());
    bytes.extend_from_slice(&(round as u64).to_le_bytes());
    let digest = Hash32::digest(&bytes);
    let mut operation = [0_u8; 16];
    operation.copy_from_slice(&digest.as_bytes()[..16]);
    operation
}

fn ensure_managed_deadline(deadline: Instant) -> Result<()> {
    ensure!(
        Instant::now() < deadline,
        deltaweave_net::share::ShareError::GrantExpired
    );
    Ok(())
}

fn managed_swarm_error_can_fallback(class: ShareError) -> bool {
    matches!(
        class,
        ShareError::Offline
            | ShareError::TransferFailed
            | ShareError::Busy
            | ShareError::RosterStale
            | ShareError::GrantExpired
    )
}

/// Replays one exact owner apply journal after a process restart or a lost
/// response.  The owner-side status is the only source of truth: a Prepared
/// row is atomically cancelled, while a Started/Restarted row is closed only
/// with the same committed value after this newly opened managed engine has
/// no caller-owned writer task left.  Unknown/transport failures remain
/// errors, so callers retain the journal instead of publishing a false drain.
pub(crate) async fn recover_managed_apply(
    session: &ShareSession,
    apply: &ManagedApplyJournal,
) -> Result<()> {
    let receipt = session
        .apply_status(&apply.permit, Some(apply.operation_id))
        .await?;
    match receipt.state {
        ApplyStateView::Prepared => {
            let cancelled = session
                .cancel_apply(&apply.permit, apply.operation_id)
                .await?;
            match cancelled.state {
                ApplyStateView::Denied | ApplyStateView::Expired => Ok(()),
                ApplyStateView::Drained => {
                    ensure!(
                        cancelled.committed == apply.committed,
                        ShareError::GrantReplay
                    );
                    Ok(())
                }
                ApplyStateView::Prepared | ApplyStateView::Started | ApplyStateView::Restarted => {
                    Err(ShareError::RevocationPending.into())
                }
            }
        }
        ApplyStateView::Drained => {
            ensure!(
                receipt.committed == apply.committed,
                ShareError::GrantReplay
            );
            Ok(())
        }
        ApplyStateView::Denied | ApplyStateView::Expired => Ok(()),
        ApplyStateView::Started | ApplyStateView::Restarted => {
            // The prior task cannot still be running after a process restart:
            // this function is called under the new engine's serialized gate.
            // Reuse the exact operation and intended result; never mint a new
            // permit or turn an old row into success merely because its TTL
            // elapsed.
            session
                .apply_drained(&apply.permit, apply.operation_id, apply.committed)
                .await
        }
    }
}

fn managed_wall_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn managed_swarm_operation_id(
    snapshot: &SnapshotToken,
    record: &SyncRecord,
    provider: EndpointId,
    hashes: &[Hash32],
    batch: usize,
) -> Result<[u8; 16]> {
    let mut bytes = b"managed-swarm-fetch-v1".to_vec();
    bytes.extend_from_slice(&snapshot.snapshot);
    bytes.extend_from_slice(snapshot.root_hash.as_bytes());
    bytes.extend_from_slice(record.logical_hash().as_bytes());
    bytes.extend_from_slice(provider.as_bytes());
    bytes.extend_from_slice(&(batch as u64).to_le_bytes());
    bytes.extend_from_slice(&postcard::to_stdvec(hashes)?);
    let digest = Hash32::digest(&bytes);
    let mut operation = [0_u8; 16];
    operation.copy_from_slice(&digest.as_bytes()[..16]);
    Ok(operation)
}

#[allow(clippy::too_many_arguments)]
async fn run_managed_swarm_assignment(
    assignment: usize,
    session: ShareSession,
    store: Arc<Store>,
    grant: ShareGrant,
    snapshot: SnapshotToken,
    record: SyncRecord,
    manifest: ManifestAttestation,
    operation_id: [u8; 16],
    hashes: Vec<Hash32>,
    observer: Option<TransferObserver>,
    start_gate: Arc<tokio::sync::Barrier>,
) -> ManagedSwarmOutcome {
    if let Some(observer) = &observer {
        observer.emit(TransferEvent {
            phase: "swarm_provider_started".into(),
            path: Some(record.path.as_str().into()),
            direction: Some("pull".into()),
            bytes: 0,
            peer: Some(grant.provider.to_string()),
        });
    }
    // The barrier makes the scheduler's concurrency contract observable: all
    // accepted provider assignments enter the fetch before any one task can
    // complete.  It is bounded by the number of assignments in this batch.
    start_gate.wait().await;
    let result = session
        .fetch_swarm_chunks(
            store,
            &grant,
            &snapshot,
            &record,
            &manifest,
            &hashes,
            operation_id,
        )
        .await;
    if let (Some(observer), Ok(receipt)) = (&observer, &result) {
        observer.emit(TransferEvent {
            phase: "swarm_provider_verified".into(),
            path: Some(record.path.as_str().into()),
            direction: Some("pull".into()),
            bytes: receipt.transferred_bytes,
            peer: Some(grant.provider.to_string()),
        });
    }
    ManagedSwarmOutcome { assignment, result }
}

/// Converts a completed managed swarm attempt into typed drain evidence before
/// the owner-side receipt is allowed to terminalize the durable intent.  The
/// public Store points at `<state_root>/store`; the admission lease binds its
/// parent private root, so pass the exact lease root rather than the nested
/// CAS directory.
async fn recover_managed_swarm_assignment(
    local: &ReplicaState,
    session: &ShareSession,
    assignment: &ManagedSwarmAssignment,
) -> Result<deltaweave_net::share::ClientIntentRow> {
    let state_root = local
        .store
        .state_root()
        .parent()
        .context(ShareError::StateUnavailable)?
        .to_path_buf();
    let proof = session
        .prove_local_io_drained(
            &assignment.grant,
            assignment.operation_id,
            Arc::clone(&local._root_lease),
            &local.root,
            state_root,
        )
        .await?;
    session
        .recover_swarm_intent_with_proof(&assignment.grant, assignment.operation_id, &proof)
        .await
}

impl SyncEngine {
    /// Opens durable state after rejecting overlapping public and private roots.
    pub fn open(config: SyncConfig) -> Result<Self> {
        Self::open_with_min_free_space(config, 0)
    }

    /// Opens durable state while reserving free space across local CAS and file writes.
    pub fn open_with_min_free_space(config: SyncConfig, min_free_space_bytes: u64) -> Result<Self> {
        let root_lease = Arc::new(deltaweave_net::root_admission::acquire_with_private(
            &config.root,
            deltaweave_net::root_admission::RootUse::Legacy,
            std::slice::from_ref(&config.state_root),
        )?);
        config.profile.validate()?;
        fs::create_dir_all(&config.root).with_context(|| {
            format!(
                "failed to create synchronization root {}",
                config.root.display()
            )
        })?;
        let mut state_directories = fs::DirBuilder::new();
        state_directories.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            state_directories.mode(0o700);
        }
        state_directories
            .create(&config.state_root)
            .with_context(|| {
                format!(
                    "failed to create private state root {}",
                    config.state_root.display()
                )
            })?;
        let root = fs::canonicalize(&config.root)?;
        let state_root = fs::canonicalize(&config.state_root)?;
        ensure!(
            !root.starts_with(&state_root) && !state_root.starts_with(&root),
            "synchronization root and private state root must not overlap"
        );

        let index = Arc::new(LocalIndex::open(
            &root,
            state_root.join("index.redb"),
            config.replica,
            IndexOptions {
                ignored_paths: config.ignored_paths,
                ..IndexOptions::default()
            },
        )?);
        let store = Arc::new(Store::open_with_recovery_reserver(
            state_root.join("store"),
            |path| deltaweave_net::root_admission::reserve_private(path),
        )?);
        deltaweave_net::recover_causal_index(&store, &index, &root)?;
        Ok(Self {
            local: Arc::new(ReplicaState {
                _root_lease: root_lease,
                root,
                index,
                store,
                swarm_sources: config
                    .swarm_sources
                    .into_iter()
                    .filter(|source| source.id != config.client.remote.id)
                    .collect(),
                profile: config.profile,
                min_free_space_bytes,
                peer: config.client.remote.id.to_string(),
            }),
            client: config.client,
        })
    }

    /// Reads the retained index snapshot without scanning or opening another owner.
    pub fn inventory(&self) -> Result<Inventory> {
        Inventory::from_index(&self.index)
    }

    /// Merges, applies, and independently verifies one complete bidirectional round.
    pub async fn sync_once(&self) -> Result<SyncReport> {
        self.sync_once_observed(None).await
    }

    /// Runs a complete round while emitting phases and successful payload observations.
    pub async fn sync_once_observed(
        &self,
        observer: Option<TransferObserver>,
    ) -> Result<SyncReport> {
        let local = self.local.clone();
        let client = self.client.clone();
        tokio::spawn(async move {
            local.observe(&observer, "scanning", None, None, 0);
            let outcome = async {
                local.recover_pending()?;
                let scan = scan_index(Arc::clone(&local.index)).await?;
                ensure_scan_is_safe(&scan, "local")?;
                let local_records = read_records(Arc::clone(&local.index)).await?;
                let local_tree = MerkleTree::from_records(local_records.clone())?;
                let session = client.open_session().await?;
                let outcome = local
                    .sync_with_session(&session, local_records, local_tree, &observer)
                    .await;
                session.close().await;
                outcome
            }
            .await;
            match &outcome {
                Ok(report) => local.observe(
                    &observer,
                    "complete",
                    None,
                    None,
                    report.pulled_bytes.saturating_add(report.pushed_bytes),
                ),
                Err(_) => local.observe(&observer, "error", None, None, 0),
            }
            outcome
        })
        .await
        .context("legacy sync task failed")?
    }
}

impl ReplicaState {
    fn recover_pending(&self) -> Result<()> {
        deltaweave_net::recover_causal_index(&self.store, &self.index, &self.root)
    }

    fn load_managed_rw_journal(&self, session: &ShareSession) -> Result<ManagedRwJournal> {
        let membership = session.membership();
        let Some(bytes) = self.index.share_metadata()? else {
            return Ok(ManagedRwJournal::new(membership.owner, membership.share_id));
        };
        let journal = decode_managed_rw_journal(&bytes)?;
        ensure!(
            journal.version == MANAGED_RW_JOURNAL_VERSION
                && journal.owner == *membership.owner.as_bytes()
                && journal.share == membership.share_id.0,
            deltaweave_net::share::ShareError::StateUnavailable
        );
        ensure!(
            journal.stage_roots.len() == journal.stage_identities.len(),
            deltaweave_net::share::ShareError::StateUnavailable
        );
        ensure!(
            journal.stage_roots.len() <= MAX_MANAGED_OWNER_ROUNDS.saturating_add(1),
            deltaweave_net::share::ShareError::StateUnavailable
        );
        Ok(journal)
    }

    fn save_managed_rw_journal(&self, journal: &ManagedRwJournal) -> Result<()> {
        self.index
            .set_share_metadata(&postcard::to_stdvec(journal)?)
    }

    /// Closes an owner apply row from a previous process lifetime using its
    /// exact nonce and operation.  A successful, exact owner receipt is
    /// required before clearing the local journal; a replay/unknown response
    /// is retained because it does not prove that the blocking writers
    /// drained.
    async fn recover_managed_rw_apply(
        &self,
        session: &ShareSession,
        journal: &mut ManagedRwJournal,
    ) -> Result<()> {
        let Some(apply) = journal.apply.clone() else {
            return Ok(());
        };
        let membership = session.membership();
        ensure!(
            apply.permit.owner == membership.owner
                && apply.permit.share == membership.share_id
                && apply.permit.consumer == membership.endpoint,
            deltaweave_net::share::ShareError::StateUnavailable
        );
        recover_managed_apply(session, &apply).await?;
        journal.apply = None;
        self.save_managed_rw_journal(journal)
    }

    /// Recovers only a durable managed ApplyStart before roster liveness or
    /// fresh owner admission is checked.  Paused/revoked owners may reject a
    /// heartbeat while still accepting exact status/cancel/drain recovery.
    pub(crate) async fn recover_managed_apply_before_liveness(
        &self,
        session: &ShareSession,
    ) -> Result<()> {
        match session.membership().permission {
            deltaweave_net::share::Permission::ReadWrite => {
                let mut journal = self.load_managed_rw_journal(session)?;
                self.recover_managed_rw_apply(session, &mut journal).await
            }
            deltaweave_net::share::Permission::ReadOnly => {
                read_only::recover_managed_apply_before_liveness(self, session).await
            }
        }
    }

    async fn finish_managed_rw_apply(
        &self,
        session: &ShareSession,
        journal: &mut ManagedRwJournal,
        permit: &deltaweave_net::share::ApplyPermit,
        operation_id: [u8; 16],
        committed: bool,
    ) -> Result<()> {
        // Persist the intended outcome before the network acknowledgement.
        // GrantReplay is deliberately retained by the caller: without an
        // exact owner receipt it is not proof that the prior drain completed.
        if let Some(apply) = journal.apply.as_mut() {
            ensure!(
                apply.operation_id == operation_id && apply.permit == *permit,
                deltaweave_net::share::ShareError::GrantReplay
            );
            apply.committed = committed;
        } else {
            journal.apply = Some(ManagedApplyJournal {
                permit: permit.clone(),
                operation_id,
                committed,
            });
        }
        self.save_managed_rw_journal(journal)?;
        session
            .apply_drained(permit, operation_id, committed)
            .await?;
        journal.apply = None;
        self.save_managed_rw_journal(journal)
    }

    /// Reconciles causal path journals left between Store materialization and
    /// LocalIndex adoption.  The legacy recovery helper is intentionally not
    /// used here: a managed member must have a fresh owner snapshot and a
    /// fresh ApplyPermit before any public or index mutation is resumed.
    async fn recover_managed_causal_changes(
        &self,
        session: &ShareSession,
        snapshot: &SnapshotToken,
        owner_records: &[SyncRecord],
        journal: &mut ManagedRwJournal,
    ) -> Result<()> {
        let pending: Vec<_> = self
            .store
            .path_changes()?
            .into_iter()
            .filter(|change| {
                change.root == self.root
                    && change.causal.is_some()
                    && matches!(
                        change.state,
                        deltaweave_store::PathChangeState::Prepared
                            | deltaweave_store::PathChangeState::Preserved
                            | deltaweave_store::PathChangeState::Materialized
                            | deltaweave_store::PathChangeState::RollingBack
                    )
            })
            .collect();
        if pending.is_empty() {
            return Ok(());
        }

        // Compare the durable index precondition before opening ApplyStart.
        // A scan must not have a chance to promote the materialized target
        // as a new local edit, and an unrelated local edit must not be
        // overwritten by a later resume attempt.
        let mut plan = Vec::with_capacity(pending.len());
        for change in pending {
            let binding = change
                .causal
                .as_ref()
                .context(deltaweave_net::share::ShareError::StateUnavailable)?;
            // Store intentionally treats authorization as opaque.  A matching
            // owner record alone is therefore insufficient to prove that a
            // retained causal attempt was created for this managed share.
            // Validate the signed, immutable permit before considering the
            // record for recovery; an old epoch/root is allowed to become a
            // stale attempt and roll back under the fresh permit below.
            let authorization = Self::validate_managed_causal_authorization(session, binding)?;
            let owner_record = owner_records
                .iter()
                .find(|record| record.path == binding.record.path);
            // The owner may have advanced or tombstoned this path since the
            // interrupted attempt.  Keep the old causal object as a stale
            // attempt and roll it back under the fresh owner permit; waiting
            // forever on ManifestMismatch would otherwise block every later
            // owner round.  An already-adopted target is retained and merely
            // finalized below, since rolling it back would discard an index
            // transition that already happened before the crash.
            let stale_owner = owner_record.is_none_or(|record| record != &binding.record)
                || authorization.epoch > session.membership().epoch
                || authorization.root_hash != snapshot.root_hash;
            let indexed = self
                .index
                .get(&change.path)?
                .map(|record| record.to_sync_record());
            let target_indexed = indexed.as_ref() == Some(&binding.record);
            let precondition_indexed = indexed == binding.precondition;
            ensure!(
                target_indexed || precondition_indexed,
                deltaweave_store::PreservationError::LocalChanged
            );
            plan.push((change, target_indexed, stale_owner));
        }

        let started = Instant::now();
        let permit = session.revalidate_before_apply(snapshot).await?;
        let deadline = started + MANAGED_APPLY_TTL;
        ensure_managed_deadline(deadline)?;
        let operation = managed_operation_id(b"managed-causal-recovery", snapshot, 0);
        journal.apply = Some(ManagedApplyJournal {
            permit: permit.clone(),
            operation_id: operation,
            committed: false,
        });
        self.save_managed_rw_journal(journal)?;
        session.apply_start(&permit, operation).await?;

        let recovery = (|| -> Result<()> {
            for (mut change, target_indexed, stale_owner) in plan {
                ensure_managed_deadline(deadline)?;
                let binding = change
                    .causal
                    .as_ref()
                    .context(deltaweave_net::share::ShareError::StateUnavailable)?;
                if target_indexed {
                    ensure!(
                        change.state == deltaweave_store::PathChangeState::Materialized,
                        deltaweave_net::share::ShareError::StateUnavailable
                    );
                    // The filesystem/index transition succeeded before the
                    // final Store journal write. This is index-only recovery
                    // under the fresh permit, and never rematerializes bytes.
                    self.store.mark_path_change_indexed(&change.id)?;
                    continue;
                }
                if stale_owner {
                    // Owner deletion/version advance invalidates the old
                    // target, but the local precondition and any captured
                    // user object remain recoverable.  This path performs no
                    // index promotion and lets the next owner round plan the
                    // current target from a fresh snapshot.
                    self.store.rollback_causal_change(&mut change)?;
                    ensure!(
                        change.state == deltaweave_store::PathChangeState::RolledBack,
                        deltaweave_net::share::ShareError::StateUnavailable
                    );
                    continue;
                }
                let owner_record = owner_records
                    .iter()
                    .find(|record| record.path == binding.record.path)
                    .context(deltaweave_net::share::ShareError::StateUnavailable)?;
                if change.state == deltaweave_store::PathChangeState::RollingBack {
                    // A previous stale-owner recovery reached its write-ahead
                    // phase.  Finish that idempotent rollback before planning
                    // any new target, even if the owner currently advertises
                    // the old record again.
                    self.store.rollback_causal_change(&mut change)?;
                    ensure!(
                        change.state == deltaweave_store::PathChangeState::RolledBack,
                        deltaweave_net::share::ShareError::StateUnavailable
                    );
                    continue;
                }
                ensure!(
                    matches!(
                        change.state,
                        deltaweave_store::PathChangeState::Prepared
                            | deltaweave_store::PathChangeState::Preserved
                            | deltaweave_store::PathChangeState::Materialized
                            | deltaweave_store::PathChangeState::RollingBack
                    ),
                    deltaweave_net::share::ShareError::StateUnavailable
                );
                self.store.resume_path_change(&mut change)?;
                ensure!(
                    change.state == deltaweave_store::PathChangeState::Materialized,
                    deltaweave_net::share::ShareError::StateUnavailable
                );
                if matches!(&change.target, deltaweave_store::PathTarget::File(_)) {
                    let observation = self.store.observe_path_change(&change)?;
                    self.index
                        .adopt_materialized_record(owner_record, &observation)?;
                } else {
                    self.index.adopt_verified_record(owner_record)?;
                }
                self.store.mark_path_change_indexed(&change.id)?;
            }
            Ok(())
        })();

        match recovery {
            Ok(()) => {
                self.finish_managed_rw_apply(session, journal, &permit, operation, true)
                    .await
            }
            Err(error) => match self
                .finish_managed_rw_apply(session, journal, &permit, operation, false)
                .await
            {
                Ok(()) => Err(error),
                Err(drain_error) => Err(drain_error),
            },
        }
    }

    /// Validates the opaque Store authorization attached to a managed causal
    /// journal.  The permit is checked at its own issue time so restart recovery
    /// can authenticate an expired historical signature without treating it as a
    /// fresh admission.  Fresh epoch/root authorization is still required before
    /// any resumed public/index mutation; a historical mismatch is handled as a
    /// stale attempt and rolled back rather than adopted.
    fn validate_managed_causal_authorization(
        session: &ShareSession,
        binding: &deltaweave_store::CausalBinding,
    ) -> Result<ApplyPermit> {
        let bytes = binding
            .authorization
            .as_deref()
            .context(ShareError::StateUnavailable)?;
        let permit: ApplyPermit = postcard::from_bytes(bytes)
            .map_err(|_| anyhow::Error::new(ShareError::StateUnavailable))?;
        let membership = session.membership();
        permit
            .verify_for(
                membership.owner,
                membership.share_id,
                membership.endpoint,
                permit.issued_at,
            )
            .map_err(|_| anyhow::Error::new(ShareError::StateUnavailable))?;
        ensure!(
            permit.epoch > 0 && permit.epoch <= membership.epoch,
            ShareError::StateUnavailable
        );
        Ok(permit)
    }

    /// Runs the managed RW algorithm.  This deliberately lives beside the
    /// legacy implementation so the latter keeps its existing recovery and
    /// share/3 semantics.  Managed callers first establish an authenticated
    /// owner snapshot, then use private/CAS sources for owner proposals, and
    /// only enter the public journal after D's apply admission succeeds.
    pub(crate) async fn sync_managed_rw(
        &self,
        session: &ShareSession,
        observer: &Option<TransferObserver>,
    ) -> Result<SyncReport> {
        self.observe(observer, "comparing", None, None, 0);
        let empty = MerkleTree::from_records(Vec::new())?;
        let mut journal = self.load_managed_rw_journal(session)?;
        let previous_stages = journal.stage_roots.clone();
        let previous_identities = journal.stage_identities.clone();
        // Recover the previous exact ApplyStart before asking for a new owner
        // snapshot.  Revoked/paused owners may correctly reject new snapshot
        // admission while still allowing an exact drain/status recovery.
        self.recover_managed_rw_apply(session, &mut journal).await?;
        for (stage_root, identity) in previous_stages.into_iter().zip(previous_identities) {
            let mut previous = ManagedStages::for_recovery_root_with_identity(
                stage_root,
                identity,
                self.store.state_root(),
            );
            previous.cleanup_owned(self.store.state_root());
        }
        retain_existing_stage_roots(&mut journal);
        self.save_managed_rw_journal(&journal)?;

        let mut owner_snapshot = session.fetch_authoritative_snapshot(&empty).await?;

        // This must precede scan_index: scanning a materialized-but-unadopted
        // causal attempt can publish it as a fresh local edit and destroy the
        // original precondition needed for managed recovery.
        self.recover_managed_causal_changes(
            session,
            &owner_snapshot.token,
            &owner_snapshot.records,
            &mut journal,
        )
        .await?;

        let local_scan = scan_index(Arc::clone(&self.index)).await?;
        ensure_scan_is_safe(&local_scan, "managed local")?;
        let local_records = read_records(Arc::clone(&self.index)).await?;
        let local_tree = MerkleTree::from_records(local_records.clone())?;
        let local_counter = self.index.replica_counter()?;
        ensure!(
            owner_snapshot
                .records
                .iter()
                .all(|record| record.version.get(self.index.replica()) <= local_counter),
            deltaweave_net::share::ShareError::InvalidRecord
        );
        self.observe(observer, "peer_seen", None, None, 0);

        let mut staged = ManagedStages::default();
        let mut stage_stats = StageStats::default();
        let mut remote_stats = RemoteStats::default();
        let mut remote_action_count = 0_usize;
        let mut managed_conflicts = BTreeMap::new();
        let mut final_plan = None;

        // A bounded proposal loop handles the normal owner-root change caused
        // by an accepted member proposal.  A third change is a retryable
        // owner race; it must not fall through to public mutation under an
        // old permit.
        for _round in 0..MAX_MANAGED_OWNER_ROUNDS {
            let owner_tree = MerkleTree::from_records(owner_snapshot.records.clone())?;
            let merged = merge_snapshots(&local_tree, &owner_tree)?;
            for conflict in &merged.conflicts {
                managed_conflicts
                    .entry(conflict.path.clone())
                    .or_insert_with(|| conflict.clone());
            }
            validate_materializable_namespace(&merged.records)?;
            let local_actions = actions_to_reach(&local_tree, &merged)?;
            let remote_actions = actions_to_reach(&owner_tree, &merged)?;
            remote_action_count = remote_action_count.saturating_add(remote_actions.len());
            let required_files: Vec<_> = local_actions
                .iter()
                .chain(&remote_actions)
                .filter_map(|action| match action {
                    ApplyAction::Materialize { record } if record.kind == SyncEntryKind::File => {
                        Some(record.clone())
                    }
                    ApplyAction::Delete { .. } | ApplyAction::Materialize { .. } => None,
                })
                .collect();
            let pending_local_bytes = local_actions
                .iter()
                .filter_map(|action| match action {
                    ApplyAction::Materialize { record }
                        if !record.tombstone && record.kind == SyncEntryKind::File =>
                    {
                        Some(record.size)
                    }
                    ApplyAction::Delete { .. } | ApplyAction::Materialize { .. } => None,
                })
                .try_fold(0_u64, |total, bytes| {
                    total
                        .checked_add(bytes)
                        .context("managed pending materialization byte count overflow")
                })?;
            let (round_staged, round_stats, marked_journal) = {
                // The stage function is awaited inside an owned managed task,
                // so its marker must be owned too.  The cell is only locked
                // for the synchronous metadata write and is extracted after
                // the stage task returns.
                let journal_cell = Arc::new(std::sync::Mutex::new(journal.clone()));
                let marker_cell = Arc::clone(&journal_cell);
                let index = Arc::clone(&self.index);
                let stage_marker: Arc<ManagedStageMarker> = Arc::new(move |root, identity| {
                    let mut marked = marker_cell
                        .lock()
                        .map_err(|_| anyhow::anyhow!("managed journal lock poisoned"))?;
                    record_managed_stage_root(&mut marked, root, identity)?;
                    index.set_share_metadata(&postcard::to_stdvec(&*marked)?)
                });
                let result = self
                    .stage_managed_files(
                        session,
                        &required_files,
                        &local_records,
                        &owner_snapshot.records,
                        pending_local_bytes,
                        &owner_snapshot,
                        true,
                        observer,
                        Some(stage_marker.clone()),
                    )
                    .await?;
                drop(stage_marker);
                let marked_journal = Arc::try_unwrap(journal_cell)
                    .map_err(|_| anyhow::anyhow!("managed journal marker still active"))?
                    .into_inner()
                    .map_err(|_| anyhow::anyhow!("managed journal lock poisoned"))?;
                (result.0, result.1, marked_journal)
            };
            journal = marked_journal;
            staged.merge(round_staged);
            stage_stats.local_files = stage_stats
                .local_files
                .saturating_add(round_stats.local_files);
            stage_stats.remote_files = stage_stats
                .remote_files
                .saturating_add(round_stats.remote_files);
            stage_stats.pulled_bytes = stage_stats
                .pulled_bytes
                .checked_add(round_stats.pulled_bytes)
                .context("managed pulled-byte counter overflow")?;
            stage_stats.reused_extents = stage_stats
                .reused_extents
                .saturating_add(round_stats.reused_extents);
            stage_stats
                .swarm_source_ids
                .extend(round_stats.swarm_source_ids);

            if remote_actions.is_empty() {
                final_plan = Some((owner_tree, merged, local_actions));
                break;
            }
            let proposal = self
                .apply_remote_managed(
                    session,
                    &remote_actions,
                    &staged,
                    observer,
                    Instant::now() + MANAGED_APPLY_TTL,
                )
                .await;
            let proposal = match proposal {
                Ok(proposal) => proposal,
                Err(error) => return Err(error),
            };
            remote_stats.pushed_bytes = remote_stats
                .pushed_bytes
                .checked_add(proposal.pushed_bytes)
                .context("managed pushed-byte counter overflow")?;
            remote_stats.reused_extents = remote_stats
                .reused_extents
                .saturating_add(proposal.reused_extents);
            owner_snapshot = session.fetch_authoritative_snapshot(&empty).await?;
        }

        let (owner_tree, merged, local_actions) =
            final_plan.context(deltaweave_net::share::ShareError::Busy)?;
        let desired_tree = merged.tree()?;

        self.observe(observer, "applying", None, None, 0);
        let mut local_operation = None;
        if !local_actions.is_empty() {
            // Start the monotonic admission window before any network round;
            // a delayed Revalidate reply may consume the whole ten-second
            // budget and must never renew it by starting a new local timer.
            let admission_started = Instant::now();
            let permit = session
                .revalidate_before_apply(&owner_snapshot.token)
                .await?;
            let deadline = admission_started + MANAGED_APPLY_TTL;
            ensure_managed_deadline(deadline)?;
            let operation = managed_operation_id(b"managed-local-apply", &owner_snapshot.token, 0);
            // Keep any earlier stage whose ownership could not be proven
            // after restart. Replacing the journal with only this round's
            // roots would turn an uncertain path into an untracked private
            // namespace on the next successful cycle.
            merge_managed_stage_roots(&mut journal, &staged)?;
            journal.apply = Some(ManagedApplyJournal {
                permit: permit.clone(),
                operation_id: operation,
                committed: false,
            });
            // The signed permit, operation id, and exact staged-attempt root
            // are durable before the owner sees ApplyStart.  A restart can
            // therefore close this precise owner row without minting a new
            // operation under an old local state.
            self.save_managed_rw_journal(&journal)?;
            session.apply_start(&permit, operation).await?;
            if let Err(error) = self
                .apply_local_with_deadline(
                    &local_tree,
                    &local_actions,
                    &staged.manifests,
                    Some(deadline),
                    Some(&permit),
                )
                .await
            {
                return match self
                    .finish_managed_rw_apply(session, &mut journal, &permit, operation, false)
                    .await
                {
                    Ok(()) => Err(error),
                    Err(drain_error) => Err(drain_error),
                };
            }
            local_operation = Some((permit, operation));
        }

        self.observe(observer, "verifying", None, None, 0);
        let verification_scan = scan_index(Arc::clone(&self.index)).await?;
        ensure_scan_is_safe(&verification_scan, "managed verified local")?;
        let verified_local =
            MerkleTree::from_records(read_records(Arc::clone(&self.index)).await?)?;
        ensure!(
            verified_local.root_hash() == desired_tree.root_hash()
                && verified_local.len() == desired_tree.len(),
            deltaweave_net::share::ShareError::ManifestMismatch
        );
        let fresh_owner = session.fetch_authoritative_snapshot(&empty).await?;
        if fresh_owner.token.root_hash != desired_tree.root_hash()
            || fresh_owner.records.len() != desired_tree.len()
        {
            if let Some((permit, operation)) = &local_operation {
                let _ = self
                    .finish_managed_rw_apply(session, &mut journal, permit, *operation, false)
                    .await;
            }
            bail!(deltaweave_net::share::ShareError::ManifestMismatch);
        }
        if let Some((permit, operation)) = local_operation {
            self.finish_managed_rw_apply(session, &mut journal, &permit, operation, true)
                .await?;
        }
        staged.cleanup_owned(self.store.state_root());
        retain_existing_stage_roots(&mut journal);
        self.save_managed_rw_journal(&journal)?;
        Ok(SyncReport {
            status: "pass",
            local_before_root: local_tree.root_hash(),
            remote_before_root: owner_tree.root_hash(),
            desired_root: desired_tree.root_hash(),
            verified_local_root: verified_local.root_hash(),
            verified_remote_root: fresh_owner.token.root_hash,
            merkle_queries: 0,
            local_actions: local_actions.len(),
            remote_actions: remote_action_count,
            staged_local_files: stage_stats.local_files,
            pulled_remote_files: stage_stats.remote_files,
            pulled_bytes: stage_stats.pulled_bytes,
            pushed_bytes: remote_stats.pushed_bytes,
            reused_extents: stage_stats
                .reused_extents
                .saturating_add(remote_stats.reused_extents),
            swarm_sources_used: stage_stats.swarm_source_ids.len(),
            conflicts: managed_conflicts.into_values().collect(),
        })
    }

    fn causal_binding(
        &self,
        record: &SyncRecord,
        permit: Option<&deltaweave_net::share::ApplyPermit>,
    ) -> Result<deltaweave_store::CausalBinding> {
        Ok(deltaweave_store::CausalBinding {
            record: record.clone(),
            precondition: self.index.get(&record.path)?.map(|r| r.to_sync_record()),
            authorization: permit.map(postcard::to_stdvec).transpose()?,
        })
    }

    /// Selects at most eight fresh roster providers, always trying the
    /// authenticated owner first.  Roster addresses are transport hints;
    /// every resulting grant is still issued and checked by the owner for the
    /// exact consumer, provider epoch, snapshot, manifest, and hash subset.
    async fn managed_swarm_providers(&self, session: &ShareSession) -> Vec<EndpointId> {
        let membership = session.membership();
        let mut providers = vec![membership.owner];
        let Ok(roster) = session.ensure_roster_heartbeat().await else {
            return providers;
        };
        for entry in roster.fresh_members(managed_wall_now()) {
            if entry.member == membership.endpoint || providers.contains(&entry.member) {
                continue;
            }
            providers.push(entry.member);
            if providers.len() == MAX_MANAGED_SWARM_PROVIDERS {
                break;
            }
        }
        providers
    }
    fn observe(
        &self,
        observer: &Option<TransferObserver>,
        phase: &str,
        path: Option<&WirePath>,
        direction: Option<&str>,
        bytes: u64,
    ) {
        if let Some(observer) = observer {
            observer.emit(TransferEvent {
                phase: phase.into(),
                path: path.map(|path| path.as_str().into()),
                direction: direction.map(str::to_owned),
                bytes,
                peer: Some(self.peer.clone()),
            });
        }
    }

    async fn sync_with_session(
        &self,
        session: &impl ReconcileTransport,
        local_records: Vec<SyncRecord>,
        local_tree: MerkleTree,
        observer: &Option<TransferObserver>,
    ) -> Result<SyncReport> {
        self.observe(observer, "comparing", None, None, 0);
        let remote = session.fetch_snapshot(&local_tree).await?;
        let local_counter = self.index.replica_counter()?;
        ensure!(
            remote
                .records
                .iter()
                .all(|record| record.version.get(self.index.replica()) <= local_counter),
            "remote snapshot advances local replica counter"
        );
        self.observe(observer, "peer_seen", None, None, 0);
        let remote_tree = MerkleTree::from_records(remote.records.clone())?;
        let merged = merge_snapshots(&local_tree, &remote_tree)?;
        validate_materializable_namespace(&merged.records)?;
        let desired_tree = merged.tree()?;
        let local_actions = actions_to_reach(&local_tree, &merged)?;
        let remote_actions = actions_to_reach(&remote_tree, &merged)?;
        let pending_local_bytes = local_actions
            .iter()
            .filter_map(|action| match action {
                ApplyAction::Materialize { record }
                    if !record.tombstone && record.kind == SyncEntryKind::File =>
                {
                    Some(record.size)
                }
                ApplyAction::Delete { .. } | ApplyAction::Materialize { .. } => None,
            })
            .try_fold(0_u64, |total, bytes| {
                total
                    .checked_add(bytes)
                    .context("pending local materialization byte count overflow")
            })?;

        let required_files: Vec<_> = local_actions
            .iter()
            .chain(&remote_actions)
            .filter_map(|action| match action {
                ApplyAction::Materialize { record } if record.kind == SyncEntryKind::File => {
                    Some(record.clone())
                }
                ApplyAction::Delete { .. } | ApplyAction::Materialize { .. } => None,
            })
            .collect();
        let (manifests, stage_stats) = self
            .stage_desired_files(
                session,
                &required_files,
                &local_records,
                &remote.records,
                pending_local_bytes,
                observer,
            )
            .await?;
        self.observe(observer, "applying", None, None, 0);
        self.apply_local(&local_tree, &local_actions, &manifests)
            .await?;
        self.observe(observer, "pushing", None, Some("push"), 0);
        let remote_stats = self
            .apply_remote(session, &remote_actions, observer)
            .await?;
        self.observe(observer, "verifying", None, None, 0);

        let verification_scan = scan_index(Arc::clone(&self.index)).await?;
        ensure_scan_is_safe(&verification_scan, "verified local")?;
        let verified_local =
            MerkleTree::from_records(read_records(Arc::clone(&self.index)).await?)?;
        if verified_local.root_hash() != desired_tree.root_hash()
            || verified_local.len() != desired_tree.len()
        {
            let different = verified_local.different_paths(&desired_tree);
            bail!(
                "local state changed before convergence verification: actual {}, desired {}, paths {:?}",
                verified_local.root_hash(),
                desired_tree.root_hash(),
                different
            );
        }
        let verified_remote = session.fetch_snapshot(&verified_local).await?;
        ensure!(
            verified_remote.root_hash == desired_tree.root_hash()
                && verified_remote.record_count == desired_tree.len(),
            "remote state did not converge to the deterministic desired root"
        );

        Ok(SyncReport {
            status: "pass",
            local_before_root: local_tree.root_hash(),
            remote_before_root: remote.root_hash,
            desired_root: desired_tree.root_hash(),
            verified_local_root: verified_local.root_hash(),
            verified_remote_root: verified_remote.root_hash,
            merkle_queries: remote.queried_nodes,
            local_actions: local_actions.len(),
            remote_actions: remote_actions.len(),
            staged_local_files: stage_stats.local_files,
            pulled_remote_files: stage_stats.remote_files,
            pulled_bytes: stage_stats.pulled_bytes,
            pushed_bytes: remote_stats.pushed_bytes,
            reused_extents: stage_stats
                .reused_extents
                .saturating_add(remote_stats.reused_extents),
            swarm_sources_used: stage_stats.swarm_source_ids.len(),
            conflicts: merged.conflicts,
        })
    }

    async fn stage_desired_files(
        &self,
        session: &impl ReconcileTransport,
        desired: &[SyncRecord],
        local: &[SyncRecord],
        remote: &[SyncRecord],
        pending_local_bytes: u64,
        observer: &Option<TransferObserver>,
    ) -> Result<(BTreeMap<Hash32, FileManifest>, StageStats)> {
        let local_sources = live_file_sources(local);
        let remote_sources = live_file_sources(remote);
        let mut required = BTreeSet::new();
        for record in desired
            .iter()
            .filter(|record| !record.tombstone && record.kind == SyncEntryKind::File)
        {
            required.insert(
                record
                    .content_hash
                    .context("validated live file unexpectedly lacks a hash")?,
            );
        }

        let mut manifests = BTreeMap::new();
        let mut stats = StageStats::default();
        let mut swarm = None;
        let mut swarm_attempted = false;
        let swarm_session = if self.swarm_sources.is_empty() {
            None
        } else {
            session.legacy_session()
        };
        for hash in required {
            if let Some(source) = local_sources.get(&hash) {
                let source_path = local_path(&self.root, &source.path);
                let admission = DiskAdmission::new(
                    self.store.state_root().to_path_buf(),
                    self.root.clone(),
                    self.min_free_space_bytes,
                    pending_local_bytes,
                );
                let store = Arc::clone(&self.store);
                let profile = self.profile;
                let manifest = tokio::task::spawn_blocking(move || {
                    store.ingest_file_with_admission(source_path, profile, |bytes| {
                        admission.check_state(bytes)
                    })
                })
                .await
                .context("local file ingestion task failed")??;
                ensure!(
                    manifest.file_hash == hash,
                    "local source changed after its snapshot"
                );
                manifests.insert(hash, manifest);
                stats.local_files += 1;
                continue;
            }
            let source = remote_sources
                .get(&hash)
                .with_context(|| format!("no peer retains required content {hash}"))?;
            self.observe(observer, "pulling", Some(&source.path), Some("pull"), 0);
            let (receipt, swarm_source_ids) = if let Some(legacy) = swarm_session {
                let manifest_receipt = legacy.pull_manifest((*source).clone()).await?;
                let missing =
                    missing_chunks(Arc::clone(&self.store), manifest_receipt.manifest.clone())
                        .await?;
                if !missing.is_empty() && swarm.is_none() && !swarm_attempted {
                    swarm_attempted = true;
                    swarm = legacy
                        .connect_swarm_sources(self.swarm_sources.clone())
                        .await
                        .ok();
                }
                self.stage_remote_file(
                    legacy,
                    swarm.as_ref(),
                    (*source).clone(),
                    manifest_receipt,
                    missing,
                    pending_local_bytes,
                )
                .await?
            } else {
                (
                    session
                        .pull_record_to_with_budget(
                            (*source).clone(),
                            Arc::clone(&self.store),
                            self.root.clone(),
                            self.min_free_space_bytes,
                            pending_local_bytes,
                        )
                        .await?,
                    Vec::new(),
                )
            };
            let PullReceipt {
                manifest,
                transferred_bytes,
                reused_extents,
                ..
            } = receipt;
            ensure!(
                manifest.file_hash == hash,
                "remote source returned different content"
            );
            self.observe(
                observer,
                "file_received",
                Some(&source.path),
                Some("pull"),
                transferred_bytes,
            );
            manifests.insert(hash, manifest);
            stats.remote_files += 1;
            stats.pulled_bytes = stats
                .pulled_bytes
                .checked_add(transferred_bytes)
                .context("pulled-byte counter overflow")?;
            stats.reused_extents = stats.reused_extents.saturating_add(reused_extents);
            stats.swarm_source_ids.extend(swarm_source_ids);
        }
        Ok((manifests, stats))
    }

    /// Stages managed content in a reserved private namespace.  Grant-gated
    /// share-swarm/1 fills verified CAS chunks from the owner/fresh roster
    /// providers first.  The existing share/3 pull is only an authenticated
    /// CAS fallback: it never receives a public destination and its returned
    /// manifest is compared with the owner attestation before private
    /// materialization.
    #[allow(clippy::too_many_arguments)]
    async fn stage_managed_files(
        &self,
        session: &ShareSession,
        desired: &[SyncRecord],
        local: &[SyncRecord],
        remote: &[SyncRecord],
        pending_local_bytes: u64,
        snapshot: &AuthoritativeSnapshot,
        prefer_local: bool,
        observer: &Option<TransferObserver>,
        stage_marker: Option<Arc<ManagedStageMarker>>,
    ) -> Result<(ManagedStages, StageStats)> {
        let local_sources = live_file_sources(local);
        let remote_sources = live_file_sources(remote);
        let mut required = BTreeSet::new();
        let mut required_sizes = BTreeMap::new();
        for record in desired
            .iter()
            .filter(|record| !record.tombstone && record.kind == SyncEntryKind::File)
        {
            let hash = record
                .content_hash
                .context("managed live file unexpectedly lacks a hash")?;
            if let Some(previous) = required_sizes.insert(hash, record.size) {
                ensure!(previous == record.size, ShareError::ManifestMismatch);
            }
            required.insert(hash);
        }
        if required.is_empty() {
            return Ok((ManagedStages::default(), StageStats::default()));
        }

        let stage_name = Hash32::from_bytes(snapshot.token.snapshot).to_hex();
        let requested_root = managed_stage_path(self.store.state_root(), &stage_name)?;
        if let Some(marker) = stage_marker.as_ref() {
            // Record the selected path before mkdir.  A crash in the narrow
            // create window leaves a harmless absent reference; a crash after
            // mkdir still leaves a durable path for exact recovery.
            marker(&requested_root, None)?;
        }
        create_managed_stage_directory(&requested_root)?;
        let stage_root = requested_root;
        let mut stage_guard = ManagedStageGuard::new(stage_root.clone(), self.store.state_root());
        if let Some(marker) = stage_marker.as_ref() {
            // Upgrade the pre-creation marker with the identity captured from
            // the no-follow directory now that creation has succeeded.
            marker(&stage_root, managed_stage_identity(&stage_root))?;
        }
        let stage_budget_bytes = required_sizes.values().try_fold(0_u64, |total, size| {
            total
                .checked_add(*size)
                .context("managed private-stage byte count overflow")
        })?;
        let mut stages = ManagedStages::for_root(stage_root.clone(), self.store.state_root());
        let mut stats = StageStats::default();
        // Reserve the public destination budget independently from the state
        // volume used by the CAS and private stage.  The actual public apply
        // repeats this check immediately before each filesystem mutation.
        DiskAdmission::new(
            self.store.state_root().to_path_buf(),
            self.root.clone(),
            self.min_free_space_bytes,
            pending_local_bytes,
        )
        .check_materialization(pending_local_bytes)?;

        for hash in required {
            let (manifest, transferred_bytes, reused_extents, source_path, swarm_source_ids) =
                if prefer_local && let Some(source) = local_sources.get(&hash) {
                    let source_path = local_path(&self.root, &source.path);
                    let admission = DiskAdmission::new(
                        self.store.state_root().to_path_buf(),
                        self.store.state_root().to_path_buf(),
                        self.min_free_space_bytes,
                        stage_budget_bytes,
                    );
                    let store = Arc::clone(&self.store);
                    let profile = self.profile;
                    let manifest = tokio::task::spawn_blocking(move || {
                        store.ingest_file_with_admission(source_path, profile, |bytes| {
                            admission.check_state(bytes)
                        })
                    })
                    .await
                    .context("managed local ingestion task failed")??;
                    ensure!(
                        manifest.file_hash == hash,
                        deltaweave_store::PreservationError::LocalChanged
                    );
                    (manifest, 0, 0, Some(source.path.clone()), BTreeSet::new())
                } else {
                    let source = remote_sources.get(&hash).with_context(|| {
                        format!("owner has no source for managed content {hash}")
                    })?;
                    self.observe(observer, "pulling", Some(&source.path), Some("pull"), 0);
                    let attestation = session.request_manifest(&snapshot.token, source).await?;
                    ensure!(
                        attestation.manifest.file_hash == hash
                            && attestation.manifest.size == source.size,
                        ShareError::ManifestMismatch
                    );
                    self.observe(
                        observer,
                        "swarm_manifest_ok",
                        Some(&source.path),
                        Some("pull"),
                        0,
                    );
                    let mut missing =
                        missing_chunks(Arc::clone(&self.store), attestation.manifest.clone())
                            .await?;
                    // The manifest preserves file order, while the owner
                    // grant binds a strictly sorted subset.  Keep the local
                    // CAS inventory deterministic before issuing any grant.
                    missing.sort();
                    let initial_missing_count = missing.len();
                    let providers = self.managed_swarm_providers(session).await;
                    let mut transferred_bytes = 0_u64;
                    let mut swarm_source_ids = BTreeSet::new();
                    let mut batch = 0_usize;
                    while !missing.is_empty() {
                        let missing_before = missing.len();
                        let batch_hashes: Vec<Hash32> = missing
                            .iter()
                            .take(MAX_MANAGED_SWARM_HASHES)
                            .copied()
                            .collect();
                        let batch_bytes = attestation
                            .manifest
                            .chunks
                            .iter()
                            .filter(|descriptor| batch_hashes.contains(&descriptor.hash))
                            .try_fold(0_u64, |total, descriptor| {
                                total
                                    .checked_add(u64::from(descriptor.length))
                                    .context("managed swarm byte count overflow")
                            })?;
                        // The swarm API writes only CAS. Keep private stage
                        // bytes in the state-volume budget and public apply
                        // bytes in the destination-volume budget separately.
                        DiskAdmission::new(
                            self.store.state_root().to_path_buf(),
                            self.store.state_root().to_path_buf(),
                            self.min_free_space_bytes,
                            stage_budget_bytes,
                        )
                        .check_state(batch_bytes)?;
                        // Request one distinct grant per provider subset first,
                        // then run all accepted assignments concurrently.  A
                        // provider roster is only a candidate list: an entry
                        // whose process has no supplier registration is skipped
                        // before any payload task is started.
                        let provider_count = providers.len().min(batch_hashes.len());
                        let mut assignments = Vec::new();
                        if provider_count > 0 {
                            for (provider_index, provider) in
                                providers.iter().take(provider_count).enumerate()
                            {
                                let assigned: Vec<Hash32> = batch_hashes
                                    .iter()
                                    .enumerate()
                                    .filter_map(|(index, hash)| {
                                        (index % provider_count == provider_index).then_some(*hash)
                                    })
                                    .collect();
                                if assigned.is_empty() {
                                    continue;
                                }
                                let grant = match session
                                    .request_swarm_grant(
                                        *provider,
                                        &snapshot.token,
                                        &attestation,
                                        &assigned,
                                    )
                                    .await
                                {
                                    Ok(grant) => grant,
                                    Err(error)
                                        if ShareError::classify(&error)
                                            == ShareError::NotMember =>
                                    {
                                        continue;
                                    }
                                    Err(error) => {
                                        self.observe(
                                            observer,
                                            "swarm_grant_error",
                                            Some(&source.path),
                                            Some("pull"),
                                            0,
                                        );
                                        return Err(error);
                                    }
                                };
                                self.observe(
                                    observer,
                                    "swarm_grant_ok",
                                    Some(&source.path),
                                    Some("pull"),
                                    0,
                                );
                                let operation = managed_swarm_operation_id(
                                    &snapshot.token,
                                    source,
                                    *provider,
                                    &assigned,
                                    batch
                                        .saturating_mul(MAX_MANAGED_SWARM_PROVIDERS)
                                        .saturating_add(provider_index),
                                )?;
                                assignments.push(ManagedSwarmAssignment {
                                    provider: *provider,
                                    grant,
                                    operation_id: operation,
                                    hashes: assigned,
                                });
                            }
                        }

                        if assignments.is_empty() {
                            // No authenticated supplier accepted this subset;
                            // leave the missing CAS set for the authenticated
                            // share/3 fallback below.  In particular, do not
                            // spin forever when the roster is empty or every
                            // candidate is a metadata-only member.
                            break;
                        }

                        let mut fetches = tokio::task::JoinSet::new();
                        let start_gate = Arc::new(tokio::sync::Barrier::new(assignments.len()));
                        for (assignment, item) in assignments.iter().enumerate() {
                            fetches.spawn(run_managed_swarm_assignment(
                                assignment,
                                session.clone(),
                                Arc::clone(&self.store),
                                item.grant.clone(),
                                snapshot.token.clone(),
                                (*source).clone(),
                                attestation.clone(),
                                item.operation_id,
                                item.hashes.clone(),
                                observer.clone(),
                                Arc::clone(&start_gate),
                            ));
                        }
                        let mut outcomes: Vec<Option<Result<SwarmTransferReceipt>>> =
                            (0..assignments.len()).map(|_| None).collect();
                        let mut join_failed = false;
                        while let Some(joined) = fetches.join_next().await {
                            match joined {
                                Ok(outcome) => {
                                    if outcome.assignment < outcomes.len() {
                                        outcomes[outcome.assignment] = Some(outcome.result);
                                    } else {
                                        join_failed = true;
                                    }
                                }
                                Err(_) => join_failed = true,
                            }
                        }
                        if join_failed {
                            for outcome in &mut outcomes {
                                if outcome.is_none() {
                                    *outcome = Some(Err(anyhow::anyhow!(
                                        "managed swarm assignment task failed"
                                    )));
                                }
                            }
                        }

                        let mut pending_drain = false;
                        let mut first_error = None;
                        for (index, outcome) in outcomes.into_iter().enumerate() {
                            let result = outcome.context("managed swarm assignment missing")?;
                            match result {
                                Ok(receipt) => {
                                    // A successful byte receipt can still be
                                    // pending the owner's bilateral drain
                                    // confirmation.  Keep the exact intent
                                    // recoverable and stop this batch: trying
                                    // another provider or the share/3 fallback
                                    // would create a second live activation.
                                    pending_drain |= receipt.drain_pending;
                                    transferred_bytes = transferred_bytes
                                        .checked_add(receipt.transferred_bytes)
                                        .context("managed swarm byte count overflow")?;
                                    if receipt.transferred_chunks > 0 {
                                        swarm_source_ids.insert(assignments[index].provider);
                                    }
                                }
                                Err(error) => {
                                    // A roster entry is membership metadata,
                                    // not proof that this process has
                                    // registered its Store as a supplier.
                                    // NotMember is the only safe pre-activation
                                    // skip.  Every other failure must recover
                                    // its exact intent before this round can
                                    // return; no second provider or share/3
                                    // fallback may hide an active blocker.
                                    let class = ShareError::classify(&error);
                                    if class == ShareError::NotMember {
                                        let cancelled = session
                                            .cancel_activation(
                                                &assignments[index].grant,
                                                None,
                                                assignments[index].operation_id,
                                            )
                                            .await?;
                                        ensure!(
                                            matches!(
                                                cancelled.state,
                                                deltaweave_net::share::ActivationStateView::Denied
                                                    | deltaweave_net::share::ActivationStateView::Expired
                                            ),
                                            ShareError::RevocationPending
                                        );
                                        let recovered = recover_managed_swarm_assignment(
                                            self,
                                            session,
                                            &assignments[index],
                                        )
                                        .await?;
                                        ensure!(
                                            matches!(
                                                recovered.phase,
                                                ClientIntentPhase::Cancelled
                                                    | ClientIntentPhase::Drained
                                            ),
                                            ShareError::RevocationPending
                                        );
                                    } else {
                                        // The exact grant remains durable even
                                        // when the fetch task failed or
                                        // panicked.  Recovery may itself stay
                                        // pending while the owner is offline;
                                        // either result is safer than starting
                                        // another grant under a stale round.
                                        let recovered = recover_managed_swarm_assignment(
                                            self,
                                            session,
                                            &assignments[index],
                                        )
                                        .await?;
                                        ensure!(
                                            matches!(
                                                recovered.phase,
                                                ClientIntentPhase::Cancelled
                                                    | ClientIntentPhase::Drained
                                            ),
                                            ShareError::RevocationPending
                                        );
                                        if !managed_swarm_error_can_fallback(class) {
                                            first_error.get_or_insert(error);
                                        }
                                    }
                                }
                            }
                        }
                        if pending_drain {
                            return Err(ShareError::RevocationPending.into());
                        }
                        if let Some(error) = first_error {
                            return Err(error);
                        }
                        missing =
                            missing_chunks(Arc::clone(&self.store), attestation.manifest.clone())
                                .await?;
                        missing.sort();
                        if missing.is_empty() {
                            break;
                        }
                        if missing.len() >= missing_before {
                            // All assignments in this round were rejected or
                            // returned no new CAS bytes.  Retry through the
                            // authenticated legacy CAS path instead of
                            // repeatedly issuing the same grant set.
                            break;
                        }
                        batch = batch.saturating_add(1);
                    }
                    let (receipt, fallback_transferred_bytes) = if missing.is_empty() {
                        (
                            PullReceipt {
                                record: (*source).clone(),
                                manifest: attestation.manifest.clone(),
                                transferred_bytes,
                                reused_extents: attestation
                                    .manifest
                                    .chunks
                                    .len()
                                    .saturating_sub(initial_missing_count),
                            },
                            0,
                        )
                    } else {
                        // share/3 remains an authenticated CAS-only fallback;
                        // it never receives the public destination path. Its
                        // returned record/manifest must match the owner
                        // attestation before private materialization.
                        let mut fallback_receipt = session
                            .pull_record_to_with_budget(
                                (*source).clone(),
                                Arc::clone(&self.store),
                                self.root.clone(),
                                self.min_free_space_bytes,
                                pending_local_bytes,
                            )
                            .await?;
                        let fallback_transferred_bytes = fallback_receipt.transferred_bytes;
                        fallback_receipt.transferred_bytes = fallback_receipt
                            .transferred_bytes
                            .checked_add(transferred_bytes)
                            .context("managed pulled-byte counter overflow")?;
                        (fallback_receipt, fallback_transferred_bytes)
                    };
                    ensure!(receipt.record == **source, ShareError::ManifestMismatch);
                    ensure!(
                        receipt.manifest == attestation.manifest,
                        ShareError::ManifestMismatch
                    );
                    if fallback_transferred_bytes > 0 {
                        // Keep this separate from the aggregate event: the
                        // management layer counts typed swarm payloads and
                        // adds this event only for bytes actually supplied by
                        // the authenticated share/3 fallback.  CAS reuse and
                        // swarm-only completion therefore emit no duplicate.
                        self.observe(
                            observer,
                            "file_received_fallback",
                            Some(&source.path),
                            Some("pull"),
                            fallback_transferred_bytes,
                        );
                    }
                    self.observe(
                        observer,
                        "file_received",
                        Some(&source.path),
                        Some("pull"),
                        receipt.transferred_bytes,
                    );
                    (
                        receipt.manifest,
                        receipt.transferred_bytes,
                        receipt.reused_extents,
                        None,
                        swarm_source_ids,
                    )
                };
            let stage_path = WirePath::new(format!("files/{hash}.bin"))?;
            DiskAdmission::new(
                self.store.state_root().to_path_buf(),
                self.store.state_root().to_path_buf(),
                self.min_free_space_bytes,
                stage_budget_bytes,
            )
            .check_materialization(manifest.size)?;
            let private_path =
                self.store
                    .materialize_private_verified(&manifest, &stage_root, &stage_path)?;
            stages.manifests.insert(hash, manifest);
            stages.sources.insert(hash, private_path);
            if source_path.is_some() {
                stats.local_files = stats.local_files.saturating_add(1);
            } else {
                stats.remote_files = stats.remote_files.saturating_add(1);
                stats.pulled_bytes = stats
                    .pulled_bytes
                    .checked_add(transferred_bytes)
                    .context("managed pulled-byte counter overflow")?;
                stats.reused_extents = stats.reused_extents.saturating_add(reused_extents);
            }
            stats.swarm_source_ids.extend(swarm_source_ids);
        }
        stage_guard.disarm();
        Ok((stages, stats))
    }

    async fn apply_remote_managed(
        &self,
        session: &ShareSession,
        actions: &[ApplyAction],
        stages: &ManagedStages,
        observer: &Option<TransferObserver>,
        deadline: Instant,
    ) -> Result<RemoteStats> {
        let mut stats = RemoteStats::default();
        let mut deletions = action_records(actions, true, None);
        deletions.sort_by_key(|record| std::cmp::Reverse(path_depth(&record.path)));
        for record in deletions {
            ensure_managed_deadline(deadline)?;
            session.apply_metadata(record.clone()).await?;
        }
        let mut directories = action_records(actions, false, Some(SyncEntryKind::Directory));
        directories.sort_by_key(|record| path_depth(&record.path));
        for record in directories {
            ensure_managed_deadline(deadline)?;
            session.apply_metadata(record.clone()).await?;
        }
        let mut files = action_records(actions, false, Some(SyncEntryKind::File));
        files.sort_by(|left, right| left.path.cmp(&right.path));
        for record in files {
            ensure_managed_deadline(deadline)?;
            let hash = record
                .content_hash
                .context("managed remote file has no content hash")?;
            let source = stages
                .sources
                .get(&hash)
                .with_context(|| format!("managed source for {hash} was not staged"))?;
            let receipt = session
                .push_record(source, record.clone(), self.profile)
                .await?;
            ensure!(
                receipt.record_hash == record.logical_hash(),
                ShareError::ManifestMismatch
            );
            self.observe(
                observer,
                "file_sent",
                Some(&record.path),
                Some("push"),
                receipt.transferred_bytes,
            );
            stats.pushed_bytes = stats
                .pushed_bytes
                .checked_add(receipt.transferred_bytes)
                .context("managed pushed-byte counter overflow")?;
            stats.reused_extents = stats.reused_extents.saturating_add(receipt.reused_extents);
        }
        Ok(stats)
    }

    async fn stage_remote_file(
        &self,
        session: &SyncSession,
        swarm: Option<&SwarmSources>,
        record: SyncRecord,
        manifest_receipt: PullManifestReceipt,
        missing: Vec<Hash32>,
        pending_local_bytes: u64,
    ) -> Result<(PullReceipt, Vec<EndpointId>)> {
        if missing.is_empty() {
            return Ok((
                PullReceipt {
                    record,
                    manifest: manifest_receipt.manifest,
                    transferred_bytes: 0,
                    reused_extents: manifest_receipt.reused_extents,
                },
                Vec::new(),
            ));
        }

        let swarm_outcome = match swarm {
            Some(swarm) => {
                let admission = DiskAdmission::new(
                    self.store.state_root().to_path_buf(),
                    self.root.clone(),
                    self.min_free_space_bytes,
                    pending_local_bytes,
                );
                swarm
                    .fill_chunks_with_admission(Arc::clone(&self.store), missing.clone(), admission)
                    .await
            }
            None => Err(anyhow::anyhow!("swarm sources unavailable")),
        };
        let mut partial_bytes = 0_u64;
        let mut partial_source_ids = Vec::new();

        match swarm_outcome {
            Ok(receipt) => {
                let still_missing =
                    missing_chunks(Arc::clone(&self.store), manifest_receipt.manifest.clone())
                        .await?;
                if still_missing.is_empty() {
                    let missing_set: std::collections::HashSet<_> = missing.into_iter().collect();
                    let reused_extents = manifest_receipt
                        .manifest
                        .chunks
                        .iter()
                        .filter(|chunk| !missing_set.contains(&chunk.hash))
                        .count();
                    return Ok((
                        PullReceipt {
                            record,
                            manifest: manifest_receipt.manifest,
                            transferred_bytes: receipt.transferred_bytes,
                            reused_extents,
                        },
                        receipt.source_ids().to_vec(),
                    ));
                }
                partial_bytes = receipt.transferred_bytes;
                partial_source_ids = receipt.source_ids().to_vec();
            }
            Err(error) if is_swarm_local_storage_error(&error) => return Err(error),
            Err(error) => {
                if let Some(partial) = swarm_partial_fill(&error) {
                    partial_bytes = partial.transferred_bytes;
                    partial_source_ids = partial.source_ids;
                }
            }
        }

        let mut fallback_receipt = session
            .pull_record_to_with_budget(
                record,
                Arc::clone(&self.store),
                self.root.clone(),
                self.min_free_space_bytes,
                pending_local_bytes,
            )
            .await?;
        fallback_receipt.transferred_bytes = fallback_receipt
            .transferred_bytes
            .checked_add(partial_bytes)
            .context("pulled-byte counter overflow")?;
        Ok((fallback_receipt, partial_source_ids))
    }

    async fn apply_local(
        &self,
        current: &MerkleTree,
        actions: &[ApplyAction],
        manifests: &BTreeMap<Hash32, FileManifest>,
    ) -> Result<()> {
        self.apply_local_with_deadline(current, actions, manifests, None, None)
            .await
    }

    async fn apply_local_with_deadline(
        &self,
        current: &MerkleTree,
        actions: &[ApplyAction],
        manifests: &BTreeMap<Hash32, FileManifest>,
        deadline: Option<Instant>,
        permit: Option<&deltaweave_net::share::ApplyPermit>,
    ) -> Result<()> {
        if let Some(deadline) = deadline {
            ensure_managed_deadline(deadline)?;
        }
        let scan = scan_index(Arc::clone(&self.index)).await?;
        ensure_scan_is_safe(&scan, "local before apply")?;
        let fresh = MerkleTree::from_records(read_records(Arc::clone(&self.index)).await?)?;
        ensure!(
            fresh.root_hash() == current.root_hash() && fresh.len() == current.len(),
            "local state changed before applying reconciliation; retry with a fresh snapshot"
        );

        let mut deletions = action_records(actions, true, None);
        deletions.sort_by_key(|record| std::cmp::Reverse(path_depth(&record.path)));
        for record in deletions {
            if let Some(deadline) = deadline {
                ensure_managed_deadline(deadline)?;
            }
            self.store.apply_causal_record(
                &self.root,
                self.causal_binding(record, permit)?,
                None,
            )?;
            self.index.adopt_verified_record(record)?;
            self.store.mark_record_indexed(&self.root, record)?;
        }

        let mut directories = action_records(actions, false, Some(SyncEntryKind::Directory));
        directories.sort_by_key(|record| path_depth(&record.path));
        for record in directories {
            if let Some(deadline) = deadline {
                ensure_managed_deadline(deadline)?;
            }
            self.store.apply_causal_record(
                &self.root,
                self.causal_binding(record, permit)?,
                None,
            )?;
            self.store
                .set_readonly(&self.root, &record.path, record.readonly)?;
            self.index.adopt_verified_record(record)?;
            self.store.mark_record_indexed(&self.root, record)?;
        }

        let mut files = action_records(actions, false, Some(SyncEntryKind::File));
        files.sort_by(|left, right| left.path.cmp(&right.path));
        for record in files {
            if let Some(deadline) = deadline {
                ensure_managed_deadline(deadline)?;
            }
            let hash = record
                .content_hash
                .context("validated live file unexpectedly lacks a hash")?;
            let manifest = manifests
                .get(&hash)
                .with_context(|| format!("required content {hash} was not staged"))?;
            DiskAdmission::new(
                self.store.state_root().to_path_buf(),
                self.root.clone(),
                self.min_free_space_bytes,
                0,
            )
            .check_materialization(manifest.size)?;
            let change = self.store.apply_causal_record(
                &self.root,
                self.causal_binding(record, permit)?,
                Some(manifest),
            )?;
            let observation = self
                .store
                .observe_path_change(&change)?
                .after_readonly_update(local_path(&self.root, &record.path), record.readonly)?;
            self.index.adopt_materialized_record(record, &observation)?;
            self.store.mark_record_indexed(&self.root, record)?;
        }
        Ok(())
    }

    async fn apply_remote(
        &self,
        session: &impl ReconcileTransport,
        actions: &[ApplyAction],
        observer: &Option<TransferObserver>,
    ) -> Result<RemoteStats> {
        let mut stats = RemoteStats::default();
        let mut deletions = action_records(actions, true, None);
        deletions.sort_by_key(|record| std::cmp::Reverse(path_depth(&record.path)));
        for record in deletions {
            session.apply_metadata(record.clone()).await?;
        }

        let mut directories = action_records(actions, false, Some(SyncEntryKind::Directory));
        directories.sort_by_key(|record| path_depth(&record.path));
        for record in directories {
            session.apply_metadata(record.clone()).await?;
        }

        let mut files = action_records(actions, false, Some(SyncEntryKind::File));
        files.sort_by(|left, right| left.path.cmp(&right.path));
        for record in files {
            let source = local_path(&self.root, &record.path);
            let SyncApplyReceipt {
                transferred_bytes,
                reused_extents,
                ..
            } = session
                .push_record(source, record.clone(), self.profile)
                .await?;
            self.observe(
                observer,
                "file_sent",
                Some(&record.path),
                Some("push"),
                transferred_bytes,
            );
            stats.pushed_bytes = stats
                .pushed_bytes
                .checked_add(transferred_bytes)
                .context("pushed-byte counter overflow")?;
            stats.reused_extents = stats.reused_extents.saturating_add(reused_extents);
        }
        Ok(stats)
    }
}

async fn scan_index(index: Arc<LocalIndex>) -> Result<ScanReport> {
    tokio::task::spawn_blocking(move || index.scan())
        .await
        .context("index scan task failed")?
}

async fn missing_chunks(store: Arc<Store>, manifest: FileManifest) -> Result<Vec<Hash32>> {
    tokio::task::spawn_blocking(move || store.missing_chunks(&manifest))
        .await
        .context("chunk inventory task failed")
}

async fn read_records(index: Arc<LocalIndex>) -> Result<Vec<SyncRecord>> {
    tokio::task::spawn_blocking(move || index.sync_records())
        .await
        .context("index snapshot task failed")?
}

fn ensure_scan_is_safe(report: &ScanReport, side: &str) -> Result<()> {
    ensure!(
        report.collisions.is_empty(),
        "{side} scan has {} cross-platform path collision(s)",
        report.collisions.len()
    );
    ensure!(
        report.issues.is_empty() && report.retries_queued == 0,
        "{side} scan is incomplete: {} issue(s), {} retry/retries queued",
        report.issues.len(),
        report.retries_queued
    );
    Ok(())
}

fn validate_materializable_namespace(records: &[SyncRecord]) -> Result<()> {
    let by_path: BTreeMap<_, _> = records
        .iter()
        .map(|record| (record.path.as_str(), record))
        .collect();
    let mut portable_paths = BTreeMap::new();
    for record in records.iter().filter(|record| !record.tombstone) {
        ensure!(
            matches!(record.kind, SyncEntryKind::File | SyncEntryKind::Directory),
            "safe materialization of {:?} at {} is not enabled",
            record.kind,
            record.path
        );
        let components: Vec<_> = record.path.components().collect();
        for end in 1..=components.len() {
            let ancestor = components[..end].join("/");
            let key = collision_key(&WirePath::new(ancestor.clone())?);
            if let Some(previous) = portable_paths.insert(key, ancestor.clone()) {
                ensure!(
                    previous == ancestor,
                    "merged namespace has a cross-platform path collision: {previous} and {ancestor}"
                );
            }
            if end < components.len()
                && let Some(ancestor_record) = by_path.get(ancestor.as_str())
            {
                ensure!(
                    !ancestor_record.tombstone && ancestor_record.kind == SyncEntryKind::Directory,
                    "namespace has non-directory ancestor {ancestor} for {}",
                    record.path
                );
            }
        }
    }
    Ok(())
}

fn live_file_sources(records: &[SyncRecord]) -> BTreeMap<Hash32, &SyncRecord> {
    let mut sources = BTreeMap::new();
    for record in records
        .iter()
        .filter(|record| !record.tombstone && record.kind == SyncEntryKind::File)
    {
        if let Some(hash) = record.content_hash {
            sources.entry(hash).or_insert(record);
        }
    }
    sources
}

fn action_records(
    actions: &[ApplyAction],
    tombstone: bool,
    kind: Option<SyncEntryKind>,
) -> Vec<&SyncRecord> {
    actions
        .iter()
        .filter_map(|action| match action {
            ApplyAction::Delete { record } if tombstone => Some(record),
            ApplyAction::Materialize { record }
                if !tombstone && kind.is_none_or(|kind| record.kind == kind) =>
            {
                Some(record)
            }
            ApplyAction::Delete { .. } | ApplyAction::Materialize { .. } => None,
        })
        .collect()
}

fn local_path(root: &Path, path: &WirePath) -> PathBuf {
    let mut local = root.to_path_buf();
    for component in path.components() {
        local.push(component);
    }
    local
}

fn path_depth(path: &WirePath) -> usize {
    path.components().count()
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, fs};

    use deltaweave_core::{SYNC_RECORD_SCHEMA_V1, VersionVector};
    use deltaweave_net::share::{Permission, ShareService};
    use deltaweave_net::{
        NetworkMode, PeerPolicy, ServerConfig, start_server, start_server_observed,
    };
    use iroh::{EndpointAddr, SecretKey};
    use tempfile::TempDir;

    use super::*;

    fn replica(key: &SecretKey) -> ReplicaId {
        ReplicaId(Hash32::digest(key.public().as_bytes()))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_missing_chunk_inventory_preserves_results() {
        let state = TempDir::new().expect("state can be created");
        let local = Arc::new(Store::open(state.path()).expect("store can open"));
        let first = Hash32::digest(b"first missing chunk");
        let second = Hash32::digest(b"second missing chunk");
        let manifest = FileManifest {
            schema_version: deltaweave_core::MANIFEST_SCHEMA_V1,
            size: 3,
            file_hash: Hash32::digest(b"aba"),
            profile: ChunkingProfile::DEFAULT,
            chunks: vec![
                deltaweave_core::ChunkDescriptor {
                    offset: 0,
                    length: 1,
                    hash: first,
                },
                deltaweave_core::ChunkDescriptor {
                    offset: 1,
                    length: 1,
                    hash: second,
                },
                deltaweave_core::ChunkDescriptor {
                    offset: 2,
                    length: 1,
                    hash: first,
                },
            ],
        };

        let missing = missing_chunks(Arc::clone(&local), manifest)
            .await
            .expect("inventory helper completes");

        assert_eq!(missing, vec![first, second]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn managed_causal_authorization_rejects_missing_and_cross_share_permits() {
        let name = "tests::managed_causal_authorization_rejects_missing_and_cross_share_permits";
        if std::env::var("DW_MANAGED_CAUSAL_AUTH_CHILD")
            .ok()
            .as_deref()
            != Some(name)
        {
            let home = tempfile::tempdir().expect("isolated home");
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", name, "--nocapture"])
                .env("DW_MANAGED_CAUSAL_AUTH_CHILD", name)
                .env("HOME", home.path())
                .env("USERPROFILE", home.path())
                .status()
                .expect("run isolated authorization test");
            assert!(status.success());
            return;
        }
        let base = TempDir::new().expect("share test root can be created");
        let owner = ShareService::open(
            base.path().join("owner-device"),
            NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap_or_else(|error| panic!("owner service opens: {error:#}"));
        let member = ShareService::open(
            base.path().join("member-device"),
            NetworkMode::DirectOnly,
            None,
        )
        .await
        .expect("member service opens");
        let first = owner
            .create_owned_share(
                "first".into(),
                base.path().join("first-root"),
                base.path().join("first-state"),
                None,
                0,
            )
            .await
            .expect("first share opens");
        let second = owner
            .create_owned_share(
                "second".into(),
                base.path().join("second-root"),
                base.path().join("second-state"),
                None,
                0,
            )
            .await
            .expect("second share opens");
        let first_grant = member
            .enroll(
                &first
                    .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
                    .expect("first ticket issues"),
                None,
            )
            .await
            .expect("first membership enrolls");
        let second_grant = member
            .enroll(
                &second
                    .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
                    .expect("second ticket issues"),
                None,
            )
            .await
            .expect("second membership enrolls");
        let first_session = member
            .open_session(first_grant.owner, first_grant.share_id)
            .expect("first session opens");
        let second_session = member
            .open_session(second_grant.owner, second_grant.share_id)
            .expect("second session opens");
        let empty = MerkleTree::from_records(Vec::new()).expect("empty tree builds");
        let first_snapshot = first_session
            .fetch_authoritative_snapshot(&empty)
            .await
            .expect("first snapshot fetches");
        let first_permit = first_session
            .revalidate_before_apply(&first_snapshot.token)
            .await
            .expect("first permit issues");
        let second_snapshot = second_session
            .fetch_authoritative_snapshot(&empty)
            .await
            .expect("second snapshot fetches");
        let second_permit = second_session
            .revalidate_before_apply(&second_snapshot.token)
            .await
            .expect("second permit issues");
        let record = SyncRecord {
            schema_version: SYNC_RECORD_SCHEMA_V1,
            path: WirePath::new("retained.bin").expect("record path validates"),
            kind: SyncEntryKind::File,
            size: 0,
            content_hash: Some(Hash32::digest(&[])),
            readonly: false,
            version: VersionVector::default(),
            tombstone: false,
        };
        let binding = |authorization| deltaweave_store::CausalBinding {
            record: record.clone(),
            precondition: None,
            authorization,
        };

        assert!(
            ReplicaState::validate_managed_causal_authorization(&first_session, &binding(None),)
                .is_err(),
            "missing opaque authorization must not become a managed recovery"
        );
        assert!(
            ReplicaState::validate_managed_causal_authorization(
                &first_session,
                &binding(Some(postcard::to_stdvec(&second_permit).unwrap())),
            )
            .is_err(),
            "a valid permit from another share must not authorize this journal"
        );
        assert!(
            ReplicaState::validate_managed_causal_authorization(
                &first_session,
                &binding(Some(postcard::to_stdvec(&first_permit).unwrap())),
            )
            .is_ok(),
            "the exact owner/share/consumer permit must authenticate"
        );
        first_session.close().await;
        second_session.close().await;
        member.shutdown().await.expect("member shuts down");
        owner.shutdown().await.expect("owner shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn observed_cycle_reports_real_bidirectional_payloads_and_failure() {
        let local = TempDir::new().unwrap();
        let local_state = TempDir::new().unwrap();
        let remote = TempDir::new().unwrap();
        let remote_state = TempDir::new().unwrap();
        fs::write(local.path().join("out.txt"), b"outgoing").unwrap();
        fs::write(remote.path().join("in.txt"), b"incoming").unwrap();
        let key = SecretKey::generate();
        let server_events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured_server = Arc::clone(&server_events);
        let server = start_server_observed(
            ServerConfig {
                secret_key: SecretKey::generate(),
                destination_root: remote.path().into(),
                state_root: remote_state.path().into(),
                peer_policy: PeerPolicy::AllowListed(HashSet::from([key.public()])),
                network_mode: NetworkMode::DirectOnly,
                bind_address: Some("127.0.0.1:0".parse().unwrap()),
                max_connections: 8,
                min_free_space_bytes: 0,
            },
            Some(TransferObserver::new(move |event| {
                captured_server.lock().unwrap().push(event)
            })),
        )
        .await
        .unwrap();
        let engine = SyncEngine::open(SyncConfig {
            swarm_sources: Vec::new(),
            root: local.path().into(),
            state_root: local_state.path().into(),
            replica: replica(&key),
            client: SyncClient {
                secret_key: key,
                remote: server.endpoint_addr(),
                network_mode: NetworkMode::DirectOnly,
            },
            profile: ChunkingProfile::DEFAULT,
            ignored_paths: Vec::new(),
        })
        .unwrap();
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = Arc::clone(&events);
        let observer = TransferObserver::new(move |event| captured.lock().unwrap().push(event));
        let report = engine
            .sync_once_observed(Some(observer.clone()))
            .await
            .unwrap();
        assert_eq!((report.pulled_bytes, report.pushed_bytes), (8, 8));
        assert_eq!(fs::read(local.path().join("in.txt")).unwrap(), b"incoming");
        assert_eq!(
            fs::read(remote.path().join("out.txt")).unwrap(),
            b"outgoing"
        );
        let inventory = engine.inventory().unwrap();
        assert_eq!(
            (inventory.files, inventory.bytes, inventory.retries),
            (2, 16, 0)
        );
        {
            let captured = events.lock().unwrap();
            let phases: Vec<_> = captured.iter().map(|event| event.phase.as_str()).collect();
            for phase in [
                "scanning",
                "comparing",
                "pulling",
                "applying",
                "pushing",
                "verifying",
                "complete",
            ] {
                assert!(phases.contains(&phase), "missing phase {phase}");
            }
            assert!(captured.iter().any(|event| event.phase == "file_received"
                && event.path.as_deref() == Some("in.txt")
                && event.bytes == 8));
            assert!(captured.iter().any(|event| event.phase == "file_sent"
                && event.path.as_deref() == Some("out.txt")
                && event.bytes == 8));
        }
        {
            let captured = server_events.lock().unwrap();
            assert!(captured.iter().any(|event| event.phase == "file_received"
                && event.path.as_deref() == Some("out.txt")
                && event.direction.as_deref() == Some("receive")
                && event.bytes == 8));
            assert!(captured.iter().any(|event| event.phase == "file_sent"
                && event.path.as_deref() == Some("in.txt")
                && event.direction.as_deref() == Some("send")
                && event.bytes == 8));
        }
        // An instrumentation failure must not turn a valid sync into a transfer failure.
        engine
            .sync_once_observed(Some(TransferObserver::new(|_| panic!("observer failed"))))
            .await
            .unwrap();
        events.lock().unwrap().clear();
        server.pause().await.unwrap();
        assert!(engine.sync_once_observed(Some(observer)).await.is_err());
        assert_eq!(events.lock().unwrap().last().unwrap().phase, "error");
        server.shutdown().await.unwrap();
    }

    fn test_engine(root: &TempDir, state: &TempDir) -> SyncEngine {
        let client_key = SecretKey::generate();
        SyncEngine::open(SyncConfig {
            swarm_sources: Vec::new(),
            root: root.path().to_path_buf(),
            state_root: state.path().to_path_buf(),
            replica: replica(&client_key),
            client: SyncClient {
                secret_key: client_key,
                remote: EndpointAddr::new(SecretKey::generate().public()),
                network_mode: NetworkMode::DirectOnly,
            },
            profile: ChunkingProfile::DEFAULT,
            ignored_paths: Vec::new(),
        })
        .expect("test sync engine can open")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn remote_counter_poisoning_is_rejected_before_content_staging() {
        let local = TempDir::new().unwrap();
        let local_state = TempDir::new().unwrap();
        let remote = TempDir::new().unwrap();
        let remote_state = TempDir::new().unwrap();
        let client_key = SecretKey::generate();
        let server_key = SecretKey::generate();
        let path = WirePath::new("poisoned.bin").unwrap();
        fs::write(remote.path().join(path.as_str()), b"remote payload").unwrap();
        fs::write(local.path().join("keep.txt"), b"keep local contents").unwrap();
        {
            let index = LocalIndex::open(
                remote.path(),
                remote_state.path().join("index.redb"),
                replica(&server_key),
                IndexOptions::default(),
            )
            .unwrap();
            index.scan().unwrap();
            let mut record = index.get(&path).unwrap().unwrap().to_sync_record();
            record.version.observe(replica(&client_key), u64::MAX);
            index.adopt_verified_record(&record).unwrap();
        }
        let server = start_server(ServerConfig {
            secret_key: server_key,
            destination_root: remote.path().into(),
            state_root: remote_state.path().into(),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
            network_mode: NetworkMode::DirectOnly,
            bind_address: Some("127.0.0.1:0".parse().unwrap()),
            max_connections: 8,
            min_free_space_bytes: 0,
        })
        .await
        .unwrap();
        let engine = SyncEngine::open(SyncConfig {
            root: local.path().into(),
            state_root: local_state.path().into(),
            replica: replica(&client_key),
            client: SyncClient {
                secret_key: client_key,
                remote: server.endpoint_addr(),
                network_mode: NetworkMode::DirectOnly,
            },
            swarm_sources: vec![server.endpoint_addr()],
            profile: ChunkingProfile::DEFAULT,
            ignored_paths: Vec::new(),
        })
        .unwrap();
        let before = scanned_tree(&engine);
        let counter = engine.index.replica_counter().unwrap();
        let error = engine.sync_once().await.unwrap_err();
        assert!(format!("{error:#}").contains("remote snapshot advances local replica counter"));
        assert_eq!(engine.index.replica_counter().unwrap(), counter);
        assert_eq!(scanned_tree(&engine).root_hash(), before.root_hash());
        assert_eq!(
            fs::read(local.path().join("keep.txt")).unwrap(),
            b"keep local contents"
        );
        assert!(!local.path().join(path.as_str()).exists());
        assert!(
            !engine
                .store
                .chunks()
                .contains(Hash32::digest(b"remote payload"))
        );
        server.shutdown().await.unwrap();
    }

    fn scanned_tree(engine: &SyncEngine) -> MerkleTree {
        let report = engine.index.scan().expect("test root can be scanned");
        ensure_scan_is_safe(&report, "test").expect("test scan is complete");
        MerkleTree::from_records(
            engine
                .index
                .sync_records()
                .expect("test records can be read"),
        )
        .expect("test records form a Merkle tree")
    }

    fn stage_file(
        engine: &SyncEngine,
        source_root: &TempDir,
        name: &str,
        bytes: &[u8],
    ) -> FileManifest {
        let source = source_root.path().join(name);
        fs::write(&source, bytes).expect("staged source can be written");
        engine
            .store
            .ingest_file(&source, ChunkingProfile::DEFAULT)
            .expect("staged source can enter the local CAS")
    }

    fn remote_file_record(path: &str, bytes: &[u8], counter: u64) -> SyncRecord {
        let remote = ReplicaId(Hash32::digest(b"test remote replica"));
        let mut version = VersionVector::default();
        version.observe(remote, counter);
        SyncRecord {
            schema_version: SYNC_RECORD_SCHEMA_V1,
            path: WirePath::new(path).expect("fixture path is portable"),
            kind: SyncEntryKind::File,
            size: bytes.len() as u64,
            content_hash: Some(Hash32::digest(bytes)),
            readonly: false,
            version,
            tombstone: false,
        }
    }

    fn causally_newer_file_record(
        current: &MerkleTree,
        path: &WirePath,
        bytes: &[u8],
    ) -> SyncRecord {
        let mut record = current
            .get(path)
            .expect("current tree contains fixture path")
            .clone();
        record
            .version
            .increment(ReplicaId(Hash32::digest(b"test remote replica")))
            .expect("fixture clock can advance");
        record.size = bytes.len() as u64;
        record.content_hash = Some(Hash32::digest(bytes));
        record
    }

    fn causally_newer_tombstone(current: &MerkleTree, path: &WirePath) -> SyncRecord {
        let mut record = current
            .get(path)
            .expect("current tree contains fixture path")
            .clone();
        record
            .version
            .increment(ReplicaId(Hash32::digest(b"test remote replica")))
            .expect("fixture clock can advance");
        record.tombstone = true;
        record
    }

    #[tokio::test]
    async fn apply_local_rejects_edit_made_after_the_planning_snapshot() {
        let root = TempDir::new().expect("local root can be created");
        let state = TempDir::new().expect("local state can be created");
        let sources = TempDir::new().expect("source root can be created");
        fs::write(root.path().join("report.txt"), b"snapshot bytes")
            .expect("snapshot file can be written");
        let engine = test_engine(&root, &state);
        let current = scanned_tree(&engine);
        let path = WirePath::new("report.txt").expect("fixture path is portable");
        let desired_bytes = b"planned remote bytes";
        let record = causally_newer_file_record(&current, &path, desired_bytes);
        let manifest = stage_file(&engine, &sources, "planned.bin", desired_bytes);
        let manifests = BTreeMap::from([(manifest.file_hash, manifest)]);

        fs::write(root.path().join("report.txt"), b"fresh local edit")
            .expect("fresh local edit can be written");
        let outcome = engine
            .apply_local(&current, &[ApplyAction::Materialize { record }], &manifests)
            .await;

        assert!(outcome.is_err(), "stale local plan must be rejected");
        assert_eq!(
            fs::read(root.path().join("report.txt")).expect("fresh local edit remains readable"),
            b"fresh local edit"
        );
    }

    #[tokio::test]
    async fn apply_local_rejects_target_created_after_the_planning_snapshot() {
        let root = TempDir::new().expect("local root can be created");
        let state = TempDir::new().expect("local state can be created");
        let sources = TempDir::new().expect("source root can be created");
        let engine = test_engine(&root, &state);
        let current = scanned_tree(&engine);
        let desired_bytes = b"planned remote creation";
        let record = remote_file_record("new.txt", desired_bytes, 1);
        let manifest = stage_file(&engine, &sources, "planned.bin", desired_bytes);
        let manifests = BTreeMap::from([(manifest.file_hash, manifest)]);

        fs::write(root.path().join("new.txt"), b"fresh local creation")
            .expect("fresh local creation can be written");
        let outcome = engine
            .apply_local(&current, &[ApplyAction::Materialize { record }], &manifests)
            .await;

        assert!(
            outcome.is_err(),
            "new target must invalidate the local plan"
        );
        assert_eq!(
            fs::read(root.path().join("new.txt")).expect("fresh local creation remains readable"),
            b"fresh local creation"
        );
    }

    #[tokio::test]
    async fn apply_local_rejects_fresh_edit_before_any_planned_deletion() {
        let root = TempDir::new().expect("local root can be created");
        let state = TempDir::new().expect("local state can be created");
        fs::write(root.path().join("delete-first.txt"), b"must remain")
            .expect("first deletion fixture can be written");
        fs::write(root.path().join("edited-delete.txt"), b"snapshot bytes")
            .expect("edited deletion fixture can be written");
        let engine = test_engine(&root, &state);
        let current = scanned_tree(&engine);
        let first_path = WirePath::new("delete-first.txt").expect("fixture path is portable");
        let edited_path = WirePath::new("edited-delete.txt").expect("fixture path is portable");
        let first = causally_newer_tombstone(&current, &first_path);
        let edited = causally_newer_tombstone(&current, &edited_path);

        fs::write(root.path().join("edited-delete.txt"), b"fresh local edit")
            .expect("fresh local edit can be written");
        let outcome = engine
            .apply_local(
                &current,
                &[
                    ApplyAction::Delete { record: first },
                    ApplyAction::Delete { record: edited },
                ],
                &BTreeMap::new(),
            )
            .await;

        assert!(outcome.is_err(), "fresh edit must invalidate all deletions");
        assert_eq!(
            fs::read(root.path().join("edited-delete.txt")).expect("fresh edit remains readable"),
            b"fresh local edit"
        );
        assert_eq!(
            fs::read(root.path().join("delete-first.txt"))
                .expect("no earlier deletion was partially applied"),
            b"must remain"
        );
    }

    #[tokio::test]
    async fn apply_local_checks_snapshot_drift_when_no_local_actions_are_planned() {
        let root = TempDir::new().expect("local root can be created");
        let state = TempDir::new().expect("local state can be created");
        fs::write(root.path().join("outgoing.txt"), b"snapshot bytes")
            .expect("outgoing fixture can be written");
        let engine = test_engine(&root, &state);
        let current = scanned_tree(&engine);

        fs::write(root.path().join("outgoing.txt"), b"fresh outgoing edit")
            .expect("fresh outgoing edit can be written");
        let outcome = engine.apply_local(&current, &[], &BTreeMap::new()).await;

        assert!(
            outcome.is_err(),
            "remote-only plans must reject a stale local source"
        );
        assert_eq!(
            fs::read(root.path().join("outgoing.txt"))
                .expect("fresh outgoing edit remains readable"),
            b"fresh outgoing edit"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn apply_local_rejects_incomplete_prescan_before_planned_deletion() {
        let root = TempDir::new().expect("local root can be created");
        let state = TempDir::new().expect("local state can be created");
        fs::write(root.path().join("delete.txt"), b"must remain")
            .expect("deletion fixture can be written");
        let engine = test_engine(&root, &state);
        let current = scanned_tree(&engine);
        let path = WirePath::new("delete.txt").expect("fixture path is portable");
        let deletion = causally_newer_tombstone(&current, &path);

        fs::write(root.path().join("bad:name"), b"unportable local file")
            .expect("Unix permits the non-portable fixture name");
        let outcome = engine
            .apply_local(
                &current,
                &[ApplyAction::Delete { record: deletion }],
                &BTreeMap::new(),
            )
            .await;

        assert!(
            outcome.is_err(),
            "incomplete local scan must abort the plan"
        );
        assert_eq!(
            fs::read(root.path().join("delete.txt"))
                .expect("planned deletion was not partially applied"),
            b"must remain"
        );
        assert_eq!(
            fs::read(root.path().join("bad:name")).expect("unportable local file remains readable"),
            b"unportable local file"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn loopback_stale_snapshot_aborts_then_fresh_sync_preserves_both_edits() {
        let local_root = TempDir::new().expect("local root can be created");
        let local_state = TempDir::new().expect("local state can be created");
        let remote_root = TempDir::new().expect("remote root can be created");
        let remote_state = TempDir::new().expect("remote state can be created");
        fs::write(local_root.path().join("shared.txt"), b"common baseline")
            .expect("local baseline can be written");
        fs::write(remote_root.path().join("shared.txt"), b"common baseline")
            .expect("remote baseline can be written");

        let client_key = SecretKey::generate();
        let server = start_server(ServerConfig {
            secret_key: SecretKey::generate(),
            destination_root: remote_root.path().to_path_buf(),
            state_root: remote_state.path().to_path_buf(),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
            network_mode: NetworkMode::DirectOnly,
            bind_address: None,
            max_connections: 64,
            min_free_space_bytes: 0,
        })
        .await
        .expect("server can start");
        let engine = SyncEngine::open(SyncConfig {
            swarm_sources: Vec::new(),
            root: local_root.path().to_path_buf(),
            state_root: local_state.path().to_path_buf(),
            replica: replica(&client_key),
            client: SyncClient {
                secret_key: client_key,
                remote: server.endpoint_addr(),
                network_mode: NetworkMode::DirectOnly,
            },
            profile: ChunkingProfile::DEFAULT,
            ignored_paths: Vec::new(),
        })
        .expect("sync engine can open");
        engine
            .sync_once()
            .await
            .expect("common baseline can converge");

        fs::write(remote_root.path().join("shared.txt"), b"remote edit")
            .expect("remote edit can be written");
        let scan = scan_index(Arc::clone(&engine.index))
            .await
            .expect("local planning scan succeeds");
        ensure_scan_is_safe(&scan, "planned local").expect("local planning scan is complete");
        let local_records = read_records(Arc::clone(&engine.index))
            .await
            .expect("planned local records can be read");
        let local_tree = MerkleTree::from_records(local_records.clone())
            .expect("planned local records form a tree");
        let session = engine
            .client
            .open_session()
            .await
            .expect("loopback session can open");
        fs::write(local_root.path().join("shared.txt"), b"fresh local edit")
            .expect("fresh local edit can be written after planning");

        let stale = engine
            .sync_with_session(&session, local_records, local_tree, &None)
            .await;
        session.close().await;

        assert!(stale.is_err(), "stale loopback pass must abort");
        assert_eq!(
            fs::read(local_root.path().join("shared.txt"))
                .expect("fresh local edit remains readable after abort"),
            b"fresh local edit"
        );
        assert_eq!(
            fs::read(remote_root.path().join("shared.txt"))
                .expect("remote edit remains readable after abort"),
            b"remote edit"
        );

        let recovered = engine
            .sync_once()
            .await
            .expect("fresh reconciliation can preserve the concurrent edits");
        assert_eq!(recovered.status, "pass");
        assert_eq!(
            recovered.verified_local_root,
            recovered.verified_remote_root
        );
        assert_eq!(recovered.conflicts.len(), 1);
        let conflict_path = recovered.conflicts[0]
            .conflict_path
            .as_ref()
            .expect("losing edit has a conflict copy");
        let expected = BTreeSet::from([b"fresh local edit".to_vec(), b"remote edit".to_vec()]);
        assert_eq!(
            BTreeSet::from([
                fs::read(local_root.path().join("shared.txt"))
                    .expect("local canonical edit can be read"),
                fs::read(local_path(local_root.path(), conflict_path))
                    .expect("local conflict edit can be read"),
            ]),
            expected
        );
        assert_eq!(
            BTreeSet::from([
                fs::read(remote_root.path().join("shared.txt"))
                    .expect("remote canonical edit can be read"),
                fs::read(local_path(remote_root.path(), conflict_path))
                    .expect("remote conflict edit can be read"),
            ]),
            BTreeSet::from([b"fresh local edit".to_vec(), b"remote edit".to_vec()])
        );
        server.shutdown().await.expect("server shuts down");
    }

    fn file_record(path: &str) -> SyncRecord {
        let mut version = deltaweave_core::VersionVector::default();
        version.observe(ReplicaId(Hash32::digest(b"fixture")), 1);
        SyncRecord {
            schema_version: deltaweave_core::SYNC_RECORD_SCHEMA_V1,
            path: WirePath::new(path).expect("portable fixture path"),
            kind: SyncEntryKind::File,
            size: 7,
            content_hash: Some(Hash32::digest(b"fixture")),
            readonly: false,
            version,
            tombstone: false,
        }
    }

    fn open_test_engine(root: &Path, state: &Path) -> SyncEngine {
        let key = SecretKey::generate();
        SyncEngine::open(SyncConfig {
            swarm_sources: Vec::new(),
            root: root.to_path_buf(),
            state_root: state.to_path_buf(),
            replica: replica(&key),
            client: SyncClient {
                secret_key: key,
                remote: SecretKey::generate().public().into(),
                network_mode: NetworkMode::DirectOnly,
            },
            profile: ChunkingProfile::DEFAULT,
            ignored_paths: Vec::new(),
        })
        .expect("sync engine")
    }

    #[test]
    #[cfg(unix)]
    fn new_sync_state_root_is_private() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().expect("workspace");
        let state = temp.path().join("private/state");
        let _engine = open_test_engine(&temp.path().join("root"), &state);
        assert_eq!(
            fs::metadata(&state)
                .expect("state directory")
                .permissions()
                .mode()
                & 0o077,
            0
        );
    }

    #[test]
    fn merged_namespace_rejects_case_and_unicode_collisions() {
        for (left, right) in [
            ("README.txt", "readme.txt"),
            ("caf\u{00e9}.txt", "cafe\u{0301}.txt"),
            ("Docs/first.txt", "docs/second.txt"),
        ] {
            let records = [file_record(left), file_record(right)];
            assert!(
                validate_materializable_namespace(&records).is_err(),
                "colliding namespace {left:?}, {right:?} must fail before materialization"
            );
        }
        assert!(
            validate_materializable_namespace(&[
                file_record("docs/first.txt"),
                file_record("docs/second.txt"),
            ])
            .is_ok()
        );
    }

    #[tokio::test]
    async fn local_edit_after_snapshot_blocks_remote_deletion() {
        let root = TempDir::new().expect("local root");
        let state = TempDir::new().expect("local state");
        let path = root.path().join("document.txt");
        fs::write(&path, b"initial content").expect("initial file");
        let engine = open_test_engine(root.path(), state.path());
        engine.index.scan().expect("initial scan");
        let initial = MerkleTree::from_records(engine.index.sync_records().expect("records"))
            .expect("snapshot");
        let mut deleted = initial.records().next().expect("initial file").clone();
        deleted.tombstone = true;
        deleted
            .version
            .observe(ReplicaId(Hash32::digest(b"remote")), 1);
        fs::write(&path, b"new local work after the snapshot").expect("local edit");

        let result = engine
            .apply_local(
                &initial,
                &[ApplyAction::Delete { record: deleted }],
                &BTreeMap::new(),
            )
            .await;
        assert!(
            result.is_err(),
            "changed local state must abort application"
        );
        assert_eq!(
            fs::read(&path).expect("local edit remains at its original path"),
            b"new local work after the snapshot"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn merged_case_collision_aborts_before_either_namespace_is_modified() {
        let workspace = TempDir::new().expect("workspace");
        let local = workspace.path().join("local");
        let remote = workspace.path().join("remote");
        fs::create_dir(&local).expect("local root");
        fs::create_dir(&remote).expect("remote root");
        fs::write(local.join("README.txt"), b"local original").expect("local fixture");
        fs::write(remote.join("readme.txt"), b"remote original").expect("remote fixture");
        let key = SecretKey::generate();
        let server = start_server(ServerConfig {
            secret_key: SecretKey::generate(),
            destination_root: remote.clone(),
            state_root: workspace.path().join("remote-state"),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([key.public()])),
            network_mode: NetworkMode::DirectOnly,
            bind_address: Some("127.0.0.1:0".parse().expect("loopback")),
            max_connections: 64,
            min_free_space_bytes: 0,
        })
        .await
        .expect("server starts");
        let engine = SyncEngine::open(SyncConfig {
            swarm_sources: Vec::new(),
            root: local.clone(),
            state_root: workspace.path().join("local-state"),
            replica: replica(&key),
            client: SyncClient {
                secret_key: key,
                remote: server.endpoint_addr(),
                network_mode: NetworkMode::DirectOnly,
            },
            profile: ChunkingProfile::DEFAULT,
            ignored_paths: Vec::new(),
        })
        .expect("engine opens");

        let outcome = engine.sync_once().await;
        server.shutdown().await.expect("server shuts down");
        assert!(outcome.is_err(), "cross-peer collision must abort sync");
        assert_eq!(
            fs::read(local.join("README.txt")).expect("local survives"),
            b"local original"
        );
        assert_eq!(
            fs::read(remote.join("readme.txt")).expect("remote survives"),
            b"remote original"
        );
        assert_eq!(fs::read_dir(&local).expect("local namespace").count(), 1);
        assert_eq!(fs::read_dir(&remote).expect("remote namespace").count(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bidirectional_conflict_delete_restart_and_type_transition_converge() {
        let local_root = TempDir::new().expect("local root can be created");
        let local_state = TempDir::new().expect("local state can be created");
        let remote_root = TempDir::new().expect("remote root can be created");
        let remote_state = TempDir::new().expect("remote state can be created");
        fs::create_dir(local_root.path().join("local")).expect("local folder can be created");
        fs::create_dir(remote_root.path().join("remote")).expect("remote folder can be created");
        fs::write(local_root.path().join("local/only.txt"), b"from local")
            .expect("local fixture can be written");
        fs::write(remote_root.path().join("remote/only.txt"), b"from remote")
            .expect("remote fixture can be written");
        fs::write(local_root.path().join("shared.txt"), b"common")
            .expect("shared local fixture can be written");
        fs::write(remote_root.path().join("shared.txt"), b"common")
            .expect("shared remote fixture can be written");

        let client_key = SecretKey::generate();
        let server = start_server(ServerConfig {
            secret_key: SecretKey::generate(),
            destination_root: remote_root.path().to_path_buf(),
            state_root: remote_state.path().to_path_buf(),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
            network_mode: NetworkMode::DirectOnly,
            bind_address: None,
            max_connections: 64,
            min_free_space_bytes: 0,
        })
        .await
        .expect("server can start");
        let config = SyncConfig {
            root: local_root.path().to_path_buf(),
            state_root: local_state.path().to_path_buf(),
            replica: replica(&client_key),
            client: SyncClient {
                secret_key: client_key.clone(),
                remote: server.endpoint_addr(),
                network_mode: NetworkMode::DirectOnly,
            },
            profile: ChunkingProfile::DEFAULT,
            swarm_sources: Vec::new(),
            ignored_paths: Vec::new(),
        };
        let engine = SyncEngine::open(config.clone()).expect("sync engine can open");

        let first = engine.sync_once().await.expect("initial merge converges");
        assert_eq!(first.status, "pass");
        assert_eq!(first.verified_local_root, first.verified_remote_root);
        assert_eq!(
            fs::read(local_root.path().join("remote/only.txt"))
                .expect("remote-only file reaches local"),
            b"from remote"
        );
        assert_eq!(
            fs::read(remote_root.path().join("local/only.txt"))
                .expect("local-only file reaches remote"),
            b"from local"
        );
        let unchanged = engine.sync_once().await.expect("unchanged retry converges");
        assert_eq!(unchanged.local_actions, 0);
        assert_eq!(unchanged.remote_actions, 0);
        assert_eq!(unchanged.merkle_queries, 1);

        fs::write(local_root.path().join("shared.txt"), b"edited on windows")
            .expect("local concurrent edit can be written");
        fs::write(remote_root.path().join("shared.txt"), b"edited on synology")
            .expect("remote concurrent edit can be written");
        let conflict = engine.sync_once().await.expect("concurrent edit converges");
        assert_eq!(conflict.conflicts.len(), 1);
        let conflict_path = conflict.conflicts[0]
            .conflict_path
            .as_ref()
            .expect("losing content has a conflict copy");
        let local_values = BTreeSet::from([
            fs::read(local_root.path().join("shared.txt")).expect("winner can be read"),
            fs::read(local_path(local_root.path(), conflict_path))
                .expect("conflict copy can be read"),
        ]);
        assert_eq!(
            local_values,
            BTreeSet::from([
                b"edited on windows".to_vec(),
                b"edited on synology".to_vec()
            ])
        );
        assert_eq!(
            fs::read(remote_root.path().join("shared.txt")).expect("remote winner can be read"),
            fs::read(local_root.path().join("shared.txt")).expect("local winner can be read")
        );
        assert_eq!(
            fs::read(local_path(remote_root.path(), conflict_path))
                .expect("remote conflict copy can be read"),
            fs::read(local_path(local_root.path(), conflict_path))
                .expect("local conflict copy can be read")
        );

        fs::remove_file(local_root.path().join("local/only.txt"))
            .expect("local file can be deleted");
        engine.sync_once().await.expect("deletion converges");
        assert!(!remote_root.path().join("local/only.txt").exists());

        fs::create_dir(local_root.path().join("switch")).expect("transition folder can be created");
        fs::write(local_root.path().join("switch/child.txt"), b"child")
            .expect("transition child can be written");
        engine.sync_once().await.expect("directory tree converges");
        fs::remove_file(local_root.path().join("switch/child.txt"))
            .expect("transition child can be deleted");
        fs::remove_dir(local_root.path().join("switch"))
            .expect("transition directory can be deleted");
        fs::write(local_root.path().join("switch"), b"now a file")
            .expect("transition file can be written");
        engine
            .sync_once()
            .await
            .expect("directory-to-file transition converges");
        assert_eq!(
            fs::read(remote_root.path().join("switch"))
                .expect("remote transition file can be read"),
            b"now a file"
        );
        assert!(!remote_root.path().join("switch/child.txt").exists());

        drop(engine);
        let restarted = SyncEngine::open(config).expect("sync engine can reopen durable state");
        let after_restart = restarted
            .sync_once()
            .await
            .expect("restart retry converges");
        assert_eq!(after_restart.local_actions, 0);
        assert_eq!(after_restart.remote_actions, 0);
        server.shutdown().await.expect("server shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sync_once_uses_partial_swarm_progress_before_v2_fallback() {
        let local_root = TempDir::new().expect("local root can be created");
        let local_state = TempDir::new().expect("local state can be created");
        let remote_root = TempDir::new().expect("remote root can be created");
        let remote_state = TempDir::new().expect("remote state can be created");
        let payload: Vec<u8> = (0..4 * 1024 * 1024)
            .map(|index| ((index * 31) ^ (index >> 5)) as u8)
            .collect();
        let remote_file = remote_root.path().join("partial.bin");
        fs::write(&remote_file, &payload).expect("remote seed file can be written");
        let seed_state = TempDir::new().expect("seed state can be created");
        let seed_store = Store::open(seed_state.path()).expect("seed store opens");
        let seed_manifest = seed_store
            .ingest_file(&remote_file, ChunkingProfile::DEFAULT)
            .expect("seed file is chunked");
        assert!(seed_manifest.chunks.len() > 1);
        let swarm_state = TempDir::new().expect("swarm state can be created");
        let swarm_destination = TempDir::new().expect("swarm destination can be created");
        {
            let store = Store::open(swarm_state.path()).expect("swarm store opens");
            let descriptor = seed_manifest.chunks.first().expect("fixture has a chunk");
            let bytes = seed_store
                .chunks()
                .read_verified(descriptor.hash)
                .expect("seed chunk readable");
            store
                .chunks()
                .put_verified(descriptor.hash, &bytes)
                .expect("partial source chunk can be stored");
        }

        let client_key = SecretKey::generate();
        let auth_server = start_server(ServerConfig {
            bind_address: Some("127.0.0.1:0".parse().unwrap()),
            max_connections: 8,
            min_free_space_bytes: 0,
            secret_key: SecretKey::generate(),
            destination_root: remote_root.path().to_path_buf(),
            state_root: remote_state.path().to_path_buf(),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
            network_mode: NetworkMode::DirectOnly,
        })
        .await
        .expect("authoritative server starts");
        let swarm_server = start_server(ServerConfig {
            bind_address: Some("127.0.0.1:0".parse().unwrap()),
            max_connections: 8,
            min_free_space_bytes: 0,
            secret_key: SecretKey::generate(),
            destination_root: swarm_destination.path().to_path_buf(),
            state_root: swarm_state.path().to_path_buf(),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
            network_mode: NetworkMode::DirectOnly,
        })
        .await
        .expect("partial swarm source starts");
        let engine = SyncEngine::open(SyncConfig {
            root: local_root.path().to_path_buf(),
            state_root: local_state.path().to_path_buf(),
            replica: replica(&client_key),
            client: SyncClient {
                secret_key: client_key,
                remote: auth_server.endpoint_addr(),
                network_mode: NetworkMode::DirectOnly,
            },
            swarm_sources: vec![swarm_server.endpoint_addr()],
            profile: ChunkingProfile::DEFAULT,
            ignored_paths: Vec::new(),
        })
        .expect("sync engine opens");

        let report = engine
            .sync_once()
            .await
            .expect("partial swarm fill and v2 fallback converge");

        assert_eq!(report.swarm_sources_used, 1);
        assert_eq!(report.verified_local_root, report.verified_remote_root);
        assert_eq!(
            fs::read(local_root.path().join("partial.bin")).expect("local file readable"),
            payload
        );
        auth_server
            .shutdown()
            .await
            .expect("auth server shuts down");
        swarm_server
            .shutdown()
            .await
            .expect("swarm server shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sync_once_stages_chunks_from_authorized_v3_swarm_sources() {
        let local_root = TempDir::new().expect("local root can be created");
        let local_state = TempDir::new().expect("local state can be created");
        let remote_root = TempDir::new().expect("remote root can be created");
        let remote_state = TempDir::new().expect("remote state can be created");

        let full_payload: Vec<u8> = (0..4 * 1024 * 1024)
            .map(|index| ((index * 31) ^ (index >> 5)) as u8)
            .collect();
        let expected_hash = Hash32::digest(&full_payload);
        let remote_file = remote_root.path().join("swarm_synced.bin");
        fs::write(&remote_file, &full_payload).expect("remote seed file can be written");
        let seed_state = TempDir::new().expect("seed state can be created");
        let seed_store = Store::open(seed_state.path()).expect("seed store opens");
        let seed_manifest = seed_store
            .ingest_file(&remote_file, ChunkingProfile::DEFAULT)
            .expect("seed file is chunked");
        assert!(seed_manifest.chunks.len() > 1);

        let client_key = SecretKey::generate();
        let auth_server = start_server(ServerConfig {
            bind_address: Some("127.0.0.1:0".parse().unwrap()),
            max_connections: 8,
            min_free_space_bytes: 0,
            secret_key: SecretKey::generate(),
            destination_root: remote_root.path().to_path_buf(),
            state_root: remote_state.path().to_path_buf(),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
            network_mode: NetworkMode::DirectOnly,
        })
        .await
        .expect("authoritative server starts");

        let mut swarm_servers = Vec::new();
        let mid = seed_manifest.chunks.len() / 2;
        let subsets = [&seed_manifest.chunks[..mid], &seed_manifest.chunks[mid..]];
        for subset in subsets {
            let state = TempDir::new().expect("swarm state can be created");
            let destination = TempDir::new().expect("swarm dest can be created");
            {
                let store = Store::open(state.path()).expect("swarm store opens");
                for descriptor in subset {
                    let bytes = seed_store
                        .chunks()
                        .read_verified(descriptor.hash)
                        .expect("seed chunk readable");
                    store
                        .chunks()
                        .put_verified(descriptor.hash, &bytes)
                        .expect("seed chunk placed in swarm source");
                }
            }
            let server = start_server(ServerConfig {
                bind_address: Some("127.0.0.1:0".parse().unwrap()),
                max_connections: 8,
                min_free_space_bytes: 0,
                secret_key: SecretKey::generate(),
                destination_root: destination.path().to_path_buf(),
                state_root: state.path().to_path_buf(),
                peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
                network_mode: NetworkMode::DirectOnly,
            })
            .await
            .expect("swarm source starts");
            swarm_servers.push((server, state, destination));
        }

        let swarm_sources: Vec<_> = swarm_servers
            .iter()
            .map(|(server, _, _)| server.endpoint_addr())
            .collect();

        let config = SyncConfig {
            root: local_root.path().to_path_buf(),
            state_root: local_state.path().to_path_buf(),
            replica: replica(&client_key),
            client: SyncClient {
                secret_key: client_key.clone(),
                remote: auth_server.endpoint_addr(),
                network_mode: NetworkMode::DirectOnly,
            },
            swarm_sources,
            profile: ChunkingProfile::DEFAULT,
            ignored_paths: Vec::new(),
        };

        // A failed swarm CAS admission must not fall back around the reserve or
        // mutate the destination. The same state must remain usable on retry.
        let rejected = SyncEngine::open_with_min_free_space(config.clone(), u64::MAX)
            .expect("reserve-limited sync engine opens");
        let error = rejected
            .sync_once()
            .await
            .expect_err("reserve rejects swarm writes");
        assert!(is_swarm_local_storage_error(&error), "{error:#}");
        assert!(!local_root.path().join("swarm_synced.bin").exists());
        assert_eq!(
            rejected.store.missing_chunks(&seed_manifest).len(),
            seed_manifest
                .chunks
                .iter()
                .map(|chunk| chunk.hash)
                .collect::<BTreeSet<_>>()
                .len()
        );
        drop(rejected);

        let engine = SyncEngine::open(config).expect("sync engine opens with swarm");
        let report = engine
            .sync_once()
            .await
            .expect("sync_once converges via swarm");
        assert_eq!(report.status, "pass");
        assert_eq!(report.pulled_remote_files, 1);
        assert!(report.pulled_bytes > 0);
        assert_eq!(report.swarm_sources_used, 2);
        assert_eq!(report.verified_local_root, report.verified_remote_root);

        let local_file = local_root.path().join("swarm_synced.bin");
        assert_eq!(
            Hash32::digest(&fs::read(&local_file).expect("file readable")),
            expected_hash
        );

        auth_server.shutdown().await.expect("auth server shut down");
        for (server, _, _) in swarm_servers {
            server.shutdown().await.expect("swarm source shut down");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn converged_sync_does_not_wait_for_unavailable_swarm_sources() {
        let local_root = TempDir::new().expect("local root can be created");
        let local_state = TempDir::new().expect("local state can be created");
        let remote_root = TempDir::new().expect("remote root can be created");
        let remote_state = TempDir::new().expect("remote state can be created");
        let stale_root = TempDir::new().expect("stale root can be created");
        let stale_state = TempDir::new().expect("stale state can be created");
        let client_key = SecretKey::generate();
        let auth_server = start_server(ServerConfig {
            bind_address: Some("127.0.0.1:0".parse().unwrap()),
            max_connections: 8,
            min_free_space_bytes: 0,
            secret_key: SecretKey::generate(),
            destination_root: remote_root.path().to_path_buf(),
            state_root: remote_state.path().to_path_buf(),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
            network_mode: NetworkMode::DirectOnly,
        })
        .await
        .expect("authoritative server starts");
        let stale_server = start_server(ServerConfig {
            bind_address: Some("127.0.0.1:0".parse().unwrap()),
            max_connections: 8,
            min_free_space_bytes: 0,
            secret_key: SecretKey::generate(),
            destination_root: stale_root.path().to_path_buf(),
            state_root: stale_state.path().to_path_buf(),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
            network_mode: NetworkMode::DirectOnly,
        })
        .await
        .expect("temporary swarm source starts");
        let stale_source = stale_server.endpoint_addr();
        stale_server
            .shutdown()
            .await
            .expect("swarm source shuts down");

        let engine = SyncEngine::open(SyncConfig {
            root: local_root.path().to_path_buf(),
            state_root: local_state.path().to_path_buf(),
            replica: replica(&client_key),
            client: SyncClient {
                secret_key: client_key,
                remote: auth_server.endpoint_addr(),
                network_mode: NetworkMode::DirectOnly,
            },
            swarm_sources: vec![stale_source],
            profile: ChunkingProfile::DEFAULT,
            ignored_paths: Vec::new(),
        })
        .expect("sync engine opens");

        let report = tokio::time::timeout(std::time::Duration::from_secs(2), engine.sync_once())
            .await
            .expect("converged sync does not wait for dead swarm source")
            .expect("converged sync succeeds");
        assert_eq!(report.local_actions, 0);
        assert_eq!(report.remote_actions, 0);
        assert_eq!(report.swarm_sources_used, 0);
        auth_server
            .shutdown()
            .await
            .expect("auth server shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cached_remote_content_does_not_wait_for_unavailable_swarm_sources() {
        let local_root = TempDir::new().expect("local root can be created");
        let local_state = TempDir::new().expect("local state can be created");
        let remote_root = TempDir::new().expect("remote root can be created");
        let remote_state = TempDir::new().expect("remote state can be created");
        let stale_root = TempDir::new().expect("stale root can be created");
        let stale_state = TempDir::new().expect("stale state can be created");
        let payload: Vec<u8> = (0..512 * 1024)
            .map(|index| ((index * 17) ^ (index >> 3)) as u8)
            .collect();
        let remote_file = remote_root.path().join("cached.bin");
        fs::write(&remote_file, &payload).expect("remote file can be written");
        {
            let cache = Store::open(local_state.path().join("store"))
                .expect("local content store can open");
            cache
                .ingest_file(&remote_file, ChunkingProfile::DEFAULT)
                .expect("remote content can be cached without a namespace record");
        }

        let client_key = SecretKey::generate();
        let auth_server = start_server(ServerConfig {
            bind_address: Some("127.0.0.1:0".parse().unwrap()),
            max_connections: 8,
            min_free_space_bytes: 0,
            secret_key: SecretKey::generate(),
            destination_root: remote_root.path().to_path_buf(),
            state_root: remote_state.path().to_path_buf(),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
            network_mode: NetworkMode::DirectOnly,
        })
        .await
        .expect("authoritative server starts");
        let stale_server = start_server(ServerConfig {
            bind_address: Some("127.0.0.1:0".parse().unwrap()),
            max_connections: 8,
            min_free_space_bytes: 0,
            secret_key: SecretKey::generate(),
            destination_root: stale_root.path().to_path_buf(),
            state_root: stale_state.path().to_path_buf(),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
            network_mode: NetworkMode::DirectOnly,
        })
        .await
        .expect("temporary swarm source starts");
        let stale_source = stale_server.endpoint_addr();
        stale_server
            .shutdown()
            .await
            .expect("swarm source shuts down");
        let engine = SyncEngine::open(SyncConfig {
            root: local_root.path().to_path_buf(),
            state_root: local_state.path().to_path_buf(),
            replica: replica(&client_key),
            client: SyncClient {
                secret_key: client_key,
                remote: auth_server.endpoint_addr(),
                network_mode: NetworkMode::DirectOnly,
            },
            swarm_sources: vec![stale_source],
            profile: ChunkingProfile::DEFAULT,
            ignored_paths: Vec::new(),
        })
        .expect("sync engine opens");

        let report = tokio::time::timeout(std::time::Duration::from_secs(2), engine.sync_once())
            .await
            .expect("cached sync does not wait for dead swarm source")
            .expect("cached sync succeeds");

        assert_eq!(report.pulled_bytes, 0);
        assert_eq!(report.swarm_sources_used, 0);
        assert_eq!(
            fs::read(local_root.path().join("cached.bin")).unwrap(),
            payload
        );
        assert_eq!(report.verified_local_root, report.verified_remote_root);
        auth_server
            .shutdown()
            .await
            .expect("auth server shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sync_once_falls_back_to_v2_when_swarm_sources_are_unavailable() {
        let local_root = TempDir::new().expect("local root can be created");
        let local_state = TempDir::new().expect("local state can be created");
        let remote_root = TempDir::new().expect("remote root can be created");
        let remote_state = TempDir::new().expect("remote state can be created");
        let payload: Vec<u8> = (0..512 * 1024)
            .map(|index| ((index * 17) ^ (index >> 3)) as u8)
            .collect();
        let expected_hash = Hash32::digest(&payload);
        fs::write(remote_root.path().join("fallback.bin"), &payload)
            .expect("remote seed file can be written");

        let client_key = SecretKey::generate();
        let auth_server = start_server(ServerConfig {
            bind_address: Some("127.0.0.1:0".parse().unwrap()),
            max_connections: 8,
            min_free_space_bytes: 0,
            secret_key: SecretKey::generate(),
            destination_root: remote_root.path().to_path_buf(),
            state_root: remote_state.path().to_path_buf(),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
            network_mode: NetworkMode::DirectOnly,
        })
        .await
        .expect("authoritative server starts");

        let swarm_state = TempDir::new().expect("swarm state can be created");
        let swarm_dest = TempDir::new().expect("swarm dest can be created");
        let swarm_server = start_server(ServerConfig {
            bind_address: Some("127.0.0.1:0".parse().unwrap()),
            max_connections: 8,
            min_free_space_bytes: 0,
            secret_key: SecretKey::generate(),
            destination_root: swarm_dest.path().to_path_buf(),
            state_root: swarm_state.path().to_path_buf(),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([SecretKey::generate().public()])),
            network_mode: NetworkMode::DirectOnly,
        })
        .await
        .expect("unauthorized swarm source starts");

        let config = SyncConfig {
            root: local_root.path().to_path_buf(),
            state_root: local_state.path().to_path_buf(),
            replica: replica(&client_key),
            client: SyncClient {
                secret_key: client_key.clone(),
                remote: auth_server.endpoint_addr(),
                network_mode: NetworkMode::DirectOnly,
            },
            swarm_sources: vec![swarm_server.endpoint_addr()],
            profile: ChunkingProfile::DEFAULT,
            ignored_paths: Vec::new(),
        };
        let engine = SyncEngine::open(config).expect("sync engine opens");
        let report = engine
            .sync_once()
            .await
            .expect("sync_once falls back to v2");
        assert_eq!(report.status, "pass");
        assert_eq!(report.swarm_sources_used, 0);
        assert_eq!(report.pulled_remote_files, 1);
        assert_eq!(
            Hash32::digest(
                &fs::read(local_root.path().join("fallback.bin")).expect("file readable")
            ),
            expected_hash
        );
        auth_server.shutdown().await.expect("auth server shut down");
        swarm_server
            .shutdown()
            .await
            .expect("swarm source shut down");
    }

    #[test]
    fn managed_stage_cleanup_keeps_same_name_replacement() {
        let temp = tempfile::tempdir().expect("stage test root");
        let state_root = temp.path().join("state");
        let stage = state_root.join(".managed-stage-test");
        std::fs::create_dir_all(&stage).expect("stage created");
        let mut stages = ManagedStages::for_root(stage.clone(), &state_root);
        if managed_stage_identity(&stage).is_none() {
            return;
        }

        let original = state_root.join(".managed-stage-original");
        std::fs::rename(&stage, &original).expect("original moved");
        std::fs::create_dir_all(&stage).expect("replacement created");
        std::fs::write(stage.join("foreign"), b"preserve").expect("replacement populated");

        stages.cleanup_owned(&state_root);
        assert!(
            stage.join("foreign").is_file(),
            "same-name replacement must not be removed"
        );
        std::fs::remove_dir_all(&stage).expect("replacement removed by test");
        std::fs::remove_dir_all(original).expect("original removed by test");
    }

    #[test]
    fn managed_stage_path_skips_existing_child_without_reusing_it() {
        let temp = tempfile::tempdir().expect("stage path test root");
        let state_root = temp.path().join("state");
        std::fs::create_dir_all(&state_root).expect("state root created");

        let first = managed_stage_path(&state_root, "unit").expect("first stage path");
        std::fs::create_dir(&first).expect("first stage created");
        let second = managed_stage_path(&state_root, "unit").expect("second stage path");
        assert_ne!(first, second, "an existing stage must never be reused");
        create_managed_stage_directory(&second).expect("second stage created");
        assert!(first.is_dir());
        assert!(second.is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn managed_stage_path_rejects_replaced_parent_alias() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("stage alias test root");
        let admitted_root = temp.path().join("admitted");
        let outside_root = temp.path().join("outside");
        let alias = temp.path().join("state");
        std::fs::create_dir_all(&admitted_root).expect("admitted root created");
        std::fs::create_dir_all(&outside_root).expect("outside root created");
        symlink(&outside_root, &alias).expect("parent alias created");

        assert!(managed_stage_path(&alias, "unit").is_err());
        assert!(
            outside_root
                .read_dir()
                .expect("outside root readable")
                .next()
                .is_none()
        );
    }

    #[test]
    fn managed_rw_journal_v1_stage_paths_migrate_without_ownership_claim() {
        let legacy = LegacyManagedRwJournalV1 {
            version: 1,
            owner: [1; 32],
            share: [2; 32],
            stage_roots: vec![PathBuf::from("state/.managed-stage-old")],
            apply: None,
        };
        let bytes = postcard::to_stdvec(&legacy).expect("legacy journal encoding");
        let decoded = decode_managed_rw_journal(&bytes).expect("legacy journal decoding");
        assert_eq!(decoded.version, MANAGED_RW_JOURNAL_VERSION);
        assert_eq!(decoded.stage_roots, legacy.stage_roots);
        assert_eq!(decoded.stage_identities, vec![None]);
        let mut trailing = bytes;
        trailing.push(0xa5);
        assert!(decode_managed_rw_journal(&trailing).is_err());
        assert!(decoded.apply.is_none());

        let first = LegacyManagedRwJournal {
            version: 1,
            owner: [3; 32],
            share: [4; 32],
            stage_root: Some(PathBuf::from("state/.managed-stage-first")),
            apply: None,
        };
        let bytes = postcard::to_stdvec(&first).expect("first journal encoding");
        let decoded = decode_managed_rw_journal(&bytes).expect("first journal decoding");
        assert_eq!(
            decoded.stage_roots,
            first.stage_root.into_iter().collect::<Vec<_>>()
        );
        assert_eq!(decoded.stage_identities, vec![None]);
    }

    #[test]
    fn managed_rw_journal_v1_apply_some_uses_legacy_shape() {
        let owner = SecretKey::generate();
        let consumer = SecretKey::generate().public();
        let permit = ApplyPermit {
            version: 1,
            owner: owner.public(),
            share: ShareId([3; 32]),
            consumer,
            epoch: 1,
            snapshot: [4; 32],
            root_hash: Hash32::digest(b"legacy-apply"),
            issued_at: 1,
            expires_at: 2,
            nonce: [5; 32],
            signature: iroh::Signature::from_bytes(&[0; 64]),
        };
        let legacy = LegacyManagedRwJournalV1 {
            version: 1,
            owner: [6; 32],
            share: [7; 32],
            stage_roots: vec![PathBuf::from("state/.managed-stage-apply")],
            apply: Some(ManagedApplyJournal {
                permit,
                operation_id: [8; 16],
                committed: true,
            }),
        };
        let bytes = postcard::to_stdvec(&legacy).expect("legacy apply journal encoding");
        let decoded = decode_managed_rw_journal(&bytes).expect("legacy apply journal decoding");
        assert_eq!(decoded.version, MANAGED_RW_JOURNAL_VERSION);
        assert_eq!(decoded.stage_roots, legacy.stage_roots);
        assert_eq!(decoded.stage_identities, vec![None]);
        assert_eq!(decoded.apply, legacy.apply);

        let mut trailing = bytes;
        trailing.push(0x5a);
        assert!(decode_managed_rw_journal(&trailing).is_err());
    }

    #[test]
    fn managed_stage_reference_drops_only_explicit_not_found() {
        let temp = tempfile::tempdir().expect("stage retention root");
        let state_root = temp.path().join("state");
        std::fs::create_dir_all(&state_root).expect("state root created");
        let blocking_parent = state_root.join("blocking-file");
        std::fs::write(&blocking_parent, b"not a directory").expect("blocking parent created");
        let not_a_directory = blocking_parent.join("child");
        let missing = state_root.join(".managed-stage-missing");
        let mut journal = ManagedRwJournal {
            version: MANAGED_RW_JOURNAL_VERSION,
            owner: [1; 32],
            share: [2; 32],
            stage_roots: vec![not_a_directory.clone(), missing],
            stage_identities: vec![None, None],
            apply: None,
        };

        retain_existing_stage_roots(&mut journal);

        assert_eq!(journal.stage_roots, vec![not_a_directory]);
        assert_eq!(journal.stage_identities, vec![None]);
    }

    #[test]
    fn managed_stage_merge_preserves_uncertain_previous_reference() {
        let mut journal = ManagedRwJournal {
            version: MANAGED_RW_JOURNAL_VERSION,
            owner: [1; 32],
            share: [2; 32],
            stage_roots: vec![PathBuf::from("state/.managed-stage-uncertain")],
            stage_identities: vec![None],
            apply: None,
        };
        let current_root = PathBuf::from("state/.managed-stage-current");
        let mut stages = ManagedStages::default();
        stages.roots.push(current_root.clone());

        merge_managed_stage_roots(&mut journal, &stages).expect("stage roots merge");

        assert_eq!(
            journal.stage_roots,
            vec![
                PathBuf::from("state/.managed-stage-uncertain"),
                current_root
            ]
        );
        assert_eq!(journal.stage_identities, vec![None, None]);
    }
}
