//! Authoritative RO application: local vectors never become owner history.
use super::*;
use deltaweave_core::CausalRelation;
use deltaweave_net::share::{Membership, ShareError, ShareSession};
use deltaweave_store::{PathChangeState, PathObservation, PathTarget};
use serde::{Deserialize, Serialize};

const READ_ONLY_STATE_VERSION: u16 = 2;

/// A retained local object. Late writes through old handles remain attached to this path.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PreservedLocalChange {
    /// Original portable path.
    pub path: WirePath,
    /// Unique Store journal attempt.
    pub operation_id: String,
    /// Actual durable same-volume object outside all admitted public roots.
    pub preserved_path: PathBuf,
}

/// A verified authoritative RO round and its discoverable recovery locations.
#[derive(Clone, Debug, Serialize)]
pub struct ReadOnlyReport {
    /// Emitted only after fresh local and owner verification.
    pub status: &'static str,
    /// Authoritative owner Merkle root.
    pub owner_root: Hash32,
    /// Root of the exact adopted local records.
    pub verified_local_root: Hash32,
    /// Downloaded payload bytes, excluding reused extents.
    pub pulled_bytes: u64,
    /// Retained local objects from this and interrupted/earlier rounds.
    pub preserved: Vec<PreservedLocalChange>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ReadOnlyState {
    version: u16,
    owner: [u8; 32],
    share: [u8; 32],
    checkpoint: Vec<SyncRecord>,
    pending: Option<Pending>,
    /// Durable local admission record.  It is written before ApplyStart and
    /// retained until the owner accepts the matching ApplyDrained message.
    #[serde(default)]
    apply: Option<ManagedApplyJournal>,
}
/// The retained RO envelope before managed apply/journal fields were added.
/// Postcard is positional, so `serde(default)` on the current type is not a
/// sufficient compatibility guarantee for an existing `pending` value.  Keep
/// this decoder local and convert the old value without rewriting it until a
/// subsequent successful state save.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct LegacyReadOnlyState {
    version: u16,
    owner: [u8; 32],
    share: [u8; 32],
    checkpoint: Vec<SyncRecord>,
    pending: Option<LegacyPending>,
}
/// Managed RO state as persisted between the first E3 journal checkpoint and
/// the identity-bound v2 envelope.  It already contains `stage_root` and the
/// ApplyStart journal, so it must be decoded separately instead of relying on
/// postcard's positional `serde(default)` behavior.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct LegacyReadOnlyStateWithStage {
    version: u16,
    owner: [u8; 32],
    share: [u8; 32],
    checkpoint: Vec<SyncRecord>,
    pending: Option<LegacyPendingWithStage>,
    apply: Option<ManagedApplyJournal>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
struct LegacyPendingWithStage {
    desired: Vec<SyncRecord>,
    attempts: Vec<String>,
    stage: Stage,
    stage_root: Option<PathBuf>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
struct LegacyPending {
    desired: Vec<SyncRecord>,
    attempts: Vec<String>,
    stage: Stage,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
struct Pending {
    desired: Vec<SyncRecord>,
    attempts: Vec<String>,
    stage: Stage,
    /// Exact managed stage root retained across an interrupted apply.  It is
    /// recorded before provider/CAS work; a retry may allocate a new unique
    /// root only after this one has been proven absent or safely cleaned.
    #[serde(default)]
    stage_root: Option<PathBuf>,
    /// Filesystem identity captured before any managed provider or CAS IO.
    /// A missing identity belongs to an older envelope and is retained
    /// fail-closed rather than used for deletion after a same-name replace.
    stage_identity: Option<super::ManagedStageIdentity>,
}
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum Stage {
    Prepared,
    Preserved,
    Materialized,
    Adopted,
}

pub(crate) fn initialize(local: &ReplicaState, member: &Membership) -> Result<()> {
    if let Some(bytes) = local.index.share_metadata()? {
        let state = decode_state(&bytes)?;
        ensure!(
            state.version == READ_ONLY_STATE_VERSION
                && state.owner == *member.owner.as_bytes()
                && state.share == member.share_id.0,
            ShareError::StateUnavailable
        );
    } else {
        // This slot is set before the first scan, including a legitimate retained-index import.
        save(
            local,
            &ReadOnlyState {
                version: READ_ONLY_STATE_VERSION,
                owner: *member.owner.as_bytes(),
                share: member.share_id.0,
                checkpoint: Vec::new(),
                pending: None,
                apply: None,
            },
        )?;
    }
    Ok(())
}
fn load(local: &ReplicaState) -> Result<ReadOnlyState> {
    let bytes = local
        .index
        .share_metadata()?
        .context(ShareError::StateUnavailable)?;
    decode_state(&bytes)
}

fn decode_state(bytes: &[u8]) -> Result<ReadOnlyState> {
    if let Ok(state) = super::decode_exact_postcard::<ReadOnlyState>(bytes)
        && state.version == READ_ONLY_STATE_VERSION
    {
        return Ok(state);
    }
    if let Ok(legacy) = super::decode_exact_postcard::<LegacyReadOnlyStateWithStage>(bytes)
        && legacy.version == 1
    {
        return Ok(ReadOnlyState {
            version: READ_ONLY_STATE_VERSION,
            owner: legacy.owner,
            share: legacy.share,
            checkpoint: legacy.checkpoint,
            pending: legacy.pending.map(|pending| Pending {
                desired: pending.desired,
                attempts: pending.attempts,
                stage: pending.stage,
                stage_root: pending.stage_root,
                stage_identity: None,
            }),
            apply: legacy.apply,
        });
    }
    if let Ok(legacy) = super::decode_exact_postcard::<LegacyReadOnlyState>(bytes)
        && legacy.version == 1
    {
        return Ok(ReadOnlyState {
            version: READ_ONLY_STATE_VERSION,
            owner: legacy.owner,
            share: legacy.share,
            checkpoint: legacy.checkpoint,
            pending: legacy.pending.map(|pending| Pending {
                desired: pending.desired,
                attempts: pending.attempts,
                stage: pending.stage,
                stage_root: None,
                stage_identity: None,
            }),
            apply: None,
        });
    }
    Err(ShareError::StateUnavailable.into())
}
fn save(local: &ReplicaState, state: &ReadOnlyState) -> Result<()> {
    local.index.set_share_metadata(&postcard::to_stdvec(state)?)
}

/// Removes a completed RO stage only when the persisted identity still names
/// the exact directory created by this managed round.  A missing identity or
/// an uncertain filesystem error keeps the Pending record as the recovery
/// reference and fails closed; clearing the envelope first would orphan the
/// private stage or make a same-name replacement eligible for deletion.
fn cleanup_pending_stage(local: &ReplicaState, state: &mut ReadOnlyState) -> Result<bool> {
    let Some(pending) = state.pending.as_mut() else {
        return Ok(true);
    };
    let Some(stage_root) = pending.stage_root.clone() else {
        return Ok(true);
    };
    let mut stages = super::ManagedStages::for_recovery_root_with_identity(
        stage_root.clone(),
        pending.stage_identity,
        local.store.state_root(),
    );
    stages.cleanup_owned(local.store.state_root());
    match fs::symlink_metadata(&stage_root) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            pending.stage_root = None;
            pending.stage_identity = None;
            Ok(true)
        }
        Ok(_) => Ok(false),
        Err(_) => Err(ShareError::StateUnavailable.into()),
    }
}

/// Replays only an already-durable managed ApplyStart.  The caller invokes
/// this before heartbeat/new owner admission so paused or revoked owners can
/// still close an exact receipt without opening a fresh public mutation.
pub(crate) async fn recover_managed_apply_before_liveness(
    local: &ReplicaState,
    session: &ShareSession,
) -> Result<()> {
    let mut state = load(local)?;
    let Some(journal) = state.apply.clone() else {
        return Ok(());
    };
    super::recover_managed_apply(session, &journal).await?;
    state.apply = None;
    save(local, &state)
}

fn validate_checkpoint(before: &[SyncRecord], after: &[SyncRecord]) -> Result<()> {
    let current: BTreeMap<_, _> = after.iter().map(|r| (&r.path, r)).collect();
    ensure!(current.len() == after.len(), ShareError::InvalidRecord);
    validate_materializable_namespace(after)?;
    for previous in before {
        let next = current
            .get(&previous.path)
            .context(ShareError::InvalidRecord)?;
        match previous.version.relation(&next.version) {
            CausalRelation::Equal => ensure!(previous == *next, ShareError::InvalidRecord),
            CausalRelation::Before => {}
            CausalRelation::After | CausalRelation::Concurrent => bail!(ShareError::InvalidRecord),
        }
    }
    Ok(())
}

pub(crate) fn preserved(local: &ReplicaState) -> Result<Vec<PreservedLocalChange>> {
    let mut preserved = Vec::new();
    for change in local
        .store
        .path_changes()?
        .into_iter()
        .filter(|c| c.root == local.root)
    {
        // `artifact` is the displaced local object. `rollback_artifact` is
        // retained incoming owner data after an unadopted rollback; exposing
        // it here would mislabel remote bytes as a local edit/conflict. Keep
        // that incoming object durable in Store for recovery, but report only
        // the user's displaced artifact through the RO API.
        if fs::symlink_metadata(&change.artifact).is_ok() {
            preserved.push(PreservedLocalChange {
                path: change.path.clone(),
                operation_id: change.id.clone(),
                preserved_path: change.artifact.clone(),
            });
        }
    }
    Ok(preserved)
}

fn physical_equal(left: &SyncRecord, right: &SyncRecord) -> bool {
    left.tombstone == right.tombstone
        && left.kind == right.kind
        && left.content_hash == right.content_hash
        && left.size == right.size
        && left.readonly == right.readonly
}

fn target_matches_record(change: &deltaweave_store::PathChange, record: &SyncRecord) -> bool {
    if record.path != change.path {
        return false;
    }
    match (&change.target, record.tombstone, record.kind) {
        (PathTarget::Absent, true, _) => true,
        (PathTarget::Directory, false, SyncEntryKind::Directory) => true,
        (PathTarget::File(manifest), false, SyncEntryKind::File) => {
            manifest.file_hash == record.content_hash.unwrap_or_default()
                && manifest.size == record.size
        }
        _ => false,
    }
}

fn owner_record<'a>(records: &'a [SyncRecord], path: &WirePath) -> Option<&'a SyncRecord> {
    records.iter().find(|record| &record.path == path)
}

fn adopt_recovered_change(
    local: &ReplicaState,
    change: &deltaweave_store::PathChange,
    record: &SyncRecord,
) -> Result<()> {
    ensure!(
        change.state == PathChangeState::Materialized,
        ShareError::StateUnavailable
    );
    ensure!(
        target_matches_record(change, record),
        ShareError::ManifestMismatch
    );
    match &change.target {
        PathTarget::File(_) => {
            let observation = local.store.observe_path_change(change)?;
            local
                .index
                .adopt_materialized_record(record, &observation)?;
        }
        PathTarget::Directory | PathTarget::Absent => {
            local.index.adopt_verified_record(record)?;
        }
    }
    local.store.mark_path_change_indexed(&change.id)
}

async fn begin_apply(
    local: &ReplicaState,
    session: &ShareSession,
    state: &mut ReadOnlyState,
    snapshot: &deltaweave_net::share::SnapshotToken,
    prefix: &[u8],
    round: usize,
) -> Result<(deltaweave_net::share::ApplyPermit, [u8; 16], Instant)> {
    let started = Instant::now();
    let permit = session.revalidate_before_apply(snapshot).await?;
    let deadline = started + super::MANAGED_APPLY_TTL;
    super::ensure_managed_deadline(deadline)?;
    let operation_id = super::managed_operation_id(prefix, snapshot, round);
    state.apply = Some(ManagedApplyJournal {
        permit: permit.clone(),
        operation_id,
        committed: false,
    });
    // The exact permit and operation are durable before ApplyStart.  A
    // restart can therefore close an uncertain owner row without inventing a
    // new operation or silently dropping a local writer.
    save(local, state)?;
    session.apply_start(&permit, operation_id).await?;
    Ok((permit, operation_id, deadline))
}

async fn finish_apply(
    local: &ReplicaState,
    session: &ShareSession,
    state: &mut ReadOnlyState,
    permit: &deltaweave_net::share::ApplyPermit,
    operation_id: [u8; 16],
    committed: bool,
) -> Result<()> {
    if let Some(apply) = state.apply.as_mut() {
        ensure!(
            apply.operation_id == operation_id && apply.permit == *permit,
            ShareError::GrantReplay
        );
        apply.committed = committed;
    } else {
        state.apply = Some(ManagedApplyJournal {
            permit: permit.clone(),
            operation_id,
            committed,
        });
    }
    save(local, state)?;
    session
        .apply_drained(permit, operation_id, committed)
        .await?;
    state.apply = None;
    save(local, state)
}

pub(crate) async fn sync(
    local: &Arc<ReplicaState>,
    session: &ShareSession,
    observer: &Option<TransferObserver>,
) -> Result<ReadOnlyReport> {
    let mut state = load(local)?;
    // Finish the exact previous ApplyStart before requesting a new owner
    // snapshot.  A revoked/paused owner may reject fresh data admission while
    // still allowing this receipt/drain recovery; reversing these operations
    // would strand the durable local writer journal forever.
    if let Some(journal) = state.apply.clone() {
        super::recover_managed_apply(session, &journal).await?;
        state.apply = None;
        save(local, &state)?;
    }
    // The managed path is owner-authoritative. On an already-initialized
    // enrollment, perform the old share/3 snapshot exchange only as a
    // compatibility *probe*: it can reject a peer that still serves a legacy
    // unsigned snapshot before we open the managed control exchange, but its
    // records are never used for planning, staging, or index adoption. This
    // also lets an authenticated old peer finish its stream cleanly while the
    // managed path reports the required fail-closed record-integrity result.
    // An empty checkpoint has no prior history to validate, so it goes
    // straight to the signed managed snapshot.
    let empty = MerkleTree::from_records(Vec::new())?;
    if !state.checkpoint.is_empty() {
        let legacy = session.fetch_snapshot(&empty).await?;
        validate_checkpoint(&state.checkpoint, &legacy.records)?;
    }
    let authoritative = match session.fetch_authoritative_snapshot(&empty).await {
        Ok(snapshot) => snapshot,
        // A peer that answers the old V3 snapshot shape cannot be accepted as
        // an authoritative managed owner. Keep the fail-closed result in the
        // stable record-integrity class used by the existing admission API.
        Err(error) if ShareError::classify(&error) == ShareError::Protocol => {
            return Err(ShareError::InvalidRecord.into());
        }
        Err(error) => return Err(error),
    };
    let remote = authoritative.records.clone();
    validate_checkpoint(&state.checkpoint, &remote)?;
    let owner_tree = MerkleTree::from_records(remote.clone())?;
    local.observe(observer, "peer_seen", None, None, 0);

    // Pending RO changes resume only after this current owner snapshot has
    // been authenticated and admitted. A stale owner target uses the narrow
    // no-promotion rollback helper and keeps both objects on drift.
    let mut pending_ids = Vec::new();
    if let Some(pending) = state.pending.as_ref() {
        for attempt in &pending.attempts {
            if !pending_ids.iter().any(|existing| existing == attempt) {
                pending_ids.push(attempt.clone());
            }
        }
    }
    // Store preparation and the state-envelope update are separate durable
    // operations.  If a process dies in that narrow interval, the path
    // change has no causal binding but is still an owned managed attempt.  On
    // the next authenticated round, attach such nonterminal rows to the
    // pending journal before any scan can promote their materialized target
    // as a fresh local edit.  The owner snapshot remains the authority for
    // whether the attempt is resumed or rolled back.
    let orphan_ids: Vec<_> = local
        .store
        .path_changes()?
        .into_iter()
        .filter(|change| {
            change.root == local.root
                && change.causal.is_none()
                && matches!(
                    change.state,
                    PathChangeState::Prepared
                        | PathChangeState::Preserved
                        | PathChangeState::Materialized
                        | PathChangeState::RollingBack
                )
        })
        .map(|change| change.id)
        .collect();
    if !orphan_ids.is_empty() {
        let pending = state.pending.get_or_insert_with(|| Pending {
            desired: remote.clone(),
            attempts: Vec::new(),
            stage: Stage::Prepared,
            stage_root: None,
            stage_identity: None,
        });
        for id in orphan_ids {
            if !pending_ids.iter().any(|existing| existing == &id) {
                pending_ids.push(id.clone());
                pending.attempts.push(id);
            }
        }
        save(local, &state)?;
    }
    if !pending_ids.is_empty() {
        let all_changes: Vec<_> = local
            .store
            .path_changes()?
            .into_iter()
            .filter(|change| {
                change.root == local.root && pending_ids.iter().any(|attempt| attempt == &change.id)
            })
            .collect();
        ensure!(
            all_changes.len() == pending_ids.len(),
            ShareError::StateUnavailable
        );
        let mut recovery_changes = Vec::new();
        let mut completed_ids = Vec::new();
        for change in all_changes {
            if matches!(
                change.state,
                PathChangeState::Indexed
                    | PathChangeState::Committed
                    | PathChangeState::RolledBack
                    | PathChangeState::Aborted
            ) {
                completed_ids.push(change.id);
            } else {
                recovery_changes.push(change);
            }
        }
        if !recovery_changes.is_empty() {
            let (permit, operation_id, deadline) = begin_apply(
                local,
                session,
                &mut state,
                &authoritative.token,
                b"managed-ro-recovery",
                0,
            )
            .await?;
            for mut change in recovery_changes {
                super::ensure_managed_deadline(deadline)?;
                let target = owner_record(&remote, &change.path);
                if change.state == PathChangeState::RollingBack {
                    // A prior stale-target recovery has already published the
                    // rollback write-ahead state.  Finish it before considering
                    // the current owner target; never adopt from a halfway
                    // rollback after restart.
                    local.store.rollback_unadopted_path_change(&mut change)?;
                    ensure!(
                        change.state == PathChangeState::RolledBack,
                        ShareError::StateUnavailable
                    );
                } else if target.is_some_and(|record| target_matches_record(&change, record)) {
                    local.store.resume_path_change(&mut change)?;
                    let record = target.context(ShareError::StateUnavailable)?;
                    adopt_recovered_change(local, &change, record)?;
                } else {
                    ensure!(change.causal.is_none(), ShareError::StateUnavailable);
                    local.store.rollback_unadopted_path_change(&mut change)?;
                    ensure!(
                        change.state == PathChangeState::RolledBack,
                        ShareError::StateUnavailable
                    );
                }
                completed_ids.push(change.id);
            }
            if let Some(pending) = state.pending.as_mut() {
                pending
                    .attempts
                    .retain(|attempt| !completed_ids.iter().any(|done| done == attempt));
                if pending.attempts.is_empty() {
                    pending.stage = Stage::Adopted;
                }
            }
            save(local, &state)?;
            finish_apply(local, session, &mut state, &permit, operation_id, true).await?;
        } else if let Some(pending) = state.pending.as_mut() {
            pending
                .attempts
                .retain(|attempt| !completed_ids.iter().any(|done| done == attempt));
            pending.stage = Stage::Adopted;
            save(local, &state)?;
        }
    }
    if state
        .pending
        .as_ref()
        .is_some_and(|pending| pending.attempts.is_empty())
    {
        ensure!(
            cleanup_pending_stage(local, &mut state)?,
            ShareError::StateUnavailable
        );
        state.pending = None;
        save(local, &state)?;
    }

    let scan = scan_index(local.index.clone()).await?;
    ensure_scan_is_safe(&scan, "read-only local")?;
    let current = read_records(local.index.clone()).await?;
    let current_tree = MerkleTree::from_records(current.clone())?;
    let current_by_path: BTreeMap<_, _> = current.iter().map(|r| (r.path.clone(), r)).collect();
    let remote_by_path: BTreeMap<_, _> = remote.iter().map(|r| (r.path.clone(), r)).collect();
    let mut changes: BTreeMap<WirePath, (Option<PathObservation>, Option<SyncRecord>)> =
        BTreeMap::new();
    for record in current.iter().filter(|r| !r.tombstone) {
        if remote_by_path.get(&record.path).is_none_or(|r| r.tombstone) {
            changes.insert(
                record.path.clone(),
                (PathObservation::read(&local.root, &record.path)?, None),
            );
        }
    }
    for record in remote.iter().filter(|r| !r.tombstone) {
        let previous = current_by_path.get(&record.path).filter(|r| !r.tombstone);
        if previous.is_some_and(|r| physical_equal(r, record)) {
            continue;
        }
        if record.kind == SyncEntryKind::Directory
            && previous.is_some_and(|r| r.kind == SyncEntryKind::Directory)
        {
            continue;
        }
        let expected = if previous.is_some() {
            PathObservation::read(&local.root, &record.path)?
        } else {
            None
        };
        changes.insert(record.path.clone(), (expected, Some(record.clone())));
    }
    // Capturing a complete directory preserves local-only descendants once.
    let paths: Vec<_> = changes.keys().cloned().collect();
    for path in paths {
        let ancestors: Vec<_> = path.components().collect();
        for end in 1..ancestors.len() {
            if changes
                .get(&WirePath::new(ancestors[..end].join("/"))?)
                .is_some_and(|(_, target)| {
                    target
                        .as_ref()
                        .is_none_or(|r| r.kind == SyncEntryKind::File)
                })
            {
                changes.remove(&path);
                break;
            }
        }
    }
    let files: Vec<_> = changes
        .values()
        .filter_map(|(_, target)| target.as_ref())
        .filter(|r| r.kind == SyncEntryKind::File)
        .cloned()
        .collect();
    let pending_bytes = files.iter().try_fold(0_u64, |sum, r| {
        sum.checked_add(r.size)
            .context("pending RO byte count overflow")
    })?;
    let old_attempts = state
        .pending
        .as_ref()
        .map_or_else(Vec::new, |pending| pending.attempts.clone());
    if !changes.is_empty() {
        state.pending = Some(Pending {
            desired: remote.clone(),
            attempts: old_attempts,
            stage: Stage::Prepared,
            stage_root: None,
            stage_identity: None,
        });
        // The marker is durable before reserving any stage directory.  The
        // stage hook below adds the exact directory identity before provider
        // requests or CAS materialization begin.
        save(local, &state)?;
    }
    let (mut staged, stats, marked_state) = {
        // `stage_managed_files` runs inside an owned managed task. Keep the
        // marker state in an owned cell so cancellation cannot leave a root
        // created by this round without its durable Pending reference.
        let state_cell = Arc::new(std::sync::Mutex::new(state.clone()));
        let marker_cell = Arc::clone(&state_cell);
        let index = Arc::clone(&local.index);
        let stage_marker: Arc<super::ManagedStageMarker> = Arc::new(move |root, identity| {
            let mut marked = marker_cell
                .lock()
                .map_err(|_| anyhow::anyhow!("managed RO journal lock poisoned"))?;
            let pending = marked
                .pending
                .as_mut()
                .context(ShareError::StateUnavailable)?;
            if let Some(existing) = &pending.stage_root {
                // The stage helper records the selected path before mkdir and
                // upgrades that same row with the post-mkdir identity. A
                // different path would indicate a corrupted/reused journal.
                ensure!(existing == root, ShareError::StateUnavailable);
                if identity.is_some() {
                    pending.stage_identity = identity;
                }
            } else {
                pending.stage_root = Some(root.to_path_buf());
                pending.stage_identity = identity;
            }
            index.set_share_metadata(&postcard::to_stdvec(&*marked)?)
        });
        let result = local
            .stage_managed_files(
                session,
                &files,
                &current,
                &remote,
                pending_bytes,
                &authoritative,
                false,
                observer,
                Some(stage_marker.clone()),
            )
            .await?;
        drop(stage_marker);
        let marked_state = Arc::try_unwrap(state_cell)
            .map_err(|_| anyhow::anyhow!("managed RO journal marker still active"))?
            .into_inner()
            .map_err(|_| anyhow::anyhow!("managed RO journal lock poisoned"))?;
        (result.0, result.1, marked_state)
    };
    state = marked_state;

    // A no-op only advances the private checkpoint. Local-only edits remain
    // local index state and never become owner history.
    if changes.is_empty() && current_tree.root_hash() == owner_tree.root_hash() {
        state.checkpoint = remote.clone();
        state.pending = None;
        save(local, &state)?;
        return Ok(ReadOnlyReport {
            status: "pass",
            owner_root: owner_tree.root_hash(),
            verified_local_root: current_tree.root_hash(),
            pulled_bytes: stats.pulled_bytes,
            preserved: preserved(local)?,
        });
    }

    ensure!(state.pending.is_some(), ShareError::StateUnavailable);

    // The monotonic deadline starts before Revalidate and cannot be renewed by
    // a delayed response. All public filesystem, readonly, and index writes
    // are below this operation's ApplyStart.
    let (permit, operation_id, deadline) = begin_apply(
        local,
        session,
        &mut state,
        &authoritative.token,
        b"managed-ro-apply",
        0,
    )
    .await?;
    local.observe(observer, "ro_prepared", None, None, 0);
    let mut changes: Vec<_> = changes.into_iter().collect();
    changes.sort_by_key(|(path, _)| path_depth(path));
    let mut materialized_ids = Vec::new();
    for (path, (expected, desired)) in changes {
        super::ensure_managed_deadline(deadline)?;
        let target = match desired {
            Some(record) if record.kind == SyncEntryKind::File => {
                let manifest = staged
                    .manifests
                    .get(&record.content_hash.context("file has no hash")?)
                    .context("required RO content not staged")?
                    .clone();
                DiskAdmission::new(
                    local.store.state_root().to_path_buf(),
                    local.root.clone(),
                    local.min_free_space_bytes,
                    0,
                )
                .check_materialization(manifest.size)?;
                PathTarget::File(manifest)
            }
            Some(_) => PathTarget::Directory,
            None => PathTarget::Absent,
        };
        let mut change =
            local
                .store
                .prepare_path_change(&local.root, &path, target, expected, true)?;
        state
            .pending
            .as_mut()
            .context("pending journal missing")?
            .attempts
            .push(change.id.clone());
        save(local, &state)?;
        local.observe(observer, "ro_path_prepared", Some(&path), None, 0);
        super::ensure_managed_deadline(deadline)?;
        local.store.capture_path_change(&mut change)?;
        state
            .pending
            .as_mut()
            .context("pending journal missing")?
            .stage = Stage::Preserved;
        save(local, &state)?;
        local.observe(observer, "ro_preserved", Some(&path), None, 0);
        super::ensure_managed_deadline(deadline)?;
        local.store.materialize_path_change(&mut change)?;
        materialized_ids.push(change.id.clone());
    }
    for record in remote.iter().filter(|r| !r.tombstone) {
        super::ensure_managed_deadline(deadline)?;
        local
            .store
            .set_readonly(&local.root, &record.path, record.readonly)?;
    }
    state
        .pending
        .as_mut()
        .context("pending journal missing")?
        .stage = Stage::Materialized;
    save(local, &state)?;
    local.observe(observer, "ro_materialized", None, None, 0);
    state.checkpoint = remote.clone();
    state
        .pending
        .as_mut()
        .context("pending journal missing")?
        .stage = Stage::Adopted;
    super::ensure_managed_deadline(deadline)?;
    local
        .index
        .adopt_authoritative_snapshot(&remote, &postcard::to_stdvec(&state)?)?;
    local.observe(observer, "ro_adopted", None, None, 0);
    // Adopt only attempts created by this admitted round.  An unrelated
    // retained Materialized row must remain recoverable instead of being
    // silently promoted by a broad state scan.
    for id in materialized_ids {
        local.store.mark_path_change_indexed(&id)?;
    }

    let verification = scan_index(local.index.clone()).await?;
    ensure_scan_is_safe(&verification, "verified read-only")?;
    let verified = MerkleTree::from_records(read_records(local.index.clone()).await?)?;
    let fresh_owner = session.fetch_authoritative_snapshot(&empty).await?;
    let fresh_owner_tree = MerkleTree::from_records(fresh_owner.records)?;
    if verified.root_hash() != owner_tree.root_hash()
        || fresh_owner_tree.root_hash() != owner_tree.root_hash()
    {
        // Keep Pending/apply state durable. A later round must be admitted
        // again rather than treating an owner race as success.
        let _ = finish_apply(local, session, &mut state, &permit, operation_id, false).await;
        bail!(ShareError::ManifestMismatch);
    }
    finish_apply(local, session, &mut state, &permit, operation_id, true).await?;
    staged.cleanup_owned(local.store.state_root());
    ensure!(
        cleanup_pending_stage(local, &mut state)?,
        ShareError::StateUnavailable
    );
    state.pending = None;
    state.checkpoint = remote.clone();
    save(local, &state)?;
    Ok(ReadOnlyReport {
        status: "pass",
        owner_root: owner_tree.root_hash(),
        verified_local_root: verified.root_hash(),
        pulled_bytes: stats.pulled_bytes,
        preserved: preserved(local)?,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use deltaweave_core::{SYNC_RECORD_SCHEMA_V1, VersionVector};
    use iroh::{SecretKey, Signature};
    fn record(bytes: &[u8], counter: u64) -> SyncRecord {
        let mut version = VersionVector::default();
        version.observe(ReplicaId(Hash32::digest(b"owner")), counter);
        SyncRecord {
            schema_version: SYNC_RECORD_SCHEMA_V1,
            path: WirePath::new("file").unwrap(),
            kind: SyncEntryKind::File,
            size: bytes.len() as u64,
            content_hash: Some(Hash32::digest(bytes)),
            readonly: false,
            version,
            tombstone: false,
        }
    }
    #[test]
    fn trusted_owner_checkpoint_rejects_rollback_divergence_and_omission() {
        let checkpoint = vec![record(b"trusted", 9)];
        for next in [vec![record(b"old", 8)], vec![record(b"forged", 9)], vec![]] {
            assert!(validate_checkpoint(&checkpoint, &next).is_err());
        }
        validate_checkpoint(&checkpoint, &[record(b"new", 10)]).unwrap();
        let mut tombstone = record(b"trusted", 10);
        tombstone.tombstone = true;
        validate_checkpoint(&checkpoint, &[tombstone.clone()]).unwrap();
        assert!(validate_checkpoint(&[tombstone], &[]).is_err());
    }

    #[test]
    fn legacy_v1_pending_some_decodes_without_loss() {
        let desired = vec![record(b"legacy", 4)];
        let attempts = vec!["legacy-operation".to_owned()];
        let legacy = LegacyReadOnlyState {
            version: 1,
            owner: [7; 32],
            share: [9; 32],
            checkpoint: vec![record(b"checkpoint", 3)],
            pending: Some(LegacyPending {
                desired: desired.clone(),
                attempts: attempts.clone(),
                stage: Stage::Preserved,
            }),
        };
        let bytes = postcard::to_stdvec(&legacy).expect("legacy fixture encoding");
        let decoded = decode_state(&bytes).expect("legacy fixture decoding");
        assert_eq!(decoded.version, READ_ONLY_STATE_VERSION);
        assert_eq!(decoded.owner, legacy.owner);
        assert_eq!(decoded.share, legacy.share);
        assert_eq!(decoded.checkpoint, legacy.checkpoint);
        let pending = decoded.pending.expect("legacy pending retained");
        assert_eq!(pending.desired, desired);
        assert_eq!(pending.attempts, attempts);
        assert_eq!(pending.stage, Stage::Preserved);
        assert_eq!(pending.stage_root, None);
        assert_eq!(pending.stage_identity, None);
        assert!(decoded.apply.is_none());
        let mut trailing = bytes;
        trailing.push(0xa5);
        assert!(decode_state(&trailing).is_err());
    }

    #[test]
    fn managed_v1_stage_root_migrates_without_dropping_pending() {
        let desired = vec![record(b"legacy-managed", 4)];
        let stage_root = PathBuf::from("state/.managed-stage-old");
        let owner = SecretKey::generate();
        let apply = ManagedApplyJournal {
            permit: ApplyPermit {
                version: 1,
                owner: owner.public(),
                share: ShareId([3; 32]),
                consumer: SecretKey::generate().public(),
                epoch: 1,
                snapshot: [4; 32],
                root_hash: Hash32::digest(b"legacy-ro-apply"),
                issued_at: 1,
                expires_at: 2,
                nonce: [5; 32],
                signature: Signature::from_bytes(&[0; 64]),
            },
            operation_id: [8; 16],
            committed: true,
        };
        let legacy = LegacyReadOnlyStateWithStage {
            version: 1,
            owner: [7; 32],
            share: [9; 32],
            checkpoint: vec![record(b"checkpoint", 3)],
            pending: Some(LegacyPendingWithStage {
                desired: desired.clone(),
                attempts: vec!["legacy-operation".to_owned()],
                stage: Stage::Materialized,
                stage_root: Some(stage_root.clone()),
            }),
            apply: Some(apply.clone()),
        };
        let bytes = postcard::to_stdvec(&legacy).expect("managed legacy fixture encoding");
        let decoded = decode_state(&bytes).expect("managed legacy fixture decoding");
        assert_eq!(decoded.version, READ_ONLY_STATE_VERSION);
        let pending = decoded.pending.expect("managed pending retained");
        assert_eq!(pending.desired, desired);
        assert_eq!(pending.stage_root, Some(stage_root));
        assert_eq!(pending.stage_identity, None);
        assert_eq!(pending.stage, Stage::Materialized);
        assert_eq!(decoded.apply, Some(apply));
    }

    #[test]
    fn preserved_reports_displaced_local_only_not_unadopted_incoming() {
        let temp = tempfile::tempdir().expect("preserved fixture root");
        let root = temp.path().join("root");
        let state_root = temp.path().join("state");
        std::fs::create_dir_all(&root).expect("public root");
        std::fs::create_dir_all(&state_root).expect("private root");
        let owner = iroh::SecretKey::generate().public();
        let share = ShareId([0x61; 32]);
        let replica = ReplicaId(Hash32::digest(b"preserved fixture replica"));
        deltaweave_net::root_admission::reserve_private(&state_root).expect("state reservation");
        let lease = deltaweave_net::root_admission::acquire_with_private(
            &root,
            deltaweave_net::root_admission::RootUse::Managed {
                share: share.0,
                owner: *owner.as_bytes(),
            },
            std::slice::from_ref(&state_root),
        )
        .expect("managed lease");
        let index = Arc::new(
            LocalIndex::open(
                &root,
                state_root.join("index.redb"),
                replica,
                IndexOptions::default(),
            )
            .expect("index"),
        );
        let store = Arc::new(
            Store::open_with_recovery_reserver(state_root.join("store"), |path| {
                deltaweave_net::root_admission::reserve_private(path)
            })
            .expect("store"),
        );
        let local = ReplicaState {
            _root_lease: Arc::new(lease),
            root: root.clone(),
            index,
            store: Arc::clone(&store),
            swarm_sources: Vec::new(),
            profile: ChunkingProfile::DEFAULT,
            min_free_space_bytes: 0,
            peer: owner.to_string(),
        };

        let incoming = temp.path().join("incoming");
        std::fs::write(&incoming, b"owner incoming").expect("incoming bytes");
        let incoming_manifest = store
            .ingest_file(&incoming, ChunkingProfile::DEFAULT)
            .expect("incoming manifest");
        let incoming_path = WirePath::new("new.txt").expect("incoming path");
        let mut incoming_change = store
            .prepare_path_change(
                &root,
                &incoming_path,
                PathTarget::File(incoming_manifest),
                None,
                true,
            )
            .expect("incoming path change");
        store
            .capture_path_change(&mut incoming_change)
            .expect("incoming capture");
        store
            .rollback_unadopted_path_change(&mut incoming_change)
            .expect("incoming rollback");
        assert_eq!(incoming_change.state, PathChangeState::RolledBack);
        assert_eq!(
            std::fs::read(&incoming_change.rollback_artifact).expect("retained incoming"),
            b"owner incoming"
        );

        std::fs::write(root.join("local.txt"), b"user edit").expect("local bytes");
        let local_source = temp.path().join("local-incoming");
        std::fs::write(&local_source, b"owner replacement").expect("replacement bytes");
        let local_manifest = store
            .ingest_file(&local_source, ChunkingProfile::DEFAULT)
            .expect("replacement manifest");
        let local_path = WirePath::new("local.txt").expect("local path");
        let expected = PathObservation::read(&root, &local_path).expect("local observation");
        let mut local_change = store
            .prepare_path_change(
                &root,
                &local_path,
                PathTarget::File(local_manifest),
                expected,
                true,
            )
            .expect("local path change");
        store
            .capture_path_change(&mut local_change)
            .expect("local capture");
        let report = preserved(&local).expect("preserved report");
        assert_eq!(report.len(), 1);
        assert_eq!(report[0].path, local_path);
        assert_eq!(
            std::fs::read(&report[0].preserved_path).expect("displaced local bytes"),
            b"user edit"
        );
        assert!(report.iter().all(|item| item.path != incoming_path));
    }
}
