//! Authoritative RO application: local vectors never become owner history.
use super::*;
use deltaweave_core::CausalRelation;
use deltaweave_net::share::{Membership, ShareError, ShareSession};
use deltaweave_store::{PathChangeState, PathObservation, PathTarget};
use serde::{Deserialize, Serialize};

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
}
#[derive(Clone, Debug, Deserialize, Serialize)]
struct Pending {
    desired: Vec<SyncRecord>,
    attempts: Vec<String>,
    stage: Stage,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
enum Stage {
    Prepared,
    Preserved,
    Materialized,
    Adopted,
}

pub(crate) fn initialize(local: &ReplicaState, member: &Membership) -> Result<()> {
    if let Some(bytes) = local.index.share_metadata()? {
        let state: ReadOnlyState =
            postcard::from_bytes(&bytes).context(ShareError::StateUnavailable)?;
        ensure!(
            state.version == 1
                && state.owner == *member.owner.as_bytes()
                && state.share == member.share_id.0,
            ShareError::StateUnavailable
        );
    } else {
        // This slot is set before the first scan, including a legitimate retained-index import.
        save(
            local,
            &ReadOnlyState {
                version: 1,
                owner: *member.owner.as_bytes(),
                share: member.share_id.0,
                checkpoint: Vec::new(),
                pending: None,
            },
        )?;
    }
    Ok(())
}
fn load(local: &ReplicaState) -> Result<ReadOnlyState> {
    postcard::from_bytes(
        &local
            .index
            .share_metadata()?
            .context(ShareError::StateUnavailable)?,
    )
    .context(ShareError::StateUnavailable)
}
fn save(local: &ReplicaState, state: &ReadOnlyState) -> Result<()> {
    local.index.set_share_metadata(&postcard::to_stdvec(state)?)
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
        for artifact in [&change.artifact, &change.rollback_artifact] {
            if fs::symlink_metadata(artifact).is_ok() {
                preserved.push(PreservedLocalChange {
                    path: change.path.clone(),
                    operation_id: change.id.clone(),
                    preserved_path: artifact.clone(),
                });
            }
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

pub(crate) async fn sync(
    local: &Arc<ReplicaState>,
    session: &ShareSession,
    observer: &Option<TransferObserver>,
) -> Result<ReadOnlyReport> {
    let mut state = load(local)?;
    // Always reconstruct the complete issuer snapshot: local-only vectors are not a Merkle base.
    let empty = MerkleTree::from_records(Vec::new())?;
    let remote = session.fetch_snapshot(&empty).await?;
    validate_checkpoint(&state.checkpoint, &remote.records)?;
    if let Some(pending) = &state.pending {
        validate_checkpoint(&pending.desired, &remote.records)?;
    }
    let owner_tree = MerkleTree::from_records(remote.records.clone())?;
    local.observe(observer, "peer_seen", None, None, 0);
    if let Some(pending) = &state.pending {
        for mut change in local
            .store
            .path_changes()?
            .into_iter()
            .filter(|c| pending.attempts.contains(&c.id))
        {
            local.store.resume_path_change(&mut change)?;
        }
    }
    let scan = scan_index(local.index.clone()).await?;
    ensure_scan_is_safe(&scan, "read-only local")?;
    let current = read_records(local.index.clone()).await?;
    let current_tree = MerkleTree::from_records(current.clone())?;
    let current_by_path: BTreeMap<_, _> = current.iter().map(|r| (r.path.clone(), r)).collect();
    let remote_by_path: BTreeMap<_, _> =
        remote.records.iter().map(|r| (r.path.clone(), r)).collect();
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
    for record in remote.records.iter().filter(|r| !r.tombstone) {
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
    // Capturing a complete directory preserves its local-only descendants exactly once.
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
    let (manifests, stats) = local
        .stage_desired_files(
            session,
            &files,
            &current,
            &remote.records,
            pending_bytes,
            observer,
        )
        .await?;
    let scan = scan_index(local.index.clone()).await?;
    ensure_scan_is_safe(&scan, "read-only before apply")?;
    ensure!(
        MerkleTree::from_records(read_records(local.index.clone()).await?)?.root_hash()
            == current_tree.root_hash(),
        "local state changed before authoritative apply"
    );
    let old_attempts = state.pending.take().map_or_else(Vec::new, |p| p.attempts);
    state.pending = Some(Pending {
        desired: remote.records.clone(),
        attempts: old_attempts,
        stage: Stage::Prepared,
    });
    save(local, &state)?;
    local.observe(observer, "ro_prepared", None, None, 0);
    // Parents first: a directory replacing a file must exist before installing its children.
    let mut changes: Vec<_> = changes.into_iter().collect();
    changes.sort_by_key(|(path, _)| path_depth(path));
    for (path, (expected, desired)) in changes {
        let target = match desired {
            Some(record) if record.kind == SyncEntryKind::File => {
                let manifest = manifests
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
        local.store.capture_path_change(&mut change)?;
        state
            .pending
            .as_mut()
            .context("pending journal missing")?
            .stage = Stage::Preserved;
        save(local, &state)?;
        local.observe(observer, "ro_preserved", Some(&path), None, 0);
        local.store.materialize_path_change(&mut change)?;
    }
    for record in remote.records.iter().filter(|r| !r.tombstone) {
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
    state.checkpoint = remote.records.clone();
    state
        .pending
        .as_mut()
        .context("pending journal missing")?
        .stage = Stage::Adopted;
    // Complete replacement, including removal of all local-only versions, is one redb commit.
    local
        .index
        .adopt_authoritative_snapshot(&remote.records, &postcard::to_stdvec(&state)?)?;
    local.observe(observer, "ro_adopted", None, None, 0);
    for change in local
        .store
        .path_changes()?
        .into_iter()
        .filter(|c| c.root == local.root && c.state == PathChangeState::Materialized)
    {
        local.store.mark_path_change_indexed(&change.id)?;
    }
    state.pending = None;
    save(local, &state)?;
    let verification = scan_index(local.index.clone()).await?;
    ensure_scan_is_safe(&verification, "verified read-only")?;
    let verified = MerkleTree::from_records(read_records(local.index.clone()).await?)?;
    let fresh_owner = session.fetch_snapshot(&owner_tree).await?;
    let fresh_owner = MerkleTree::from_records(fresh_owner.records)?;
    ensure!(
        verified.root_hash() == owner_tree.root_hash()
            && fresh_owner.root_hash() == owner_tree.root_hash(),
        "owner or local namespace changed during authoritative verification; retry"
    );
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
}
