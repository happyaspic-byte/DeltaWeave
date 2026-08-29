//! Deterministic, retry-safe bidirectional folder reconciliation.

#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail, ensure};
use deltaweave_core::{
    ChunkingProfile, FileManifest, Hash32, ReplicaId, SyncEntryKind, SyncRecord, WirePath,
};
use deltaweave_index::{IndexOptions, LocalIndex, ScanReport};
use deltaweave_net::{PullReceipt, SyncApplyReceipt, SyncClient, SyncSession};
use deltaweave_reconcile::{
    ApplyAction, ConflictRecord, MerkleTree, actions_to_reach, merge_snapshots,
};
use deltaweave_store::Store;
use serde::Serialize;

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
    /// Content-defined chunking profile.
    pub profile: ChunkingProfile,
    /// Additional local paths excluded from indexing.
    pub ignored_paths: Vec<PathBuf>,
}

/// Reusable local half of a bidirectional reconciliation relationship.
#[derive(Debug)]
pub struct SyncEngine {
    root: PathBuf,
    index: Arc<LocalIndex>,
    store: Arc<Store>,
    client: SyncClient,
    profile: ChunkingProfile,
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
    /// Deterministic conflict decisions, including preserved conflict-copy paths.
    pub conflicts: Vec<ConflictRecord>,
}

#[derive(Default)]
struct StageStats {
    local_files: usize,
    remote_files: usize,
    pulled_bytes: u64,
    reused_extents: usize,
}

#[derive(Default)]
struct RemoteStats {
    pushed_bytes: u64,
    reused_extents: usize,
}

impl SyncEngine {
    /// Opens durable state after rejecting overlapping public and private roots.
    pub fn open(config: SyncConfig) -> Result<Self> {
        config.profile.validate()?;
        reject_symlink_root(&config.root, "synchronization root")?;
        reject_symlink_root(&config.state_root, "private state root")?;
        fs::create_dir_all(&config.root).with_context(|| {
            format!(
                "failed to create synchronization root {}",
                config.root.display()
            )
        })?;
        fs::create_dir_all(&config.state_root).with_context(|| {
            format!(
                "failed to create private state root {}",
                config.state_root.display()
            )
        })?;
        reject_symlink_root(&config.root, "synchronization root")?;
        reject_symlink_root(&config.state_root, "private state root")?;
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
        let store = Arc::new(Store::open(state_root.join("store"))?);
        Ok(Self {
            root,
            index,
            store,
            client: config.client,
            profile: config.profile,
        })
    }

    /// Merges, applies, and independently verifies one complete bidirectional round.
    pub async fn sync_once(&self) -> Result<SyncReport> {
        let scan = scan_index(Arc::clone(&self.index)).await?;
        ensure_scan_is_safe(&scan, "local")?;
        let local_records = read_records(Arc::clone(&self.index)).await?;
        let local_tree = MerkleTree::from_records(local_records.clone())?;
        let session = self.client.open_session().await?;
        let outcome = self
            .sync_with_session(&session, local_records, local_tree)
            .await;
        session.close().await;
        outcome
    }

    async fn sync_with_session(
        &self,
        session: &SyncSession,
        local_records: Vec<SyncRecord>,
        local_tree: MerkleTree,
    ) -> Result<SyncReport> {
        let remote = session.fetch_snapshot(&local_tree).await?;
        let remote_tree = MerkleTree::from_records(remote.records.clone())?;
        let merged = merge_snapshots(&local_tree, &remote_tree)?;
        validate_materializable_namespace(&merged.records)?;
        let desired_tree = merged.tree()?;
        let local_actions = actions_to_reach(&local_tree, &merged)?;
        let remote_actions = actions_to_reach(&remote_tree, &merged)?;

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
            .stage_desired_files(session, &required_files, &local_records, &remote.records)
            .await?;
        self.apply_local(&local_tree, &local_actions, &manifests)?;
        let remote_stats = self.apply_remote(session, &remote_actions).await?;

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
            conflicts: merged.conflicts,
        })
    }

    async fn stage_desired_files(
        &self,
        session: &SyncSession,
        desired: &[SyncRecord],
        local: &[SyncRecord],
        remote: &[SyncRecord],
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
        for hash in required {
            if let Some(source) = local_sources.get(&hash) {
                let source_path = local_path(&self.root, &source.path);
                let manifest = self.store.ingest_file(&source_path, self.profile)?;
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
            let PullReceipt {
                manifest,
                transferred_bytes,
                reused_extents,
                ..
            } = session
                .pull_record((*source).clone(), Arc::clone(&self.store))
                .await?;
            ensure!(
                manifest.file_hash == hash,
                "remote source returned different content"
            );
            manifests.insert(hash, manifest);
            stats.remote_files += 1;
            stats.pulled_bytes = stats
                .pulled_bytes
                .checked_add(transferred_bytes)
                .context("pulled-byte counter overflow")?;
            stats.reused_extents = stats.reused_extents.saturating_add(reused_extents);
        }
        Ok((manifests, stats))
    }

    fn apply_local(
        &self,
        current: &MerkleTree,
        actions: &[ApplyAction],
        manifests: &BTreeMap<Hash32, FileManifest>,
    ) -> Result<()> {
        let mut deletions = action_records(actions, true, None);
        deletions.sort_by_key(|record| std::cmp::Reverse(path_depth(&record.path)));
        for record in deletions {
            self.store
                .remove_path(&record.path, &self.root, record.logical_hash())?;
            self.index.adopt_verified_record(record)?;
        }

        let mut live = action_records(actions, false, None);
        live.sort_by_key(|record| std::cmp::Reverse(path_depth(&record.path)));
        for record in &live {
            if current
                .get(&record.path)
                .is_some_and(|existing| !existing.tombstone && existing.kind != record.kind)
            {
                self.store
                    .remove_path(&record.path, &self.root, record.logical_hash())?;
            }
        }

        let mut directories = action_records(actions, false, Some(SyncEntryKind::Directory));
        directories.sort_by_key(|record| path_depth(&record.path));
        for record in directories {
            let path = self.store.materialize_directory(&record.path, &self.root)?;
            apply_readonly(&path, record.readonly)?;
            self.index.adopt_verified_record(record)?;
        }

        let mut files = action_records(actions, false, Some(SyncEntryKind::File));
        files.sort_by(|left, right| left.path.cmp(&right.path));
        for record in files {
            let hash = record
                .content_hash
                .context("validated live file unexpectedly lacks a hash")?;
            let manifest = manifests
                .get(&hash)
                .with_context(|| format!("required content {hash} was not staged"))?;
            let outcome = self.store.materialize(manifest, &record.path, &self.root)?;
            apply_readonly(&outcome.destination, record.readonly)?;
            self.index.adopt_verified_record(record)?;
        }
        Ok(())
    }

    async fn apply_remote(
        &self,
        session: &SyncSession,
        actions: &[ApplyAction],
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
    for record in records.iter().filter(|record| !record.tombstone) {
        ensure!(
            matches!(record.kind, SyncEntryKind::File | SyncEntryKind::Directory),
            "safe materialization of {:?} at {} is not enabled",
            record.kind,
            record.path
        );
        let components: Vec<_> = record.path.components().collect();
        for end in 1..components.len() {
            let ancestor = components[..end].join("/");
            if let Some(ancestor_record) = by_path.get(ancestor.as_str()) {
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

fn reject_symlink_root(path: &Path, label: &str) -> Result<()> {
    let mut current = path;
    loop {
        if let Ok(metadata) = fs::symlink_metadata(current) {
            ensure!(
                !metadata.file_type().is_symlink(),
                "{label} must not contain a symbolic link"
            );
        }
        let Some(parent) = current.parent() else {
            break;
        };
        if parent == current {
            break;
        }
        current = parent;
    }
    if let Ok(metadata) = fs::symlink_metadata(path) {
        ensure!(
            metadata.is_dir(),
            "{label} must be a real directory, not a symlink"
        );
    }
    Ok(())
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

fn apply_readonly(path: &Path, readonly: bool) -> Result<()> {
    let mut permissions = fs::symlink_metadata(path)?.permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = permissions.mode();
        permissions.set_mode(if readonly {
            mode & !0o222
        } else {
            mode | 0o200
        });
    }
    #[cfg(not(unix))]
    permissions.set_readonly(readonly);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, fs};

    use deltaweave_net::{NetworkMode, PeerPolicy, ServerConfig, start_server};
    use iroh::SecretKey;
    use tempfile::TempDir;

    use super::*;

    fn replica(key: &SecretKey) -> ReplicaId {
        ReplicaId(Hash32::digest(key.public().as_bytes()))
    }

    #[cfg(unix)]
    #[test]
    fn sync_root_rejects_symbolic_link() {
        use std::os::unix::fs::symlink;

        let parent = TempDir::new().expect("root parent can be created");
        let outside = TempDir::new().expect("outside root can be created");
        let root = parent.path().join("root");
        symlink(outside.path(), &root).expect("root symlink can be created");

        assert!(reject_symlink_root(&root, "synchronization root").is_err());
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
            quota_policy: None,
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
}
