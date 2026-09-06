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
use deltaweave_index::{IndexOptions, LocalIndex, ScanReport, collision_key};
use deltaweave_net::{
    Inventory, PullReceipt, SyncApplyReceipt, SyncClient, SyncSession, TransferEvent,
    TransferObserver,
};
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
        let store = Arc::new(Store::open(state_root.join("store"))?);
        Ok(Self {
            root,
            index,
            store,
            client: config.client,
            profile: config.profile,
        })
    }

    /// Reads the retained index snapshot without scanning or opening another owner.
    pub fn inventory(&self) -> Result<Inventory> {
        let mut inventory = Inventory {
            retries: self.index.retries()?.len(),
            ..Inventory::default()
        };
        for record in self.index.sync_records()? {
            if !record.tombstone && record.kind == SyncEntryKind::File {
                inventory.files += 1;
                inventory.bytes = inventory
                    .bytes
                    .checked_add(record.size)
                    .context("inventory byte overflow")?;
            }
        }
        Ok(inventory)
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
        self.observe(&observer, "scanning", None, None, 0);
        let outcome = async {
            let scan = scan_index(Arc::clone(&self.index)).await?;
            ensure_scan_is_safe(&scan, "local")?;
            let local_records = read_records(Arc::clone(&self.index)).await?;
            let local_tree = MerkleTree::from_records(local_records.clone())?;
            let session = self.client.open_session().await?;
            let outcome = self
                .sync_with_session(&session, local_records, local_tree, &observer)
                .await;
            session.close().await;
            outcome
        }
        .await;
        match &outcome {
            Ok(report) => self.observe(
                &observer,
                "complete",
                None,
                None,
                report.pulled_bytes.saturating_add(report.pushed_bytes),
            ),
            Err(_) => self.observe(&observer, "error", None, None, 0),
        }
        outcome
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
                peer: Some(self.client.remote.id.to_string()),
            });
        }
    }

    async fn sync_with_session(
        &self,
        session: &SyncSession,
        local_records: Vec<SyncRecord>,
        local_tree: MerkleTree,
        observer: &Option<TransferObserver>,
    ) -> Result<SyncReport> {
        self.observe(observer, "comparing", None, None, 0);
        let remote = session.fetch_snapshot(&local_tree).await?;
        self.observe(observer, "peer_seen", None, None, 0);
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
            .stage_desired_files(
                session,
                &required_files,
                &local_records,
                &remote.records,
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
            conflicts: merged.conflicts,
        })
    }

    async fn stage_desired_files(
        &self,
        session: &SyncSession,
        desired: &[SyncRecord],
        local: &[SyncRecord],
        remote: &[SyncRecord],
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
            self.observe(observer, "pulling", Some(&source.path), Some("pull"), 0);
            let PullReceipt {
                manifest,
                transferred_bytes,
                reused_extents,
                ..
            } = session
                .pull_record_to(
                    (*source).clone(),
                    Arc::clone(&self.store),
                    self.root.clone(),
                )
                .await?;
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
        }
        Ok((manifests, stats))
    }

    async fn apply_local(
        &self,
        current: &MerkleTree,
        actions: &[ApplyAction],
        manifests: &BTreeMap<Hash32, FileManifest>,
    ) -> Result<()> {
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
            let observation = outcome
                .observation
                .after_readonly_update(&outcome.destination, record.readonly)?;
            self.index.adopt_materialized_record(record, &observation)?;
        }
        Ok(())
    }

    async fn apply_remote(
        &self,
        session: &SyncSession,
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

    use deltaweave_core::{SYNC_RECORD_SCHEMA_V1, VersionVector};
    use deltaweave_net::{
        NetworkMode, PeerPolicy, ServerConfig, start_server, start_server_observed,
    };
    use iroh::{EndpointAddr, SecretKey};
    use tempfile::TempDir;

    use super::*;

    fn replica(key: &SecretKey) -> ReplicaId {
        ReplicaId(Hash32::digest(key.public().as_bytes()))
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
