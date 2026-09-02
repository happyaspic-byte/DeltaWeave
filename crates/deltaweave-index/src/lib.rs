//! Authoritative local filesystem index, collision detection, retries, and watcher hints.

#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    fs::{self, File, Metadata},
    io::Read,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use deltaweave_core::{
    Hash32, ReplicaId, SYNC_RECORD_SCHEMA_V1, SyncEntryKind, SyncRecord, VersionVector, WirePath,
};
use deltaweave_store::MaterializationObservation;
use icu_casemap::CaseMapper;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;

const INDEX_SCHEMA_V1: u16 = 1;
const RECORDS: TableDefinition<&str, &[u8]> = TableDefinition::new("path_records");
const RETRIES: TableDefinition<&str, &[u8]> = TableDefinition::new("index_retries");
const METADATA: TableDefinition<&str, u64> = TableDefinition::new("index_metadata");
const CONFIG: TableDefinition<&str, &[u8]> = TableDefinition::new("index_config");
const GENERATION_KEY: &str = "generation";
const REPLICA_COUNTER_KEY: &str = "replica_counter";
const CONFIG_SCHEMA_KEY: &str = "schema";
const CONFIG_ROOT_KEY: &str = "root";
const CONFIG_REPLICA_KEY: &str = "replica";
const RETRY_BASE_MS: u64 = 500;
const RETRY_MAX_MS: u64 = 5 * 60 * 1000;

/// Filesystem object kinds represented in the local index.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    /// Regular file with a complete-file BLAKE3 digest.
    File,
    /// Directory. The synchronization layer may use it to preserve empty folders.
    Directory,
    /// Symbolic link or Windows reparse-point-like link. It is indexed but never followed.
    Symlink,
    /// A filesystem object that is neither a regular file, directory, nor symlink.
    Other,
}

/// Best-effort stable file identity exposed by the host filesystem.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct FileIdentity {
    /// Filesystem or volume namespace.
    pub namespace: u64,
    /// Inode or file index within the namespace.
    pub object: u64,
}

/// Immutable logical state for one portable path.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PathRecord {
    /// On-disk record schema.
    pub schema_version: u16,
    /// Portable path relative to the indexed root.
    pub path: WirePath,
    /// Unicode-normalized, case-folded key used only to detect cross-platform collisions.
    pub collision_key: String,
    /// Filesystem object kind.
    pub kind: EntryKind,
    /// Stable OS identity when available.
    pub identity: Option<FileIdentity>,
    /// File length, or zero for non-files.
    pub size: u64,
    /// Regular-file modification time as nanoseconds since the Unix epoch when available.
    /// Directory timestamps are normalized away because child updates mutate them implicitly.
    pub modified_ns: Option<u128>,
    /// Whether a regular file was read-only when scanned. Directory write semantics are not
    /// portable and are intentionally normalized to writable for safe child synchronization.
    pub readonly: bool,
    /// Complete-file digest for regular files.
    pub content_hash: Option<Hash32>,
    /// Causal version associated with this path state.
    pub version: VersionVector,
    /// True when this record represents a durable deletion.
    pub tombstone: bool,
    /// Scan generation that last changed this record.
    pub generation: u64,
}

impl PathRecord {
    /// Projects host-specific index metadata into the portable distributed state model.
    #[must_use]
    pub fn to_sync_record(&self) -> SyncRecord {
        SyncRecord {
            schema_version: SYNC_RECORD_SCHEMA_V1,
            path: self.path.clone(),
            kind: self.kind.into(),
            size: self.size,
            content_hash: self.content_hash,
            readonly: self.readonly,
            version: self.version.clone(),
            tombstone: self.tombstone,
        }
    }
}

impl From<EntryKind> for SyncEntryKind {
    fn from(value: EntryKind) -> Self {
        match value {
            EntryKind::File => Self::File,
            EntryKind::Directory => Self::Directory,
            EntryKind::Symlink => Self::Symlink,
            EntryKind::Other => Self::Other,
        }
    }
}

/// One persistent exponential-backoff retry.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RetryRecord {
    /// Path that could not be read stably.
    pub path: WirePath,
    /// Number of consecutive failures.
    pub attempts: u32,
    /// Earliest Unix time in milliseconds at which another attempt is allowed.
    pub not_before_ms: u64,
    /// Last observed error, bounded before persistence.
    pub last_error: String,
}

impl RetryRecord {
    /// Returns whether this retry may be attempted at `now_ms`.
    #[must_use]
    pub const fn is_due(&self, now_ms: u64) -> bool {
        now_ms >= self.not_before_ms
    }
}

/// A group of names that cannot coexist safely on every supported filesystem.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CollisionGroup {
    /// Normalized comparison key.
    pub collision_key: String,
    /// Distinct live paths sharing the key.
    pub paths: Vec<WirePath>,
}

/// Authoritative state transition detected by a complete scan.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScanChange {
    /// A path became live.
    Created { path: WirePath },
    /// A live path changed type, metadata, or content.
    Modified { path: WirePath },
    /// A live path disappeared and became a tombstone.
    Deleted { path: WirePath },
    /// Stable identity correlated an old and new path.
    Renamed { from: WirePath, to: WirePath },
}

/// Non-fatal conditions retained in scan output instead of causing unsafe deletion.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScanIssueKind {
    /// A path cannot be represented by the portable wire format.
    NonPortablePath,
    /// A directory or metadata entry could not be enumerated.
    EnumerationFailed,
    /// A file could not be opened or read.
    HashFailed,
    /// A file changed while being hashed.
    MutatedDuringRead,
    /// A previous failure is still inside its backoff window.
    RetryDeferred,
    /// Multiple live names collapse to the same cross-platform comparison key.
    PathCollision,
}

/// One diagnostic produced by a scan.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScanIssue {
    /// Best-effort display path. It may be non-portable or non-UTF-8 escaped.
    pub path: String,
    /// Machine-readable issue class.
    pub kind: ScanIssueKind,
    /// Bounded human-readable detail.
    pub message: String,
}

/// Result of one complete authoritative scan and atomic index commit.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScanReport {
    /// Committed scan generation.
    pub generation: u64,
    /// Number of live indexed paths after the scan.
    pub live_records: usize,
    /// Number of durable tombstones after the scan.
    pub tombstones: usize,
    /// Number of regular files successfully hashed during this scan.
    pub files_hashed: usize,
    /// Number of unchanged current paths.
    pub unchanged: usize,
    /// Persistent retry entries after the scan.
    pub retries_queued: usize,
    /// State transitions committed by this scan.
    pub changes: Vec<ScanChange>,
    /// Cross-platform path collisions requiring operator resolution.
    pub collisions: Vec<CollisionGroup>,
    /// Non-fatal read and portability diagnostics.
    pub issues: Vec<ScanIssue>,
}

/// Scanner resource and safety controls.
#[derive(Clone, Debug)]
pub struct IndexOptions {
    /// Maximum number of simultaneous file hashers.
    pub hash_workers: usize,
    /// Absolute or relative paths excluded from traversal, commonly the private state root.
    pub ignored_paths: Vec<PathBuf>,
    /// Test-only time override. Production callers should leave this as `None`.
    pub now_ms: Option<u64>,
}

impl Default for IndexOptions {
    fn default() -> Self {
        let workers = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
            .clamp(1, 8);
        Self {
            hash_workers: workers,
            ignored_paths: Vec::new(),
            now_ms: None,
        }
    }
}

/// Persistent authoritative index bound to one local root and replica identity.
pub struct LocalIndex {
    root: PathBuf,
    ignored_paths: Vec<PathBuf>,
    replica: ReplicaId,
    options: IndexOptions,
    database: Database,
}

impl std::fmt::Debug for LocalIndex {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalIndex")
            .field("root", &self.root)
            .field("replica", &self.replica)
            .finish_non_exhaustive()
    }
}

impl LocalIndex {
    /// Opens an index database and validates that `root` is a real directory.
    pub fn open(
        root: impl AsRef<Path>,
        database_path: impl AsRef<Path>,
        replica: ReplicaId,
        options: IndexOptions,
    ) -> Result<Self> {
        let root_metadata = fs::symlink_metadata(root.as_ref())
            .with_context(|| format!("failed to inspect index root {}", root.as_ref().display()))?;
        if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
            bail!("index root must be a real directory, not a symlink");
        }
        let root = fs::canonicalize(root.as_ref())
            .with_context(|| format!("failed to resolve index root {}", root.as_ref().display()))?;

        let mut database_path = absolute_path(database_path.as_ref())?;
        if fs::symlink_metadata(&database_path)
            .is_ok_and(|metadata| metadata.file_type().is_symlink())
        {
            bail!("index DB must not be a symbolic link");
        }
        if let Some(parent) = database_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let database = Database::create(&database_path)
            .with_context(|| format!("failed to open index DB {}", database_path.display()))?;
        database_path = fs::canonicalize(&database_path).with_context(|| {
            format!(
                "failed to resolve opened index DB {}",
                database_path.display()
            )
        })?;
        initialize_tables(&database)?;
        bind_database(&database, &root, replica)?;

        let mut ignored_paths = options
            .ignored_paths
            .iter()
            .map(|path| absolute_path(path))
            .collect::<Result<Vec<_>>>()?;
        if database_path.starts_with(&root) {
            match database_path.parent() {
                Some(parent) if parent != root => ignored_paths.push(parent.to_path_buf()),
                _ => ignored_paths.push(database_path),
            }
        }
        ignored_paths.sort();
        ignored_paths.dedup();
        if ignored_paths
            .iter()
            .any(|ignored| root.starts_with(ignored))
        {
            bail!("an ignored path must not contain the index root");
        }

        Ok(Self {
            root,
            ignored_paths,
            replica,
            options,
            database,
        })
    }

    /// Returns the canonical root represented by this index.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Returns normalized paths excluded from scanning and watcher-triggered rescans.
    #[must_use]
    pub fn ignored_paths(&self) -> &[PathBuf] {
        &self.ignored_paths
    }

    /// Performs a complete scan and atomically commits all safe observations.
    pub fn scan(&self) -> Result<ScanReport> {
        self.scan_internal(None)
    }

    /// Scans after native watcher events, reusing hashes for metadata-stable files that were not
    /// touched by the event batch. A periodic [`Self::scan`] remains the authoritative safety net.
    pub fn scan_incremental(&self, changed_paths: &[PathBuf]) -> Result<ScanReport> {
        let changed_paths = changed_paths
            .iter()
            .map(|path| {
                if path.is_absolute() {
                    absolute_path(path)
                } else {
                    absolute_path(&self.root.join(path))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        self.scan_internal(Some(&changed_paths))
    }

    fn scan_internal(&self, changed_paths: Option<&[PathBuf]>) -> Result<ScanReport> {
        let now_ms = self.options.now_ms.unwrap_or_else(unix_time_ms);
        let previous = self.records_map()?;
        let previous_retries = self.retries_map()?;
        let mut traversal = Traversal::default();
        traversal.uncertain_prefixes.extend(
            self.ignored_paths
                .iter()
                .filter_map(|ignored| portable_path(&self.root, ignored).ok()),
        );
        collect_directory(
            &self.root,
            &self.root,
            &self.ignored_paths,
            true,
            &mut traversal,
        )?;

        let mut scanned = Vec::new();
        let mut hash_candidates = Vec::new();
        let mut preserved = traversal.uncertain_paths.clone();
        for candidate in traversal.candidates {
            if candidate.kind != EntryKind::File {
                scanned.push(candidate.into_scanned(None));
                continue;
            }
            let needs_verification = changed_paths.is_none_or(|changed_paths| {
                changed_paths.iter().any(|changed| {
                    candidate.local_path == *changed || candidate.local_path.starts_with(changed)
                })
            });
            if !needs_verification
                && let Some(existing) = previous.get(&candidate.path)
                && !existing.tombstone
                && existing.content_hash.is_some()
                && record_fingerprint_matches(existing, &candidate)
            {
                scanned.push(candidate.into_scanned(existing.content_hash));
                continue;
            }
            if let Some(retry) = previous_retries.get(&candidate.path)
                && !retry.is_due(now_ms)
            {
                preserved.insert(candidate.path.clone());
                traversal.issues.push(ScanIssue {
                    path: candidate.local_path.display().to_string(),
                    kind: ScanIssueKind::RetryDeferred,
                    message: format!(
                        "retry attempt {} deferred until {}",
                        retry.attempts, retry.not_before_ms
                    ),
                });
                continue;
            }
            hash_candidates.push(candidate);
        }

        let hash_outcomes = hash_files(hash_candidates, self.options.hash_workers.max(1));
        let mut failures = Vec::new();
        let mut files_hashed = 0_usize;
        for outcome in hash_outcomes {
            match outcome.result {
                Ok(hash) => {
                    files_hashed += 1;
                    scanned.push(outcome.candidate.into_scanned(Some(hash)));
                }
                Err(failure) => {
                    preserved.insert(outcome.candidate.path.clone());
                    traversal.issues.push(ScanIssue {
                        path: outcome.candidate.local_path.display().to_string(),
                        kind: failure.kind,
                        message: bounded_message(&failure.message),
                    });
                    failures.push(HashFailure {
                        path: outcome.candidate.path,
                        message: bounded_message(&failure.message),
                    });
                }
            }
        }

        self.apply_scan(
            previous,
            previous_retries,
            scanned,
            preserved,
            traversal.uncertain_prefixes,
            traversal.preserve_all_missing,
            failures,
            traversal.issues,
            files_hashed,
            now_ms,
        )
    }

    /// Returns all records sorted by portable path.
    pub fn records(&self) -> Result<Vec<PathRecord>> {
        Ok(self.records_map()?.into_values().collect())
    }

    /// Returns the complete portable causal snapshot used by peer reconciliation.
    pub fn sync_records(&self) -> Result<Vec<SyncRecord>> {
        Ok(self
            .records_map()?
            .into_values()
            .map(|record| record.to_sync_record())
            .collect())
    }

    /// Returns all persistent retries sorted by portable path.
    pub fn retries(&self) -> Result<Vec<RetryRecord>> {
        Ok(self.retries_map()?.into_values().collect())
    }

    /// Returns one record by path.
    pub fn get(&self, path: &WirePath) -> Result<Option<PathRecord>> {
        let read = self.database.begin_read()?;
        let table = read.open_table(RECORDS)?;
        let encoded = table
            .get(path.as_str())?
            .map(|value| value.value().to_vec());
        encoded
            .map(|bytes| postcard::from_bytes(&bytes).context("invalid path record in index DB"))
            .transpose()
    }

    /// Adopts a verified direct-push file as a local index change without rehashing it.
    pub fn adopt_materialized_file(
        &self,
        path: &WirePath,
        observation: &MaterializationObservation,
    ) -> Result<()> {
        let previous = self.get(path)?;
        let mut version = previous
            .as_ref()
            .map_or_else(VersionVector::default, |record| record.version.clone());
        let counter = next_replica_counter(
            self.metadata_value(REPLICA_COUNTER_KEY)?,
            &version,
            self.replica,
        )?;
        version.observe(self.replica, counter);
        let record = SyncRecord {
            schema_version: SYNC_RECORD_SCHEMA_V1,
            path: path.clone(),
            kind: SyncEntryKind::File,
            size: observation.size(),
            content_hash: Some(observation.file_hash()),
            readonly: observation.readonly(),
            version,
            tombstone: false,
        };
        self.commit_adopted_record(&record, Some(observation))
    }

    /// Adopts a live file using a locally produced materialization observation.
    ///
    /// The observation is trusted only as a content digest produced by this process. Current
    /// metadata and fingerprint must still match before the causal record is stored.
    pub fn adopt_materialized_record(
        &self,
        record: &SyncRecord,
        observation: &MaterializationObservation,
    ) -> Result<()> {
        record.validate()?;
        ensure!(
            !record.tombstone && record.kind == SyncEntryKind::File,
            "materialized adoption requires a live file record"
        );
        ensure!(
            observation.size() == record.size
                && Some(observation.file_hash()) == record.content_hash,
            "materialization observation does not match causal record"
        );
        self.commit_adopted_record(record, Some(observation))
    }

    /// Adopts a verified filesystem state with the exact causal version received from peers.
    ///
    /// The caller must materialize or delete the local object first. This method re-inspects and
    /// hashes live files before atomically updating the index, preventing a remote version from
    /// being attached to bytes that were not actually installed.
    pub fn adopt_verified_record(&self, record: &SyncRecord) -> Result<()> {
        record.validate()?;
        self.commit_adopted_record(record, None)
    }

    fn commit_adopted_record(
        &self,
        record: &SyncRecord,
        observation: Option<&MaterializationObservation>,
    ) -> Result<()> {
        let generation = self
            .metadata_value(GENERATION_KEY)?
            .checked_add(1)
            .context("index generation overflow")?;
        let mut records = self.records_map()?;
        let mut retries = self.retries_map()?;
        let local_path = local_path(&self.root, &record.path);

        let adopted = if record.tombstone {
            ensure!(
                observation.is_none(),
                "tombstones cannot be adopted from a materialization observation"
            );
            ensure!(
                fs::symlink_metadata(&local_path)
                    .is_err_and(|error| { error.kind() == std::io::ErrorKind::NotFound }),
                "cannot adopt tombstone while local object still exists at {}",
                local_path.display()
            );
            let mut path_record = records.get(&record.path).cloned().unwrap_or(PathRecord {
                schema_version: INDEX_SCHEMA_V1,
                path: record.path.clone(),
                collision_key: collision_key(&record.path),
                kind: sync_entry_kind(record.kind),
                identity: None,
                size: record.size,
                modified_ns: None,
                readonly: record.readonly,
                content_hash: record.content_hash,
                version: record.version.clone(),
                tombstone: true,
                generation,
            });
            path_record.kind = sync_entry_kind(record.kind);
            path_record.size = record.size;
            path_record.readonly = record.readonly;
            path_record.content_hash = record.content_hash;
            path_record.version = record.version.clone();
            path_record.tombstone = true;
            path_record.generation = generation;
            path_record
        } else {
            let metadata = fs::symlink_metadata(&local_path).with_context(|| {
                format!(
                    "failed to inspect materialized remote path {}",
                    local_path.display()
                )
            })?;
            let kind = entry_kind(&metadata);
            ensure!(
                kind == sync_entry_kind(record.kind),
                "materialized kind at {} does not match remote record",
                local_path.display()
            );
            let fingerprint = metadata_fingerprint(&local_path, &metadata, kind);
            ensure!(
                fingerprint.readonly == record.readonly,
                "materialized permissions at {} do not match remote record",
                local_path.display()
            );
            let content_hash = if kind == EntryKind::File {
                match observation {
                    Some(observation)
                        if observation_trusts_no_rehash(
                            Some(observation),
                            &fingerprint,
                            record,
                        ) =>
                    {
                        Some(observation.file_hash())
                    }
                    _ => {
                        let hash =
                            hash_stable_file(&local_path, fingerprint).map_err(|failure| {
                                anyhow::anyhow!(
                                    "failed to verify materialized remote file {}: {}",
                                    local_path.display(),
                                    failure.message
                                )
                            })?;
                        ensure!(
                            fingerprint.size == record.size && Some(hash) == record.content_hash,
                            "materialized content at {} does not match remote record",
                            local_path.display()
                        );
                        Some(hash)
                    }
                }
            } else {
                ensure!(
                    observation.is_none(),
                    "materialization observations apply only to regular files"
                );
                None
            };
            PathRecord {
                schema_version: INDEX_SCHEMA_V1,
                path: record.path.clone(),
                collision_key: collision_key(&record.path),
                kind,
                identity: fingerprint.identity,
                size: fingerprint.size,
                modified_ns: fingerprint.modified_ns,
                readonly: fingerprint.readonly,
                content_hash,
                version: record.version.clone(),
                tombstone: false,
                generation,
            }
        };

        records.insert(record.path.clone(), adopted);
        retries.remove(&record.path);
        let replica_counter = self
            .metadata_value(REPLICA_COUNTER_KEY)?
            .max(record.version.get(self.replica));
        self.commit_state(&records, &retries, generation, replica_counter)
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_scan(
        &self,
        previous: BTreeMap<WirePath, PathRecord>,
        previous_retries: BTreeMap<WirePath, RetryRecord>,
        scanned: Vec<ScannedEntry>,
        preserved: BTreeSet<WirePath>,
        uncertain_prefixes: Vec<WirePath>,
        preserve_all_missing: bool,
        failures: Vec<HashFailure>,
        mut issues: Vec<ScanIssue>,
        files_hashed: usize,
        now_ms: u64,
    ) -> Result<ScanReport> {
        let generation = self
            .metadata_value(GENERATION_KEY)?
            .checked_add(1)
            .context("index generation overflow")?;
        let mut replica_counter = self.metadata_value(REPLICA_COUNTER_KEY)?;
        let current: BTreeMap<_, _> = scanned
            .into_iter()
            .map(|entry| (entry.path.clone(), entry))
            .collect();

        let rename_map = correlate_renames(
            &previous,
            &current,
            &preserved,
            &uncertain_prefixes,
            preserve_all_missing,
        );
        let mut renamed_sources = BTreeSet::new();
        let mut next = previous.clone();
        let mut changes = Vec::new();
        let mut unchanged = 0_usize;
        let mut successful_paths = BTreeSet::new();

        for (path, entry) in &current {
            successful_paths.insert(path.clone());
            if let Some(existing) = previous.get(path)
                && !existing.tombstone
                && record_matches(existing, entry)
            {
                unchanged += 1;
                continue;
            }

            if let Some(from) = rename_map.get(path) {
                let source = previous
                    .get(from)
                    .context("correlated rename source disappeared")?;
                replica_counter =
                    next_replica_counter(replica_counter, &source.version, self.replica)?;
                let mut version = source.version.clone();
                version.observe(self.replica, replica_counter);
                let mut tombstone = source.clone();
                tombstone.tombstone = true;
                tombstone.version = version.clone();
                tombstone.generation = generation;
                next.insert(from.clone(), tombstone);
                next.insert(path.clone(), entry.to_record(version, generation));
                renamed_sources.insert(from.clone());
                changes.push(ScanChange::Renamed {
                    from: from.clone(),
                    to: path.clone(),
                });
                continue;
            }

            let (mut version, change) = match previous.get(path) {
                Some(existing) => (
                    existing.version.clone(),
                    if existing.tombstone {
                        ScanChange::Created { path: path.clone() }
                    } else {
                        ScanChange::Modified { path: path.clone() }
                    },
                ),
                None => (
                    VersionVector::default(),
                    ScanChange::Created { path: path.clone() },
                ),
            };
            replica_counter = next_replica_counter(replica_counter, &version, self.replica)?;
            version.observe(self.replica, replica_counter);
            next.insert(path.clone(), entry.to_record(version, generation));
            changes.push(change);
        }

        for (path, existing) in &previous {
            if existing.tombstone
                || current.contains_key(path)
                || renamed_sources.contains(path)
                || preserve_all_missing
                || should_preserve(path, &preserved, &uncertain_prefixes)
            {
                continue;
            }
            replica_counter =
                next_replica_counter(replica_counter, &existing.version, self.replica)?;
            let mut tombstone = existing.clone();
            tombstone.version.observe(self.replica, replica_counter);
            tombstone.tombstone = true;
            tombstone.generation = generation;
            next.insert(path.clone(), tombstone);
            changes.push(ScanChange::Deleted { path: path.clone() });
        }

        let mut retry_next = previous_retries;
        retry_next.retain(|path, _| {
            !self.wire_path_is_ignored(path)
                && (current.contains_key(path)
                    || preserve_all_missing
                    || should_preserve(path, &preserved, &uncertain_prefixes))
        });
        for path in &successful_paths {
            retry_next.remove(path);
        }
        for change in &changes {
            match change {
                ScanChange::Deleted { path } => {
                    retry_next.remove(path);
                }
                ScanChange::Renamed { from, to } => {
                    retry_next.remove(from);
                    retry_next.remove(to);
                }
                ScanChange::Created { .. } | ScanChange::Modified { .. } => {}
            }
        }
        for failure in failures {
            let retry = schedule_retry(
                retry_next.get(&failure.path),
                failure.path,
                now_ms,
                failure.message,
            );
            retry_next.insert(retry.path.clone(), retry);
        }

        let collisions = collision_groups(next.values().filter(|record| !record.tombstone));
        for collision in &collisions {
            issues.push(ScanIssue {
                path: collision
                    .paths
                    .iter()
                    .map(WirePath::as_str)
                    .collect::<Vec<_>>()
                    .join(", "),
                kind: ScanIssueKind::PathCollision,
                message: format!(
                    "{} paths share cross-platform collision key {:?}",
                    collision.paths.len(),
                    collision.collision_key
                ),
            });
        }

        changes.sort_by(|left, right| change_key(left).cmp(&change_key(right)));
        issues.sort_by(|left, right| left.path.cmp(&right.path));
        self.commit_state(&next, &retry_next, generation, replica_counter)?;

        let live_records = next.values().filter(|record| !record.tombstone).count();
        let tombstones = next.len().saturating_sub(live_records);
        Ok(ScanReport {
            generation,
            live_records,
            tombstones,
            files_hashed,
            unchanged,
            retries_queued: retry_next.len(),
            changes,
            collisions,
            issues,
        })
    }

    fn records_map(&self) -> Result<BTreeMap<WirePath, PathRecord>> {
        let read = self.database.begin_read()?;
        let table = read.open_table(RECORDS)?;
        let mut records = BTreeMap::new();
        for item in table.iter()? {
            let (key, encoded) = item?;
            let record: PathRecord =
                postcard::from_bytes(encoded.value()).context("invalid path record in index DB")?;
            if record.schema_version != INDEX_SCHEMA_V1 {
                bail!("unsupported path-record schema {}", record.schema_version);
            }
            if key.value() != record.path.as_str() {
                bail!("index record key does not match its encoded path");
            }
            if record.collision_key != collision_key(&record.path) {
                bail!("index record has an invalid collision key");
            }
            records.insert(record.path.clone(), record);
        }
        Ok(records)
    }

    fn retries_map(&self) -> Result<BTreeMap<WirePath, RetryRecord>> {
        let read = self.database.begin_read()?;
        let table = read.open_table(RETRIES)?;
        let mut retries = BTreeMap::new();
        for item in table.iter()? {
            let (key, encoded) = item?;
            let retry: RetryRecord = postcard::from_bytes(encoded.value())
                .context("invalid retry record in index DB")?;
            if key.value() != retry.path.as_str() {
                bail!("retry key does not match its encoded path");
            }
            retries.insert(retry.path.clone(), retry);
        }
        Ok(retries)
    }

    fn metadata_value(&self, key: &str) -> Result<u64> {
        let read = self.database.begin_read()?;
        let table = read.open_table(METADATA)?;
        Ok(table.get(key)?.map(|value| value.value()).unwrap_or(0))
    }

    fn wire_path_is_ignored(&self, path: &WirePath) -> bool {
        let mut local_path = self.root.clone();
        for component in path.components() {
            local_path.push(component);
        }
        self.ignored_paths
            .iter()
            .any(|ignored| local_path.starts_with(ignored))
    }

    fn commit_state(
        &self,
        records: &BTreeMap<WirePath, PathRecord>,
        retries: &BTreeMap<WirePath, RetryRecord>,
        generation: u64,
        replica_counter: u64,
    ) -> Result<()> {
        let write = self.database.begin_write()?;
        {
            let mut table = write.open_table(RECORDS)?;
            for (path, record) in records {
                let encoded = postcard::to_stdvec(record)?;
                let needs_update = table
                    .get(path.as_str())?
                    .is_none_or(|existing| existing.value() != encoded);
                if needs_update {
                    table.insert(path.as_str(), encoded.as_slice())?;
                }
            }
        }
        {
            let mut table = write.open_table(RETRIES)?;
            let retry_keys: BTreeSet<_> = retries.keys().map(|path| path.as_str()).collect();
            let existing = table
                .iter()?
                .map(|item| item.map(|(key, _)| key.value().to_owned()))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            for key in existing {
                if !retry_keys.contains(key.as_str()) {
                    table.remove(key.as_str())?;
                }
            }
            for (path, retry) in retries {
                let encoded = postcard::to_stdvec(retry)?;
                let needs_update = table
                    .get(path.as_str())?
                    .is_none_or(|existing| existing.value() != encoded);
                if needs_update {
                    table.insert(path.as_str(), encoded.as_slice())?;
                }
            }
        }
        {
            let mut table = write.open_table(METADATA)?;
            table.insert(GENERATION_KEY, generation)?;
            table.insert(REPLICA_COUNTER_KEY, replica_counter)?;
        }
        write.commit()?;
        Ok(())
    }
}

#[derive(Default)]
struct Traversal {
    candidates: Vec<RawEntry>,
    uncertain_paths: BTreeSet<WirePath>,
    uncertain_prefixes: Vec<WirePath>,
    preserve_all_missing: bool,
    issues: Vec<ScanIssue>,
}

#[derive(Clone, Debug)]
struct RawEntry {
    local_path: PathBuf,
    path: WirePath,
    kind: EntryKind,
    fingerprint: MetadataFingerprint,
}

impl RawEntry {
    fn into_scanned(self, content_hash: Option<Hash32>) -> ScannedEntry {
        ScannedEntry {
            path: self.path,
            kind: self.kind,
            fingerprint: self.fingerprint,
            content_hash,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MetadataFingerprint {
    identity: Option<FileIdentity>,
    size: u64,
    modified_ns: Option<u128>,
    changed_ns: Option<u128>,
    readonly: bool,
}

#[derive(Clone, Debug)]
struct ScannedEntry {
    path: WirePath,
    kind: EntryKind,
    fingerprint: MetadataFingerprint,
    content_hash: Option<Hash32>,
}

impl ScannedEntry {
    fn to_record(&self, version: VersionVector, generation: u64) -> PathRecord {
        PathRecord {
            schema_version: INDEX_SCHEMA_V1,
            path: self.path.clone(),
            collision_key: collision_key(&self.path),
            kind: self.kind,
            identity: self.fingerprint.identity,
            size: self.fingerprint.size,
            modified_ns: self.fingerprint.modified_ns,
            readonly: self.fingerprint.readonly,
            content_hash: self.content_hash,
            version,
            tombstone: false,
            generation,
        }
    }
}

struct HashOutcome {
    candidate: RawEntry,
    result: std::result::Result<Hash32, FileHashFailure>,
}

struct FileHashFailure {
    kind: ScanIssueKind,
    message: String,
}

struct HashFailure {
    path: WirePath,
    message: String,
}

fn initialize_tables(database: &Database) -> Result<()> {
    let write = database.begin_write()?;
    {
        let _ = write.open_table(RECORDS)?;
        let _ = write.open_table(RETRIES)?;
        let _ = write.open_table(METADATA)?;
        let _ = write.open_table(CONFIG)?;
    }
    write.commit()?;
    Ok(())
}

fn bind_database(database: &Database, root: &Path, replica: ReplicaId) -> Result<()> {
    let schema = INDEX_SCHEMA_V1.to_le_bytes();
    let root = root_binding(root);
    let expected = [
        (CONFIG_SCHEMA_KEY, schema.as_slice()),
        (CONFIG_ROOT_KEY, root.as_bytes().as_slice()),
        (CONFIG_REPLICA_KEY, replica.0.as_bytes().as_slice()),
    ];
    let write = database.begin_write()?;
    {
        let mut table = write.open_table(CONFIG)?;
        for (key, value) in expected {
            if let Some(existing) = table.get(key)? {
                if existing.value() != value {
                    bail!("index DB is bound to a different {key}");
                }
            } else {
                table.insert(key, value)?;
            }
        }
    }
    write.commit()?;
    Ok(())
}

fn root_binding(root: &Path) -> Hash32 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"deltaweave-index-root-v1\0");
    hasher.update(std::env::consts::OS.as_bytes());
    hasher.update(b"\0");
    update_path_hash(&mut hasher, root);
    Hash32::from_bytes(*hasher.finalize().as_bytes())
}

#[cfg(unix)]
fn update_path_hash(hasher: &mut blake3::Hasher, path: &Path) {
    use std::os::unix::ffi::OsStrExt;
    hasher.update(path.as_os_str().as_bytes());
}

#[cfg(windows)]
fn update_path_hash(hasher: &mut blake3::Hasher, path: &Path) {
    use std::os::windows::ffi::OsStrExt;
    for unit in path.as_os_str().encode_wide() {
        hasher.update(&unit.to_le_bytes());
    }
}

#[cfg(not(any(unix, windows)))]
fn update_path_hash(hasher: &mut blake3::Hasher, path: &Path) {
    hasher.update(path.to_string_lossy().as_bytes());
}

fn collect_directory(
    root: &Path,
    directory: &Path,
    ignored_paths: &[PathBuf],
    is_root: bool,
    traversal: &mut Traversal,
) -> Result<()> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if is_root => {
            return Err(error).with_context(|| {
                format!("failed to enumerate index root {}", directory.display())
            });
        }
        Err(error) => {
            if let Ok(path) = portable_path(root, directory) {
                traversal.uncertain_prefixes.push(path);
            }
            traversal.issues.push(ScanIssue {
                path: directory.display().to_string(),
                kind: ScanIssueKind::EnumerationFailed,
                message: bounded_message(&error.to_string()),
            });
            return Ok(());
        }
    };

    let mut collected = Vec::new();
    for entry in entries {
        match entry {
            Ok(entry) => collected.push(entry),
            Err(error) if is_root => {
                return Err(error).with_context(|| {
                    format!(
                        "failed while enumerating index root {}",
                        directory.display()
                    )
                });
            }
            Err(error) => {
                mark_directory_uncertain(root, directory, false, traversal);
                traversal.issues.push(ScanIssue {
                    path: directory.display().to_string(),
                    kind: ScanIssueKind::EnumerationFailed,
                    message: bounded_message(&error.to_string()),
                });
            }
        }
    }
    let mut entries = collected;
    entries.sort_by_key(fs::DirEntry::file_name);

    for entry in entries {
        let local_path = entry.path();
        if ignored_paths
            .iter()
            .any(|ignored| local_path.starts_with(ignored))
        {
            continue;
        }
        let path = match portable_path(root, &local_path) {
            Ok(path) => path,
            Err(message) => {
                mark_directory_uncertain(root, directory, is_root, traversal);
                traversal.issues.push(ScanIssue {
                    path: local_path.display().to_string(),
                    kind: ScanIssueKind::NonPortablePath,
                    message: bounded_message(&message),
                });
                continue;
            }
        };
        let metadata = match fs::symlink_metadata(&local_path) {
            Ok(metadata) => metadata,
            Err(error) => {
                traversal.uncertain_paths.insert(path.clone());
                traversal.uncertain_prefixes.push(path);
                traversal.issues.push(ScanIssue {
                    path: local_path.display().to_string(),
                    kind: ScanIssueKind::EnumerationFailed,
                    message: bounded_message(&error.to_string()),
                });
                continue;
            }
        };
        let kind = entry_kind(&metadata);
        let fingerprint = metadata_fingerprint(&local_path, &metadata, kind);
        traversal.candidates.push(RawEntry {
            local_path: local_path.clone(),
            path: path.clone(),
            kind,
            fingerprint,
        });
        if kind == EntryKind::Directory {
            collect_directory(root, &local_path, ignored_paths, false, traversal)?;
        }
    }
    Ok(())
}

fn mark_directory_uncertain(
    root: &Path,
    directory: &Path,
    is_root: bool,
    traversal: &mut Traversal,
) {
    if is_root {
        traversal.preserve_all_missing = true;
    } else if let Ok(path) = portable_path(root, directory) {
        traversal.uncertain_prefixes.push(path);
    }
}

fn portable_path(root: &Path, path: &Path) -> std::result::Result<WirePath, String> {
    let relative = path
        .strip_prefix(root)
        .map_err(|error| format!("path escaped root: {error}"))?;
    let mut components = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(value) => {
                let value = value
                    .to_str()
                    .ok_or_else(|| "path is not valid UTF-8".to_owned())?;
                components.push(value);
            }
            _ => return Err("path contains a non-normal component".to_owned()),
        }
    }
    WirePath::new(components.join("/")).map_err(|error| error.to_string())
}

fn local_path(root: &Path, path: &WirePath) -> PathBuf {
    let mut local = root.to_path_buf();
    for component in path.components() {
        local.push(component);
    }
    local
}

const fn sync_entry_kind(kind: SyncEntryKind) -> EntryKind {
    match kind {
        SyncEntryKind::File => EntryKind::File,
        SyncEntryKind::Directory => EntryKind::Directory,
        SyncEntryKind::Symlink => EntryKind::Symlink,
        SyncEntryKind::Other => EntryKind::Other,
    }
}

fn entry_kind(metadata: &Metadata) -> EntryKind {
    let file_type = metadata.file_type();
    if file_type.is_symlink() || is_windows_reparse_point(metadata) {
        EntryKind::Symlink
    } else if file_type.is_file() {
        EntryKind::File
    } else if file_type.is_dir() {
        EntryKind::Directory
    } else {
        EntryKind::Other
    }
}

#[cfg(windows)]
fn is_windows_reparse_point(metadata: &Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(windows))]
const fn is_windows_reparse_point(_metadata: &Metadata) -> bool {
    false
}

fn metadata_fingerprint(path: &Path, metadata: &Metadata, kind: EntryKind) -> MetadataFingerprint {
    MetadataFingerprint {
        identity: file_identity(path, metadata, kind),
        size: if kind == EntryKind::File {
            metadata.len()
        } else {
            0
        },
        modified_ns: (kind == EntryKind::File)
            .then(|| modified_ns(metadata))
            .flatten(),
        changed_ns: (kind == EntryKind::File)
            .then(|| change_time_ns(metadata))
            .flatten(),
        readonly: kind == EntryKind::File && metadata.permissions().readonly(),
    }
}

fn modified_ns(metadata: &Metadata) -> Option<u128> {
    metadata
        .modified()
        .ok()?
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_nanos())
}

#[cfg(unix)]
fn change_time_ns(metadata: &Metadata) -> Option<u128> {
    use std::os::unix::fs::MetadataExt;
    let seconds = u128::try_from(metadata.ctime()).ok()?;
    let nanos = u128::try_from(metadata.ctime_nsec()).ok()?;
    Some(seconds.saturating_mul(1_000_000_000).saturating_add(nanos))
}

#[cfg(not(unix))]
fn change_time_ns(_metadata: &Metadata) -> Option<u128> {
    None
}

#[cfg(unix)]
fn file_identity(_path: &Path, metadata: &Metadata, _kind: EntryKind) -> Option<FileIdentity> {
    use std::os::unix::fs::MetadataExt;
    if metadata.ino() == 0 {
        return None;
    }
    Some(FileIdentity {
        namespace: metadata.dev(),
        object: metadata.ino(),
    })
}

#[cfg(windows)]
fn file_identity(path: &Path, _metadata: &Metadata, kind: EntryKind) -> Option<FileIdentity> {
    if kind == EntryKind::Symlink {
        return None;
    }
    let handle = winapi_util::Handle::from_path_any(path).ok()?;
    let information = winapi_util::file::information(&handle).ok()?;
    if information.file_index() == 0 {
        return None;
    }
    Some(FileIdentity {
        namespace: information.volume_serial_number(),
        object: information.file_index(),
    })
}

#[cfg(not(any(unix, windows)))]
fn file_identity(_path: &Path, _metadata: &Metadata, _kind: EntryKind) -> Option<FileIdentity> {
    None
}

fn hash_files(candidates: Vec<RawEntry>, worker_limit: usize) -> Vec<HashOutcome> {
    if candidates.is_empty() {
        return Vec::new();
    }
    let count = candidates.len();
    let workers = worker_limit.min(count).max(1);
    let queue = Arc::new(Mutex::new(VecDeque::from(candidates)));
    let (sender, receiver) = mpsc::channel();
    std::thread::scope(|scope| {
        for _ in 0..workers {
            let queue = Arc::clone(&queue);
            let sender = sender.clone();
            scope.spawn(move || {
                loop {
                    let candidate = {
                        let Ok(mut queue) = queue.lock() else {
                            return;
                        };
                        queue.pop_front()
                    };
                    let Some(candidate) = candidate else {
                        return;
                    };
                    let result = hash_stable_file(&candidate.local_path, candidate.fingerprint);
                    if sender.send(HashOutcome { candidate, result }).is_err() {
                        return;
                    }
                }
            });
        }
    });
    drop(sender);
    let mut outcomes: Vec<_> = receiver.into_iter().collect();
    outcomes.sort_by(|left, right| left.candidate.path.cmp(&right.candidate.path));
    outcomes
}

fn hash_stable_file(
    path: &Path,
    before: MetadataFingerprint,
) -> std::result::Result<Hash32, FileHashFailure> {
    let mut file = File::open(path).map_err(|error| FileHashFailure {
        kind: ScanIssueKind::HashFailed,
        message: error.to_string(),
    })?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|error| FileHashFailure {
            kind: ScanIssueKind::HashFailed,
            message: error.to_string(),
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let after_metadata = fs::symlink_metadata(path).map_err(|error| FileHashFailure {
        kind: ScanIssueKind::MutatedDuringRead,
        message: format!("metadata disappeared after read: {error}"),
    })?;
    let after_kind = entry_kind(&after_metadata);
    let after = metadata_fingerprint(path, &after_metadata, after_kind);
    if after_kind != EntryKind::File || before != after {
        return Err(FileHashFailure {
            kind: ScanIssueKind::MutatedDuringRead,
            message: "file identity, size, permissions, or modification time changed while hashing"
                .to_owned(),
        });
    }
    Ok(Hash32::from_bytes(*hasher.finalize().as_bytes()))
}

fn correlate_renames(
    previous: &BTreeMap<WirePath, PathRecord>,
    current: &BTreeMap<WirePath, ScannedEntry>,
    preserved: &BTreeSet<WirePath>,
    uncertain_prefixes: &[WirePath],
    preserve_all_missing: bool,
) -> BTreeMap<WirePath, WirePath> {
    let mut old_by_identity: HashMap<(EntryKind, FileIdentity), Vec<WirePath>> = HashMap::new();
    let mut new_by_identity: HashMap<(EntryKind, FileIdentity), Vec<WirePath>> = HashMap::new();
    for (path, record) in previous {
        if preserve_all_missing
            || record.tombstone
            || current.contains_key(path)
            || should_preserve(path, preserved, uncertain_prefixes)
        {
            continue;
        }
        if let Some(identity) = record.identity {
            old_by_identity
                .entry((record.kind, identity))
                .or_default()
                .push(path.clone());
        }
    }
    for (path, entry) in current {
        if previous.get(path).is_some_and(|record| !record.tombstone) {
            continue;
        }
        if let Some(identity) = entry.fingerprint.identity {
            new_by_identity
                .entry((entry.kind, identity))
                .or_default()
                .push(path.clone());
        }
    }

    let mut renames = BTreeMap::new();
    for (identity, old_paths) in old_by_identity {
        let Some(new_paths) = new_by_identity.get(&identity) else {
            continue;
        };
        if old_paths.len() == 1 && new_paths.len() == 1 {
            let old_path = &old_paths[0];
            let new_path = &new_paths[0];
            if previous
                .get(old_path)
                .zip(current.get(new_path))
                .is_some_and(|(old, new)| record_matches(old, new))
            {
                renames.insert(new_path.clone(), old_path.clone());
            }
        }
    }
    renames
}

fn record_matches(record: &PathRecord, entry: &ScannedEntry) -> bool {
    record.kind == entry.kind
        && record.identity == entry.fingerprint.identity
        && record.size == entry.fingerprint.size
        && record.modified_ns == entry.fingerprint.modified_ns
        && record.readonly == entry.fingerprint.readonly
        && record.content_hash == entry.content_hash
}

fn record_fingerprint_matches(record: &PathRecord, entry: &RawEntry) -> bool {
    record.kind == entry.kind
        && record.identity == entry.fingerprint.identity
        && record.size == entry.fingerprint.size
        && record.modified_ns == entry.fingerprint.modified_ns
        && record.readonly == entry.fingerprint.readonly
}

fn reliable_changed_ns_match(observed: Option<u128>, current: Option<u128>) -> bool {
    matches!((observed, current), (Some(observed), Some(current)) if observed == current)
}

fn observation_trusts_no_rehash(
    observation: Option<&MaterializationObservation>,
    fingerprint: &MetadataFingerprint,
    record: &SyncRecord,
) -> bool {
    let Some(observation) = observation else {
        return false;
    };
    if !reliable_changed_ns_match(observation.changed_ns(), fingerprint.changed_ns) {
        return false;
    }
    observation.file_hash() == record.content_hash.unwrap_or_default()
        && observation.size() == fingerprint.size
        && observation.size() == record.size
        && observation.modified_ns() == fingerprint.modified_ns
        && observation.readonly() == fingerprint.readonly
        && observation.readonly() == record.readonly
        && observation.identity()
            == fingerprint
                .identity
                .map(|identity| (identity.namespace, identity.object))
}

fn next_replica_counter(global: u64, version: &VersionVector, replica: ReplicaId) -> Result<u64> {
    global
        .max(version.get(replica))
        .checked_add(1)
        .context("local replica counter overflow")
}

fn should_preserve(
    path: &WirePath,
    preserved: &BTreeSet<WirePath>,
    uncertain_prefixes: &[WirePath],
) -> bool {
    preserved.contains(path)
        || uncertain_prefixes.iter().any(|prefix| {
            path == prefix
                || path
                    .as_str()
                    .strip_prefix(prefix.as_str())
                    .is_some_and(|suffix| suffix.starts_with('/'))
        })
}

fn schedule_retry(
    previous: Option<&RetryRecord>,
    path: WirePath,
    now_ms: u64,
    message: String,
) -> RetryRecord {
    let attempts = previous.map_or(1, |retry| retry.attempts.saturating_add(1));
    let shift = attempts.saturating_sub(1).min(31);
    let delay = RETRY_BASE_MS
        .saturating_mul(1_u64 << shift)
        .min(RETRY_MAX_MS);
    RetryRecord {
        path,
        attempts,
        not_before_ms: now_ms.saturating_add(delay),
        last_error: bounded_message(&message),
    }
}

/// Builds a conservative Unicode-normalized, case-insensitive comparison key.
#[must_use]
pub fn collision_key(path: &WirePath) -> String {
    path.components()
        .map(|component| {
            let normalized: String = component.nfkc().collect();
            CaseMapper::new()
                .fold_string(&normalized)
                .nfkc()
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn collision_groups<'a>(records: impl Iterator<Item = &'a PathRecord>) -> Vec<CollisionGroup> {
    let mut groups: BTreeMap<String, Vec<WirePath>> = BTreeMap::new();
    for record in records {
        groups
            .entry(record.collision_key.clone())
            .or_default()
            .push(record.path.clone());
    }
    groups
        .into_iter()
        .filter_map(|(collision_key, mut paths)| {
            paths.sort();
            (paths.len() > 1).then_some(CollisionGroup {
                collision_key,
                paths,
            })
        })
        .collect()
}

fn change_key(change: &ScanChange) -> (&str, &str) {
    match change {
        ScanChange::Created { path } => (path.as_str(), "created"),
        ScanChange::Modified { path } => (path.as_str(), "modified"),
        ScanChange::Deleted { path } => (path.as_str(), "deleted"),
        ScanChange::Renamed { from, .. } => (from.as_str(), "renamed"),
    }
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    if absolute.exists() {
        return fs::canonicalize(&absolute)
            .with_context(|| format!("failed to resolve path {}", absolute.display()));
    }

    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
        }
    }
    Ok(normalized)
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn bounded_message(message: &str) -> String {
    message.chars().take(1024).collect()
}

/// Debounces event bursts without sleeping inside correctness tests.
#[derive(Debug)]
pub struct EventCoalescer {
    debounce: Duration,
    maximum_delay: Duration,
    pending: Option<PendingBatch>,
}

#[derive(Debug)]
struct PendingBatch {
    first: Instant,
    last: Instant,
    event_count: usize,
    rescan_required: bool,
}

/// One watcher trigger. A full authoritative scan is always safe for either variant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchTrigger {
    /// Number of native events represented by this batch.
    pub event_count: usize,
    /// True when the watcher reported loss, ambiguity, or an internal error.
    pub rescan_required: bool,
    /// Normalized paths mentioned by the native event batch.
    pub changed_paths: Vec<PathBuf>,
}

impl EventCoalescer {
    /// Creates an adaptive debounce window with a hard maximum delay.
    #[must_use]
    pub fn new(debounce: Duration, maximum_delay: Duration) -> Self {
        Self {
            debounce,
            maximum_delay: maximum_delay.max(debounce),
            pending: None,
        }
    }

    /// Adds one event at a caller-supplied monotonic time.
    pub fn push(&mut self, at: Instant, rescan_required: bool) {
        match &mut self.pending {
            Some(pending) => {
                pending.last = at;
                pending.event_count = pending.event_count.saturating_add(1);
                pending.rescan_required |= rescan_required;
            }
            None => {
                self.pending = Some(PendingBatch {
                    first: at,
                    last: at,
                    event_count: 1,
                    rescan_required,
                });
            }
        }
    }

    /// Returns and clears a ready batch.
    pub fn take_ready(&mut self, now: Instant) -> Option<WatchTrigger> {
        let pending = self.pending.as_ref()?;
        let quiet_deadline = pending.last + self.debounce;
        let maximum_deadline = pending.first + self.maximum_delay;
        if !pending.rescan_required && now < quiet_deadline && now < maximum_deadline {
            return None;
        }
        let pending = self.pending.take()?;
        Some(WatchTrigger {
            event_count: pending.event_count,
            rescan_required: pending.rescan_required,
            changed_paths: Vec::new(),
        })
    }
}

/// Native recursive watcher that treats events only as hints for an authoritative scan.
pub struct WatchService {
    _watcher: RecommendedWatcher,
    receiver: mpsc::Receiver<notify::Result<Event>>,
    coalescer: EventCoalescer,
    ignored_paths: Vec<PathBuf>,
    changed_paths: BTreeSet<PathBuf>,
}

impl std::fmt::Debug for WatchService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("WatchService(..)")
    }
}

impl WatchService {
    /// Starts a recursive native watcher for `root`.
    pub fn new(
        root: impl AsRef<Path>,
        ignored_paths: &[PathBuf],
        debounce: Duration,
        maximum_delay: Duration,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::channel();
        let mut watcher = notify::recommended_watcher(move |result| {
            let _ = sender.send(result);
        })?;
        watcher.watch(root.as_ref(), RecursiveMode::Recursive)?;
        Ok(Self {
            _watcher: watcher,
            receiver,
            coalescer: EventCoalescer::new(debounce, maximum_delay),
            ignored_paths: ignored_paths.to_vec(),
            changed_paths: BTreeSet::new(),
        })
    }

    /// Drains native events and returns a ready debounced trigger when available.
    pub fn poll(&mut self, now: Instant) -> Option<WatchTrigger> {
        while let Ok(result) = self.receiver.try_recv() {
            match result {
                Ok(event) => {
                    if matches!(event.kind, EventKind::Access(_)) {
                        continue;
                    }
                    let paths: Vec<_> = event
                        .paths
                        .into_iter()
                        .map(|path| absolute_path(&path).unwrap_or(path))
                        .collect();
                    if !paths.is_empty()
                        && paths.iter().all(|path| {
                            self.ignored_paths
                                .iter()
                                .any(|ignored| path.starts_with(ignored))
                        })
                    {
                        continue;
                    }
                    let ambiguous = matches!(event.kind, EventKind::Any | EventKind::Other);
                    self.changed_paths.extend(paths);
                    self.coalescer.push(now, ambiguous);
                }
                Err(_) => self.coalescer.push(now, true),
            }
        }
        self.coalescer.take_ready(now).map(|mut trigger| {
            trigger.changed_paths = std::mem::take(&mut self.changed_paths)
                .into_iter()
                .collect();
            trigger
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    fn replica() -> ReplicaId {
        ReplicaId(Hash32::digest(b"test replica"))
    }

    fn open_index(root: &Path, state: &Path, now_ms: u64) -> LocalIndex {
        LocalIndex::open(
            root,
            state.join("index.redb"),
            replica(),
            IndexOptions {
                hash_workers: 2,
                ignored_paths: vec![state.to_path_buf()],
                now_ms: Some(now_ms),
            },
        )
        .expect("index can open")
    }

    #[test]
    fn no_rehash_requires_matching_reliable_change_times() {
        assert!(reliable_changed_ns_match(Some(7), Some(7)));
        assert!(!reliable_changed_ns_match(Some(7), Some(8)));
        assert!(!reliable_changed_ns_match(None, Some(7)));
        assert!(!reliable_changed_ns_match(Some(7), None));
        assert!(!reliable_changed_ns_match(None, None));
    }

    #[test]
    fn initial_scan_persists_files_and_directories() {
        let root = TempDir::new().expect("root can be created");
        let state = TempDir::new().expect("state can be created");
        fs::create_dir(root.path().join("docs")).expect("directory can be created");
        fs::write(root.path().join("docs/report.txt"), b"alpha").expect("file can be written");
        let index = open_index(root.path(), state.path(), 1_000);

        let report = index.scan().expect("scan succeeds");
        assert_eq!(report.live_records, 2);
        assert_eq!(report.files_hashed, 1);
        assert_eq!(report.changes.len(), 2);
        let path = WirePath::new("docs/report.txt").expect("path is portable");
        let record = index
            .get(&path)
            .expect("record can be read")
            .expect("record exists");
        assert_eq!(record.content_hash, Some(Hash32::digest(b"alpha")));
        assert!(!record.tombstone);

        drop(index);
        let reopened = open_index(root.path(), state.path(), 2_000);
        assert_eq!(reopened.records().expect("records load").len(), 2);
    }

    #[test]
    fn child_updates_do_not_create_directory_metadata_churn() {
        let root = TempDir::new().expect("root can be created");
        let state = TempDir::new().expect("state can be created");
        fs::create_dir(root.path().join("docs")).expect("directory can be created");
        fs::write(root.path().join("docs/first.txt"), b"first")
            .expect("first child can be written");
        let index = open_index(root.path(), state.path(), 1_000);
        index.scan().expect("initial scan succeeds");
        let directory = WirePath::new("docs").expect("path is portable");
        let before = index
            .get(&directory)
            .expect("record can be read")
            .expect("directory is indexed");

        fs::write(root.path().join("docs/second.txt"), b"second")
            .expect("second child can be written");
        let report = index.scan().expect("child update scan succeeds");
        let after = index
            .get(&directory)
            .expect("record can be read")
            .expect("directory remains indexed");
        assert_eq!(before.version, after.version);
        assert_eq!(before.modified_ns, None);
        assert_eq!(after.modified_ns, None);
        assert!(report.changes.iter().all(|change| {
            !matches!(change, ScanChange::Modified { path } if path == &directory)
        }));
    }

    #[test]
    fn verified_remote_version_survives_the_next_authoritative_scan() {
        let root = TempDir::new().expect("root can be created");
        let state = TempDir::new().expect("state can be created");
        fs::write(root.path().join("report.txt"), b"before").expect("file can be written");
        let index = open_index(root.path(), state.path(), 1_000);
        index.scan().expect("initial scan succeeds");

        let path = WirePath::new("report.txt").expect("path is portable");
        let mut remote = index
            .get(&path)
            .expect("record can be read")
            .expect("record exists")
            .to_sync_record();
        let remote_replica = ReplicaId(Hash32::digest(b"remote replica"));
        remote.version.observe(remote_replica, 7);
        remote.size = b"after".len() as u64;
        remote.content_hash = Some(Hash32::digest(b"after"));
        fs::write(root.path().join("report.txt"), b"after").expect("remote bytes are installed");

        index
            .adopt_verified_record(&remote)
            .expect("verified remote record can be adopted");
        assert_eq!(
            index
                .get(&path)
                .expect("record can be read")
                .expect("record exists")
                .version,
            remote.version
        );
        let scan = index.scan().expect("authoritative scan succeeds");
        assert!(scan.changes.is_empty());
        assert_eq!(scan.unchanged, 1);
        assert_eq!(index.sync_records().expect("snapshot loads"), vec![remote]);
    }

    #[test]
    fn remote_tombstone_requires_deletion_and_persists_exact_clock() {
        let root = TempDir::new().expect("root can be created");
        let state = TempDir::new().expect("state can be created");
        fs::write(root.path().join("gone.txt"), b"content").expect("file can be written");
        let index = open_index(root.path(), state.path(), 1_000);
        index.scan().expect("initial scan succeeds");
        let path = WirePath::new("gone.txt").expect("path is portable");
        let mut tombstone = index
            .get(&path)
            .expect("record can be read")
            .expect("record exists")
            .to_sync_record();
        tombstone
            .version
            .increment(ReplicaId(Hash32::digest(b"remote replica")))
            .expect("clock can advance");
        tombstone.tombstone = true;

        assert!(index.adopt_verified_record(&tombstone).is_err());
        fs::remove_file(root.path().join("gone.txt")).expect("file can be deleted");
        index
            .adopt_verified_record(&tombstone)
            .expect("tombstone can be adopted after deletion");
        assert_eq!(
            index.sync_records().expect("snapshot loads"),
            vec![tombstone]
        );
    }

    #[test]
    fn verified_materialization_observation_adopts_without_rehash_and_rejects_mutation() {
        let root = TempDir::new().expect("root can be created");
        let state = TempDir::new().expect("state can be created");
        let bytes = vec![0_u8; 64 * 1024];
        let store_state = TempDir::new().expect("store state can be created");
        let store = deltaweave_store::Store::open(store_state.path()).expect("store can open");
        let hash = Hash32::digest(&bytes);
        let manifest = deltaweave_core::FileManifest {
            schema_version: deltaweave_core::MANIFEST_SCHEMA_V1,
            size: bytes.len() as u64,
            file_hash: hash,
            profile: deltaweave_core::ChunkingProfile::DEFAULT,
            chunks: vec![deltaweave_core::ChunkDescriptor {
                offset: 0,
                length: bytes.len() as u32,
                hash,
            }],
        };
        store
            .chunks()
            .put_verified(hash, &bytes)
            .expect("chunk can be stored");
        let index = open_index(root.path(), state.path(), 1_000);
        let path = WirePath::new("report.txt").expect("path is portable");
        let observation = store
            .materialize(&manifest, &path, root.path())
            .expect("file can be materialized")
            .observation;
        let record = SyncRecord {
            schema_version: SYNC_RECORD_SCHEMA_V1,
            path: path.clone(),
            kind: SyncEntryKind::File,
            size: bytes.len() as u64,
            content_hash: Some(hash),
            readonly: false,
            version: VersionVector::default(),
            tombstone: false,
        };
        index
            .adopt_materialized_record(&record, &observation)
            .expect("matching observation can be adopted");
        let stored = index
            .get(&path)
            .expect("record can be read")
            .expect("record exists");
        assert_eq!(stored.content_hash, Some(hash));
        assert_eq!(stored.size, bytes.len() as u64);

        fs::write(root.path().join("report.txt"), vec![1_u8; bytes.len()])
            .expect("same-size materialized file can be changed");
        assert!(
            index
                .adopt_materialized_record(&record, &observation)
                .is_err()
        );
    }

    #[test]
    fn missing_change_time_falls_back_to_stable_hash_verification() {
        let root = TempDir::new().expect("root can be created");
        let state = TempDir::new().expect("state can be created");
        let bytes = vec![0_u8; 64 * 1024];
        let store_state = TempDir::new().expect("store state can be created");
        let store = deltaweave_store::Store::open(store_state.path()).expect("store can open");
        let hash = Hash32::digest(&bytes);
        let manifest = deltaweave_core::FileManifest {
            schema_version: deltaweave_core::MANIFEST_SCHEMA_V1,
            size: bytes.len() as u64,
            file_hash: hash,
            profile: deltaweave_core::ChunkingProfile::DEFAULT,
            chunks: vec![deltaweave_core::ChunkDescriptor {
                offset: 0,
                length: bytes.len() as u32,
                hash,
            }],
        };
        store
            .chunks()
            .put_verified(hash, &bytes)
            .expect("chunk can be stored");
        let index = open_index(root.path(), state.path(), 1_000);
        let path = WirePath::new("report.txt").expect("path is portable");
        let observation = store
            .materialize(&manifest, &path, root.path())
            .expect("file can be materialized")
            .observation;
        let local_path = root.path().join("report.txt");
        let metadata = fs::symlink_metadata(&local_path).expect("metadata can be read");
        let fingerprint = metadata_fingerprint(&local_path, &metadata, EntryKind::File);
        let record = SyncRecord {
            schema_version: SYNC_RECORD_SCHEMA_V1,
            path: path.clone(),
            kind: SyncEntryKind::File,
            size: bytes.len() as u64,
            content_hash: Some(hash),
            readonly: false,
            version: VersionVector::default(),
            tombstone: false,
        };
        assert!(!observation_trusts_no_rehash(
            Some(&observation),
            &MetadataFingerprint {
                changed_ns: None,
                ..fingerprint
            },
            &record
        ));
        assert!(!observation_trusts_no_rehash(None, &fingerprint, &record));
        if fingerprint.changed_ns.is_some() && observation.changed_ns().is_some() {
            assert!(observation_trusts_no_rehash(
                Some(&observation),
                &fingerprint,
                &record
            ));
        }
        index
            .adopt_materialized_record(&record, &observation)
            .expect("materialized observation can be adopted");
        let stored = index
            .get(&path)
            .expect("record can be read")
            .expect("record exists");
        assert_eq!(stored.content_hash, Some(hash));
        assert_eq!(stored.size, bytes.len() as u64);
    }

    #[test]
    fn remote_record_is_rejected_when_installed_content_does_not_match() {
        let root = TempDir::new().expect("root can be created");
        let state = TempDir::new().expect("state can be created");
        fs::write(root.path().join("report.txt"), b"before").expect("file can be written");
        let index = open_index(root.path(), state.path(), 1_000);
        index.scan().expect("initial scan succeeds");
        let path = WirePath::new("report.txt").expect("path is portable");
        let mut remote = index
            .get(&path)
            .expect("record can be read")
            .expect("record exists")
            .to_sync_record();
        remote.size = b"claimed".len() as u64;
        remote.content_hash = Some(Hash32::digest(b"claimed"));
        fs::write(root.path().join("report.txt"), b"different").expect("file can be written");

        assert!(index.adopt_verified_record(&remote).is_err());
    }

    #[test]
    fn incremental_scan_reuses_untouched_hashes() {
        let root = TempDir::new().expect("root can be created");
        let state = TempDir::new().expect("state can be created");
        let first = root.path().join("first.bin");
        let second = root.path().join("second.bin");
        fs::write(&first, b"first").expect("first file can be written");
        fs::write(&second, b"second").expect("second file can be written");
        let index = open_index(root.path(), state.path(), 1_000);
        assert_eq!(index.scan().expect("initial scan succeeds").files_hashed, 2);
        let first_generation = index
            .get(&WirePath::new("first.bin").expect("path is portable"))
            .expect("record loads")
            .expect("record exists")
            .generation;

        let cached = index
            .scan_incremental(&[])
            .expect("empty incremental scan succeeds");
        assert_eq!(cached.files_hashed, 0);
        assert_eq!(cached.unchanged, 2);
        assert_eq!(
            index
                .get(&WirePath::new("first.bin").expect("path is portable"))
                .expect("record loads")
                .expect("record exists")
                .generation,
            first_generation
        );

        fs::write(&second, b"second version").expect("second file can be modified");
        let changed = index
            .scan_incremental(std::slice::from_ref(&second))
            .expect("targeted incremental scan succeeds");
        assert_eq!(changed.files_hashed, 1);
        assert!(changed.changes.iter().any(|change| {
            matches!(change, ScanChange::Modified { path } if path.as_str() == "second.bin")
        }));
    }

    #[test]
    fn database_file_at_root_does_not_exclude_the_entire_root() {
        let root = TempDir::new().expect("root can be created");
        fs::write(root.path().join("payload.txt"), b"payload").expect("file can be written");
        let database = root.path().join("index.redb");
        let index = LocalIndex::open(
            root.path(),
            &database,
            replica(),
            IndexOptions {
                hash_workers: 1,
                now_ms: Some(1_000),
                ..IndexOptions::default()
            },
        )
        .expect("index can open");

        let report = index.scan().expect("scan succeeds");
        assert_eq!(report.live_records, 1);
        assert!(
            index
                .get(&WirePath::new("payload.txt").expect("path is portable"))
                .expect("lookup succeeds")
                .is_some()
        );
        assert_eq!(
            index.ignored_paths(),
            &[fs::canonicalize(database).unwrap()]
        );
    }

    #[test]
    fn newly_ignored_subtree_is_preserved_without_deletion_tombstones() {
        let root = TempDir::new().expect("root can be created");
        let state = TempDir::new().expect("state can be created");
        let ignored = root.path().join("ignored");
        fs::create_dir(&ignored).expect("directory can be created");
        fs::write(ignored.join("data.bin"), b"data").expect("file can be written");
        let index = open_index(root.path(), state.path(), 1_000);
        assert_eq!(index.scan().expect("initial scan succeeds").live_records, 2);
        drop(index);

        let reopened = LocalIndex::open(
            root.path(),
            state.path().join("index.redb"),
            replica(),
            IndexOptions {
                hash_workers: 1,
                ignored_paths: vec![ignored],
                now_ms: Some(2_000),
            },
        )
        .expect("index can reopen with an ignored subtree");
        let report = reopened.scan().expect("ignored scan succeeds");
        assert!(report.changes.is_empty());
        assert_eq!(report.live_records, 2);
        assert_eq!(report.tombstones, 0);
    }

    #[test]
    fn ignored_path_cannot_contain_the_index_root() {
        let root = TempDir::new().expect("root can be created");
        let state = TempDir::new().expect("state can be created");
        let error = LocalIndex::open(
            root.path(),
            state.path().join("index.redb"),
            replica(),
            IndexOptions {
                ignored_paths: vec![root.path().to_path_buf()],
                ..IndexOptions::default()
            },
        )
        .expect_err("ignoring the complete root is unsafe");
        assert!(error.to_string().contains("must not contain"));
    }

    #[test]
    fn database_is_bound_to_one_root_and_replica() {
        let first_root = TempDir::new().expect("first root can be created");
        let second_root = TempDir::new().expect("second root can be created");
        let state = TempDir::new().expect("state can be created");
        let database = state.path().join("index.redb");
        let index = LocalIndex::open(
            first_root.path(),
            &database,
            replica(),
            IndexOptions::default(),
        )
        .expect("index can open");
        drop(index);

        let root_error = LocalIndex::open(
            second_root.path(),
            &database,
            replica(),
            IndexOptions::default(),
        )
        .expect_err("database reuse for another root is unsafe");
        assert!(root_error.to_string().contains("different root"));

        let other_replica = ReplicaId(Hash32::digest(b"another replica"));
        let replica_error = LocalIndex::open(
            first_root.path(),
            &database,
            other_replica,
            IndexOptions::default(),
        )
        .expect_err("database reuse for another replica is unsafe");
        assert!(replica_error.to_string().contains("different replica"));
    }

    #[test]
    fn modifications_and_deletions_advance_state() {
        let root = TempDir::new().expect("root can be created");
        let state = TempDir::new().expect("state can be created");
        let file = root.path().join("data.bin");
        fs::write(&file, b"first").expect("file can be written");
        let index = open_index(root.path(), state.path(), 1_000);
        index.scan().expect("first scan succeeds");

        fs::write(&file, b"second version").expect("file can be modified");
        let modified = index.scan().expect("modified scan succeeds");
        assert!(modified.changes.iter().any(|change| {
            matches!(change, ScanChange::Modified { path } if path.as_str() == "data.bin")
        }));

        fs::remove_file(&file).expect("file can be removed");
        let deleted = index.scan().expect("delete scan succeeds");
        assert!(deleted.changes.iter().any(|change| {
            matches!(change, ScanChange::Deleted { path } if path.as_str() == "data.bin")
        }));
        assert!(
            index
                .get(&WirePath::new("data.bin").expect("path is portable"))
                .expect("record can be read")
                .expect("tombstone exists")
                .tombstone
        );
    }

    #[test]
    fn stable_identity_correlates_rename() {
        let root = TempDir::new().expect("root can be created");
        let state = TempDir::new().expect("state can be created");
        fs::write(root.path().join("before.txt"), b"same").expect("file can be written");
        let index = open_index(root.path(), state.path(), 1_000);
        index.scan().expect("first scan succeeds");

        fs::rename(
            root.path().join("before.txt"),
            root.path().join("after.txt"),
        )
        .expect("file can be renamed");
        let report = index.scan().expect("rename scan succeeds");
        assert!(report.changes.iter().any(|change| {
            matches!(
                change,
                ScanChange::Renamed { from, to }
                    if from.as_str() == "before.txt" && to.as_str() == "after.txt"
            )
        }));
    }

    #[test]
    fn rename_with_different_content_is_not_false_correlated() {
        let root = TempDir::new().expect("root can be created");
        let state = TempDir::new().expect("state can be created");
        let before = root.path().join("before.txt");
        let after = root.path().join("after.txt");
        fs::write(&before, b"old").expect("file can be written");
        let index = open_index(root.path(), state.path(), 1_000);
        index.scan().expect("first scan succeeds");

        fs::rename(&before, &after).expect("file can be renamed");
        fs::write(&after, b"completely different content").expect("file can be modified");
        let report = index.scan().expect("second scan succeeds");
        assert!(
            !report
                .changes
                .iter()
                .any(|change| { matches!(change, ScanChange::Renamed { .. }) })
        );
        assert!(report.changes.iter().any(|change| {
            matches!(change, ScanChange::Deleted { path } if path.as_str() == "before.txt")
        }));
        assert!(report.changes.iter().any(|change| {
            matches!(change, ScanChange::Created { path } if path.as_str() == "after.txt")
        }));
    }

    #[test]
    fn collision_key_normalizes_case_and_unicode_composition() {
        let upper = WirePath::new("Reports/CAFÉ.txt").expect("path is portable");
        let decomposed = WirePath::new("reports/CAFE\u{301}.TXT").expect("path is portable");
        assert_eq!(collision_key(&upper), collision_key(&decomposed));

        let sharp_s = WirePath::new("Straße.txt").expect("path is portable");
        let expanded = WirePath::new("STRASSE.TXT").expect("path is portable");
        assert_eq!(collision_key(&sharp_s), collision_key(&expanded));

        let final_sigma = WirePath::new("κόσμος.txt").expect("path is portable");
        let standard_sigma = WirePath::new("ΚΌΣΜΟΣ.TXT").expect("path is portable");
        assert_eq!(collision_key(&final_sigma), collision_key(&standard_sigma));
    }

    #[cfg(not(windows))]
    #[test]
    fn scan_reports_case_collisions_without_overwriting_records() {
        let root = TempDir::new().expect("root can be created");
        let state = TempDir::new().expect("state can be created");
        fs::write(root.path().join("Report.txt"), b"upper").expect("file can be written");
        fs::write(root.path().join("report.txt"), b"lower").expect("file can be written");
        let index = open_index(root.path(), state.path(), 1_000);
        let report = index.scan().expect("scan succeeds");
        assert_eq!(report.collisions.len(), 1);
        assert_eq!(report.collisions[0].paths.len(), 2);
        assert_eq!(report.live_records, 2);
    }

    #[test]
    fn stale_metadata_detects_mutation_and_retry_is_exponential() {
        let root = TempDir::new().expect("root can be created");
        let file = root.path().join("changing.bin");
        fs::write(&file, b"one").expect("file can be written");
        let metadata = fs::symlink_metadata(&file).expect("metadata can be read");
        let before = metadata_fingerprint(&file, &metadata, EntryKind::File);
        fs::write(&file, b"a different length").expect("file can be changed");
        let failure = hash_stable_file(&file, before).expect_err("mutation is detected");
        assert_eq!(failure.kind, ScanIssueKind::MutatedDuringRead);

        let path = WirePath::new("changing.bin").expect("path is portable");
        let first = schedule_retry(None, path.clone(), 1_000, "locked".to_owned());
        assert_eq!(first.not_before_ms, 1_500);
        let second = schedule_retry(Some(&first), path, 1_500, "locked".to_owned());
        assert_eq!(second.not_before_ms, 2_500);
        assert_eq!(second.attempts, 2);
    }

    #[test]
    fn retry_queue_survives_restart_and_defers_early_retry() {
        let root = TempDir::new().expect("root can be created");
        let state = TempDir::new().expect("state can be created");
        fs::write(root.path().join("locked.bin"), b"data").expect("file can be written");
        let index = open_index(root.path(), state.path(), 1_000);
        index.scan().expect("initial scan succeeds");
        let path = WirePath::new("locked.bin").expect("path is portable");
        let previous = index.records_map().expect("records load");
        let mut preserved = BTreeSet::new();
        preserved.insert(path.clone());
        index
            .apply_scan(
                previous,
                BTreeMap::new(),
                Vec::new(),
                preserved,
                Vec::new(),
                false,
                vec![HashFailure {
                    path: path.clone(),
                    message: "sharing violation".to_owned(),
                }],
                Vec::new(),
                0,
                2_000,
            )
            .expect("failure state commits");
        assert_eq!(
            index.retries().expect("retries load")[0].not_before_ms,
            2_500
        );

        drop(index);
        let reopened = open_index(root.path(), state.path(), 2_250);
        let report = reopened.scan().expect("deferred scan succeeds");
        assert_eq!(report.retries_queued, 1);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.kind == ScanIssueKind::RetryDeferred)
        );
        assert!(
            !reopened
                .get(&path)
                .expect("record loads")
                .expect("record exists")
                .tombstone
        );
    }

    #[test]
    fn retry_for_disappeared_unindexed_file_is_removed() {
        let root = TempDir::new().expect("root can be created");
        let state = TempDir::new().expect("state can be created");
        let index = open_index(root.path(), state.path(), 1_000);
        let path = WirePath::new("vanished.bin").expect("path is portable");
        let mut preserved = BTreeSet::new();
        preserved.insert(path.clone());
        index
            .apply_scan(
                BTreeMap::new(),
                BTreeMap::new(),
                Vec::new(),
                preserved,
                Vec::new(),
                false,
                vec![HashFailure {
                    path,
                    message: "sharing violation".to_owned(),
                }],
                Vec::new(),
                0,
                1_000,
            )
            .expect("failure state commits");
        assert_eq!(index.retries().expect("retries load").len(), 1);

        let report = index.scan().expect("absence scan succeeds");
        assert_eq!(report.retries_queued, 0);
        assert!(index.retries().expect("retries load").is_empty());
    }

    #[cfg(windows)]
    #[test]
    fn windows_sharing_violation_is_retried_after_unlock() {
        use std::os::windows::fs::OpenOptionsExt;

        let root = TempDir::new().expect("root can be created");
        let state = TempDir::new().expect("state can be created");
        let file = root.path().join("locked.bin");
        fs::write(&file, b"locked data").expect("file can be written");
        let lock = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .share_mode(0)
            .open(&file)
            .expect("exclusive handle can open");
        let index = open_index(root.path(), state.path(), 1_000);
        let blocked = index.scan().expect("locked scan remains non-fatal");
        assert_eq!(blocked.live_records, 0);
        assert_eq!(blocked.retries_queued, 1);
        assert!(
            blocked
                .issues
                .iter()
                .any(|issue| issue.kind == ScanIssueKind::HashFailed)
        );
        drop(lock);
        drop(index);

        let reopened = open_index(root.path(), state.path(), 1_500);
        let recovered = reopened.scan().expect("unlocked retry succeeds");
        assert_eq!(recovered.live_records, 1);
        assert_eq!(recovered.retries_queued, 0);
    }

    #[test]
    fn uncertain_subtree_is_never_converted_to_deletions() {
        let root = TempDir::new().expect("root can be created");
        let state = TempDir::new().expect("state can be created");
        fs::create_dir(root.path().join("private")).expect("directory can be created");
        fs::write(root.path().join("private/data.bin"), b"data").expect("file can be written");
        let index = open_index(root.path(), state.path(), 1_000);
        index.scan().expect("initial scan succeeds");
        let prefix = WirePath::new("private").expect("path is portable");

        let report = index
            .apply_scan(
                index.records_map().expect("records load"),
                BTreeMap::new(),
                Vec::new(),
                BTreeSet::new(),
                vec![prefix],
                false,
                Vec::new(),
                Vec::new(),
                0,
                2_000,
            )
            .expect("partial observation commits safely");
        assert!(report.changes.is_empty());
        assert_eq!(report.live_records, 2);
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_entry_prevents_false_deletion_from_its_directory() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let root = TempDir::new().expect("root can be created");
        let state = TempDir::new().expect("state can be created");
        let original = root.path().join("portable.txt");
        fs::write(&original, b"data").expect("file can be written");
        let index = open_index(root.path(), state.path(), 1_000);
        index.scan().expect("initial scan succeeds");
        fs::rename(
            &original,
            root.path()
                .join(OsString::from_vec(b"invalid-\xff.txt".to_vec())),
        )
        .expect("file can be renamed");

        let report = index.scan().expect("incomplete scan remains safe");
        assert!(report.changes.is_empty());
        assert!(
            !index
                .get(&WirePath::new("portable.txt").expect("path is portable"))
                .expect("record loads")
                .expect("previous record is preserved")
                .tombstone
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.kind == ScanIssueKind::NonPortablePath)
        );
    }

    #[cfg(unix)]
    #[test]
    fn scanner_indexes_but_never_follows_symlinks() {
        use std::os::unix::fs::symlink;

        let root = TempDir::new().expect("root can be created");
        let state = TempDir::new().expect("state can be created");
        let outside = TempDir::new().expect("outside can be created");
        fs::write(outside.path().join("secret.txt"), b"secret")
            .expect("outside file can be written");
        symlink(outside.path(), root.path().join("linked")).expect("symlink can be made");
        let index = open_index(root.path(), state.path(), 1_000);
        let report = index.scan().expect("scan succeeds");
        assert_eq!(report.live_records, 1);
        let record = index
            .get(&WirePath::new("linked").expect("path is portable"))
            .expect("record can be read")
            .expect("symlink is indexed");
        assert_eq!(record.kind, EntryKind::Symlink);
        assert!(
            index
                .get(&WirePath::new("linked/secret.txt").expect("path is portable"))
                .expect("lookup succeeds")
                .is_none()
        );
    }

    #[cfg(windows)]
    #[test]
    fn scanner_never_follows_windows_directory_symlink() {
        use std::io::ErrorKind;
        use std::os::windows::fs::symlink_dir;

        let root = TempDir::new().expect("root can be created");
        let state = TempDir::new().expect("state can be created");
        let outside = TempDir::new().expect("outside can be created");
        fs::write(outside.path().join("secret.txt"), b"secret")
            .expect("outside file can be written");
        match symlink_dir(outside.path(), root.path().join("linked")) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::PermissionDenied => return,
            Err(error) => panic!("directory symlink failed: {error}"),
        }
        let index = open_index(root.path(), state.path(), 1_000);
        let report = index.scan().expect("scan succeeds");
        assert_eq!(report.live_records, 1);
        let record = index
            .get(&WirePath::new("linked").expect("path is portable"))
            .expect("record can be read")
            .expect("link is indexed");
        assert_eq!(record.kind, EntryKind::Symlink);
        assert!(
            index
                .get(&WirePath::new("linked/secret.txt").expect("path is portable"))
                .expect("lookup succeeds")
                .is_none()
        );
    }

    #[cfg(unix)]
    #[test]
    fn symbolic_link_index_root_is_rejected() {
        use std::os::unix::fs::symlink;

        let real_root = TempDir::new().expect("root can be created");
        let holder = TempDir::new().expect("holder can be created");
        let state = TempDir::new().expect("state can be created");
        let linked_root = holder.path().join("linked-root");
        symlink(real_root.path(), &linked_root).expect("symlink can be made");
        let error = LocalIndex::open(
            &linked_root,
            state.path().join("index.redb"),
            replica(),
            IndexOptions::default(),
        )
        .expect_err("symlink root must be rejected");
        assert!(error.to_string().contains("real directory"));
    }

    #[test]
    fn deterministic_operation_storm_converges_after_restart() {
        let root = TempDir::new().expect("root can be created");
        let state = TempDir::new().expect("state can be created");
        let mut expected = BTreeMap::new();
        let mut value = 0x9e37_79b9_u64;
        {
            let index = open_index(root.path(), state.path(), 1_000);
            for step in 0..80_u64 {
                value = value
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                let slot = value % 12;
                let name = format!("file-{slot}.bin");
                let path = root.path().join(&name);
                match value % 3 {
                    0 => {
                        let bytes = format!("step-{step}-value-{value}").into_bytes();
                        fs::write(&path, &bytes).expect("file can be written");
                        expected.insert(name, bytes);
                    }
                    1 => {
                        if path.exists() {
                            fs::remove_file(&path).expect("file can be removed");
                        }
                        expected.remove(&name);
                    }
                    _ => {
                        let other = format!("renamed-{slot}.bin");
                        let destination = root.path().join(&other);
                        if path.exists() && !destination.exists() {
                            fs::rename(&path, &destination).expect("file can be renamed");
                            if let Some(bytes) = expected.remove(&name) {
                                expected.insert(other, bytes);
                            }
                        }
                    }
                }
                index.scan().expect("operation scan succeeds");
            }
        }

        let reopened = open_index(root.path(), state.path(), 2_000);
        reopened.scan().expect("restart scan succeeds");
        let actual: BTreeMap<_, _> = reopened
            .records()
            .expect("records load")
            .into_iter()
            .filter(|record| record.kind == EntryKind::File && !record.tombstone)
            .map(|record| {
                (
                    record.path.as_str().to_owned(),
                    record.content_hash.expect("live file has a hash"),
                )
            })
            .collect();
        let expected_hashes: BTreeMap<_, _> = expected
            .into_iter()
            .map(|(path, bytes)| (path, Hash32::digest(&bytes)))
            .collect();
        assert_eq!(actual, expected_hashes);
    }

    #[test]
    fn event_coalescer_has_quiet_and_maximum_deadlines() {
        let start = Instant::now();
        let mut coalescer = EventCoalescer::new(Duration::from_millis(750), Duration::from_secs(3));
        coalescer.push(start, false);
        assert!(
            coalescer
                .take_ready(start + Duration::from_millis(749))
                .is_none()
        );
        let ready = coalescer
            .take_ready(start + Duration::from_millis(750))
            .expect("quiet window elapsed");
        assert_eq!(ready.event_count, 1);

        coalescer.push(start, true);
        assert!(
            coalescer
                .take_ready(start)
                .expect("loss is immediate")
                .rescan_required
        );
    }

    #[test]
    fn native_watcher_reports_created_path_with_bounded_wait() {
        let root = TempDir::new().expect("root can be created");
        let mut watcher = WatchService::new(root.path(), &[], Duration::ZERO, Duration::ZERO)
            .expect("native watcher can start");
        let created = root.path().join("created.txt");
        fs::write(&created, b"created").expect("file can be written");

        let deadline = Instant::now() + Duration::from_secs(5);
        let trigger = loop {
            if let Some(trigger) = watcher.poll(Instant::now()) {
                break trigger;
            }
            assert!(Instant::now() < deadline, "native watcher event timed out");
            std::thread::park_timeout(Duration::from_millis(10));
        };
        assert!(trigger.event_count > 0);
        let created = fs::canonicalize(created).expect("path resolves");
        assert!(trigger.changed_paths.iter().any(|path| path == &created));
    }
}
