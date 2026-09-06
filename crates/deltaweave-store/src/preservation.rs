//! Unique, durable path changes. Recovery artifacts are never disposable staging.
use super::*;
use anyhow::ensure;
use redb::ReadableTable;

const CHANGES: TableDefinition<&str, &[u8]> = TableDefinition::new("path_changes_v1");

/// Safe classification for preservation failures; its Display never includes a local path.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PreservationError {
    /// Select an ordinary enclosing folder with a private recovery location on its volume.
    RecoveryUnavailable,
    /// A concurrent local edit requires a fresh reconciliation.
    LocalChanged,
    /// Retained recovery metadata or an artifact is missing or inconsistent.
    StateUnavailable,
}
impl fmt::Display for PreservationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::RecoveryUnavailable => "private recovery unavailable; choose a folder with same-volume private recovery space",
            Self::LocalChanged => "local state changed; retry synchronization",
            Self::StateUnavailable => "retained recovery state unavailable",
        })
    }
}
impl std::error::Error for PreservationError {}

/// Host-level private namespace reservation, invoked before creating an external vault.
pub type RecoveryReserver = fn(&Path) -> Result<PathBuf>;

/// Durable progress of one unique filesystem attempt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PathChangeState {
    /// Exact paths and the expected local object are durable; capture has not been confirmed.
    Prepared,
    /// The displaced object is retained in the private vault.
    Preserved,
    /// The requested file, directory, or absence has been installed.
    Materialized,
    /// The caller durably adopted the exact target into its index.
    Indexed,
    /// The attempt is finished. Its artifact remains until explicit user purge.
    Committed,
    /// Local drift prevented application; any artifact remains available for recovery.
    Aborted,
    /// Recovery is atomically restoring the prior local object.
    RollingBack,
    /// The unadopted incoming object was retained and the prior namespace restored.
    RolledBack,
}

/// No-follow local precondition. Hashes include the entire file or directory tree.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PathObservation {
    identity: Option<(u64, u64)>,
    kind: u8,
    size: u64,
    modified_ns: Option<u128>,
    readonly: bool,
    hash: Hash32,
}

impl PathObservation {
    /// Observes a leaf without following symlinks; rejects symlink ancestors.
    pub fn read(root: &Path, path: &WirePath) -> Result<Option<Self>> {
        Self::at(&checked_destination(root, path)?)
    }

    /// Tests the observed bytes/kind/metadata against an index precondition.
    pub fn matches_record(&self, record: &deltaweave_core::SyncRecord) -> bool {
        !record.tombstone
            && self.readonly == record.readonly
            && match record.kind {
                deltaweave_core::SyncEntryKind::File => {
                    self.kind == 0
                        && self.size == record.size
                        && Some(self.hash) == record.content_hash
                }
                deltaweave_core::SyncEntryKind::Directory => self.kind == 1,
                deltaweave_core::SyncEntryKind::Symlink => self.kind == 2,
                deltaweave_core::SyncEntryKind::Other => false,
            }
    }

    fn at(path: &Path) -> Result<Option<Self>> {
        let meta = match fs::symlink_metadata(path) {
            Ok(meta) => meta,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let kind = if meta.file_type().is_symlink() {
            2
        } else if meta.is_file() {
            0
        } else if meta.is_dir() {
            1
        } else {
            bail!("unsupported local filesystem object")
        };
        let mut hasher = blake3::Hasher::new();
        if kind == 0 {
            // O_NOFOLLOW also protects a leaf swapped after symlink_metadata.
            let mut file = open_nofollow(path, false, false)?;
            ensure!(
                file_identity(path, &file.metadata()?) == file_identity(path, &meta),
                "local leaf changed while observing"
            );
            let mut buffer = [0; 65536];
            loop {
                let n = file.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buffer[..n]);
            }
        } else if kind == 2 {
            let target = fs::read_link(path)?;
            hasher.update(target.as_os_str().as_encoded_bytes());
        } else {
            let mut entries = fs::read_dir(path)?.collect::<std::io::Result<Vec<_>>>()?;
            entries.sort_by_key(|entry| entry.file_name());
            for entry in entries {
                hasher.update(entry.file_name().as_encoded_bytes());
                let child =
                    Self::at(&entry.path())?.context("directory changed while observing")?;
                hasher.update(&postcard::to_stdvec(&child)?);
            }
        }
        let after = fs::symlink_metadata(path)?;
        ensure!(
            file_identity(path, &meta) == file_identity(path, &after)
                && meta.len() == after.len()
                && modified_ns(&meta) == modified_ns(&after)
                && change_time_ns(&meta) == change_time_ns(&after),
            "local object changed while observing"
        );
        Ok(Some(Self {
            identity: file_identity(path, &meta),
            kind,
            size: if kind == 1 { 0 } else { meta.len() },
            modified_ns: modified_ns(&meta),
            readonly: meta.permissions().readonly(),
            hash: Hash32::from_bytes(*hasher.finalize().as_bytes()),
        }))
    }
}

/// Complete filesystem target persisted before a path is captured.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PathTarget {
    /// Verified incoming file assembled from CAS extents.
    File(FileManifest),
    /// Real directory, created atomically without replacing an occupant.
    Directory,
    /// No object at this path.
    Absent,
}

/// Trusted local causal intent persisted before capture. Authorization is opaque to Store.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CausalBinding {
    /// Exact target record, never reconstructed from installed bytes.
    pub record: deltaweave_core::SyncRecord,
    /// Exact durable index row before application, including genuine absence.
    pub precondition: Option<deltaweave_core::SyncRecord>,
    /// Bounded authenticated actor context owned and validated by the networking layer.
    pub authorization: Option<Vec<u8>>,
}

/// Versioned journal entry binding the exact artifact and staging paths to one root.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PathChange {
    /// Journal format version.
    pub version: u16,
    /// Unique attempt, independent of the logical target hash.
    pub id: String,
    /// Canonical public root.
    pub root: PathBuf,
    /// Portable destination.
    pub path: WirePath,
    /// Durable private root used for this attempt.
    pub vault: PathBuf,
    /// Exact retained object location; may be absent before capture or after restoration.
    pub artifact: PathBuf,
    /// Disposable incoming file; never report this as preserved user data.
    pub staging: PathBuf,
    /// Expected local object, including its content digest.
    pub expected: Option<PathObservation>,
    /// Target bound before capture.
    pub target: PathTarget,
    /// Latest durable stage.
    pub state: PathChangeState,
    /// Whether RO explicitly authorized preserving a complete directory tree.
    pub preserve_tree: bool,
    /// Exact causal intent and index precondition, when a causal caller initiated this attempt.
    pub causal: Option<CausalBinding>,
    /// Existing directory metadata update; the directory tree is not detached.
    pub metadata_only: bool,
    /// Retained incoming object when an unadopted operation is rolled back.
    pub rollback_artifact: PathBuf,
}

impl Store {
    fn vault(&self, root: &Path, destination_parent: &Path) -> Result<PathBuf> {
        self.select_vault(root, destination_parent)
            .context(PreservationError::RecoveryUnavailable)
    }

    fn select_vault(&self, root: &Path, destination_parent: &Path) -> Result<PathBuf> {
        validate_real_directory(&self.chunks.trash)?;
        let candidate = if same_volume(destination_parent, &self.chunks.trash)? {
            self.chunks.trash.clone()
        } else {
            let parent = root.parent().context(
                "no private same-volume recovery placement; choose an enclosing directory",
            )?;
            let name = Hash32::digest(root.as_os_str().as_encoded_bytes());
            let candidate = parent.join(format!(".deltaweave-{name}.recovery"));
            validate_real_directory(parent)?;
            match fs::symlink_metadata(&candidate) {
                Ok(metadata) => ensure!(
                    metadata.is_dir() && !metadata.file_type().is_symlink(),
                    "recovery vault is not a real directory"
                ),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            let reserve = self
                .recovery_reserver
                .context("private same-volume recovery requires host namespace admission")?;
            let reserved = reserve(&candidate)?;
            validate_real_directory(&candidate)?;
            ensure!(
                fs::canonicalize(&candidate)? == reserved,
                "recovery reservation changed destination"
            );
            reserved
        };
        validate_real_directory(&candidate)?;
        let vault = fs::canonicalize(candidate)?;
        ensure!(
            !vault.starts_with(root) && !root.starts_with(&vault),
            "recovery vault overlaps shared namespace"
        );
        ensure!(
            same_volume(destination_parent, &vault)?,
            "no private same-volume recovery placement; choose an enclosing directory"
        );
        Ok(vault)
    }

    /// Persists a unique attempt and verified incoming staging before any local capture.
    /// Callers serialize all stages through their mutation gate and retain their root lease.
    pub fn prepare_path_change(
        &self,
        root: &Path,
        path: &WirePath,
        target: PathTarget,
        expected: Option<PathObservation>,
        preserve_tree: bool,
    ) -> Result<PathChange> {
        let root = fs::canonicalize(root)?;
        let destination = checked_destination(&root, path)?;
        ensure!(
            PathObservation::at(&destination)? == expected,
            PreservationError::LocalChanged
        );
        if let Some(observation) = &expected
            && !preserve_tree
            && observation.kind == 1
            && fs::read_dir(&destination)?.next().is_some()
        {
            return Err(std::io::Error::from(std::io::ErrorKind::DirectoryNotEmpty).into());
        }
        let parent = destination.parent().context("destination lacks parent")?;
        fs::create_dir_all(parent)?;
        checked_destination(&root, path)?;
        let vault = self.vault(&root, parent)?;
        let directory = loop {
            let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let directory = vault.join(format!(
                "change-{}-{}-{sequence}",
                Hash32::digest(root.as_os_str().as_encoded_bytes()),
                std::process::id()
            ));
            match private_directory_builder().create(&directory) {
                Ok(()) => break directory,
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e.into()),
            }
        };
        // Preserve legacy recovery layouts, including nested wire paths.
        let artifact = directory.join(path.as_str());
        private_directory_builder()
            .recursive(true)
            .create(artifact.parent().context("artifact lacks parent")?)?;
        let change = PathChange {
            version: 1,
            id: directory
                .file_name()
                .context("attempt lacks id")?
                .to_string_lossy()
                .into_owned(),
            root,
            path: path.clone(),
            vault,
            artifact,
            staging: directory.with_extension("incoming"),
            expected,
            target,
            state: PathChangeState::Prepared,
            preserve_tree,
            causal: None,
            metadata_only: false,
            rollback_artifact: directory.with_extension("rollback"),
        };
        sync_directory(Some(&directory))?;
        sync_directory(directory.parent())?;
        self.put_change(&change)?;
        if let PathTarget::File(manifest) = &change.target {
            manifest.validate()?;
            let mut output = open_nofollow(&change.staging, true, true)?;
            let mut hasher = blake3::Hasher::new();
            for chunk in &manifest.chunks {
                let bytes = self.chunks.read_verified(chunk.hash)?;
                ensure!(
                    bytes.len() == chunk.length as usize,
                    "cached extent has incorrect length"
                );
                output.write_all(&bytes)?;
                hasher.update(&bytes);
            }
            ensure!(
                Hash32::from_bytes(*hasher.finalize().as_bytes()) == manifest.file_hash,
                "materialized file hash mismatch"
            );
            output.sync_all()?;
            sync_directory(change.staging.parent())?;
        }
        Ok(change)
    }

    /// Atomically captures and rechecks the expected object. Never follows a leaf symlink.
    pub fn capture_path_change(&self, change: &mut PathChange) -> Result<()> {
        self.validate_change(change)?;
        ensure!(
            change.state == PathChangeState::Prepared,
            "path change is not prepared"
        );
        let destination = checked_destination(&change.root, &change.path)?;
        ensure!(
            PathObservation::at(&destination)? == change.expected,
            PreservationError::LocalChanged
        );
        if let PathTarget::File(manifest) = &change.target {
            ensure!(
                PathObservation::at(&change.staging)?.is_some_and(|o| o.kind == 0)
                    && file_matches_manifest(&change.staging, manifest)?,
                "incoming staging changed before capture"
            );
        }
        if change.expected.is_some() && !change.metadata_only {
            capture_into_vault(&destination, &change.artifact)?;
            sync_directory(destination.parent())?;
            sync_directory(change.artifact.parent())?;
            if change.expected.as_ref().is_some_and(|o| o.kind == 0) {
                sync_preserved_file(&change.artifact)?;
            }
            if PathObservation::at(&change.artifact)? != change.expected {
                self.abort_path_change(change)?;
                bail!("captured local state changed; retry reconciliation");
            }
        }
        change.state = PathChangeState::Preserved;
        self.put_change(change)
    }

    /// Installs without replacing any concurrently recreated local occupant.
    pub fn materialize_path_change(&self, change: &mut PathChange) -> Result<()> {
        self.validate_change(change)?;
        ensure!(
            change.state == PathChangeState::Preserved,
            "path change has not captured its precondition"
        );
        let destination = checked_destination(&change.root, &change.path)?;
        if change.expected.is_some()
            && !change.metadata_only
            && PathObservation::at(&change.artifact)? != change.expected
        {
            self.abort_path_change(change)?;
            bail!("preserved local object changed before installation");
        }
        let result = (|| -> Result<()> {
            match &change.target {
                PathTarget::File(manifest) => {
                    ensure!(
                        PathObservation::at(&change.staging)?
                            .is_some_and(|o| o.kind == 0 && o.hash == manifest.file_hash),
                        "incoming staging changed"
                    );
                    ensure!(
                        file_matches_manifest(&change.staging, manifest)?,
                        "incoming staging changed"
                    );
                    rename_noreplace(&change.staging, &destination)?;
                    self.metadata.put_manifest(&change.path, manifest)?;
                }
                PathTarget::Directory if change.metadata_only => ensure!(
                    PathObservation::at(&destination)? == change.expected,
                    PreservationError::LocalChanged
                ),
                PathTarget::Directory => fs::create_dir(&destination)?,
                PathTarget::Absent => ensure!(
                    fs::symlink_metadata(&destination)
                        .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound),
                    "local object recreated after capture"
                ),
            }
            sync_directory(destination.parent())?;
            Ok(())
        })();
        if let Err(error) = result {
            self.abort_path_change(change)?;
            return Err(error);
        }
        change.state = PathChangeState::Materialized;
        self.put_change(change)
    }

    /// Applies one bound causal operation with the same capture/install journal as legacy calls.
    pub fn apply_causal_record(
        &self,
        root: &Path,
        binding: CausalBinding,
        manifest: Option<&FileManifest>,
    ) -> Result<PathChange> {
        let _guard = self
            .materialize_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("materialization lock poisoned"))?;
        let record = &binding.record;
        record.validate()?;
        ensure!(
            binding
                .authorization
                .as_ref()
                .is_none_or(|bytes| bytes.len() <= 2 * 1024 * 1024),
            "authorization recovery context exceeds limit"
        );
        let expected = PathObservation::read(root, &record.path)?;
        ensure!(
            match (&expected, &binding.precondition) {
                (None, None) => true,
                (None, Some(previous)) => previous.tombstone,
                (Some(observation), Some(previous)) => observation.matches_record(previous),
                _ => false,
            },
            PreservationError::LocalChanged
        );
        let metadata_only = !record.tombstone
            && record.kind == deltaweave_core::SyncEntryKind::Directory
            && expected.as_ref().is_some_and(|o| o.kind == 1);
        let target = if record.tombstone {
            PathTarget::Absent
        } else if record.kind == deltaweave_core::SyncEntryKind::Directory {
            PathTarget::Directory
        } else {
            let manifest = manifest.context("causal file has no manifest")?;
            ensure!(
                record.kind == deltaweave_core::SyncEntryKind::File
                    && record.content_hash == Some(manifest.file_hash)
                    && record.size == manifest.size,
                "manifest differs from causal target"
            );
            PathTarget::File(manifest.clone())
        };
        let mut change =
            self.prepare_path_change(root, &record.path, target, expected, metadata_only)?;
        change.causal = Some(binding);
        change.metadata_only = metadata_only;
        self.put_change(&change)?;
        self.capture_path_change(&mut change)?;
        self.materialize_path_change(&mut change)?;
        Ok(change)
    }

    /// Produces a verified observation for a file installed by a bound path attempt.
    pub fn observe_path_change(&self, change: &PathChange) -> Result<MaterializationObservation> {
        self.validate_change(change)?;
        let PathTarget::File(manifest) = &change.target else {
            bail!("path change is not a file")
        };
        let destination = checked_destination(&change.root, &change.path)?;
        ensure!(
            file_matches_manifest(&destination, manifest)?,
            PreservationError::LocalChanged
        );
        materialization_observation(&destination, manifest.file_hash)
    }

    /// Changes readonly metadata through a no-follow handle, leaving unrelated symlinks alone.
    pub fn set_readonly(&self, root: &Path, path: &WirePath, readonly: bool) -> Result<()> {
        apply_readonly(&checked_destination(root, path)?, readonly)
    }

    /// Records durable index adoption. This never purges the captured object.
    pub fn mark_path_change_indexed(&self, id: &str) -> Result<()> {
        let mut change = self
            .path_changes()?
            .into_iter()
            .find(|c| c.id == id)
            .context("unknown path change")?;
        self.validate_change(&change)?;
        ensure!(
            matches!(
                change.state,
                PathChangeState::Materialized
                    | PathChangeState::Indexed
                    | PathChangeState::Committed
            ),
            "path change not materialized"
        );
        change.state = PathChangeState::Indexed;
        self.put_change(&change)?;
        change.state = PathChangeState::Committed;
        self.put_change(&change)
    }

    /// Completes all materialized attempts for a path after the caller's verified adoption.
    pub fn mark_record_indexed(
        &self,
        root: &Path,
        record: &deltaweave_core::SyncRecord,
    ) -> Result<()> {
        for change in self.path_changes()? {
            let matches = match &change.target {
                PathTarget::File(manifest) => {
                    !record.tombstone
                        && record.kind == deltaweave_core::SyncEntryKind::File
                        && record.content_hash == Some(manifest.file_hash)
                        && record.size == manifest.size
                }
                PathTarget::Directory => {
                    !record.tombstone && record.kind == deltaweave_core::SyncEntryKind::Directory
                }
                PathTarget::Absent => record.tombstone,
            };
            if change.root == root
                && change.path == record.path
                && change.state == PathChangeState::Materialized
                && matches
                && change
                    .causal
                    .as_ref()
                    .is_none_or(|binding| binding.record == *record)
            {
                self.mark_path_change_indexed(&change.id)?;
            }
        }
        Ok(())
    }

    /// Enumerates retained recovery metadata, including completed and aborted attempts.
    pub fn path_changes(&self) -> Result<Vec<PathChange>> {
        let read = self.metadata.database.begin_read()?;
        let table = match read.open_table(CHANGES) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        table
            .iter()?
            .map(|entry| {
                let (_, bytes) = entry?;
                postcard::from_bytes(bytes.value()).context("corrupt path-change journal")
            })
            .collect()
    }

    fn put_change(&self, change: &PathChange) -> Result<()> {
        let encoded = postcard::to_stdvec(change)?;
        let write = self.metadata.database.begin_write()?;
        write
            .open_table(CHANGES)?
            .insert(change.id.as_str(), encoded.as_slice())?;
        write.commit()?;
        Ok(())
    }

    fn validate_change(&self, change: &PathChange) -> Result<()> {
        ensure!(
            change.version == 1
                && !change.id.is_empty()
                && change
                    .id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-'),
            "invalid path-change binding"
        );
        let mut parent = change.root.join(change.path.as_str());
        parent.pop();
        while !parent.is_dir() {
            ensure!(parent.pop(), "recovery root missing");
        }
        let vault = self.vault(&change.root, &parent)?;
        ensure!(
            vault == change.vault
                && change.artifact == vault.join(&change.id).join(change.path.as_str())
                && change.staging == vault.join(&change.id).with_extension("incoming")
                && change.rollback_artifact == vault.join(&change.id).with_extension("rollback"),
            "invalid recovery journal paths"
        );
        validate_real_directory(change.artifact.parent().context("artifact lacks parent")?)?;
        validate_real_directory(change.staging.parent().context("staging lacks parent")?)?;
        if change.expected.is_some()
            && !change.metadata_only
            && matches!(
                change.state,
                PathChangeState::Preserved
                    | PathChangeState::Materialized
                    | PathChangeState::Indexed
                    | PathChangeState::Committed
            )
        {
            ensure!(
                fs::symlink_metadata(&change.artifact).is_ok(),
                PreservationError::StateUnavailable
            );
            if let Some(expected) = &change.expected {
                let metadata = fs::symlink_metadata(&change.artifact)?;
                ensure!(
                    file_identity(&change.artifact, &metadata) == expected.identity,
                    PreservationError::StateUnavailable
                );
            }
        }
        Ok(())
    }

    /// Restores only into an absent destination; otherwise retains both objects and aborts.
    pub fn abort_path_change(&self, change: &mut PathChange) -> Result<()> {
        self.validate_change(change)?;
        let destination = checked_destination(&change.root, &change.path)?;
        if fs::symlink_metadata(&change.artifact).is_ok() {
            // A failed no-replace restore is safe: the artifact remains durable and discoverable.
            let _ = rename_noreplace(&change.artifact, &destination);
            sync_directory(destination.parent())?;
            sync_directory(change.artifact.parent())?;
        }
        change.state = PathChangeState::Aborted;
        self.put_change(change)
    }

    /// Resumes a journaled target after the caller reauthenticates its authority.
    /// Reuses the exact artifact and staging paths across interruption boundaries.
    pub fn resume_path_change(&self, change: &mut PathChange) -> Result<()> {
        self.validate_change(change)?;
        if change.state == PathChangeState::Prepared {
            if fs::symlink_metadata(&change.artifact).is_ok() {
                ensure!(
                    PathObservation::at(&change.artifact)? == change.expected,
                    "captured recovery object changed"
                );
                if change.expected.as_ref().is_some_and(|o| o.kind == 0) {
                    sync_preserved_file(&change.artifact)?;
                }
                change.state = PathChangeState::Preserved;
                self.put_change(change)?;
            } else if matches!(&change.target, PathTarget::File(_)) && !change.staging.exists() {
                // Preparation was interrupted before staging finished; no capture is permitted.
                self.abort_path_change(change)?;
            } else {
                self.capture_path_change(change)?;
            }
        }
        if change.state == PathChangeState::Preserved {
            let destination = checked_destination(&change.root, &change.path)?;
            let installed = match &change.target {
                PathTarget::File(manifest) => {
                    fs::symlink_metadata(&destination)
                        .is_ok_and(|m| m.is_file() && !m.file_type().is_symlink())
                        && file_matches_manifest(&destination, manifest)?
                }
                PathTarget::Directory => fs::symlink_metadata(&destination)
                    .is_ok_and(|m| m.is_dir() && !m.file_type().is_symlink()),
                PathTarget::Absent => fs::symlink_metadata(&destination)
                    .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound),
            };
            if installed {
                ensure!(
                    change.metadata_only
                        || change.expected.is_none()
                        || fs::symlink_metadata(&change.artifact).is_ok(),
                    PreservationError::StateUnavailable
                );
                change.state = PathChangeState::Materialized;
                self.put_change(change)?;
            } else {
                self.materialize_path_change(change)?;
            }
        }
        Ok(())
    }

    /// Rolls an unadopted target back without discarding either the incoming inode or local work.
    /// The caller has validated the durable index precondition and decided authority is revoked.
    pub fn rollback_causal_change(&self, change: &mut PathChange) -> Result<()> {
        self.validate_change(change)?;
        ensure!(
            change.causal.is_some(),
            "rollback requires a causal binding"
        );
        let destination = checked_destination(&change.root, &change.path)?;
        let current = PathObservation::at(&destination)?;
        if change.metadata_only {
            let mut observed = current.context(PreservationError::StateUnavailable)?;
            let expected = change
                .expected
                .as_ref()
                .context(PreservationError::StateUnavailable)?;
            observed.readonly = expected.readonly;
            ensure!(&observed == expected, PreservationError::LocalChanged);
            apply_readonly(&destination, expected.readonly)?;
            change.state = PathChangeState::RolledBack;
            return self.put_change(change);
        }
        if matches!(
            change.state,
            PathChangeState::Prepared | PathChangeState::Aborted
        ) && !change.artifact.exists()
            && current == change.expected
        {
            change.state = PathChangeState::RolledBack;
            return self.put_change(change);
        }
        if change.state == PathChangeState::RollingBack
            && change.expected.as_ref().is_some_and(|expected| {
                current
                    .as_ref()
                    .is_some_and(|o| o.identity == expected.identity)
            })
            && fs::symlink_metadata(&change.artifact).is_err()
        {
            change.state = PathChangeState::RolledBack;
            return self.put_change(change);
        }
        if let Some(current) = &current {
            let target_matches = match &change.target {
                PathTarget::File(manifest) => {
                    current.kind == 0
                        && current.size == manifest.size
                        && current.hash == manifest.file_hash
                }
                PathTarget::Directory => current.kind == 1,
                PathTarget::Absent => false,
            };
            ensure!(
                target_matches
                    && fs::symlink_metadata(&change.rollback_artifact)
                        .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound),
                PreservationError::LocalChanged
            );
        } else if change.state == PathChangeState::Materialized
            && !matches!(change.target, PathTarget::Absent)
        {
            bail!(PreservationError::LocalChanged);
        }
        if let Some(expected) = &change.expected {
            let metadata = fs::symlink_metadata(&change.artifact)
                .context(PreservationError::StateUnavailable)?;
            ensure!(
                file_identity(&change.artifact, &metadata) == expected.identity,
                PreservationError::StateUnavailable
            );
        }
        change.state = PathChangeState::RollingBack;
        self.put_change(change)?;
        if current.is_some() {
            capture_into_vault(&destination, &change.rollback_artifact)?;
            sync_directory(destination.parent())?;
            sync_directory(change.rollback_artifact.parent())?;
            ensure!(
                PathObservation::at(&change.rollback_artifact)? == current,
                PreservationError::LocalChanged
            );
        }
        if change.expected.is_some() {
            rename_noreplace(&change.artifact, &destination)
                .context(PreservationError::LocalChanged)?;
            sync_directory(destination.parent())?;
            sync_directory(change.artifact.parent())?;
        }
        change.state = PathChangeState::RolledBack;
        self.put_change(change)
    }

    /// Reconciles interrupted attempts before scanning or accepting new filesystem work.
    /// Unadopted captures restore without overwrite; installed targets remain for verified rescan.
    pub fn recover_path_changes(&self, root: &Path) -> Result<Vec<PathChange>> {
        let _guard = self
            .materialize_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("materialization lock poisoned"))?;
        for mut change in self.path_changes()?.into_iter().filter(|c| c.root == root) {
            self.validate_change(&change)?;
            match change.state {
                PathChangeState::Prepared | PathChangeState::Preserved
                    if change.causal.is_none() =>
                {
                    // Installation may have completed before the journal write. Do not restore
                    // over it; retain the old inode and let the authenticated caller verify it.
                    self.abort_path_change(&mut change)?;
                }
                PathChangeState::Indexed => {
                    change.state = PathChangeState::Committed;
                    self.put_change(&change)?;
                }
                _ => {}
            }
        }
        self.path_changes()
    }
}

fn validate_real_directory(path: &Path) -> Result<()> {
    let mut cursor = PathBuf::new();
    for component in path.components() {
        cursor.push(component);
        // A Windows prefix is not a complete filesystem location. In particular,
        // canonical paths begin with a verbatim disk prefix that needs RootDir.
        if matches!(component, std::path::Component::Prefix(_)) {
            continue;
        }
        let metadata = fs::symlink_metadata(&cursor)?;
        ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "recovery path contains a symlink or non-directory"
        );
    }
    Ok(())
}

fn sync_preserved_file(path: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        let observed = open_nofollow(path, false, false)?;
        // FlushFileBuffers requires GENERIC_WRITE. Do not clear readonly attributes:
        // that would also modify any hardlink outside the managed namespace.
        // Windows readonly originals retain process-recovery protection, without an
        // explicit content flush or a power-loss durability guarantee.
        if observed.metadata()?.permissions().readonly() {
            return Ok(());
        }
        drop(observed);
        open_nofollow(path, true, false)?.sync_all()?;
    }
    #[cfg(not(windows))]
    open_nofollow(path, false, false)?.sync_all()?;
    Ok(())
}

fn same_volume(left: &Path, right: &Path) -> Result<bool> {
    let left =
        file_identity(left, &fs::metadata(left)?).context("filesystem identity unavailable")?;
    let right =
        file_identity(right, &fs::metadata(right)?).context("filesystem identity unavailable")?;
    Ok(left.0 == right.0)
}

#[cfg(target_os = "linux")]
fn real_parent(path: &Path) -> Result<rustix::fd::OwnedFd> {
    use rustix::fs::{Mode, OFlags, open, openat};
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let parent = path.parent().context("path lacks parent")?;
    let mut fd = open(
        if parent.is_absolute() { "/" } else { "." },
        flags,
        Mode::empty(),
    )?;
    for part in parent.components() {
        match part {
            std::path::Component::Normal(name) => fd = openat(&fd, name, flags, Mode::empty())?,
            std::path::Component::RootDir | std::path::Component::CurDir => {}
            _ => bail!("noncanonical filesystem parent"),
        }
    }
    Ok(fd)
}

#[cfg(target_os = "linux")]
pub(super) fn open_nofollow(path: &Path, write: bool, create: bool) -> Result<File> {
    use rustix::fs::{Mode, OFlags, openat};
    let mut flags =
        OFlags::NOFOLLOW | OFlags::CLOEXEC | if write { OFlags::RDWR } else { OFlags::RDONLY };
    if create {
        flags |= OFlags::CREATE | OFlags::EXCL;
    }
    Ok(File::from(openat(
        real_parent(path)?,
        path.file_name().context("path lacks leaf")?,
        flags,
        Mode::RUSR | Mode::WUSR,
    )?))
}

#[cfg(not(target_os = "linux"))]
pub(super) fn open_nofollow(path: &Path, write: bool, create: bool) -> Result<File> {
    validate_real_directory(path.parent().context("path lacks parent")?)?;
    let mut options = OpenOptions::new();
    options.read(true).write(write).create_new(create);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.custom_flags(0x00200000 | 0x02000000); // OPEN_REPARSE_POINT | BACKUP_SEMANTICS
    }
    let file = options.open(path)?;
    ensure!(
        !file.metadata()?.file_type().is_symlink(),
        "refusing symlink file handle"
    );
    Ok(file)
}

#[cfg(windows)]
pub(super) fn open_for_permissions(path: &Path) -> Result<File> {
    use std::os::windows::fs::OpenOptionsExt;
    validate_real_directory(path.parent().context("path lacks parent")?)?;
    let file = OpenOptions::new()
        // Attribute writes are allowed even when the file has the readonly flag.
        .access_mode(0x80000000 | 0x00000100) // GENERIC_READ | FILE_WRITE_ATTRIBUTES
        .custom_flags(0x00200000 | 0x02000000) // OPEN_REPARSE_POINT | BACKUP_SEMANTICS
        .open(path)?;
    ensure!(
        !file.metadata()?.file_type().is_symlink(),
        "refusing symlink file handle"
    );
    Ok(file)
}

#[cfg(target_os = "linux")]
fn rename_noreplace(source: &Path, destination: &Path) -> Result<()> {
    let source_parent = real_parent(source)?;
    let destination_parent = real_parent(destination)?;
    rustix::fs::renameat_with(
        source_parent,
        source.file_name().context("source lacks leaf")?,
        destination_parent,
        destination.file_name().context("destination lacks leaf")?,
        rustix::fs::RenameFlags::NOREPLACE,
    )?;
    Ok(())
}

fn capture_into_vault(source: &Path, destination: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        // Destination is in this attempt's exclusive private directory. Atomic rename moves
        // the leaf itself (including reparse points), retaining open-handle writes.
        validate_real_directory(destination.parent().context("artifact lacks parent")?)?;
        ensure!(
            fs::symlink_metadata(destination)
                .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound),
            "artifact already occupied"
        );
        fs::rename(source, destination)?;
        Ok(())
    }
    #[cfg(not(windows))]
    rename_noreplace(source, destination)
}

#[cfg(not(target_os = "linux"))]
fn rename_noreplace(source: &Path, destination: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(source)?;
    if metadata.is_file() && !metadata.file_type().is_symlink() {
        // Install/restore only: the source is private, so linking retains the same inode and
        // fails atomically if a local writer already recreated the destination.
        fs::hard_link(source, destination)?;
        fs::remove_file(source)?;
        return Ok(());
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileTypeExt;
        if metadata.file_type().is_symlink() {
            let target = fs::read_link(source)?;
            if metadata.file_type().is_symlink_dir() {
                std::os::windows::fs::symlink_dir(target, destination)?;
            } else {
                std::os::windows::fs::symlink_file(target, destination)?;
            }
            // Keep the captured link as recovery; creation above never replaces a destination.
            return Ok(());
        }
        if metadata.is_dir() {
            // Win32 MoveFileEx cannot replace a destination with a directory.
            fs::rename(source, destination)?;
            return Ok(());
        }
    }
    bail!("atomic no-replace move unavailable for this filesystem object")
}
