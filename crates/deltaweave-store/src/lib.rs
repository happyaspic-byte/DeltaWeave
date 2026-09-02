//! Durable metadata and content-addressed chunk storage.

#![forbid(unsafe_code)]

use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Context, Result, bail};
use deltaweave_cdc::{manifest_from_path, read_chunk, verify_chunk};
use deltaweave_core::{ChunkDescriptor, ChunkingProfile, FileManifest, Hash32, WirePath};
use redb::{Database, ReadableDatabase, TableDefinition};
use serde::{Deserialize, Serialize};

const MANIFESTS: TableDefinition<&str, &[u8]> = TableDefinition::new("manifests");
const OPERATIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("operations");
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Chunk bytes validated against a manifest descriptor.
#[derive(Debug)]
pub struct VerifiedChunk {
    hash: Hash32,
    bytes: Vec<u8>,
}

impl VerifiedChunk {
    /// Validates owned bytes against `descriptor`.
    pub fn validate(descriptor: &ChunkDescriptor, bytes: Vec<u8>) -> Result<Self> {
        verify_chunk(descriptor, &bytes)?;
        Ok(Self {
            hash: descriptor.hash,
            bytes,
        })
    }

    /// Returns the validated chunk's digest.
    #[must_use]
    pub const fn hash(&self) -> Hash32 {
        self.hash
    }

    /// Returns the validated chunk's bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Content-addressed chunk files and their temporary/quarantine areas.
#[derive(Debug)]
pub struct ChunkStore {
    chunks: PathBuf,
    temporary: PathBuf,
    trash: PathBuf,
}

impl ChunkStore {
    /// Opens or creates a chunk store under `state_root`.
    pub fn open(state_root: impl AsRef<Path>) -> Result<Self> {
        let state_root = state_root.as_ref();
        let chunks = state_root.join("chunks");
        let temporary = state_root.join("tmp");
        let trash = state_root.join("trash");
        for directory in [&chunks, &temporary, &trash] {
            fs::create_dir_all(directory)
                .with_context(|| format!("failed to create {}", directory.display()))?;
        }
        Ok(Self {
            chunks,
            temporary,
            trash,
        })
    }

    /// Returns whether a chunk path currently exists.
    #[must_use]
    pub fn contains(&self, hash: Hash32) -> bool {
        self.chunk_path(hash).is_file()
    }

    /// Stores bytes only after verifying that their content matches `hash`.
    ///
    /// Returns `true` when new bytes were written and `false` when a verified chunk
    /// already existed.
    pub fn put_verified(&self, hash: Hash32, bytes: &[u8]) -> Result<bool> {
        let actual = Hash32::digest(bytes);
        if actual != hash {
            bail!("refusing chunk {hash}: supplied bytes hash to {actual}");
        }
        match self.install_validated_chunk(hash, bytes)? {
            Some(parent) => {
                sync_directory(Some(&parent))?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Stores many verified chunks, fsyncing each file and each unique parent once.
    ///
    /// All hashes are checked before any durable write. Returns the number of newly
    /// installed chunks.
    pub fn put_verified_batch(
        &self,
        chunks: impl IntoIterator<Item = (Hash32, Vec<u8>)>,
    ) -> Result<usize> {
        self.put_verified_batch_with_sync(chunks, |parent| sync_directory(Some(parent)))
    }

    fn put_verified_batch_with_sync(
        &self,
        chunks: impl IntoIterator<Item = (Hash32, Vec<u8>)>,
        sync_parent: impl FnMut(&Path) -> Result<()>,
    ) -> Result<usize> {
        let chunks: Vec<_> = chunks.into_iter().collect();
        for (hash, bytes) in &chunks {
            let actual = Hash32::digest(bytes);
            if actual != *hash {
                bail!("refusing chunk {hash}: supplied bytes hash to {actual}");
            }
        }
        self.install_validated_batch(chunks, sync_parent)
    }

    /// Installs previously validated chunks without hashing the newly supplied bytes.
    ///
    /// Existing destination files are still verified and corrupt files are quarantined.
    /// Each written file and each unique parent directory is fsynced once.
    pub fn put_validated_batch(
        &self,
        chunks: impl IntoIterator<Item = VerifiedChunk>,
    ) -> Result<usize> {
        self.put_validated_batch_with_sync(chunks, |parent| sync_directory(Some(parent)))
    }

    fn put_validated_batch_with_sync(
        &self,
        chunks: impl IntoIterator<Item = VerifiedChunk>,
        sync_parent: impl FnMut(&Path) -> Result<()>,
    ) -> Result<usize> {
        let chunks: Vec<(Hash32, Vec<u8>)> = chunks
            .into_iter()
            .map(|chunk| (chunk.hash, chunk.bytes))
            .collect();
        self.install_validated_batch(chunks, sync_parent)
    }

    fn install_validated_batch(
        &self,
        chunks: Vec<(Hash32, Vec<u8>)>,
        mut sync_parent: impl FnMut(&Path) -> Result<()>,
    ) -> Result<usize> {
        let mut written = 0_usize;
        let mut parents = std::collections::BTreeSet::new();
        let mut install_error = None;
        for (hash, bytes) in chunks {
            match self.install_validated_chunk(hash, &bytes) {
                Ok(Some(parent)) => {
                    parents.insert(parent);
                    written += 1;
                }
                Ok(None) => {}
                Err(error) => {
                    install_error = Some(error);
                    break;
                }
            }
        }
        for parent in parents {
            sync_parent(&parent)?;
        }
        if let Some(error) = install_error {
            return Err(error);
        }
        Ok(written)
    }

    fn install_validated_chunk(&self, hash: Hash32, bytes: &[u8]) -> Result<Option<PathBuf>> {
        let destination = self.chunk_path(hash);
        if destination.is_file() {
            match self.read_verified(hash) {
                Ok(_) => return Ok(None),
                Err(_) => {
                    let quarantine = self.unique_path(&self.trash, "corrupt", hash);
                    if let Some(parent) = quarantine.parent() {
                        fs::create_dir_all(parent)?;
                    }
                    fs::rename(&destination, &quarantine).with_context(|| {
                        format!(
                            "failed to quarantine corrupt chunk {}",
                            destination.display()
                        )
                    })?;
                }
            }
        }

        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = self.unique_path(&self.temporary, "chunk", hash);
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .with_context(|| format!("failed to create {}", temporary.display()))?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);

        match fs::rename(&temporary, &destination) {
            Ok(()) => {}
            Err(error) if destination.is_file() => {
                let existing = self.read_verified(hash);
                let _ = fs::remove_file(&temporary);
                existing.with_context(|| {
                    format!("chunk race left invalid destination after {error}")
                })?;
                return Ok(None);
            }
            Err(error) => {
                let _ = fs::remove_file(&temporary);
                return Err(error)
                    .with_context(|| format!("failed to install chunk {}", destination.display()));
            }
        }
        Ok(destination.parent().map(Path::to_path_buf))
    }

    /// Reads a chunk and verifies its name against its content.
    pub fn read_verified(&self, hash: Hash32) -> Result<Vec<u8>> {
        let path = self.chunk_path(hash);
        let bytes =
            fs::read(&path).with_context(|| format!("failed to read chunk {}", path.display()))?;
        let actual = Hash32::digest(&bytes);
        if actual != hash {
            bail!("chunk {hash} is corrupt; actual digest is {actual}");
        }
        Ok(bytes)
    }

    fn chunk_path(&self, hash: Hash32) -> PathBuf {
        let encoded = hash.to_hex();
        self.chunks.join(&encoded[..2]).join(&encoded[2..])
    }

    fn unique_path(&self, root: &Path, kind: &str, hash: Hash32) -> PathBuf {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        root.join(format!(
            "{kind}-{}-{}-{sequence}.part",
            std::process::id(),
            hash
        ))
    }

    fn trash_path(&self, path: &WirePath, manifest_hash: Hash32) -> PathBuf {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let mut destination = self
            .trash
            .join(format!("replace-{manifest_hash}-{sequence}"));
        for component in path.components() {
            destination.push(component);
        }
        destination
    }
}

/// ACID metadata index backed by redb.
pub struct MetadataStore {
    database: Database,
}

impl fmt::Debug for MetadataStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MetadataStore(..)")
    }
}

impl MetadataStore {
    /// Opens or creates the metadata database.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let database = Database::create(path)
            .with_context(|| format!("failed to open metadata DB {}", path.display()))?;
        Ok(Self { database })
    }

    /// Atomically stores the current manifest for a path.
    pub fn put_manifest(&self, path: &WirePath, manifest: &FileManifest) -> Result<()> {
        manifest.validate()?;
        let encoded = postcard::to_stdvec(manifest)?;
        let write = self.database.begin_write()?;
        {
            let mut table = write.open_table(MANIFESTS)?;
            table.insert(path.as_str(), encoded.as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    /// Reads the latest manifest for a path.
    pub fn get_manifest(&self, path: &WirePath) -> Result<Option<FileManifest>> {
        let read = self.database.begin_read()?;
        let table = read.open_table(MANIFESTS)?;
        let encoded = table
            .get(path.as_str())?
            .map(|guard| guard.value().to_vec());
        encoded
            .map(|bytes| postcard::from_bytes(&bytes).context("invalid manifest in metadata DB"))
            .transpose()
    }

    /// Writes an operation journal record durably.
    pub fn put_operation(&self, operation: &OperationRecord) -> Result<()> {
        let key = operation.id.to_hex();
        let encoded = postcard::to_stdvec(operation)?;
        let write = self.database.begin_write()?;
        {
            let mut table = write.open_table(OPERATIONS)?;
            table.insert(key.as_str(), encoded.as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    /// Reads one operation journal record.
    pub fn get_operation(&self, id: Hash32) -> Result<Option<OperationRecord>> {
        let key = id.to_hex();
        let read = self.database.begin_read()?;
        let table = read.open_table(OPERATIONS)?;
        let encoded = table.get(key.as_str())?.map(|guard| guard.value().to_vec());
        encoded
            .map(|bytes| postcard::from_bytes(&bytes).context("invalid operation in metadata DB"))
            .transpose()
    }
}

/// Durable state of a file-application transaction.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum OperationState {
    /// The manifest was accepted and materialization may be incomplete.
    Prepared,
    /// The final file and metadata were committed.
    Committed,
}

/// Idempotent operation journal entry.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OperationRecord {
    /// Domain-separated manifest identity.
    pub id: Hash32,
    /// Destination protocol path.
    pub path: WirePath,
    /// Expected complete-file digest.
    pub file_hash: Hash32,
    /// Current durable operation state.
    pub state: OperationState,
}

/// Combined content and metadata store used by the transfer engine.
#[derive(Debug)]
pub struct Store {
    chunks: ChunkStore,
    metadata: MetadataStore,
    materialize_lock: Mutex<()>,
}

impl Store {
    /// Opens a complete DeltaWeave state directory.
    pub fn open(state_root: impl AsRef<Path>) -> Result<Self> {
        let state_root = state_root.as_ref();
        fs::create_dir_all(state_root)?;
        Ok(Self {
            chunks: ChunkStore::open(state_root)?,
            metadata: MetadataStore::open(state_root.join("metadata.redb"))?,
            materialize_lock: Mutex::new(()),
        })
    }

    /// Returns the content-addressed chunk store.
    #[must_use]
    pub const fn chunks(&self) -> &ChunkStore {
        &self.chunks
    }

    /// Returns the ACID metadata store.
    #[must_use]
    pub const fn metadata(&self) -> &MetadataStore {
        &self.metadata
    }

    /// Returns unique chunk hashes that are not currently present.
    #[must_use]
    pub fn missing_chunks(&self, manifest: &FileManifest) -> Vec<Hash32> {
        let mut seen = std::collections::HashSet::new();
        manifest
            .chunks
            .iter()
            .filter_map(|chunk| {
                (seen.insert(chunk.hash) && self.chunks.read_verified(chunk.hash).is_err())
                    .then_some(chunk.hash)
            })
            .collect()
    }

    /// Chunks and verifies a local file into the durable CAS, returning its manifest.
    ///
    /// Every manifest extent is read and reverified before this succeeds. If the source mutates
    /// between manifest construction and ingestion, a length or digest mismatch aborts the call.
    pub fn ingest_file(
        &self,
        source: impl AsRef<Path>,
        profile: ChunkingProfile,
    ) -> Result<FileManifest> {
        let source = source.as_ref();
        let manifest = manifest_from_path(source, profile)?;
        let mut file = File::open(source)?;
        for descriptor in &manifest.chunks {
            let bytes = read_chunk(&mut file, descriptor)?;
            self.chunks.put_verified(descriptor.hash, &bytes)?;
        }
        Ok(manifest)
    }

    /// Materializes a verified manifest beneath `destination_root`.
    pub fn materialize(
        &self,
        manifest: &FileManifest,
        path: &WirePath,
        destination_root: impl AsRef<Path>,
    ) -> Result<MaterializeOutcome> {
        let _guard = self
            .materialize_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("materialization lock is poisoned"))?;
        manifest.validate()?;
        let destination_root = destination_root.as_ref();
        fs::create_dir_all(destination_root)?;
        let destination = checked_destination(destination_root, path)?;
        let existing = fs::symlink_metadata(&destination).ok();
        if existing
            .as_ref()
            .is_some_and(|metadata| metadata.is_dir() && !metadata.file_type().is_symlink())
        {
            bail!(
                "refusing to replace destination directory {} with a file",
                destination.display()
            );
        }
        if existing
            .as_ref()
            .is_some_and(|metadata| metadata.is_file() && !metadata.file_type().is_symlink())
            && hash_file(&destination)? == manifest.file_hash
        {
            let operation = committed_operation(path, manifest);
            self.metadata.put_manifest(path, manifest)?;
            self.metadata.put_operation(&operation)?;
            let observation = materialization_observation(&destination, manifest.file_hash)?;
            return Ok(MaterializeOutcome {
                destination,
                bytes_written: 0,
                replaced_existing: false,
                already_current: true,
                observation,
            });
        }

        let parent = destination
            .parent()
            .context("validated destination unexpectedly has no parent")?;
        fs::create_dir_all(parent)?;
        let temporary = parent.join(format!(
            ".deltaweave-{}-{}.part",
            manifest.manifest_hash(),
            TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));

        let operation = OperationRecord {
            id: operation_id(path, manifest),
            path: path.clone(),
            file_hash: manifest.file_hash,
            state: OperationState::Prepared,
        };
        self.metadata.put_operation(&operation)?;

        let write_result = (|| -> Result<()> {
            let mut output = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)?;
            let mut file_hasher = blake3::Hasher::new();
            for chunk in &manifest.chunks {
                let bytes = self.chunks.read_verified(chunk.hash)?;
                if bytes.len() != chunk.length as usize {
                    bail!(
                        "chunk {} has length {}, expected {}",
                        chunk.hash,
                        bytes.len(),
                        chunk.length
                    );
                }
                output.write_all(&bytes)?;
                file_hasher.update(&bytes);
            }
            output.sync_all()?;
            let actual = Hash32::from_bytes(*file_hasher.finalize().as_bytes());
            if actual != manifest.file_hash {
                bail!(
                    "materialized file hash mismatch: expected {}, got {}",
                    manifest.file_hash,
                    actual
                );
            }
            Ok(())
        })();
        if let Err(error) = write_result {
            let _ = fs::remove_file(&temporary);
            return Err(error);
        }

        let replaced_existing = destination.exists();
        let backup =
            replaced_existing.then(|| self.chunks.trash_path(path, manifest.manifest_hash()));
        if let Some(backup) = &backup {
            if let Some(parent) = backup.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::rename(&destination, backup).with_context(|| {
                format!("failed to preserve existing {}", destination.display())
            })?;
        }

        if let Err(error) = fs::rename(&temporary, &destination) {
            if let Some(backup) = &backup {
                let _ = fs::rename(backup, &destination);
            }
            let _ = fs::remove_file(&temporary);
            return Err(error)
                .with_context(|| format!("failed to install {}", destination.display()));
        }
        sync_directory(Some(parent))?;

        self.metadata.put_manifest(path, manifest)?;
        self.metadata.put_operation(&OperationRecord {
            state: OperationState::Committed,
            ..operation
        })?;

        let observation = materialization_observation(&destination, manifest.file_hash)?;
        Ok(MaterializeOutcome {
            destination,
            bytes_written: manifest.size,
            replaced_existing,
            already_current: false,
            observation,
        })
    }

    /// Creates one real directory beneath the destination root without traversing symlinks.
    pub fn materialize_directory(
        &self,
        path: &WirePath,
        destination_root: impl AsRef<Path>,
    ) -> Result<PathBuf> {
        let _guard = self
            .materialize_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("materialization lock is poisoned"))?;
        let destination_root = destination_root.as_ref();
        fs::create_dir_all(destination_root)?;
        let destination = checked_destination(destination_root, path)?;
        match fs::symlink_metadata(&destination) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                bail!(
                    "refusing to replace non-directory {} with a directory",
                    destination.display()
                );
            }
            Ok(_) => return Ok(destination),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        fs::create_dir_all(&destination)?;
        sync_directory(destination.parent())?;
        Ok(destination)
    }

    /// Removes one path idempotently, preserving non-directory content in private trash.
    ///
    /// Directories are removed only when empty, so an unindexed or concurrently created child is
    /// never deleted recursively by remote state.
    pub fn remove_path(
        &self,
        path: &WirePath,
        destination_root: impl AsRef<Path>,
        tombstone_hash: Hash32,
    ) -> Result<RemoveOutcome> {
        let _guard = self
            .materialize_lock
            .lock()
            .map_err(|_| anyhow::anyhow!("materialization lock is poisoned"))?;
        let destination_root = destination_root.as_ref();
        fs::create_dir_all(destination_root)?;
        let destination = checked_destination(destination_root, path)?;
        let metadata = match fs::symlink_metadata(&destination) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(RemoveOutcome {
                    destination,
                    removed: false,
                    preserved_in_trash: false,
                    preserved_path: None,
                });
            }
            Err(error) => return Err(error.into()),
        };

        let preserved_path = if metadata.is_dir() && !metadata.file_type().is_symlink() {
            fs::remove_dir(&destination).with_context(|| {
                format!(
                    "refusing to remove non-empty or unavailable directory {}",
                    destination.display()
                )
            })?;
            None
        } else {
            let backup = self.chunks.trash_path(path, tombstone_hash);
            if let Some(parent) = backup.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::rename(&destination, &backup).with_context(|| {
                format!(
                    "failed to preserve deleted {} in trash",
                    destination.display()
                )
            })?;
            Some(backup)
        };
        sync_directory(destination.parent())?;
        Ok(RemoveOutcome {
            destination,
            removed: true,
            preserved_in_trash: preserved_path.is_some(),
            preserved_path,
        })
    }
}

/// Metadata captured immediately after verified local materialization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializationObservation {
    file_hash: Hash32,
    identity: Option<(u64, u64)>,
    size: u64,
    modified_ns: Option<u128>,
    changed_ns: Option<u128>,
    readonly: bool,
}

impl MaterializationObservation {
    /// Verified whole-file digest associated with this observation.
    #[must_use]
    pub const fn file_hash(&self) -> Hash32 {
        self.file_hash
    }

    /// Best-effort stable filesystem identity.
    #[must_use]
    pub const fn identity(&self) -> Option<(u64, u64)> {
        self.identity
    }

    /// Observed file length.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }

    /// Observed modification time in Unix-epoch nanoseconds.
    #[must_use]
    pub const fn modified_ns(&self) -> Option<u128> {
        self.modified_ns
    }

    /// Observed inode change time in Unix-epoch nanoseconds when the platform exposes it.
    #[must_use]
    pub const fn changed_ns(&self) -> Option<u128> {
        self.changed_ns
    }

    /// Observed read-only state.
    #[must_use]
    pub const fn readonly(&self) -> bool {
        self.readonly
    }

    /// Recaptures the observation after an intentional readonly chmod.
    ///
    /// Identity, size, and mtime must remain unchanged. Unix chmod updates ctime, so that field
    /// is allowed to change. This does not exclude concurrent writers; it only rejects identity,
    /// size, or mtime drift around the permission update.
    pub fn after_readonly_update(&self, path: impl AsRef<Path>, readonly: bool) -> Result<Self> {
        let path = path.as_ref();
        let before = materialization_observation(path, self.file_hash)?;
        if before != *self {
            bail!("materialized file changed before readonly update");
        }
        apply_readonly(path, readonly)?;
        let after = materialization_observation(path, self.file_hash)?;
        if after.identity != before.identity
            || after.size != before.size
            || after.modified_ns != before.modified_ns
        {
            bail!("materialized file identity, size, or mtime changed during readonly update");
        }
        if after.readonly != readonly {
            bail!("materialized file readonly state did not match the requested update");
        }
        Ok(after)
    }
}

/// Result of safely materializing one file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializeOutcome {
    /// Final local path.
    pub destination: PathBuf,
    /// Bytes written during this operation.
    pub bytes_written: u64,
    /// Whether a different existing file was preserved in trash.
    pub replaced_existing: bool,
    /// Whether the destination already contained the requested content.
    pub already_current: bool,
    /// Metadata fingerprint captured after the verified file was installed.
    pub observation: MaterializationObservation,
}

/// Result of an idempotent path deletion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoveOutcome {
    /// Final local path that is now absent.
    pub destination: PathBuf,
    /// Whether an object existed and was removed.
    pub removed: bool,
    /// Whether non-directory content was moved into private trash.
    pub preserved_in_trash: bool,
    /// Private recovery path when content was preserved.
    pub preserved_path: Option<PathBuf>,
}

fn checked_destination(root: &Path, path: &WirePath) -> Result<PathBuf> {
    let root_metadata = fs::symlink_metadata(root)?;
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        bail!("destination root must be a real directory, not a symlink");
    }

    let mut destination = root.to_path_buf();
    let components: Vec<_> = path.components().collect();
    for (index, component) in components.iter().enumerate() {
        destination.push(component);
        if index + 1 == components.len() {
            continue;
        }
        match fs::symlink_metadata(&destination) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                bail!(
                    "refusing destination beneath symlink {}",
                    destination.display()
                );
            }
            Ok(metadata) if !metadata.is_dir() => {
                bail!(
                    "destination ancestor is not a directory: {}",
                    destination.display()
                );
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(destination)
}

fn materialization_observation(
    path: &Path,
    file_hash: Hash32,
) -> Result<MaterializationObservation> {
    let metadata = fs::symlink_metadata(path)?;
    ensure_regular_file(&metadata, path)?;
    Ok(MaterializationObservation {
        file_hash,
        identity: file_identity(path, &metadata),
        size: metadata.len(),
        modified_ns: modified_ns(&metadata),
        changed_ns: change_time_ns(&metadata),
        readonly: metadata.permissions().readonly(),
    })
}

fn ensure_regular_file(metadata: &std::fs::Metadata, path: &Path) -> Result<()> {
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("{} is not a regular file", path.display());
    }
    Ok(())
}

fn modified_ns(metadata: &std::fs::Metadata) -> Option<u128> {
    metadata
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_nanos())
}

#[cfg(unix)]
fn change_time_ns(metadata: &std::fs::Metadata) -> Option<u128> {
    use std::os::unix::fs::MetadataExt;
    let seconds = u128::try_from(metadata.ctime()).ok()?;
    let nanos = u128::try_from(metadata.ctime_nsec()).ok()?;
    Some(seconds.saturating_mul(1_000_000_000).saturating_add(nanos))
}

#[cfg(not(unix))]
fn change_time_ns(_metadata: &std::fs::Metadata) -> Option<u128> {
    None
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

#[cfg(unix)]
fn file_identity(_path: &Path, metadata: &std::fs::Metadata) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    if metadata.ino() == 0 {
        return None;
    }
    Some((metadata.dev(), metadata.ino()))
}

#[cfg(windows)]
fn file_identity(path: &Path, _metadata: &std::fs::Metadata) -> Option<(u64, u64)> {
    let handle = winapi_util::Handle::from_path_any(path).ok()?;
    let information = winapi_util::file::information(&handle).ok()?;
    if information.file_index() == 0 {
        return None;
    }
    Some((information.volume_serial_number(), information.file_index()))
}

#[cfg(not(any(unix, windows)))]
fn file_identity(_path: &Path, _metadata: &std::fs::Metadata) -> Option<(u64, u64)> {
    None
}

fn hash_file(path: &Path) -> Result<Hash32> {
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(Hash32::from_bytes(*hasher.finalize().as_bytes()))
}

fn operation_id(path: &WirePath, manifest: &FileManifest) -> Hash32 {
    let mut hasher = blake3::Hasher::new_derive_key("deltaweave operation v1");
    hasher.update(path.as_str().as_bytes());
    hasher.update(manifest.manifest_hash().as_bytes());
    Hash32::from_bytes(*hasher.finalize().as_bytes())
}

fn committed_operation(path: &WirePath, manifest: &FileManifest) -> OperationRecord {
    OperationRecord {
        id: operation_id(path, manifest),
        path: path.clone(),
        file_hash: manifest.file_hash,
        state: OperationState::Committed,
    }
}

#[cfg(unix)]
fn sync_directory(path: Option<&Path>) -> Result<()> {
    if let Some(path) = path {
        File::open(path)?.sync_all()?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: Option<&Path>) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use deltaweave_cdc::{manifest_from_reader, read_chunk};
    use deltaweave_core::ChunkingProfile;
    use tempfile::TempDir;

    use super::*;

    fn fixture(length: usize) -> Vec<u8> {
        (0..length)
            .map(|index| ((index * 31) ^ (index >> 3)) as u8)
            .collect()
    }

    fn populate(store: &Store, bytes: &[u8], manifest: &FileManifest) {
        let mut reader = Cursor::new(bytes);
        for chunk in &manifest.chunks {
            let data = read_chunk(&mut reader, chunk).expect("chunk can be read");
            store
                .chunks()
                .put_verified(chunk.hash, &data)
                .expect("chunk can be stored");
        }
    }

    #[test]
    fn chunk_store_batch_commits_verified_chunks() {
        let temp = TempDir::new().expect("temporary directory can be created");
        let store = ChunkStore::open(temp.path()).expect("chunk store can open");
        let chunks: Vec<_> = (0..32)
            .map(|index| {
                let bytes = fixture(64 * 1024 + index);
                (Hash32::digest(&bytes), bytes)
            })
            .collect();

        let written = store
            .put_verified_batch(chunks.clone())
            .expect("verified batch can be committed");

        assert_eq!(written, chunks.len());
        for (hash, bytes) in chunks {
            assert_eq!(
                store.read_verified(hash).expect("batch chunk can be read"),
                bytes
            );
        }
    }

    #[test]
    fn chunk_store_batch_syncs_installed_parents_before_returning_later_error() {
        let temp = TempDir::new().expect("temporary directory can be created");
        let store = ChunkStore::open(temp.path()).expect("chunk store can open");
        let first = fixture(64 * 1024);
        let first_hash = Hash32::digest(&first);
        let mut second = fixture(64 * 1024 + 1);
        let second_hash = loop {
            let hash = Hash32::digest(&second);
            if hash.to_hex()[..2] != first_hash.to_hex()[..2] {
                break hash;
            }
            second.push(1);
        };
        let blocked_parent = store
            .chunk_path(second_hash)
            .parent()
            .expect("chunk path has a parent")
            .to_path_buf();
        fs::write(&blocked_parent, b"not a directory").expect("blocked parent can be created");
        let mut synced = Vec::new();

        let result = store.put_verified_batch_with_sync(
            vec![(first_hash, first), (second_hash, second)],
            |parent| {
                synced.push(parent.to_path_buf());
                Ok(())
            },
        );

        assert!(result.is_err());
        assert!(store.contains(first_hash));
        assert_eq!(synced, vec![store.chunk_path(first_hash).parent().unwrap()]);
    }

    #[test]
    fn chunk_store_batch_rejects_before_installing_any_chunk() {
        let temp = TempDir::new().expect("temporary directory can be created");
        let store = ChunkStore::open(temp.path()).expect("chunk store can open");
        let good = fixture(64 * 1024);
        let good_hash = Hash32::digest(&good);
        let invalid_hash = Hash32::digest(b"different");

        assert!(
            store
                .put_verified_batch(vec![(good_hash, good), (invalid_hash, b"bad".to_vec())])
                .is_err()
        );
        assert!(!store.contains(good_hash));
        assert!(!store.contains(invalid_hash));
    }

    #[test]
    fn chunk_store_rejects_mislabeled_data() {
        let temp = TempDir::new().expect("temporary directory can be created");
        let store = ChunkStore::open(temp.path()).expect("chunk store can open");
        assert!(
            store
                .put_verified(Hash32::digest(b"good"), b"evil")
                .is_err()
        );
    }

    fn descriptor_for(bytes: &[u8]) -> ChunkDescriptor {
        ChunkDescriptor {
            offset: 0,
            length: u32::try_from(bytes.len()).expect("fixture fits in a chunk descriptor"),
            hash: Hash32::digest(bytes),
        }
    }

    fn validated(bytes: Vec<u8>) -> VerifiedChunk {
        let descriptor = descriptor_for(&bytes);
        VerifiedChunk::validate(&descriptor, bytes).expect("fixture matches its descriptor")
    }

    #[test]
    fn verified_chunk_rejects_hash_mismatch() {
        let descriptor = ChunkDescriptor {
            offset: 0,
            length: 4,
            hash: Hash32::digest(b"good"),
        };

        let error = VerifiedChunk::validate(&descriptor, b"evil".to_vec())
            .expect_err("mislabeled bytes must not become a VerifiedChunk");
        assert!(error.to_string().contains("hash mismatch"));
    }

    #[test]
    fn verified_chunk_rejects_length_mismatch() {
        let descriptor = ChunkDescriptor {
            offset: 0,
            length: 4,
            hash: Hash32::digest(b"good"),
        };

        let short = VerifiedChunk::validate(&descriptor, b"goo".to_vec())
            .expect_err("short bytes must not become a VerifiedChunk");
        let long = VerifiedChunk::validate(&descriptor, b"goods".to_vec())
            .expect_err("long bytes must not become a VerifiedChunk");
        assert!(short.to_string().contains("length mismatch"));
        assert!(long.to_string().contains("length mismatch"));
    }

    #[test]
    fn put_validated_batch_installs_prechecked_chunks() {
        let temp = TempDir::new().expect("temporary directory can be created");
        let store = ChunkStore::open(temp.path()).expect("chunk store can open");
        let chunks: Vec<_> = (0..8)
            .map(|index| validated(fixture(32 * 1024 + index)))
            .collect();
        let expected: Vec<_> = chunks
            .iter()
            .map(|chunk| (chunk.hash(), chunk.bytes().to_vec()))
            .collect();

        let written = store
            .put_validated_batch(chunks)
            .expect("validated batch can be committed");

        assert_eq!(written, expected.len());
        for (hash, bytes) in expected {
            assert_eq!(
                store
                    .read_verified(hash)
                    .expect("validated chunk can be read"),
                bytes
            );
        }
    }

    #[test]
    fn put_validated_batch_quarantines_corrupt_destination() {
        let temp = TempDir::new().expect("temporary directory can be created");
        let store = ChunkStore::open(temp.path()).expect("chunk store can open");
        let bytes = fixture(48 * 1024);
        let hash = Hash32::digest(&bytes);
        let destination = store.chunk_path(hash);
        fs::create_dir_all(destination.parent().expect("chunk path has a parent"))
            .expect("chunk parent can be created");
        fs::write(&destination, b"corrupt").expect("corrupt destination can be planted");

        let written = store
            .put_validated_batch(vec![validated(bytes.clone())])
            .expect("validated chunk replaces a corrupt destination");

        assert_eq!(written, 1);
        assert_eq!(
            store
                .read_verified(hash)
                .expect("replaced chunk can be read"),
            bytes
        );
        let quarantined = fs::read_dir(&store.trash)
            .expect("trash can be read")
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("corrupt-"))
            .count();
        assert_eq!(quarantined, 1);
    }

    #[test]
    fn put_validated_batch_syncs_installed_parents_before_returning_later_error() {
        let temp = TempDir::new().expect("temporary directory can be created");
        let store = ChunkStore::open(temp.path()).expect("chunk store can open");
        let first = fixture(64 * 1024);
        let first_hash = Hash32::digest(&first);
        let mut second = fixture(64 * 1024 + 1);
        let second_hash = loop {
            let hash = Hash32::digest(&second);
            if hash.to_hex()[..2] != first_hash.to_hex()[..2] {
                break hash;
            }
            second.push(1);
        };
        let blocked_parent = store
            .chunk_path(second_hash)
            .parent()
            .expect("chunk path has a parent")
            .to_path_buf();
        fs::write(&blocked_parent, b"not a directory").expect("blocked parent can be created");
        let mut synced = Vec::new();

        let result = store.put_validated_batch_with_sync(
            vec![validated(first), validated(second)],
            |parent| {
                synced.push(parent.to_path_buf());
                Ok(())
            },
        );

        assert!(result.is_err());
        assert!(store.contains(first_hash));
        assert_eq!(synced, vec![store.chunk_path(first_hash).parent().unwrap()]);
    }

    #[test]
    fn metadata_survives_reopen() {
        let temp = TempDir::new().expect("temporary directory can be created");
        let bytes = fixture(300_000);
        let manifest = manifest_from_reader(Cursor::new(&bytes), ChunkingProfile::DEFAULT)
            .expect("fixture can be chunked");
        let path = WirePath::new("documents/report.bin").expect("path is portable");
        {
            let store = Store::open(temp.path()).expect("store can open");
            store
                .metadata()
                .put_manifest(&path, &manifest)
                .expect("manifest can be written");
        }
        let reopened = Store::open(temp.path()).expect("store can reopen");
        assert_eq!(
            reopened
                .metadata()
                .get_manifest(&path)
                .expect("manifest can be read"),
            Some(manifest)
        );
    }

    #[test]
    fn materialization_is_verified_and_idempotent() {
        let temp = TempDir::new().expect("temporary directory can be created");
        let destination = TempDir::new().expect("destination can be created");
        let bytes = fixture(2 * 1024 * 1024 + 7);
        let manifest = manifest_from_reader(Cursor::new(&bytes), ChunkingProfile::DEFAULT)
            .expect("fixture can be chunked");
        let path = WirePath::new("nested/output.bin").expect("path is portable");
        let store = Store::open(temp.path()).expect("store can open");
        populate(&store, &bytes, &manifest);

        let first = store
            .materialize(&manifest, &path, destination.path())
            .expect("file can be materialized");
        assert_eq!(
            fs::read(&first.destination).expect("file can be read"),
            bytes
        );
        assert!(!first.already_current);

        let second = store
            .materialize(&manifest, &path, destination.path())
            .expect("same file is idempotent");
        assert!(second.already_current);
        assert_eq!(second.bytes_written, 0);
        assert_eq!(second.observation.file_hash(), manifest.file_hash);
        assert_eq!(second.observation.size(), manifest.size);
        assert!(!second.observation.readonly());

        let readonly = second
            .observation
            .after_readonly_update(&second.destination, true)
            .expect("readonly update recaptures the observation");
        assert!(readonly.readonly());
        assert_eq!(readonly.file_hash(), second.observation.file_hash());
        assert_eq!(readonly.identity(), second.observation.identity());
        assert_eq!(readonly.size(), second.observation.size());
        assert_eq!(readonly.modified_ns(), second.observation.modified_ns());
    }

    #[test]
    fn replacement_preserves_old_content() {
        let temp = TempDir::new().expect("temporary directory can be created");
        let destination = TempDir::new().expect("destination can be created");
        let bytes = fixture(900_000);
        let manifest = manifest_from_reader(Cursor::new(&bytes), ChunkingProfile::DEFAULT)
            .expect("fixture can be chunked");
        let path = WirePath::new("replace.bin").expect("path is portable");
        fs::write(destination.path().join(path.as_str()), b"old content")
            .expect("old file can be created");
        let store = Store::open(temp.path()).expect("store can open");
        populate(&store, &bytes, &manifest);

        let outcome = store
            .materialize(&manifest, &path, destination.path())
            .expect("file can be replaced");
        assert!(outcome.replaced_existing);
        assert_eq!(
            fs::read(outcome.destination).expect("file can be read"),
            bytes
        );
        let trash_entries = fs::read_dir(temp.path().join("trash"))
            .expect("trash can be read")
            .count();
        assert_eq!(trash_entries, 1);
    }

    #[test]
    fn corrupt_cached_chunk_is_requested_again() {
        let temp = TempDir::new().expect("temporary directory can be created");
        let bytes = fixture(300_000);
        let manifest = manifest_from_reader(Cursor::new(&bytes), ChunkingProfile::DEFAULT)
            .expect("fixture can be chunked");
        let store = Store::open(temp.path()).expect("store can open");
        populate(&store, &bytes, &manifest);
        let first = manifest.chunks.first().expect("fixture has a chunk");
        fs::write(store.chunks.chunk_path(first.hash), b"corrupt")
            .expect("test can corrupt cached chunk");

        assert!(store.missing_chunks(&manifest).contains(&first.hash));
    }

    #[test]
    fn ingest_file_populates_every_manifest_chunk() {
        let state = TempDir::new().expect("state directory can be created");
        let source = state.path().join("source.bin");
        let bytes = fixture(2 * 1024 * 1024 + 31);
        fs::write(&source, &bytes).expect("source can be written");
        let store = Store::open(state.path().join("private")).expect("store can open");
        let manifest = store
            .ingest_file(&source, ChunkingProfile::DEFAULT)
            .expect("file can be ingested");
        assert_eq!(manifest.file_hash, Hash32::digest(&bytes));
        assert!(store.missing_chunks(&manifest).is_empty());
    }

    #[test]
    fn deletion_is_idempotent_and_preserves_file_content() {
        let state = TempDir::new().expect("state directory can be created");
        let destination = TempDir::new().expect("destination can be created");
        let path = WirePath::new("folder/file.txt").expect("path is portable");
        fs::create_dir(destination.path().join("folder")).expect("folder can be created");
        fs::write(destination.path().join(path.as_str()), b"important")
            .expect("file can be written");
        let store = Store::open(state.path()).expect("store can open");
        let tombstone = Hash32::digest(b"tombstone");

        let first = store
            .remove_path(&path, destination.path(), tombstone)
            .expect("file can be removed");
        assert!(first.removed);
        assert!(first.preserved_in_trash);
        assert!(!first.destination.exists());
        assert_eq!(
            fs::read(
                first
                    .preserved_path
                    .as_ref()
                    .expect("preserved deletion exposes its recovery path")
            )
            .expect("trash content can be read"),
            b"important"
        );

        let second = store
            .remove_path(&path, destination.path(), tombstone)
            .expect("missing path deletion is idempotent");
        assert!(!second.removed);
        assert!(second.preserved_path.is_none());
    }

    #[test]
    fn remote_directory_delete_never_removes_unknown_children() {
        let state = TempDir::new().expect("state directory can be created");
        let destination = TempDir::new().expect("destination can be created");
        let path = WirePath::new("folder").expect("path is portable");
        fs::create_dir(destination.path().join(path.as_str())).expect("folder can be created");
        fs::write(destination.path().join("folder/unindexed.txt"), b"keep")
            .expect("child can be written");
        let store = Store::open(state.path()).expect("store can open");
        assert!(
            store
                .remove_path(&path, destination.path(), Hash32::digest(b"delete"))
                .is_err()
        );
        assert!(destination.path().join("folder/unindexed.txt").exists());
    }

    #[cfg(unix)]
    #[test]
    fn materialization_rejects_symlinked_parent() {
        use std::os::unix::fs::symlink;

        let state = TempDir::new().expect("state directory can be created");
        let destination = TempDir::new().expect("destination can be created");
        let outside = TempDir::new().expect("outside directory can be created");
        symlink(outside.path(), destination.path().join("linked")).expect("symlink can be created");
        let bytes = fixture(300_000);
        let manifest = manifest_from_reader(Cursor::new(&bytes), ChunkingProfile::DEFAULT)
            .expect("fixture can be chunked");
        let store = Store::open(state.path()).expect("store can open");
        populate(&store, &bytes, &manifest);
        let path = WirePath::new("linked/escape.bin").expect("path is portable");

        assert!(
            store
                .materialize(&manifest, &path, destination.path())
                .is_err()
        );
        assert!(!outside.path().join("escape.bin").exists());
    }
}
