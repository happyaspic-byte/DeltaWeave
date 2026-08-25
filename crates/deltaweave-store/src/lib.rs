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
use deltaweave_core::{FileManifest, Hash32, WirePath};
use redb::{Database, ReadableDatabase, TableDefinition};
use serde::{Deserialize, Serialize};

const MANIFESTS: TableDefinition<&str, &[u8]> = TableDefinition::new("manifests");
const OPERATIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("operations");
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

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

        let destination = self.chunk_path(hash);
        if destination.is_file() {
            match self.read_verified(hash) {
                Ok(_) => return Ok(false),
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
                return Ok(false);
            }
            Err(error) => {
                let _ = fs::remove_file(&temporary);
                return Err(error)
                    .with_context(|| format!("failed to install chunk {}", destination.display()));
            }
        }
        sync_directory(destination.parent())?;
        Ok(true)
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
        if destination.is_dir() {
            bail!(
                "refusing to replace destination directory {} with a file",
                destination.display()
            );
        }
        if destination.is_file() && hash_file(&destination)? == manifest.file_hash {
            let operation = committed_operation(path, manifest);
            self.metadata.put_manifest(path, manifest)?;
            self.metadata.put_operation(&operation)?;
            return Ok(MaterializeOutcome {
                destination,
                bytes_written: 0,
                replaced_existing: false,
                already_current: true,
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

        Ok(MaterializeOutcome {
            destination,
            bytes_written: manifest.size,
            replaced_existing,
            already_current: false,
        })
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
    fn chunk_store_rejects_mislabeled_data() {
        let temp = TempDir::new().expect("temporary directory can be created");
        let store = ChunkStore::open(temp.path()).expect("chunk store can open");
        assert!(
            store
                .put_verified(Hash32::digest(b"good"), b"evil")
                .is_err()
        );
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
