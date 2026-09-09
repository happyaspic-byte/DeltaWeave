//! Host/user-wide admission for network publication and mutation roots.
//!
//! The fixed registry is independent of folder state paths. A managed entry is
//! durable even when its worker is stopped; legacy entries expire only when their
//! OS lifetime lock can be acquired. Current binaries coordinate here; a trusted
//! OS user removing the registry or copying files is an explicit export.

use anyhow::{Context, Result, ensure};
use deltaweave_core::Hash32;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

const PRIVATE: TableDefinition<&str, &[u8]> = TableDefinition::new("private_roots_v1");

const ROOTS: TableDefinition<&str, &[u8]> = TableDefinition::new("root_admission_v1");

/// Publication/mutation role for a root. Managed bindings survive lease release.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RootUse {
    Legacy,
    Managed { share: [u8; 32], owner: [u8; 32] },
}

/// An exclusive OS lifetime lease; retain it through every active disk handler.
#[derive(Debug)]
pub struct RootLease {
    root: PathBuf,
    kind: RootUse,
    private: Vec<PathBuf>,
    _lock: File,
}
impl RootLease {
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Admission identity captured when this lease was acquired.  Consumers
    /// transferring a lease between lifecycle stages must validate this
    /// binding instead of relying on the public path alone.
    pub fn kind(&self) -> &RootUse {
        &self.kind
    }

    /// Canonical private roots reserved together with the public lease.
    pub fn private_roots(&self) -> &[PathBuf] {
        &self.private
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Entry {
    version: u8,
    root: PathBuf,
    kind: RootUse,
    preparing: bool,
}

/// Uses the fixed private directory for the current OS user's application data.
/// Configurable state paths and web data directories do not affect admission.
pub fn acquire(path: impl AsRef<Path>, kind: RootUse) -> Result<RootLease> {
    acquire_with_private(path, kind, &[])
}

/// Atomically preflights a public root and all its external private directories.
/// Private reservations are durable; retain the returned public lease through all
/// handlers. No target directory is created when namespace preflight is denied.
pub fn acquire_with_private(
    path: impl AsRef<Path>,
    kind: RootUse,
    private: &[PathBuf],
) -> Result<RootLease> {
    Ok(admit_with_private(path.as_ref(), kind, private, |_, _| Ok(()))?.0)
}

/// Permanently excludes a private directory from every current-binary public
/// namespace, even after shutdown. Private/private nesting is allowed. Call this
/// before writing device state, member state, or an external recovery vault.
/// Successful reservation creates the directory; later preparation failures keep
/// the reservation. This API never removes ownership or private reservations.
pub fn reserve_private(path: impl AsRef<Path>) -> Result<PathBuf> {
    let (_, root) = admit_at(
        &registry_path()?,
        None,
        &[path.as_ref().to_path_buf()],
        |_, private| Ok(private[0].clone()),
    )?;
    Ok(root)
}

/// Permanently excludes and prepares one private directory under the global
/// admission lock.
///
/// The callback runs only after all namespace checks, private directory
/// creation, and the private reservation commit.  It is called for each
/// directory component created by this invocation in parent-to-child order
/// with `created=true`; if the requested final directory already existed, it
/// is called once for that final directory with `created=false`.  The callback
/// must be short, synchronous, and must not recurse into admission.
pub fn reserve_private_prepared(
    path: impl AsRef<Path>,
    mut prepare: impl FnMut(&Path, bool) -> Result<()>,
) -> Result<PathBuf> {
    let (_, root) = admit_at_with_creation(
        &registry_path()?,
        None,
        &[path.as_ref().to_path_buf()],
        &[],
        true,
        |_, private, created| {
            ensure!(private.len() == 1);
            let prepared_final = created.iter().any(|path| path == &private[0]);
            for created_path in created {
                prepare(created_path, true)?;
            }
            if !prepared_final {
                prepare(&private[0], false)?;
            }
            Ok(private[0].clone())
        },
    )?;
    Ok(root)
}

/// Permanently excludes one private file and public roots containing it.
///
/// Existing aliases resolve to the actual file before admission. Only the exact
/// file path is reserved, so sibling public directories remain usable. Missing
/// parents are created privately; the file itself is not created and existing
/// parent permissions are unchanged. Use the returned path when opening the file.
pub fn reserve_private_file(path: impl AsRef<Path>) -> Result<PathBuf> {
    ensure!(
        path.as_ref().file_name().is_some(),
        "private file must have a filename"
    );
    let (_, file) = admit_at_with_files(
        &registry_path()?,
        None,
        &[],
        &[path.as_ref().to_path_buf()],
        |_, private| Ok(private[0].clone()),
    )?;
    Ok(file)
}

fn registry_path() -> Result<PathBuf> {
    #[cfg(windows)]
    let home = std::env::var_os("USERPROFILE").context("user profile is unavailable")?;
    #[cfg(not(windows))]
    let home = std::env::var_os("HOME").context("user home is unavailable")?;
    Ok(PathBuf::from(home).join(".deltaweave/root-admission"))
}

/// Lock order: service lifecycle -> global admission file lock -> short catalog
/// callback. The callback must not recurse into admission or await. Runtime gates
/// and disk handlers are acquired only after this function returns.
pub(crate) fn admit_with_private<T>(
    path: &Path,
    kind: RootUse,
    private: &[PathBuf],
    prepare: impl FnOnce(&Path, &[PathBuf]) -> Result<T>,
) -> Result<(RootLease, T)> {
    let (lease, result) = admit_at(
        &registry_path()?,
        Some((path, kind)),
        private,
        |root, private| prepare(root.context("missing public admission")?, private),
    )?;
    Ok((lease.context("missing public lease")?, result))
}

pub(crate) fn private_directory(path: &Path) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)?;
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()?;
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
        }
    }
    ensure!(
        !fs::symlink_metadata(path)?.file_type().is_symlink(),
        "private directory must not be a symlink"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        ensure!(
            fs::metadata(path)?.permissions().mode() & 0o077 == 0,
            "private directory permissions are too broad"
        );
    }
    Ok(())
}

fn private_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    ensure!(!path.is_symlink(), "private file must not be a symlink");
    Ok(options.open(path)?)
}

/// Creates a private directory using either the legacy recursive behavior or
/// one-component-at-a-time creation for the prepared API.  The latter avoids
/// treating a recursively created Windows leaf as an already trusted private
/// directory and reports every component created by this call so the caller
/// can harden them in parent-to-child order.
fn create_private_directory(path: &Path, report_created: bool) -> Result<Vec<PathBuf>> {
    if !report_created {
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(path)?;
        return Ok(Vec::new());
    }

    let mut current = PathBuf::new();
    let mut missing = false;
    let mut created = Vec::new();
    for component in path.components() {
        current.push(component.as_os_str());
        if !missing {
            match fs::symlink_metadata(&current) {
                Ok(metadata) => {
                    ensure!(
                        metadata.is_dir(),
                        "private path component is not a directory"
                    );
                    ensure!(
                        fs::canonicalize(&current)? == current,
                        "private path component changed during preparation"
                    );
                    continue;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    missing = true;
                }
                Err(error) => return Err(error.into()),
            }
        }

        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        match builder.create(&current) {
            Ok(()) => created.push(current.clone()),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let metadata = fs::symlink_metadata(&current)?;
                ensure!(
                    metadata.is_dir(),
                    "private path component is not a directory"
                );
                ensure!(
                    fs::canonicalize(&current)? == current,
                    "private path component changed during preparation"
                );
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(created)
}

fn binding(root: &Path) -> Result<String> {
    Ok(Hash32::digest(&postcard::to_stdvec(root)?).to_string())
}
fn sidecar_path(root: &Path) -> Result<PathBuf> {
    Ok(root
        .parent()
        .context("managed root requires a usable parent")?
        .join(format!(".deltaweave-{}.managed", binding(root)?)))
}
fn persist(db: &Database, key: &str, entry: &Entry) -> Result<()> {
    let bytes = postcard::to_stdvec(entry)?;
    let tx = db.begin_write()?;
    tx.open_table(ROOTS)?.insert(key, bytes.as_slice())?;
    tx.commit()?;
    Ok(())
}
fn write_marker(entry: &Entry) -> Result<()> {
    let path = sidecar_path(&entry.root)?;
    let mut expected = entry.clone();
    expected.preparing = false;
    let bytes = postcard::to_stdvec(&expected)?;
    if path.exists() {
        ensure!(
            fs::read(&path)? == bytes,
            "managed ownership marker mismatch"
        );
    } else {
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut marker = options.open(&path)?;
        marker.write_all(&bytes)?;
        marker.sync_all()?;
        #[cfg(unix)]
        File::open(path.parent().context("marker parent unavailable")?)?.sync_all()?;
    }
    Ok(())
}

/// Resolves existing components (including aliases) and then missing suffixes
/// without mkdir. Parent components are evaluated against the canonical prefix.
fn prospective_root(path: &Path) -> Result<PathBuf> {
    use std::path::Component;
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut root = PathBuf::new();
    for component in absolute.components() {
        match component {
            // A Windows drive/UNC prefix is not a complete absolute path until
            // RootDir is appended; querying a verbatim prefix alone fails.
            Component::Prefix(_) => root.push(component.as_os_str()),
            Component::ParentDir => {
                root.pop();
            }
            Component::CurDir => {}
            other => {
                root.push(other.as_os_str());
                match fs::symlink_metadata(&root) {
                    Ok(_) => {
                        root = fs::canonicalize(&root).context("cannot resolve admitted path")?
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
    }
    Ok(root)
}
fn overlaps(a: &Path, b: &Path) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

#[cfg(test)]
fn acquire_at(registry: &Path, path: &Path, kind: RootUse) -> Result<RootLease> {
    Ok(admit_at(registry, Some((path, kind)), &[], |_, _| Ok(()))?
        .0
        .unwrap())
}

#[derive(Default)]
struct AdmissionCatalog {
    public: Vec<(String, Entry)>,
    private: Vec<PathBuf>,
}
fn read_admission(path: &Path) -> Result<AdmissionCatalog> {
    if !path.exists() {
        return Ok(AdmissionCatalog::default());
    }
    let db = match redb::ReadOnlyDatabase::open(path) {
        Ok(db) => db,
        Err(redb::DatabaseError::RepairAborted) => {
            // A process may die after committing intent but before redb saves
            // allocator state on close. Recover only that specific condition,
            // under the global lock, without resetting either reservation table.
            drop(Database::open(path)?);
            redb::ReadOnlyDatabase::open(path)?
        }
        Err(error) => return Err(error.into()),
    };
    let read = db.begin_read()?;
    let mut entries = Vec::new();
    match read.open_table(ROOTS) {
        Ok(table) => {
            for row in table.iter()? {
                let (key, value) = row?;
                ensure!(value.value().len() <= 32768, "admission entry too large");
                let entry: Entry = postcard::from_bytes(value.value())?;
                ensure!(
                    postcard::to_stdvec(&entry)? == value.value()
                        && (!entry.preparing || matches!(entry.kind, RootUse::Managed { .. })),
                    "invalid admission encoding"
                );
                ensure!(
                    entry.version == 1 && binding(&entry.root)? == key.value(),
                    "invalid admission binding"
                );
                entries.push((key.value().to_owned(), entry));
            }
        }
        Err(redb::TableError::TableDoesNotExist(_)) => {}
        Err(error) => return Err(error.into()),
    }
    let mut private = Vec::new();
    match read.open_table(PRIVATE) {
        Ok(table) => {
            for row in table.iter()? {
                let (key, value) = row?;
                ensure!(
                    value.value().len() <= 32768,
                    "private reservation too large"
                );
                let root: PathBuf = postcard::from_bytes(value.value())?;
                ensure!(
                    postcard::to_stdvec(&root)? == value.value() && binding(&root)? == key.value(),
                    "invalid private reservation"
                );
                private.push(root);
            }
        }
        Err(redb::TableError::TableDoesNotExist(_)) => {}
        Err(error) => return Err(error.into()),
    }
    Ok(AdmissionCatalog {
        public: entries,
        private,
    })
}

fn admit_at<T>(
    registry: &Path,
    public: Option<(&Path, RootUse)>,
    private: &[PathBuf],
    prepare: impl FnOnce(Option<&Path>, &[PathBuf]) -> Result<T>,
) -> Result<(Option<RootLease>, T)> {
    admit_at_with_files(registry, public, private, &[], prepare)
}

fn admit_at_with_files<T>(
    registry: &Path,
    public: Option<(&Path, RootUse)>,
    private_directories: &[PathBuf],
    private_files: &[PathBuf],
    prepare: impl FnOnce(Option<&Path>, &[PathBuf]) -> Result<T>,
) -> Result<(Option<RootLease>, T)> {
    admit_at_with_creation(
        registry,
        public,
        private_directories,
        private_files,
        false,
        |public, private, _| prepare(public, private),
    )
}

fn admit_at_with_creation<T>(
    registry: &Path,
    public: Option<(&Path, RootUse)>,
    private_directories: &[PathBuf],
    private_files: &[PathBuf],
    prepared_creation: bool,
    prepare: impl FnOnce(Option<&Path>, &[PathBuf], &[PathBuf]) -> Result<T>,
) -> Result<(Option<RootLease>, T)> {
    // The registry itself is private even before the initial bootstrap mkdir.
    let proposed_registry = prospective_root(registry)?;
    if let Some((path, _)) = &public {
        ensure!(
            !overlaps(&prospective_root(path)?, &proposed_registry),
            "network root overlaps private admission registry"
        );
    }
    private_directory(registry)?;
    let registry = fs::canonicalize(registry)?;
    let global = private_file(&registry.join("registry.lock"))?;
    fs2::FileExt::lock_exclusive(&global)?;
    let public = public
        .map(|(path, kind)| Ok::<_, anyhow::Error>((prospective_root(path)?, kind)))
        .transpose()?;
    let private: Vec<PathBuf> = private_directories
        .iter()
        .chain(private_files)
        .map(|path| prospective_root(path))
        .collect::<Result<_>>()?;
    if let Some((root, kind)) = &public {
        ensure!(
            !overlaps(root, &registry),
            "network root overlaps private admission registry"
        );
        ensure!(
            !private.iter().any(|path| overlaps(root, path)),
            "network root overlaps requested private state"
        );
        if matches!(kind, RootUse::Managed { .. }) {
            sidecar_path(root)?;
        }
    }
    let catalog = read_admission(&registry.join("roots.redb"))?;
    for root in catalog.private {
        if let Some((public, _)) = &public {
            ensure!(
                !overlaps(public, &root),
                "network root overlaps reserved private state"
            );
        }
    }
    let mut stale = Vec::new();
    for (other_key, mut entry) in catalog.public {
        let lock = private_file(&registry.join(format!("{other_key}.lease")))?;
        let live = fs2::FileExt::try_lock_exclusive(&lock).is_err();
        if entry.kind == RootUse::Legacy && !live {
            stale.push(other_key);
            continue;
        }
        ensure!(
            !private.iter().any(|path| overlaps(path, &entry.root)),
            "private state overlaps an active or managed root"
        );
        if matches!(entry.kind, RootUse::Managed { .. }) {
            if entry.preparing {
                write_marker(&entry)?;
                entry.preparing = false;
                // Recovery of previously committed intent is independent of this request.
                persist(
                    &Database::open(registry.join("roots.redb"))?,
                    &other_key,
                    &entry,
                )?;
            } else {
                ensure!(
                    fs::read(sidecar_path(&entry.root)?)? == postcard::to_stdvec(&entry)?,
                    "managed ownership marker missing or inconsistent"
                );
            }
        }
        if let Some((root, kind)) = &public
            && overlaps(root, &entry.root)
        {
            ensure!(
                !live && *root == entry.root && *kind == entry.kind && *kind != RootUse::Legacy,
                "network root overlaps an active or managed root"
            );
        }
    }
    // Complete every overlap check before touching any requested directory or
    // calling the catalog writer. Global serialization continues through commit.
    let public_state = if let Some((root, kind)) = public {
        let key = binding(&root)?;
        let lock = private_file(&registry.join(format!("{key}.lease")))?;
        fs2::FileExt::try_lock_exclusive(&lock).context("network root already leased")?;
        let entry = Entry {
            version: 1,
            root: root.clone(),
            preparing: matches!(kind, RootUse::Managed { .. }),
            kind,
        };
        let marker = sidecar_path(&root)?;
        if marker.exists() {
            let bytes = fs::read(marker)?;
            ensure!(bytes.len() <= 32768, "managed marker too large");
            let stored: Entry = postcard::from_bytes(&bytes)?;
            ensure!(
                stored.version == 1
                    && stored.root == root
                    && stored.kind == entry.kind
                    && entry.kind != RootUse::Legacy,
                "managed ownership marker conflicts with requested root use"
            );
        }
        if !root.exists() {
            fs::create_dir_all(&root)?;
        }
        ensure!(
            fs::canonicalize(&root)? == root,
            "admitted root changed during preparation"
        );
        if matches!(entry.kind, RootUse::Managed { .. }) {
            ensure!(root.is_dir(), "managed root must be a directory");
        }
        Some((
            key,
            entry.clone(),
            RootLease {
                root,
                kind: entry.kind,
                private: private.clone(),
                _lock: lock,
            },
        ))
    } else {
        None
    };
    let mut private_created = Vec::new();
    for (index, path) in private.iter().enumerate() {
        let directory = if index < private_directories.len() {
            path.as_path()
        } else {
            ensure!(
                !path.try_exists()? || path.is_file(),
                "private file path is not a regular file"
            );
            path.parent().context("private file has no parent")?
        };
        if index < private_directories.len() {
            private_created.extend(create_private_directory(directory, prepared_creation)?);
        } else {
            create_private_directory(directory, false)?;
        }
        ensure!(
            fs::canonicalize(directory)? == directory,
            "private root changed during preparation"
        );
        if index >= private_directories.len() && path.try_exists()? {
            ensure!(
                fs::canonicalize(path)? == *path && path.is_file(),
                "private file changed during preparation"
            );
        }
    }
    private_created.sort_by_key(|path| path.components().count());
    let db = Database::create(registry.join("roots.redb"))?;
    // The writable catalog is opened only after namespace preflight succeeds.
    if db.begin_read()?.open_table(ROOTS).is_err() {
        let tx = db.begin_write()?;
        tx.open_table(ROOTS)?;
        tx.commit()?;
    }
    #[cfg(unix)]
    File::open(&registry)?.sync_all()?;
    // Persist private exclusions before the callback may write any sensitive data.
    // A later preparation failure intentionally leaves conservative reservations.
    if !private.is_empty() || !stale.is_empty() {
        let tx = db.begin_write()?;
        {
            let mut table = tx.open_table(PRIVATE)?;
            for path in &private {
                table.insert(
                    binding(path)?.as_str(),
                    postcard::to_stdvec(path)?.as_slice(),
                )?;
            }
        }
        for key in stale {
            tx.open_table(ROOTS)?.remove(key.as_str())?;
        }
        tx.commit()?;
    }
    let result = prepare(
        public_state.as_ref().map(|(_, _, lease)| lease.root()),
        &private,
        &private_created,
    )?;
    let lease = if let Some((key, mut entry, lease)) = public_state {
        ensure!(
            fs::canonicalize(lease.root())? == lease.root(),
            "admitted root changed during preparation"
        );
        persist(&db, &key, &entry)?;
        if entry.preparing {
            write_marker(&entry)?;
            entry.preparing = false;
            persist(&db, &key, &entry)?;
        }
        Some(lease)
    } else {
        None
    };
    Ok((lease, result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn canonical_roots_resolve_existing_paths_and_missing_suffixes() {
        let temp = TempDir::new().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        assert_eq!(prospective_root(&root).unwrap(), root);
        let missing = root.join("missing/nested");
        assert_eq!(prospective_root(&missing).unwrap(), missing);
        assert!(!missing.exists(), "admission must not create directories");
    }

    fn managed() -> RootUse {
        RootUse::Managed {
            share: [1; 32],
            owner: [2; 32],
        }
    }

    #[test]
    fn managed_lease_retains_public_and_private_binding_for_transfer() {
        let temp = TempDir::new().unwrap();
        let registry = temp.path().join("registry");
        let root = temp.path().join("public");
        let state = temp.path().join("private/state");
        let expected_root = fs::canonicalize(&root).unwrap_or_else(|_| root.clone());
        let expected_kind = managed();
        let lease = admit_at(
            &registry,
            Some((&root, expected_kind.clone())),
            std::slice::from_ref(&state),
            |_, _| Ok(()),
        )
        .unwrap()
        .0
        .unwrap();

        assert_eq!(lease.root(), expected_root);
        assert_eq!(lease.kind(), &expected_kind);
        assert_eq!(lease.private_roots(), &[fs::canonicalize(state).unwrap()]);
    }

    #[test]
    fn managed_roots_deny_exact_ancestors_descendants_and_publication_after_restart() {
        let temp = TempDir::new().unwrap();
        let registry = temp.path().join("registry");
        let root = temp.path().join("public/owned");
        fs::create_dir_all(root.join("nested")).unwrap();
        fs::write(root.join("secret.txt"), b"protected").unwrap();
        let lease = acquire_at(&registry, &root, managed()).unwrap();
        for path in [
            &root,
            &root.join("nested"),
            &root.join("secret.txt"),
            &temp.path().join("public"),
        ] {
            assert!(
                acquire_at(&registry, path, RootUse::Legacy).is_err(),
                "allowed {}",
                path.display()
            );
        }
        assert!(
            acquire_at(
                &registry,
                &temp.path().join("public/owned2"),
                RootUse::Legacy
            )
            .is_ok()
        );
        drop(lease);
        assert!(acquire_at(&registry, &root, RootUse::Legacy).is_err());
        drop(
            acquire_at(&registry, &root, managed())
                .expect("same managed binding must be reacquired after lease release"),
        );
        assert_eq!(fs::read(root.join("secret.txt")).unwrap(), b"protected");
    }

    #[test]
    fn legacy_ancestor_and_descendant_leases_prevent_managed_creation() {
        for legacy_parent in [false, true] {
            let temp = TempDir::new().unwrap();
            let registry = temp.path().join("registry");
            let parent = temp.path().join("public");
            let child = parent.join("nested");
            fs::create_dir_all(&child).unwrap();
            let (legacy, target) = if legacy_parent {
                (&parent, &child)
            } else {
                (&child, &parent)
            };
            let lease = acquire_at(&registry, legacy, RootUse::Legacy).unwrap();
            assert!(acquire_at(&registry, target, managed()).is_err());
            drop(lease);
            assert!(acquire_at(&registry, target, managed()).is_ok());
        }
    }

    #[test]
    fn managed_sidecar_mismatch_or_loss_fails_closed() {
        let temp = TempDir::new().unwrap();
        let registry = temp.path().join("registry");
        let root = temp.path().join("public");
        drop(acquire_at(&registry, &root, managed()).unwrap());
        let marker = sidecar_path(&fs::canonicalize(&root).unwrap()).unwrap();
        fs::write(&marker, b"corrupt").unwrap();
        assert!(acquire_at(&registry, &root, managed()).is_err());
        assert!(acquire_at(&registry, &root, RootUse::Legacy).is_err());
        fs::remove_file(marker).unwrap();
        assert!(acquire_at(&registry, &root, RootUse::Legacy).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn canonical_aliases_cannot_bypass_admission() {
        let temp = TempDir::new().unwrap();
        let registry = temp.path().join("registry");
        let root = temp.path().join("public");
        let lease = acquire_at(&registry, &root, managed()).unwrap();
        fs::create_dir(root.join("nested")).unwrap();
        std::os::unix::fs::symlink(&root, temp.path().join("alias")).unwrap();
        assert!(
            acquire_at(
                &registry,
                &temp.path().join("alias/nested/.."),
                RootUse::Legacy
            )
            .is_err()
        );
        drop(lease);
    }

    #[test]
    fn admission_registry_is_private_before_bootstrap_and_for_descendants() {
        let temp = TempDir::new().unwrap();
        let registry = temp.path().join("not-created/private-admission");
        for root in [
            &temp.path().join("not-created"),
            &registry,
            &registry.join("missing"),
        ] {
            assert!(acquire_at(&registry, root, RootUse::Legacy).is_err());
            assert!(!temp.path().join("not-created").exists());
        }
    }

    #[test]
    fn private_reservations_nest_persist_and_block_public_in_both_orders() {
        let temp = TempDir::new().unwrap();
        let registry = temp.path().join("registry");
        let private = temp.path().join("private");
        for root in [&private, &private.join("nested")] {
            admit_at(&registry, None, std::slice::from_ref(root), |_, _| Ok(())).unwrap();
        }
        for root in [&private, &private.join("new/deep"), temp.path()] {
            assert!(acquire_at(&registry, root, RootUse::Legacy).is_err());
        }
        assert!(!private.join("new").exists());
        let public = temp.path().join("public");
        let lease = acquire_at(&registry, &public, managed()).unwrap();
        let before = fs::read(registry.join("roots.redb")).unwrap();
        for root in [&public, &public.join("new/deep"), temp.path()] {
            assert!(
                admit_at(
                    &registry,
                    None,
                    std::slice::from_ref(&root.to_path_buf()),
                    |_, _| Ok(())
                )
                .is_err()
            );
            assert!(
                fs::read(registry.join("roots.redb")).unwrap() == before,
                "denial changed admission catalog"
            );
        }
        assert!(!public.join("new").exists());
        drop(lease);
        assert!(admit_at(&registry, None, &[public], |_, _| Ok(())).is_err());
    }

    #[test]
    fn failed_valid_preparation_retains_private_reservation_without_public_intent() {
        let temp = TempDir::new().unwrap();
        let registry = temp.path().join("registry");
        let public = temp.path().join("public");
        let private = temp.path().join("state");
        assert!(
            admit_at(
                &registry,
                Some((&public, managed())),
                std::slice::from_ref(&private),
                |_, _| -> Result<()> { anyhow::bail!("preparation failed") }
            )
            .is_err()
        );
        assert!(public.is_dir());
        assert!(private.is_dir());
        assert!(!sidecar_path(&public).unwrap().exists());
        assert!(acquire_at(&registry, &private, RootUse::Legacy).is_err());
        assert!(acquire_at(&registry, &public, managed()).is_ok());
    }

    #[test]
    fn prepared_private_creation_reports_new_components_in_parent_order() {
        let temp = TempDir::new().unwrap();
        let registry = temp.path().join("registry");
        let target = temp.path().join("private/nested/leaf");
        let expected_target = fs::canonicalize(&target).unwrap_or_else(|_| target.clone());
        let (_, returned) =
            admit_at_with_creation(
                &registry,
                None,
                std::slice::from_ref(&target),
                &[],
                true,
                |_, private, created| {
                    assert_eq!(private, std::slice::from_ref(&expected_target));
                    assert_eq!(created.len(), 3);
                    assert!(created.windows(2).all(|pair| {
                        pair[0].components().count() < pair[1].components().count()
                    }));
                    assert_eq!(created.last(), Some(&expected_target));
                    Ok(private[0].clone())
                },
            )
            .unwrap();
        assert_eq!(returned, expected_target);
        assert!(target.is_dir());
        assert!(
            read_admission(&registry.join("roots.redb"))
                .unwrap()
                .private
                .contains(&expected_target)
        );
    }

    #[test]
    fn prepared_private_existing_leaf_reports_no_creation_and_preserves_parent() {
        let temp = TempDir::new().unwrap();
        let registry = temp.path().join("registry");
        let target = temp.path().join("private/nested/leaf");
        fs::create_dir_all(&target).unwrap();
        let (_, returned) = admit_at_with_creation(
            &registry,
            None,
            std::slice::from_ref(&target),
            &[],
            true,
            |_, private, created| {
                assert!(created.is_empty());
                Ok(private[0].clone())
            },
        )
        .unwrap();
        assert_eq!(returned, fs::canonicalize(target).unwrap());
    }

    #[test]
    fn prepared_private_overlap_is_rejected_before_creation_or_callback() {
        let temp = TempDir::new().unwrap();
        let registry = temp.path().join("registry");
        let public = temp.path().join("public");
        let target = public.join("private/nested");
        let _lease = acquire_at(&registry, &public, managed()).unwrap();
        let callback_called = std::sync::atomic::AtomicBool::new(false);
        let result = admit_at_with_creation(
            &registry,
            None,
            std::slice::from_ref(&target),
            &[],
            true,
            |_, _, _| {
                callback_called.store(true, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            },
        );
        assert!(result.is_err());
        assert!(!target.exists());
        assert!(!callback_called.load(std::sync::atomic::Ordering::Relaxed));
    }

    fn wait_file(path: &Path) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while !path.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "process barrier timed out: {}",
                path.display()
            );
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    }
    #[test]
    fn process_worker() {
        let Some(base) = std::env::var_os("DW_ADMISSION_PROCESS") else {
            return;
        };
        let base = PathBuf::from(base);
        let role = std::env::var("DW_ADMISSION_ROLE").unwrap();
        fs::write(base.join(format!("{role}.ready")), b"ready").unwrap();
        wait_file(&base.join("go"));
        let kind = if role == "managed" {
            managed()
        } else {
            RootUse::Legacy
        };
        let selected = std::env::var("DW_ADMISSION_ROOT").unwrap_or_else(|_| "public".into());
        let lease = if role == "private" {
            admit_at(
                &base.join("registry"),
                None,
                &[base.join(selected)],
                |_, _| Ok(()),
            )
            .map(|_| None)
        } else {
            acquire_at(&base.join("registry"), &base.join(selected), kind).map(Some)
        };
        fs::write(
            base.join(format!("{role}.result.tmp")),
            if lease.is_ok() {
                b"ok".as_slice()
            } else {
                b"denied".as_slice()
            },
        )
        .unwrap();
        fs::rename(
            base.join(format!("{role}.result.tmp")),
            base.join(format!("{role}.result")),
        )
        .unwrap();
        wait_file(&base.join("release"));
        drop(lease);
    }
    fn child(base: &Path, role: &str) -> std::process::Child {
        child_at(base, role, "public")
    }
    fn child_at(base: &Path, role: &str, root: &str) -> std::process::Child {
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "root_admission::tests::process_worker",
                "--nocapture",
            ])
            .env("DW_ADMISSION_PROCESS", base)
            .env("DW_ADMISSION_ROLE", role)
            .env("DW_ADMISSION_ROOT", root)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap()
    }
    #[test]
    fn separate_process_check_register_race_has_exactly_one_winner() {
        let temp = TempDir::new().unwrap();
        let base = temp.path();
        let mut legacy = child(base, "legacy");
        let mut owner = child(base, "managed");
        wait_file(&base.join("legacy.ready"));
        wait_file(&base.join("managed.ready"));
        fs::write(base.join("go"), b"go").unwrap();
        wait_file(&base.join("legacy.result"));
        wait_file(&base.join("managed.result"));
        let a = fs::read(base.join("legacy.result")).unwrap();
        let b = fs::read(base.join("managed.result")).unwrap();
        fs::write(base.join("release"), b"release").unwrap();
        assert!(legacy.wait().unwrap().success());
        assert!(owner.wait().unwrap().success());
        assert_ne!(
            a == b"ok",
            b == b"ok",
            "both processes admitted or both denied"
        );
    }
    #[test]
    fn crashed_legacy_lease_is_reaped_but_preparing_managed_entry_survives() {
        let temp = TempDir::new().unwrap();
        let base = temp.path();
        let mut legacy = child(base, "legacy");
        wait_file(&base.join("legacy.ready"));
        fs::write(base.join("go"), b"go").unwrap();
        wait_file(&base.join("legacy.result"));
        assert_eq!(fs::read(base.join("legacy.result")).unwrap(), b"ok");
        legacy.kill().unwrap();
        legacy.wait().unwrap();
        drop(acquire_at(&base.join("registry"), &base.join("public"), managed()).unwrap());
        let root = fs::canonicalize(base.join("public")).unwrap();
        let db = Database::create(base.join("registry/roots.redb")).unwrap();
        persist(
            &db,
            &binding(&root).unwrap(),
            &Entry {
                version: 1,
                root: root.clone(),
                kind: managed(),
                preparing: true,
            },
        )
        .unwrap();
        drop(db);
        fs::remove_file(sidecar_path(&root).unwrap()).unwrap();
        assert!(acquire_at(&base.join("registry"), &root, RootUse::Legacy).is_err());
        assert!(sidecar_path(&root).unwrap().exists());
        assert!(acquire_at(&base.join("registry"), &root, managed()).is_ok());
    }

    #[test]
    fn separate_process_hierarchy_conflicts_hold_in_both_start_orders() {
        for managed_first in [true, false] {
            for (managed_root, legacy_root) in [
                ("public", "public"),
                ("public", "public/nested"),
                ("public/nested", "public"),
            ] {
                let temp = TempDir::new().unwrap();
                let base = temp.path();
                let (first_role, first_root, second_role, second_root) = if managed_first {
                    ("managed", managed_root, "legacy", legacy_root)
                } else {
                    ("legacy", legacy_root, "managed", managed_root)
                };
                let mut first = child_at(base, first_role, first_root);
                wait_file(&base.join(format!("{first_role}.ready")));
                fs::write(base.join("go"), b"go").unwrap();
                wait_file(&base.join(format!("{first_role}.result")));
                assert_eq!(
                    fs::read(base.join(format!("{first_role}.result"))).unwrap(),
                    b"ok"
                );
                let mut second = child_at(base, second_role, second_root);
                wait_file(&base.join(format!("{second_role}.result")));
                let denied = fs::read(base.join(format!("{second_role}.result"))).unwrap();
                fs::write(base.join("release"), b"release").unwrap();
                assert!(first.wait().unwrap().success());
                assert!(second.wait().unwrap().success());
                assert_eq!(denied, b"denied");
            }
        }
    }
    #[test]
    fn separate_process_private_public_hierarchy_preflight_has_one_winner() {
        for (private_root, public_root) in [
            ("area", "area"),
            ("area", "area/nested"),
            ("area/nested", "area"),
        ] {
            let temp = TempDir::new().unwrap();
            let base = temp.path();
            let mut private = child_at(base, "private", private_root);
            let mut public = child_at(base, "managed", public_root);
            wait_file(&base.join("private.ready"));
            wait_file(&base.join("managed.ready"));
            fs::write(base.join("go"), b"go").unwrap();
            wait_file(&base.join("private.result"));
            wait_file(&base.join("managed.result"));
            let a = fs::read(base.join("private.result")).unwrap();
            let b = fs::read(base.join("managed.result")).unwrap();
            fs::write(base.join("release"), b"release").unwrap();
            assert!(private.wait().unwrap().success());
            assert!(public.wait().unwrap().success());
            assert_ne!(
                a == b"ok",
                b == b"ok",
                "private/public registration race did not have exactly one winner"
            );
        }
    }
    #[test]
    fn dirty_catalog_worker() {
        let Some(base) = std::env::var_os("DW_DIRTY_ADMISSION") else {
            return;
        };
        let base = PathBuf::from(base);
        let db = Database::open(base.join("registry/roots.redb")).unwrap();
        let tx = db.begin_write().unwrap();
        let root = base.join("private");
        tx.open_table(PRIVATE)
            .unwrap()
            .insert(
                binding(&root).unwrap().as_str(),
                postcard::to_stdvec(&root).unwrap().as_slice(),
            )
            .unwrap();
        tx.commit().unwrap();
        fs::write(base.join("dirty-ready"), b"ready").unwrap();
        wait_file(&base.join("never-release"));
        drop(db);
    }
    #[test]
    fn dirty_catalog_recovers_without_losing_private_reservations() {
        let temp = TempDir::new().unwrap();
        let base = temp.path();
        admit_at(
            &base.join("registry"),
            None,
            &[base.join("private")],
            |_, _| Ok(()),
        )
        .unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "root_admission::tests::dirty_catalog_worker",
                "--nocapture",
            ])
            .env("DW_DIRTY_ADMISSION", base)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        wait_file(&base.join("dirty-ready"));
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(
            acquire_at(&base.join("registry"), &base.join("public"), managed()).is_ok(),
            "dirty admission catalog prevented recovery"
        );
        assert!(
            acquire_at(
                &base.join("registry"),
                &base.join("private/missing"),
                RootUse::Legacy
            )
            .is_err()
        );
        assert!(!base.join("private/missing").exists());
    }
}
