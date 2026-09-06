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
    _lock: File,
}
impl RootLease {
    pub fn root(&self) -> &Path {
        &self.root
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
    #[cfg(windows)]
    let home = std::env::var_os("USERPROFILE").context("user profile is unavailable")?;
    #[cfg(not(windows))]
    let home = std::env::var_os("HOME").context("user home is unavailable")?;
    acquire_at(
        &PathBuf::from(home).join(".deltaweave/root-admission"),
        path.as_ref(),
        kind,
    )
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

fn acquire_at(registry: &Path, path: &Path, kind: RootUse) -> Result<RootLease> {
    private_directory(registry)?;
    let registry = fs::canonicalize(registry)?;
    let global = private_file(&registry.join("registry.lock"))?;
    fs2::FileExt::lock_exclusive(&global)?;
    if !path.exists() {
        fs::create_dir_all(path)?;
    }
    let root = fs::canonicalize(path).context("cannot canonicalize admitted root")?;
    ensure!(
        !registry.starts_with(&root),
        "network root contains private admission registry"
    );
    if matches!(kind, RootUse::Managed { .. }) {
        ensure!(root.is_dir(), "managed root must be a directory");
        sidecar_path(&root)?;
    }
    let key = binding(&root)?;
    let db = Database::create(registry.join("roots.redb"))?;
    #[cfg(unix)]
    File::open(&registry)?.sync_all()?;
    {
        let tx = db.begin_write()?;
        tx.open_table(ROOTS)?;
        tx.commit()?;
    }
    let entries: Vec<(String, Entry)> = {
        let read = db.begin_read()?;
        read.open_table(ROOTS)?
            .iter()?
            .map(|row| {
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
                Ok((key.value().to_owned(), entry))
            })
            .collect::<Result<_>>()?
    };
    for (other_key, mut entry) in entries {
        let lock = private_file(&registry.join(format!("{other_key}.lease")))?;
        let live = fs2::FileExt::try_lock_exclusive(&lock).is_err();
        if entry.kind == RootUse::Legacy && !live {
            let tx = db.begin_write()?;
            tx.open_table(ROOTS)?.remove(other_key.as_str())?;
            tx.commit()?;
            continue;
        }
        if matches!(entry.kind, RootUse::Managed { .. }) {
            if entry.preparing {
                write_marker(&entry)?;
                entry.preparing = false;
                persist(&db, &other_key, &entry)?;
            } else {
                let marker = sidecar_path(&entry.root)?;
                ensure!(
                    fs::read(marker)? == postcard::to_stdvec(&entry)?,
                    "managed ownership marker missing or inconsistent"
                );
            }
        }
        if root.starts_with(&entry.root) || entry.root.starts_with(&root) {
            ensure!(
                !live && root == entry.root && kind == entry.kind && kind != RootUse::Legacy,
                "network root overlaps an active or managed root"
            );
        }
    }
    let lock = private_file(&registry.join(format!("{key}.lease")))?;
    fs2::FileExt::try_lock_exclusive(&lock).context("network root already leased")?;
    let mut entry = Entry {
        version: 1,
        root: root.clone(),
        preparing: matches!(kind, RootUse::Managed { .. }),
        kind,
    };
    // Detect an orphaned marker even if a previous registry creation was interrupted.
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
    persist(&db, &key, &entry)?;
    if entry.preparing {
        write_marker(&entry)?;
        entry.preparing = false;
        persist(&db, &key, &entry)?;
    }
    Ok(RootLease { root, _lock: lock })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn managed() -> RootUse {
        RootUse::Managed {
            share: [1; 32],
            owner: [2; 32],
        }
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
        assert!(acquire_at(&registry, &root, managed()).is_ok());
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
        let lease = acquire_at(&base.join("registry"), &base.join(selected), kind);
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
}
