//! Physical-volume regressions. Linux CI must provide /tmp and /dev/shm.
#[cfg(target_os = "linux")]
#[test]
fn cross_filesystem_delete_retains_the_open_inode() {
    use deltaweave_core::{Hash32, WirePath};
    use std::{fs, io::Write, os::unix::fs::MetadataExt};
    let root = tempfile::Builder::new()
        .prefix("dw-root-")
        .tempdir_in("/tmp")
        .unwrap();
    let state = tempfile::Builder::new()
        .prefix("dw-state-")
        .tempdir_in("/dev/shm")
        .unwrap();
    assert_ne!(
        fs::metadata(root.path()).unwrap().dev(),
        fs::metadata(state.path()).unwrap().dev()
    );
    let path = root.path().join("work.txt");
    fs::write(&path, b"original").unwrap();
    let mut handle = fs::OpenOptions::new().append(true).open(&path).unwrap();
    let inode = handle.metadata().unwrap().ino();
    let store = deltaweave_store::Store::open_with_recovery_reserver(state.path(), |path| {
        std::fs::create_dir_all(path)?;
        Ok(std::fs::canonicalize(path)?)
    })
    .unwrap();
    let outcome = store
        .remove_path(
            &WirePath::new("work.txt").unwrap(),
            root.path(),
            Hash32::digest(b"delete"),
        )
        .unwrap();
    let artifact = outcome.preserved_path.unwrap();
    assert!(!artifact.starts_with(root.path()));
    assert_eq!(fs::metadata(&artifact).unwrap().ino(), inode);
    handle.write_all(b" late work").unwrap();
    handle.sync_all().unwrap();
    assert_eq!(fs::read(&artifact).unwrap(), b"original late work");
    assert!(!path.exists());
    eprintln!(
        "physical evidence: root device {}, state device {}, artifact {}",
        fs::metadata(root.path()).unwrap().dev(),
        fs::metadata(state.path()).unwrap().dev(),
        artifact.display()
    );
}

#[cfg(target_os = "linux")]
mod journal {
    use deltaweave_core::{ChunkingProfile, Hash32, WirePath};
    use deltaweave_store::{PathObservation, PathTarget, Store};
    use std::{fs, io::Write};

    fn store(path: &std::path::Path) -> Store {
        Store::open_with_recovery_reserver(path, |path| {
            fs::create_dir_all(path)?;
            Ok(fs::canonicalize(path)?)
        })
        .unwrap()
    }
    fn fixture() -> (tempfile::TempDir, tempfile::TempDir, std::path::PathBuf) {
        let base = tempfile::tempdir_in("/tmp").unwrap();
        let state = tempfile::tempdir_in("/dev/shm").unwrap();
        let root = base.path().join("root");
        fs::create_dir(&root).unwrap();
        (base, state, root)
    }
    fn target(store: &Store, base: &std::path::Path) -> PathTarget {
        let source = base.join("incoming-source");
        fs::write(&source, b"owner content").unwrap();
        PathTarget::File(store.ingest_file(source, ChunkingProfile::DEFAULT).unwrap())
    }

    #[test]
    fn recreation_after_capture_is_never_overwritten() {
        let (base, state, root) = fixture();
        let store = store(state.path());
        let path = WirePath::new("file").unwrap();
        fs::write(root.join("file"), b"original").unwrap();
        let expected = PathObservation::read(&root, &path).unwrap();
        let mut change = store
            .prepare_path_change(&root, &path, target(&store, base.path()), expected, false)
            .unwrap();
        store.capture_path_change(&mut change).unwrap();
        fs::write(root.join("file"), b"new occupant").unwrap();
        assert!(store.materialize_path_change(&mut change).is_err());
        assert_eq!(fs::read(root.join("file")).unwrap(), b"new occupant");
        assert_eq!(fs::read(change.artifact).unwrap(), b"original");
    }

    #[test]
    fn edits_before_and_after_capture_force_retry_without_losing_work() {
        for after in [false, true] {
            let (base, state, root) = fixture();
            let store = store(state.path());
            let path = WirePath::new("file").unwrap();
            fs::write(root.join("file"), b"original").unwrap();
            let mut handle = fs::OpenOptions::new()
                .append(true)
                .open(root.join("file"))
                .unwrap();
            let expected = PathObservation::read(&root, &path).unwrap();
            let mut change = store
                .prepare_path_change(&root, &path, target(&store, base.path()), expected, false)
                .unwrap();
            if after {
                store.capture_path_change(&mut change).unwrap();
            }
            handle.write_all(b" late work").unwrap();
            handle.sync_all().unwrap();
            let result = if after {
                store.materialize_path_change(&mut change)
            } else {
                store.capture_path_change(&mut change)
            };
            assert!(result.is_err());
            assert_eq!(fs::read(root.join("file")).unwrap(), b"original late work");
        }
    }

    #[test]
    fn missing_materialized_artifact_stops_recovery() {
        let (base, state, root) = fixture();
        let store = store(state.path());
        let path = WirePath::new("file").unwrap();
        fs::write(root.join("file"), b"local").unwrap();
        let mut change = store
            .prepare_path_change(
                &root,
                &path,
                target(&store, base.path()),
                PathObservation::read(&root, &path).unwrap(),
                false,
            )
            .unwrap();
        store.capture_path_change(&mut change).unwrap();
        store.materialize_path_change(&mut change).unwrap();
        fs::remove_file(&change.artifact).unwrap();
        assert!(
            store.recover_path_changes(&root).is_err(),
            "missing recovery artifact was silently accepted"
        );
        assert_eq!(fs::read(root.join("file")).unwrap(), b"owner content");
    }

    #[test]
    fn physical_replacement_and_both_type_transitions_retain_artifacts() {
        let (base, state, root) = fixture();
        let store = store(state.path());
        let path = WirePath::new("file").unwrap();
        let PathTarget::File(manifest) = target(&store, base.path()) else {
            unreachable!()
        };
        store.materialize(&manifest, &path, &root).unwrap();
        fs::write(root.join("file"), b"modified local bytes").unwrap();
        let outcome = store.materialize(&manifest, &path, &root).unwrap();
        assert_eq!(
            fs::read(outcome.preserved_path.unwrap()).unwrap(),
            b"modified local bytes"
        );
        store.materialize_directory(&path, &root).unwrap();
        assert!(root.join("file").is_dir());
        store.materialize(&manifest, &path, &root).unwrap();
        assert_eq!(fs::read(root.join("file")).unwrap(), b"owner content");
        let first = store
            .remove_path(&path, &root, Hash32::digest(b"same tombstone"))
            .unwrap()
            .preserved_path
            .unwrap();
        fs::write(root.join("file"), b"recreated local bytes").unwrap();
        let second = store
            .remove_path(&path, &root, Hash32::digest(b"same tombstone"))
            .unwrap()
            .preserved_path
            .unwrap();
        assert_ne!(first, second);
        assert_eq!(fs::read(first).unwrap(), b"owner content");
        assert_eq!(fs::read(second).unwrap(), b"recreated local bytes");
    }

    #[test]
    fn incoming_name_cannot_alias_preserved_wire_path() {
        let (base, state, root) = fixture();
        let store = store(state.path());
        let path = WirePath::new(".incoming").unwrap();
        fs::write(root.join(".incoming"), b"private work").unwrap();
        let PathTarget::File(manifest) = target(&store, base.path()) else {
            unreachable!()
        };
        let outcome = store.materialize(&manifest, &path, &root).unwrap();
        assert_eq!(
            fs::read(outcome.preserved_path.unwrap()).unwrap(),
            b"private work"
        );
        assert_eq!(fs::read(root.join(".incoming")).unwrap(), b"owner content");
    }
}

#[test]
fn same_volume_replacement_type_transition_and_noreplace_restore() {
    use deltaweave_core::{ChunkingProfile, WirePath};
    use deltaweave_store::{PathObservation, PathTarget, Store};
    use std::fs;
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    fs::create_dir(&root).unwrap();
    let store = Store::open(base.path().join("state")).unwrap();
    let source = base.path().join("source");
    fs::write(&source, b"remote").unwrap();
    let manifest = store.ingest_file(source, ChunkingProfile::DEFAULT).unwrap();
    let path = WirePath::new("file").unwrap();
    fs::write(root.join("file"), b"local").unwrap();
    let replacement = store.materialize(&manifest, &path, &root).unwrap();
    assert_eq!(
        fs::read(replacement.preserved_path.unwrap()).unwrap(),
        b"local"
    );
    store.materialize_directory(&path, &root).unwrap();
    assert!(root.join("file").is_dir());
    store.materialize(&manifest, &path, &root).unwrap();
    let mut change = store
        .prepare_path_change(
            &root,
            &path,
            PathTarget::Absent,
            PathObservation::read(&root, &path).unwrap(),
            false,
        )
        .unwrap();
    store.capture_path_change(&mut change).unwrap();
    fs::write(root.join("file"), b"recreated").unwrap();
    store.abort_path_change(&mut change).unwrap();
    assert_eq!(fs::read(root.join("file")).unwrap(), b"recreated");
    assert_eq!(fs::read(change.artifact).unwrap(), b"remote");
}

#[cfg(target_os = "linux")]
#[test]
fn symlinked_recovery_vault_is_rejected_before_capture() {
    use deltaweave_core::{Hash32, WirePath};
    use deltaweave_store::Store;
    use std::{fs, os::unix::fs::symlink};
    let base = tempfile::tempdir_in("/tmp").unwrap();
    let state = tempfile::tempdir_in("/dev/shm").unwrap();
    let root = base.path().join("root");
    fs::create_dir(&root).unwrap();
    let outside = base.path().join("outside");
    fs::create_dir(&outside).unwrap();
    fs::write(outside.join("sentinel"), b"external").unwrap();
    fs::write(root.join("file"), b"local bytes").unwrap();
    let vault = base.path().join(format!(
        ".deltaweave-{}.recovery",
        Hash32::digest(root.as_os_str().as_encoded_bytes())
    ));
    symlink(&outside, &vault).unwrap();
    let store = Store::open_with_recovery_reserver(state.path(), |path| {
        fs::create_dir_all(path)?;
        Ok(fs::canonicalize(path)?)
    })
    .unwrap();
    assert!(
        store
            .remove_path(
                &WirePath::new("file").unwrap(),
                &root,
                Hash32::digest(b"delete")
            )
            .is_err(),
        "symlink vault accepted"
    );
    assert_eq!(fs::read(root.join("file")).unwrap(), b"local bytes");
    assert_eq!(fs::read_dir(outside).unwrap().count(), 1);
}

#[cfg(unix)]
#[test]
fn leaf_and_parent_symlinks_never_modify_external_targets() {
    use deltaweave_core::{Hash32, WirePath};
    use deltaweave_store::{PathObservation, PathTarget, Store};
    use std::{fs, os::unix::fs::symlink};
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    fs::create_dir(&root).unwrap();
    let outside = base.path().join("external");
    fs::write(&outside, b"external").unwrap();
    let store = Store::open(base.path().join("state")).unwrap();
    let path = WirePath::new("link").unwrap();
    for target in [&outside, &base.path().join("dangling")] {
        symlink(target, root.join("link")).unwrap();
        let outcome = store
            .remove_path(&path, &root, Hash32::digest(b"remove link"))
            .unwrap();
        assert_eq!(
            fs::read_link(outcome.preserved_path.unwrap()).unwrap(),
            *target
        );
    }
    fs::write(root.join("link"), b"local").unwrap();
    let mut change = store
        .prepare_path_change(
            &root,
            &path,
            PathTarget::Absent,
            PathObservation::read(&root, &path).unwrap(),
            false,
        )
        .unwrap();
    fs::remove_file(root.join("link")).unwrap();
    symlink(&outside, root.join("link")).unwrap();
    assert!(store.capture_path_change(&mut change).is_err());
    assert_eq!(fs::read(&outside).unwrap(), b"external");
    symlink(base.path(), root.join("parent")).unwrap();
    assert!(
        store
            .remove_path(
                &WirePath::new("parent/external").unwrap(),
                &root,
                Hash32::digest(b"delete external")
            )
            .is_err()
    );
    assert_eq!(fs::read(outside).unwrap(), b"external");
}
