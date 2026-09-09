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

#[test]
fn private_verified_materialization_is_cas_only() {
    use deltaweave_core::{ChunkingProfile, WirePath};
    use deltaweave_store::Store;
    use std::fs;

    let base = tempfile::tempdir().unwrap();
    let state = base.path().join("state");
    let stage_root = state.join("managed-stage");
    fs::create_dir_all(&stage_root).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&stage_root, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let source = base.path().join("source");
    fs::write(&source, b"private verified bytes").unwrap();
    let store = Store::open(&state).unwrap();
    let manifest = store
        .ingest_file(&source, ChunkingProfile::DEFAULT)
        .unwrap();
    let path = WirePath::new("round/file.bin").unwrap();
    let sentinel = WirePath::new("sentinel").unwrap();
    store.metadata().put_manifest(&sentinel, &manifest).unwrap();

    let installed = store
        .materialize_private_verified(&manifest, &stage_root, &path)
        .unwrap();
    assert_eq!(fs::read(&installed).unwrap(), b"private verified bytes");
    assert_eq!(
        store.metadata().get_manifest(&sentinel).unwrap(),
        Some(manifest)
    );
    assert!(store.path_changes().unwrap().is_empty());
}

#[test]
fn private_verified_materialization_rejects_bad_cas_and_occupied_destinations() {
    use deltaweave_core::{ChunkingProfile, Hash32, WirePath};
    use deltaweave_store::Store;
    use std::fs;

    let base = tempfile::tempdir().unwrap();
    let state = base.path().join("state");
    let stage_root = state.join("managed-stage");
    fs::create_dir_all(&stage_root).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&stage_root, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let source = base.path().join("source");
    let bytes = b"private CAS validation fixture";
    fs::write(&source, bytes).unwrap();
    let store = Store::open(&state).unwrap();
    let manifest = store
        .ingest_file(&source, ChunkingProfile::DEFAULT)
        .unwrap();
    let sentinel = WirePath::new("sentinel").unwrap();
    store.metadata().put_manifest(&sentinel, &manifest).unwrap();
    let chunk = manifest.chunks.first().unwrap();
    let encoded = chunk.hash.to_hex();
    let chunk_path = state.join("chunks").join(&encoded[..2]).join(&encoded[2..]);

    fs::write(&chunk_path, b"tampered").unwrap();
    assert!(
        store
            .materialize_private_verified(
                &manifest,
                &stage_root,
                &WirePath::new("tampered.bin").unwrap()
            )
            .is_err()
    );
    assert!(!stage_root.join("tampered.bin").exists());
    fs::write(&chunk_path, bytes).unwrap();

    fs::remove_file(&chunk_path).unwrap();
    assert!(
        store
            .materialize_private_verified(
                &manifest,
                &stage_root,
                &WirePath::new("missing.bin").unwrap()
            )
            .is_err()
    );
    assert!(!stage_root.join("missing.bin").exists());
    fs::write(&chunk_path, bytes).unwrap();

    let mut bad_full_hash = manifest.clone();
    bad_full_hash.file_hash = Hash32::digest(b"different complete file");
    assert!(
        store
            .materialize_private_verified(
                &bad_full_hash,
                &stage_root,
                &WirePath::new("bad-full-hash.bin").unwrap()
            )
            .is_err()
    );
    assert!(!stage_root.join("bad-full-hash.bin").exists());

    let mut bad_length = manifest.clone();
    bad_length.chunks[0].length += 1;
    bad_length.size += 1;
    assert!(
        store
            .materialize_private_verified(
                &bad_length,
                &stage_root,
                &WirePath::new("bad-length.bin").unwrap()
            )
            .is_err()
    );
    assert!(!stage_root.join("bad-length.bin").exists());

    let occupied = WirePath::new("occupied.bin").unwrap();
    let occupied_path = stage_root.join(occupied.as_str());
    fs::write(&occupied_path, b"existing").unwrap();
    assert!(
        store
            .materialize_private_verified(&manifest, &stage_root, &occupied)
            .is_err()
    );
    assert_eq!(fs::read(&occupied_path).unwrap(), b"existing");
    assert_eq!(
        store.metadata().get_manifest(&sentinel).unwrap(),
        Some(manifest)
    );
    assert!(store.path_changes().unwrap().is_empty());
}

#[cfg(unix)]
#[test]
fn private_verified_materialization_rejects_symlink_ancestors_and_broad_root() {
    use deltaweave_core::{ChunkingProfile, WirePath};
    use deltaweave_store::Store;
    use std::os::unix::fs::PermissionsExt;
    use std::{fs, os::unix::fs::symlink};

    let base = tempfile::tempdir().unwrap();
    let state = base.path().join("state");
    let stage_root = state.join("managed-stage");
    fs::create_dir_all(&stage_root).unwrap();
    fs::set_permissions(&stage_root, fs::Permissions::from_mode(0o700)).unwrap();
    let outside = base.path().join("outside");
    fs::create_dir(&outside).unwrap();
    let source = base.path().join("source");
    fs::write(&source, b"safe bytes").unwrap();
    let store = Store::open(&state).unwrap();
    let manifest = store
        .ingest_file(&source, ChunkingProfile::DEFAULT)
        .unwrap();

    symlink(&outside, stage_root.join("alias")).unwrap();
    assert!(
        store
            .materialize_private_verified(
                &manifest,
                &stage_root,
                &WirePath::new("alias/escape.bin").unwrap()
            )
            .is_err()
    );
    assert!(!outside.join("escape.bin").exists());

    fs::remove_file(stage_root.join("alias")).unwrap();
    fs::set_permissions(&stage_root, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(
        store
            .materialize_private_verified(
                &manifest,
                &stage_root,
                &WirePath::new("broad.bin").unwrap()
            )
            .is_err()
    );
    assert!(!stage_root.join("broad.bin").exists());

    fs::set_permissions(&stage_root, fs::Permissions::from_mode(0o700)).unwrap();
    let outside_file = base.path().join("outside-file");
    fs::write(&outside_file, b"outside").unwrap();
    symlink(&outside_file, stage_root.join("leaf.bin")).unwrap();
    assert!(
        store
            .materialize_private_verified(
                &manifest,
                &stage_root,
                &WirePath::new("leaf.bin").unwrap()
            )
            .is_err()
    );
    assert_eq!(fs::read(&outside_file).unwrap(), b"outside");
}

#[cfg(unix)]
#[test]
fn private_verified_materialization_rejects_a_symlinked_cas_chunk() {
    use deltaweave_core::{ChunkingProfile, WirePath};
    use deltaweave_store::Store;
    use std::os::unix::fs::PermissionsExt;
    use std::{fs, os::unix::fs::symlink};

    let base = tempfile::tempdir().unwrap();
    let state = base.path().join("state");
    let stage_root = state.join("managed-stage");
    fs::create_dir_all(&stage_root).unwrap();
    fs::set_permissions(&stage_root, fs::Permissions::from_mode(0o700)).unwrap();
    let source = base.path().join("source");
    let bytes = b"CAS symlink fixture";
    fs::write(&source, bytes).unwrap();
    let store = Store::open(&state).unwrap();
    let manifest = store
        .ingest_file(&source, ChunkingProfile::DEFAULT)
        .unwrap();
    let descriptor = manifest.chunks.first().unwrap();
    let encoded = descriptor.hash.to_hex();
    let chunk_path = state.join("chunks").join(&encoded[..2]).join(&encoded[2..]);
    let outside = base.path().join("outside-chunk");
    fs::rename(&chunk_path, &outside).unwrap();
    symlink(&outside, &chunk_path).unwrap();

    assert!(
        store
            .materialize_private_verified(
                &manifest,
                &stage_root,
                &WirePath::new("symlink-cas.bin").unwrap()
            )
            .is_err()
    );
    assert!(!stage_root.join("symlink-cas.bin").exists());
}

#[test]
fn rollback_unadopted_preserves_incoming_and_restores_each_unadopted_state() {
    use deltaweave_core::{ChunkingProfile, WirePath};
    use deltaweave_store::{PathChangeState, PathObservation, PathTarget, Store};
    use std::fs;

    for state in [
        PathChangeState::Prepared,
        PathChangeState::Preserved,
        PathChangeState::Materialized,
    ] {
        let base = tempfile::tempdir().unwrap();
        let root = base.path().join("root");
        fs::create_dir(&root).unwrap();
        let source = base.path().join("source");
        fs::write(&source, b"incoming content").unwrap();
        fs::write(root.join("file"), b"local content").unwrap();
        let store = Store::open(base.path().join("state")).unwrap();
        let manifest = store
            .ingest_file(&source, ChunkingProfile::DEFAULT)
            .unwrap();
        let path = WirePath::new("file").unwrap();
        let expected = PathObservation::read(&root, &path).unwrap();
        let mut change = store
            .prepare_path_change(&root, &path, PathTarget::File(manifest), expected, false)
            .unwrap();
        if matches!(
            state,
            PathChangeState::Preserved | PathChangeState::Materialized
        ) {
            store.capture_path_change(&mut change).unwrap();
        }
        if state == PathChangeState::Materialized {
            store.materialize_path_change(&mut change).unwrap();
        }

        store.rollback_unadopted_path_change(&mut change).unwrap();
        assert_eq!(change.state, PathChangeState::RolledBack);
        assert_eq!(fs::read(root.join("file")).unwrap(), b"local content");
        assert_eq!(
            fs::read(&change.rollback_artifact).unwrap(),
            b"incoming content"
        );
        assert!(!change.staging.exists());
        if state == PathChangeState::Preserved || state == PathChangeState::Materialized {
            assert!(!change.artifact.exists());
        }
    }
}

#[test]
fn rollback_unadopted_rejects_drift_causal_and_indexed_changes() {
    use deltaweave_core::{ChunkingProfile, SyncEntryKind, SyncRecord, VersionVector, WirePath};
    use deltaweave_store::{CausalBinding, PathChangeState, PathObservation, PathTarget, Store};
    use std::fs;

    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    fs::create_dir(&root).unwrap();
    let source = base.path().join("source");
    fs::write(&source, b"incoming content").unwrap();
    fs::write(root.join("file"), b"local content").unwrap();
    let store = Store::open(base.path().join("state")).unwrap();
    let manifest = store
        .ingest_file(&source, ChunkingProfile::DEFAULT)
        .unwrap();
    let path = WirePath::new("file").unwrap();
    let expected = PathObservation::read(&root, &path).unwrap();
    let mut drifted = store
        .prepare_path_change(
            &root,
            &path,
            PathTarget::File(manifest.clone()),
            expected.clone(),
            false,
        )
        .unwrap();
    store.capture_path_change(&mut drifted).unwrap();
    store.materialize_path_change(&mut drifted).unwrap();
    fs::write(root.join("file"), b"local drift").unwrap();
    assert!(store.rollback_unadopted_path_change(&mut drifted).is_err());
    assert_eq!(fs::read(root.join("file")).unwrap(), b"local drift");
    assert_eq!(fs::read(&drifted.artifact).unwrap(), b"local content");
    assert!(!drifted.rollback_artifact.exists());
    assert_eq!(drifted.state, PathChangeState::Materialized);

    let mut causal = store
        .prepare_path_change(
            &root,
            &WirePath::new("causal").unwrap(),
            PathTarget::File(manifest.clone()),
            None,
            false,
        )
        .unwrap();
    causal.causal = Some(CausalBinding {
        record: SyncRecord {
            schema_version: 1,
            path: WirePath::new("causal").unwrap(),
            kind: SyncEntryKind::File,
            size: manifest.size,
            content_hash: Some(manifest.file_hash),
            readonly: false,
            version: VersionVector::default(),
            tombstone: false,
        },
        precondition: None,
        authorization: None,
    });
    assert!(store.rollback_unadopted_path_change(&mut causal).is_err());
    assert!(!root.join("causal").exists());

    let mut indexed = store
        .prepare_path_change(
            &root,
            &WirePath::new("indexed").unwrap(),
            PathTarget::File(manifest),
            None,
            false,
        )
        .unwrap();
    indexed.state = PathChangeState::Indexed;
    assert!(store.rollback_unadopted_path_change(&mut indexed).is_err());
    assert_eq!(indexed.state, PathChangeState::Indexed);
}

#[test]
fn rollback_unadopted_restores_nonfile_targets_without_incoming_stage() {
    use deltaweave_core::WirePath;
    use deltaweave_store::{PathChangeState, PathObservation, PathTarget, Store};
    use std::fs;

    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    fs::create_dir(&root).unwrap();
    let store = Store::open(base.path().join("state")).unwrap();

    let directory = root.join("directory");
    fs::create_dir(&directory).unwrap();
    let directory_path = WirePath::new("directory").unwrap();
    let directory_expected = PathObservation::read(&root, &directory_path).unwrap();
    let mut directory_change = store
        .prepare_path_change(
            &root,
            &directory_path,
            PathTarget::Directory,
            directory_expected,
            false,
        )
        .unwrap();
    store.capture_path_change(&mut directory_change).unwrap();
    store
        .rollback_unadopted_path_change(&mut directory_change)
        .unwrap();
    assert_eq!(directory_change.state, PathChangeState::RolledBack);
    assert!(directory.is_dir());
    assert!(!directory_change.rollback_artifact.exists());

    let file = root.join("file");
    fs::write(&file, b"local content").unwrap();
    let file_path = WirePath::new("file").unwrap();
    let file_expected = PathObservation::read(&root, &file_path).unwrap();
    let mut absence_change = store
        .prepare_path_change(&root, &file_path, PathTarget::Absent, file_expected, false)
        .unwrap();
    store.capture_path_change(&mut absence_change).unwrap();
    store
        .rollback_unadopted_path_change(&mut absence_change)
        .unwrap();
    assert_eq!(absence_change.state, PathChangeState::RolledBack);
    assert_eq!(fs::read(file).unwrap(), b"local content");
    assert!(!absence_change.rollback_artifact.exists());
}

#[cfg(windows)]
#[test]
fn readonly_replacement_preserves_original_bytes_and_attributes() {
    use deltaweave_core::{ChunkingProfile, WirePath};
    use deltaweave_store::Store;
    use std::fs;
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    fs::create_dir(&root).unwrap();
    let store = Store::open(base.path().join("state")).unwrap();
    let source = base.path().join("source");
    fs::write(&source, b"incoming revision").unwrap();
    let manifest = store.ingest_file(source, ChunkingProfile::DEFAULT).unwrap();
    let destination = root.join("document");
    fs::write(&destination, b"original readonly content").unwrap();
    let outside_link = base.path().join("outside-link");
    fs::hard_link(&destination, &outside_link).unwrap();
    let mut permissions = fs::metadata(&destination).unwrap().permissions();
    permissions.set_readonly(true);
    fs::set_permissions(&destination, permissions).unwrap();
    let outcome = store
        .materialize(&manifest, &WirePath::new("document").unwrap(), &root)
        .unwrap();
    let preserved = outcome.preserved_path.unwrap();
    assert_eq!(fs::read(&destination).unwrap(), b"incoming revision");
    assert_eq!(fs::read(&preserved).unwrap(), b"original readonly content");
    assert!(fs::metadata(&preserved).unwrap().permissions().readonly());
    assert_eq!(
        fs::read(&outside_link).unwrap(),
        b"original readonly content"
    );
    assert!(
        fs::metadata(&outside_link)
            .unwrap()
            .permissions()
            .readonly()
    );
}

#[cfg(windows)]
#[test]
fn recovery_retries_preservation_flush_before_installing_incoming_content() {
    use deltaweave_core::{ChunkingProfile, WirePath};
    use deltaweave_store::{PathObservation, PathTarget, Store};
    use std::{fs, os::windows::fs::OpenOptionsExt};
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    fs::create_dir(&root).unwrap();
    let store = Store::open(base.path().join("state")).unwrap();
    let source = base.path().join("source");
    fs::write(&source, b"incoming").unwrap();
    let manifest = store.ingest_file(source, ChunkingProfile::DEFAULT).unwrap();
    let path = WirePath::new("document").unwrap();
    fs::write(root.join("document"), b"original").unwrap();
    let mut change = store
        .prepare_path_change(
            &root,
            &path,
            PathTarget::File(manifest),
            PathObservation::read(&root, &path).unwrap(),
            false,
        )
        .unwrap();
    // Permit observation and rename, but prevent a write handle for FlushFileBuffers.
    let held = fs::OpenOptions::new()
        .read(true)
        .share_mode(0x00000001 | 0x00000004)
        .open(root.join("document"))
        .unwrap();
    assert!(store.capture_path_change(&mut change).is_err());
    assert!(store.resume_path_change(&mut change).is_err());
    assert!(!root.join("document").exists());
    assert_eq!(fs::read(&change.artifact).unwrap(), b"original");
    drop(held);
    store.resume_path_change(&mut change).unwrap();
    assert_eq!(fs::read(root.join("document")).unwrap(), b"incoming");
    assert_eq!(fs::read(&change.artifact).unwrap(), b"original");
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
