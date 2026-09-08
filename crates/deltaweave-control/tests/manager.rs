use deltaweave_control::{FolderCommand, FolderInput, Manager, Settings};
use std::{path::Path, time::Duration};
fn receive(base: &Path, name: &str) -> FolderInput {
    FolderInput {
        name: name.into(),
        root: base.join(name).display().to_string(),
        role: "receive".into(),
        enabled: Some(false),
        bind: Some("127.0.0.1:0".into()),
        min_free_space_mib: Some(0),
        ..Default::default()
    }
}

fn regular_files_below(path: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| {
            if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                regular_files_below(&entry.path())
            } else {
                usize::from(entry.file_type().is_ok_and(|kind| kind.is_file()))
            }
        })
        .sum()
}
#[tokio::test]
async fn persistence_identity_ownership_and_preserved_files() {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().join("admin");
    let manager = Manager::open(data.clone()).await.unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&data).unwrap().permissions().mode() & 0o077,
            0,
            "management data must be owner-only"
        );
    }
    assert!(Manager::open(data.clone()).await.is_err());
    let key_path = temp.path().join("import.key");
    let identity = deltaweave_net::load_or_create_identity(&key_path).unwrap();
    let original = std::fs::read(&key_path).unwrap();
    let mut input = receive(temp.path(), "files");
    input.identity_path = Some(key_path.display().to_string());
    let folder = manager.add_folder(input.clone()).await.unwrap();
    assert_eq!(folder.endpoint_id, identity.endpoint_id().to_string());
    assert_eq!(std::fs::read(&key_path).unwrap(), original);
    std::fs::write(Path::new(&folder.input.root).join("keep.txt"), b"keep me").unwrap();
    let mut overlap = receive(temp.path(), "other");
    overlap.state_path = folder.input.state_path.clone();
    assert!(manager.add_folder(overlap).await.is_err());
    let mut invalid = input.clone();
    invalid.name = "".into();
    assert!(manager.update_folder(&folder.id, invalid).await.is_err());
    manager
        .update_settings(Settings {
            node_name: "My NAS".into(),
            poll_interval_seconds: 42,
            history_limit: 20,
        })
        .await
        .unwrap();
    manager.shutdown().await.unwrap();
    let reopened = Manager::open(data).await.unwrap();
    let snapshot = reopened.snapshot().await;
    assert_eq!(snapshot.settings.node_name, "My NAS");
    assert_eq!(snapshot.folders[0].status, "paused");
    reopened.remove_folder(&folder.id).await.unwrap();
    assert_eq!(
        std::fs::read(Path::new(&folder.input.root).join("keep.txt")).unwrap(),
        b"keep me"
    );
    assert_eq!(std::fs::read(key_path).unwrap(), original);
    reopened.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_pair_sync_idle_pause_and_reopen() {
    async fn manual_sync(manager: &Manager, id: &str) {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                match manager.command(id, FolderCommand::Sync).await {
                    Ok(()) => break,
                    Err(error)
                        if error.to_string()
                            == "folder is busy; retry after its current command" =>
                    {
                        // Watcher events can start an automatic cycle between manual requests.
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Err(error) => panic!("manual sync failed: {error:#}"),
                }
            }
        })
        .await
        .expect("manual sync must complete after any active watcher cycle");
    }

    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().join("admin");
    let manager = Manager::open(data.clone()).await.unwrap();
    let sync_identity = temp.path().join("sync.key");
    let identity = deltaweave_net::load_or_create_identity(&sync_identity).unwrap();
    let mut receiver = receive(temp.path(), "remote");
    receiver.enabled = Some(true);
    receiver.allowed_peers = vec![identity.endpoint_id().to_string()];
    let remote = manager.add_folder(receiver).await.unwrap();
    assert!(!remote.addresses.is_empty());
    let local = manager
        .add_folder(FolderInput {
            name: "local".into(),
            root: temp.path().join("local").display().to_string(),
            role: "sync".into(),
            identity_path: Some(sync_identity.display().to_string()),
            peer_endpoint_id: Some(remote.endpoint_id.clone()),
            direct_addresses: remote.addresses.clone(),
            interval_seconds: Some(3600),
            ..Default::default()
        })
        .await
        .unwrap();
    std::fs::write(
        Path::new(&local.input.root).join("hello.txt"),
        b"hello from web",
    )
    .unwrap();
    manual_sync(&manager, &local.id).await;
    assert_eq!(
        std::fs::read(Path::new(&remote.input.root).join("hello.txt")).unwrap(),
        b"hello from web"
    );
    manual_sync(&manager, &local.id).await;
    let view = manager
        .snapshot()
        .await
        .folders
        .into_iter()
        .find(|f| f.id == local.id)
        .unwrap();
    let report = view.last_report.unwrap();
    for key in [
        "pushed_bytes",
        "pulled_bytes",
        "local_actions",
        "remote_actions",
    ] {
        assert_eq!(report[key], 0, "{key}");
    }
    std::fs::write(
        Path::new(&remote.input.root).join("reply.txt"),
        b"hello from receiver",
    )
    .unwrap();
    manual_sync(&manager, &local.id).await;
    assert_eq!(
        std::fs::read(Path::new(&local.input.root).join("reply.txt")).unwrap(),
        b"hello from receiver"
    );
    let activities = manager.snapshot().await.activities;
    let received = activities
        .iter()
        .find(|activity| {
            activity.folder_id.as_deref() == Some(remote.id.as_str())
                && activity.kind == "file_received"
                && activity.path.as_deref() == Some("hello.txt")
        })
        .expect("real receive activity");
    assert_eq!(received.pulled_bytes, b"hello from web".len() as u64);
    assert_eq!(received.pushed_bytes, 0);
    let sent = activities
        .iter()
        .find(|activity| {
            activity.folder_id.as_deref() == Some(remote.id.as_str())
                && activity.kind == "file_sent"
                && activity.path.as_deref() == Some("reply.txt")
        })
        .expect("real send activity");
    assert_eq!(sent.pushed_bytes, b"hello from receiver".len() as u64);
    assert_eq!(sent.pulled_bytes, 0);
    manager
        .update_settings(Settings {
            history_limit: 3,
            ..Settings::default()
        })
        .await
        .unwrap();
    std::fs::write(
        Path::new(&local.input.root).join("watched.txt"),
        b"watcher payload",
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if std::fs::read(Path::new(&remote.input.root).join("watched.txt"))
                .ok()
                .as_deref()
                == Some(b"watcher payload")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("watcher must trigger without manual sync");
    manager
        .command(&local.id, FolderCommand::Pause)
        .await
        .unwrap();
    assert!(
        manager
            .command(&local.id, FolderCommand::Sync)
            .await
            .is_err()
    );
    manager
        .command(&remote.id, FolderCommand::Pause)
        .await
        .unwrap();
    manager
        .command(&remote.id, FolderCommand::Resume)
        .await
        .unwrap();
    std::fs::write(
        Path::new(&local.input.root).join("paused.txt"),
        b"changed while paused",
    )
    .unwrap();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(!Path::new(&remote.input.root).join("paused.txt").exists());
    manager
        .command(&local.id, FolderCommand::Resume)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if Path::new(&remote.input.root).join("paused.txt").exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("resume must sync pending changes immediately");
    manager
        .command(&local.id, FolderCommand::Pause)
        .await
        .unwrap();
    let snapshot = manager.snapshot().await;
    assert!(snapshot.history.len() <= 3);
    assert!(!snapshot.history.is_empty());
    let saved_report = snapshot
        .folders
        .iter()
        .find(|f| f.id == local.id)
        .unwrap()
        .last_report
        .clone();
    let saved_sync_at = snapshot
        .folders
        .iter()
        .find(|f| f.id == local.id)
        .unwrap()
        .last_sync_at;
    manager.shutdown().await.unwrap();
    let manager = Manager::open(data).await.unwrap();
    let snapshot = manager.snapshot().await;
    assert_eq!(
        snapshot
            .folders
            .iter()
            .find(|f| f.id == local.id)
            .unwrap()
            .status,
        "paused"
    );
    assert!(!snapshot.history.is_empty());
    assert!(snapshot.history.len() <= 3);
    assert!(!snapshot.activities.is_empty());
    let reopened = snapshot.folders.iter().find(|f| f.id == local.id).unwrap();
    assert_eq!(
        reopened.last_report, saved_report,
        "verified report must survive restart"
    );
    assert_eq!(reopened.last_sync_at, saved_sync_at);
    std::fs::write(
        Path::new(&local.input.root).join("after-restart.txt"),
        b"stable port after restart",
    )
    .unwrap();
    manager
        .command(&local.id, FolderCommand::Resume)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if std::fs::read(Path::new(&remote.input.root).join("after-restart.txt"))
                .ok()
                .as_deref()
                == Some(b"stable port after restart")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("restarted sender must reach receiver at saved port");
    manager.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_sync_impossible_reserve_rejects_before_file_or_cas_write() {
    const MIB: u64 = 1024 * 1024;
    let temp = tempfile::tempdir().unwrap();
    let remote_root = temp.path().join("remote");
    std::fs::create_dir(&remote_root).unwrap();
    std::fs::write(remote_root.join("only-remote.txt"), b"must not be pulled").unwrap();

    let manager = Manager::open(temp.path().join("admin")).await.unwrap();
    let sync_identity = temp.path().join("sync.key");
    let identity = deltaweave_net::load_or_create_identity(&sync_identity).unwrap();
    let mut receiver = receive(temp.path(), "remote");
    receiver.enabled = Some(true);
    receiver.allowed_peers = vec![identity.endpoint_id().to_string()];
    let remote = manager.add_folder(receiver).await.unwrap();
    let impossible_reserve = fs2::available_space(temp.path()).unwrap() / MIB + 2;
    let local = manager
        .add_folder(FolderInput {
            name: "limited local".into(),
            root: temp.path().join("local").display().to_string(),
            role: "sync".into(),
            identity_path: Some(sync_identity.display().to_string()),
            peer_endpoint_id: Some(remote.endpoint_id),
            direct_addresses: remote.addresses,
            interval_seconds: Some(3600),
            min_free_space_mib: Some(impossible_reserve),
            ..Default::default()
        })
        .await
        .unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        manager.command(&local.id, FolderCommand::Sync),
    )
    .await
    .unwrap();
    assert!(
        result.is_err(),
        "an impossible reserve must reject the pull"
    );
    assert!(
        !Path::new(&local.input.root)
            .join("only-remote.txt")
            .exists()
    );
    let chunks = Path::new(local.input.state_path.as_deref().unwrap())
        .join("store")
        .join("chunks");
    assert_eq!(regular_files_below(&chunks), 0, "CAS must remain empty");
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn root_owner_and_failed_update_preserve_previous_worker() {
    let temp = tempfile::tempdir().unwrap();
    let manager = Manager::open(temp.path().join("admin")).await.unwrap();
    let second = Manager::open(temp.path().join("admin2")).await.unwrap();
    let original = manager
        .add_folder(receive(temp.path(), "files"))
        .await
        .unwrap();
    assert!(
        second
            .add_folder(receive(temp.path(), "files"))
            .await
            .is_err()
    );
    let mut changed = original.input.clone();
    changed.root = temp.path().join("replacement").display().to_string();
    assert!(manager.update_folder(&original.id, changed).await.is_err());
    let current = manager.snapshot().await.folders.remove(0);
    assert_eq!(current.input.root, original.input.root);
    assert_eq!(current.status, "paused");
    manager
        .command(&original.id, FolderCommand::Resume)
        .await
        .unwrap();
    manager.shutdown().await.unwrap();
    let imported = second
        .add_folder(receive(temp.path(), "files"))
        .await
        .unwrap();
    assert_eq!(imported.status, "paused");
    second.shutdown().await.unwrap();
}

#[tokio::test]
async fn copied_identity_cannot_serve_multiple_roots() {
    let temp = tempfile::tempdir().unwrap();
    let manager = Manager::open(temp.path().join("admin")).await.unwrap();
    let first = manager
        .add_folder(receive(temp.path(), "first"))
        .await
        .unwrap();
    let copied = temp.path().join("copied.key");
    std::fs::copy(first.input.identity_path.as_ref().unwrap(), &copied).unwrap();
    let mut duplicate = receive(temp.path(), "second");
    duplicate.identity_path = Some(copied.display().to_string());
    assert!(
        manager.add_folder(duplicate).await.is_err(),
        "distinct key paths must not bypass endpoint identity uniqueness"
    );
    let second = manager
        .add_folder(receive(temp.path(), "third"))
        .await
        .unwrap();
    let mut input = second.input.clone();
    input.identity_path = Some(copied.display().to_string());
    assert!(manager.update_folder(&second.id, input).await.is_err());
    assert_eq!(manager.snapshot().await.folders.len(), 2);
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejects_private_control_paths_and_wrong_state_role() {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().join("admin");
    let manager = Manager::open(data.clone()).await.unwrap();
    let mut invalid = receive(temp.path(), "files");
    invalid.state_path = Some(data.display().to_string());
    assert!(
        manager.add_folder(invalid).await.is_err(),
        "must not open engine state inside control directory"
    );
    let state = temp.path().join("old-sync-state");
    std::fs::create_dir_all(state.join("store")).unwrap();
    std::fs::write(
        state.join("store/metadata.redb"),
        b"existing sync state marker",
    )
    .unwrap();
    let mut wrong_role = receive(temp.path(), "files");
    wrong_role.state_path = Some(state.display().to_string());
    assert!(
        manager.add_folder(wrong_role).await.is_err(),
        "receive must not silently reuse sync state"
    );
    manager.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_waits_for_concurrent_adds_and_releases_ownership() {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().join("admin");
    let manager = Manager::open(data.clone()).await.unwrap();
    let mut additions = Vec::new();
    for index in 0..8 {
        let manager = manager.clone();
        let input = receive(temp.path(), &format!("files-{index}"));
        additions.push(tokio::spawn(async move { manager.add_folder(input).await }));
    }
    tokio::task::yield_now().await;
    manager.shutdown().await.unwrap();
    for task in additions {
        let _ = task.await.unwrap();
    }
    assert!(
        manager
            .snapshot()
            .await
            .folders
            .iter()
            .all(|folder| folder.status == "stopped")
    );
    assert!(
        manager
            .add_folder(receive(temp.path(), "after-shutdown"))
            .await
            .is_err()
    );
    let reopened = Manager::open(data).await.unwrap();
    assert!(
        reopened
            .snapshot()
            .await
            .folders
            .iter()
            .all(|folder| folder.status == "paused")
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_persistence_keeps_saved_settings_and_running_folder() {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().join("admin");
    let manager = Manager::open(data.clone()).await.unwrap();
    let folder = manager
        .add_folder(receive(temp.path(), "files"))
        .await
        .unwrap();
    let original = std::fs::read(data.join("config.json")).unwrap();
    std::fs::remove_file(data.join("config.json")).unwrap();
    std::fs::create_dir(data.join("config.json")).unwrap();
    assert!(
        manager
            .update_settings(Settings {
                node_name: "Unsaved".into(),
                ..Settings::default()
            })
            .await
            .is_err()
    );
    let mut changed = folder.input.clone();
    changed.name = "Unsaved folder".into();
    assert!(manager.update_folder(&folder.id, changed).await.is_err());
    let snapshot = manager.snapshot().await;
    assert_eq!(snapshot.settings.node_name, "DeltaWeave");
    assert_eq!(snapshot.folders[0].input.name, "files");
    assert_eq!(snapshot.folders[0].status, "paused");
    std::fs::remove_dir(data.join("config.json")).unwrap();
    std::fs::write(data.join("config.json"), original).unwrap();
    manager
        .command(&folder.id, FolderCommand::Resume)
        .await
        .unwrap();
    manager.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejected_receiver_sets_real_retry_then_recovers() {
    let temp = tempfile::tempdir().unwrap();
    let manager = Manager::open(temp.path().join("admin")).await.unwrap();
    let identity_path = temp.path().join("sender.key");
    let identity = deltaweave_net::load_or_create_identity(&identity_path).unwrap();
    let mut receiver = receive(temp.path(), "receiver");
    receiver.allowed_peers = vec![identity.endpoint_id().to_string()];
    let remote = manager.add_folder(receiver).await.unwrap();
    manager
        .add_device(deltaweave_control::DeviceInput {
            name: "Paused receiver".into(),
            endpoint_id: remote.endpoint_id.clone(),
            address: remote.addresses[0].clone(),
        })
        .await
        .unwrap();
    let local = manager
        .add_folder(FolderInput {
            name: "sender".into(),
            root: temp.path().join("sender").display().to_string(),
            role: "sync".into(),
            identity_path: Some(identity_path.display().to_string()),
            peer_endpoint_id: Some(remote.endpoint_id),
            direct_addresses: remote.addresses,
            interval_seconds: Some(3600),
            ..Default::default()
        })
        .await
        .unwrap();
    std::fs::write(
        Path::new(&local.input.root).join("retry.txt"),
        b"retry after receiver resumes",
    )
    .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        manager.command(&local.id, FolderCommand::Sync),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    let snapshot = manager.snapshot().await;
    let failed = snapshot
        .folders
        .iter()
        .find(|folder| folder.id == local.id)
        .unwrap();
    assert_eq!(failed.status, "error");
    assert!(failed.last_error.is_some());
    assert!(failed.retry_at.unwrap() >= snapshot.node.started_at + 1000);
    assert!(snapshot.node.started_at > 1_000_000_000_000);
    assert!(snapshot.node.uptime_seconds < 30);
    assert!(
        snapshot.devices[0].last_seen_at.is_none(),
        "local scanning and failed requests must not claim a successful peer observation"
    );
    manager
        .command(&remote.id, FolderCommand::Resume)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let snapshot = manager.snapshot().await;
            let local = snapshot
                .folders
                .iter()
                .find(|folder| folder.id == local.id)
                .unwrap();
            if local.last_sync_at.is_some() && local.status == "idle" {
                assert!(snapshot.devices[0].last_seen_at.is_some());
                assert!(local.retry_at.is_none());
                assert!(local.last_error.is_none());
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("automatic retry should recover after receiver resumes");
    assert_eq!(
        std::fs::read(Path::new(&remote.input.root).join("retry.txt")).unwrap(),
        b"retry after receiver resumes"
    );
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn receiver_allocated_bind_survives_rename_and_reopen() {
    for bind in [None, Some("127.0.0.1:0".to_string())] {
        let temp = tempfile::tempdir().unwrap();
        let data = temp.path().join("admin");
        let manager = Manager::open(data.clone()).await.unwrap();
        let mut input = receive(temp.path(), "files");
        input.bind = bind.clone();
        let first = manager.add_folder(input).await.unwrap();
        let allocated: std::net::SocketAddr = first
            .input
            .bind
            .as_deref()
            .expect("allocated bind must be returned")
            .parse()
            .unwrap();
        assert_ne!(allocated.port(), 0);
        if bind.is_some() {
            assert_eq!(allocated.ip().to_string(), "127.0.0.1");
        }
        let mut renamed = first.input.clone();
        renamed.name = "renamed".into();
        let updated = manager.update_folder(&first.id, renamed).await.unwrap();
        assert_eq!(updated.input.bind, first.input.bind);
        assert!(updated.addresses.iter().all(|address| {
            address.parse::<std::net::SocketAddr>().unwrap().port() == allocated.port()
        }));
        manager.shutdown().await.unwrap();
        let reopened = Manager::open(data).await.unwrap();
        assert_eq!(
            reopened.snapshot().await.folders[0].input.bind,
            first.input.bind
        );
        reopened.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conflicts_remain_discoverable_after_idle_and_restart() {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().join("admin");
    let manager = Manager::open(data.clone()).await.unwrap();
    let identity_path = temp.path().join("sender.key");
    let identity = deltaweave_net::load_or_create_identity(&identity_path).unwrap();
    let mut receiver = receive(temp.path(), "receiver");
    receiver.enabled = Some(true);
    receiver.allowed_peers = vec![identity.endpoint_id().to_string()];
    let remote = manager.add_folder(receiver).await.unwrap();
    let local = manager
        .add_folder(FolderInput {
            name: "sender".into(),
            root: temp.path().join("sender").display().to_string(),
            role: "sync".into(),
            identity_path: Some(identity_path.display().to_string()),
            peer_endpoint_id: Some(remote.endpoint_id),
            direct_addresses: remote.addresses,
            interval_seconds: Some(3600),
            ..Default::default()
        })
        .await
        .unwrap();
    std::fs::write(
        Path::new(&local.input.root).join("shared.txt"),
        b"initial shared bytes",
    )
    .unwrap();
    manager
        .command(&local.id, FolderCommand::Sync)
        .await
        .unwrap();
    std::fs::write(
        Path::new(&local.input.root).join("shared.txt"),
        b"edited on sender",
    )
    .unwrap();
    std::fs::write(
        Path::new(&remote.input.root).join("shared.txt"),
        b"edited on receiver",
    )
    .unwrap();
    manager
        .command(&local.id, FolderCommand::Sync)
        .await
        .unwrap();
    let snapshot = manager.snapshot().await;
    let activity = snapshot
        .activities
        .iter()
        .find(|activity| activity.kind.contains("conflict"))
        .expect("conflict must have a durable activity")
        .clone();
    let detail: serde_json::Value = serde_json::from_str(&activity.detail).unwrap();
    for key in [
        "path",
        "conflict_path",
        "winner_hash",
        "loser_hash",
        "reason",
    ] {
        assert!(!detail[key].is_null(), "missing {key}");
    }
    assert_eq!(detail["path"], "shared.txt");
    let copy = detail["conflict_path"].as_str().unwrap();
    assert!(Path::new(&local.input.root).join(copy).exists());
    assert!(Path::new(&remote.input.root).join(copy).exists());
    manager
        .command(&local.id, FolderCommand::Sync)
        .await
        .unwrap();
    manager
        .command(&local.id, FolderCommand::Pause)
        .await
        .unwrap();
    manager.shutdown().await.unwrap();
    let reopened = Manager::open(data).await.unwrap();
    let snapshot = reopened.snapshot().await;
    assert!(
        snapshot
            .activities
            .iter()
            .any(|saved| saved.id == activity.id && saved.detail == activity.detail)
    );
    let report = snapshot
        .folders
        .iter()
        .find(|folder| folder.id == local.id)
        .unwrap()
        .last_report
        .as_ref()
        .unwrap();
    assert_eq!(report["conflicts"].as_array().unwrap().len(), 0);
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removing_receiver_drains_admitted_v1_stream() {
    let temp = tempfile::tempdir().unwrap();
    let manager = Manager::open(temp.path().join("admin")).await.unwrap();
    let identity = deltaweave_net::load_or_create_identity(temp.path().join("sender.key")).unwrap();
    let device = manager
        .add_device(deltaweave_control::DeviceInput {
            name: "admitted sender".into(),
            endpoint_id: identity.endpoint_id().to_string(),
            address: "127.0.0.1:1".into(),
        })
        .await
        .unwrap();
    let mut input = receive(temp.path(), "receiver");
    input.enabled = Some(true);
    input.allowed_peers = vec![identity.endpoint_id().to_string()];
    let folder = manager.add_folder(input).await.unwrap();
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .secret_key(identity.secret_key)
        .bind()
        .await
        .unwrap();
    let remote = deltaweave_net::endpoint_addr(
        &folder.endpoint_id,
        &folder
            .addresses
            .iter()
            .map(|address| address.parse().unwrap())
            .collect::<Vec<_>>(),
        &[],
    )
    .unwrap();
    let connection = endpoint
        .connect(remote, deltaweave_net::ALPN_V1)
        .await
        .unwrap();
    let (mut send, _receive) = connection.open_bi().await.unwrap();
    send.write_all(&[0, 0]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if manager
                .snapshot()
                .await
                .devices
                .iter()
                .find(|entry| entry.id == device.id)
                .unwrap()
                .last_seen_at
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("peer_seen confirms real admitted stream");
    let removing = manager.remove_folder(&folder.id);
    tokio::pin!(removing);
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut removing)
            .await
            .is_err(),
        "removal must wait for admitted work"
    );
    connection.close(0u32.into(), b"release admitted stream");
    tokio::time::timeout(Duration::from_secs(5), &mut removing)
        .await
        .unwrap()
        .unwrap();
    endpoint.close().await;
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn startup_rejects_copied_identity_before_starting_workers() {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().join("admin");
    let manager = Manager::open(data.clone()).await.unwrap();
    let first = manager
        .add_folder(receive(temp.path(), "first"))
        .await
        .unwrap();
    let second = manager
        .add_folder(receive(temp.path(), "second"))
        .await
        .unwrap();
    manager.shutdown().await.unwrap();
    std::fs::copy(
        first.input.identity_path.unwrap(),
        second.input.identity_path.unwrap(),
    )
    .unwrap();
    assert!(
        Manager::open(data).await.is_err(),
        "copied keys must not bypass startup endpoint uniqueness"
    );
}
