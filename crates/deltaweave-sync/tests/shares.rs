use deltaweave_core::ChunkingProfile;
use deltaweave_net::{NetworkMode, share::*};
use deltaweave_sync::{ManagedSyncConfig, ManagedSyncEngine};
use std::fs;

fn isolated(name: &str, body: impl FnOnce()) {
    if std::env::var("DW_MANAGED_SYNC_TEST").ok().as_deref() == Some(name) {
        body();
        return;
    }
    let home = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture"])
        .env("DW_MANAGED_SYNC_TEST", name)
        .env("HOME", home.path())
        .env("USERPROFILE", home.path())
        .status()
        .unwrap();
    assert!(status.success());
}

fn config(base: &std::path::Path, name: &str) -> ManagedSyncConfig {
    ManagedSyncConfig {
        root: base.join(format!("{name}-root")),
        state_root: base.join(format!("{name}-state")),
        profile: ChunkingProfile::DEFAULT,
        min_free_space_bytes: 0,
    }
}

#[test]
fn independent_owner_rw_ro_roundtrip_preserves_local_work_and_restart() {
    isolated(
        "independent_owner_rw_ro_roundtrip_preserves_local_work_and_restart",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let temp = tempfile::tempdir().unwrap();
                let base = temp.path();
                let owner =
                    ShareService::open(base.join("owner-device"), NetworkMode::DirectOnly, None)
                        .await
                        .unwrap();
                let rw = ShareService::open(base.join("rw-device"), NetworkMode::DirectOnly, None)
                    .await
                    .unwrap();
                let ro = ShareService::open(base.join("ro-device"), NetworkMode::DirectOnly, None)
                    .await
                    .unwrap();
                assert_ne!(owner.endpoint_id(), rw.endpoint_id());
                assert_ne!(rw.endpoint_id(), ro.endpoint_id());
                let owned = owner
                    .create_owned_share(
                        "Test".into(),
                        base.join("owner-root"),
                        base.join("owner-state"),
                        None,
                        0,
                    )
                    .await
                    .unwrap();
                let share = owned.config().share_id;
                fs::write(base.join("owner-root/file"), b"owner initial").unwrap();
                rw.enroll(
                    &owned
                        .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
                        .unwrap(),
                    None,
                )
                .await
                .unwrap();
                ro.enroll(
                    &owned
                        .issue_key(Permission::ReadOnly, None, owner.endpoint_addr())
                        .unwrap(),
                    None,
                )
                .await
                .unwrap();
                let writer =
                    ManagedSyncEngine::open(&rw, owner.endpoint_id(), share, config(base, "rw"))
                        .unwrap();
                let reader =
                    ManagedSyncEngine::open(&ro, owner.endpoint_id(), share, config(base, "ro"))
                        .unwrap();
                assert_eq!(writer.sync_read_write(None).await.unwrap().status, "pass");
                reader.sync_read_only(None).await.unwrap();
                fs::write(base.join("rw-root/from-rw"), b"writer addition").unwrap();
                writer.sync_read_write(None).await.unwrap();
                assert_eq!(
                    fs::read(base.join("owner-root/from-rw")).unwrap(),
                    b"writer addition"
                );
                fs::write(base.join("ro-root/file"), b"exact local work").unwrap();
                fs::write(base.join("ro-root/private-only"), b"must never upload").unwrap();
                fs::write(base.join("owner-root/file"), b"owner changed").unwrap();
                let report = reader.sync_read_only(None).await.unwrap();
                assert_eq!(
                    fs::read(base.join("ro-root/file")).unwrap(),
                    b"owner changed"
                );
                assert!(!base.join("ro-root/private-only").exists());
                assert!(!base.join("owner-root/private-only").exists());
                for (path, bytes) in [
                    ("file", b"exact local work".as_slice()),
                    ("private-only", b"must never upload".as_slice()),
                ] {
                    let artifact = report
                        .preserved
                        .iter()
                        .find(|p| p.path.as_str() == path)
                        .unwrap();
                    assert!(!artifact.preserved_path.starts_with(base.join("ro-root")));
                    assert_eq!(fs::read(&artifact.preserved_path).unwrap(), bytes);
                }
                fs::remove_file(base.join("ro-root/file")).unwrap();
                reader.sync_read_only(None).await.unwrap();
                assert_eq!(
                    fs::read(base.join("ro-root/file")).unwrap(),
                    b"owner changed"
                );
                fs::write(base.join("ro-root/file"), b"work before owner delete").unwrap();
                fs::remove_file(base.join("owner-root/file")).unwrap();
                let report = reader.sync_read_only(None).await.unwrap();
                assert!(!base.join("ro-root/file").exists());
                assert!(
                    report
                        .preserved
                        .iter()
                        .any(|p| fs::read(&p.preserved_path).ok().as_deref()
                            == Some(b"work before owner delete"))
                );
                writer.sync_read_write(None).await.unwrap();
                assert!(!base.join("rw-root/private-only").exists());
                assert!(!base.join("rw-root/file").exists());
                reader.shutdown().await.unwrap();
                ro.shutdown().await.unwrap();
                let ro = ShareService::open(base.join("ro-device"), NetworkMode::DirectOnly, None)
                    .await
                    .unwrap();
                let reader =
                    ManagedSyncEngine::open(&ro, owner.endpoint_id(), share, config(base, "ro"))
                        .unwrap();
                reader.sync_read_only(None).await.unwrap();
                assert!(!base.join("owner-root/private-only").exists());
                reader.shutdown().await.unwrap();
                writer.shutdown().await.unwrap();
                ro.shutdown().await.unwrap();
                rw.shutdown().await.unwrap();
                owner.shutdown().await.unwrap();
            });
        },
    );
}

#[test]
fn read_only_type_transitions_preserve_local_directory_trees() {
    isolated(
        "read_only_type_transitions_preserve_local_directory_trees",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let temp = tempfile::tempdir().unwrap();
                let base = temp.path();
                let owner =
                    ShareService::open(base.join("owner-device"), NetworkMode::DirectOnly, None)
                        .await
                        .unwrap();
                let member =
                    ShareService::open(base.join("ro-device"), NetworkMode::DirectOnly, None)
                        .await
                        .unwrap();
                let share = owner
                    .create_owned_share(
                        "Types".into(),
                        base.join("owner-root"),
                        base.join("owner-state"),
                        None,
                        0,
                    )
                    .await
                    .unwrap();
                fs::create_dir(base.join("owner-root/item")).unwrap();
                fs::write(base.join("owner-root/item/child"), b"owner child").unwrap();
                let grant = member
                    .enroll(
                        &share
                            .issue_key(Permission::ReadOnly, None, owner.endpoint_addr())
                            .unwrap(),
                        None,
                    )
                    .await
                    .unwrap();
                let engine = ManagedSyncEngine::open(
                    &member,
                    grant.owner,
                    grant.share_id,
                    config(base, "ro"),
                )
                .unwrap();
                engine.sync_read_only(None).await.unwrap();
                fs::write(base.join("ro-root/item/child"), b"local changed child").unwrap();
                fs::write(base.join("ro-root/item/new"), b"local unknown child").unwrap();
                fs::remove_file(base.join("owner-root/item/child")).unwrap();
                fs::remove_dir(base.join("owner-root/item")).unwrap();
                fs::write(base.join("owner-root/item"), b"owner file").unwrap();
                let report = engine.sync_read_only(None).await.unwrap();
                let artifact = report
                    .preserved
                    .iter()
                    .find(|c| c.path.as_str() == "item")
                    .unwrap();
                assert_eq!(
                    fs::read(artifact.preserved_path.join("child")).unwrap(),
                    b"local changed child"
                );
                assert_eq!(
                    fs::read(artifact.preserved_path.join("new")).unwrap(),
                    b"local unknown child"
                );
                assert_eq!(fs::read(base.join("ro-root/item")).unwrap(), b"owner file");
                fs::write(base.join("ro-root/item"), b"edited local file").unwrap();
                fs::remove_file(base.join("owner-root/item")).unwrap();
                fs::create_dir(base.join("owner-root/item")).unwrap();
                fs::write(base.join("owner-root/item/new-child"), b"new owner child").unwrap();
                let report = engine.sync_read_only(None).await.unwrap();
                assert!(
                    report
                        .preserved
                        .iter()
                        .any(|c| fs::read(&c.preserved_path).ok().as_deref()
                            == Some(b"edited local file"))
                );
                assert_eq!(
                    fs::read(base.join("ro-root/item/new-child")).unwrap(),
                    b"new owner child"
                );
                engine.shutdown().await.unwrap();
                member.shutdown().await.unwrap();
                owner.shutdown().await.unwrap();
            });
        },
    );
}

#[test]
fn ro_interruption_child() {
    let Some(base) = std::env::var_os("DW_INTERRUPT_BASE") else {
        return;
    };
    let base = std::path::PathBuf::from(base);
    let state = std::path::PathBuf::from(std::env::var_os("DW_INTERRUPT_STATE").unwrap());
    let phase = std::env::var("DW_INTERRUPT_PHASE").unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let owner = ShareService::open(base.join("owner-device"), NetworkMode::DirectOnly, None)
            .await
            .unwrap();
        let ro = ShareService::open(base.join("ro-device"), NetworkMode::DirectOnly, None)
            .await
            .unwrap();
        let share = owner
            .create_owned_share(
                "Crash".into(),
                base.join("owner-root"),
                base.join("owner-state"),
                None,
                0,
            )
            .await
            .unwrap();
        fs::write(base.join("owner-root/file"), b"initial owner").unwrap();
        let grant = ro
            .enroll(
                &share
                    .issue_key(Permission::ReadOnly, None, owner.endpoint_addr())
                    .unwrap(),
                None,
            )
            .await
            .unwrap();
        let mut cfg = config(&base, "ro");
        cfg.state_root = state;
        let engine = ManagedSyncEngine::open(&ro, grant.owner, grant.share_id, cfg).unwrap();
        engine.sync_read_only(None).await.unwrap();
        fs::write(base.join("ro-root/file"), b"exact interrupted local work").unwrap();
        fs::write(base.join("owner-root/file"), b"updated owner").unwrap();
        let observer = deltaweave_net::TransferObserver::new(move |event| {
            if event.phase == phase {
                std::process::exit(73);
            }
        });
        engine.sync_read_only(Some(observer)).await.unwrap();
        panic!("interruption barrier did not execute");
    });
}

#[cfg(target_os = "linux")]
#[test]
fn real_process_restart_recovers_every_ro_journal_stage_on_two_filesystems() {
    isolated(
        "real_process_restart_recovers_every_ro_journal_stage_on_two_filesystems",
        || {
            let mut fixtures = Vec::new();
            for phase in [
                "ro_prepared",
                "ro_path_prepared",
                "ro_preserved",
                "ro_materialized",
                "ro_adopted",
            ] {
                let base = tempfile::tempdir_in("/tmp").unwrap();
                let state = tempfile::tempdir_in("/dev/shm").unwrap();
                fixtures.push((base, state));
                let (base, state) = fixtures.last().unwrap();
                let status = std::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", "ro_interruption_child", "--nocapture"])
                    .env("DW_INTERRUPT_BASE", base.path())
                    .env("DW_INTERRUPT_STATE", state.path())
                    .env("DW_INTERRUPT_PHASE", phase)
                    .status()
                    .unwrap();
                assert_eq!(status.code(), Some(73), "child did not stop at {phase}");
                tokio::runtime::Runtime::new().unwrap().block_on(async {
                    let owner = ShareService::open(
                        base.path().join("owner-device"),
                        NetworkMode::DirectOnly,
                        None,
                    )
                    .await
                    .unwrap();
                    let share = owner
                        .load_owned_share(owner.owned_configs().unwrap()[0].share_id)
                        .await
                        .unwrap();
                    let ro = ShareService::open(
                        base.path().join("ro-device"),
                        NetworkMode::DirectOnly,
                        None,
                    )
                    .await
                    .unwrap();
                    let grant = ro
                        .enroll(
                            &share
                                .issue_key(Permission::ReadOnly, None, owner.endpoint_addr())
                                .unwrap(),
                            None,
                        )
                        .await
                        .unwrap();
                    let mut cfg = config(base.path(), "ro");
                    cfg.state_root = state.path().to_path_buf();
                    let engine =
                        ManagedSyncEngine::open(&ro, grant.owner, grant.share_id, cfg).unwrap();
                    let prior = engine.preserved_changes().unwrap();
                    let report = engine.sync_read_only(None).await.unwrap();
                    assert_eq!(
                        fs::read(base.path().join("ro-root/file")).unwrap(),
                        b"updated owner"
                    );
                    let copies: Vec<_> = report
                        .preserved
                        .iter()
                        .filter(|c| {
                            fs::read(&c.preserved_path).ok().as_deref()
                                == Some(b"exact interrupted local work")
                        })
                        .collect();
                    assert_eq!(copies.len(), 1, "exactly one local artifact after {phase}");
                    if let Some(original) = prior.iter().find(|c| {
                        fs::read(&c.preserved_path).ok().as_deref()
                            == Some(b"exact interrupted local work")
                    }) {
                        assert_eq!(
                            copies[0].preserved_path, original.preserved_path,
                            "recovery duplicated preservation"
                        );
                    }
                    use std::os::unix::fs::MetadataExt;
                    eprintln!(
                        "{phase}: root dev {}, state dev {}, retained {}",
                        fs::metadata(base.path()).unwrap().dev(),
                        fs::metadata(state.path()).unwrap().dev(),
                        copies[0].preserved_path.display()
                    );
                    engine.shutdown().await.unwrap();
                    ro.shutdown().await.unwrap();
                    owner.shutdown().await.unwrap();
                });
            }
        },
    );
}

#[cfg(target_os = "linux")]
#[test]
fn real_shared_rw_conflict_restart_and_cross_filesystem_owner_mutations() {
    isolated(
        "real_shared_rw_conflict_restart_and_cross_filesystem_owner_mutations",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
            let base = tempfile::tempdir_in("/tmp").unwrap(); let state = tempfile::tempdir_in("/dev/shm").unwrap();
            let owner = ShareService::open(base.path().join("owner-device"), NetworkMode::DirectOnly, None).await.unwrap();
            let member = ShareService::open(base.path().join("rw-device"), NetworkMode::DirectOnly, None).await.unwrap();
            let share = owner.create_owned_share("RW".into(), base.path().join("owner-root"), state.path().join("owner-state"), None, 0).await.unwrap();
            fs::write(base.path().join("owner-root/file"), b"initial").unwrap();
            let ticket = share.issue_key(Permission::ReadWrite, None, owner.endpoint_addr()).unwrap();
            let grant = member.enroll(&ticket, None).await.unwrap();
            let mut cfg = config(base.path(), "rw"); cfg.state_root = state.path().join("rw-state");
            let engine = ManagedSyncEngine::open(&member, grant.owner, grant.share_id, cfg.clone()).unwrap();
            engine.sync_read_write(None).await.unwrap();
            fs::write(base.path().join("owner-root/file"), b"owner concurrent").unwrap();
            fs::write(base.path().join("rw-root/file"), b"writer concurrent").unwrap();
            let report = engine.sync_read_write(None).await.unwrap();
            assert_eq!(report.conflicts.len(), 1);
            let contents = |root: &std::path::Path| {
                let mut files: Vec<_> = fs::read_dir(root).unwrap().map(|e| fs::read(e.unwrap().path()).unwrap()).collect(); files.sort(); files
            };
            assert_eq!(contents(&base.path().join("owner-root")), vec![b"owner concurrent".to_vec(), b"writer concurrent".to_vec()]);
            assert_eq!(contents(&base.path().join("rw-root")), contents(&base.path().join("owner-root")));
            fs::write(base.path().join("rw-root/file"), b"writer replacement").unwrap();
            engine.sync_read_write(None).await.unwrap();
            assert_eq!(fs::read(base.path().join("owner-root/file")).unwrap(), b"writer replacement");
            fs::remove_file(base.path().join("rw-root/file")).unwrap();
            engine.sync_read_write(None).await.unwrap();
            assert!(!base.path().join("owner-root/file").exists());
            fs::create_dir(base.path().join("rw-root/file")).unwrap();
            engine.sync_read_write(None).await.unwrap();
            assert!(base.path().join("owner-root/file").is_dir());
            engine.shutdown().await.unwrap(); member.shutdown().await.unwrap();
            let member = ShareService::open(base.path().join("rw-device"), NetworkMode::DirectOnly, None).await.unwrap();
            let engine = ManagedSyncEngine::open(&member, grant.owner, grant.share_id, cfg).unwrap();
            let restarted = engine.sync_read_write(None).await.unwrap();
            assert_eq!(restarted.local_actions, 0); assert_eq!(restarted.remote_actions, 0);
            use std::os::unix::fs::MetadataExt;
            eprintln!("real shared RW identities: owner {}, writer {}; root device {}, state device {}", owner.endpoint_id(), member.endpoint_id(), fs::metadata(base.path()).unwrap().dev(), fs::metadata(state.path()).unwrap().dev());
            engine.shutdown().await.unwrap(); member.shutdown().await.unwrap(); owner.shutdown().await.unwrap();
        });
        },
    );
}

#[test]
fn read_only_racing_recreation_is_preserved_and_never_propagated() {
    isolated(
        "read_only_racing_recreation_is_preserved_and_never_propagated",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let base = tempfile::tempdir().unwrap();
                let owner = ShareService::open(
                    base.path().join("owner-device"),
                    NetworkMode::DirectOnly,
                    None,
                )
                .await
                .unwrap();
                let member = ShareService::open(
                    base.path().join("ro-device"),
                    NetworkMode::DirectOnly,
                    None,
                )
                .await
                .unwrap();
                let share = owner
                    .create_owned_share(
                        "Race".into(),
                        base.path().join("owner-root"),
                        base.path().join("owner-state"),
                        None,
                        0,
                    )
                    .await
                    .unwrap();
                fs::write(base.path().join("owner-root/file"), b"initial").unwrap();
                let grant = member
                    .enroll(
                        &share
                            .issue_key(Permission::ReadOnly, None, owner.endpoint_addr())
                            .unwrap(),
                        None,
                    )
                    .await
                    .unwrap();
                let engine = ManagedSyncEngine::open(
                    &member,
                    grant.owner,
                    grant.share_id,
                    config(base.path(), "ro"),
                )
                .unwrap();
                engine.sync_read_only(None).await.unwrap();
                fs::write(base.path().join("ro-root/file"), b"local before capture").unwrap();
                fs::write(base.path().join("owner-root/file"), b"owner changed").unwrap();
                let root = base.path().join("ro-root");
                let observer = deltaweave_net::TransferObserver::new(move |event| {
                    if event.phase == "ro_preserved" {
                        fs::write(root.join("file"), b"concurrently recreated").unwrap();
                    }
                });
                assert!(engine.sync_read_only(Some(observer)).await.is_err());
                assert_eq!(
                    fs::read(base.path().join("ro-root/file")).unwrap(),
                    b"concurrently recreated"
                );
                assert_eq!(
                    fs::read(base.path().join("owner-root/file")).unwrap(),
                    b"owner changed"
                );
                let report = engine.sync_read_only(None).await.unwrap();
                for expected in [
                    b"local before capture".as_slice(),
                    b"concurrently recreated".as_slice(),
                ] {
                    assert!(
                        report
                            .preserved
                            .iter()
                            .any(|p| fs::read(&p.preserved_path).ok().as_deref() == Some(expected))
                    );
                }
                assert_eq!(
                    fs::read(base.path().join("ro-root/file")).unwrap(),
                    b"owner changed"
                );
                engine.shutdown().await.unwrap();
                member.shutdown().await.unwrap();
                owner.shutdown().await.unwrap();
            });
        },
    );
}

#[test]
fn malicious_ro_v2_v3_snapshots_cannot_be_laundered_into_managed_rw() {
    isolated(
        "malicious_ro_v2_v3_snapshots_cannot_be_laundered_into_managed_rw",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                use deltaweave_core::{
                    Hash32, ReplicaId, SYNC_RECORD_SCHEMA_V1, SyncEntryKind, SyncRecord,
                    VersionVector, WirePath,
                };
                use deltaweave_reconcile::MerkleTree;
                let base = tempfile::tempdir().unwrap();
                let owner = ShareService::open(
                    base.path().join("owner-device"),
                    NetworkMode::DirectOnly,
                    None,
                )
                .await
                .unwrap();
                let rw = ShareService::open(
                    base.path().join("rw-device"),
                    NetworkMode::DirectOnly,
                    None,
                )
                .await
                .unwrap();
                let other = ShareService::open(
                    base.path().join("other-device"),
                    NetworkMode::DirectOnly,
                    None,
                )
                .await
                .unwrap();
                let ro = ShareService::open(
                    base.path().join("ro-device"),
                    NetworkMode::DirectOnly,
                    None,
                )
                .await
                .unwrap();
                let share = owner
                    .create_owned_share(
                        "Protected".into(),
                        base.path().join("owner-root"),
                        base.path().join("owner-state"),
                        None,
                        0,
                    )
                    .await
                    .unwrap();
                fs::write(base.path().join("owner-root/file"), b"trusted owner bytes").unwrap();
                let rw_key = share
                    .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
                    .unwrap();
                let grant = rw.enroll(&rw_key, None).await.unwrap();
                other.enroll(&rw_key, None).await.unwrap();
                let ro_grant = ro
                    .enroll(
                        &share
                            .issue_key(Permission::ReadOnly, None, owner.endpoint_addr())
                            .unwrap(),
                        None,
                    )
                    .await
                    .unwrap();
                let writer = ManagedSyncEngine::open(
                    &rw,
                    grant.owner,
                    grant.share_id,
                    config(base.path(), "rw"),
                )
                .unwrap();
                let another = ManagedSyncEngine::open(
                    &other,
                    grant.owner,
                    grant.share_id,
                    config(base.path(), "other"),
                )
                .unwrap();
                writer.sync_read_write(None).await.unwrap();
                another.sync_read_write(None).await.unwrap();
                let malicious_key = deltaweave_net::load_or_create_identity(
                    base.path().join("ro-device/device.key"),
                )
                .unwrap()
                .secret_key;
                assert_eq!(malicious_key.public(), ro_grant.endpoint);
                ro.shutdown().await.unwrap();
                // The attacker deliberately replaces its legitimate RO implementation with a raw
                // server that advertises the original share on both legacy v2 and managed v3.
                let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
                    .secret_key(malicious_key)
                    .alpns(vec![deltaweave_net::ALPN_V2.to_vec(), ALPN_V3.to_vec()])
                    .clear_ip_transports()
                    .bind_addr("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
                    .unwrap()
                    .bind()
                    .await
                    .unwrap();
                let address = iroh::EndpointAddr::new(endpoint.id())
                    .with_ip_addr(endpoint.bound_sockets()[0]);
                let mut version = VersionVector::default();
                version.observe(ReplicaId(Hash32::digest(b"fake owner history")), 999);
                let poison = SyncRecord {
                    schema_version: SYNC_RECORD_SCHEMA_V1,
                    path: WirePath::new("file").unwrap(),
                    kind: SyncEntryKind::File,
                    size: 13,
                    content_hash: Some(Hash32::digest(b"RO poison!!!!")),
                    readonly: false,
                    version,
                    tombstone: false,
                };
                let tree = MerkleTree::from_records(vec![poison.clone()]).unwrap();
                let serving = endpoint.clone();
                let malicious = tokio::spawn(async move {
                    for _ in 0..2 {
                        let connection = serving.accept().await.unwrap().await.unwrap();
                        if connection.alpn() == ALPN_V3 {
                            let (mut send, mut receive) = connection.accept_bi().await.unwrap();
                            let hello = raw_read(&mut receive).await;
                            assert_eq!(hello[0], 3);
                            raw_write(&mut send, &[2]).await; // v3 Accepted
                            send.finish().unwrap();
                        }
                        let (mut send, mut receive) = connection.accept_bi().await.unwrap();
                        loop {
                            let frame = raw_read(&mut receive).await;
                            if frame == [5] {
                                raw_write(&mut send, &[4]).await;
                                send.finish().unwrap();
                                break;
                            }
                            let (variant, prefix): (u32, String) =
                                postcard::from_bytes(&frame).unwrap();
                            assert_eq!(variant, 0);
                            let frame =
                                postcard::to_stdvec(&(0_u32, tree.node_summary(&prefix).unwrap()))
                                    .unwrap();
                            raw_write(&mut send, &frame).await;
                        }
                        connection.closed().await;
                    }
                });
                let probe = deltaweave_net::SyncClient {
                    secret_key: iroh::SecretKey::generate(),
                    remote: address.clone(),
                    network_mode: NetworkMode::DirectOnly,
                };
                let snapshot = probe
                    .fetch_snapshot(&MerkleTree::from_records(Vec::new()).unwrap())
                    .await
                    .unwrap();
                assert_eq!(snapshot.records, vec![poison]);
                let probe = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
                    .bind()
                    .await
                    .unwrap();
                let connection = probe.connect(address, ALPN_V3).await.unwrap();
                let (mut send, mut receive) = connection.open_bi().await.unwrap();
                raw_write(
                    &mut send,
                    &postcard::to_stdvec(&(3_u8, grant.share_id, 2_u32)).unwrap(),
                )
                .await;
                send.finish().unwrap();
                assert_eq!(raw_read(&mut receive).await, vec![2]);
                let (mut send, mut receive) = connection.open_bi().await.unwrap();
                raw_write(&mut send, &postcard::to_stdvec(&(0_u32, "file")).unwrap()).await;
                let (_, summary): (u32, Option<deltaweave_reconcile::MerkleNodeSummary>) =
                    postcard::from_bytes(&raw_read(&mut receive).await).unwrap();
                assert_eq!(
                    summary.unwrap().record.unwrap().content_hash,
                    Some(Hash32::digest(b"RO poison!!!!"))
                );
                raw_write(&mut send, &[5]).await;
                assert_eq!(raw_read(&mut receive).await, vec![4]);
                send.finish().unwrap();
                connection.close(0u8.into(), b"probe complete");
                probe.close().await;
                malicious.await.unwrap();
                // The actual managed engine takes no arbitrary peer/snapshot input. A source switch
                // naming the RO identity for this existing share is refused before root admission.
                let error = ManagedSyncEngine::open(
                    &rw,
                    ro_grant.endpoint,
                    grant.share_id,
                    config(base.path(), "attack"),
                )
                .err()
                .unwrap();
                assert_eq!(ShareError::classify(&error), ShareError::NotMember);
                assert!(!base.path().join("attack-root").exists());
                writer.sync_read_write(None).await.unwrap();
                another.sync_read_write(None).await.unwrap();
                for root in ["owner-root", "rw-root", "other-root"] {
                    assert_eq!(
                        fs::read(base.path().join(root).join("file")).unwrap(),
                        b"trusted owner bytes"
                    );
                }
                assert!(share.provenance().unwrap().is_empty());
                endpoint.close().await;
                writer.shutdown().await.unwrap();
                another.shutdown().await.unwrap();
                rw.shutdown().await.unwrap();
                other.shutdown().await.unwrap();
                owner.shutdown().await.unwrap();
            });
        },
    );
}

async fn raw_read(receive: &mut iroh::endpoint::RecvStream) -> Vec<u8> {
    use tokio::io::AsyncReadExt;
    let len = receive.read_u32().await.unwrap() as usize;
    assert!(len < 1024 * 1024);
    let mut bytes = vec![0; len];
    receive.read_exact(&mut bytes).await.unwrap();
    bytes
}
async fn raw_write(send: &mut iroh::endpoint::SendStream, bytes: &[u8]) {
    use tokio::io::AsyncWriteExt;
    send.write_u32(bytes.len() as u32).await.unwrap();
    send.write_all(bytes).await.unwrap();
}

#[test]
fn managed_resume_rejects_missing_history_without_recreating_it() {
    isolated(
        "managed_resume_rejects_missing_history_without_recreating_it",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let base = tempfile::tempdir().unwrap();
                let owner = ShareService::open(
                    base.path().join("owner-device"),
                    NetworkMode::DirectOnly,
                    None,
                )
                .await
                .unwrap();
                let member = ShareService::open(
                    base.path().join("ro-device"),
                    NetworkMode::DirectOnly,
                    None,
                )
                .await
                .unwrap();
                let share = owner
                    .create_owned_share(
                        "Resume".into(),
                        base.path().join("owner-root"),
                        base.path().join("owner-state"),
                        None,
                        0,
                    )
                    .await
                    .unwrap();
                fs::write(base.path().join("owner-root/file"), b"trusted").unwrap();
                let grant = member
                    .enroll(
                        &share
                            .issue_key(Permission::ReadOnly, None, owner.endpoint_addr())
                            .unwrap(),
                        None,
                    )
                    .await
                    .unwrap();
                let cfg = config(base.path(), "ro");
                let engine =
                    ManagedSyncEngine::open(&member, grant.owner, grant.share_id, cfg.clone())
                        .unwrap();
                engine.sync_read_only(None).await.unwrap();
                engine.shutdown().await.unwrap();
                fs::remove_file(cfg.state_root.join("store/metadata.redb")).unwrap();
                let error =
                    ManagedSyncEngine::resume(&member, grant.owner, grant.share_id, cfg.clone())
                        .err()
                        .unwrap();
                assert_eq!(ShareError::classify(&error), ShareError::StateUnavailable);
                assert!(!cfg.state_root.join("store/metadata.redb").exists());
                assert_eq!(fs::read(cfg.root.join("file")).unwrap(), b"trusted");
                member.shutdown().await.unwrap();
                owner.shutdown().await.unwrap();
            });
        },
    );
}

#[cfg(target_os = "linux")]
#[test]
fn legacy_v1_v2_replacement_and_delete_work_across_real_filesystems() {
    isolated(
        "legacy_v1_v2_replacement_and_delete_work_across_real_filesystems",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                use deltaweave_core::{Hash32, ReplicaId, WirePath};
                use deltaweave_net::{
                    PeerPolicy, PushOptions, ServerConfig, SyncClient, push_file, start_server,
                };
                use deltaweave_sync::{SyncConfig, SyncEngine};
                let base = tempfile::tempdir_in("/tmp").unwrap();
                let state = tempfile::tempdir_in("/dev/shm").unwrap();
                let server = start_server(ServerConfig {
                    secret_key: iroh::SecretKey::generate(),
                    destination_root: base.path().join("receiver"),
                    state_root: state.path().join("receiver-state"),
                    peer_policy: PeerPolicy::AnyAuthenticated,
                    network_mode: NetworkMode::DirectOnly,
                    bind_address: Some("127.0.0.1:0".parse().unwrap()),
                    max_connections: 8,
                    min_free_space_bytes: 0,
                })
                .await
                .unwrap();
                let key = iroh::SecretKey::generate();
                let source = base.path().join("v1-source");
                for bytes in [b"v1 original".as_slice(), b"v1 replacement".as_slice()] {
                    fs::write(&source, bytes).unwrap();
                    push_file(PushOptions {
                        secret_key: key.clone(),
                        source: source.clone(),
                        remote_path: WirePath::new("file").unwrap(),
                        remote: server.endpoint_addr(),
                        profile: ChunkingProfile::DEFAULT,
                        network_mode: NetworkMode::DirectOnly,
                        state_root: None,
                    })
                    .await
                    .unwrap();
                    assert_eq!(fs::read(base.path().join("receiver/file")).unwrap(), bytes);
                }
                let engine = SyncEngine::open(SyncConfig {
                    swarm_sources: Vec::new(),
                    root: base.path().join("sender"),
                    state_root: state.path().join("sender-state"),
                    replica: ReplicaId(Hash32::digest(key.public().as_bytes())),
                    client: SyncClient {
                        secret_key: key,
                        remote: server.endpoint_addr(),
                        network_mode: NetworkMode::DirectOnly,
                    },
                    profile: ChunkingProfile::DEFAULT,
                    ignored_paths: Vec::new(),
                })
                .unwrap();
                engine.sync_once().await.unwrap();
                fs::write(base.path().join("sender/file"), b"v2 replacement").unwrap();
                engine.sync_once().await.unwrap();
                assert_eq!(
                    fs::read(base.path().join("receiver/file")).unwrap(),
                    b"v2 replacement"
                );
                fs::remove_file(base.path().join("sender/file")).unwrap();
                engine.sync_once().await.unwrap();
                assert!(!base.path().join("receiver/file").exists());
                drop(engine);
                server.shutdown().await.unwrap();
                let store = deltaweave_store::Store::open_with_recovery_reserver(
                    state.path().join("receiver-state"),
                    |path| deltaweave_net::root_admission::reserve_private(path),
                )
                .unwrap();
                let changes = store
                    .recover_path_changes(&base.path().join("receiver"))
                    .unwrap();
                for bytes in [
                    b"v1 original".as_slice(),
                    b"v1 replacement".as_slice(),
                    b"v2 replacement".as_slice(),
                ] {
                    assert!(
                        changes
                            .iter()
                            .any(|c| fs::read(&c.artifact).ok().as_deref() == Some(bytes))
                    );
                }
                assert!(
                    changes
                        .iter()
                        .all(|c| c.state == deltaweave_store::PathChangeState::Committed)
                );
                use std::os::unix::fs::MetadataExt;
                eprintln!(
                    "real v1/v2 transfer: root device {}, state device {}, {} committed attempts",
                    fs::metadata(base.path()).unwrap().dev(),
                    fs::metadata(state.path()).unwrap().dev(),
                    changes.len()
                );
            });
        },
    );
}

#[test]
fn authenticated_owner_rollback_divergence_and_missing_tombstones_are_rejected() {
    isolated(
        "authenticated_owner_rollback_divergence_and_missing_tombstones_are_rejected",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                use deltaweave_core::Hash32;
                use deltaweave_reconcile::MerkleTree;
                let base = tempfile::tempdir().unwrap();
                let owner = ShareService::open(
                    base.path().join("owner-device"),
                    NetworkMode::DirectOnly,
                    Some("127.0.0.1:0".parse().unwrap()),
                )
                .await
                .unwrap();
                let member = ShareService::open(
                    base.path().join("ro-device"),
                    NetworkMode::DirectOnly,
                    None,
                )
                .await
                .unwrap();
                let share = owner
                    .create_owned_share(
                        "Checkpoint".into(),
                        base.path().join("owner-root"),
                        base.path().join("owner-state"),
                        None,
                        0,
                    )
                    .await
                    .unwrap();
                fs::write(base.path().join("owner-root/file"), b"old owner").unwrap();
                let grant = member
                    .enroll(
                        &share
                            .issue_key(Permission::ReadOnly, None, owner.endpoint_addr())
                            .unwrap(),
                        None,
                    )
                    .await
                    .unwrap();
                let engine = ManagedSyncEngine::open(
                    &member,
                    grant.owner,
                    grant.share_id,
                    config(base.path(), "ro"),
                )
                .unwrap();
                engine.sync_read_only(None).await.unwrap();
                let session = member.open_session(grant.owner, grant.share_id).unwrap();
                let empty = MerkleTree::from_records(Vec::new()).unwrap();
                let old = session.fetch_snapshot(&empty).await.unwrap().records;
                fs::write(base.path().join("owner-root/file"), b"trusted new owner").unwrap();
                engine.sync_read_only(None).await.unwrap();
                let mut divergent = session.fetch_snapshot(&empty).await.unwrap().records;
                divergent[0].content_hash = Some(Hash32::digest(b"equal-version poison"));
                divergent[0].size = 20;
                let key = deltaweave_net::load_or_create_identity(
                    base.path().join("owner-device/device.key"),
                )
                .unwrap()
                .secret_key;
                let bind = *owner.endpoint_addr().ip_addrs().next().unwrap();
                session.close().await;
                owner.shutdown().await.unwrap();
                // Restore/hostile owner uses the original authenticated identity and address.
                let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
                    .secret_key(key)
                    .alpns(vec![ALPN_V3.to_vec()])
                    .clear_ip_transports()
                    .bind_addr(bind)
                    .unwrap()
                    .bind()
                    .await
                    .unwrap();
                let serving = endpoint.clone();
                let responses = tokio::spawn(async move {
                    for records in [old, divergent, Vec::new()] {
                        let tree = MerkleTree::from_records(records).unwrap();
                        let connection = serving.accept().await.unwrap().await.unwrap();
                        let (mut send, mut receive) = connection.accept_bi().await.unwrap();
                        raw_read(&mut receive).await;
                        raw_write(&mut send, &[2]).await;
                        send.finish().unwrap();
                        let (mut send, mut receive) = connection.accept_bi().await.unwrap();
                        loop {
                            let frame = raw_read(&mut receive).await;
                            if frame == [5] {
                                raw_write(&mut send, &[4]).await;
                                send.finish().unwrap();
                                break;
                            }
                            let (_, prefix): (u32, String) = postcard::from_bytes(&frame).unwrap();
                            raw_write(
                                &mut send,
                                &postcard::to_stdvec(&(0_u32, tree.node_summary(&prefix).unwrap()))
                                    .unwrap(),
                            )
                            .await;
                        }
                        connection.closed().await;
                    }
                });
                let before = engine.preserved_changes().unwrap();
                for _ in 0..3 {
                    let error = engine.sync_read_only(None).await.err().unwrap();
                    assert_eq!(ShareError::classify(&error), ShareError::InvalidRecord);
                    assert_eq!(
                        fs::read(base.path().join("ro-root/file")).unwrap(),
                        b"trusted new owner"
                    );
                    assert_eq!(engine.preserved_changes().unwrap(), before);
                }
                responses.await.unwrap();
                endpoint.close().await;
                engine.shutdown().await.unwrap();
                member.shutdown().await.unwrap();
            });
        },
    );
}

#[test]
fn owner_causal_interruption_child() {
    let Some(base) = std::env::var_os("DW_CAUSAL_INTERRUPT_BASE") else {
        return;
    };
    let base = std::path::PathBuf::from(base);
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        use deltaweave_core::Hash32;
        let owner = ShareService::open(base.join("owner-device"), NetworkMode::DirectOnly, None)
            .await
            .unwrap();
        let member = ShareService::open(base.join("rw-device"), NetworkMode::DirectOnly, None)
            .await
            .unwrap();
        let share = owner
            .create_owned_share(
                "Causal crash".into(),
                base.join("owner-root"),
                base.join("owner-state"),
                None,
                0,
            )
            .await
            .unwrap();
        fs::write(base.join("owner-root/file"), b"original owner").unwrap();
        let grant = member
            .enroll(
                &share
                    .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
                    .unwrap(),
                None,
            )
            .await
            .unwrap();
        let session = member.open_session(grant.owner, grant.share_id).unwrap();
        let mut record = session
            .fetch_snapshot(&deltaweave_reconcile::MerkleTree::from_records(Vec::new()).unwrap())
            .await
            .unwrap()
            .records
            .remove(0);
        fs::write(
            base.join("original-record"),
            postcard::to_stdvec(&record).unwrap(),
        )
        .unwrap();
        record.version.observe(grant.replica, 1);
        record.content_hash = Some(Hash32::digest(b"accepted writer"));
        record.size = 15;
        fs::write(
            base.join("expected-record"),
            postcard::to_stdvec(&record).unwrap(),
        )
        .unwrap();
        fs::write(base.join("source"), b"accepted writer").unwrap();
        let observed_share = share.clone();
        let observed_base = base.clone();
        share.set_observer(Some(deltaweave_net::TransferObserver::new(move |event| {
            if event.phase == "materialized" {
                if let Ok(mode) = std::env::var("DW_CAUSAL_MODE") {
                    let revoking = observed_share.clone();
                    let runtime = tokio::runtime::Handle::current();
                    std::thread::spawn(move || {
                        runtime.block_on(async move {
                            revoking.revoke_member(grant.endpoint).await.unwrap();
                        })
                    });
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
                    while observed_share.members().unwrap()[0].revoked_at.is_none() {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "revocation did not become durable"
                        );
                        std::thread::yield_now();
                    }
                    assert!(observed_share.members().unwrap()[0].epoch > grant.epoch);
                    if mode == "diverged" {
                        fs::write(observed_base.join("owner-root/file"), b"racing local bytes")
                            .unwrap();
                    }
                }
                std::process::exit(74);
            }
        })));
        let _ = session
            .push_record(base.join("source"), record, ChunkingProfile::DEFAULT)
            .await;
        // Revocation closes the client connection before the observer thread exits the crash
        // worker. Give that thread time to record the intended interruption status instead of
        // racing an unrelated unwrap panic in this task.
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        panic!("owner materialization barrier did not fire");
    });
}

#[test]
fn active_owner_recovery_keeps_exact_causal_target_and_authenticated_provenance() {
    isolated(
        "active_owner_recovery_keeps_exact_causal_target_and_authenticated_provenance",
        || {
            let base = tempfile::tempdir().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "owner_causal_interruption_child", "--nocapture"])
                .env("DW_CAUSAL_INTERRUPT_BASE", base.path())
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(74));
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let owner = ShareService::open(
                    base.path().join("owner-device"),
                    NetworkMode::DirectOnly,
                    None,
                )
                .await
                .unwrap();
                let share = owner
                    .load_owned_share(owner.owned_configs().unwrap()[0].share_id)
                    .await
                    .unwrap();
                let member = ShareService::open(
                    base.path().join("rw-device"),
                    NetworkMode::DirectOnly,
                    None,
                )
                .await
                .unwrap();
                let grant = member
                    .enroll(
                        &share
                            .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
                            .unwrap(),
                        None,
                    )
                    .await
                    .unwrap();
                let expected: deltaweave_core::SyncRecord =
                    postcard::from_bytes(&fs::read(base.path().join("expected-record")).unwrap())
                        .unwrap();
                let session = member.open_session(grant.owner, grant.share_id).unwrap();
                let actual = session
                    .fetch_snapshot(
                        &deltaweave_reconcile::MerkleTree::from_records(Vec::new()).unwrap(),
                    )
                    .await
                    .unwrap()
                    .records;
                assert_eq!(
                    actual,
                    vec![expected.clone()],
                    "recovery relabeled the unadopted writer target"
                );
                let audit = share.provenance().unwrap();
                assert_eq!(audit.len(), 1);
                assert_eq!(audit[0].peer, grant.endpoint);
                assert_eq!(audit[0].membership_epoch, grant.epoch);
                assert_eq!(audit[0].record_hash, expected.logical_hash());
                assert_eq!(
                    fs::read(base.path().join("owner-root/file")).unwrap(),
                    b"accepted writer"
                );
                session.close().await;
                member.shutdown().await.unwrap();
                owner.shutdown().await.unwrap();
            });
        },
    );
}

#[test]
fn revoked_epoch_recovery_rolls_back_and_divergence_blocks_scans() {
    isolated(
        "revoked_epoch_recovery_rolls_back_and_divergence_blocks_scans",
        || {
            let retained = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
            for (mode, base) in ["revoked", "diverged"].into_iter().zip(&retained) {
                let status = std::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", "owner_causal_interruption_child", "--nocapture"])
                    .env("DW_CAUSAL_INTERRUPT_BASE", base.path())
                    .env("DW_CAUSAL_MODE", mode)
                    .status()
                    .unwrap();
                assert_eq!(status.code(), Some(74));
                tokio::runtime::Runtime::new().unwrap().block_on(async {
                    let owner = ShareService::open(
                        base.path().join("owner-device"),
                        NetworkMode::DirectOnly,
                        None,
                    )
                    .await
                    .unwrap();
                    let share_id = owner.owned_configs().unwrap()[0].share_id;
                    if mode == "diverged" {
                        let error = owner.load_owned_share(share_id).await.err().unwrap();
                        assert!(matches!(
                            deltaweave_sync::ManagedSyncFailure::classify(&error),
                            deltaweave_sync::ManagedSyncFailure::LocalChanged
                                | deltaweave_sync::ManagedSyncFailure::StateUnavailable
                                | deltaweave_sync::ManagedSyncFailure::Share(
                                    ShareError::StateUnavailable
                                )
                        ));
                        assert_eq!(
                            fs::read(base.path().join("owner-root/file")).unwrap(),
                            b"racing local bytes"
                        );
                        // A caller may safely save its changed object, restore the exact pending target,
                        // then retry load. The failed load must not have scanned/advanced the old index.
                        fs::rename(
                            base.path().join("owner-root/file"),
                            base.path().join("saved-racing-local"),
                        )
                        .unwrap();
                        fs::write(base.path().join("owner-root/file"), b"accepted writer").unwrap();
                    }
                    let share = owner.load_owned_share(share_id).await.unwrap();
                    assert!(share.provenance().unwrap().is_empty());
                    assert!(share.members().unwrap()[0].revoked_at.is_some());
                    let reader = ShareService::open(
                        base.path().join("reader-device"),
                        NetworkMode::DirectOnly,
                        None,
                    )
                    .await
                    .unwrap();
                    let grant = reader
                        .enroll(
                            &share
                                .issue_key(Permission::ReadOnly, None, owner.endpoint_addr())
                                .unwrap(),
                            None,
                        )
                        .await
                        .unwrap();
                    let session = reader.open_session(grant.owner, grant.share_id).unwrap();
                    let records = session
                        .fetch_snapshot(
                            &deltaweave_reconcile::MerkleTree::from_records(Vec::new()).unwrap(),
                        )
                        .await
                        .unwrap()
                        .records;
                    let original: deltaweave_core::SyncRecord = postcard::from_bytes(
                        &fs::read(base.path().join("original-record")).unwrap(),
                    )
                    .unwrap();
                    assert_eq!(
                        records,
                        vec![original],
                        "rollback advanced the owner's causal history"
                    );
                    assert_eq!(
                        fs::read(base.path().join("owner-root/file")).unwrap(),
                        b"original owner"
                    );
                    session.close().await;
                    reader.shutdown().await.unwrap();
                    drop(share);
                    owner.shutdown().await.unwrap();
                    let store =
                        deltaweave_store::Store::open(base.path().join("owner-state")).unwrap();
                    let changes = store.path_changes().unwrap();
                    assert_eq!(changes.len(), 1);
                    assert_eq!(
                        changes[0].state,
                        deltaweave_store::PathChangeState::RolledBack
                    );
                    assert_eq!(
                        fs::read(&changes[0].rollback_artifact).unwrap(),
                        b"accepted writer"
                    );
                    assert!(!changes[0].artifact.exists());
                    if mode == "diverged" {
                        assert_eq!(
                            fs::read(base.path().join("saved-racing-local")).unwrap(),
                            b"racing local bytes"
                        );
                    }
                });
            }
        },
    );
}
