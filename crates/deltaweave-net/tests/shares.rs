use deltaweave_core::{Hash32, ReplicaId};
use deltaweave_net::{NetworkMode, share::*};

fn isolated(name: &str, body: impl FnOnce()) {
    if std::env::var("DELTAWEAVE_SHARE_TEST").ok().as_deref() == Some(name) {
        body();
        return;
    }
    let home = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture"])
        .env("DELTAWEAVE_SHARE_TEST", name)
        .env("HOME", home.path())
        .env("USERPROFILE", home.path())
        .status()
        .unwrap();
    assert!(status.success(), "isolated share test failed");
}

#[test]
fn actual_quic_enrollment_is_scoped_read_only_and_durable() {
    isolated(
        "actual_quic_enrollment_is_scoped_read_only_and_durable",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let temp = tempfile::tempdir().unwrap();
                let owner = ShareService::open(
                    temp.path().join("owner-device"),
                    NetworkMode::DirectOnly,
                    None,
                )
                .await
                .unwrap();
                let member = ShareService::open(
                    temp.path().join("member-device"),
                    NetworkMode::DirectOnly,
                    None,
                )
                .await
                .unwrap();
                let share = owner
                    .create_owned_share(
                        "Documents".into(),
                        temp.path().join("root"),
                        temp.path().join("state"),
                        Some(ReplicaId(Hash32::digest(b"retained-owner"))),
                        0,
                    )
                    .await
                    .unwrap();
                std::fs::write(temp.path().join("root/file"), b"protected").unwrap();
                let ticket = share
                    .issue_key(Permission::ReadOnly, None, owner.endpoint_addr())
                    .unwrap();
                let preview = member.validate_ticket(&ticket).await.unwrap();
                assert_eq!(preview.permission, Permission::ReadOnly);
                assert!(
                    share.members().unwrap().is_empty(),
                    "preview must not enroll"
                );
                let grant = member.enroll(&ticket, None).await.unwrap();
                assert_eq!(grant.permission, Permission::ReadOnly);
                let stronger = share
                    .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
                    .unwrap();
                assert_eq!(member.enroll(&stronger, None).await.unwrap(), grant);
                assert_eq!(share.members().unwrap(), vec![grant.clone()]);
                let session = member.open_session(grant.owner, grant.share_id).unwrap();
                let empty = deltaweave_reconcile::MerkleTree::from_records(Vec::new()).unwrap();
                let snapshot = session.fetch_snapshot(&empty).await.unwrap();
                let local = temp.path().join("member-root");
                let _lease = member
                    .admit_member_root(grant.owner, grant.share_id, &local)
                    .unwrap();
                let store = std::sync::Arc::new(
                    deltaweave_store::Store::open(temp.path().join("member-store")).unwrap(),
                );
                let file = snapshot
                    .records
                    .iter()
                    .find(|r| r.path.as_str() == "file")
                    .unwrap()
                    .clone();
                assert!(
                    session
                        .pull_record_to_with_budget(
                            file.clone(),
                            store.clone(),
                            local.clone(),
                            u64::MAX,
                            file.size
                        )
                        .await
                        .is_err()
                );
                assert_eq!(
                    std::fs::read_dir(temp.path().join("member-store/chunks"))
                        .unwrap()
                        .count(),
                    0
                );
                let pulled = session
                    .pull_record_to_with_budget(
                        file.clone(),
                        store.clone(),
                        local.clone(),
                        0,
                        file.size,
                    )
                    .await
                    .unwrap();
                assert!(pulled.transferred_bytes > 0);
                store
                    .materialize(&pulled.manifest, &file.path, &local)
                    .unwrap();
                assert_eq!(std::fs::read(local.join("file")).unwrap(), b"protected");
                let mut deleted = snapshot
                    .records
                    .iter()
                    .find(|r| r.path.as_str() == "file")
                    .unwrap()
                    .clone();
                deleted.tombstone = true;
                deleted.content_hash = None;
                deleted.size = 0;
                deleted.version.increment(grant.replica).unwrap();
                assert!(session.apply_metadata(deleted).await.is_err());
                assert_eq!(
                    std::fs::read(temp.path().join("root/file")).unwrap(),
                    b"protected"
                );
                let mut write = snapshot
                    .records
                    .iter()
                    .find(|r| r.path.as_str() == "file")
                    .unwrap()
                    .clone();
                write.version.increment(grant.replica).unwrap();
                write.content_hash = Some(Hash32::digest(b"malicious"));
                write.size = 9;
                std::fs::write(temp.path().join("attack"), b"malicious").unwrap();
                assert!(
                    session
                        .push_record(
                            temp.path().join("attack"),
                            write.clone(),
                            deltaweave_core::ChunkingProfile::DEFAULT
                        )
                        .await
                        .is_err()
                );
                write.path = deltaweave_core::WirePath::new("intruder-directory").unwrap();
                write.kind = deltaweave_core::SyncEntryKind::Directory;
                write.size = 0;
                write.content_hash = None;
                assert!(session.apply_metadata(write).await.is_err());
                assert!(!temp.path().join("root/intruder-directory").exists());
                assert_eq!(
                    session.fetch_snapshot(&empty).await.unwrap().root_hash,
                    snapshot.root_hash
                );
                assert_eq!(
                    std::fs::read(temp.path().join("root/file")).unwrap(),
                    b"protected"
                );
                share.revoke_member(grant.endpoint).await.unwrap();
                assert!(session.fetch_snapshot(&empty).await.is_err());
                drop(session);
                drop(share);
                owner.shutdown().await.unwrap();
                member.shutdown().await.unwrap();
            })
        },
    );
}

#[test]
fn read_write_counters_and_cross_share_authority_are_enforced() {
    isolated(
        "read_write_counters_and_cross_share_authority_are_enforced",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                use deltaweave_core::{ChunkingProfile, SyncEntryKind, WirePath};
                let temp = tempfile::tempdir().unwrap();
                let owner =
                    ShareService::open(temp.path().join("owner"), NetworkMode::DirectOnly, None)
                        .await
                        .unwrap();
                let writer =
                    ShareService::open(temp.path().join("writer"), NetworkMode::DirectOnly, None)
                        .await
                        .unwrap();
                let victim =
                    ShareService::open(temp.path().join("victim"), NetworkMode::DirectOnly, None)
                        .await
                        .unwrap();
                let share = owner
                    .create_owned_share(
                        "A".into(),
                        temp.path().join("A"),
                        temp.path().join("A-state"),
                        None,
                        0,
                    )
                    .await
                    .unwrap();
                let other = owner
                    .create_owned_share(
                        "B".into(),
                        temp.path().join("B"),
                        temp.path().join("B-state"),
                        None,
                        0,
                    )
                    .await
                    .unwrap();
                std::fs::write(temp.path().join("A/file"), b"before").unwrap();
                std::fs::write(temp.path().join("B/file"), b"secret B").unwrap();
                let key = share
                    .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
                    .unwrap();
                let grant = writer.enroll(&key, None).await.unwrap();
                let victim_grant = victim.enroll(&key, None).await.unwrap();
                assert!(
                    writer
                        .open_session(owner.endpoint_id(), other.config().share_id)
                        .is_err()
                );
                let session = writer.open_session(grant.owner, grant.share_id).unwrap();
                let empty = deltaweave_reconcile::MerkleTree::from_records(Vec::new()).unwrap();
                let snapshot = session.fetch_snapshot(&empty).await.unwrap();
                let original = snapshot
                    .records
                    .iter()
                    .find(|r| r.path.as_str() == "file")
                    .unwrap()
                    .clone();
                let source = temp.path().join("source");
                std::fs::write(&source, b"after!").unwrap();
                let mut valid = original.clone();
                valid.version.observe(grant.replica, 1_000_000);
                valid.content_hash = Some(Hash32::digest(b"after!"));
                session
                    .push_record(&source, valid.clone(), ChunkingProfile::DEFAULT)
                    .await
                    .unwrap();
                assert_eq!(
                    std::fs::read(temp.path().join("A/file")).unwrap(),
                    b"after!"
                );
                let before_attack = session.fetch_snapshot(&empty).await.unwrap();
                for foreign in [
                    victim_grant.replica,
                    ReplicaId(Hash32::digest(b"unknown")),
                    ReplicaId(Hash32::digest(
                        b"deltaweave deterministic conflict resolver v1",
                    )),
                ] {
                    let mut attack = valid.clone();
                    attack.version.observe(foreign, u64::MAX);
                    attack.version.observe(grant.replica, 1_000_001);
                    assert!(
                        session
                            .push_record(&source, attack, ChunkingProfile::DEFAULT)
                            .await
                            .is_err()
                    );
                    assert_eq!(
                        std::fs::read(temp.path().join("A/file")).unwrap(),
                        b"after!"
                    );
                    assert_eq!(
                        session.fetch_snapshot(&empty).await.unwrap().root_hash,
                        before_attack.root_hash
                    );
                }
                let mut oversized = valid.clone();
                for value in 0u64..4097 {
                    oversized
                        .version
                        .observe(ReplicaId(Hash32::digest(&value.to_le_bytes())), 1);
                }
                assert!(
                    session
                        .push_record(&source, oversized, ChunkingProfile::DEFAULT)
                        .await
                        .is_err()
                );
                let mut directory = valid.clone();
                directory.path = WirePath::new("folder").unwrap();
                directory.kind = SyncEntryKind::Directory;
                directory.size = 0;
                directory.content_hash = None;
                directory.version.observe(grant.replica, 1_000_002);
                directory.version.observe(
                    ReplicaId(Hash32::digest(
                        b"deltaweave deterministic conflict resolver v1",
                    )),
                    1,
                );
                session.apply_metadata(directory).await.unwrap();
                assert!(temp.path().join("A/folder").is_dir());
                assert_eq!(
                    std::fs::read(temp.path().join("B/file")).unwrap(),
                    b"secret B"
                );
                let audit = share.provenance().unwrap();
                assert_eq!(audit.len(), 2);
                assert!(
                    audit
                        .iter()
                        .all(|entry| entry.peer == writer.endpoint_id()
                            && entry.membership_epoch == 1)
                );
                drop(session);
                drop(share);
                drop(other);
                owner.shutdown().await.unwrap();
                writer.shutdown().await.unwrap();
                victim.shutdown().await.unwrap();
            })
        },
    );
}

#[test]
fn revocation_waits_for_blocking_namespace_operations() {
    isolated("revocation_waits_for_blocking_namespace_operations", || {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        tokio::task::LocalSet::new().block_on(&runtime, async {
            use deltaweave_core::{SyncEntryKind, WirePath};
            use std::sync::{Arc, Barrier};
            let temp = tempfile::tempdir().unwrap();
            let owner =
                ShareService::open(temp.path().join("owner"), NetworkMode::DirectOnly, None)
                    .await
                    .unwrap();
            let member =
                ShareService::open(temp.path().join("member"), NetworkMode::DirectOnly, None)
                    .await
                    .unwrap();
            let share = owner
                .create_owned_share(
                    "Files".into(),
                    temp.path().join("root"),
                    temp.path().join("state"),
                    None,
                    0,
                )
                .await
                .unwrap();
            std::fs::write(temp.path().join("root/file"), b"protected").unwrap();
            let ticket = share
                .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
                .unwrap();
            let grant = member.enroll(&ticket, None).await.unwrap();
            let session = member.open_session(grant.owner, grant.share_id).unwrap();
            let empty = deltaweave_reconcile::MerkleTree::from_records(Vec::new()).unwrap();
            let mut record = session.fetch_snapshot(&empty).await.unwrap().records[0].clone();
            record.path = WirePath::new("new-directory").unwrap();
            record.kind = SyncEntryKind::Directory;
            record.size = 0;
            record.content_hash = None;
            record.version.increment(grant.replica).unwrap();
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(Barrier::new(2));
            let e = entered.clone();
            let r = release.clone();
            share.set_observer(Some(deltaweave_net::TransferObserver::new(move |event| {
                if event.phase == "applying" {
                    e.notify_one();
                    r.wait();
                }
            })));
            let applying = tokio::spawn(async move { session.apply_metadata(record).await });
            tokio::time::timeout(std::time::Duration::from_secs(10), entered.notified())
                .await
                .unwrap();
            let revoke_share = share.clone();
            let revoking = tokio::task::spawn_local(async move {
                revoke_share.revoke_member(grant.endpoint).await
            });
            while share.members().unwrap()[0].revoked_at.is_none() {
                tokio::task::yield_now().await;
            }
            // Both this assertion task and revoke run on one LocalSet thread.
            // Seeing durable denial means revoke yielded at its drain or returned.
            let returned_early = revoking.is_finished();
            tokio::task::spawn_blocking(move || release.wait())
                .await
                .unwrap();
            revoking.await.unwrap().unwrap();
            assert!(
                !returned_early,
                "revocation returned with a blocked disk operation"
            );
            let existed_at_return = temp.path().join("root/new-directory").exists();
            let _ = applying.await.unwrap();
            assert_eq!(
                temp.path().join("root/new-directory").exists(),
                existed_at_return
            );
            assert_eq!(
                std::fs::read(temp.path().join("root/file")).unwrap(),
                b"protected"
            );
            assert!(
                share.provenance().unwrap().is_empty(),
                "denied adoption must not commit provenance"
            );
            drop(share);
            owner.shutdown().await.unwrap();
            member.shutdown().await.unwrap();
        })
    });
}

#[test]
fn actual_legacy_keys_preserve_two_historical_replicas_and_reject_rebinding() {
    isolated(
        "actual_legacy_keys_preserve_two_historical_replicas_and_reject_rebinding",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                use deltaweave_core::ChunkingProfile;
                use deltaweave_index::{IndexOptions, LocalIndex};
                use deltaweave_net::{PeerPolicy, ServerConfig, SyncClient, start_server};
                let temp = tempfile::tempdir().unwrap();
                let old_owner = iroh::SecretKey::generate();
                let old_member = iroh::SecretKey::generate();
                let old_owner_id = ReplicaId(Hash32::digest(old_owner.public().as_bytes()));
                let old_member_id = ReplicaId(Hash32::digest(old_member.public().as_bytes()));
                let root = temp.path().join("owner-root");
                let state = temp.path().join("owner-state");
                std::fs::create_dir(&root).unwrap();
                std::fs::write(root.join("owner-file"), b"owner").unwrap();
                let legacy = start_server(ServerConfig {
                    secret_key: old_owner.clone(),
                    destination_root: root.clone(),
                    state_root: state.clone(),
                    peer_policy: PeerPolicy::AllowListed(
                        [old_member.public()].into_iter().collect(),
                    ),
                    network_mode: NetworkMode::DirectOnly,
                    bind_address: None,
                    max_connections: 8,
                    min_free_space_bytes: 0,
                })
                .await
                .unwrap();
                let client = SyncClient {
                    secret_key: old_member.clone(),
                    remote: legacy.endpoint_addr(),
                    network_mode: NetworkMode::DirectOnly,
                };
                let empty = deltaweave_reconcile::MerkleTree::from_records(Vec::new()).unwrap();
                let owner_record = client.fetch_snapshot(&empty).await.unwrap().records[0].clone();
                let participant_root = temp.path().join("participant-root");
                std::fs::create_dir(&participant_root).unwrap();
                std::fs::write(participant_root.join("owner-file"), b"owner").unwrap();
                let participant_state = temp.path().join("participant-state");
                let participant = LocalIndex::open(
                    &participant_root,
                    participant_state.join("index.redb"),
                    old_member_id,
                    IndexOptions::default(),
                )
                .unwrap();
                participant.adopt_verified_record(&owner_record).unwrap();
                std::fs::write(participant_root.join("member-file"), b"member").unwrap();
                participant.scan().unwrap();
                let member_record = participant
                    .sync_records()
                    .unwrap()
                    .into_iter()
                    .find(|record| record.path.as_str() == "member-file")
                    .unwrap();
                client
                    .push_record(
                        participant_root.join("member-file"),
                        member_record,
                        ChunkingProfile::DEFAULT,
                    )
                    .await
                    .unwrap();
                let retained = participant.sync_records().unwrap();
                drop(participant);
                legacy.shutdown().await.unwrap();
                let owner = ShareService::open(
                    temp.path().join("owner-device"),
                    NetworkMode::DirectOnly,
                    None,
                )
                .await
                .unwrap();
                assert_ne!(owner.endpoint_id(), old_owner.public());
                let share = owner
                    .create_owned_share("Migrated".into(), root.clone(), state.clone(), None, 0)
                    .await
                    .unwrap();
                let ticket = share
                    .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
                    .unwrap();
                let member = ShareService::open(
                    temp.path().join("member-device"),
                    NetworkMode::DirectOnly,
                    None,
                )
                .await
                .unwrap();
                assert_ne!(member.endpoint_id(), old_member.public());
                let proof =
                    LegacyProof::create(&ticket, &old_member, member.endpoint_id(), old_member_id)
                        .unwrap();
                let grant = member.enroll(&ticket, Some(proof.clone())).await.unwrap();
                assert_eq!(grant.replica, old_member_id);
                assert_eq!(member.enroll(&ticket, Some(proof)).await.unwrap(), grant);
                let participant = LocalIndex::open(
                    &participant_root,
                    participant_state.join("index.redb"),
                    grant.replica,
                    IndexOptions::default(),
                )
                .unwrap();
                assert_eq!(participant.sync_records().unwrap(), retained);
                std::fs::write(participant_root.join("member-file"), b"new member edit").unwrap();
                participant.scan().unwrap();
                let edited = participant
                    .sync_records()
                    .unwrap()
                    .into_iter()
                    .find(|r| r.path.as_str() == "member-file")
                    .unwrap();
                assert!(
                    edited.version.get(old_member_id)
                        > retained
                            .iter()
                            .find(|r| r.path == edited.path)
                            .unwrap()
                            .version
                            .get(old_member_id)
                );
                let session = member.open_session(grant.owner, grant.share_id).unwrap();
                session
                    .push_record(
                        participant_root.join("member-file"),
                        edited,
                        ChunkingProfile::DEFAULT,
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    std::fs::read(root.join("member-file")).unwrap(),
                    b"new member edit"
                );
                let intruder =
                    ShareService::open(temp.path().join("intruder"), NetworkMode::DirectOnly, None)
                        .await
                        .unwrap();
                let stolen_claim = LegacyProof::create(
                    &ticket,
                    &old_member,
                    intruder.endpoint_id(),
                    old_member_id,
                )
                .unwrap();
                assert!(
                    intruder
                        .enroll(&ticket, Some(stolen_claim.clone()))
                        .await
                        .is_err()
                );
                share.revoke_member(grant.endpoint).await.unwrap();
                assert!(intruder.enroll(&ticket, Some(stolen_claim)).await.is_err());
                let fresh = intruder.enroll(&ticket, None).await.unwrap();
                assert_ne!(fresh.replica, old_member_id);
                assert_ne!(fresh.replica, old_owner_id);
                let config = share.config().clone();
                drop(session);
                drop(share);
                drop(participant);
                owner.shutdown().await.unwrap();
                member.shutdown().await.unwrap();
                intruder.shutdown().await.unwrap();
                let restarted = ShareService::open(
                    temp.path().join("owner-device"),
                    NetworkMode::DirectOnly,
                    None,
                )
                .await
                .unwrap();
                let loaded = restarted.load_owned_share(config.share_id).await.unwrap();
                assert_eq!(loaded.config().replica, old_owner_id);
                assert!(
                    loaded
                        .members()
                        .unwrap()
                        .iter()
                        .find(|m| m.endpoint == grant.endpoint)
                        .unwrap()
                        .revoked_at
                        .is_some()
                );
                assert_eq!(loaded.provenance().unwrap()[0].peer, grant.endpoint);
                drop(loaded);
                restarted.shutdown().await.unwrap();
            })
        },
    );
}

#[test]
fn nonauthoring_legacy_participant_can_retain_its_nonempty_index() {
    isolated(
        "nonauthoring_legacy_participant_can_retain_its_nonempty_index",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                use deltaweave_index::{IndexOptions, LocalIndex};
                let temp = tempfile::tempdir().unwrap();
                let owner =
                    ShareService::open(temp.path().join("owner"), NetworkMode::DirectOnly, None)
                        .await
                        .unwrap();
                let share = owner
                    .create_owned_share(
                        "Files".into(),
                        temp.path().join("root"),
                        temp.path().join("state"),
                        None,
                        0,
                    )
                    .await
                    .unwrap();
                std::fs::write(temp.path().join("root/file"), b"original").unwrap();
                let key = share
                    .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
                    .unwrap();
                let first =
                    ShareService::open(temp.path().join("first"), NetworkMode::DirectOnly, None)
                        .await
                        .unwrap();
                let first_grant = first.enroll(&key, None).await.unwrap();
                let session = first
                    .open_session(first_grant.owner, first_grant.share_id)
                    .unwrap();
                let empty = deltaweave_reconcile::MerkleTree::from_records(Vec::new()).unwrap();
                let record = session.fetch_snapshot(&empty).await.unwrap().records[0].clone();
                let old = iroh::SecretKey::generate();
                let replica = ReplicaId(Hash32::digest(old.public().as_bytes()));
                let local = temp.path().join("local");
                std::fs::create_dir(&local).unwrap();
                std::fs::write(local.join("file"), b"original").unwrap();
                let index_path = temp.path().join("local-state/index.redb");
                let index = LocalIndex::open(&local, &index_path, replica, IndexOptions::default())
                    .unwrap();
                index.adopt_verified_record(&record).unwrap();
                assert_eq!(index.sync_records().unwrap()[0].version.get(replica), 0);
                drop(index);
                let participant = ShareService::open(
                    temp.path().join("participant"),
                    NetworkMode::DirectOnly,
                    None,
                )
                .await
                .unwrap();
                let proof =
                    LegacyProof::create(&key, &old, participant.endpoint_id(), replica).unwrap();
                let grant = participant.enroll(&key, Some(proof)).await.unwrap();
                assert_eq!(grant.replica, replica);
                let index =
                    LocalIndex::open(&local, &index_path, grant.replica, IndexOptions::default())
                        .unwrap();
                assert_eq!(index.sync_records().unwrap(), vec![record]);
                drop(index);
                drop(session);
                drop(share);
                owner.shutdown().await.unwrap();
                first.shutdown().await.unwrap();
                participant.shutdown().await.unwrap();
            })
        },
    );
}

#[test]
fn managed_endpoint_rejects_legacy_alpns_and_raw_cross_share_requests() {
    isolated(
        "managed_endpoint_rejects_legacy_alpns_and_raw_cross_share_requests",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                use serde::{Deserialize, Serialize};
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                #[derive(Serialize)]
                struct Hello {
                    version: u8,
                    share_id: ShareId,
                    operation: Operation,
                }
                #[derive(Serialize)]
                enum Operation {
                    Validate(ShareTicket),
                    Enroll {
                        ticket: ShareTicket,
                        proof: Option<Box<LegacyProof>>,
                    },
                    Session,
                }
                #[derive(Deserialize)]
                enum Reply {
                    Validated(TicketPreview),
                    Enrolled(Membership),
                    Accepted,
                    Error(ShareError),
                }
                async fn exchange(
                    endpoint: &iroh::Endpoint,
                    address: iroh::EndpointAddr,
                    hello: Hello,
                ) -> Reply {
                    let connection = endpoint.connect(address, ALPN_V3).await.unwrap();
                    let (mut send, mut receive) = connection.open_bi().await.unwrap();
                    let bytes = postcard::to_stdvec(&hello).unwrap();
                    send.write_u32(bytes.len() as u32).await.unwrap();
                    send.write_all(&bytes).await.unwrap();
                    send.finish().unwrap();
                    let size = receive.read_u32().await.unwrap();
                    assert!(size < 16384);
                    let mut bytes = vec![0; size as usize];
                    receive.read_exact(&mut bytes).await.unwrap();
                    let reply = postcard::from_bytes(&bytes).unwrap();
                    connection.close(0u8.into(), b"test complete");
                    reply
                }
                let temp = tempfile::tempdir().unwrap();
                let owner =
                    ShareService::open(temp.path().join("owner"), NetworkMode::DirectOnly, None)
                        .await
                        .unwrap();
                let a = owner
                    .create_owned_share(
                        "A".into(),
                        temp.path().join("A"),
                        temp.path().join("A-state"),
                        None,
                        0,
                    )
                    .await
                    .unwrap();
                let b = owner
                    .create_owned_share(
                        "B".into(),
                        temp.path().join("B"),
                        temp.path().join("B-state"),
                        None,
                        0,
                    )
                    .await
                    .unwrap();
                std::fs::write(temp.path().join("B/secret"), b"B secret").unwrap();
                let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
                    .bind()
                    .await
                    .unwrap();
                for alpn in [deltaweave_net::ALPN_V1, deltaweave_net::ALPN_V2] {
                    assert!(endpoint.connect(owner.endpoint_addr(), alpn).await.is_err());
                }
                let ticket = a
                    .issue_key(Permission::ReadOnly, None, owner.endpoint_addr())
                    .unwrap();
                let reply = exchange(
                    &endpoint,
                    owner.endpoint_addr(),
                    Hello {
                        version: 3,
                        share_id: a.config().share_id,
                        operation: Operation::Validate(ticket.clone()),
                    },
                )
                .await;
                let Reply::Validated(preview) = reply else {
                    panic!("signed preview failed")
                };
                assert_eq!(preview.permission, Permission::ReadOnly);
                assert!(a.members().unwrap().is_empty());
                let reply = exchange(
                    &endpoint,
                    owner.endpoint_addr(),
                    Hello {
                        version: 3,
                        share_id: b.config().share_id,
                        operation: Operation::Enroll {
                            ticket: ticket.clone(),
                            proof: None,
                        },
                    },
                )
                .await;
                assert!(matches!(reply, Reply::Error(ShareError::InvalidTicket)));
                assert!(b.members().unwrap().is_empty());
                let reply = exchange(
                    &endpoint,
                    owner.endpoint_addr(),
                    Hello {
                        version: 3,
                        share_id: a.config().share_id,
                        operation: Operation::Enroll {
                            ticket,
                            proof: None,
                        },
                    },
                )
                .await;
                let Reply::Enrolled(grant) = reply else {
                    panic!("enrollment failed")
                };
                assert_eq!(grant.endpoint, endpoint.id());
                assert_eq!(grant.permission, Permission::ReadOnly);
                let reply = exchange(
                    &endpoint,
                    owner.endpoint_addr(),
                    Hello {
                        version: 3,
                        share_id: b.config().share_id,
                        operation: Operation::Session,
                    },
                )
                .await;
                assert!(matches!(reply, Reply::Error(ShareError::NotMember)));
                assert_eq!(
                    std::fs::read(temp.path().join("B/secret")).unwrap(),
                    b"B secret"
                );
                endpoint.close().await;
                drop(a);
                drop(b);
                owner.shutdown().await.unwrap();
            })
        },
    );
}

#[test]
fn one_device_owns_joins_and_keeps_legacy_endpoint_independent() {
    isolated(
        "one_device_owns_joins_and_keeps_legacy_endpoint_independent",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                use deltaweave_net::{PeerPolicy, ServerConfig, start_server};
                let temp = tempfile::tempdir().unwrap();
                let device =
                    ShareService::open(temp.path().join("device"), NetworkMode::DirectOnly, None)
                        .await
                        .unwrap();
                let other =
                    ShareService::open(temp.path().join("other"), NetworkMode::DirectOnly, None)
                        .await
                        .unwrap();
                let a = device
                    .create_owned_share(
                        "A".into(),
                        temp.path().join("A"),
                        temp.path().join("A-state"),
                        None,
                        0,
                    )
                    .await
                    .unwrap();
                let b = other
                    .create_owned_share(
                        "B".into(),
                        temp.path().join("B"),
                        temp.path().join("B-state"),
                        None,
                        0,
                    )
                    .await
                    .unwrap();
                std::fs::write(temp.path().join("B/file"), b"remote").unwrap();
                let legacy = start_server(ServerConfig {
                    secret_key: iroh::SecretKey::generate(),
                    destination_root: temp.path().join("C"),
                    state_root: temp.path().join("C-state"),
                    peer_policy: PeerPolicy::AnyAuthenticated,
                    network_mode: NetworkMode::DirectOnly,
                    bind_address: None,
                    max_connections: 8,
                    min_free_space_bytes: 0,
                })
                .await
                .unwrap();
                assert_ne!(device.endpoint_id(), legacy.endpoint_addr().id);
                let ticket = b
                    .issue_key(Permission::ReadOnly, None, other.endpoint_addr())
                    .unwrap();
                let grant = device.enroll(&ticket, None).await.unwrap();
                assert!(
                    device.load_owned_share(grant.share_id).await.is_err(),
                    "joined share became a snapshot server"
                );
                let session = device.open_session(grant.owner, grant.share_id).unwrap();
                let empty = deltaweave_reconcile::MerkleTree::from_records(Vec::new()).unwrap();
                a.pause().await;
                assert_eq!(
                    session.fetch_snapshot(&empty).await.unwrap().record_count,
                    1
                );
                session.close().await;
                assert_eq!(device.endpoint_id(), a.config().owner);
                a.resume();
                let receiver = other
                    .enroll(
                        &a.issue_key(Permission::ReadOnly, None, device.endpoint_addr())
                            .unwrap(),
                        None,
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    other
                        .open_session(receiver.owner, receiver.share_id)
                        .unwrap()
                        .fetch_snapshot(&empty)
                        .await
                        .unwrap()
                        .record_count,
                    0
                );
                assert!(legacy.inventory().is_ok());
                drop(a);
                drop(b);
                legacy.shutdown().await.unwrap();
                device.shutdown().await.unwrap();
                other.shutdown().await.unwrap();
            })
        },
    );
}

#[test]
fn key_revocation_and_member_tombstones_survive_actual_restart() {
    isolated(
        "key_revocation_and_member_tombstones_survive_actual_restart",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let temp = tempfile::tempdir().unwrap();
                let owner =
                    ShareService::open(temp.path().join("owner"), NetworkMode::DirectOnly, None)
                        .await
                        .unwrap();
                let bind = owner
                    .endpoint_addr()
                    .ip_addrs()
                    .find(|ip| ip.is_ipv4())
                    .copied()
                    .unwrap();
                let member =
                    ShareService::open(temp.path().join("member"), NetworkMode::DirectOnly, None)
                        .await
                        .unwrap();
                let share = owner
                    .create_owned_share(
                        "Files".into(),
                        temp.path().join("root"),
                        temp.path().join("state"),
                        None,
                        0,
                    )
                    .await
                    .unwrap();
                let old = share
                    .issue_key(Permission::ReadOnly, None, owner.endpoint_addr())
                    .unwrap();
                let grant = member.enroll(&old, None).await.unwrap();
                share.revoke_key(old.preview().invitation_id).unwrap();
                let empty = deltaweave_reconcile::MerkleTree::from_records(Vec::new()).unwrap();
                assert!(
                    member
                        .open_session(grant.owner, grant.share_id)
                        .unwrap()
                        .fetch_snapshot(&empty)
                        .await
                        .is_ok()
                );
                drop(share);
                owner.shutdown().await.unwrap();
                member.shutdown().await.unwrap();
                let owner = ShareService::open(
                    temp.path().join("owner"),
                    NetworkMode::DirectOnly,
                    Some(bind),
                )
                .await
                .unwrap();
                let share = owner.load_owned_share(grant.share_id).await.unwrap();
                let member =
                    ShareService::open(temp.path().join("member"), NetworkMode::DirectOnly, None)
                        .await
                        .unwrap();
                assert_eq!(member.relationships().unwrap()[0].membership, grant);
                assert!(member.validate_ticket(&old).await.is_err());
                assert!(
                    member
                        .open_session(grant.owner, grant.share_id)
                        .unwrap()
                        .fetch_snapshot(&empty)
                        .await
                        .is_ok()
                );
                share.revoke_member(grant.endpoint).await.unwrap();
                let active = share
                    .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
                    .unwrap();
                assert!(member.enroll(&active, None).await.is_err());
                let fresh =
                    ShareService::open(temp.path().join("fresh"), NetworkMode::DirectOnly, None)
                        .await
                        .unwrap();
                assert!(fresh.enroll(&active, None).await.is_ok());
                drop(share);
                owner.shutdown().await.unwrap();
                let owner = ShareService::open(
                    temp.path().join("owner"),
                    NetworkMode::DirectOnly,
                    Some(bind),
                )
                .await
                .unwrap();
                let share = owner.load_owned_share(grant.share_id).await.unwrap();
                let error = member
                    .open_session(grant.owner, grant.share_id)
                    .unwrap()
                    .fetch_snapshot(&empty)
                    .await
                    .unwrap_err();
                assert_eq!(
                    error.downcast_ref::<ShareError>(),
                    Some(&ShareError::MemberRevoked)
                );
                drop(share);
                owner.shutdown().await.unwrap();
                member.shutdown().await.unwrap();
                fresh.shutdown().await.unwrap();
            })
        },
    );
}

async fn revoke_during_transfer(phase: &'static str) {
    use std::sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    };
    let temp = tempfile::tempdir().unwrap();
    let owner = ShareService::open(temp.path().join("owner"), NetworkMode::DirectOnly, None)
        .await
        .unwrap();
    let member = ShareService::open(temp.path().join("member"), NetworkMode::DirectOnly, None)
        .await
        .unwrap();
    let share = owner
        .create_owned_share(
            "Files".into(),
            temp.path().join("root"),
            temp.path().join("state"),
            None,
            0,
        )
        .await
        .unwrap();
    std::fs::write(temp.path().join("root/file"), b"protected").unwrap();
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
    let empty = deltaweave_reconcile::MerkleTree::from_records(Vec::new()).unwrap();
    let mut record = session.fetch_snapshot(&empty).await.unwrap().records[0].clone();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(Barrier::new(2));
    let first = AtomicBool::new(true);
    let e = entered.clone();
    let r = release.clone();
    share.set_observer(Some(deltaweave_net::TransferObserver::new(move |event| {
        if event.phase == phase && first.swap(false, Ordering::SeqCst) {
            e.notify_one();
            r.wait();
        }
    })));
    let transfer = if phase == "receiving" {
        let source = temp.path().join("source");
        std::fs::write(&source, b"incoming unique bytes").unwrap();
        record.size = 21;
        record.content_hash = Some(Hash32::digest(b"incoming unique bytes"));
        record.version.increment(grant.replica).unwrap();
        tokio::spawn(async move {
            session
                .push_record(source, record, deltaweave_core::ChunkingProfile::DEFAULT)
                .await
                .map(|_| ())
        })
    } else {
        let store =
            Arc::new(deltaweave_store::Store::open(temp.path().join("pull-store")).unwrap());
        tokio::spawn(async move { session.pull_record(record, store).await.map(|_| ()) })
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), entered.notified())
        .await
        .expect("transfer never reached barrier");
    let revoked = share.clone();
    let revoke =
        tokio::task::spawn_local(async move { revoked.revoke_member(grant.endpoint).await });
    while share.members().unwrap()[0].revoked_at.is_none() {
        tokio::task::yield_now().await;
    }
    let returned_early = revoke.is_finished();
    tokio::task::spawn_blocking(move || release.wait())
        .await
        .unwrap();
    revoke.await.unwrap().unwrap();
    assert!(
        !returned_early,
        "revocation returned before tracked transfer drained"
    );
    assert!(transfer.await.unwrap().is_err());
    assert_eq!(
        std::fs::read(temp.path().join("root/file")).unwrap(),
        b"protected"
    );
    assert!(share.provenance().unwrap().is_empty());
    drop(share);
    owner.shutdown().await.unwrap();
    member.shutdown().await.unwrap();
}
#[test]
fn revocation_drains_chunk_writers_before_returning() {
    isolated("revocation_drains_chunk_writers_before_returning", || {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        tokio::task::LocalSet::new().block_on(&runtime, revoke_during_transfer("receiving"))
    });
}
#[test]
fn revocation_drains_content_senders_before_returning() {
    isolated("revocation_drains_content_senders_before_returning", || {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        tokio::task::LocalSet::new().block_on(&runtime, revoke_during_transfer("sending"))
    });
}

#[test]
fn active_share_missing_index_is_not_silently_reset_on_restart() {
    isolated(
        "active_share_missing_index_is_not_silently_reset_on_restart",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let temp = tempfile::tempdir().unwrap();
                let owner =
                    ShareService::open(temp.path().join("owner"), NetworkMode::DirectOnly, None)
                        .await
                        .unwrap();
                let share = owner
                    .create_owned_share(
                        "Files".into(),
                        temp.path().join("root"),
                        temp.path().join("state"),
                        None,
                        0,
                    )
                    .await
                    .unwrap();
                let id = share.config().share_id;
                drop(share);
                owner.shutdown().await.unwrap();
                std::fs::remove_file(temp.path().join("state/index.redb")).unwrap();
                let owner =
                    ShareService::open(temp.path().join("owner"), NetworkMode::DirectOnly, None)
                        .await
                        .unwrap();
                let result = owner.load_owned_share(id).await;
                let rejected = result.is_err();
                drop(result);
                owner.shutdown().await.unwrap();
                assert!(rejected, "active managed index was silently recreated");
                assert!(!temp.path().join("state/index.redb").exists());
            })
        },
    );
}

#[test]
fn incomplete_creation_resumes_from_durable_intent() {
    isolated("incomplete_creation_resumes_from_durable_intent", || {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join("root");
            let state = temp.path().join("state");
            std::fs::create_dir(&root).unwrap();
            std::fs::create_dir(&state).unwrap();
            std::fs::write(root.join("file"), b"retained").unwrap();
            std::fs::write(state.join("chunks"), b"blocks private store creation").unwrap();
            let owner =
                ShareService::open(temp.path().join("owner"), NetworkMode::DirectOnly, None)
                    .await
                    .unwrap();
            assert!(
                owner
                    .create_owned_share("Files".into(), root.clone(), state.clone(), None, 0)
                    .await
                    .is_err()
            );
            let configs = owner.owned_configs().unwrap();
            assert_eq!(configs.len(), 1);
            let config = configs[0].clone();
            owner.shutdown().await.unwrap();
            std::fs::remove_file(state.join("chunks")).unwrap();
            let owner =
                ShareService::open(temp.path().join("owner"), NetworkMode::DirectOnly, None)
                    .await
                    .unwrap();
            let share = owner.load_owned_share(config.share_id).await.unwrap();
            assert_eq!(share.config(), &config);
            assert_eq!(share.inventory().unwrap().files, 1);
            assert_eq!(std::fs::read(root.join("file")).unwrap(), b"retained");
            assert!(
                share
                    .issue_key(Permission::ReadOnly, None, owner.endpoint_addr())
                    .is_ok()
            );
            drop(share);
            owner.shutdown().await.unwrap();
        })
    });
}

fn tree_bytes(root: &std::path::Path) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    fn visit(
        base: &std::path::Path,
        path: &std::path::Path,
        out: &mut std::collections::BTreeMap<std::path::PathBuf, Vec<u8>>,
    ) {
        for item in std::fs::read_dir(path).unwrap() {
            let item = item.unwrap();
            let path = item.path();
            if item.file_type().unwrap().is_symlink() {
                continue;
            }
            if path.is_dir() {
                out.insert(path.strip_prefix(base).unwrap().into(), Vec::new());
                visit(base, &path, out);
            } else {
                out.insert(
                    path.strip_prefix(base).unwrap().into(),
                    std::fs::read(&path).unwrap(),
                );
            }
        }
    }
    let mut out = std::collections::BTreeMap::new();
    visit(root, root, &mut out);
    out
}

#[test]
fn private_state_and_denied_descendants_never_enter_served_namespaces() {
    isolated(
        "private_state_and_denied_descendants_never_enter_served_namespaces",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let temp = tempfile::tempdir().unwrap();
                let base = temp.path();
                let owner =
                    ShareService::open(base.join("private/device"), NetworkMode::DirectOnly, None)
                        .await
                        .unwrap();
                let initial = owner.owned_configs().unwrap();
                let private_before = tree_bytes(&base.join("private"));
                for (i, root) in [
                    base.join("private/device"),
                    base.join("private"),
                    base.join("private/device/new/deep"),
                ]
                .into_iter()
                .enumerate()
                {
                    let state = base.join(format!("denied-state-{i}"));
                    assert!(
                        owner
                            .create_owned_share("Denied".into(), root, state.clone(), None, 0)
                            .await
                            .is_err(),
                        "device namespace admitted"
                    );
                    assert!(!state.exists());
                    assert_eq!(owner.owned_configs().unwrap(), initial);
                    assert!(
                        tree_bytes(&base.join("private")) == private_before,
                        "protected device tree changed"
                    );
                }
                let share = owner
                    .create_owned_share(
                        "A".into(),
                        base.join("public"),
                        base.join("a-state"),
                        None,
                        0,
                    )
                    .await
                    .unwrap();
                std::fs::write(base.join("public/visible"), b"public bytes").unwrap();
                let before = tree_bytes(&base.join("public"));
                let configs = owner.owned_configs().unwrap();
                let owner_catalog = std::fs::read(base.join("private/device/shares.redb")).unwrap();
                for (root, state) in [
                    (base.join("other-root"), base.join("public/b-state")),
                    (base.join("public/new/deep"), base.join("denied-state")),
                ] {
                    assert!(
                        owner
                            .create_owned_share(
                                "Denied".into(),
                                root.clone(),
                                state.clone(),
                                None,
                                0
                            )
                            .await
                            .is_err()
                    );
                    assert!(!root.exists());
                    assert!(!state.exists());
                    assert_eq!(owner.owned_configs().unwrap(), configs);
                    assert!(
                        std::fs::read(base.join("private/device/shares.redb")).unwrap()
                            == owner_catalog,
                        "denied share changed device catalog"
                    );
                    assert!(tree_bytes(&base.join("public")) == before);
                }
                assert!(
                    ShareService::open(
                        base.join("public/other-device"),
                        NetworkMode::DirectOnly,
                        None
                    )
                    .await
                    .is_err()
                );
                assert!(!base.join("public/other-device").exists());
                let other =
                    ShareService::open(base.join("other-device"), NetworkMode::DirectOnly, None)
                        .await
                        .unwrap();
                let b = other
                    .create_owned_share(
                        "B".into(),
                        base.join("b-public"),
                        base.join("b-private/store"),
                        None,
                        0,
                    )
                    .await
                    .unwrap();
                std::fs::write(
                    base.join("b-private/store/hidden"),
                    b"other share private bytes",
                )
                .unwrap();
                let b_before = tree_bytes(&base.join("b-private"));
                assert!(
                    owner
                        .create_owned_share(
                            "Denied".into(),
                            base.join("b-private"),
                            base.join("denied-reverse-state"),
                            None,
                            0
                        )
                        .await
                        .is_err()
                );
                assert!(!base.join("denied-reverse-state").exists());
                assert!(tree_bytes(&base.join("b-private")) == b_before);
                #[cfg(unix)]
                {
                    std::os::unix::fs::symlink(base.join("private"), base.join("alias-private"))
                        .unwrap();
                    std::os::unix::fs::symlink(base.join("public"), base.join("alias-public"))
                        .unwrap();
                    assert!(
                        other
                            .create_owned_share(
                                "Denied".into(),
                                base.join("alias-private/device"),
                                base.join("alias-denied-state"),
                                None,
                                0
                            )
                            .await
                            .is_err()
                    );
                    assert!(
                        other
                            .create_owned_share(
                                "Denied".into(),
                                base.join("alias-public/missing/deep"),
                                base.join("alias-desc-state"),
                                None,
                                0
                            )
                            .await
                            .is_err()
                    );
                    assert!(!base.join("alias-denied-state").exists());
                    assert!(!base.join("alias-desc-state").exists());
                    assert!(!base.join("public/missing").exists());
                }
                let ticket = share
                    .issue_key(Permission::ReadOnly, None, owner.endpoint_addr())
                    .unwrap();
                let grant = other.enroll(&ticket, None).await.unwrap();
                assert!(
                    other
                        .admit_member_root(grant.owner, grant.share_id, base.join("private"))
                        .is_err()
                );
                assert!(
                    other
                        .admit_member_root(
                            grant.owner,
                            grant.share_id,
                            base.join("b-private/store/new")
                        )
                        .is_err()
                );
                assert!(!base.join("b-private/store/new").exists());
                let session = other.open_session(grant.owner, grant.share_id).unwrap();
                let empty = deltaweave_reconcile::MerkleTree::from_records(Vec::new()).unwrap();
                let snapshot = session.fetch_snapshot(&empty).await.unwrap();
                assert_eq!(snapshot.records.len(), 1);
                let visible = snapshot.records[0].clone();
                assert_eq!(visible.path.as_str(), "visible");
                let store = std::sync::Arc::new(
                    deltaweave_store::Store::open(base.join("received-store")).unwrap(),
                );
                let pull = session
                    .pull_record(visible.clone(), store.clone())
                    .await
                    .unwrap();
                let output = base.join("received");
                std::fs::create_dir(&output).unwrap();
                store
                    .materialize(&pull.manifest, &visible.path, &output)
                    .unwrap();
                assert_eq!(
                    std::fs::read(output.join("visible")).unwrap(),
                    b"public bytes"
                );
                for (path, secret) in [
                    (
                        "device/device.key",
                        std::fs::read(base.join("private/device/device.key")).unwrap(),
                    ),
                    ("b-state/hidden", b"other share private bytes".to_vec()),
                ] {
                    let mut forged = visible.clone();
                    forged.path = deltaweave_core::WirePath::new(path).unwrap();
                    forged.size = secret.len() as u64;
                    forged.content_hash = Some(Hash32::digest(&secret));
                    assert!(
                        session.pull_record(forged, store.clone()).await.is_err(),
                        "private bytes were served"
                    );
                    assert!(!tree_bytes(&output).values().any(|bytes| bytes == &secret));
                }
                assert!(tree_bytes(&base.join("public")) == before);
                drop(session);
                drop(b);
                drop(share);
                other.shutdown().await.unwrap();
                owner.shutdown().await.unwrap();
            });
        },
    );
}
