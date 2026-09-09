use deltaweave_core::ChunkingProfile;
use deltaweave_net::{NetworkMode, TransferEvent, TransferObserver, share::*};
use deltaweave_sync::{ManagedSyncConfig, ManagedSyncEngine};
use std::collections::BTreeSet;
use std::fs;
use std::sync::{Arc, Mutex};

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

fn unique_payload() -> Vec<u8> {
    let mut payload = Vec::with_capacity(8 * 1024 * 1024);
    for block in 0..8192_u64 {
        let mut state = block ^ 0x9e37_79b9_7f4a_7c15;
        for _ in 0..1024 {
            state ^= state << 7;
            state ^= state >> 9;
            state ^= state << 8;
            payload.push((state >> 24) as u8);
        }
    }
    payload
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
fn managed_read_only_uses_owner_and_member_suppliers_for_real_chunks() {
    isolated(
        "managed_read_only_uses_owner_and_member_suppliers_for_real_chunks",
        || {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(run_managed_read_only_two_suppliers(false));
        },
    );
}

#[test]
fn managed_read_only_partial_swarm_emits_single_fallback_event() {
    isolated(
        "managed_read_only_partial_swarm_emits_single_fallback_event",
        || {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(run_managed_read_only_two_suppliers(true));
        },
    );
}

#[test]
fn managed_read_only_mid_transfer_supplier_loss_is_observed() {
    isolated(
        "managed_read_only_mid_transfer_supplier_loss_is_observed",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(
                run_managed_read_only_two_suppliers_with_options(false, true),
            );
        },
    );
}

async fn run_managed_read_only_two_suppliers(force_owner_fallback: bool) {
    run_managed_read_only_two_suppliers_with_options(force_owner_fallback, false).await;
}

async fn run_managed_read_only_two_suppliers_with_options(
    force_owner_fallback: bool,
    interrupt_provider: bool,
) {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path();
    let owner = ShareService::open(base.join("owner-device"), NetworkMode::DirectOnly, None)
        .await
        .unwrap();
    let provider_path = base.join("provider-device");
    let provider = ShareService::open(&provider_path, NetworkMode::DirectOnly, None)
        .await
        .unwrap();
    let consumer_path = base.join("consumer-device");
    let consumer = ShareService::open(&consumer_path, NetworkMode::DirectOnly, None)
        .await
        .unwrap();
    let share_events = Arc::new(Mutex::new(Vec::<ShareTransferEvent>::new()));
    let observed_share_events = Arc::clone(&share_events);
    let share_observer = ShareTransferObserver::new(move |event| {
        observed_share_events
            .lock()
            .expect("share observer lock")
            .push(event);
    });
    owner.set_share_observer(Some(share_observer.clone()));
    provider.set_share_observer(Some(share_observer.clone()));
    consumer.set_share_observer(Some(share_observer));
    let owned = owner
        .create_owned_share(
            "two suppliers".into(),
            base.join("owner-root"),
            base.join("owner-state"),
            None,
            0,
        )
        .await
        .unwrap();
    // This deterministic 8 MiB payload produces multiple default
    // FastCDC chunks, allowing the managed scheduler to assign
    // distinct subsets to both authenticated providers.
    let payload = unique_payload();
    let provider_grant = provider
        .enroll(
            &owned
                .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
                .unwrap(),
            None,
        )
        .await
        .unwrap();
    let provider_engine = ManagedSyncEngine::open(
        &provider,
        provider_grant.owner,
        provider_grant.share_id,
        config(base, "provider"),
    )
    .unwrap();
    fs::write(base.join("provider-root/payload.bin"), &payload).unwrap();
    let provider_report = provider_engine.sync_read_write(None).await.unwrap();
    assert!(provider_report.pushed_bytes > 0);

    // The owner's inventory scan supplies authoritative metadata.  The
    // authenticated Manifest request below warms the already-open owner CAS;
    // the RW upload above gives the member supplier its own complete CAS, so
    // both selected suppliers are real sources for the subsequent RO transfer.
    let probe = provider
        .open_session(provider_grant.owner, provider_grant.share_id)
        .unwrap();
    let probe_snapshot = probe
        .fetch_authoritative_snapshot(
            &deltaweave_reconcile::MerkleTree::from_records(Vec::new()).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(probe_snapshot.records.len(), 1);
    let probe_record = probe_snapshot.records[0].clone();
    let probe_manifest = probe
        .request_manifest(&probe_snapshot.token, &probe_record)
        .await
        .unwrap();
    assert_eq!(
        probe_manifest.manifest.file_hash,
        probe_record.content_hash.unwrap()
    );
    let probe_hashes: BTreeSet<_> = probe_manifest
        .manifest
        .chunks
        .iter()
        .map(|chunk| chunk.hash)
        .collect();
    assert!(probe_hashes.len() >= 2);
    assert!(probe_manifest.manifest.chunks.len() <= 64);
    let mut probe_hashes: Vec<_> = probe_hashes.into_iter().collect();
    probe_hashes.sort();
    probe
        .request_swarm_grant(
            owner.endpoint_id(),
            &probe_snapshot.token,
            &probe_manifest,
            &probe_hashes,
        )
        .await
        .unwrap();
    probe.close().await;
    let provider_peer_id = provider.endpoint_id();
    let mut provider_holder = Some(provider);
    let mut provider_engine_holder = Some(provider_engine);
    let interrupted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let provider_stop = if interrupt_provider {
        let stop = Arc::new(tokio::sync::Notify::new());
        let observed = Arc::clone(&share_events);
        let interrupted_by_observer = Arc::clone(&interrupted);
        let stop_observer = stop.clone();
        let observer = ShareTransferObserver::new(move |event| {
            observed
                .lock()
                .expect("share observer lock")
                .push(event.clone());
            if event.phase == SharePhase::Swarm
                && event.direction == TransferDirection::Outbound
                && event.bytes > 0
                && !interrupted_by_observer.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                // Keep the provider operation in-flight long enough for the
                // independent service shutdown to close its connection.  A
                // positive outbound event is the byte-level interruption
                // marker; this delay is only a scheduling barrier.
                stop_observer.notify_one();
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        });
        provider_holder
            .as_ref()
            .expect("provider service")
            .set_share_observer(Some(observer));
        let provider = provider_holder.take().expect("provider service");
        let provider_engine = provider_engine_holder
            .take()
            .expect("provider managed engine");
        Some(tokio::spawn(async move {
            stop.notified().await;
            provider.shutdown().await.unwrap();
            provider_engine.shutdown().await.unwrap();
        }))
    } else {
        None
    };
    let deleted_owner_chunk = if force_owner_fallback {
        // Force one owner-assigned chunk to be absent after the
        // authenticated manifest response.  The member supplier
        // still has the complete CAS, so this makes one real
        // swarm assignment partial while the managed share/3
        // fallback must supply the missing bytes. The operation
        // ID guard limits deletion to this exact response.
        let fallback_hash = *probe_hashes.first().expect("manifest has a chunk");
        let owner_chunk_hex = fallback_hash.to_hex();
        let owner_chunk_path = base
            .join("owner-state/chunks")
            .join(&owner_chunk_hex[..2])
            .join(&owner_chunk_hex[2..]);
        assert!(
            owner_chunk_path.is_file(),
            "selected owner CAS chunk exists"
        );
        let manifest_operation = Arc::new(Mutex::new(None::<[u8; 16]>));
        let deleted_owner_chunk = Arc::new(Mutex::new(false));
        let consumer_events = Arc::clone(&share_events);
        let operation_for_observer = Arc::clone(&manifest_operation);
        let deleted_for_observer = Arc::clone(&deleted_owner_chunk);
        let consumer_delete_observer = ShareTransferObserver::new(move |event| {
            consumer_events
                .lock()
                .expect("share observer lock")
                .push(event.clone());
            if event.phase == SharePhase::Manifest && event.grant.is_none() {
                *operation_for_observer
                    .lock()
                    .expect("manifest operation lock") = Some(event.operation_id);
            } else if event.phase == SharePhase::Done
                && event.grant.is_none()
                && operation_for_observer
                    .lock()
                    .expect("manifest operation lock")
                    .take()
                    == Some(event.operation_id)
            {
                let removed = fs::remove_file(&owner_chunk_path).is_ok();
                *deleted_for_observer.lock().expect("deleted chunk lock") = removed;
            }
        });
        // The manifest control exchange is observed on the
        // consumer session. The owner handler has no local
        // client-side ShareEventGuard, so install the hook there.
        consumer.set_share_observer(Some(consumer_delete_observer));
        Some(deleted_owner_chunk)
    } else {
        None
    };
    let consumer_grant = consumer
        .enroll(
            &owned
                .issue_key(Permission::ReadOnly, None, owner.endpoint_addr())
                .unwrap(),
            None,
        )
        .await
        .unwrap();
    let consumer_engine = ManagedSyncEngine::open(
        &consumer,
        consumer_grant.owner,
        consumer_grant.share_id,
        config(base, "consumer"),
    )
    .unwrap();
    let events = Arc::new(Mutex::new(Vec::<TransferEvent>::new()));
    let observed = Arc::clone(&events);
    let observer = TransferObserver::new(move |event| {
        observed.lock().expect("observer lock").push(event);
    });
    let share_event_start = share_events.lock().expect("share observer lock").len();
    let first_result = consumer_engine.sync_read_only(Some(observer)).await;
    let mut provider_stop = provider_stop;
    if interrupt_provider {
        assert!(
            interrupted.load(std::sync::atomic::Ordering::SeqCst),
            "provider shutdown was triggered only after a positive payload event"
        );
        if let Some(stop_task) = provider_stop.take() {
            tokio::time::timeout(std::time::Duration::from_secs(20), stop_task)
                .await
                .expect("provider shutdown task must finish")
                .expect("provider shutdown task must not panic");
        }
    }
    let first_report = match first_result {
        Ok(report) => {
            assert!(
                !interrupt_provider,
                "provider shutdown must interrupt the managed transfer"
            );
            Some(report)
        }
        Err(error) if interrupt_provider => {
            // A provider connection loss after positive bytes is not proof
            // that either side drained its activation.  The exact grant must
            // remain pending across recovery until the owner receives the
            // missing endpoint acknowledgement; this test must never turn
            // that unknown state into a successful public apply.
            assert_eq!(
                ShareError::classify(&error),
                ShareError::RevocationPending,
                "mid-transfer provider loss remains a durable blocker"
            );
            {
                let events = share_events.lock().expect("share observer lock");
                assert!(events[share_event_start..].iter().any(|event| {
                    event.phase == SharePhase::Swarm
                        && event.direction == TransferDirection::Outbound
                        && event.peer == consumer.endpoint_id()
                        && event.bytes > 0
                        && event.grant.is_some()
                }));
            }

            // Reopen the same member service through recovery-only mode. It
            // must have no heartbeat or supplier registration and must return
            // the same pending classification without touching the public
            // namespace. A later owner-side bilateral drain is required
            // before any retry can materialize the retained private CAS.
            consumer_engine.shutdown().await.unwrap();
            consumer.shutdown().await.unwrap();
            let consumer = ShareService::open(&consumer_path, NetworkMode::DirectOnly, None)
                .await
                .unwrap();
            let recovery = ManagedSyncEngine::open_recovery(
                &consumer,
                owner.endpoint_id(),
                provider_grant.share_id,
                config(base, "consumer"),
            )
            .unwrap();
            let recovery_error = recovery
                .recover_pending()
                .await
                .expect_err("unknown bilateral drain must remain pending");
            assert_eq!(
                ShareError::classify(&recovery_error),
                ShareError::RevocationPending
            );
            assert!(!base.join("consumer-root/payload.bin").exists());
            recovery.shutdown().await.unwrap();
            consumer.shutdown().await.unwrap();
            owner.shutdown().await.unwrap();
            return;
        }
        Err(_) => panic!("non-interrupted transfer failed"),
    };
    let report = first_report.expect("non-interrupted transfer has a report");
    assert_eq!(report.status, "pass");
    assert!(report.pulled_bytes > 0);
    if let Some(deleted_owner_chunk) = deleted_owner_chunk {
        assert!(
            *deleted_owner_chunk.lock().expect("deleted chunk lock"),
            "owner manifest callback removed the selected owner CAS chunk"
        );
        let events = events.lock().expect("observer lock");
        let fallback: Vec<_> = events
            .iter()
            .filter(|event| event.phase == "file_received_fallback")
            .collect();
        assert_eq!(fallback.len(), 1, "fallback payload is emitted once");
        assert!(fallback[0].bytes > 0);
        assert_eq!(fallback[0].path.as_deref(), Some("payload.bin"));
        assert_eq!(fallback[0].direction.as_deref(), Some("pull"));
        let expected_peer = owner.endpoint_id().to_string();
        assert_eq!(fallback[0].peer.as_deref(), Some(expected_peer.as_str()));
        let aggregate: Vec<_> = events
            .iter()
            .filter(|event| event.phase == "file_received")
            .collect();
        assert_eq!(aggregate.len(), 1, "aggregate receive is emitted once");
        assert_eq!(aggregate[0].bytes, report.pulled_bytes);
        assert!(fallback[0].bytes < aggregate[0].bytes);
    }
    let fallback_bytes = events
        .lock()
        .expect("observer lock")
        .iter()
        .filter(|event| event.phase == "file_received_fallback")
        .map(|event| event.bytes)
        .sum::<u64>();
    if !force_owner_fallback {
        assert_eq!(fallback_bytes, 0, "swarm-only transfer emits no fallback");
    }
    {
        let events = events.lock().expect("observer lock");
        let starts: Vec<_> = events
            .iter()
            .enumerate()
            .filter(|(_, event)| event.phase == "swarm_provider_started")
            .collect();
        let verified: Vec<_> = events
            .iter()
            .enumerate()
            .filter(|(_, event)| event.phase == "swarm_provider_verified" && event.bytes > 0)
            .collect();
        let started_peers: BTreeSet<_> = starts
            .iter()
            .filter_map(|(_, event)| event.peer.as_deref())
            .collect();
        let verified_peers: BTreeSet<_> = verified
            .iter()
            .filter_map(|(_, event)| event.peer.as_deref())
            .collect();
        assert!(started_peers.len() >= 2);
        assert!(verified_peers.len() >= 2);
        assert!(starts.len() >= 2);
        if !force_owner_fallback {
            assert!(verified.first().is_some_and(|(first_verified, _)| {
                starts.iter().all(|(started, _)| started < first_verified)
            }));
        }
    }
    {
        let share_events = share_events.lock().expect("share observer lock");
        let events = &share_events[share_event_start..];
        let inbound_swarm = |event: &&ShareTransferEvent| {
            event.phase == SharePhase::Swarm
                && event.direction == TransferDirection::Inbound
                && event.grant.is_some()
        };
        let provider_peers: BTreeSet<_> = events
            .iter()
            .filter(|event| inbound_swarm(event))
            .filter(|event| event.peer == provider_peer_id || event.peer == owner.endpoint_id())
            .filter(|event| event.bytes > 0)
            .map(|event| event.peer)
            .collect();
        if interrupt_provider {
            assert!(
                provider_peers.contains(&provider_peer_id),
                "the interrupted member supplier delivered bytes before shutdown"
            );
        } else {
            assert_eq!(
                provider_peers.len(),
                2,
                "both authenticated suppliers delivered verified bytes"
            );
        }
        let first_verified = events
            .iter()
            .position(|event| inbound_swarm(&event) && event.bytes > 0)
            .expect("verified swarm payload event");
        let started_before_first: BTreeSet<_> = events[..first_verified]
            .iter()
            .filter(|event| inbound_swarm(event) && event.bytes == 0)
            .map(|event| event.operation_id)
            .collect();
        if !interrupt_provider {
            assert!(
                started_before_first.len() >= 2,
                "two admitted providers must start before the first verified chunk"
            );
        }
        if !interrupt_provider {
            let verified_swarm_bytes = events
                .iter()
                .filter(|event| inbound_swarm(event) && event.bytes > 0)
                .map(|event| event.bytes)
                .sum::<u64>();
            assert_eq!(
                verified_swarm_bytes + fallback_bytes,
                report.pulled_bytes,
                "typed verified swarm plus fallback bytes equals the aggregate report"
            );
        }
    }
    assert_eq!(
        fs::read(base.join("consumer-root/payload.bin")).unwrap(),
        payload
    );
    // Keep the owner roster entry from the successful round, then
    // take the member supplier offline.  A fresh RO consumer must
    // recover that terminal provider failure and continue through
    // the authenticated owner source/fallback instead of turning
    // one unavailable roster hint into a false global failure.
    let provider_peer = provider_peer_id.to_string();
    let owner_peer = owner.endpoint_id().to_string();
    provider_engine_holder
        .take()
        .expect("provider managed engine")
        .shutdown()
        .await
        .unwrap();
    provider_holder
        .take()
        .expect("provider service")
        .shutdown()
        .await
        .unwrap();
    let consumer2 =
        ShareService::open(base.join("consumer2-device"), NetworkMode::DirectOnly, None)
            .await
            .unwrap();
    let consumer2_grant = consumer2
        .enroll(
            &owned
                .issue_key(Permission::ReadOnly, None, owner.endpoint_addr())
                .unwrap(),
            None,
        )
        .await
        .unwrap();
    let consumer2_engine = ManagedSyncEngine::open(
        &consumer2,
        consumer2_grant.owner,
        consumer2_grant.share_id,
        config(base, "consumer2"),
    )
    .unwrap();
    let loss_events = Arc::new(Mutex::new(Vec::<TransferEvent>::new()));
    let loss_observed = Arc::clone(&loss_events);
    let loss_observer = TransferObserver::new(move |event| {
        loss_observed.lock().expect("observer lock").push(event);
    });
    let loss_report = consumer2_engine
        .sync_read_only(Some(loss_observer))
        .await
        .unwrap();
    assert_eq!(loss_report.status, "pass");
    assert!(loss_report.pulled_bytes > 0);
    assert_eq!(
        fs::read(base.join("consumer2-root/payload.bin")).unwrap(),
        payload
    );
    {
        let loss_events = loss_events.lock().unwrap();
        assert!(loss_events.iter().any(|event| {
            event.phase == "swarm_provider_started"
                && event.peer.as_deref() == Some(provider_peer.as_str())
        }));
        assert!(loss_events.iter().any(|event| {
            event.phase == "swarm_provider_verified"
                && event.peer.as_deref() == Some(owner_peer.as_str())
                && event.bytes > 0
        }));
    }
    {
        let events = share_events.lock().expect("share observer lock");
        assert!(events.iter().any(|event| {
            event.phase == SharePhase::Swarm
                && event.direction == TransferDirection::Inbound
                && event.peer == owner.endpoint_id()
                && event.bytes > 0
                && event.grant.is_some()
        }));
    }
    consumer2_engine.shutdown().await.unwrap();
    consumer2.shutdown().await.unwrap();
    consumer_engine.shutdown().await.unwrap();
    consumer.shutdown().await.unwrap();
    owner.shutdown().await.unwrap();
}

#[test]
fn managed_read_only_owner_originated_file_warms_owner_supplier() {
    isolated(
        "managed_read_only_owner_originated_file_warms_owner_supplier",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let temp = tempfile::tempdir().unwrap();
                let base = temp.path();
                let owner =
                    ShareService::open(base.join("owner-device"), NetworkMode::DirectOnly, None)
                        .await
                        .unwrap();
                let consumer =
                    ShareService::open(base.join("consumer-device"), NetworkMode::DirectOnly, None)
                        .await
                        .unwrap();
                let owned = owner
                    .create_owned_share(
                        "owner cold CAS".into(),
                        base.join("owner-root"),
                        base.join("owner-state"),
                        None,
                        0,
                    )
                    .await
                    .unwrap();
                let payload = unique_payload();
                fs::write(base.join("owner-root/payload.bin"), &payload).unwrap();
                owned.refresh_inventory().await.unwrap();
                let grant = consumer
                    .enroll(
                        &owned
                            .issue_key(Permission::ReadOnly, None, owner.endpoint_addr())
                            .unwrap(),
                        None,
                    )
                    .await
                    .unwrap();
                let engine = ManagedSyncEngine::open(
                    &consumer,
                    grant.owner,
                    grant.share_id,
                    config(base, "consumer"),
                )
                .unwrap();
                let share_events = Arc::new(Mutex::new(Vec::<ShareTransferEvent>::new()));
                let observed_share_events = Arc::clone(&share_events);
                let share_observer = ShareTransferObserver::new(move |event| {
                    observed_share_events
                        .lock()
                        .expect("share observer lock")
                        .push(event);
                });
                owner.set_share_observer(Some(share_observer.clone()));
                consumer.set_share_observer(Some(share_observer));
                let events = Arc::new(Mutex::new(Vec::<TransferEvent>::new()));
                let observed = Arc::clone(&events);
                let observer = TransferObserver::new(move |event| {
                    observed.lock().expect("observer lock").push(event);
                });
                let report = engine.sync_read_only(Some(observer)).await.unwrap();
                assert_eq!(report.status, "pass");
                assert_eq!(report.pulled_bytes, payload.len() as u64);
                // The owner manifest request ingests the file into the
                // already-open owner CAS while holding the runtime admission.
                // The subsequent managed swarm transfer therefore proves the
                // normal owner-originated cold-file path without a manual CAS
                // preseed.  share/3 remains a CAS-only fallback for missing
                // chunks and is covered by the legacy fallback fixture.
                assert!(share_events.lock().unwrap().iter().any(|event| {
                    event.phase == SharePhase::Swarm
                        && event.direction == TransferDirection::Outbound
                        && event.peer == consumer.endpoint_id()
                        && event.bytes > 0
                        && event.grant.is_some()
                }));
                assert_eq!(
                    fs::read(base.join("consumer-root/payload.bin")).unwrap(),
                    payload
                );
                engine.shutdown().await.unwrap();
                consumer.shutdown().await.unwrap();
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

#[cfg(target_os = "linux")]
#[test]
fn managed_rw_preserves_unseen_descendant_when_owner_deletes_ancestor() {
    isolated(
        "managed_rw_preserves_unseen_descendant_when_owner_deletes_ancestor",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let base = tempfile::tempdir_in("/tmp").unwrap();
                let state = tempfile::tempdir_in("/dev/shm").unwrap();
                let owner = ShareService::open(
                    base.path().join("owner-device"),
                    NetworkMode::DirectOnly,
                    None,
                )
                .await
                .unwrap();
                let member = ShareService::open(
                    base.path().join("rw-device"),
                    NetworkMode::DirectOnly,
                    None,
                )
                .await
                .unwrap();
                let share = owner
                    .create_owned_share(
                        "unseen descendant".into(),
                        base.path().join("owner-root"),
                        state.path().join("owner-state"),
                        None,
                        0,
                    )
                    .await
                    .unwrap();
                fs::create_dir(base.path().join("owner-root/tree")).unwrap();
                fs::write(
                    base.path().join("owner-root/tree/known.txt"),
                    b"owner-known",
                )
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
                let mut cfg = config(base.path(), "rw-unseen");
                cfg.state_root = state.path().join("member-state");
                let engine =
                    ManagedSyncEngine::open(&member, grant.owner, grant.share_id, cfg.clone())
                        .unwrap();
                engine.sync_read_write(None).await.unwrap();

                // This child exists only on the member.  The owner then
                // removes the known directory, so the merge must preserve the
                // member-only bytes while retaining the directory namespace.
                fs::write(
                    base.path().join("rw-unseen-root/tree/new.txt"),
                    b"member-unseen",
                )
                .unwrap();
                fs::remove_file(base.path().join("owner-root/tree/known.txt")).unwrap();
                fs::remove_dir(base.path().join("owner-root/tree")).unwrap();

                let report = engine.sync_read_write(None).await.unwrap();
                assert!(
                    report
                        .conflicts
                        .iter()
                        .any(|conflict| conflict.path.as_str() == "tree"),
                    "ancestor deletion with an unseen child must be a namespace conflict"
                );
                for root in [
                    base.path().join("owner-root"),
                    base.path().join("rw-unseen-root"),
                ] {
                    assert!(root.join("tree").is_dir());
                    assert_eq!(
                        fs::read(root.join("tree/new.txt")).unwrap(),
                        b"member-unseen"
                    );
                    assert!(!root.join("tree/known.txt").exists());
                }

                // A second round must converge without re-emitting the local
                // child or resurrecting the causally deleted known child.
                let second = engine.sync_read_write(None).await.unwrap();
                assert_eq!(second.local_actions, 0);
                assert_eq!(second.remote_actions, 0);
                assert_eq!(
                    fs::read(base.path().join("owner-root/tree/new.txt")).unwrap(),
                    b"member-unseen"
                );
                assert!(!base.path().join("owner-root/tree/known.txt").exists());
                engine.shutdown().await.unwrap();
                member.shutdown().await.unwrap();
                owner.shutdown().await.unwrap();
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
                // Capture a real owner-signed roster before replacing the
                // owner endpoint. The resumed managed engine performs the D3
                // roster and heartbeat exchange before each snapshot; the
                // hostile transport below must answer those operations as
                // well so the three snapshot attempts still reach the
                // rollback, divergence, and missing-tombstone checks.
                let roster = session.refresh_roster().await.unwrap();
                let roster_challenge = session.roster_challenge().unwrap();
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
                session.close().await;
                let before = engine.preserved_changes().unwrap();
                // Drain the old session and release its lease before replacing the
                // owner endpoint. The resumed engine below must reopen the same
                // index/store, rather than silently creating a fresh binding.
                engine.shutdown().await.unwrap();
                owner.shutdown().await.unwrap();
                // The replacement uses the original authenticated identity, but a
                // fresh ephemeral UDP port. Reusing the old socket is a separate
                // transport guarantee covered by the network tests.
                let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
                    .secret_key(key)
                    .alpns(vec![ALPN_V3.to_vec()])
                    .clear_ip_transports()
                    .bind_addr("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
                    .unwrap()
                    .bind()
                    .await
                    .unwrap();
                let address = iroh::EndpointAddr::new(endpoint.id())
                    .with_ip_addr(endpoint.bound_sockets()[0]);
                let resumed_membership = grant.clone();
                let roster_for_control = roster.clone();
                let serving = endpoint.clone();
                let responses = tokio::spawn(async move {
                    // Authenticate the address update through the real resume
                    // operation before serving the three hostile snapshots.
                    let connection = serving.accept().await.unwrap().await.unwrap();
                    let (mut send, mut receive) = connection.accept_bi().await.unwrap();
                    let hello = raw_read(&mut receive).await;
                    assert_eq!(hello[0], 3);
                    raw_write(
                        &mut send,
                        &postcard::to_stdvec(&(4_u32, resumed_membership)).unwrap(),
                    )
                    .await;
                    send.finish().unwrap();
                    connection.closed().await;
                    for records in [old, divergent, Vec::new()] {
                        let tree = MerkleTree::from_records(records).unwrap();
                        // D3 liveness control is independent from the data
                        // sync gate. Route its two share-control operations by
                        // the serialized Operation tag, then continue with
                        // the legacy Session/QueryNode exchange for this
                        // snapshot attempt.
                        let (connection, mut send) = loop {
                            let connection = serving.accept().await.unwrap().await.unwrap();
                            let (mut send, mut receive) = connection.accept_bi().await.unwrap();
                            let hello = raw_read(&mut receive).await;
                            let operation = hello.get(33).copied().unwrap_or(u8::MAX);
                            match operation {
                                4 => {
                                    raw_write(
                                        &mut send,
                                        &postcard::to_stdvec(&(
                                            5_u32,
                                            roster_for_control.clone(),
                                            roster_challenge,
                                        ))
                                        .unwrap(),
                                    )
                                    .await;
                                    send.finish().unwrap();
                                    connection.closed().await;
                                }
                                5 => {
                                    raw_write(
                                        &mut send,
                                        &postcard::to_stdvec(&(6_u32, roster_for_control.clone()))
                                            .unwrap(),
                                    )
                                    .await;
                                    send.finish().unwrap();
                                    connection.closed().await;
                                }
                                _ => break (connection, send),
                            }
                        };
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
                let resumed = member
                    .resume_membership(grant.owner, grant.share_id, address.clone())
                    .await
                    .unwrap();
                assert_eq!(resumed, grant);
                let relationship = member
                    .relationships()
                    .unwrap()
                    .into_iter()
                    .find(|item| {
                        item.membership.owner == grant.owner
                            && item.membership.share_id == grant.share_id
                    })
                    .unwrap();
                assert_eq!(relationship.membership, grant);
                assert_eq!(relationship.address, address);
                let resumed_engine = ManagedSyncEngine::resume(
                    &member,
                    grant.owner,
                    grant.share_id,
                    config(base.path(), "ro"),
                )
                .unwrap();
                for _ in 0..3 {
                    let error = resumed_engine.sync_read_only(None).await.err().unwrap();
                    assert_eq!(ShareError::classify(&error), ShareError::InvalidRecord);
                    assert_eq!(
                        fs::read(base.path().join("ro-root/file")).unwrap(),
                        b"trusted new owner"
                    );
                    assert_eq!(resumed_engine.preserved_changes().unwrap(), before);
                }
                responses.await.unwrap();
                endpoint.close().await;
                resumed_engine.shutdown().await.unwrap();
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
