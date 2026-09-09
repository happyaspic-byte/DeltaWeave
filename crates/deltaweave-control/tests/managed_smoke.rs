use deltaweave_control::{
    CreateShareInput, FolderInput, IssueKeyInput, JoinShareInput, Manager, ManagerOptions,
    Permission, PreviewKeyInput, RemoveShareInput, ShareId, classify_managed_error,
};
use std::time::{SystemTime, UNIX_EPOCH};

fn isolated(name: &str, body: impl FnOnce()) {
    if std::env::var("DELTAWEAVE_MANAGED_TEST").ok().as_deref() == Some(name) {
        body();
        return;
    }
    let home = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture"])
        .env("DELTAWEAVE_MANAGED_TEST", name)
        .env("HOME", home.path())
        .env("USERPROFILE", home.path())
        .status()
        .unwrap();
    assert!(status.success(), "isolated managed test failed");
}

#[test]
fn managed_owner_member_and_key_lifecycle() {
    isolated("managed_owner_member_and_key_lifecycle", || {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let temp = tempfile::tempdir().unwrap();
            let owner_data = temp.path().join("owner-admin");
            let owner_root = temp.path().join("owner-files");
            let member_data = temp.path().join("member-admin");
            let member_root = temp.path().join("member-files");
            std::fs::create_dir_all(&owner_root).unwrap();
            std::fs::write(owner_root.join("one.txt"), b"one").unwrap();

            let options = ManagerOptions {
                managed_network: deltaweave_control::NetworkMode::DirectOnly,
                managed_bind: None,
            };
            let owner = Manager::open_with_options(owner_data, options)
                .await
                .unwrap_or_else(|error| panic!("create failed: {error:#}"));
            let share = owner
                .create_share(CreateShareInput {
                    request_id: "owner-create".into(),
                    name: "Files".into(),
                    root: owner_root,
                    min_free_space_mib: Some(0),
                })
                .await
                .unwrap_or_else(|error| panic!("create failed: {error:#}"));
            let key = owner
                .issue_key(IssueKeyInput {
                    request_id: "owner-issue".into(),
                    share: ShareId(hex::decode(&share.share_id).unwrap().try_into().unwrap()),
                    permission: Permission::ReadWrite,
                    expires_at: None,
                })
                .await
                .unwrap();

            let preview = owner
                .preview_share_key(PreviewKeyInput {
                    request_id: "owner-preview".into(),
                    encoded_key: key.key.clone(),
                })
                .await
                .unwrap();
            assert_eq!(preview.share_id, share.share_id);

            let member = Manager::open_with_options(member_data, options)
                .await
                .unwrap();
            let joined = member
                .join_share(JoinShareInput {
                    request_id: "member-join".into(),
                    encoded_key: key.key,
                    destination_root: member_root.clone(),
                })
                .await
                .unwrap();
            assert_eq!(
                joined.enrollment,
                deltaweave_control::EnrollmentState::Enrolled
            );
            assert_eq!(joined.permission, Some(Permission::ReadWrite));

            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    if member_root.join("one.txt").is_file()
                        && member.snapshot().await.shares.first().is_some_and(|share| {
                            share.status == deltaweave_control::ManagedStatus::Complete
                        })
                    {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("managed member worker did not pull owner data");
            let snapshot = member.snapshot().await;
            assert_eq!(snapshot.shares.len(), 1);
            assert_eq!(
                snapshot.shares[0].status,
                deltaweave_control::ManagedStatus::Complete
            );
            assert_eq!(std::fs::read(member_root.join("one.txt")).unwrap(), b"one");

            member.shutdown().await.unwrap();
            owner.shutdown().await.unwrap();
        });
    });
}

#[test]
fn managed_join_service_init_failure_keeps_pending_binding_leased() {
    isolated(
        "managed_join_service_init_failure_keeps_pending_binding_leased",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let temp = tempfile::tempdir().unwrap();
                let owner_data = temp.path().join("owner-admin");
                let owner_root = temp.path().join("owner-files");
                let member_data = temp.path().join("member-admin");
                let member_root = temp.path().join("member-files");
                std::fs::create_dir_all(&owner_root).unwrap();
                std::fs::write(owner_root.join("seed.txt"), b"seed").unwrap();
                let options = ManagerOptions {
                    managed_network: deltaweave_control::NetworkMode::DirectOnly,
                    managed_bind: None,
                };
                let owner = Manager::open_with_options(owner_data, options)
                    .await
                    .unwrap();
                let share = owner
                    .create_share(CreateShareInput {
                        request_id: "service-failure-create".into(),
                        name: "Files".into(),
                        root: owner_root,
                        min_free_space_mib: Some(0),
                    })
                    .await
                    .unwrap();
                let share_id: ShareId =
                    ShareId(hex::decode(&share.share_id).unwrap().try_into().unwrap());
                let key = owner
                    .issue_key(IssueKeyInput {
                        request_id: "service-failure-key".into(),
                        share: share_id,
                        permission: Permission::ReadWrite,
                        expires_at: None,
                    })
                    .await
                    .unwrap();
                let ticket = deltaweave_net::share::ShareTicket::parse(&key.key).unwrap();
                let preview = ticket.preview();

                // A file at the managed service directory makes only service
                // initialization fail.  The pending root/state lease was
                // already acquired and must be returned unchanged.
                std::fs::create_dir_all(member_data.join("managed")).unwrap();
                std::fs::write(member_data.join("managed/service"), b"not a directory").unwrap();
                let member = Manager::open_with_options(member_data.clone(), options)
                    .await
                    .unwrap();
                let error = member
                    .join_share(JoinShareInput {
                        request_id: "service-failure-join".into(),
                        encoded_key: key.key,
                        destination_root: member_root,
                    })
                    .await
                    .expect_err("managed service initialization should fail closed");
                assert_eq!(classify_managed_error(&error).code, "invalid_input");

                let config: serde_json::Value = serde_json::from_slice(
                    &std::fs::read(member_data.join("config.json")).unwrap(),
                )
                .unwrap();
                let pending = &config["managed"]["pending"];
                assert_eq!(pending.as_array().unwrap().len(), 1);
                let pending = &pending[0];
                let root = std::path::PathBuf::from(pending["root"].as_str().unwrap());
                let state_root = std::path::PathBuf::from(pending["state_root"].as_str().unwrap());
                assert!(
                    deltaweave_net::root_admission::acquire_with_private(
                        &root,
                        deltaweave_net::root_admission::RootUse::Managed {
                            share: preview.share_id.0,
                            owner: *preview.owner.as_bytes(),
                        },
                        std::slice::from_ref(&state_root),
                    )
                    .is_err(),
                    "service-init failure must retain the pending root/state lease"
                );
                member.shutdown().await.unwrap();
                owner.shutdown().await.unwrap();
            });
        },
    );
}

#[test]
fn mixed_manual_and_managed_reopen_preserves_manual_worker_during_clock_quarantine() {
    isolated(
        "mixed_manual_and_managed_reopen_preserves_manual_worker_during_clock_quarantine",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let temp = tempfile::tempdir().unwrap();
                let data_dir = temp.path().join("admin");
                let manual_root = temp.path().join("manual-files");
                std::fs::create_dir_all(&manual_root).unwrap();
                let manager = Manager::open(data_dir.clone()).await.unwrap();
                let manual = manager
                    .add_folder(FolderInput {
                        name: "manual".into(),
                        root: manual_root.to_string_lossy().into_owned(),
                        role: "receive".into(),
                        enabled: Some(false),
                        bind: Some("127.0.0.1:0".into()),
                        ..FolderInput::default()
                    })
                    .await
                    .unwrap();
                manager.shutdown().await.unwrap();

                let config_path = data_dir.join("config.json");
                let mut config: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(&config_path).unwrap()).unwrap();
                let stale_clock = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    .saturating_add(3600);
                let share_id = "00".repeat(32);
                let ticket_file = data_dir
                    .join("managed/pending")
                    .join(format!("{share_id}.ticket"));
                config["managed"]["clock_last"] = serde_json::json!(stale_clock);
                config["managed"]["pending"] = serde_json::json!([{
                    "request_id": "mixed-rollback-test",
                    "share_id": share_id,
                    "owner": "00".repeat(32),
                    "owner_address": null,
                    "name": "managed",
                    "permission": "read_only",
                    "root": temp.path().join("managed-root"),
                    "state_root": temp.path().join("managed-state"),
                    "ticket_file": ticket_file,
                    "created_at": stale_clock,
                    "expires_at": null,
                    "status": "waiting",
                    "retry_at": null,
                    "min_free_space_bytes": 0
                }]);
                std::fs::write(config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();

                let reopened = Manager::open(data_dir).await.unwrap();
                let snapshot = reopened.snapshot().await;
                assert_eq!(snapshot.pending.len(), 1);
                assert_eq!(snapshot.folders.len(), 1);
                assert_eq!(snapshot.folders[0].id, manual.id);
                assert_eq!(snapshot.folders[0].status, "paused");
                reopened.shutdown().await.unwrap();
            });
        },
    );
}

#[test]
fn remove_tombstone_replay_after_recovery_is_idempotent() {
    isolated(
        "remove_tombstone_replay_after_recovery_is_idempotent",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let temp = tempfile::tempdir().unwrap();
                let data_dir = temp.path().join("admin");
                let owner_root = temp.path().join("owner-files");
                std::fs::create_dir_all(&owner_root).unwrap();
                std::fs::write(owner_root.join("keep.txt"), b"keep").unwrap();
                let options = ManagerOptions {
                    managed_network: deltaweave_control::NetworkMode::DirectOnly,
                    managed_bind: None,
                };
                let manager = Manager::open_with_options(data_dir.clone(), options)
                    .await
                    .unwrap();
                let share = manager
                    .create_share(CreateShareInput {
                        request_id: "remove-create".into(),
                        name: "Files".into(),
                        root: owner_root,
                        min_free_space_mib: Some(0),
                    })
                    .await
                    .unwrap();
                manager.shutdown().await.unwrap();

                let share_id = ShareId(hex::decode(&share.share_id).unwrap().try_into().unwrap());
                let remove = RemoveShareInput {
                    request_id: "remove-after-crash".into(),
                    share: share_id,
                };
                let mut hasher = blake3::Hasher::new();
                hasher.update(b"deltaweave/control/managed-request/v1\0");
                hasher.update(b"remove_share\0");
                hasher.update(&serde_json::to_vec(&remove).unwrap());
                let request_hash = hasher.finalize().to_hex().to_string();
                let config_path = data_dir.join("config.json");
                let mut config: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(&config_path).unwrap()).unwrap();
                let stale_clock = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                config["managed"]["tombstones"] = serde_json::json!([share.share_id]);
                config["managed"]["removals"] = serde_json::json!([{
                    "request_id": "remove-after-crash",
                    "request_hash": request_hash,
                    "share_id": share.share_id,
                    "created_at": stale_clock
                }]);
                std::fs::write(config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();

                let reopened = Manager::open_with_options(data_dir, options).await.unwrap();
                assert!(reopened.snapshot().await.shares.is_empty());
                let completed = reopened.remove_share(remove.clone()).await.unwrap();
                assert_eq!(
                    completed.completion,
                    deltaweave_control::MutationCompletion::Complete
                );
                assert_eq!(
                    completed.status,
                    deltaweave_control::ManagedStatus::Complete
                );
                let replay = reopened.remove_share(remove).await.unwrap();
                assert_eq!(
                    replay.completion,
                    deltaweave_control::MutationCompletion::Complete
                );
                assert!(reopened.snapshot().await.shares.is_empty());
                reopened.shutdown().await.unwrap();
            });
        },
    );
}

#[test]
fn staged_key_intent_replays_exact_ticket_without_duplicate_invitation() {
    isolated(
        "staged_key_intent_replays_exact_ticket_without_duplicate_invitation",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let temp = tempfile::tempdir().unwrap();
                let data_dir = temp.path().join("admin");
                let owner_root = temp.path().join("owner-files");
                std::fs::create_dir_all(&owner_root).unwrap();
                let options = ManagerOptions {
                    managed_network: deltaweave_control::NetworkMode::DirectOnly,
                    managed_bind: None,
                };
                let manager = Manager::open_with_options(data_dir.clone(), options)
                    .await
                    .unwrap();
                let share = manager
                    .create_share(CreateShareInput {
                        request_id: "intent-create".into(),
                        name: "Files".into(),
                        root: owner_root,
                        min_free_space_mib: Some(0),
                    })
                    .await
                    .unwrap();
                let share_id = ShareId(hex::decode(&share.share_id).unwrap().try_into().unwrap());
                let request = IssueKeyInput {
                    request_id: "intent-key".into(),
                    share: share_id,
                    permission: Permission::ReadWrite,
                    expires_at: None,
                };
                let original = manager.issue_key(request.clone()).await.unwrap();
                manager.shutdown().await.unwrap();

                let mut hasher = blake3::Hasher::new();
                hasher.update(b"deltaweave/control/managed-request/v1\0");
                hasher.update(b"issue_key\0");
                hasher.update(&serde_json::to_vec(&request).unwrap());
                let request_hash = hasher.finalize().to_hex().to_string();
                let response_file = data_dir
                    .join("managed/responses")
                    .join(format!("{request_hash}.ticket"));
                assert!(response_file.is_file());
                let config_path = data_dir.join("config.json");
                let mut config: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(&config_path).unwrap()).unwrap();
                let requests = config["managed"]["requests"].as_array_mut().unwrap();
                requests.retain(|record| record["request_id"] != "intent-key");
                config["managed"]["key_intents"] = serde_json::json!([{
                    "request_id": "intent-key",
                    "operation": "issue_key",
                    "request_hash": request_hash,
                    "share_id": share.share_id,
                    "permission": "read_write",
                    "expires_at": null,
                    "invitation_id": original.invitation_id,
                    "response_file": response_file,
                    "rotate_invitation": null,
                    "created_at": SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                }]);
                std::fs::write(config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();

                let reopened = Manager::open_with_options(data_dir, options).await.unwrap();
                let replay = reopened.issue_key(request).await.unwrap();
                assert!(
                    replay.key == original.key,
                    "replay must preserve the same ticket"
                );
                assert_eq!(reopened.list_keys(share_id).await.unwrap().len(), 1);
                reopened.shutdown().await.unwrap();
            });
        },
    );
}

#[test]
fn invalid_staged_key_file_is_removed_before_new_ticket_is_generated() {
    isolated(
        "invalid_staged_key_file_is_removed_before_new_ticket_is_generated",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let temp = tempfile::tempdir().unwrap();
                let data_dir = temp.path().join("admin");
                let owner_root = temp.path().join("owner-files");
                std::fs::create_dir_all(&owner_root).unwrap();
                let options = ManagerOptions {
                    managed_network: deltaweave_control::NetworkMode::DirectOnly,
                    managed_bind: None,
                };
                let manager = Manager::open_with_options(data_dir.clone(), options)
                    .await
                    .unwrap();
                let share = manager
                    .create_share(CreateShareInput {
                        request_id: "invalid-intent-create".into(),
                        name: "Files".into(),
                        root: owner_root,
                        min_free_space_mib: Some(0),
                    })
                    .await
                    .unwrap();
                // Exercise the product-created response namespace rather than
                // creating it with an unvalidated recursive mkdir. The staged
                // file below must pass the same private ACL preparation as a
                // real key issuance on Windows.
                let preparation_root = temp.path().join("response-preparation-files");
                std::fs::create_dir_all(&preparation_root).unwrap();
                let preparation = manager
                    .create_share(CreateShareInput {
                        request_id: "invalid-intent-preparation-share".into(),
                        name: "Preparation".into(),
                        root: preparation_root,
                        min_free_space_mib: Some(0),
                    })
                    .await
                    .unwrap();
                let preparation_id = ShareId(
                    hex::decode(&preparation.share_id)
                        .unwrap()
                        .try_into()
                        .unwrap(),
                );
                manager
                    .issue_key(IssueKeyInput {
                        request_id: "invalid-intent-preparation-key".into(),
                        share: preparation_id,
                        permission: Permission::ReadWrite,
                        expires_at: None,
                    })
                    .await
                    .unwrap();
                manager
                    .remove_share(RemoveShareInput {
                        request_id: "invalid-intent-preparation-remove".into(),
                        share: preparation_id,
                    })
                    .await
                    .unwrap();
                manager.shutdown().await.unwrap();

                let request = IssueKeyInput {
                    request_id: "invalid-intent-key".into(),
                    share: ShareId(hex::decode(&share.share_id).unwrap().try_into().unwrap()),
                    permission: Permission::ReadWrite,
                    expires_at: None,
                };
                let share_id = request.share;
                let mut hasher = blake3::Hasher::new();
                hasher.update(b"deltaweave/control/managed-request/v1\0");
                hasher.update(b"issue_key\0");
                hasher.update(&serde_json::to_vec(&request).unwrap());
                let request_hash = hasher.finalize().to_hex().to_string();
                let response_file = data_dir
                    .join("managed/responses")
                    .join(format!("{request_hash}.ticket"));
                std::fs::write(&response_file, b"corrupt staged ticket").unwrap();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(
                        response_file.parent().unwrap(),
                        std::fs::Permissions::from_mode(0o700),
                    )
                    .unwrap();
                    std::fs::set_permissions(
                        &response_file,
                        std::fs::Permissions::from_mode(0o600),
                    )
                    .unwrap();
                }
                let config_path = data_dir.join("config.json");
                let mut config: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(&config_path).unwrap()).unwrap();
                config["managed"]["key_intents"] = serde_json::json!([{
                    "request_id": request.request_id.clone(),
                    "operation": "issue_key",
                    "request_hash": request_hash,
                    "share_id": share.share_id.clone(),
                    "permission": "read_write",
                    "expires_at": null,
                    "invitation_id": "00".repeat(32),
                    "response_file": response_file,
                    "rotate_invitation": null,
                    "created_at": SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                }]);
                std::fs::write(config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();

                let reopened = Manager::open_with_options(data_dir, options).await.unwrap();
                let issued = reopened.issue_key(request).await.unwrap();
                assert!(
                    deltaweave_net::share::ShareTicket::parse(&issued.key).is_ok(),
                    "replacement response must be a valid ticket"
                );
                assert_eq!(reopened.list_keys(share_id).await.unwrap().len(), 1);
                reopened.shutdown().await.unwrap();
            });
        },
    );
}

#[test]
fn managed_restart_rejects_conflicting_active_and_pending_binding() {
    isolated(
        "managed_restart_rejects_conflicting_active_and_pending_binding",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let temp = tempfile::tempdir().unwrap();
                let data_dir = temp.path().join("admin");
                let owner_root = temp.path().join("owner-files");
                let conflicting_root = temp.path().join("conflicting-files");
                let conflicting_state = temp.path().join("conflicting-state");
                std::fs::create_dir_all(&owner_root).unwrap();
                std::fs::create_dir_all(&conflicting_root).unwrap();
                let options = ManagerOptions {
                    managed_network: deltaweave_control::NetworkMode::DirectOnly,
                    managed_bind: None,
                };
                let manager = Manager::open_with_options(data_dir.clone(), options)
                    .await
                    .unwrap();
                let share = manager
                    .create_share(CreateShareInput {
                        request_id: "binding-conflict-create".into(),
                        name: "Files".into(),
                        root: owner_root,
                        min_free_space_mib: Some(0),
                    })
                    .await
                    .unwrap();
                manager.shutdown().await.unwrap();

                let share_id = share.share_id.clone();
                let pending_ticket = data_dir
                    .join("managed/pending")
                    .join(format!("{share_id}.ticket"));
                let config_path = data_dir.join("config.json");
                let mut config: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(&config_path).unwrap()).unwrap();
                config["managed"]["pending"] = serde_json::json!([{
                    "request_id": "conflicting-pending",
                    "share_id": share_id,
                    "owner": config["managed"]["shares"][0]["owner"].clone(),
                    "owner_address": config["managed"]["shares"][0]["owner_address"].clone(),
                    "name": "Files",
                    "permission": "read_write",
                    "root": conflicting_root,
                    "state_root": conflicting_state,
                    "ticket_file": pending_ticket,
                    "created_at": 1,
                    "expires_at": null,
                    "status": "waiting",
                    "retry_at": null,
                    "min_free_space_bytes": 0
                }]);
                std::fs::write(config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();

                let error = match Manager::open_with_options(data_dir, options).await {
                    Ok(manager) => {
                        manager.shutdown().await.unwrap();
                        panic!("recovery unexpectedly accepted two immutable bindings");
                    }
                    Err(error) => error,
                };
                assert!(
                    classify_managed_error(&error).code == "state_unavailable",
                    "unexpected classified recovery failure"
                );
                assert!(!conflicting_root.join("created-by-recovery").exists());
            });
        },
    );
}
