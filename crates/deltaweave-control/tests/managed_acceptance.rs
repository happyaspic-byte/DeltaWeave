use deltaweave_control::{
    CreateShareInput, EnrollmentState, IssueKeyInput, JoinShareInput, ManagedShareView,
    ManagedStatus, Manager, ManagerOptions, MutationCompletion, Permission, RetryPendingJoinInput,
    RevokeKeyInput, RevokeMemberInput, RotateKeyInput, ShareCommand, ShareCommandInput, ShareId,
    classify_managed_error,
};
use serde_json::{Value, json};
use std::{
    fs,
    net::{SocketAddr, UdpSocket},
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tempfile::TempDir;

static TEST_WORKSPACES: OnceLock<Mutex<Vec<TempDir>>> = OnceLock::new();

fn retain_workspace(workspace: TempDir) {
    TEST_WORKSPACES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap()
        .push(workspace);
}

/// Run each managed acceptance case in a child with its own profile and
/// process-scoped temporary directory. The root admission catalog is keyed by
/// profile, so tests never touch the developer's real admission database.
fn isolated(name: &str, body: impl FnOnce()) {
    if std::env::var("DELTAWEAVE_MANAGED_TEST").ok().as_deref() == Some(name) {
        body();
        return;
    }
    let profile = tempfile::Builder::new()
        .prefix("deltaweave-managed-profile-")
        .tempdir()
        .unwrap();
    let temporary = tempfile::Builder::new()
        .prefix("deltaweave-managed-tmp-")
        .tempdir()
        .unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture"])
        .env("DELTAWEAVE_MANAGED_TEST", name)
        .env("HOME", profile.path())
        .env("USERPROFILE", profile.path())
        .env("TMPDIR", temporary.path())
        .env("TMP", temporary.path())
        .env("TEMP", temporary.path())
        .status()
        .unwrap();
    assert!(status.success(), "isolated managed test failed: {name}");
}

fn options(port: Option<u16>) -> ManagerOptions {
    ManagerOptions {
        managed_network: deltaweave_control::NetworkMode::DirectOnly,
        managed_bind: port.map(|port| SocketAddr::from(([127, 0, 0, 1], port))),
    }
}

async fn open_manager(data: PathBuf, port: Option<u16>) -> std::sync::Arc<Manager> {
    Manager::open_with_options(data, options(port))
        .await
        .unwrap()
}

fn free_udp_port() -> u16 {
    UdpSocket::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn share_id(value: &str) -> ShareId {
    hex::decode(value).unwrap().try_into().map(ShareId).unwrap()
}

fn invitation_id(value: &str) -> deltaweave_control::InvitationId {
    hex::decode(value)
        .unwrap()
        .try_into()
        .map(deltaweave_control::InvitationId)
        .unwrap()
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

async fn wait_status(
    manager: &std::sync::Arc<Manager>,
    share: ShareId,
    expected: ManagedStatus,
) -> ManagedShareView {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(view) = manager
                .snapshot()
                .await
                .shares
                .into_iter()
                .find(|view| view.share_id == hex::encode(share.0) && view.status == expected)
            {
                return view;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("managed share did not reach {expected:?}"))
}

async fn wait_file(path: PathBuf, expected: Vec<u8>) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if fs::read(&path).is_ok_and(|bytes| bytes == expected) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("file did not reach expected content: {}", path.display()));
}

fn assert_conflict(error: anyhow::Error) {
    let summary = classify_managed_error(&error);
    assert_eq!(summary.code, "idempotency_conflict", "{summary:?}");
}

fn config_value(data: &Path) -> Value {
    serde_json::from_slice(&fs::read(data.join("config.json")).unwrap()).unwrap()
}

fn managed_record(data: &Path, share: ShareId) -> Value {
    let id = hex::encode(share.0);
    config_value(data)["managed"]["shares"]
        .as_array()
        .unwrap()
        .iter()
        .find(|record| record["share_id"].as_str() == Some(id.as_str()))
        .cloned()
        .unwrap_or_else(|| panic!("managed record {id} is absent"))
}

fn binding_snapshot(data: &Path, share: ShareId) -> Value {
    let record = managed_record(data, share);
    json!({
        "owner": record["owner"],
        "owner_address": record["owner_address"],
        "permission": record["permission"],
        "replica": record["replica"],
        "enrolled_at": record["enrolled_at"],
        "membership_epoch": record["membership_epoch"],
    })
}

fn write_json(data: &Path, value: &Value) {
    fs::write(
        data.join("config.json"),
        serde_json::to_vec_pretty(value).unwrap(),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(data.join("config.json"), fs::Permissions::from_mode(0o600)).unwrap();
    }
}

fn write_private(path: &Path, value: &str) {
    let parent = path.parent().unwrap();
    fs::create_dir_all(parent).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::write(path, value).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }
}

fn regular_files(path: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(path) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            regular_files(&path, files);
        } else if file_type.is_file() {
            files.push(path);
        }
    }
}

fn private_contains(path: &Path, needle: &[u8]) -> bool {
    let mut files = Vec::new();
    regular_files(path, &mut files);
    files
        .into_iter()
        .filter_map(|path| fs::read(path).ok())
        .any(|bytes| bytes.windows(needle.len()).any(|window| window == needle))
}

fn member_summary(mut members: Vec<deltaweave_control::MemberView>) -> Vec<Value> {
    let mut summary: Vec<_> = members
        .drain(..)
        .map(|member| {
            json!({
                "member_id": member.member_id,
                "permission": member.permission,
                "enrolled_at": member.enrolled_at,
                "revoked_at": member.revoked_at,
            })
        })
        .collect();
    summary.sort_by(|left, right| left["member_id"].as_str().cmp(&right["member_id"].as_str()));
    summary
}

#[test]
fn managed_concurrent_mutations_are_idempotent() {
    isolated("managed_concurrent_mutations_are_idempotent", || {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let workspace = TempDir::new().unwrap();
            let owner_data = workspace.path().join("owner-admin");
            let owner_root = workspace.path().join("owner-root");
            fs::create_dir_all(&owner_root).unwrap();
            fs::write(owner_root.join("seed.txt"), b"seed").unwrap();
            let owner = open_manager(owner_data, Some(free_udp_port())).await;

            let create_input = CreateShareInput {
                request_id: "create-race".into(),
                name: "Files".into(),
                root: owner_root.clone(),
                min_free_space_mib: Some(0),
            };
            let (created_a, created_b) = tokio::join!(
                owner.create_share(create_input.clone()),
                owner.create_share(create_input.clone())
            );
            assert!(created_a.is_ok() && created_b.is_ok());
            assert_eq!(
                serde_json::to_value(created_a.unwrap()).unwrap(),
                serde_json::to_value(created_b.unwrap()).unwrap()
            );
            let share = share_id(
                owner
                    .snapshot()
                    .await
                    .shares
                    .first()
                    .unwrap()
                    .share_id
                    .as_str(),
            );

            let other_a = workspace.path().join("other-a");
            let other_b = workspace.path().join("other-b");
            let (different_create_a, different_create_b) = tokio::join!(
                owner.create_share(CreateShareInput {
                    request_id: "different-create".into(),
                    name: "A".into(),
                    root: other_a.clone(),
                    min_free_space_mib: Some(0),
                }),
                owner.create_share(CreateShareInput {
                    request_id: "different-create".into(),
                    name: "B".into(),
                    root: other_b.clone(),
                    min_free_space_mib: Some(0),
                })
            );
            assert!(different_create_a.is_ok() ^ different_create_b.is_ok());
            assert_conflict(
                different_create_a
                    .err()
                    .or_else(|| different_create_b.err())
                    .unwrap(),
            );
            assert!(other_a.exists() ^ other_b.exists());

            let (issued_a, issued_b) = tokio::join!(
                owner.issue_key(IssueKeyInput {
                    request_id: "issue-race".into(),
                    share,
                    permission: Permission::ReadWrite,
                    expires_at: None,
                }),
                owner.issue_key(IssueKeyInput {
                    request_id: "issue-race".into(),
                    share,
                    permission: Permission::ReadWrite,
                    expires_at: None,
                })
            );
            assert!(issued_a.is_ok() && issued_b.is_ok());
            let key = issued_a.unwrap();
            let same_key = issued_b.unwrap();
            assert!(
                key.key == same_key.key,
                "same request must replay the same ticket"
            );
            assert_eq!(key.invitation_id, same_key.invitation_id);

            let (different_key_a, different_key_b) = tokio::join!(
                owner.issue_key(IssueKeyInput {
                    request_id: "different-issue".into(),
                    share,
                    permission: Permission::ReadWrite,
                    expires_at: None,
                }),
                owner.issue_key(IssueKeyInput {
                    request_id: "different-issue".into(),
                    share,
                    permission: Permission::ReadOnly,
                    expires_at: None,
                })
            );
            assert!(different_key_a.is_ok() ^ different_key_b.is_ok());
            assert_conflict(
                different_key_a
                    .err()
                    .or_else(|| different_key_b.err())
                    .unwrap(),
            );

            let member_data = workspace.path().join("member-admin");
            let member_root = workspace.path().join("member-root");
            let member = open_manager(member_data, None).await;
            let join_input = JoinShareInput {
                request_id: "join-race".into(),
                encoded_key: key.key.clone(),
                destination_root: member_root,
            };
            let (joined_a, joined_b) = tokio::join!(
                member.join_share(join_input.clone()),
                member.join_share(join_input.clone())
            );
            assert!(joined_a.is_ok() && joined_b.is_ok());
            assert_eq!(
                serde_json::to_value(joined_a.unwrap()).unwrap(),
                serde_json::to_value(joined_b.unwrap()).unwrap()
            );
            member.shutdown().await.unwrap();

            let member_two_data = workspace.path().join("member-two-admin");
            let member_two_root_a = workspace.path().join("member-two-a");
            let member_two_root_b = workspace.path().join("member-two-b");
            let member_two = open_manager(member_two_data, None).await;
            let (different_join_a, different_join_b) = tokio::join!(
                member_two.join_share(JoinShareInput {
                    request_id: "different-join".into(),
                    encoded_key: key.key.clone(),
                    destination_root: member_two_root_a.clone(),
                }),
                member_two.join_share(JoinShareInput {
                    request_id: "different-join".into(),
                    encoded_key: key.key,
                    destination_root: member_two_root_b.clone(),
                })
            );
            assert!(different_join_a.is_ok() ^ different_join_b.is_ok());
            assert_conflict(
                different_join_a
                    .err()
                    .or_else(|| different_join_b.err())
                    .unwrap(),
            );
            assert!(member_two_root_a.exists() ^ member_two_root_b.exists());
            member_two.shutdown().await.unwrap();

            let (same_command_a, same_command_b) = tokio::join!(
                owner.share_command(ShareCommandInput {
                    request_id: "command-race".into(),
                    share,
                    command: ShareCommand::Sync,
                }),
                owner.share_command(ShareCommandInput {
                    request_id: "command-race".into(),
                    share,
                    command: ShareCommand::Sync,
                })
            );
            assert!(same_command_a.is_ok() && same_command_b.is_ok());
            assert_eq!(
                serde_json::to_value(same_command_a.unwrap()).unwrap(),
                serde_json::to_value(same_command_b.unwrap()).unwrap()
            );

            let (different_command_a, different_command_b) = tokio::join!(
                owner.share_command(ShareCommandInput {
                    request_id: "different-command".into(),
                    share,
                    command: ShareCommand::Pause,
                }),
                owner.share_command(ShareCommandInput {
                    request_id: "different-command".into(),
                    share,
                    command: ShareCommand::Resume,
                })
            );
            assert!(different_command_a.is_ok() ^ different_command_b.is_ok());
            assert_conflict(
                different_command_a
                    .err()
                    .or_else(|| different_command_b.err())
                    .unwrap(),
            );
            owner.shutdown().await.unwrap();
            retain_workspace(workspace);
        });
    });
}

#[test]
fn managed_rotate_preserves_permission_and_replays_exact_ticket() {
    isolated(
        "managed_rotate_preserves_permission_and_replays_exact_ticket",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let workspace = TempDir::new().unwrap();
                let owner_data = workspace.path().join("owner-admin");
                let owner_root = workspace.path().join("owner-root");
                fs::create_dir_all(&owner_root).unwrap();
                let owner = open_manager(owner_data, Some(free_udp_port())).await;
                let share = share_id(
                    &owner
                        .create_share(CreateShareInput {
                            request_id: "rotate-create".into(),
                            name: "Rotate".into(),
                            root: owner_root,
                            min_free_space_mib: Some(0),
                        })
                        .await
                        .unwrap()
                        .share_id,
                );
                let rw = owner
                    .issue_key(IssueKeyInput {
                        request_id: "rotate-rw-issue".into(),
                        share,
                        permission: Permission::ReadWrite,
                        expires_at: None,
                    })
                    .await
                    .unwrap();
                let ro = owner
                    .issue_key(IssueKeyInput {
                        request_id: "rotate-ro-issue".into(),
                        share,
                        permission: Permission::ReadOnly,
                        expires_at: None,
                    })
                    .await
                    .unwrap();

                let rotated_rw = owner
                    .rotate_key(RotateKeyInput {
                        request_id: "rotate-rw".into(),
                        share,
                        invitation: invitation_id(&rw.invitation_id),
                        expires_at: None,
                    })
                    .await
                    .unwrap();
                assert_eq!(rotated_rw.permission, Permission::ReadWrite);
                let replayed_rw = owner
                    .rotate_key(RotateKeyInput {
                        request_id: "rotate-rw".into(),
                        share,
                        invitation: invitation_id(&rw.invitation_id),
                        expires_at: None,
                    })
                    .await
                    .unwrap();
                assert!(
                    replayed_rw.key == rotated_rw.key,
                    "same rotate request must replay the same ticket"
                );
                assert_eq!(replayed_rw.permission, Permission::ReadWrite);

                let rotated_ro = owner
                    .rotate_key(RotateKeyInput {
                        request_id: "rotate-ro".into(),
                        share,
                        invitation: invitation_id(&ro.invitation_id),
                        expires_at: None,
                    })
                    .await
                    .unwrap();
                assert_eq!(rotated_ro.permission, Permission::ReadOnly);

                let keys = owner.list_keys(share).await.unwrap();
                assert_eq!(keys.len(), 4);
                for old in [&rw, &ro] {
                    let old_id = invitation_id(&old.invitation_id);
                    assert!(keys.iter().any(|key| {
                        key.invitation_id == hex::encode(old_id.0) && key.revoked_at.is_some()
                    }));
                }
                assert!(keys.iter().any(|key| {
                    key.invitation_id == rotated_rw.invitation_id
                        && key.permission == Permission::ReadWrite
                        && key.revoked_at.is_none()
                }));
                assert!(keys.iter().any(|key| {
                    key.invitation_id == rotated_ro.invitation_id
                        && key.permission == Permission::ReadOnly
                        && key.revoked_at.is_none()
                }));
                owner.shutdown().await.unwrap();
                retain_workspace(workspace);
            });
        },
    );
}

#[test]
fn managed_join_rejects_second_binding_while_first_pending() {
    isolated(
        "managed_join_rejects_second_binding_while_first_pending",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let workspace = TempDir::new().unwrap();
                let owner_data = workspace.path().join("owner-admin");
                let owner_root = workspace.path().join("owner-root");
                fs::create_dir_all(&owner_root).unwrap();
                fs::write(owner_root.join("seed.txt"), b"seed").unwrap();
                let owner = open_manager(owner_data, Some(free_udp_port())).await;
                let share = share_id(
                    &owner
                        .create_share(CreateShareInput {
                            request_id: "binding-create".into(),
                            name: "Binding".into(),
                            root: owner_root,
                            min_free_space_mib: Some(0),
                        })
                        .await
                        .unwrap()
                        .share_id,
                );
                let key = owner
                    .issue_key(IssueKeyInput {
                        request_id: "binding-key".into(),
                        share,
                        permission: Permission::ReadWrite,
                        expires_at: None,
                    })
                    .await
                    .unwrap();
                owner.shutdown().await.unwrap();

                let member_data = workspace.path().join("member-admin");
                let first_root = workspace.path().join("member-first");
                let second_root = workspace.path().join("member-second");
                let member = open_manager(member_data.clone(), None).await;
                let first = member
                    .join_share(JoinShareInput {
                        request_id: "binding-first".into(),
                        encoded_key: key.key.clone(),
                        destination_root: first_root.clone(),
                    })
                    .await
                    .unwrap();
                assert_eq!(first.enrollment, EnrollmentState::Waiting);
                assert_eq!(member.snapshot().await.pending.len(), 1);

                let second = member
                    .join_share(JoinShareInput {
                        request_id: "binding-second".into(),
                        encoded_key: key.key,
                        destination_root: second_root.clone(),
                    })
                    .await
                    .expect_err("one share cannot acquire a second local binding");
                assert_eq!(classify_managed_error(&second).code, "busy");
                assert_eq!(member.snapshot().await.pending.len(), 1);
                assert!(!second_root.exists());
                assert!(first_root.exists());
                member.shutdown().await.unwrap();
                retain_workspace(workspace);
            });
        },
    );
}

#[test]
fn managed_memberships_survive_restart_and_status_gates() {
    isolated(
        "managed_memberships_survive_restart_and_status_gates",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let workspace = TempDir::new().unwrap();
                let owner_data = workspace.path().join("owner-admin");
                let owner_root = workspace.path().join("owner-root");
                fs::create_dir_all(&owner_root).unwrap();
                fs::write(owner_root.join("shared.txt"), b"owner bytes").unwrap();
                let owner_port = free_udp_port();
                let owner = open_manager(owner_data.clone(), Some(owner_port)).await;
                let share_view = owner
                    .create_share(CreateShareInput {
                        request_id: "create".into(),
                        name: "Restartable".into(),
                        root: owner_root,
                        min_free_space_mib: Some(0),
                    })
                    .await
                    .unwrap();
                let share = share_id(&share_view.share_id);
                let rw_key = owner
                    .issue_key(IssueKeyInput {
                        request_id: "rw-key".into(),
                        share,
                        permission: Permission::ReadWrite,
                        expires_at: None,
                    })
                    .await
                    .unwrap();
                let ro_key = owner
                    .issue_key(IssueKeyInput {
                        request_id: "ro-key".into(),
                        share,
                        permission: Permission::ReadOnly,
                        expires_at: None,
                    })
                    .await
                    .unwrap();

                let rw_data = workspace.path().join("rw-admin");
                let rw_root = workspace.path().join("rw-root");
                let rw = open_manager(rw_data.clone(), None).await;
                rw.join_share(JoinShareInput {
                    request_id: "rw-join".into(),
                    encoded_key: rw_key.key,
                    destination_root: rw_root,
                })
                .await
                .unwrap();
                wait_status(&rw, share, ManagedStatus::Complete).await;

                let ro_data = workspace.path().join("ro-admin");
                let ro_root = workspace.path().join("ro-root");
                let ro = open_manager(ro_data.clone(), None).await;
                ro.join_share(JoinShareInput {
                    request_id: "ro-join".into(),
                    encoded_key: ro_key.key,
                    destination_root: ro_root,
                })
                .await
                .unwrap();
                wait_status(&ro, share, ManagedStatus::Complete).await;
                let rw_binding_before = binding_snapshot(&rw_data, share);
                let ro_binding_before = binding_snapshot(&ro_data, share);
                let members_before = member_summary(owner.list_members(share).await.unwrap());

                ro.share_command(ShareCommandInput {
                    request_id: "pause-ro".into(),
                    share,
                    command: ShareCommand::Pause,
                })
                .await
                .unwrap();
                let paused = wait_status(&ro, share, ManagedStatus::Paused).await;
                // The pause transition is the durable observation baseline;
                // a final tick may have advanced `last_sync_at` after the
                // earlier Complete snapshot was read.
                let paused_last_sync_at = paused.last_sync_at;

                let rw_member = owner
                    .list_members(share)
                    .await
                    .unwrap()
                    .into_iter()
                    .find(|member| member.permission == Permission::ReadWrite)
                    .unwrap();
                let revoke = owner
                    .revoke_member(RevokeMemberInput {
                        request_id: "revoke-rw".into(),
                        share,
                        member_id: rw_member.member_id,
                    })
                    .await
                    .unwrap();
                assert_eq!(revoke.completion, MutationCompletion::Complete);
                let _ = rw
                    .share_command(ShareCommandInput {
                        request_id: "observe-revocation".into(),
                        share,
                        command: ShareCommand::Sync,
                    })
                    .await;
                wait_status(&rw, share, ManagedStatus::Revoked).await;
                let members_after_revoke = member_summary(owner.list_members(share).await.unwrap());

                rw.shutdown().await.unwrap();
                ro.shutdown().await.unwrap();
                owner.shutdown().await.unwrap();

                let owner_reopened = open_manager(owner_data, Some(owner_port)).await;
                let rw_reopened = open_manager(rw_data.clone(), None).await;
                let ro_reopened = open_manager(ro_data.clone(), None).await;
                tokio::time::sleep(Duration::from_millis(2500)).await;
                let rw_view = rw_reopened.snapshot().await.shares[0].clone();
                let ro_view = ro_reopened.snapshot().await.shares[0].clone();
                assert_eq!(rw_view.status, ManagedStatus::Revoked);
                assert_eq!(ro_view.status, ManagedStatus::Paused);
                assert_eq!(ro_view.last_sync_at, paused_last_sync_at);
                assert_eq!(binding_snapshot(&rw_data, share), rw_binding_before);
                assert_eq!(binding_snapshot(&ro_data, share), ro_binding_before);
                assert_eq!(
                    member_summary(owner_reopened.list_members(share).await.unwrap()),
                    members_after_revoke
                );
                assert!(
                    rw_reopened
                        .share_command(ShareCommandInput {
                            request_id: "revoked-must-not-restart".into(),
                            share,
                            command: ShareCommand::Sync,
                        })
                        .await
                        .is_err()
                );

                rw_reopened.shutdown().await.unwrap();
                ro_reopened.shutdown().await.unwrap();
                owner_reopened.shutdown().await.unwrap();
                assert!(!members_before.is_empty());
                retain_workspace(workspace);
            });
        },
    );
}

#[test]
fn managed_pending_resume_survives_ticket_loss_and_owner_offline() {
    isolated(
        "managed_pending_resume_survives_ticket_loss_and_owner_offline",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let workspace = TempDir::new().unwrap();
                let owner_data = workspace.path().join("owner-admin");
                let owner_root = workspace.path().join("owner-root");
                fs::create_dir_all(&owner_root).unwrap();
                fs::write(owner_root.join("seed.txt"), b"seed").unwrap();
                let owner_port = free_udp_port();
                let owner = open_manager(owner_data.clone(), Some(owner_port)).await;
                let share = share_id(
                    &owner
                        .create_share(CreateShareInput {
                            request_id: "create".into(),
                            name: "Pending".into(),
                            root: owner_root,
                            min_free_space_mib: Some(0),
                        })
                        .await
                        .unwrap()
                        .share_id,
                );
                let expiring_key = owner
                    .issue_key(IssueKeyInput {
                        request_id: "expiring-key".into(),
                        share,
                        permission: Permission::ReadWrite,
                        // Windows private-namespace preparation can take
                        // several seconds. Keep enough bounded lifetime for
                        // issue + enrollment, then wait against this actual
                        // expiry below before asserting the terminal path.
                        expires_at: Some(now_seconds().saturating_add(30)),
                    })
                    .await
                    .unwrap();
                let wrong_operation = owner
                    .retry_pending_join(RetryPendingJoinInput {
                        request_id: "expiring-key".into(),
                        share,
                    })
                    .await
                    .expect_err("a key issuance journal entry is not a join request");
                assert_eq!(
                    classify_managed_error(&wrong_operation).code,
                    "idempotency_conflict"
                );
                let member_data = workspace.path().join("member-admin");
                let member_root = workspace.path().join("member-root");
                let member = open_manager(member_data.clone(), None).await;
                member
                    .join_share(JoinShareInput {
                        request_id: "lost-response".into(),
                        encoded_key: expiring_key.key.clone(),
                        destination_root: member_root.clone(),
                    })
                    .await
                    .unwrap();
                wait_status(&member, share, ManagedStatus::Complete).await;
                let original_record = managed_record(&member_data, share);
                let members_before_resume =
                    member_summary(owner.list_members(share).await.unwrap());
                let owner_id = original_record["owner"]
                    .as_str()
                    .unwrap()
                    .parse::<iroh::EndpointId>()
                    .unwrap();
                member.shutdown().await.unwrap();
                let member_service = deltaweave_net::share::ShareService::open(
                    member_data.join("managed").join("service"),
                    deltaweave_net::NetworkMode::DirectOnly,
                    None,
                )
                .await
                .unwrap();
                member_service.forget_membership(owner_id, share).unwrap();
                assert!(member_service.open_session(owner_id, share).is_err());
                member_service.shutdown().await.unwrap();
                owner
                    .revoke_key(RevokeKeyInput {
                        request_id: "revoke-expiring-ticket".into(),
                        share,
                        invitation: invitation_id(&expiring_key.invitation_id),
                    })
                    .await
                    .unwrap();

                let expires_at = expiring_key
                    .expires_at
                    .expect("the fixture ticket has a bounded expiry");
                let remaining = expires_at.saturating_sub(now_seconds());
                if remaining > 0 {
                    tokio::time::timeout(
                        Duration::from_secs(remaining.saturating_add(2)),
                        tokio::time::sleep(Duration::from_secs(remaining.saturating_add(1))),
                    )
                    .await
                    .expect("bounded wait for the issued ticket expiry");
                }

                let ticket_path = member_data
                    .join("managed")
                    .join("pending")
                    .join(format!("{}.ticket", "ab".repeat(32)));
                write_private(&ticket_path, &expiring_key.key);
                let mut config = config_value(&member_data);
                let managed = config["managed"].as_object_mut().unwrap();
                managed["shares"] = json!([]);
                managed["pending"] = json!([{
                    "request_id": "lost-response",
                    "share_id": original_record["share_id"],
                    "owner": original_record["owner"],
                    "owner_address": original_record["owner_address"],
                    "name": original_record["name"],
                    "permission": original_record["permission"],
                    "root": original_record["root"],
                    "state_root": original_record["state_root"],
                    "ticket_file": ticket_path,
                    "created_at": now_seconds().saturating_sub(10),
                    "expires_at": expiring_key.expires_at,
                    "status": "waiting",
                    "retry_at": 0,
                }]);
                for request in managed["requests"].as_array_mut().unwrap() {
                    if request["request_id"].as_str() == Some("lost-response") {
                        request["result_ref"] = json!(format!(
                            "pending:{}",
                            original_record["share_id"].as_str().unwrap()
                        ));
                    }
                }
                write_json(&member_data, &config);
                tokio::time::sleep(Duration::from_secs(3)).await;
                let resumed = open_manager(member_data.clone(), None).await;
                wait_status(&resumed, share, ManagedStatus::Complete).await;
                let resumed_config = config_value(&member_data);
                assert!(
                    resumed_config["managed"]["pending"]
                        .as_array()
                        .unwrap()
                        .is_empty()
                );
                assert_eq!(
                    resumed_config["managed"]["requests"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|request| request["request_id"] == "lost-response")
                        .unwrap()["result_ref"],
                    json!(format!(
                        "share:{}",
                        original_record["share_id"].as_str().unwrap()
                    ))
                );
                assert!(!ticket_path.exists(), "resume must remove stale raw ticket");
                assert_eq!(
                    member_summary(owner.list_members(share).await.unwrap()),
                    members_before_resume,
                    "resume must not mint or mutate the owner membership"
                );
                let resumed_record = managed_record(&member_data, share);
                for key in ["permission", "replica", "enrolled_at", "membership_epoch"] {
                    assert_eq!(
                        resumed_record[key], original_record[key],
                        "binding field {key}"
                    );
                }
                resumed.shutdown().await.unwrap();

                let offline_key = owner
                    .issue_key(IssueKeyInput {
                        request_id: "offline-key".into(),
                        share,
                        permission: Permission::ReadWrite,
                        expires_at: None,
                    })
                    .await
                    .unwrap();
                owner.shutdown().await.unwrap();

                let offline_data = workspace.path().join("offline-admin");
                let offline_root = workspace.path().join("offline-root");
                let offline = open_manager(offline_data.clone(), None).await;
                let waiting = offline
                    .join_share(JoinShareInput {
                        request_id: "offline-join".into(),
                        encoded_key: offline_key.key.clone(),
                        destination_root: offline_root,
                    })
                    .await
                    .unwrap();
                assert_eq!(waiting.enrollment, EnrollmentState::Waiting);
                let pending_config = config_value(&offline_data);
                let pending = pending_config["managed"]["pending"].as_array().unwrap();
                assert_eq!(pending.len(), 1);
                let offline_ticket = PathBuf::from(pending[0]["ticket_file"].as_str().unwrap());
                assert!(offline_ticket.exists());
                offline.shutdown().await.unwrap();

                let offline_reopened = open_manager(offline_data.clone(), None).await;
                assert_eq!(offline_reopened.snapshot().await.pending.len(), 1);
                let wrong_share = offline_reopened
                    .retry_pending_join(RetryPendingJoinInput {
                        request_id: "offline-join".into(),
                        share: ShareId([0xff; 32]),
                    })
                    .await
                    .expect_err("a pending request cannot be replayed for another share");
                assert_eq!(
                    classify_managed_error(&wrong_share).code,
                    "idempotency_conflict"
                );
                let still_waiting = offline_reopened
                    .retry_pending_join(RetryPendingJoinInput {
                        request_id: "offline-join".into(),
                        share,
                    })
                    .await
                    .unwrap();
                assert_eq!(still_waiting.enrollment, EnrollmentState::Waiting);
                let owner_reopened = open_manager(owner_data.clone(), Some(owner_port)).await;
                let retried = offline_reopened
                    .retry_pending_join(RetryPendingJoinInput {
                        request_id: "offline-join".into(),
                        share,
                    })
                    .await
                    .unwrap();
                assert_eq!(retried.enrollment, EnrollmentState::Enrolled);
                assert_eq!(retried.permission, Some(Permission::ReadWrite));
                let replayed = offline_reopened
                    .retry_pending_join(RetryPendingJoinInput {
                        request_id: "offline-join".into(),
                        share,
                    })
                    .await
                    .unwrap();
                assert_eq!(replayed.enrollment, EnrollmentState::Enrolled);
                assert_eq!(replayed.member_id, retried.member_id);
                assert_eq!(replayed.permission, retried.permission);
                wait_status(&offline_reopened, share, ManagedStatus::Complete).await;
                assert!(
                    config_value(&offline_data)["managed"]["pending"]
                        .as_array()
                        .unwrap()
                        .is_empty()
                );
                assert!(!offline_ticket.exists());
                offline_reopened.shutdown().await.unwrap();

                // A pending request whose bearer expires while the owner is
                // offline must become a stable terminal result once the owner
                // can authenticate the NotMember state.  This exercises the
                // public retry endpoint's terminal mapping without exposing
                // the raw ticket.
                let expired_join_key = owner_reopened
                    .issue_key(IssueKeyInput {
                        request_id: "expired-join-key".into(),
                        share,
                        permission: Permission::ReadOnly,
                        expires_at: Some(now_seconds().saturating_add(5)),
                    })
                    .await
                    .unwrap();
                owner_reopened.shutdown().await.unwrap();

                let expired_data = workspace.path().join("expired-admin");
                let expired_root = workspace.path().join("expired-root");
                let expired = open_manager(expired_data.clone(), None).await;
                let expired_waiting = expired
                    .join_share(JoinShareInput {
                        request_id: "expired-join".into(),
                        encoded_key: expired_join_key.key,
                        destination_root: expired_root,
                    })
                    .await
                    .unwrap();
                assert_eq!(expired_waiting.enrollment, EnrollmentState::Waiting);
                tokio::time::sleep(Duration::from_secs(6)).await;
                let owner_final = open_manager(owner_data, Some(owner_port)).await;
                let expired_error = expired
                    .retry_pending_join(RetryPendingJoinInput {
                        request_id: "expired-join".into(),
                        share,
                    })
                    .await
                    .expect_err("expired pending enrollment must be terminal");
                assert_eq!(
                    classify_managed_error(&expired_error).code,
                    "pending_expired"
                );
                assert!(
                    config_value(&expired_data)["managed"]["pending"]
                        .as_array()
                        .unwrap()
                        .is_empty()
                );
                expired.shutdown().await.unwrap();
                owner_final.shutdown().await.unwrap();
                retain_workspace(workspace);
            });
        },
    );
}

#[test]
fn managed_rw_ro_lifecycle_preserves_ro_work_and_drains() {
    isolated(
        "managed_rw_ro_lifecycle_preserves_ro_work_and_drains",
        || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let workspace = TempDir::new().unwrap();
                let owner_data = workspace.path().join("owner-admin");
                let owner_root = workspace.path().join("owner-root");
                fs::create_dir_all(&owner_root).unwrap();
                fs::write(owner_root.join("shared.txt"), b"owner initial").unwrap();
                let owner_port = free_udp_port();
                let owner = open_manager(owner_data.clone(), Some(owner_port)).await;
                let share = share_id(
                    &owner
                        .create_share(CreateShareInput {
                            request_id: "create".into(),
                            name: "Lifecycle".into(),
                            root: owner_root.clone(),
                            min_free_space_mib: Some(0),
                        })
                        .await
                        .unwrap()
                        .share_id,
                );
                let rw_key = owner
                    .issue_key(IssueKeyInput {
                        request_id: "rw-key".into(),
                        share,
                        permission: Permission::ReadWrite,
                        expires_at: None,
                    })
                    .await
                    .unwrap();
                let ro_key = owner
                    .issue_key(IssueKeyInput {
                        request_id: "ro-key".into(),
                        share,
                        permission: Permission::ReadOnly,
                        expires_at: None,
                    })
                    .await
                    .unwrap();

                let rw_data = workspace.path().join("rw-admin");
                let rw_root = workspace.path().join("rw-root");
                let rw = open_manager(rw_data.clone(), None).await;
                rw.join_share(JoinShareInput {
                    request_id: "rw-join".into(),
                    encoded_key: rw_key.key,
                    destination_root: rw_root.clone(),
                })
                .await
                .unwrap();
                wait_status(&rw, share, ManagedStatus::Complete).await;

                let ro_data = workspace.path().join("ro-admin");
                let ro_root = workspace.path().join("ro-root");
                let ro = open_manager(ro_data.clone(), None).await;
                ro.join_share(JoinShareInput {
                    request_id: "ro-join".into(),
                    encoded_key: ro_key.key,
                    destination_root: ro_root.clone(),
                })
                .await
                .unwrap();
                wait_status(&ro, share, ManagedStatus::Complete).await;

                assert_eq!(
                    rw.snapshot().await.shares[0].active_peer_count,
                    0,
                    "observer must clear completed RW transfer"
                );
                assert_eq!(ro.snapshot().await.shares[0].active_peer_count, 0);
                assert_eq!(ro.snapshot().await.shares[0].speed_bps, 0);
                assert!(ro.snapshot().await.shares[0].connected_devices.is_empty());

                fs::write(rw_root.join("rw-local.txt"), b"rw local").unwrap();
                wait_file(owner_root.join("rw-local.txt"), b"rw local".to_vec()).await;
                fs::write(owner_root.join("owner-local.txt"), b"owner local").unwrap();
                wait_file(rw_root.join("owner-local.txt"), b"owner local".to_vec()).await;
                wait_file(ro_root.join("owner-local.txt"), b"owner local".to_vec()).await;

                fs::write(ro_root.join("shared.txt"), b"ro local edit").unwrap();
                fs::write(ro_root.join("ro-only.txt"), b"ro only").unwrap();
                fs::remove_file(ro_root.join("owner-local.txt")).unwrap();
                wait_status(&ro, share, ManagedStatus::Conflict).await;
                assert_eq!(
                    fs::read(ro_root.join("shared.txt")).unwrap(),
                    b"owner initial"
                );
                assert!(!ro_root.join("ro-only.txt").exists());
                assert_eq!(
                    fs::read(ro_root.join("owner-local.txt")).unwrap(),
                    b"owner local"
                );
                let ro_record = managed_record(&ro_data, share);
                let ro_state = PathBuf::from(ro_record["state_root"].as_str().unwrap());
                assert!(
                    private_contains(&ro_state, b"ro local edit")
                        || private_contains(&ro_state, b"ro only"),
                    "RO local changes must have a private recovery artifact"
                );
                let ro_conflict = ro.snapshot().await.shares[0].clone();
                assert_eq!(ro_conflict.active_peer_count, 0);
                assert_eq!(ro_conflict.speed_bps, 0);
                assert!(ro_conflict.connected_devices.is_empty());

                let rw_member = owner
                    .list_members(share)
                    .await
                    .unwrap()
                    .into_iter()
                    .find(|member| member.permission == Permission::ReadWrite)
                    .unwrap();
                let revoke = owner
                    .revoke_member(RevokeMemberInput {
                        request_id: "revoke-rw".into(),
                        share,
                        member_id: rw_member.member_id,
                    })
                    .await
                    .unwrap();
                assert_eq!(revoke.completion, MutationCompletion::Complete);
                let _ = rw
                    .share_command(ShareCommandInput {
                        request_id: "rw-revoked-sync".into(),
                        share,
                        command: ShareCommand::Sync,
                    })
                    .await;
                let revoked = wait_status(&rw, share, ManagedStatus::Revoked).await;
                assert_eq!(revoked.active_peer_count, 0);
                assert_eq!(revoked.speed_bps, 0);

                ro.share_command(ShareCommandInput {
                    request_id: "pause-ro".into(),
                    share,
                    command: ShareCommand::Pause,
                })
                .await
                .unwrap();
                wait_status(&ro, share, ManagedStatus::Paused).await;
                let ro_root_before_remove = ro_root.clone();
                let ro_state_before_remove = ro_state.clone();
                let removed_ro = ro
                    .remove_share(deltaweave_control::RemoveShareInput {
                        request_id: "remove-ro".into(),
                        share,
                    })
                    .await
                    .unwrap();
                assert_eq!(removed_ro.completion, MutationCompletion::Complete);
                assert!(ro_root_before_remove.exists());
                assert!(ro_state_before_remove.exists());
                let removed_rw = rw
                    .remove_share(deltaweave_control::RemoveShareInput {
                        request_id: "remove-rw".into(),
                        share,
                    })
                    .await
                    .unwrap();
                assert_eq!(removed_rw.completion, MutationCompletion::Complete);
                assert!(rw_root.exists());
                assert!(rw_data.join("config.json").exists());

                rw.shutdown().await.unwrap();
                ro.shutdown().await.unwrap();
                owner.shutdown().await.unwrap();
                let owner_reopened = open_manager(owner_data, Some(owner_port)).await;
                assert_eq!(owner_reopened.snapshot().await.shares.len(), 1);
                owner_reopened.shutdown().await.unwrap();
                retain_workspace(workspace);
            });
        },
    );
}
