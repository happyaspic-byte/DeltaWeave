use deltaweave_control::{
    CreateShareInput, IssueKeyInput, JoinShareInput, Manager, ManagerOptions, Permission,
    PreviewKeyInput, ShareId,
};

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
