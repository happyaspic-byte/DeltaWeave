use deltaweave_net::{
    NetworkMode, PeerPolicy, ServerConfig,
    root_admission::{self, RootUse},
    start_server,
};
use iroh::SecretKey;

#[test]
fn legacy_server_rejects_managed_root_with_alternate_state() {
    if std::env::var_os("DELTAWEAVE_ADMISSION_CHILD").is_none() {
        let isolated = tempfile::tempdir().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "legacy_server_rejects_managed_root_with_alternate_state",
                "--nocapture",
            ])
            .env("DELTAWEAVE_ADMISSION_CHILD", "1")
            .env("HOME", isolated.path())
            .env("USERPROFILE", isolated.path())
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("managed");
        let lease = root_admission::acquire(
            &root,
            RootUse::Managed {
                share: [1; 32],
                owner: [2; 32],
            },
        )
        .unwrap();
        std::fs::write(root.join("protected.txt"), b"keep").unwrap();
        drop(lease);
        let result = start_server(ServerConfig {
            secret_key: SecretKey::generate(),
            destination_root: root.clone(),
            state_root: temp.path().join("alternate-state"),
            peer_policy: PeerPolicy::AnyAuthenticated,
            network_mode: NetworkMode::DirectOnly,
            bind_address: None,
            max_connections: 8,
            min_free_space_bytes: 0,
        })
        .await;
        if let Ok(server) = result {
            server.shutdown().await.unwrap();
            panic!("legacy server reopened a managed root");
        }
        assert_eq!(std::fs::read(root.join("protected.txt")).unwrap(), b"keep");
    });
}

#[test]
fn legacy_push_cannot_publish_a_managed_file() {
    if std::env::var_os("DELTAWEAVE_PUSH_ADMISSION_CHILD").is_none() {
        let isolated = tempfile::tempdir().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "legacy_push_cannot_publish_a_managed_file",
                "--nocapture",
            ])
            .env("DELTAWEAVE_PUSH_ADMISSION_CHILD", "1")
            .env("HOME", isolated.path())
            .env("USERPROFILE", isolated.path())
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("managed");
        drop(
            root_admission::acquire(
                &root,
                RootUse::Managed {
                    share: [1; 32],
                    owner: [2; 32],
                },
            )
            .unwrap(),
        );
        std::fs::write(root.join("secret"), b"protected").unwrap();
        let target = temp.path().join("receiver");
        let receiver = start_server(ServerConfig {
            secret_key: SecretKey::generate(),
            destination_root: target.clone(),
            state_root: temp.path().join("receiver-state"),
            peer_policy: PeerPolicy::AnyAuthenticated,
            network_mode: NetworkMode::DirectOnly,
            bind_address: None,
            max_connections: 8,
            min_free_space_bytes: 0,
        })
        .await
        .unwrap();
        let result = deltaweave_net::push_file(deltaweave_net::PushOptions {
            secret_key: SecretKey::generate(),
            source: root.join("secret"),
            remote_path: deltaweave_core::WirePath::new("stolen").unwrap(),
            remote: receiver.endpoint_addr(),
            profile: deltaweave_core::ChunkingProfile::DEFAULT,
            network_mode: NetworkMode::DirectOnly,
            state_root: Some(temp.path().join("alternate-sender-state")),
        })
        .await;
        assert!(result.is_err());
        assert!(!target.join("stolen").exists());
        assert_eq!(std::fs::read(root.join("secret")).unwrap(), b"protected");
        receiver.shutdown().await.unwrap();
    });
}
