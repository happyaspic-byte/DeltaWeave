use deltaweave_core::{ChunkingProfile, Hash32, ReplicaId};
use deltaweave_net::{
    NetworkMode, SyncClient,
    root_admission::{self, RootUse},
};
use deltaweave_sync::{SyncConfig, SyncEngine};
#[test]
fn sync_engine_cannot_reopen_managed_namespace_with_new_state() {
    if std::env::var_os("DW_SYNC_ADMISSION_CHILD").is_none() {
        let home = tempfile::tempdir().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "sync_engine_cannot_reopen_managed_namespace_with_new_state",
                "--nocapture",
            ])
            .env("DW_SYNC_ADMISSION_CHILD", "1")
            .env("HOME", home.path())
            .env("USERPROFILE", home.path())
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
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
    std::fs::write(root.join("file"), b"protected").unwrap();
    let candidates = vec![root.clone(), root.join("missing/deep")];
    #[cfg(unix)]
    let candidates = {
        std::os::unix::fs::symlink(&root, temp.path().join("alias")).unwrap();
        let mut paths = candidates;
        paths.push(temp.path().join("alias/missing/deep"));
        paths
    };
    for candidate in candidates {
        let key = iroh::SecretKey::generate();
        let remote = iroh::SecretKey::generate();
        let result = SyncEngine::open(SyncConfig {
            root: candidate,
            state_root: temp.path().join("alternate-state"),
            replica: ReplicaId(Hash32::digest(key.public().as_bytes())),
            client: SyncClient {
                secret_key: key,
                remote: iroh::EndpointAddr::new(remote.public()),
                network_mode: NetworkMode::DirectOnly,
            },
            profile: ChunkingProfile::DEFAULT,
            ignored_paths: Vec::new(),
        });
        assert!(result.is_err());
        assert_eq!(std::fs::read(root.join("file")).unwrap(), b"protected");
        assert!(
            !root.join("missing").exists(),
            "denied admission mutated managed root"
        );
        assert!(!temp.path().join("alternate-state").exists());
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
    }
}
