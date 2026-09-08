use std::{collections::HashSet, fs, time::Duration};

use deltaweave_core::{ChunkingProfile, Hash32, ReplicaId};
use deltaweave_net::{NetworkMode, PeerPolicy, ServerConfig, SyncClient, start_server};
use deltaweave_store::Store;
use deltaweave_sync::{SyncConfig, SyncEngine};
use iroh::SecretKey;
use tempfile::TempDir;

async fn sync_with_primary_also_configured_as_swarm(
    files: &[(&str, &[u8])],
    seed_primary_cas: bool,
) {
    let local_root = TempDir::new().unwrap();
    let local_state = TempDir::new().unwrap();
    let remote_root = TempDir::new().unwrap();
    let remote_state = TempDir::new().unwrap();
    for (name, bytes) in files {
        fs::write(remote_root.path().join(name), bytes).unwrap();
    }
    if seed_primary_cas {
        let store = Store::open(remote_state.path()).unwrap();
        for (name, _) in files {
            store
                .ingest_file(remote_root.path().join(name), ChunkingProfile::DEFAULT)
                .unwrap();
        }
    }

    let client_key = SecretKey::generate();
    let replica = ReplicaId(Hash32::digest(client_key.public().as_bytes()));
    let server = start_server(ServerConfig {
        secret_key: SecretKey::generate(),
        destination_root: remote_root.path().into(),
        state_root: remote_state.path().into(),
        peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
        network_mode: NetworkMode::DirectOnly,
        bind_address: Some("127.0.0.1:0".parse().unwrap()),
        max_connections: 1,
        min_free_space_bytes: 0,
    })
    .await
    .unwrap();
    let remote = server.endpoint_addr();
    let engine = SyncEngine::open(SyncConfig {
        root: local_root.path().into(),
        state_root: local_state.path().into(),
        replica,
        client: SyncClient {
            secret_key: client_key,
            remote: remote.clone(),
            network_mode: NetworkMode::DirectOnly,
        },
        swarm_sources: vec![remote],
        profile: ChunkingProfile::DEFAULT,
        ignored_paths: Vec::new(),
    })
    .unwrap();

    let outcome = tokio::time::timeout(Duration::from_secs(30), async {
        let report = engine.sync_once().await?;
        assert_eq!(report.status, "pass");
        assert_eq!(report.verified_local_root, report.desired_root);
        assert_eq!(report.verified_remote_root, report.desired_root);
        assert_eq!(report.pulled_remote_files, files.len());
        for (name, expected) in files {
            let local = fs::read(local_root.path().join(name)).unwrap();
            let remote = fs::read(remote_root.path().join(name)).unwrap();
            assert_eq!(local, *expected, "local contents differ for {name}");
            assert_eq!(remote, *expected, "remote contents differ for {name}");
            assert_eq!(Hash32::digest(&local), Hash32::digest(expected));
            assert_eq!(Hash32::digest(&remote), Hash32::digest(expected));
        }
        let retry = engine.sync_once().await?;
        assert_eq!(retry.status, "pass");
        assert_eq!(retry.local_actions, 0);
        assert_eq!(retry.remote_actions, 0);
        assert_eq!(retry.pulled_bytes, 0);
        assert_eq!(retry.pushed_bytes, 0);
        assert_eq!(retry.verified_local_root, report.desired_root);
        assert_eq!(retry.verified_remote_root, report.desired_root);
        Ok::<_, anyhow::Error>(())
    })
    .await;

    // Close the receiver before propagating a transfer error or timeout.
    tokio::time::timeout(Duration::from_secs(10), server.shutdown())
        .await
        .expect("receiver shutdown must drain promptly")
        .unwrap();
    outcome
        .expect("a single connection must not deadlock reconciliation")
        .expect("the primary's swarm connection must not exclude its reconciliation requests");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_primary_cas_allows_fallback_with_a_single_connection() {
    // With an empty primary CAS, the authoritative file still needs a V2 pull
    // after the swarm availability query finds no chunks.
    sync_with_primary_also_configured_as_swarm(
        &[(
            "cold.txt",
            b"available in the authoritative root, absent from its CAS",
        )],
        false,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_files_allow_successive_manifests_with_a_single_connection() {
    // A warm primary CAS permits the first swarm fetch to finish. The next file
    // must still be able to request its own authoritative manifest over V2.
    sync_with_primary_also_configured_as_swarm(
        &[
            ("first.txt", b"first distinct file payload"),
            ("second.txt", b"second distinct file payload"),
        ],
        true,
    )
    .await;
}
