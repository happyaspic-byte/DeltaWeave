use super::*;
use tempfile::TempDir;

async fn source_server(state: &Path, root: &Path, client: EndpointId) -> Server {
    start_server(ServerConfig {
        secret_key: SecretKey::generate(),
        destination_root: root.into(),
        state_root: state.into(),
        peer_policy: PeerPolicy::AllowListed(HashSet::from([client])),
        network_mode: NetworkMode::DirectOnly,
        bind_address: None,
        max_connections: 8,
        min_free_space_bytes: 0,
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn paused_receiver_rejects_swarm_streams_on_an_existing_connection() {
    let state = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let key = SecretKey::generate();
    let server = source_server(state.path(), root.path(), key.public()).await;
    let endpoint = bind_endpoint(key, NetworkMode::DirectOnly, None, None)
        .await
        .unwrap();
    let connection = endpoint
        .connect(server.endpoint_addr(), ALPN_SWARM_V3)
        .await
        .unwrap();
    assert!(swarm_availability_on(&connection, Vec::new()).await.is_ok());
    server.pause().await.unwrap();
    assert!(
        swarm_availability_on(&connection, Vec::new())
            .await
            .is_err()
    );
    server.resume().await.unwrap();
    assert!(swarm_availability_on(&connection, Vec::new()).await.is_ok());
    connection.close(0u8.into(), b"test complete");
    endpoint.close().await;
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn swarm_budget_failure_leaves_cas_empty_and_is_not_a_source_failure() {
    let source_state = TempDir::new().unwrap();
    let source_root = TempDir::new().unwrap();
    let destination_state = TempDir::new().unwrap();
    let destination_root = TempDir::new().unwrap();
    let bytes = b"swarm payload must not consume reserved destination space";
    let hash = Hash32::digest(bytes);
    {
        let store = Store::open(source_state.path()).unwrap();
        store.chunks().put_verified(hash, bytes).unwrap();
    }
    let key = SecretKey::generate();
    let server = source_server(source_state.path(), source_root.path(), key.public()).await;
    let endpoint = bind_endpoint(key, NetworkMode::DirectOnly, None, None)
        .await
        .unwrap();
    let swarm = connect_swarm_sources(&endpoint, vec![server.endpoint_addr()])
        .await
        .unwrap();
    let store = Arc::new(Store::open(destination_state.path()).unwrap());
    for (reserve, pending) in [(u64::MAX, 0), (0, u64::MAX)] {
        let error = swarm
            .fill_chunks_with_admission(
                Arc::clone(&store),
                vec![hash],
                DiskAdmission::new(
                    destination_state.path().into(),
                    destination_root.path().into(),
                    reserve,
                    pending,
                ),
            )
            .await
            .unwrap_err();
        assert!(is_swarm_local_storage_error(&error));
        assert!(!store.chunks().contains(hash));
    }
    assert_eq!(fs::read_dir(destination_root.path()).unwrap().count(), 0);
    drop(swarm);
    endpoint.close().await;
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_endpoint_does_not_expose_its_private_cas_as_legacy_swarm() {
    let state = TempDir::new().unwrap();
    let service =
        share::ShareService::open(state.path().join("device"), NetworkMode::DirectOnly, None)
            .await
            .unwrap();
    let key = SecretKey::generate();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        swarm_hello(key, service.endpoint_addr(), NetworkMode::DirectOnly),
    )
    .await
    .expect("protocol mismatch must fail without waiting for a transfer timeout");
    assert!(result.is_err());
    service.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_drains_admitted_swarm_stream_before_releasing_root() {
    let state = TempDir::new().unwrap();
    let root = TempDir::new().unwrap();
    let key = SecretKey::generate();
    let server = source_server(state.path(), root.path(), key.public()).await;
    let endpoint = bind_endpoint(key, NetworkMode::DirectOnly, None, None)
        .await
        .unwrap();
    let connection = endpoint
        .connect(server.endpoint_addr(), ALPN_SWARM_V3)
        .await
        .unwrap();
    let (mut send, _receive) = connection.open_bi().await.unwrap();
    send.write_u32(16).await.unwrap();
    // Keep the admitted stream waiting for the remainder of its control frame.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            // Taking the admission write lock while polling can itself reject
            // this stream when the handler calls try_read_owned(). Observe the
            // registered task without competing for the admission gate.
            if server
                .swarm_tasks
                .lock()
                .unwrap()
                .iter()
                .any(|task| !task.is_finished())
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("swarm request is admitted");
    assert!(root_admission::acquire(root.path(), root_admission::RootUse::Legacy).is_err());
    tokio::time::timeout(Duration::from_secs(5), server.shutdown())
        .await
        .expect("router closure unblocks the pending frame and drains its handler")
        .unwrap();
    let lease = root_admission::acquire(root.path(), root_admission::RootUse::Legacy)
        .expect("shutdown has released every retained root lease");
    drop(lease);
    connection.close(0u8.into(), b"test complete");
    endpoint.close().await;
}
