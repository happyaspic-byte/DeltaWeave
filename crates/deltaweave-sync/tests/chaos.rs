//! Chaos fault-injection tests over real DeltaWeave servers and engines.
//!
//! Each test builds an N-peer topology, injects a failure (server death
//! mid-mesh, CAS bit corruption, concurrent three-way edits), then proves
//! every peer converges to one Merkle root and one on-disk namespace.

#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs,
    path::{Path, PathBuf},
};

use deltaweave_core::{ChunkingProfile, Hash32, ReplicaId};
use deltaweave_net::{NetworkMode, PeerPolicy, Server, ServerConfig, SyncClient, start_server};
use deltaweave_reconcile::{MerkleTree, merge_snapshots};
use deltaweave_store::ChunkStore;
use deltaweave_sync::{SyncConfig, SyncEngine, SyncReport};
use iroh::SecretKey;
use tempfile::TempDir;

/// One peer's durable roots and identity in the chaos mesh.
struct Peer {
    key: SecretKey,
    root: TempDir,
    server_state: TempDir,
    client_state: TempDir,
}

impl Peer {
    fn new(label: &str) -> Self {
        let _ = label;
        Self {
            key: SecretKey::generate(),
            root: TempDir::new().expect("peer root can be created"),
            server_state: TempDir::new().expect("peer server state can be created"),
            client_state: TempDir::new().expect("peer client state can be created"),
        }
    }

    fn replica(&self) -> ReplicaId {
        ReplicaId(Hash32::digest(self.key.public().as_bytes()))
    }

    fn engine_config(&self, remote: &Server) -> SyncConfig {
        SyncConfig {
            root: self.root.path().to_path_buf(),
            state_root: self.client_state.path().to_path_buf(),
            replica: self.replica(),
            client: SyncClient {
                secret_key: self.key.clone(),
                remote: remote.endpoint_addr(),
                network_mode: NetworkMode::DirectOnly,
            },
            profile: ChunkingProfile::DEFAULT,
            ignored_paths: Vec::new(),
        }
    }
}

async fn start_mesh_peer(peer: &Peer, clients: &[&Peer]) -> Server {
    let allowed: HashSet<_> = clients.iter().map(|peer| peer.key.public()).collect();
    start_server(ServerConfig {
        secret_key: peer.key.clone(),
        destination_root: peer.root.path().to_path_buf(),
        state_root: peer.server_state.path().to_path_buf(),
        peer_policy: PeerPolicy::AllowListed(allowed),
        network_mode: NetworkMode::DirectOnly,
        quota_policy: None,
    })
    .await
    .expect("mesh server can start")
}

/// Runs one directed sync from `source` acting as the local side against `remote`.
async fn sync_pair(source: &Peer, remote: &Server) -> SyncReport {
    let engine = SyncEngine::open(source.engine_config(remote)).expect("mesh engine can open");
    engine.sync_once().await.expect("mesh sync converges")
}

async fn start_all(peers: &[Peer]) -> Vec<Server> {
    let refs: Vec<_> = peers.iter().collect();
    let mut servers = Vec::new();
    for (index, peer) in peers.iter().enumerate() {
        let mut clients = refs.clone();
        clients.swap_remove(index);
        servers.push(start_mesh_peer(peer, &clients).await);
    }
    servers
}

async fn shutdown_all(servers: Vec<Server>) {
    for server in servers {
        server.shutdown().await.expect("mesh server shuts down");
    }
}

/// Full pairwise sweep over every directed pair of the mesh.
async fn sweep_all(peers: &[Peer], servers: &[Server]) {
    for (source_index, source) in peers.iter().enumerate() {
        for (remote_index, remote) in servers.iter().enumerate() {
            if source_index != remote_index {
                sync_pair(source, remote).await;
            }
        }
    }
}

/// Reads one file from a peer root, asserting it exists.
fn read_peer_file(peer: &Peer, relative: &str) -> Vec<u8> {
    fs::read(peer.root.path().join(relative))
        .unwrap_or_else(|_| panic!("peer file {relative} exists"))
}

/// Runs `count` pairwise sync rounds; returns all roots asserted equal.
async fn converge_rounds(peers: &[Peer], servers: &[Server], rounds: usize) {
    for _ in 0..rounds {
        sweep_all(peers, servers).await;
    }
    assert_all_converged(peers).await;
}

async fn assert_all_converged(peers: &[Peer]) {
    for (source_index, source) in peers.iter().enumerate() {
        for (other_index, other) in peers.iter().enumerate() {
            if source_index == other_index {
                continue;
            }
            let mut left: BTreeMap<String, Vec<u8>> = BTreeMap::new();
            let mut right: BTreeMap<String, Vec<u8>> = BTreeMap::new();
            collect_live_files(source.root.path(), "", &mut left);
            collect_live_files(other.root.path(), "", &mut right);
            let keys: BTreeSet<_> = left.keys().chain(right.keys()).cloned().collect();
            for key in keys {
                assert_eq!(
                    left.get(&key),
                    right.get(&key),
                    "peers {source_index} and {other_index} diverged at {key}"
                );
            }
        }
    }
}

fn collect_live_files(root: &Path, prefix: &str, out: &mut BTreeMap<String, Vec<u8>>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        let relative = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}/{name}")
        };
        let metadata = entry.metadata().expect("peer entry is readable");
        if metadata.is_dir() {
            collect_live_files(&entry.path(), &relative, out);
        } else if metadata.is_file() {
            let bytes = fs::read(entry.path()).expect("peer file is readable");
            out.insert(relative, bytes);
        }
    }
}

/// Corrupts one CAS chunk file in a peer's state store by flipping bytes.
fn corrupt_one_cas_chunk(store_root: &Path) -> PathBuf {
    let chunks = store_root.join("chunks");
    let mut victim: Option<PathBuf> = None;
    let mut stack = vec![chunks.clone()];
    while let Some(current) = stack.pop() {
        for entry in fs::read_dir(&current)
            .expect("chunk directory is readable")
            .flatten()
        {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if victim.is_none() {
                victim = Some(path);
            }
        }
    }
    let victim = victim.expect("peer CAS has at least one chunk");
    let bytes = fs::read(&victim).expect("victim chunk is readable");
    let mut corrupted = bytes;
    if corrupted.is_empty() {
        corrupted.push(1);
    } else {
        corrupted[0] ^= 0xff;
    }
    fs::write(&victim, &corrupted).expect("chunk can be corrupted in place");
    victim
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_peer_mesh_converges_to_one_namespace() {
    let peers = [Peer::new("alpha"), Peer::new("beta"), Peer::new("gamma")];
    fs::write(peers[0].root.path().join("alpha.txt"), b"alpha file").expect("alpha writes");
    fs::write(peers[1].root.path().join("beta.txt"), b"beta file").expect("beta writes");
    fs::write(peers[2].root.path().join("gamma.txt"), b"gamma file").expect("gamma writes");

    let servers = start_all(&peers).await;
    converge_rounds(&peers, &servers, 2).await;

    for peer in &peers {
        assert_eq!(read_peer_file(peer, "alpha.txt"), b"alpha file");
        assert_eq!(read_peer_file(peer, "beta.txt"), b"beta file");
        assert_eq!(read_peer_file(peer, "gamma.txt"), b"gamma file");
    }
    shutdown_all(servers).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_death_partition_heals_after_restart() {
    let peers = [Peer::new("alpha"), Peer::new("beta"), Peer::new("gamma")];
    fs::write(peers[0].root.path().join("before.txt"), b"before partition").expect("alpha writes");
    let servers = start_all(&peers).await;
    converge_rounds(&peers, &servers, 1).await;

    // Partition: kill the gamma server and drop its engine handle.
    let mut servers = servers;
    let gamma = servers.pop().expect("gamma server exists");
    gamma.shutdown().await.expect("gamma server dies");
    let gamma_peer = &peers[2];

    // Alpha and beta keep syncing during the partition; alpha adds a file.
    fs::write(peers[0].root.path().join("during.txt"), b"during partition")
        .expect("alpha writes during partition");
    sync_pair(&peers[0], &servers[1]).await;
    assert!(
        !gamma_peer.root.path().join("during.txt").exists(),
        "gamma is partitioned and has not received the new file"
    );

    // Heal: restart gamma's server and sweep the mesh until it converges.
    let allowed: HashSet<_> = [peers[0].key.public(), peers[1].key.public()].into();
    let healed = start_server(ServerConfig {
        secret_key: gamma_peer.key.clone(),
        destination_root: gamma_peer.root.path().to_path_buf(),
        state_root: gamma_peer.server_state.path().to_path_buf(),
        peer_policy: PeerPolicy::AllowListed(allowed),
        network_mode: NetworkMode::DirectOnly,
        quota_policy: None,
    })
    .await
    .expect("gamma server restarts with the same identity and state");
    let mut mesh = servers;
    mesh.push(healed);
    converge_rounds(&peers, &mesh, 2).await;
    assert_eq!(
        read_peer_file(gamma_peer, "during.txt"),
        b"during partition",
        "healed gamma receives the file written during the partition"
    );
    shutdown_all(mesh).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupted_cas_chunk_is_detected_and_repulled() {
    let peers = [Peer::new("alpha"), Peer::new("beta")];
    fs::write(
        peers[0].root.path().join("doc.bin"),
        b"payload for corruption test",
    )
    .expect("alpha writes");

    let servers = start_all(&peers).await;
    sync_pair(&peers[0], &servers[1]).await;
    assert_eq!(
        read_peer_file(&peers[1], "doc.bin"),
        b"payload for corruption test"
    );

    // Corrupt one chunk in beta's receiving CAS. Alpha then introduces a
    // same-content path, forcing beta's inventory check to detect and repull it.
    let victim = corrupt_one_cas_chunk(peers[1].server_state.path());
    fs::write(
        peers[0].root.path().join("doc-copy.bin"),
        b"payload for corruption test",
    )
    .expect("alpha writes a same-content path");
    let report = sync_pair(&peers[0], &servers[1]).await;
    assert_eq!(report.status, "pass");
    assert_eq!(
        read_peer_file(&peers[1], "doc-copy.bin"),
        b"payload for corruption test"
    );

    let chunks = ChunkStore::open(peers[1].server_state.path()).expect("beta chunk store reopens");
    let manifest = deltaweave_cdc::manifest_from_path(
        peers[1].root.path().join("doc.bin"),
        ChunkingProfile::DEFAULT,
    )
    .expect("beta file has a manifest");
    for chunk in &manifest.chunks {
        assert!(
            chunks.read_verified(chunk.hash).is_ok(),
            "chunk {} is verified after healing",
            chunk.hash
        );
    }
    assert!(
        victim.is_file(),
        "repaired chunk remains at its content address"
    );
    shutdown_all(servers).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aborted_mid_transfer_retries_to_convergence() {
    let peers = [Peer::new("alpha"), Peer::new("beta")];
    let payload = vec![0xA5_u8; 4 * 1024 * 1024];
    fs::write(peers[0].root.path().join("big.bin"), &payload).expect("alpha writes");

    let servers = start_all(&peers).await;
    let engine = SyncEngine::open(peers[0].engine_config(&servers[1])).expect("engine opens");
    let join = tokio::spawn(async move { engine.sync_once().await });
    tokio::task::yield_now().await;
    join.abort();
    let aborted = join.await;
    assert!(
        aborted.is_err(),
        "the in-flight transfer is cancelled before completion"
    );

    let report = sync_pair(&peers[0], &servers[1]).await;
    assert_eq!(report.status, "pass");
    assert_eq!(read_peer_file(&peers[1], "big.bin"), payload);
    shutdown_all(servers).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_way_concurrent_write_race_converges() {
    let peers = [Peer::new("alpha"), Peer::new("beta"), Peer::new("gamma")];
    fs::write(peers[0].root.path().join("shared.txt"), b"base").expect("alpha writes base");

    let servers = start_all(&peers).await;
    converge_rounds(&peers, &servers, 1).await;

    // Three-way race: every peer independently rewrites shared.txt.
    fs::write(peers[0].root.path().join("shared.txt"), b"alpha version").expect("alpha races");
    fs::write(peers[1].root.path().join("shared.txt"), b"beta version").expect("beta races");
    fs::write(peers[2].root.path().join("shared.txt"), b"gamma version").expect("gamma races");

    converge_rounds(&peers, &servers, 3).await;

    // All peers converge; deterministic conflict resolution preserves a losing value.
    let namespaces: Vec<BTreeSet<Vec<u8>>> = peers
        .iter()
        .map(|peer| {
            let mut files = BTreeMap::new();
            collect_live_files(peer.root.path(), "", &mut files);
            BTreeSet::from_iter(files.into_values())
        })
        .collect();
    for namespace in &namespaces[1..] {
        assert_eq!(
            namespace, &namespaces[0],
            "every peer converges to the same set of file contents after the three-way race"
        );
    }
    let racing_values = [
        b"alpha version".as_ref(),
        b"beta version".as_ref(),
        b"gamma version".as_ref(),
    ];
    assert!(
        racing_values
            .iter()
            .filter(|value| namespaces[0].contains(**value))
            .count()
            >= 2,
        "three-way resolution preserves a conflict value; observed {:?}",
        namespaces[0]
    );
    shutdown_all(servers).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deterministic_merge_soak_reaches_one_root_from_random_inputs() {
    let mut state = 0x853c49e6748fea9b_u64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let alpha = ReplicaId(Hash32::digest(b"soak-alpha"));
    let beta = ReplicaId(Hash32::digest(b"soak-beta"));
    for iteration in 0..100_000 {
        let choice = next() % 8;
        let counter_alpha = next() % 10 + 1;
        let counter_beta = next() % 10 + 1;
        let alpha_bytes = format!("alpha-{counter_alpha}-{choice}").into_bytes();
        let beta_bytes = format!("beta-{counter_beta}-{choice}").into_bytes();
        let left = MerkleTree::from_records([deltaweave_core::SyncRecord {
            schema_version: deltaweave_core::SYNC_RECORD_SCHEMA_V1,
            path: deltaweave_core::WirePath::new(format!("f{choice}.txt"))
                .expect("soak path is portable"),
            kind: deltaweave_core::SyncEntryKind::File,
            size: alpha_bytes.len() as u64,
            content_hash: Some(Hash32::digest(&alpha_bytes)),
            readonly: false,
            version: soak_version(alpha, counter_alpha),
            tombstone: choice % 2 == 0,
        }])
        .expect("soak left tree is valid");
        let right = MerkleTree::from_records([deltaweave_core::SyncRecord {
            schema_version: deltaweave_core::SYNC_RECORD_SCHEMA_V1,
            path: deltaweave_core::WirePath::new(format!("f{choice}.txt"))
                .expect("soak path is portable"),
            kind: deltaweave_core::SyncEntryKind::File,
            size: beta_bytes.len() as u64,
            content_hash: Some(Hash32::digest(&beta_bytes)),
            readonly: false,
            version: soak_version(beta, counter_beta),
            tombstone: choice % 3 == 0,
        }])
        .expect("soak right tree is valid");

        let forward = merge_snapshots(&left, &right).expect("soak forward merge");
        let reverse = merge_snapshots(&right, &left).expect("soak reverse merge");
        assert_eq!(
            forward, reverse,
            "merge stays orientation-independent at soak iteration {iteration}"
        );
        let merged = forward.tree().expect("soak merged tree is valid");
        let idempotent = merge_snapshots(&merged, &merged).expect("soak idempotent merge");
        assert!(
            idempotent.conflicts.is_empty(),
            "re-merging a converged tree stays conflict-free at iteration {iteration}"
        );
        assert_eq!(
            idempotent.tree().expect("idempotent tree").root_hash(),
            merged.root_hash()
        );
    }
}

fn soak_version(replica: ReplicaId, counter: u64) -> deltaweave_core::VersionVector {
    let mut version = deltaweave_core::VersionVector::default();
    version.observe(replica, counter);
    version
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cdc_manifest_soak_never_panics_or_mislabels() {
    let mut state = 0x9e3779b97f4a7c15_u64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for iteration in 0..100_000 {
        let length = (next() % 4096) as usize;
        let mut bytes = vec![0_u8; length];
        for byte in &mut bytes {
            *byte = (next() % 256) as u8;
        }
        let manifest = deltaweave_cdc::manifest_from_reader(&bytes[..], ChunkingProfile::DEFAULT)
            .unwrap_or_else(|_| panic!("cdc soaks a manifest at iteration {iteration}"));
        assert!(manifest.validate().is_ok(), "manifest stays valid");
        assert_eq!(
            manifest.file_hash,
            Hash32::digest(&bytes),
            "file hash matches"
        );
        let mut expected_offset = 0_u64;
        for chunk in &manifest.chunks {
            assert_eq!(chunk.offset, expected_offset, "chunks stay contiguous");
            expected_offset += u64::from(chunk.length);
        }
        assert_eq!(
            manifest.size, expected_offset,
            "manifest size matches chunks"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cdc_and_reconcile_panic_free_on_random_bytes() {
    let mut state = 0xda3e39cb94b95bdb_u64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for _ in 0..100_000 {
        let length = (next() % 2048) as usize;
        let mut bytes = vec![0_u8; length];
        for byte in &mut bytes {
            *byte = (next() % 256) as u8;
        }
        // CDC must either produce a valid manifest or a typed error — never panic.
        if let Ok(manifest) =
            deltaweave_cdc::manifest_from_reader(&bytes[..], ChunkingProfile::DEFAULT)
        {
            assert!(manifest.validate().is_ok());
        }
        // WirePath must reject or accept without panicking on any UTF-8 input.
        if let Ok(text) = std::str::from_utf8(&bytes) {
            let _ = deltaweave_core::WirePath::new(text.to_owned());
        }
    }
}
