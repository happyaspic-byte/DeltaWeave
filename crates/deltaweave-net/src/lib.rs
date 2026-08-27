//! Authenticated iroh transport and resumable delta transfer protocol.

#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fmt,
    fs::{self, OpenOptions},
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use deltaweave_cdc::{manifest_from_path, verify_chunk};
use deltaweave_core::{
    CausalRelation, ChunkingProfile, FileManifest, Hash32, ReplicaId, SyncEntryKind, SyncRecord,
    WirePath,
};
use deltaweave_index::{IndexOptions, LocalIndex};
use deltaweave_reconcile::{MerkleNodeSummary, MerkleTree};
use deltaweave_store::Store;
use iroh::{
    Endpoint, EndpointAddr, EndpointId, RelayUrl, SecretKey, TransportAddr, Watcher,
    endpoint::{Connection, RecvStream, SendStream, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::{info, warn};

/// Versioned ALPN identifier for DeltaWeave's initial push protocol.
pub const ALPN_V1: &[u8] = b"deltaweave/sync/1";
/// Versioned ALPN identifier for Merkle state reconciliation and bidirectional transfer.
pub const ALPN_V2: &[u8] = b"deltaweave/sync/2";
/// Versioned ALPN identifier for CAS-only multi-peer chunk swarming.
pub const ALPN_V3: &[u8] = b"deltaweave/sync/3";
const MAX_CONTROL_FRAME: usize = 16 * 1024 * 1024;
const MAX_CHUNKS_PER_FILE: usize = 250_000;
const MAX_FILE_SIZE: u64 = 16 * 1024 * 1024 * 1024 * 1024;
const CHUNK_WRITE_BATCH: usize = 8;
const CHUNK_WRITE_CONCURRENCY: usize = 8;

/// Whether an endpoint uses internet discovery/relays or direct addresses only.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkMode {
    /// Use iroh's default address lookup and encrypted relay fallback.
    Internet,
    /// Do not contact discovery or relay services; direct addresses are required.
    DirectOnly,
}

/// Result of loading a persistent iroh identity.
#[derive(Clone, Debug)]
pub struct Identity {
    /// Secret key used to authenticate the endpoint.
    pub secret_key: SecretKey,
    /// Whether this call created a new key file.
    pub created: bool,
}

impl Identity {
    /// Public endpoint identifier.
    #[must_use]
    pub fn endpoint_id(&self) -> EndpointId {
        self.secret_key.public()
    }
}

/// Loads an iroh secret key or atomically creates a new owner-only key file.
pub fn load_or_create_identity(path: impl AsRef<Path>) -> Result<Identity> {
    let path = path.as_ref();
    if path.exists() {
        return read_identity(path).map(|secret_key| Identity {
            secret_key,
            created: false,
        });
    }
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }

    let secret_key = SecretKey::generate();
    let encoded = format!("{}\n", hex::encode(secret_key.to_bytes()));
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(path) {
        Ok(mut file) => {
            file.write_all(encoded.as_bytes())?;
            file.sync_all()?;
            Ok(Identity {
                secret_key,
                created: true,
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            read_identity(path).map(|secret_key| Identity {
                secret_key,
                created: false,
            })
        }
        Err(error) => {
            Err(error).with_context(|| format!("failed to create identity file {}", path.display()))
        }
    }
}

fn read_identity(path: &Path) -> Result<SecretKey> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = fs::metadata(path)?.permissions().mode();
        ensure!(
            mode & 0o077 == 0,
            "identity file {} is accessible by group or other users; run chmod 600",
            path.display()
        );
    }
    let encoded = fs::read_to_string(path)
        .with_context(|| format!("failed to read identity file {}", path.display()))?;
    SecretKey::from_str(encoded.trim())
        .with_context(|| format!("invalid identity file {}", path.display()))
}

/// Application-level authorization applied after iroh authenticates the peer key.
#[derive(Clone, Debug)]
pub enum PeerPolicy {
    /// Only explicitly listed endpoint IDs may push files.
    AllowListed(HashSet<EndpointId>),
    /// Accept any authenticated iroh endpoint. Intended only for controlled testing.
    AnyAuthenticated,
}

impl PeerPolicy {
    fn allows(&self, peer: EndpointId) -> bool {
        match self {
            Self::AllowListed(peers) => peers.contains(&peer),
            Self::AnyAuthenticated => true,
        }
    }
}

/// Configuration for a receiving endpoint.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// Persistent node identity.
    pub secret_key: SecretKey,
    /// Root beneath which received files are materialized.
    pub destination_root: PathBuf,
    /// Private DeltaWeave metadata/chunk state directory.
    pub state_root: PathBuf,
    /// Authorized remote endpoint IDs.
    pub peer_policy: PeerPolicy,
    /// Discovery and relay behavior.
    pub network_mode: NetworkMode,
}

/// A running DeltaWeave protocol router.
#[derive(Debug)]
pub struct Server {
    router: Router,
    network_mode: NetworkMode,
}

impl Server {
    /// Returns the endpoint's current authenticated address information.
    #[must_use]
    pub fn endpoint_addr(&self) -> EndpointAddr {
        endpoint_addr_with_local_fallback(self.router.endpoint())
    }

    /// Returns data suitable for displaying or copying to a client.
    #[must_use]
    pub fn address_info(&self) -> AddressInfo {
        let address = self.endpoint_addr();
        AddressInfo {
            endpoint_id: address.id.to_string(),
            direct_addresses: address.ip_addrs().map(|value| value.to_string()).collect(),
            relay_urls: address.relay_urls().map(ToString::to_string).collect(),
        }
    }

    /// Waits for the internet-mode endpoint to establish discovery/relay reachability.
    pub async fn wait_online(&self, timeout: Duration) -> bool {
        if self.network_mode == NetworkMode::DirectOnly {
            return wait_for_direct_address(self.router.endpoint(), timeout)
                .await
                .is_ok();
        }
        tokio::time::timeout(timeout, self.router.endpoint().online())
            .await
            .is_ok()
    }

    /// Gracefully shuts down the router and all connections.
    pub async fn shutdown(self) -> Result<()> {
        self.router.shutdown().await.context("iroh router shutdown")
    }
}

/// Copyable endpoint information printed by the CLI.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AddressInfo {
    /// Authenticated endpoint public key.
    pub endpoint_id: String,
    /// Direct UDP addresses currently known.
    pub direct_addresses: Vec<String>,
    /// Encrypted relay fallback URLs currently known.
    pub relay_urls: Vec<String>,
}

/// Starts a receiving DeltaWeave endpoint.
pub async fn start_server(config: ServerConfig) -> Result<Server> {
    let ServerConfig {
        secret_key,
        destination_root,
        state_root,
        peer_policy,
        network_mode,
    } = config;
    let replica = ReplicaId(Hash32::digest(secret_key.public().as_bytes()));
    let (destination_root, state_root) = prepare_server_roots(&destination_root, &state_root)?;
    let store = Arc::new(Store::open(&state_root)?);
    let index = Arc::new(LocalIndex::open(
        &destination_root,
        state_root.join("index.redb"),
        replica,
        IndexOptions::default(),
    )?);
    let apply_lock = Arc::new(tokio::sync::Mutex::new(()));
    let endpoint = bind_endpoint(
        secret_key,
        network_mode,
        Some(vec![ALPN_V1.to_vec(), ALPN_V2.to_vec(), ALPN_V3.to_vec()]),
    )
    .await?;
    let push_handler = PushHandler {
        store: Arc::clone(&store),
        index: Arc::clone(&index),
        destination_root: destination_root.clone(),
        peer_policy: peer_policy.clone(),
        apply_lock: Arc::clone(&apply_lock),
    };
    let sync_handler = SyncHandler {
        store: Arc::clone(&store),
        index,
        destination_root,
        peer_policy: peer_policy.clone(),
        apply_lock,
    };
    let swarm_handler = SwarmHandler { store, peer_policy };
    let router = Router::builder(endpoint)
        .accept(ALPN_V1, push_handler)
        .accept(ALPN_V2, sync_handler)
        .accept(ALPN_V3, swarm_handler)
        .spawn();
    Ok(Server {
        router,
        network_mode,
    })
}

async fn wait_for_direct_address(endpoint: &Endpoint, limit: Duration) -> Result<()> {
    let mut addresses = endpoint.watch_addr();
    let wait = async {
        loop {
            let current = addresses.get();
            if current.ip_addrs().next().is_some() || !endpoint.bound_sockets().is_empty() {
                return Ok::<(), anyhow::Error>(());
            }
            addresses
                .updated()
                .await
                .context("endpoint address watcher disconnected")?;
        }
    };
    tokio::time::timeout(limit, wait)
        .await
        .context("direct endpoint advertised no address before the readiness deadline")??;
    Ok(())
}

fn endpoint_addr_with_local_fallback(endpoint: &Endpoint) -> EndpointAddr {
    let current = endpoint.addr();
    if current.ip_addrs().next().is_some() {
        return current;
    }

    let relays = current.relay_urls().cloned().map(TransportAddr::Relay);
    let sockets = endpoint.bound_sockets().into_iter().map(|socket| {
        let socket = if socket.ip().is_unspecified() {
            let ip = if socket.is_ipv4() {
                std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
            } else {
                std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)
            };
            SocketAddr::new(ip, socket.port())
        } else {
            socket
        };
        TransportAddr::Ip(socket)
    });
    EndpointAddr::from_parts(current.id, relays.chain(sockets))
}

fn prepare_server_roots(destination_root: &Path, state_root: &Path) -> Result<(PathBuf, PathBuf)> {
    fs::create_dir_all(destination_root).with_context(|| {
        format!(
            "failed to create destination root {}",
            destination_root.display()
        )
    })?;
    fs::create_dir_all(state_root)
        .with_context(|| format!("failed to create state root {}", state_root.display()))?;
    let destination_root = fs::canonicalize(destination_root)?;
    let state_root = fs::canonicalize(state_root)?;
    ensure!(
        !destination_root.starts_with(&state_root) && !state_root.starts_with(&destination_root),
        "destination root {} and state root {} must not overlap",
        destination_root.display(),
        state_root.display()
    );
    Ok((destination_root, state_root))
}

async fn bind_endpoint(
    secret_key: SecretKey,
    mode: NetworkMode,
    alpns: Option<Vec<Vec<u8>>>,
) -> Result<Endpoint> {
    let mut builder = match mode {
        NetworkMode::Internet => Endpoint::builder(presets::N0),
        NetworkMode::DirectOnly => Endpoint::builder(presets::Minimal),
    }
    .secret_key(secret_key);
    if let Some(alpns) = alpns {
        builder = builder.alpns(alpns);
    }
    builder.bind().await.context("failed to bind iroh endpoint")
}

/// Parameters for one file push.
#[derive(Clone, Debug)]
pub struct PushOptions {
    /// Persistent sender identity.
    pub secret_key: SecretKey,
    /// Local source file.
    pub source: PathBuf,
    /// Portable relative destination path.
    pub remote_path: WirePath,
    /// Complete authenticated receiver address.
    pub remote: EndpointAddr,
    /// FastCDC profile.
    pub profile: ChunkingProfile,
    /// Discovery and relay behavior for the sender.
    pub network_mode: NetworkMode,
}

/// Builds an iroh endpoint address from CLI-friendly values.
pub fn endpoint_addr(
    endpoint_id: &str,
    direct_addresses: &[SocketAddr],
    relay_urls: &[String],
) -> Result<EndpointAddr> {
    let endpoint_id = endpoint_id.parse::<EndpointId>()?;
    let relays = relay_urls
        .iter()
        .map(|url| url.parse::<RelayUrl>().map(TransportAddr::Relay))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let addresses = direct_addresses
        .iter()
        .copied()
        .map(TransportAddr::Ip)
        .chain(relays);
    Ok(EndpointAddr::from_parts(endpoint_id, addresses))
}

/// Server-confirmed result of a verified transfer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransferReceipt {
    /// Complete file digest.
    pub file_hash: Hash32,
    /// Manifest identity.
    pub manifest_hash: Hash32,
    /// Unique payload bytes received in this session.
    pub transferred_bytes: u64,
    /// Manifest extents satisfied from the existing chunk store.
    pub reused_extents: usize,
    /// Final portable destination path.
    pub path: WirePath,
}

/// Reusable authenticated client configuration for reconciliation protocol calls.
#[derive(Clone)]
pub struct SyncClient {
    /// Persistent local endpoint identity.
    pub secret_key: SecretKey,
    /// Complete authenticated remote address.
    pub remote: EndpointAddr,
    /// Discovery and relay behavior.
    pub network_mode: NetworkMode,
}

/// One reusable local iroh endpoint for a complete reconciliation pass.
pub struct SyncSession {
    client: SyncClient,
    endpoint: Endpoint,
}

impl fmt::Debug for SyncSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SyncSession")
            .field("client", &self.client)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for SyncClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SyncClient")
            .field("endpoint_id", &self.secret_key.public())
            .field("remote", &self.remote)
            .field("network_mode", &self.network_mode)
            .finish()
    }
}

/// Remote causal snapshot recovered by querying only mismatched Merkle nodes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RemoteSnapshot {
    /// Complete reconstructed remote records in path order.
    pub records: Vec<SyncRecord>,
    /// Verified remote Merkle root.
    pub root_hash: Hash32,
    /// Number of remote records.
    pub record_count: usize,
    /// Merkle nodes requested over the network.
    pub queried_nodes: usize,
}

/// Receipt for one exact causal record applied by a peer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SyncApplyReceipt {
    /// Applied portable path.
    pub path: WirePath,
    /// Digest of the exact adopted [`SyncRecord`].
    pub record_hash: Hash32,
    /// Unique chunk bytes transferred by this operation.
    pub transferred_bytes: u64,
    /// File manifest extents already present in the receiver CAS.
    pub reused_extents: usize,
}

/// File manifest and transfer counters returned after pulling content into a local CAS.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PullReceipt {
    /// Exact remote causal record used for the pull.
    pub record: SyncRecord,
    /// Verified FastCDC manifest now available in the local CAS.
    pub manifest: FileManifest,
    /// Unique payload bytes received from the remote peer.
    pub transferred_bytes: u64,
    /// Manifest extents reused from the local CAS.
    pub reused_extents: usize,
}

impl SyncClient {
    /// Opens one authenticated local endpoint that can serve all calls in a sync pass.
    pub async fn open_session(&self) -> Result<SyncSession> {
        let endpoint = bind_endpoint(self.secret_key.clone(), self.network_mode, None).await?;
        Ok(SyncSession {
            client: self.clone(),
            endpoint,
        })
    }

    /// Reconstructs the remote snapshot while querying only mismatched Merkle subtrees.
    pub async fn fetch_snapshot(&self, local: &MerkleTree) -> Result<RemoteSnapshot> {
        let session = self.open_session().await?;
        let outcome = session.fetch_snapshot(local).await;
        session.close().await;
        outcome
    }

    async fn fetch_snapshot_connected(
        &self,
        endpoint: &Endpoint,
        local: &MerkleTree,
    ) -> Result<RemoteSnapshot> {
        let connection = endpoint
            .connect(self.remote.clone(), ALPN_V2)
            .await
            .context("failed to connect to reconciliation endpoint")?;
        let (mut send, mut receive) = connection
            .open_bi()
            .await
            .context("open reconciliation stream")?;
        let mut queue = VecDeque::from([String::new()]);
        let mut remote_records = BTreeMap::new();
        let mut root = None;
        let mut queried_nodes = 0_usize;

        while let Some(prefix) = queue.pop_front() {
            queried_nodes = queried_nodes
                .checked_add(1)
                .context("Merkle query counter overflow")?;
            ensure!(
                queried_nodes <= 1_000_000,
                "remote Merkle tree exceeds query safety limit"
            );
            write_frame(
                &mut send,
                &SyncWireRequest::QueryNode {
                    prefix: prefix.clone(),
                },
            )
            .await?;
            let summary = match read_frame::<SyncWireResponse>(&mut receive).await? {
                SyncWireResponse::Node { summary } => summary,
                SyncWireResponse::Error { message } => {
                    bail!("remote Merkle query failed: {message}")
                }
                _ => bail!("remote sent an unexpected Merkle response"),
            }
            .with_context(|| format!("remote Merkle prefix {prefix:?} disappeared"))?;
            ensure!(summary.prefix == prefix, "remote Merkle prefix mismatch");
            ensure!(
                summary.record_count <= 1_000_000,
                "remote snapshot exceeds record safety limit"
            );
            if prefix.is_empty() {
                root = Some((summary.hash, summary.record_count));
            }

            let local_summary = local.node_summary(&prefix)?;
            if local_summary.as_ref().is_some_and(|local_summary| {
                local_summary.hash == summary.hash
                    && local_summary.record_count == summary.record_count
            }) {
                insert_snapshot_records(&mut remote_records, local.records_under(&prefix)?)?;
                continue;
            }

            if let Some(record) = summary.record {
                insert_snapshot_record(&mut remote_records, record)?;
            }
            for child in summary.children {
                let child_prefix = if prefix.is_empty() {
                    child.name
                } else {
                    format!("{prefix}/{}", child.name)
                };
                let local_child = local.node_summary(&child_prefix)?;
                if local_child.as_ref().is_some_and(|local_child| {
                    local_child.hash == child.hash && local_child.record_count == child.record_count
                }) {
                    insert_snapshot_records(
                        &mut remote_records,
                        local.records_under(&child_prefix)?,
                    )?;
                } else {
                    queue.push_back(child_prefix);
                }
            }
        }

        write_frame(&mut send, &SyncWireRequest::Finish).await?;
        match read_frame::<SyncWireResponse>(&mut receive).await? {
            SyncWireResponse::Finished => {}
            SyncWireResponse::Error { message } => bail!("remote snapshot failed: {message}"),
            _ => bail!("remote sent an unexpected snapshot completion"),
        }
        send.finish().context("finish snapshot request")?;
        let (root_hash, record_count) = root.context("remote omitted Merkle root")?;
        ensure!(
            remote_records.len() == record_count,
            "reconstructed remote record count mismatch"
        );
        let records: Vec<_> = remote_records.into_values().collect();
        let verified = MerkleTree::from_records(records.clone())?;
        ensure!(
            verified.root_hash() == root_hash,
            "reconstructed remote Merkle root mismatch"
        );
        connection.close(0_u8.into(), b"snapshot complete");
        Ok(RemoteSnapshot {
            records,
            root_hash,
            record_count,
            queried_nodes,
        })
    }

    /// Pushes one live file and makes the receiver adopt the exact supplied causal record.
    pub async fn push_record(
        &self,
        source: impl AsRef<Path>,
        record: SyncRecord,
        profile: ChunkingProfile,
    ) -> Result<SyncApplyReceipt> {
        record.validate()?;
        ensure!(
            !record.tombstone && record.kind == SyncEntryKind::File,
            "push_record requires a live file record"
        );
        let source = source.as_ref().to_path_buf();
        ensure!(source.is_file(), "source is not a regular file");
        let source_for_manifest = source.clone();
        let manifest =
            tokio::task::spawn_blocking(move || manifest_from_path(source_for_manifest, profile))
                .await
                .context("manifest task failed")??;
        ensure!(
            record.size == manifest.size && record.content_hash == Some(manifest.file_hash),
            "source content does not match causal record"
        );

        let session = self.open_session().await?;
        let outcome = session
            .client
            .push_record_connected(&session.endpoint, &source, record, manifest)
            .await;
        session.close().await;
        outcome
    }

    async fn push_record_connected(
        &self,
        endpoint: &Endpoint,
        source_path: &Path,
        record: SyncRecord,
        manifest: FileManifest,
    ) -> Result<SyncApplyReceipt> {
        let connection = endpoint
            .connect(self.remote.clone(), ALPN_V2)
            .await
            .context("failed to connect for causal file push")?;
        let (mut send, mut receive) = connection
            .open_bi()
            .await
            .context("open sync push stream")?;
        write_frame(
            &mut send,
            &SyncWireRequest::PushRecord {
                record: record.clone(),
                manifest: manifest.clone(),
            },
        )
        .await?;
        let missing = match read_frame::<SyncWireResponse>(&mut receive).await? {
            SyncWireResponse::NeedChunks { hashes } => hashes,
            SyncWireResponse::Error { message } => bail!("remote rejected causal push: {message}"),
            _ => bail!("remote sent an unexpected causal push response"),
        };
        let (sent_bytes, reused_extents) =
            send_requested_chunks(&mut send, source_path, &manifest, missing).await?;
        send.finish().context("finish causal file upload")?;
        let receipt = match read_frame::<SyncWireResponse>(&mut receive).await? {
            SyncWireResponse::Applied(receipt) => receipt,
            SyncWireResponse::Error { message } => bail!("remote causal push failed: {message}"),
            _ => bail!("remote sent an unexpected causal push completion"),
        };
        ensure!(
            receipt.path == record.path,
            "causal push receipt path mismatch"
        );
        ensure!(
            receipt.record_hash == record.logical_hash(),
            "causal push receipt record mismatch"
        );
        ensure!(
            receipt.transferred_bytes == sent_bytes && receipt.reused_extents == reused_extents,
            "causal push receipt counters mismatch"
        );
        connection.close(0_u8.into(), b"causal push complete");
        Ok(receipt)
    }

    /// Pulls one exact remote live-file record into `store` without publishing a path yet.
    pub async fn pull_record(&self, record: SyncRecord, store: Arc<Store>) -> Result<PullReceipt> {
        record.validate()?;
        ensure!(
            !record.tombstone && record.kind == SyncEntryKind::File,
            "pull_record requires a live file record"
        );
        let session = self.open_session().await?;
        let outcome = session.pull_record(record, store).await;
        session.close().await;
        outcome
    }

    async fn pull_record_connected(
        &self,
        endpoint: &Endpoint,
        expected: SyncRecord,
        store: Arc<Store>,
    ) -> Result<PullReceipt> {
        let connection = endpoint
            .connect(self.remote.clone(), ALPN_V2)
            .await
            .context("failed to connect for causal file pull")?;
        let (mut send, mut receive) = connection
            .open_bi()
            .await
            .context("open sync pull stream")?;
        write_frame(
            &mut send,
            &SyncWireRequest::PullRecord {
                record: expected.clone(),
            },
        )
        .await?;
        let (record, manifest) = match read_frame::<SyncWireResponse>(&mut receive).await? {
            SyncWireResponse::PullManifest { record, manifest } => (record, manifest),
            SyncWireResponse::Error { message } => bail!("remote rejected causal pull: {message}"),
            _ => bail!("remote sent an unexpected causal pull response"),
        };
        ensure!(record == expected, "remote path changed after snapshot");
        manifest.validate()?;
        ensure!(
            manifest.size == record.size && Some(manifest.file_hash) == record.content_hash,
            "remote pull manifest does not match causal record"
        );
        let inventory_store = Arc::clone(&store);
        let inventory_manifest = manifest.clone();
        let missing = tokio::task::spawn_blocking(move || {
            inventory_store.missing_chunks(&inventory_manifest)
        })
        .await
        .context("local chunk inventory task failed")?;
        let missing_set: HashSet<_> = missing.iter().copied().collect();
        let reused_extents = manifest
            .chunks
            .iter()
            .filter(|chunk| !missing_set.contains(&chunk.hash))
            .count();
        write_frame(
            &mut send,
            &SyncWireRequest::NeedChunks {
                hashes: missing.clone(),
            },
        )
        .await?;
        send.finish().context("finish causal pull request")?;
        let descriptors: HashMap<_, _> = manifest
            .chunks
            .iter()
            .map(|chunk| (chunk.hash, chunk.clone()))
            .collect();
        let mut transferred_bytes = 0_u64;
        let mut writer = ChunkWritePipeline::new(Arc::clone(&store), CHUNK_WRITE_CONCURRENCY);
        for expected_hash in missing {
            let header: ChunkHeader = read_frame(&mut receive).await?;
            ensure!(
                header.hash == expected_hash,
                "remote sent an out-of-order chunk"
            );
            let descriptor = descriptors
                .get(&header.hash)
                .context("remote sent a chunk absent from its manifest")?;
            ensure!(
                header.length == descriptor.length,
                "remote chunk length mismatch"
            );
            let mut bytes = vec![0_u8; header.length as usize];
            receive.read_exact(&mut bytes).await?;
            verify_chunk(descriptor, &bytes)?;
            writer.push(header.hash, bytes).await?;
            transferred_bytes = transferred_bytes
                .checked_add(u64::from(header.length))
                .context("pulled-byte counter overflow")?;
        }
        writer.finish().await?;
        let receipt = match read_frame::<SyncWireResponse>(&mut receive).await? {
            SyncWireResponse::Applied(receipt) => receipt,
            SyncWireResponse::Error { message } => bail!("remote causal pull failed: {message}"),
            _ => bail!("remote sent an unexpected causal pull completion"),
        };
        ensure!(
            receipt.record_hash == record.logical_hash()
                && receipt.transferred_bytes == transferred_bytes
                && receipt.reused_extents == reused_extents,
            "causal pull receipt mismatch"
        );
        connection.close(0_u8.into(), b"causal pull complete");
        Ok(PullReceipt {
            record,
            manifest,
            transferred_bytes,
            reused_extents,
        })
    }

    /// Applies a directory or tombstone record without transferring file content.
    pub async fn apply_metadata(&self, record: SyncRecord) -> Result<SyncApplyReceipt> {
        let session = self.open_session().await?;
        let outcome = session.apply_metadata(record).await;
        session.close().await;
        outcome
    }

    async fn apply_metadata_connected(
        &self,
        endpoint: &Endpoint,
        record: SyncRecord,
    ) -> Result<SyncApplyReceipt> {
        record.validate()?;
        ensure!(
            record.tombstone || record.kind == SyncEntryKind::Directory,
            "metadata apply supports only directories and tombstones"
        );
        let connection = endpoint
            .connect(self.remote.clone(), ALPN_V2)
            .await
            .context("failed to connect for metadata apply")?;
        let outcome = async {
            let (mut send, mut receive) =
                connection.open_bi().await.context("open metadata stream")?;
            write_frame(
                &mut send,
                &SyncWireRequest::ApplyMetadata {
                    record: record.clone(),
                },
            )
            .await?;
            send.finish().context("finish metadata request")?;
            match read_frame::<SyncWireResponse>(&mut receive).await? {
                SyncWireResponse::Applied(receipt) => {
                    ensure!(
                        receipt.path == record.path && receipt.record_hash == record.logical_hash(),
                        "metadata receipt mismatch"
                    );
                    Ok(receipt)
                }
                SyncWireResponse::Error { message } => {
                    bail!("remote metadata apply failed: {message}")
                }
                _ => bail!("remote sent an unexpected metadata response"),
            }
        }
        .await;
        connection.close(0_u8.into(), b"metadata complete");
        outcome
    }
}

impl SyncSession {
    /// Reconstructs the remote snapshot through this reusable endpoint.
    pub async fn fetch_snapshot(&self, local: &MerkleTree) -> Result<RemoteSnapshot> {
        self.client
            .fetch_snapshot_connected(&self.endpoint, local)
            .await
    }

    /// Pushes one exact live-file record through this reusable endpoint.
    pub async fn push_record(
        &self,
        source: impl AsRef<Path>,
        record: SyncRecord,
        profile: ChunkingProfile,
    ) -> Result<SyncApplyReceipt> {
        record.validate()?;
        ensure!(
            !record.tombstone && record.kind == SyncEntryKind::File,
            "push_record requires a live file record"
        );
        let source = source.as_ref().to_path_buf();
        ensure!(source.is_file(), "source is not a regular file");
        let source_for_manifest = source.clone();
        let manifest =
            tokio::task::spawn_blocking(move || manifest_from_path(source_for_manifest, profile))
                .await
                .context("manifest task failed")??;
        ensure!(
            record.size == manifest.size && record.content_hash == Some(manifest.file_hash),
            "source content does not match causal record"
        );
        self.client
            .push_record_connected(&self.endpoint, &source, record, manifest)
            .await
    }

    /// Pulls one exact live-file record through this reusable endpoint.
    pub async fn pull_record(&self, record: SyncRecord, store: Arc<Store>) -> Result<PullReceipt> {
        record.validate()?;
        ensure!(
            !record.tombstone && record.kind == SyncEntryKind::File,
            "pull_record requires a live file record"
        );
        self.client
            .pull_record_connected(&self.endpoint, record, store)
            .await
    }

    /// Applies a directory or tombstone record through this reusable endpoint.
    pub async fn apply_metadata(&self, record: SyncRecord) -> Result<SyncApplyReceipt> {
        self.client
            .apply_metadata_connected(&self.endpoint, record)
            .await
    }

    /// Gracefully closes the reusable local endpoint.
    pub async fn close(self) {
        self.endpoint.close().await;
    }
}

fn insert_snapshot_records(
    records: &mut BTreeMap<WirePath, SyncRecord>,
    incoming: impl IntoIterator<Item = SyncRecord>,
) -> Result<()> {
    for record in incoming {
        insert_snapshot_record(records, record)?;
    }
    Ok(())
}

fn insert_snapshot_record(
    records: &mut BTreeMap<WirePath, SyncRecord>,
    record: SyncRecord,
) -> Result<()> {
    record.validate()?;
    if let Some(existing) = records.insert(record.path.clone(), record.clone()) {
        ensure!(
            existing == record,
            "remote snapshot repeated a path inconsistently"
        );
    }
    Ok(())
}

/// Sends one file, transmitting only chunks the receiver reports missing.
pub async fn push_file(options: PushOptions) -> Result<TransferReceipt> {
    ensure!(options.source.is_file(), "source is not a regular file");
    let source_for_manifest = options.source.clone();
    let profile = options.profile;
    let manifest =
        tokio::task::spawn_blocking(move || manifest_from_path(source_for_manifest, profile))
            .await
            .context("manifest task failed")??;

    let endpoint = bind_endpoint(options.secret_key, options.network_mode, None).await?;
    let result = push_connected(
        &endpoint,
        &options.source,
        options.remote_path,
        options.remote,
        manifest,
    )
    .await;
    endpoint.close().await;
    result
}

async fn push_connected(
    endpoint: &Endpoint,
    source_path: &Path,
    remote_path: WirePath,
    remote: EndpointAddr,
    manifest: FileManifest,
) -> Result<TransferReceipt> {
    let expected_path = remote_path.clone();
    let connection = endpoint
        .connect(remote, ALPN_V1)
        .await
        .context("failed to connect to receiver")?;
    let (mut send, mut receive) = connection.open_bi().await.context("open transfer stream")?;
    write_frame(
        &mut send,
        &WireRequest::Push {
            path: remote_path,
            manifest: manifest.clone(),
        },
    )
    .await?;

    let response: WireResponse = read_frame(&mut receive).await?;
    let missing = match response {
        WireResponse::NeedChunks { hashes } => hashes,
        WireResponse::Rejected { message } => bail!("receiver rejected peer: {message}"),
        WireResponse::Error { message } => bail!("receiver rejected transfer: {message}"),
        WireResponse::Complete(_) => bail!("receiver completed before requesting chunks"),
    };

    let descriptors: HashMap<_, _> = manifest
        .chunks
        .iter()
        .map(|chunk| (chunk.hash, chunk.clone()))
        .collect();
    let missing_set: HashSet<_> = missing.iter().copied().collect();
    ensure!(
        missing_set.len() == missing.len(),
        "receiver requested duplicate chunks"
    );
    let expected_reused_extents = manifest
        .chunks
        .iter()
        .filter(|chunk| !missing_set.contains(&chunk.hash))
        .count();
    let mut source = tokio::fs::File::open(source_path).await?;
    let mut sent = HashSet::new();
    let mut sent_bytes = 0_u64;
    for hash in missing {
        ensure!(
            sent.insert(hash),
            "receiver requested duplicate chunk {hash}"
        );
        let descriptor = descriptors
            .get(&hash)
            .with_context(|| format!("receiver requested unknown chunk {hash}"))?;
        let mut bytes = vec![0_u8; descriptor.length as usize];
        source
            .seek(std::io::SeekFrom::Start(descriptor.offset))
            .await?;
        source.read_exact(&mut bytes).await?;
        verify_chunk(descriptor, &bytes)?;
        write_frame(
            &mut send,
            &ChunkHeader {
                hash,
                length: descriptor.length,
            },
        )
        .await?;
        send.write_all(&bytes).await?;
        sent_bytes = sent_bytes
            .checked_add(u64::from(descriptor.length))
            .context("sent-byte counter overflow")?;
    }
    send.finish().context("finish transfer upload")?;

    match read_frame(&mut receive).await? {
        WireResponse::Complete(receipt) => {
            ensure!(
                receipt.file_hash == manifest.file_hash,
                "receipt file hash mismatch"
            );
            ensure!(
                receipt.manifest_hash == manifest.manifest_hash(),
                "receipt manifest hash mismatch"
            );
            ensure!(
                receipt.path == expected_path,
                "receipt destination path mismatch"
            );
            ensure!(
                receipt.transferred_bytes == sent_bytes,
                "receipt transferred-byte count mismatch"
            );
            ensure!(
                receipt.reused_extents == expected_reused_extents,
                "receipt reused-extent count mismatch"
            );
            connection.close(0_u8.into(), b"complete");
            Ok(receipt)
        }
        WireResponse::Error { message } => bail!("receiver failed transfer: {message}"),
        WireResponse::Rejected { message } => bail!("receiver rejected peer: {message}"),
        WireResponse::NeedChunks { .. } => bail!("receiver sent a second chunk request"),
    }
}

async fn send_requested_chunks(
    send: &mut SendStream,
    source_path: &Path,
    manifest: &FileManifest,
    missing: Vec<Hash32>,
) -> Result<(u64, usize)> {
    let descriptors: HashMap<_, _> = manifest
        .chunks
        .iter()
        .map(|chunk| (chunk.hash, chunk.clone()))
        .collect();
    let missing_set: HashSet<_> = missing.iter().copied().collect();
    ensure!(
        missing_set.len() == missing.len(),
        "receiver requested duplicate chunks"
    );
    let reused_extents = manifest
        .chunks
        .iter()
        .filter(|chunk| !missing_set.contains(&chunk.hash))
        .count();
    let mut source = tokio::fs::File::open(source_path).await?;
    let mut sent_bytes = 0_u64;
    for hash in missing {
        let descriptor = descriptors
            .get(&hash)
            .with_context(|| format!("receiver requested unknown chunk {hash}"))?;
        let mut bytes = vec![0_u8; descriptor.length as usize];
        source
            .seek(std::io::SeekFrom::Start(descriptor.offset))
            .await?;
        source.read_exact(&mut bytes).await?;
        verify_chunk(descriptor, &bytes)?;
        write_frame(
            send,
            &ChunkHeader {
                hash,
                length: descriptor.length,
            },
        )
        .await?;
        send.write_all(&bytes).await?;
        sent_bytes = sent_bytes
            .checked_add(u64::from(descriptor.length))
            .context("sent-byte counter overflow")?;
    }
    Ok((sent_bytes, reused_extents))
}

#[derive(Clone)]
struct PushHandler {
    store: Arc<Store>,
    index: Arc<LocalIndex>,
    destination_root: PathBuf,
    peer_policy: PeerPolicy,
    apply_lock: Arc<tokio::sync::Mutex<()>>,
}

impl fmt::Debug for PushHandler {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PushHandler")
            .field("destination_root", &self.destination_root)
            .field("peer_policy", &self.peer_policy)
            .finish_non_exhaustive()
    }
}

impl ProtocolHandler for PushHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let peer = connection.remote_id();
        if !self.peer_policy.allows(peer) {
            warn!(%peer, "rejected unauthorized DeltaWeave peer");
            connection.close(0_u8.into(), b"endpoint ID is not allow-listed");
            return Ok(());
        }

        let (mut send, mut receive) = connection.accept_bi().await?;
        info!(%peer, "accepted DeltaWeave peer");
        if let Err(error) = self.handle_push(&mut send, &mut receive).await {
            warn!(%peer, error = %error, "DeltaWeave transfer failed");
            let message = truncate_error(&error.to_string());
            let _ = write_frame(&mut send, &WireResponse::Error { message }).await;
            let _ = send.finish();
            connection.closed().await;
            return Err(AcceptError::from_err(std::io::Error::other(
                error.to_string(),
            )));
        }
        send.finish()?;
        connection.closed().await;
        Ok(())
    }
}

impl PushHandler {
    async fn handle_push(&self, send: &mut SendStream, receive: &mut RecvStream) -> Result<()> {
        let request: WireRequest = read_frame(receive).await?;
        let (path, manifest) = match request {
            WireRequest::Push { path, manifest } => (path, manifest),
        };
        manifest.validate()?;
        ensure!(
            manifest.size <= MAX_FILE_SIZE,
            "file exceeds protocol size limit"
        );
        ensure!(
            manifest.chunks.len() <= MAX_CHUNKS_PER_FILE,
            "manifest exceeds chunk-count limit"
        );

        let inventory_store = Arc::clone(&self.store);
        let inventory_manifest = manifest.clone();
        let missing = tokio::task::spawn_blocking(move || {
            inventory_store.missing_chunks(&inventory_manifest)
        })
        .await
        .context("chunk inventory task failed")?;
        let missing_set: HashSet<_> = missing.iter().copied().collect();
        let reused_extents = manifest
            .chunks
            .iter()
            .filter(|chunk| !missing_set.contains(&chunk.hash))
            .count();
        write_frame(
            send,
            &WireResponse::NeedChunks {
                hashes: missing.clone(),
            },
        )
        .await?;

        let descriptor_by_hash: HashMap<_, _> = manifest
            .chunks
            .iter()
            .map(|chunk| (chunk.hash, chunk.clone()))
            .collect();
        let mut transferred_bytes = 0_u64;
        let mut writer = ChunkWritePipeline::new(Arc::clone(&self.store), CHUNK_WRITE_CONCURRENCY);
        for expected_hash in missing {
            let header: ChunkHeader = read_frame(receive).await?;
            ensure!(
                header.hash == expected_hash,
                "out-of-order chunk: expected {expected_hash}, got {}",
                header.hash
            );
            let descriptor = descriptor_by_hash
                .get(&header.hash)
                .context("requested hash disappeared from manifest")?;
            ensure!(
                header.length == descriptor.length,
                "chunk header length does not match manifest"
            );
            ensure!(
                header.length <= manifest.profile.max_size,
                "chunk exceeds configured maximum"
            );
            let mut bytes = vec![0_u8; header.length as usize];
            receive.read_exact(&mut bytes).await?;
            verify_chunk(descriptor, &bytes)?;
            writer.push(header.hash, bytes).await?;
            transferred_bytes = transferred_bytes
                .checked_add(u64::from(header.length))
                .context("transferred-byte counter overflow")?;
        }
        writer.finish().await?;

        let _apply_guard = self.apply_lock.lock().await;
        let store = Arc::clone(&self.store);
        let root = self.destination_root.clone();
        let materialize_manifest = manifest.clone();
        let materialize_path = path.clone();
        tokio::task::spawn_blocking(move || {
            prepare_destination_kind(
                &store,
                &materialize_path,
                &root,
                SyncEntryKind::File,
                materialize_manifest.manifest_hash(),
            )?;
            store.materialize(&materialize_manifest, &materialize_path, root)
        })
        .await
        .context("materialization task failed")??;

        let index = Arc::clone(&self.index);
        tokio::task::spawn_blocking(move || {
            let report = index.scan()?;
            ensure_index_report_safe(&report)?;
            Ok::<_, anyhow::Error>(report)
        })
        .await
        .context("receiver index task failed")??;

        write_frame(
            send,
            &WireResponse::Complete(TransferReceipt {
                file_hash: manifest.file_hash,
                manifest_hash: manifest.manifest_hash(),
                transferred_bytes,
                reused_extents,
                path,
            }),
        )
        .await?;
        Ok(())
    }
}

#[derive(Clone)]
struct SyncHandler {
    store: Arc<Store>,
    index: Arc<LocalIndex>,
    destination_root: PathBuf,
    peer_policy: PeerPolicy,
    apply_lock: Arc<tokio::sync::Mutex<()>>,
}

impl fmt::Debug for SyncHandler {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SyncHandler")
            .field("destination_root", &self.destination_root)
            .field("peer_policy", &self.peer_policy)
            .finish_non_exhaustive()
    }
}

impl ProtocolHandler for SyncHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let peer = connection.remote_id();
        if !self.peer_policy.allows(peer) {
            warn!(%peer, "rejected unauthorized DeltaWeave reconciliation peer");
            connection.close(0_u8.into(), b"endpoint ID is not allow-listed");
            return Ok(());
        }

        let (mut send, mut receive) = connection.accept_bi().await?;
        info!(%peer, "accepted DeltaWeave reconciliation peer");
        let outcome = async {
            match read_frame::<SyncWireRequest>(&mut receive).await? {
                request @ SyncWireRequest::QueryNode { .. } => {
                    self.handle_query_session(request, &mut send, &mut receive)
                        .await
                }
                SyncWireRequest::PullRecord { record } => {
                    self.handle_pull(record, &mut send, &mut receive).await
                }
                SyncWireRequest::PushRecord { record, manifest } => {
                    self.handle_push_record(record, manifest, &mut send, &mut receive)
                        .await
                }
                SyncWireRequest::ApplyMetadata { record } => {
                    self.handle_metadata(record, &mut send).await
                }
                SyncWireRequest::NeedChunks { .. } | SyncWireRequest::Finish => {
                    bail!("unexpected reconciliation session message")
                }
            }
        }
        .await;
        if let Err(error) = outcome {
            warn!(%peer, error = %error, "DeltaWeave reconciliation operation failed");
            let message = truncate_error(&error.to_string());
            let _ = write_frame(&mut send, &SyncWireResponse::Error { message }).await;
            let _ = send.finish();
            connection.closed().await;
            return Err(AcceptError::from_err(std::io::Error::other(
                error.to_string(),
            )));
        }
        send.finish()?;
        connection.closed().await;
        Ok(())
    }
}

impl SyncHandler {
    async fn handle_query_session(
        &self,
        first: SyncWireRequest,
        send: &mut SendStream,
        receive: &mut RecvStream,
    ) -> Result<()> {
        let index = Arc::clone(&self.index);
        let records = tokio::task::spawn_blocking(move || {
            let report = index.scan()?;
            ensure_index_report_safe(&report)?;
            index.sync_records()
        })
        .await
        .context("snapshot index task failed")??;
        ensure!(
            records.len() <= 1_000_000,
            "snapshot exceeds record safety limit"
        );
        let tree = MerkleTree::from_records(records)?;
        let mut request = first;
        loop {
            match request {
                SyncWireRequest::QueryNode { prefix } => {
                    let summary = tree.node_summary(&prefix)?;
                    write_frame(send, &SyncWireResponse::Node { summary }).await?;
                }
                SyncWireRequest::Finish => {
                    write_frame(send, &SyncWireResponse::Finished).await?;
                    return Ok(());
                }
                _ => bail!("only Merkle queries are valid in a snapshot session"),
            }
            request = read_frame(receive).await?;
        }
    }

    async fn handle_pull(
        &self,
        expected: SyncRecord,
        send: &mut SendStream,
        receive: &mut RecvStream,
    ) -> Result<()> {
        expected.validate()?;
        ensure!(
            !expected.tombstone && expected.kind == SyncEntryKind::File,
            "pull requires a live file record"
        );
        let index = Arc::clone(&self.index);
        let path = expected.path.clone();
        let current = tokio::task::spawn_blocking(move || {
            let report = index.scan()?;
            ensure_index_report_safe(&report)?;
            Ok::<_, anyhow::Error>(index.get(&path)?.map(|record| record.to_sync_record()))
        })
        .await
        .context("pull index task failed")??
        .context("requested path is absent")?;
        ensure!(current == expected, "requested path changed after snapshot");

        let source = sync_local_path(&self.destination_root, &expected.path);
        let source_for_manifest = source.clone();
        let manifest = tokio::task::spawn_blocking(move || {
            manifest_from_path(source_for_manifest, ChunkingProfile::DEFAULT)
        })
        .await
        .context("pull manifest task failed")??;
        ensure!(
            manifest.size == expected.size && Some(manifest.file_hash) == expected.content_hash,
            "indexed file changed while preparing pull"
        );
        write_frame(
            send,
            &SyncWireResponse::PullManifest {
                record: expected.clone(),
                manifest: manifest.clone(),
            },
        )
        .await?;
        let missing = match read_frame::<SyncWireRequest>(receive).await? {
            SyncWireRequest::NeedChunks { hashes } => hashes,
            _ => bail!("pull client did not send a chunk request"),
        };
        let (transferred_bytes, reused_extents) =
            send_requested_chunks(send, &source, &manifest, missing).await?;
        write_frame(
            send,
            &SyncWireResponse::Applied(SyncApplyReceipt {
                path: expected.path.clone(),
                record_hash: expected.logical_hash(),
                transferred_bytes,
                reused_extents,
            }),
        )
        .await?;
        Ok(())
    }

    async fn handle_push_record(
        &self,
        record: SyncRecord,
        manifest: FileManifest,
        send: &mut SendStream,
        receive: &mut RecvStream,
    ) -> Result<()> {
        record.validate()?;
        manifest.validate()?;
        ensure!(
            !record.tombstone && record.kind == SyncEntryKind::File,
            "causal push requires a live file record"
        );
        ensure!(
            manifest.size == record.size && Some(manifest.file_hash) == record.content_hash,
            "causal push manifest does not match record"
        );
        ensure!(
            manifest.size <= MAX_FILE_SIZE,
            "file exceeds protocol size limit"
        );
        ensure!(
            manifest.chunks.len() <= MAX_CHUNKS_PER_FILE,
            "manifest exceeds chunk-count limit"
        );

        let inventory_store = Arc::clone(&self.store);
        let inventory_manifest = manifest.clone();
        let missing = tokio::task::spawn_blocking(move || {
            inventory_store.missing_chunks(&inventory_manifest)
        })
        .await
        .context("chunk inventory task failed")?;
        let missing_set: HashSet<_> = missing.iter().copied().collect();
        let reused_extents = manifest
            .chunks
            .iter()
            .filter(|chunk| !missing_set.contains(&chunk.hash))
            .count();
        write_frame(
            send,
            &SyncWireResponse::NeedChunks {
                hashes: missing.clone(),
            },
        )
        .await?;

        let transferred_bytes = receive_chunks(&self.store, receive, &manifest, missing).await?;
        let _apply_guard = self.apply_lock.lock().await;
        let index = Arc::clone(&self.index);
        let candidate = record.clone();
        tokio::task::spawn_blocking(move || ensure_causally_applicable(&index, &candidate))
            .await
            .context("causal precondition task failed")??;
        let store = Arc::clone(&self.store);
        let root = self.destination_root.clone();
        let path = record.path.clone();
        let operation_hash = record.logical_hash();
        let materialize_manifest = manifest;
        tokio::task::spawn_blocking(move || {
            prepare_destination_kind(&store, &path, &root, SyncEntryKind::File, operation_hash)?;
            store.materialize(&materialize_manifest, &path, root)
        })
        .await
        .context("causal materialization task failed")??;
        let installed = sync_local_path(&self.destination_root, &record.path);
        apply_readonly(&installed, record.readonly)?;
        let index = Arc::clone(&self.index);
        let adopted = record.clone();
        tokio::task::spawn_blocking(move || index.adopt_verified_record(&adopted))
            .await
            .context("causal index adoption task failed")??;
        write_frame(
            send,
            &SyncWireResponse::Applied(SyncApplyReceipt {
                path: record.path.clone(),
                record_hash: record.logical_hash(),
                transferred_bytes,
                reused_extents,
            }),
        )
        .await?;
        Ok(())
    }

    async fn handle_metadata(&self, record: SyncRecord, send: &mut SendStream) -> Result<()> {
        record.validate()?;
        ensure!(
            record.tombstone || record.kind == SyncEntryKind::Directory,
            "metadata apply supports only directories and tombstones"
        );
        let _apply_guard = self.apply_lock.lock().await;
        let index = Arc::clone(&self.index);
        let candidate = record.clone();
        tokio::task::spawn_blocking(move || ensure_causally_applicable(&index, &candidate))
            .await
            .context("metadata causal precondition task failed")??;
        let store = Arc::clone(&self.store);
        let root = self.destination_root.clone();
        let apply_record = record.clone();
        tokio::task::spawn_blocking(move || {
            if apply_record.tombstone {
                store.remove_path(&apply_record.path, root, apply_record.logical_hash())?;
            } else {
                prepare_destination_kind(
                    &store,
                    &apply_record.path,
                    &root,
                    SyncEntryKind::Directory,
                    apply_record.logical_hash(),
                )?;
                let directory = store.materialize_directory(&apply_record.path, root)?;
                apply_readonly(&directory, apply_record.readonly)?;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("metadata filesystem task failed")??;
        let index = Arc::clone(&self.index);
        let adopted = record.clone();
        tokio::task::spawn_blocking(move || index.adopt_verified_record(&adopted))
            .await
            .context("metadata index adoption task failed")??;
        write_frame(
            send,
            &SyncWireResponse::Applied(SyncApplyReceipt {
                path: record.path.clone(),
                record_hash: record.logical_hash(),
                transferred_bytes: 0,
                reused_extents: 0,
            }),
        )
        .await?;
        Ok(())
    }
}

fn ensure_causally_applicable(index: &LocalIndex, incoming: &SyncRecord) -> Result<()> {
    let report = index.scan()?;
    ensure_index_report_safe(&report)?;
    let Some(current) = index
        .get(&incoming.path)?
        .map(|record| record.to_sync_record())
    else {
        return Ok(());
    };
    match current.version.relation(&incoming.version) {
        CausalRelation::Before => Ok(()),
        CausalRelation::Equal if current.same_state(incoming) => Ok(()),
        CausalRelation::Equal => {
            bail!("incoming record reuses an existing causal version for different state")
        }
        CausalRelation::After => bail!("incoming record is causally stale"),
        CausalRelation::Concurrent => {
            bail!("incoming record is concurrent; reconcile it before applying")
        }
    }
}

fn ensure_index_report_safe(report: &deltaweave_index::ScanReport) -> Result<()> {
    ensure!(
        report.collisions.is_empty(),
        "filesystem scan has {} cross-platform path collision(s)",
        report.collisions.len()
    );
    ensure!(
        report.issues.is_empty() && report.retries_queued == 0,
        "filesystem scan is incomplete: {} issue(s), {} retry/retries queued",
        report.issues.len(),
        report.retries_queued
    );
    Ok(())
}

fn prepare_destination_kind(
    store: &Store,
    path: &WirePath,
    root: &Path,
    desired: SyncEntryKind,
    operation_hash: Hash32,
) -> Result<()> {
    let destination = sync_local_path(root, path);
    let metadata = match fs::symlink_metadata(&destination) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let already_compatible = match desired {
        SyncEntryKind::File => metadata.is_file() && !metadata.file_type().is_symlink(),
        SyncEntryKind::Directory => metadata.is_dir() && !metadata.file_type().is_symlink(),
        SyncEntryKind::Symlink | SyncEntryKind::Other => false,
    };
    if !already_compatible {
        store.remove_path(path, root, operation_hash)?;
    }
    Ok(())
}

struct ChunkWritePipeline {
    store: Arc<Store>,
    max_inflight: usize,
    inflight: Vec<tokio::task::JoinHandle<Result<usize>>>,
    pending: Vec<(Hash32, Vec<u8>)>,
}

impl ChunkWritePipeline {
    fn new(store: Arc<Store>, max_inflight: usize) -> Self {
        Self {
            store,
            max_inflight: max_inflight.max(1),
            inflight: Vec::new(),
            pending: Vec::new(),
        }
    }

    async fn push(&mut self, hash: Hash32, bytes: Vec<u8>) -> Result<()> {
        self.pending.push((hash, bytes));
        if self.pending.len() >= CHUNK_WRITE_BATCH {
            self.flush_pending().await?;
        }
        Ok(())
    }

    async fn finish(mut self) -> Result<()> {
        if !self.pending.is_empty() {
            self.flush_pending().await?;
        }
        drain_chunk_tasks(self.inflight).await
    }

    async fn flush_pending(&mut self) -> Result<()> {
        if self.inflight.len() >= self.max_inflight {
            let task = self.inflight.remove(0);
            task.await.context("chunk-store task failed")??;
        }
        let batch = std::mem::take(&mut self.pending);
        let store = Arc::clone(&self.store);
        self.inflight.push(tokio::task::spawn_blocking(move || {
            store.chunks().put_verified_batch(batch)
        }));
        Ok(())
    }
}

async fn drain_chunk_tasks(tasks: Vec<tokio::task::JoinHandle<Result<usize>>>) -> Result<()> {
    let mut first_error = None;
    for task in tasks {
        match task.await.context("chunk-store task failed") {
            Ok(Ok(_)) => {}
            Ok(Err(error)) | Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    Ok(())
}

async fn receive_chunks(
    store: &Arc<Store>,
    receive: &mut RecvStream,
    manifest: &FileManifest,
    missing: Vec<Hash32>,
) -> Result<u64> {
    let descriptor_by_hash: HashMap<_, _> = manifest
        .chunks
        .iter()
        .map(|chunk| (chunk.hash, chunk.clone()))
        .collect();
    let mut transferred_bytes = 0_u64;
    let mut writer = ChunkWritePipeline::new(Arc::clone(store), CHUNK_WRITE_CONCURRENCY);
    for expected_hash in missing {
        let header: ChunkHeader = read_frame(receive).await?;
        ensure!(
            header.hash == expected_hash,
            "out-of-order chunk: expected {expected_hash}, got {}",
            header.hash
        );
        let descriptor = descriptor_by_hash
            .get(&header.hash)
            .context("requested hash disappeared from manifest")?;
        ensure!(header.length == descriptor.length, "chunk length mismatch");
        ensure!(
            header.length <= manifest.profile.max_size,
            "chunk exceeds configured maximum"
        );
        transferred_bytes = transferred_bytes
            .checked_add(u64::from(header.length))
            .context("transfer byte count overflow")?;
        let mut bytes = vec![0_u8; header.length as usize];
        receive.read_exact(&mut bytes).await?;
        verify_chunk(descriptor, &bytes)?;
        writer.push(header.hash, bytes).await?;
    }
    writer.finish().await?;
    Ok(transferred_bytes)
}

fn sync_local_path(root: &Path, path: &WirePath) -> PathBuf {
    let mut local = root.to_path_buf();
    for component in path.components() {
        local.push(component);
    }
    local
}

fn apply_readonly(path: &Path, readonly: bool) -> Result<()> {
    let mut permissions = fs::symlink_metadata(path)?.permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = permissions.mode();
        permissions.set_mode(if readonly {
            mode & !0o222
        } else {
            mode | 0o200
        });
    }
    #[cfg(not(unix))]
    permissions.set_readonly(readonly);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
enum SyncWireRequest {
    QueryNode {
        prefix: String,
    },
    PullRecord {
        record: SyncRecord,
    },
    PushRecord {
        record: SyncRecord,
        manifest: FileManifest,
    },
    ApplyMetadata {
        record: SyncRecord,
    },
    NeedChunks {
        hashes: Vec<Hash32>,
    },
    Finish,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
enum SyncWireResponse {
    Node {
        summary: Option<MerkleNodeSummary>,
    },
    PullManifest {
        record: SyncRecord,
        manifest: FileManifest,
    },
    NeedChunks {
        hashes: Vec<Hash32>,
    },
    Applied(SyncApplyReceipt),
    Finished,
    Error {
        message: String,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
enum WireRequest {
    Push {
        path: WirePath,
        manifest: FileManifest,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
enum WireResponse {
    NeedChunks { hashes: Vec<Hash32> },
    Complete(TransferReceipt),
    Rejected { message: String },
    Error { message: String },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ChunkHeader {
    hash: Hash32,
    length: u32,
}

const SWARM_PROTOCOL_VERSION: u16 = 3;
const SWARM_MAX_INFLIGHT: u16 = 64;
const SWARM_MAX_WANT: usize = 64;

/// Result of an authorized swarm Hello handshake.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SwarmHelloOk {
    /// Protocol version advertised by the remote swarm handler.
    pub protocol_version: u16,
    /// Maximum concurrent in-flight chunk requests accepted by the remote.
    pub max_inflight: u16,
}

/// Result of requesting CAS chunks from an authorized swarm peer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SwarmChunkFetch {
    /// Verified chunks returned by the remote peer.
    pub chunks: Vec<(Hash32, Vec<u8>)>,
    /// Requested hashes that the remote did not have.
    pub missing: Vec<Hash32>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
enum SwarmWireRequest {
    Hello { protocol_version: u16 },
    GetChunks { hashes: Vec<Hash32> },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
enum SwarmWireResponse {
    HelloOk {
        protocol_version: u16,
        max_inflight: u16,
    },
    Chunks {
        present: Vec<Hash32>,
        missing: Vec<Hash32>,
    },
}

#[derive(Clone, Debug)]
struct SwarmHandler {
    store: Arc<Store>,
    peer_policy: PeerPolicy,
}

impl ProtocolHandler for SwarmHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let peer = connection.remote_id();
        if !self.peer_policy.allows(peer) {
            warn!(%peer, "rejected unauthorized DeltaWeave swarm peer");
            connection.close(0_u8.into(), b"endpoint ID is not allow-listed");
            return Ok(());
        }

        let (mut send, mut receive) = connection.accept_bi().await?;
        if let Err(error) = self.handle_stream(&mut send, &mut receive).await {
            warn!(%peer, error = %error, "swarm stream failed");
            connection.close(0_u8.into(), b"swarm stream failed");
            return Ok(());
        }
        let _ = send.finish();
        connection.closed().await;
        Ok(())
    }
}

impl SwarmHandler {
    async fn handle_stream(&self, send: &mut SendStream, receive: &mut RecvStream) -> Result<()> {
        match read_frame::<SwarmWireRequest>(receive).await? {
            SwarmWireRequest::Hello { protocol_version } => {
                ensure!(
                    protocol_version == SWARM_PROTOCOL_VERSION,
                    "unsupported swarm protocol version {protocol_version}"
                );
                write_frame(
                    send,
                    &SwarmWireResponse::HelloOk {
                        protocol_version: SWARM_PROTOCOL_VERSION,
                        max_inflight: SWARM_MAX_INFLIGHT,
                    },
                )
                .await
            }
            SwarmWireRequest::GetChunks { hashes } => self.serve_chunks(send, hashes).await,
        }
    }

    async fn serve_chunks(&self, send: &mut SendStream, hashes: Vec<Hash32>) -> Result<()> {
        ensure!(
            hashes.len() <= SWARM_MAX_WANT,
            "swarm chunk request exceeds {SWARM_MAX_WANT} hashes"
        );
        let unique: HashSet<_> = hashes.iter().copied().collect();
        ensure!(
            unique.len() == hashes.len(),
            "swarm chunk request contains duplicates"
        );

        let store = Arc::clone(&self.store);
        let (present, missing, payloads) = tokio::task::spawn_blocking(move || {
            let mut present = Vec::new();
            let mut missing = Vec::new();
            let mut payloads = Vec::new();
            for hash in hashes {
                match store.chunks().read_verified(hash) {
                    Ok(bytes) => {
                        present.push(hash);
                        payloads.push(bytes);
                    }
                    Err(_) => missing.push(hash),
                }
            }
            Result::<_, anyhow::Error>::Ok((present, missing, payloads))
        })
        .await
        .context("swarm chunk inventory task failed")??;

        write_frame(
            send,
            &SwarmWireResponse::Chunks {
                present: present.clone(),
                missing,
            },
        )
        .await?;
        for (hash, bytes) in present.into_iter().zip(payloads) {
            write_frame(
                send,
                &ChunkHeader {
                    hash,
                    length: u32::try_from(bytes.len()).context("swarm chunk length overflow")?,
                },
            )
            .await?;
            send.write_all(&bytes).await?;
        }
        Ok(())
    }
}

/// Completes an authorized swarm Hello handshake against a receiver.
pub async fn swarm_hello(
    secret_key: SecretKey,
    remote: EndpointAddr,
    mode: NetworkMode,
) -> Result<SwarmHelloOk> {
    let endpoint = bind_endpoint(secret_key, mode, None).await?;
    let connection = endpoint
        .connect(remote, ALPN_V3)
        .await
        .context("failed to connect swarm peer")?;
    let (mut send, mut receive) = connection
        .open_bi()
        .await
        .context("failed to open swarm hello stream")?;
    write_frame(
        &mut send,
        &SwarmWireRequest::Hello {
            protocol_version: SWARM_PROTOCOL_VERSION,
        },
    )
    .await?;
    send.finish()?;
    let response = read_frame::<SwarmWireResponse>(&mut receive).await?;
    connection.close(0_u8.into(), b"swarm hello complete");
    endpoint.close().await;
    match response {
        SwarmWireResponse::HelloOk {
            protocol_version,
            max_inflight,
        } => Ok(SwarmHelloOk {
            protocol_version,
            max_inflight,
        }),
        SwarmWireResponse::Chunks { .. } => bail!("swarm peer sent chunks during hello"),
    }
}

/// Outcome after filling a local CAS from multiple authorized swarm peers.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SwarmFillReceipt {
    /// Number of unique chunks durably installed or already present.
    pub transferred_chunks: usize,
    /// Verified payload bytes received from source peers.
    pub transferred_bytes: u64,
}

/// Requests verified CAS chunks from an authorized swarm peer.
pub async fn swarm_get_chunks(
    secret_key: SecretKey,
    remote: EndpointAddr,
    mode: NetworkMode,
    hashes: Vec<Hash32>,
) -> Result<SwarmChunkFetch> {
    ensure!(
        hashes.len() <= SWARM_MAX_WANT,
        "swarm chunk request exceeds {SWARM_MAX_WANT} hashes"
    );
    let unique: HashSet<_> = hashes.iter().copied().collect();
    ensure!(
        unique.len() == hashes.len(),
        "swarm chunk request contains duplicates"
    );

    let endpoint = bind_endpoint(secret_key, mode, None).await?;
    let connection = endpoint
        .connect(remote, ALPN_V3)
        .await
        .context("failed to connect swarm peer")?;
    let (mut send, mut receive) = connection
        .open_bi()
        .await
        .context("failed to open swarm chunk stream")?;
    write_frame(
        &mut send,
        &SwarmWireRequest::GetChunks {
            hashes: hashes.clone(),
        },
    )
    .await?;
    send.finish()?;
    let response = read_frame::<SwarmWireResponse>(&mut receive).await?;
    let (present, missing) = match response {
        SwarmWireResponse::Chunks { present, missing } => (present, missing),
        SwarmWireResponse::HelloOk { .. } => bail!("swarm peer sent hello during chunk fetch"),
    };
    let mut chunks = Vec::with_capacity(present.len());
    for expected in present {
        let header: ChunkHeader = read_frame(&mut receive).await?;
        ensure!(
            header.hash == expected,
            "swarm chunk arrived out of inventory order"
        );
        let mut bytes = vec![0_u8; header.length as usize];
        receive.read_exact(&mut bytes).await?;
        let actual = Hash32::digest(&bytes);
        ensure!(
            actual == expected,
            "swarm chunk {expected} hashed to {actual}"
        );
        chunks.push((expected, bytes));
    }
    connection.close(0_u8.into(), b"swarm chunk fetch complete");
    endpoint.close().await;
    Ok(SwarmChunkFetch { chunks, missing })
}

/// Fills missing hashes in a local CAS from multiple authorized V3 sources.
pub async fn swarm_fill_chunks(
    secret_key: SecretKey,
    sources: Vec<EndpointAddr>,
    mode: NetworkMode,
    store: Arc<Store>,
    hashes: Vec<Hash32>,
) -> Result<SwarmFillReceipt> {
    ensure!(
        !sources.is_empty(),
        "swarm fill requires at least one source"
    );
    ensure!(
        sources.len() <= 8,
        "swarm fill supports at most eight sources"
    );
    let unique: HashSet<_> = hashes.iter().copied().collect();
    ensure!(
        unique.len() == hashes.len(),
        "swarm fill contains duplicate hashes"
    );

    let mut remaining: HashSet<_> = hashes.iter().copied().collect();
    let mut payloads = Vec::new();
    let mut transferred_bytes = 0_u64;
    for source in sources {
        if remaining.is_empty() {
            break;
        }
        let assigned: Vec<_> = remaining.iter().copied().collect();
        let fetch = swarm_get_chunks(secret_key.clone(), source, mode, assigned).await?;
        for (hash, bytes) in fetch.chunks {
            remaining.remove(&hash);
            transferred_bytes = transferred_bytes
                .checked_add(bytes.len() as u64)
                .context("swarm transferred-byte counter overflow")?;
            payloads.push((hash, bytes));
        }
    }
    ensure!(
        remaining.is_empty(),
        "swarm sources lack {} assigned chunk(s)",
        remaining.len()
    );
    let installed = payloads.len();
    let chunk_store = Arc::clone(&store);
    tokio::task::spawn_blocking(move || chunk_store.chunks().put_verified_batch(payloads))
        .await
        .context("local swarm chunk-store task failed")??;
    for hash in hashes {
        ensure!(
            store.chunks().contains(hash),
            "swarm fill left chunk {hash} missing"
        );
    }
    Ok(SwarmFillReceipt {
        transferred_chunks: installed,
        transferred_bytes,
    })
}

async fn write_frame<T: Serialize>(send: &mut SendStream, value: &T) -> Result<()> {
    let bytes = postcard::to_stdvec(value)?;
    ensure!(
        bytes.len() <= MAX_CONTROL_FRAME,
        "control frame exceeds {MAX_CONTROL_FRAME} bytes"
    );
    let length = u32::try_from(bytes.len()).context("control frame length overflow")?;
    send.write_u32(length).await?;
    send.write_all(&bytes).await?;
    Ok(())
}

async fn read_frame<T: DeserializeOwned>(receive: &mut RecvStream) -> Result<T> {
    let length = receive.read_u32().await? as usize;
    ensure!(
        length <= MAX_CONTROL_FRAME,
        "remote control frame exceeds {MAX_CONTROL_FRAME} bytes"
    );
    let mut bytes = vec![0_u8; length];
    receive.read_exact(&mut bytes).await?;
    postcard::from_bytes(&bytes).context("malformed control frame")
}

fn truncate_error(message: &str) -> String {
    const LIMIT: usize = 1024;
    if message.len() <= LIMIT {
        return message.to_owned();
    }
    let mut end = LIMIT;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &message[..end])
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, fs};

    use deltaweave_core::{SYNC_RECORD_SCHEMA_V1, VersionVector};
    use tempfile::TempDir;

    use super::*;

    fn fixture(length: usize) -> Vec<u8> {
        let mut value = 0x243f_6a88_85a3_08d3_u64;
        (0..length)
            .map(|index| {
                value = value
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                (value >> 29) as u8 ^ index as u8
            })
            .collect()
    }

    fn version(label: &[u8], counter: u64) -> VersionVector {
        let replica = ReplicaId(Hash32::digest(label));
        let mut version = VersionVector::default();
        version.observe(replica, counter);
        version
    }

    fn file_record(path: &str, bytes: &[u8], label: &[u8], counter: u64) -> SyncRecord {
        SyncRecord {
            schema_version: SYNC_RECORD_SCHEMA_V1,
            path: WirePath::new(path).expect("test path is portable"),
            kind: SyncEntryKind::File,
            size: bytes.len() as u64,
            content_hash: Some(Hash32::digest(bytes)),
            readonly: false,
            version: version(label, counter),
            tombstone: false,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn chunk_writer_drain_waits_for_all_tasks_after_first_error() {
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let delayed_completed = Arc::clone(&completed);
        let failed = tokio::task::spawn_blocking(|| bail!("expected writer failure"));
        let delayed = tokio::task::spawn_blocking(move || {
            std::thread::sleep(Duration::from_millis(50));
            delayed_completed.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(1)
        });

        let result = drain_chunk_tasks(vec![delayed, failed]).await;

        assert!(result.is_err());
        assert!(completed.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bounded_chunk_writer_persists_every_verified_chunk() {
        let state = TempDir::new().expect("state directory can be created");
        let store = Arc::new(Store::open(state.path()).expect("store can open"));
        let chunks: Vec<_> = (0..32)
            .map(|index| {
                let bytes = fixture(64 * 1024 + index);
                (Hash32::digest(&bytes), bytes)
            })
            .collect();
        let mut writer = ChunkWritePipeline::new(Arc::clone(&store), 4);

        for (hash, bytes) in &chunks {
            writer
                .push(*hash, bytes.clone())
                .await
                .expect("verified chunk can enter the write pipeline");
        }
        writer
            .finish()
            .await
            .expect("all queued chunks are durably stored");

        for (hash, bytes) in chunks {
            assert_eq!(
                store
                    .chunks()
                    .read_verified(hash)
                    .expect("pipeline chunk can be read"),
                bytes
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn p2p_round_trip_and_delta_reuse() {
        let state = TempDir::new().expect("state directory can be created");
        let destination = TempDir::new().expect("destination can be created");
        let source_dir = TempDir::new().expect("source directory can be created");
        let source = source_dir.path().join("source.bin");
        let client_key = SecretKey::generate();
        let server_key = SecretKey::generate();
        let server = start_server(ServerConfig {
            secret_key: server_key,
            destination_root: destination.path().to_path_buf(),
            state_root: state.path().to_path_buf(),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
            network_mode: NetworkMode::DirectOnly,
        })
        .await
        .expect("server can start");

        let original = fixture(4 * 1024 * 1024);
        fs::write(&source, &original).expect("source can be written");
        let remote_path = WirePath::new("sync/data.bin").expect("path is portable");
        let first = push_file(PushOptions {
            secret_key: client_key.clone(),
            source: source.clone(),
            remote_path: remote_path.clone(),
            remote: server.endpoint_addr(),
            profile: ChunkingProfile::DEFAULT,
            network_mode: NetworkMode::DirectOnly,
        })
        .await
        .expect("first transfer succeeds");
        assert_eq!(
            fs::read(destination.path().join(remote_path.as_str()))
                .expect("destination can be read"),
            original
        );
        assert!(first.transferred_bytes > 0);

        let mut modified = original;
        modified.splice(700_000..700_000, b"delta insertion".iter().copied());
        fs::write(&source, &modified).expect("modified source can be written");
        let second = push_file(PushOptions {
            secret_key: client_key.clone(),
            source: source.clone(),
            remote_path: remote_path.clone(),
            remote: server.endpoint_addr(),
            profile: ChunkingProfile::DEFAULT,
            network_mode: NetworkMode::DirectOnly,
        })
        .await
        .expect("delta transfer succeeds");
        assert_eq!(
            fs::read(destination.path().join(remote_path.as_str()))
                .expect("destination can be read"),
            modified
        );
        assert!(second.reused_extents > 0);
        assert!(second.transferred_bytes < modified.len() as u64);

        let unchanged = push_file(PushOptions {
            secret_key: client_key.clone(),
            source: source.clone(),
            remote_path: remote_path.clone(),
            remote: server.endpoint_addr(),
            profile: ChunkingProfile::DEFAULT,
            network_mode: NetworkMode::DirectOnly,
        })
        .await
        .expect("unchanged retry succeeds");
        assert_eq!(unchanged.transferred_bytes, 0);
        assert!(unchanged.reused_extents > 0);

        fs::write(&source, b"").expect("empty source can be written");
        let empty_path = WirePath::new("sync/empty.bin").expect("path is portable");
        let empty = push_file(PushOptions {
            secret_key: client_key,
            source,
            remote_path: empty_path.clone(),
            remote: server.endpoint_addr(),
            profile: ChunkingProfile::DEFAULT,
            network_mode: NetworkMode::DirectOnly,
        })
        .await
        .expect("empty transfer succeeds");
        assert_eq!(empty.transferred_bytes, 0);
        assert_eq!(empty.reused_extents, 0);
        assert_eq!(
            fs::read(destination.path().join(empty_path.as_str()))
                .expect("empty destination can be read"),
            Vec::<u8>::new()
        );
        server.shutdown().await.expect("server shuts down");
    }

    #[tokio::test]
    async fn server_rejects_overlapping_destination_and_state_roots() {
        let root = TempDir::new().expect("root can be created");
        let result = start_server(ServerConfig {
            secret_key: SecretKey::generate(),
            destination_root: root.path().to_path_buf(),
            state_root: root.path().join("state"),
            peer_policy: PeerPolicy::AllowListed(HashSet::new()),
            network_mode: NetworkMode::DirectOnly,
        })
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn direct_server_is_ready_from_its_bound_socket_without_netlink_discovery() {
        let state = TempDir::new().expect("state directory can be created");
        let destination = TempDir::new().expect("destination can be created");
        let server = start_server(ServerConfig {
            secret_key: SecretKey::generate(),
            destination_root: destination.path().to_path_buf(),
            state_root: state.path().to_path_buf(),
            peer_policy: PeerPolicy::AllowListed(HashSet::new()),
            network_mode: NetworkMode::DirectOnly,
        })
        .await
        .expect("server can start");
        assert!(server.wait_online(Duration::from_millis(250)).await);
        assert!(!server.address_info().direct_addresses.is_empty());
        server.shutdown().await.expect("server shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn swarm_v3_retries_not_have_chunks_on_another_source() {
        let client_key = SecretKey::generate();
        let bytes = fixture(96 * 1024);
        let hash = Hash32::digest(&bytes);
        let mut servers = Vec::new();

        for has_chunk in [false, true] {
            let state = TempDir::new().expect("server state can be created");
            let destination = TempDir::new().expect("server destination can be created");
            if has_chunk {
                let store = Store::open(state.path()).expect("server store can open");
                store
                    .chunks()
                    .put_verified(hash, &bytes)
                    .expect("source chunk can be stored");
            }
            let server = start_server(ServerConfig {
                secret_key: SecretKey::generate(),
                destination_root: destination.path().to_path_buf(),
                state_root: state.path().to_path_buf(),
                peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
                network_mode: NetworkMode::DirectOnly,
            })
            .await
            .expect("swarm source can start");
            servers.push((server, state, destination));
        }

        let local = TempDir::new().expect("local state can be created");
        let local_store = Arc::new(Store::open(local.path()).expect("local store can open"));
        let result = swarm_fill_chunks(
            client_key,
            servers
                .iter()
                .map(|(server, _, _)| server.endpoint_addr())
                .collect(),
            NetworkMode::DirectOnly,
            Arc::clone(&local_store),
            vec![hash],
        )
        .await
        .expect("missing chunk is retried on second source");

        assert_eq!(result.transferred_chunks, 1);
        assert_eq!(local_store.chunks().read_verified(hash).unwrap(), bytes);
        for (server, _, _) in servers {
            server.shutdown().await.expect("source shuts down");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn swarm_v3_downloads_from_two_sources_into_local_cas() {
        let client_key = SecretKey::generate();
        let first = fixture(96 * 1024);
        let second = fixture(128 * 1024 + 7);
        let first_hash = Hash32::digest(&first);
        let second_hash = Hash32::digest(&second);
        let mut servers = Vec::new();

        for (bytes, hash) in [(&first, first_hash), (&second, second_hash)] {
            let state = TempDir::new().expect("server state can be created");
            let destination = TempDir::new().expect("server destination can be created");
            {
                let store = Store::open(state.path()).expect("server store can open");
                store
                    .chunks()
                    .put_verified(hash, bytes)
                    .expect("source chunk can be stored");
            }
            let server = start_server(ServerConfig {
                secret_key: SecretKey::generate(),
                destination_root: destination.path().to_path_buf(),
                state_root: state.path().to_path_buf(),
                peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
                network_mode: NetworkMode::DirectOnly,
            })
            .await
            .expect("swarm source can start");
            servers.push((server, state, destination));
        }

        let local = TempDir::new().expect("local state can be created");
        let local_store = Arc::new(Store::open(local.path()).expect("local store can open"));
        let result = swarm_fill_chunks(
            client_key,
            servers
                .iter()
                .map(|(server, _, _)| server.endpoint_addr())
                .collect(),
            NetworkMode::DirectOnly,
            Arc::clone(&local_store),
            vec![first_hash, second_hash],
        )
        .await
        .expect("two-source swarm fill succeeds");

        assert_eq!(result.transferred_chunks, 2);
        assert_eq!(
            result.transferred_bytes,
            (first.len() + second.len()) as u64
        );
        assert_eq!(
            local_store
                .chunks()
                .read_verified(first_hash)
                .expect("first swarm chunk stored"),
            first
        );
        assert_eq!(
            local_store
                .chunks()
                .read_verified(second_hash)
                .expect("second swarm chunk stored"),
            second
        );
        for (server, _, _) in servers {
            server.shutdown().await.expect("source shuts down");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn swarm_v3_serves_only_verified_local_cas_chunks() {
        let state = TempDir::new().expect("state directory can be created");
        let destination = TempDir::new().expect("destination can be created");
        let present = fixture(96 * 1024);
        let present_hash = Hash32::digest(&present);
        let missing_hash = Hash32::digest(b"absent swarm chunk");
        {
            let store = Store::open(state.path()).expect("store can open");
            store
                .chunks()
                .put_verified(present_hash, &present)
                .expect("seed chunk can be stored");
        }
        let authorized_key = SecretKey::generate();
        let server = start_server(ServerConfig {
            secret_key: SecretKey::generate(),
            destination_root: destination.path().to_path_buf(),
            state_root: state.path().to_path_buf(),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([authorized_key.public()])),
            network_mode: NetworkMode::DirectOnly,
        })
        .await
        .expect("server can start");

        let fetched = swarm_get_chunks(
            authorized_key.clone(),
            server.endpoint_addr(),
            NetworkMode::DirectOnly,
            vec![present_hash, missing_hash],
        )
        .await
        .expect("authorized swarm peer can request chunks");
        assert_eq!(fetched.chunks, vec![(present_hash, present)]);
        assert_eq!(fetched.missing, vec![missing_hash]);

        let rejected = swarm_get_chunks(
            SecretKey::generate(),
            server.endpoint_addr(),
            NetworkMode::DirectOnly,
            vec![present_hash],
        )
        .await;
        assert!(rejected.is_err());
        server.shutdown().await.expect("server shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn swarm_v3_hello_requires_an_authorized_peer() {
        let state = TempDir::new().expect("state directory can be created");
        let destination = TempDir::new().expect("destination can be created");
        let authorized_key = SecretKey::generate();
        let server = start_server(ServerConfig {
            secret_key: SecretKey::generate(),
            destination_root: destination.path().to_path_buf(),
            state_root: state.path().to_path_buf(),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([authorized_key.public()])),
            network_mode: NetworkMode::DirectOnly,
        })
        .await
        .expect("server can start");

        let hello = swarm_hello(
            authorized_key,
            server.endpoint_addr(),
            NetworkMode::DirectOnly,
        )
        .await
        .expect("authorized swarm peer completes hello");
        assert_eq!(hello.protocol_version, 3);
        assert_eq!(hello.max_inflight, 64);

        let rejected = swarm_hello(
            SecretKey::generate(),
            server.endpoint_addr(),
            NetworkMode::DirectOnly,
        )
        .await;
        assert!(rejected.is_err());
        server.shutdown().await.expect("server shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unauthorized_peer_is_rejected() {
        let state = TempDir::new().expect("state directory can be created");
        let destination = TempDir::new().expect("destination can be created");
        let source_dir = TempDir::new().expect("source directory can be created");
        let source = source_dir.path().join("source.bin");
        fs::write(&source, b"not authorized").expect("source can be written");
        let server = start_server(ServerConfig {
            secret_key: SecretKey::generate(),
            destination_root: destination.path().to_path_buf(),
            state_root: state.path().to_path_buf(),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([SecretKey::generate().public()])),
            network_mode: NetworkMode::DirectOnly,
        })
        .await
        .expect("server can start");
        let result = push_file(PushOptions {
            secret_key: SecretKey::generate(),
            source,
            remote_path: WirePath::new("rejected.bin").expect("path is portable"),
            remote: server.endpoint_addr(),
            profile: ChunkingProfile::DEFAULT,
            network_mode: NetworkMode::DirectOnly,
        })
        .await;
        assert!(result.is_err());
        assert!(!destination.path().join("rejected.bin").exists());

        let empty =
            MerkleTree::from_records(Vec::<SyncRecord>::new()).expect("empty Merkle tree is valid");
        let reconciliation = SyncClient {
            secret_key: SecretKey::generate(),
            remote: server.endpoint_addr(),
            network_mode: NetworkMode::DirectOnly,
        }
        .fetch_snapshot(&empty)
        .await;
        assert!(reconciliation.is_err());
        server.shutdown().await.expect("server shuts down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reconciliation_v2_covers_snapshot_delta_pull_causal_push_and_metadata() {
        let server_state = TempDir::new().expect("server state can be created");
        let server_root = TempDir::new().expect("server root can be created");
        let client_state = TempDir::new().expect("client state can be created");
        let client_root = TempDir::new().expect("client root can be created");
        let sources = TempDir::new().expect("source directory can be created");
        fs::create_dir(server_root.path().join("seed")).expect("seed folder can be created");
        let seed = fixture(2 * 1024 * 1024 + 97);
        fs::write(server_root.path().join("seed/remote.bin"), &seed)
            .expect("seed file can be written");

        let client_key = SecretKey::generate();
        let server = start_server(ServerConfig {
            secret_key: SecretKey::generate(),
            destination_root: server_root.path().to_path_buf(),
            state_root: server_state.path().to_path_buf(),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
            network_mode: NetworkMode::DirectOnly,
        })
        .await
        .expect("server can start");
        let client = SyncClient {
            secret_key: client_key,
            remote: server.endpoint_addr(),
            network_mode: NetworkMode::DirectOnly,
        };

        let empty =
            MerkleTree::from_records(Vec::<SyncRecord>::new()).expect("empty Merkle tree is valid");
        let initial = client
            .fetch_snapshot(&empty)
            .await
            .expect("remote snapshot can be reconstructed");
        assert!(initial.queried_nodes >= 3);
        assert!(initial.record_count >= 2);
        let initial_tree = MerkleTree::from_records(initial.records.clone())
            .expect("remote records remain a valid tree");
        let unchanged = client
            .fetch_snapshot(&initial_tree)
            .await
            .expect("unchanged root can use the constant-size fast path");
        assert_eq!(unchanged.queried_nodes, 1);
        assert_eq!(unchanged.root_hash, initial.root_hash);

        let remote_file = initial
            .records
            .iter()
            .find(|record| record.path.as_str() == "seed/remote.bin")
            .cloned()
            .expect("snapshot contains the seeded file");
        let local_store =
            Arc::new(Store::open(client_state.path()).expect("client content store can be opened"));
        let first_pull = client
            .pull_record(remote_file.clone(), Arc::clone(&local_store))
            .await
            .expect("first causal pull succeeds");
        assert!(first_pull.transferred_bytes > 0);
        local_store
            .materialize(&first_pull.manifest, &remote_file.path, client_root.path())
            .expect("pulled content can be atomically materialized");
        assert_eq!(
            fs::read(client_root.path().join("seed/remote.bin"))
                .expect("materialized pull can be read"),
            seed
        );
        let second_pull = client
            .pull_record(remote_file, Arc::clone(&local_store))
            .await
            .expect("repeated causal pull succeeds");
        assert_eq!(second_pull.transferred_bytes, 0);
        assert_eq!(
            second_pull.reused_extents,
            second_pull.manifest.chunks.len()
        );

        let first_bytes = fixture(2 * 1024 * 1024 + 211);
        let first_source = sources.path().join("first.bin");
        fs::write(&first_source, &first_bytes).expect("first source can be written");
        let first_record = file_record("shared/outgoing.bin", &first_bytes, b"client-a", 1);
        let first_push = client
            .push_record(
                &first_source,
                first_record.clone(),
                ChunkingProfile::DEFAULT,
            )
            .await
            .expect("first causal push succeeds");
        assert!(first_push.transferred_bytes > 0);
        let repeated = client
            .push_record(
                &first_source,
                first_record.clone(),
                ChunkingProfile::DEFAULT,
            )
            .await
            .expect("idempotent causal push succeeds");
        assert_eq!(repeated.transferred_bytes, 0);

        let divergent_bytes = b"same clock, different state".to_vec();
        let divergent_source = sources.path().join("divergent.bin");
        fs::write(&divergent_source, &divergent_bytes).expect("divergent source can be written");
        let equivocation = file_record("shared/outgoing.bin", &divergent_bytes, b"client-a", 1);
        assert!(
            client
                .push_record(&divergent_source, equivocation, ChunkingProfile::DEFAULT)
                .await
                .is_err()
        );
        assert_eq!(
            fs::read(server_root.path().join("shared/outgoing.bin"))
                .expect("server file survives rejected equivocation"),
            first_bytes
        );

        let second_bytes = fixture(2 * 1024 * 1024 + 307);
        let second_source = sources.path().join("second.bin");
        fs::write(&second_source, &second_bytes).expect("second source can be written");
        let second_record = file_record("shared/outgoing.bin", &second_bytes, b"client-a", 2);
        client
            .push_record(&second_source, second_record, ChunkingProfile::DEFAULT)
            .await
            .expect("causally newer push succeeds");
        assert!(
            client
                .push_record(&first_source, first_record, ChunkingProfile::DEFAULT)
                .await
                .is_err()
        );
        assert_eq!(
            fs::read(server_root.path().join("shared/outgoing.bin"))
                .expect("server file survives rejected stale push"),
            second_bytes
        );

        let directory = SyncRecord {
            schema_version: SYNC_RECORD_SCHEMA_V1,
            path: WirePath::new("empty-folder").expect("directory path is portable"),
            kind: SyncEntryKind::Directory,
            size: 0,
            content_hash: None,
            readonly: false,
            version: version(b"client-a", 1),
            tombstone: false,
        };
        client
            .apply_metadata(directory.clone())
            .await
            .expect("directory metadata can be applied");
        assert!(server_root.path().join("empty-folder").is_dir());
        let tombstone = SyncRecord {
            version: version(b"client-a", 2),
            tombstone: true,
            ..directory.clone()
        };
        client
            .apply_metadata(tombstone.clone())
            .await
            .expect("directory tombstone can be applied");
        assert!(!server_root.path().join("empty-folder").exists());
        assert!(client.apply_metadata(directory).await.is_err());

        let final_snapshot = client
            .fetch_snapshot(&empty)
            .await
            .expect("final snapshot can be verified");
        assert!(final_snapshot.records.contains(&tombstone));
        assert!(final_snapshot.records.iter().any(|record| {
            record.path.as_str() == "shared/outgoing.bin"
                && record.content_hash == Some(Hash32::digest(&second_bytes))
        }));
        server.shutdown().await.expect("server shuts down");
    }
}
