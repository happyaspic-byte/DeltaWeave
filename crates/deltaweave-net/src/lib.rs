//! Authenticated iroh transport and resumable delta transfer protocol.

#![forbid(unsafe_code)]

pub mod root_admission;
pub mod share;

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fmt,
    fs::{self, File, Metadata, OpenOptions},
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use deltaweave_cdc::{manifest_from_path, manifest_from_reader, verify_chunk};
use deltaweave_core::{
    CausalRelation, ChunkingProfile, FileManifest, Hash32, ReplicaId, SyncEntryKind, SyncRecord,
    WirePath,
};
use deltaweave_index::{IndexOptions, LocalIndex};
use deltaweave_reconcile::{MerkleNodeSummary, MerkleTree};
use deltaweave_store::{Store, VerifiedChunk};
use iroh::{
    Endpoint, EndpointAddr, EndpointId, RelayUrl, SecretKey, TransportAddr, Watcher,
    endpoint::{Connection, RecvStream, SendStream, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};
use redb::{Database, ReadableDatabase, TableDefinition};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::{info, warn};

/// Versioned ALPN identifier for DeltaWeave's initial push protocol.
pub const ALPN_V1: &[u8] = b"deltaweave/sync/1";
/// Versioned ALPN identifier for Merkle state reconciliation and bidirectional transfer.
pub const ALPN_V2: &[u8] = b"deltaweave/sync/2";
const MAX_CONTROL_FRAME: usize = 16 * 1024 * 1024;
const MAX_CHUNKS_PER_FILE: usize = 250_000;
const MAX_FILE_SIZE: u64 = 16 * 1024 * 1024 * 1024 * 1024;
const CHUNK_WRITE_BATCH: usize = 8;
const CHUNK_WRITE_CONCURRENCY: usize = 8;
const CHUNK_WRITE_MAX_QUEUED_BYTES: usize = 32 * 1024 * 1024;

/// A completed payload or current operation observed by management clients.
#[derive(Clone, Debug, Serialize)]
pub struct TransferEvent {
    pub phase: String,
    pub path: Option<String>,
    pub direction: Option<String>,
    pub bytes: u64,
    pub peer: Option<String>,
}

/// Optional non-blocking instrumentation callback. Panics cannot affect transfers.
#[derive(Clone)]
pub struct TransferObserver(Arc<dyn Fn(TransferEvent) + Send + Sync>);

impl TransferObserver {
    pub fn new(callback: impl Fn(TransferEvent) + Send + Sync + 'static) -> Self {
        Self(Arc::new(callback))
    }

    pub fn emit(&self, event: TransferEvent) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (self.0)(event)));
    }
}

/// Live file totals and queued retries in the retained index snapshot.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Inventory {
    pub files: u64,
    pub bytes: u64,
    pub retries: usize,
}

impl Inventory {
    /// Counts live files, their logical bytes, and queued retries from one retained index.
    pub fn from_index(index: &LocalIndex) -> Result<Self> {
        let mut inventory = Self {
            retries: index.retries()?.len(),
            ..Self::default()
        };
        for record in index.sync_records()? {
            if !record.tombstone && record.kind == SyncEntryKind::File {
                inventory.files += 1;
                inventory.bytes = inventory
                    .bytes
                    .checked_add(record.size)
                    .context("inventory byte overflow")?;
            }
        }
        Ok(inventory)
    }
}

fn observed_event(
    observer: &Option<TransferObserver>,
    phase: &str,
    path: Option<&WirePath>,
    direction: Option<&str>,
    bytes: u64,
    peer: EndpointId,
) {
    if let Some(observer) = observer {
        observer.emit(TransferEvent {
            phase: phase.into(),
            path: path.map(|path| path.as_str().into()),
            direction: direction.map(str::to_owned),
            bytes,
            peer: Some(peer.to_string()),
        });
    }
}

#[derive(Debug, Default)]
struct OperationAdmission {
    paused: std::sync::atomic::AtomicBool,
    active: Arc<tokio::sync::RwLock<()>>,
    lifecycle: tokio::sync::Mutex<()>,
}

impl OperationAdmission {
    fn admit(&self) -> Option<tokio::sync::OwnedRwLockReadGuard<()>> {
        let guard = Arc::clone(&self.active).try_read_owned().ok()?;
        if self.paused.load(std::sync::atomic::Ordering::SeqCst) {
            return None;
        }
        Some(guard)
    }

    async fn pause(&self) {
        let _lifecycle = self.lifecycle.lock().await;
        self.paused.store(true, std::sync::atomic::Ordering::SeqCst);
        let _drained = self.active.write().await;
    }

    async fn resume(&self) {
        let _lifecycle = self.lifecycle.lock().await;
        self.paused
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

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
    /// Optional local UDP socket address for stable direct connectivity.
    pub bind_address: Option<SocketAddr>,
    /// Maximum concurrently handled protocol connections.
    pub max_connections: usize,
    /// Free bytes reserved before receiving new content.
    pub min_free_space_bytes: u64,
}

/// A running DeltaWeave protocol router.
#[derive(Debug)]
pub struct Server {
    active_handlers: Arc<tokio::sync::RwLock<()>>,
    router: Router,
    network_mode: NetworkMode,
    index: Arc<LocalIndex>,
    admission: Arc<OperationAdmission>,
}

impl Server {
    /// Reads the retained index snapshot without scanning or opening another owner.
    pub fn inventory(&self) -> Result<Inventory> {
        Inventory::from_index(&self.index)
    }

    /// Rejects new operations, then drains all admitted operations.
    pub async fn pause(&self) -> Result<()> {
        self.admission.pause().await;
        Ok(())
    }

    /// Reopens operation admission without replacing the endpoint identity.
    pub async fn resume(&self) -> Result<()> {
        self.admission.resume().await;
        Ok(())
    }
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
        self.router
            .shutdown()
            .await
            .context("iroh router shutdown")?;
        let _drained = self.active_handlers.write().await;
        Ok(())
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
    start_server_observed(config, None).await
}

/// Starts a receiving endpoint with optional best-effort observations.
pub async fn start_server_observed(
    config: ServerConfig,
    observer: Option<TransferObserver>,
) -> Result<Server> {
    let ServerConfig {
        secret_key,
        destination_root,
        state_root,
        peer_policy,
        network_mode,
        bind_address,
        max_connections,
        min_free_space_bytes,
    } = config;
    ensure!(
        max_connections > 0,
        "max_connections must be greater than zero"
    );
    let root_lease = Arc::new(root_admission::acquire(
        &destination_root,
        root_admission::RootUse::Legacy,
    )?);
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
    let connection_limit = Arc::new(tokio::sync::Semaphore::new(max_connections));
    let receive_admission_lock = Arc::new(tokio::sync::Mutex::new(()));
    let endpoint = bind_endpoint(
        secret_key,
        network_mode,
        Some(vec![ALPN_V1.to_vec(), ALPN_V2.to_vec()]),
        bind_address,
    )
    .await?;
    let admission = Arc::new(OperationAdmission::default());
    let active_handlers = Arc::new(tokio::sync::RwLock::new(()));
    let push_handler = PushHandler {
        active_handlers: Arc::clone(&active_handlers),
        _root_lease: Arc::clone(&root_lease),
        admission: Arc::clone(&admission),
        observer: observer.clone(),
        store: Arc::clone(&store),
        index: Arc::clone(&index),
        destination_root: destination_root.clone(),
        peer_policy: peer_policy.clone(),
        apply_lock: Arc::clone(&apply_lock),
        connection_limit: Arc::clone(&connection_limit),
        min_free_space_bytes,
        state_root: state_root.clone(),
        receive_admission_lock: Arc::clone(&receive_admission_lock),
    };
    let sync_handler = SyncHandler {
        active_handlers: Arc::clone(&active_handlers),
        share_authorization: None,
        _root_lease: root_lease,
        admission: Arc::clone(&admission),
        observer,
        store,
        index: Arc::clone(&index),
        destination_root,
        peer_policy,
        apply_lock,
        connection_limit,
        min_free_space_bytes,
        state_root,
        receive_admission_lock,
    };
    let router = Router::builder(endpoint)
        .accept(ALPN_V1, push_handler)
        .accept(ALPN_V2, sync_handler)
        .spawn();
    Ok(Server {
        active_handlers,
        router,
        network_mode,
        index,
        admission,
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
    let mut state_directory = fs::DirBuilder::new();
    state_directory.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        state_directory.mode(0o700);
    }
    state_directory
        .create(state_root)
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
    bind_address: Option<SocketAddr>,
) -> Result<Endpoint> {
    let mut builder = match mode {
        NetworkMode::Internet => Endpoint::builder(presets::N0),
        NetworkMode::DirectOnly => Endpoint::builder(presets::Minimal),
    }
    .secret_key(secret_key);
    if let Some(alpns) = alpns {
        builder = builder.alpns(alpns);
    }
    if let Some(bind_address) = bind_address {
        builder = builder.clear_ip_transports().bind_addr(bind_address)?;
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
    /// Optional private sender state directory for a persistent manifest cache.
    pub state_root: Option<PathBuf>,
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
    share: Option<share::ShareId>,
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
        let endpoint =
            bind_endpoint(self.secret_key.clone(), self.network_mode, None, None).await?;
        Ok(SyncSession {
            client: self.clone(),
            endpoint,
            share: None,
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
        connection: OperationConnection,
        local: &MerkleTree,
    ) -> Result<RemoteSnapshot> {
        let (mut send, mut receive) = connection
            .open_bi()
            .await
            .context("open reconciliation stream")?;
        let mut queue = VecDeque::from([(String::new(), None)]);
        let mut remote_records = BTreeMap::new();
        let mut root = None;
        let mut queried_nodes = 0_usize;

        while let Some((prefix, expected)) = queue.pop_front() {
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
            let summary = match read_sync_response(&mut receive).await? {
                SyncWireResponse::Node { summary } => summary,
                SyncWireResponse::Error { message } => {
                    bail!("remote Merkle query failed: {message}")
                }
                _ => bail!("remote sent an unexpected Merkle response"),
            }
            .with_context(|| format!("remote Merkle prefix {prefix:?} disappeared"))?;
            ensure!(summary.prefix == prefix, "remote Merkle prefix mismatch");
            validate_snapshot_summary(&summary)?;
            if let Some((hash, record_count)) = expected {
                ensure!(
                    summary.hash == hash && summary.record_count == record_count,
                    "remote Merkle child differs from its parent summary"
                );
            }
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
                    ensure!(
                        queue.len() < 1_000_000 - queried_nodes,
                        "remote Merkle tree exceeds query safety limit"
                    );
                    queue.push_back((child_prefix, Some((child.hash, child.record_count))));
                }
            }
        }

        write_frame(&mut send, &SyncWireRequest::Finish).await?;
        match read_sync_response(&mut receive).await? {
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
            .push_record_connected(session.connect().await?, &source, record, manifest)
            .await;
        session.close().await;
        outcome
    }

    async fn push_record_connected(
        &self,
        connection: OperationConnection,
        source_path: &Path,
        record: SyncRecord,
        manifest: FileManifest,
    ) -> Result<SyncApplyReceipt> {
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
        let missing = match read_sync_response(&mut receive).await? {
            SyncWireResponse::NeedChunks { hashes } => hashes,
            SyncWireResponse::Error { message } => bail!("remote rejected causal push: {message}"),
            _ => bail!("remote sent an unexpected causal push response"),
        };
        let (sent_bytes, reused_extents) =
            send_requested_chunks(&mut send, source_path, &manifest, missing, None).await?;
        send.finish().context("finish causal file upload")?;
        let receipt = match read_sync_response(&mut receive).await? {
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
        let destination_root = store.state_root().to_path_buf();
        self.pull_record_to_with_budget(record, store, destination_root, 0, 0)
            .await
    }

    /// Pulls one exact remote record with admission checks for its eventual destination.
    pub async fn pull_record_to(
        &self,
        record: SyncRecord,
        store: Arc<Store>,
        destination_root: PathBuf,
    ) -> Result<PullReceipt> {
        let pending_destination_bytes = record.size;
        self.pull_record_to_with_budget(
            record,
            store,
            destination_root,
            0,
            pending_destination_bytes,
        )
        .await
    }

    /// Pulls one record while preserving a reserve across CAS and pending destination writes.
    pub async fn pull_record_to_with_budget(
        &self,
        record: SyncRecord,
        store: Arc<Store>,
        destination_root: PathBuf,
        min_free_space_bytes: u64,
        pending_destination_bytes: u64,
    ) -> Result<PullReceipt> {
        record.validate()?;
        ensure!(
            !record.tombstone && record.kind == SyncEntryKind::File,
            "pull_record requires a live file record"
        );
        let session = self.open_session().await?;
        let outcome = session
            .pull_record_to_with_budget(
                record,
                store,
                destination_root,
                min_free_space_bytes,
                pending_destination_bytes,
            )
            .await;
        session.close().await;
        outcome
    }

    async fn pull_record_connected(
        &self,
        connection: OperationConnection,
        expected: SyncRecord,
        store: Arc<Store>,
        destination_root: PathBuf,
        min_free_space_bytes: u64,
        pending_destination_bytes: u64,
    ) -> Result<PullReceipt> {
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
        let (record, manifest) = match read_sync_response(&mut receive).await? {
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
        let admission = DiskAdmission::new(
            store.state_root().to_path_buf(),
            destination_root,
            min_free_space_bytes,
            pending_destination_bytes,
        );
        admission.check_state(unique_missing_chunk_bytes(&manifest, &missing)?)?;
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
        let mut writer = ChunkWritePipeline::with_admission(
            Arc::clone(&store),
            CHUNK_WRITE_CONCURRENCY,
            CHUNK_WRITE_MAX_QUEUED_BYTES,
            admission,
        );
        let receive_result = async {
            let mut transferred_bytes = 0_u64;
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
                writer
                    .push(VerifiedChunk::validate(descriptor, bytes)?)
                    .await?;
                transferred_bytes = transferred_bytes
                    .checked_add(u64::from(header.length))
                    .context("pulled-byte counter overflow")?;
            }
            Ok(transferred_bytes)
        }
        .await;
        let transferred_bytes = finish_chunk_writes(writer, receive_result).await?;
        let receipt = match read_sync_response(&mut receive).await? {
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
        connection: OperationConnection,
        record: SyncRecord,
    ) -> Result<SyncApplyReceipt> {
        record.validate()?;
        ensure!(
            record.tombstone || record.kind == SyncEntryKind::Directory,
            "metadata apply supports only directories and tombstones"
        );

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
            match read_sync_response(&mut receive).await? {
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

struct OperationConnection(Connection);
impl std::ops::Deref for OperationConnection {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        &self.0
    }
}
impl Drop for OperationConnection {
    fn drop(&mut self) {
        self.0.close(0u8.into(), b"operation complete");
    }
}

impl SyncSession {
    async fn connect(&self) -> Result<OperationConnection> {
        let alpn = if self.share.is_some() {
            share::ALPN_V3
        } else {
            ALPN_V2
        };
        let connection = OperationConnection(
            self.endpoint
                .connect(self.client.remote.clone(), alpn)
                .await
                .map_err(|_| share::ShareError::Offline)?,
        );
        if let Some(share_id) = self.share {
            share::wire::open_session(&connection, share_id).await?;
        }
        Ok(connection)
    }
    /// Reconstructs the remote snapshot through this reusable endpoint.
    pub async fn fetch_snapshot(&self, local: &MerkleTree) -> Result<RemoteSnapshot> {
        self.client
            .fetch_snapshot_connected(self.connect().await?, local)
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
            .push_record_connected(self.connect().await?, &source, record, manifest)
            .await
    }

    /// Pulls one exact live-file record through this reusable endpoint.
    pub async fn pull_record(&self, record: SyncRecord, store: Arc<Store>) -> Result<PullReceipt> {
        let destination_root = store.state_root().to_path_buf();
        self.pull_record_to_with_budget(record, store, destination_root, 0, 0)
            .await
    }

    /// Pulls a record with admission checks for its eventual destination filesystem.
    pub async fn pull_record_to(
        &self,
        record: SyncRecord,
        store: Arc<Store>,
        destination_root: PathBuf,
    ) -> Result<PullReceipt> {
        let pending_destination_bytes = record.size;
        self.pull_record_to_with_budget(
            record,
            store,
            destination_root,
            0,
            pending_destination_bytes,
        )
        .await
    }

    /// Pulls a record while preserving a reserve across CAS and pending destination writes.
    pub async fn pull_record_to_with_budget(
        &self,
        record: SyncRecord,
        store: Arc<Store>,
        destination_root: PathBuf,
        min_free_space_bytes: u64,
        pending_destination_bytes: u64,
    ) -> Result<PullReceipt> {
        record.validate()?;
        ensure!(
            !record.tombstone && record.kind == SyncEntryKind::File,
            "pull_record requires a live file record"
        );
        self.client
            .pull_record_connected(
                self.connect().await?,
                record,
                store,
                destination_root,
                min_free_space_bytes,
                pending_destination_bytes,
            )
            .await
    }

    /// Applies a directory or tombstone record through this reusable endpoint.
    pub async fn apply_metadata(&self, record: SyncRecord) -> Result<SyncApplyReceipt> {
        self.client
            .apply_metadata_connected(self.connect().await?, record)
            .await
    }

    /// Gracefully closes the reusable local endpoint.
    pub async fn close(self) {
        if self.share.is_none() {
            self.endpoint.close().await;
        }
    }
}

fn validate_snapshot_summary(summary: &MerkleNodeSummary) -> Result<()> {
    ensure!(
        summary.record_count <= 1_000_000,
        "remote snapshot exceeds record safety limit"
    );
    if !summary.prefix.is_empty() {
        WirePath::new(summary.prefix.clone())?;
        ensure!(summary.record_count > 0, "remote Merkle child is empty");
    }
    if let Some(record) = &summary.record {
        record.validate()?;
        ensure!(
            record.path.as_str() == summary.prefix,
            "remote Merkle record does not match its prefix"
        );
    }
    let mut record_count = usize::from(summary.record.is_some());
    let mut previous_name: Option<&str> = None;
    for child in &summary.children {
        let name = WirePath::new(child.name.clone())?;
        ensure!(
            name.components().count() == 1,
            "remote Merkle child is not an immediate path component"
        );
        ensure!(
            previous_name.is_none_or(|previous| previous < child.name.as_str()),
            "remote Merkle children are duplicated or out of order"
        );
        previous_name = Some(&child.name);
        ensure!(child.record_count > 0, "remote Merkle child is empty");
        record_count = record_count
            .checked_add(child.record_count)
            .context("remote Merkle record count overflow")?;
    }
    ensure!(
        record_count == summary.record_count,
        "remote Merkle child record counts do not match their parent"
    );
    Ok(())
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

const MANIFEST_CACHE_SCHEMA_V1: u16 = 1;
const MANIFEST_GENERATOR_V1: u16 = 1;
const SENDER_MANIFESTS: TableDefinition<&str, &[u8]> = TableDefinition::new("sender_manifests");

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct SourceFingerprint {
    identity: Option<(u64, u64)>,
    size: u64,
    modified_ns: Option<u128>,
    changed_ns: Option<u128>,
    readonly: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CachedManifest {
    schema_version: u16,
    generator_version: u16,
    profile: ChunkingProfile,
    fingerprint: SourceFingerprint,
    manifest: FileManifest,
}

fn prepare_sender_manifest(
    source: &Path,
    requested_profile: ChunkingProfile,
    state_root: Option<&Path>,
) -> Result<FileManifest> {
    let mut file = File::open(source)
        .with_context(|| format!("failed to open source {}", source.display()))?;
    let before_metadata = file.metadata()?;
    ensure!(before_metadata.is_file(), "source is not a regular file");
    let before = source_fingerprint(&file, &before_metadata);
    let profile = requested_profile.for_file_size(before.size);
    let cache_key = fs::canonicalize(source)
        .unwrap_or_else(|_| source.to_path_buf())
        .to_string_lossy()
        .into_owned();

    let cache = state_root.and_then(|root| SenderManifestCache::open(root).ok());
    if sender_cache_eligible(&before)
        && let Some(cache) = &cache
        && let Some(cached) = cache.get(&cache_key)
        && cached_manifest_matches(&cached, profile, before)
    {
        let after_metadata = file.metadata()?;
        let after = source_fingerprint(&file, &after_metadata);
        if manifest_fingerprints_match(&cached.manifest, before, after) {
            return Ok(cached.manifest);
        }
    }

    let manifest = manifest_from_reader(&mut file, profile)?;
    let after_metadata = file.metadata()?;
    let after = source_fingerprint(&file, &after_metadata);
    ensure!(
        manifest_fingerprints_match(&manifest, before, after),
        "source changed while preparing manifest"
    );
    if sender_cache_eligible(&after)
        && let Some(cache) = cache
    {
        let _ = cache.put(
            &cache_key,
            &CachedManifest {
                schema_version: MANIFEST_CACHE_SCHEMA_V1,
                generator_version: MANIFEST_GENERATOR_V1,
                profile,
                fingerprint: after,
                manifest: manifest.clone(),
            },
        );
    }
    Ok(manifest)
}

fn sender_cache_eligible(fingerprint: &SourceFingerprint) -> bool {
    fingerprint.identity.is_some() && fingerprint.changed_ns.is_some()
}

fn cached_manifest_matches(
    cached: &CachedManifest,
    profile: ChunkingProfile,
    before: SourceFingerprint,
) -> bool {
    cached.schema_version == MANIFEST_CACHE_SCHEMA_V1
        && cached.generator_version == MANIFEST_GENERATOR_V1
        && cached.profile == profile
        && cached.fingerprint == before
        && cached.manifest.validate().is_ok()
}

fn manifest_fingerprints_match(
    manifest: &FileManifest,
    before: SourceFingerprint,
    after: SourceFingerprint,
) -> bool {
    before == after && manifest.size == before.size && manifest.size == after.size
}

struct SenderManifestCache {
    database: Database,
}

impl SenderManifestCache {
    fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        let mut directory = fs::DirBuilder::new();
        directory.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            directory.mode(0o700);
        }
        directory.create(root)?;
        let mut database_file = OpenOptions::new();
        database_file
            .read(true)
            .write(true)
            .create(true)
            .truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            database_file.mode(0o600);
        }
        let database = Database::builder()
            .create_file(database_file.open(root.join("sender-manifests.redb"))?)?;
        let write = database.begin_write()?;
        {
            let _ = write.open_table(SENDER_MANIFESTS)?;
        }
        write.commit()?;
        Ok(Self { database })
    }

    fn get(&self, key: &str) -> Option<CachedManifest> {
        let read = self.database.begin_read().ok()?;
        let table = read.open_table(SENDER_MANIFESTS).ok()?;
        let encoded = table.get(key).ok()??.value().to_vec();
        postcard::from_bytes(&encoded).ok()
    }

    fn put(&self, key: &str, entry: &CachedManifest) -> Result<()> {
        let encoded = postcard::to_stdvec(entry)?;
        let write = self.database.begin_write()?;
        {
            let mut table = write.open_table(SENDER_MANIFESTS)?;
            table.insert(key, encoded.as_slice())?;
        }
        write.commit()?;
        Ok(())
    }
}

fn source_fingerprint(file: &File, metadata: &Metadata) -> SourceFingerprint {
    SourceFingerprint {
        identity: source_identity(file, metadata),
        size: metadata.len(),
        modified_ns: metadata_time_ns(metadata.modified().ok()),
        changed_ns: change_time_ns(metadata),
        readonly: metadata.permissions().readonly(),
    }
}

fn metadata_time_ns(time: Option<std::time::SystemTime>) -> Option<u128> {
    time?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_nanos())
}

#[cfg(unix)]
fn change_time_ns(metadata: &Metadata) -> Option<u128> {
    use std::os::unix::fs::MetadataExt;
    let seconds = u128::try_from(metadata.ctime()).ok()?;
    let nanos = u128::try_from(metadata.ctime_nsec()).ok()?;
    Some(seconds.saturating_mul(1_000_000_000).saturating_add(nanos))
}

#[cfg(not(unix))]
fn change_time_ns(_metadata: &Metadata) -> Option<u128> {
    None
}

#[cfg(unix)]
fn source_identity(_file: &File, metadata: &Metadata) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    (metadata.ino() != 0).then_some((metadata.dev(), metadata.ino()))
}

#[cfg(windows)]
fn source_identity(file: &File, _metadata: &Metadata) -> Option<(u64, u64)> {
    let information = winapi_util::file::information(file).ok()?;
    (information.file_index() != 0)
        .then_some((information.volume_serial_number(), information.file_index()))
}

#[cfg(not(any(unix, windows)))]
fn source_identity(_file: &File, _metadata: &Metadata) -> Option<(u64, u64)> {
    None
}

/// Sends one file, transmitting only chunks the receiver reports missing.
pub async fn push_file(options: PushOptions) -> Result<TransferReceipt> {
    let _root_lease = root_admission::acquire(&options.source, root_admission::RootUse::Legacy)?;
    let source_for_manifest = options.source.clone();
    let profile = options.profile;
    let state_root = options.state_root.clone();
    let manifest = tokio::task::spawn_blocking(move || {
        prepare_sender_manifest(&source_for_manifest, profile, state_root.as_deref())
    })
    .await
    .context("manifest task failed")??;

    let endpoint = bind_endpoint(options.secret_key, options.network_mode, None, None).await?;
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
    authorization: Option<&share::Authorization>,
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
        if let Some(auth) = authorization {
            auth.check(false)?;
            auth.phase("sending", None);
        }
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
        if let Some(auth) = authorization {
            auth.check(false)?;
        }
        send.write_all(&bytes).await?;
        sent_bytes = sent_bytes
            .checked_add(u64::from(descriptor.length))
            .context("sent-byte counter overflow")?;
    }
    Ok((sent_bytes, reused_extents))
}

#[derive(Clone)]
struct PushHandler {
    active_handlers: Arc<tokio::sync::RwLock<()>>,
    _root_lease: Arc<root_admission::RootLease>,
    admission: Arc<OperationAdmission>,
    observer: Option<TransferObserver>,
    store: Arc<Store>,
    index: Arc<LocalIndex>,
    destination_root: PathBuf,
    peer_policy: PeerPolicy,
    apply_lock: Arc<tokio::sync::Mutex<()>>,
    connection_limit: Arc<tokio::sync::Semaphore>,
    min_free_space_bytes: u64,
    state_root: PathBuf,
    receive_admission_lock: Arc<tokio::sync::Mutex<()>>,
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
        let handler = self.clone();
        let active = self.active_handlers.clone().read_owned().await;
        tokio::spawn(async move {
            let _active = active;
            handler.accept_retained(connection).await
        })
        .await
        .map_err(AcceptError::from_err)?
    }
}

impl PushHandler {
    async fn accept_retained(&self, connection: Connection) -> Result<(), AcceptError> {
        let Ok(_permit) = Arc::clone(&self.connection_limit).try_acquire_owned() else {
            connection.close(0_u8.into(), b"server connection limit reached; retry later");
            return Ok(());
        };
        let peer = connection.remote_id();
        if !self.peer_policy.allows(peer) {
            warn!(%peer, "rejected unauthorized DeltaWeave peer");
            connection.close(0_u8.into(), b"endpoint ID is not allow-listed");
            return Ok(());
        }

        let Some(operation) = self.admission.admit() else {
            connection.close(0_u8.into(), b"receiver paused; retry later");
            return Ok(());
        };
        observed_event(&self.observer, "peer_seen", None, None, 0, peer);
        let (mut send, mut receive) = connection.accept_bi().await?;
        info!(%peer, "accepted DeltaWeave peer");
        if let Err(error) = self.handle_push(&mut send, &mut receive, peer).await {
            drop(operation);
            observed_event(&self.observer, "error", None, None, 0, peer);
            let message = public_error_message(&error);
            warn!(%peer, error = message, "DeltaWeave transfer failed");
            let _ = write_frame(
                &mut send,
                &WireResponse::Error {
                    message: message.to_owned(),
                },
            )
            .await;
            let _ = send.finish();
            connection.closed().await;
            return Err(AcceptError::from_err(std::io::Error::other(message)));
        }
        drop(operation);
        send.finish()?;
        connection.closed().await;
        Ok(())
    }
}

impl PushHandler {
    async fn handle_push(
        &self,
        send: &mut SendStream,
        receive: &mut RecvStream,
        peer: EndpointId,
    ) -> Result<()> {
        let request: WireRequest = read_frame(receive).await?;
        let (path, manifest) = match request {
            WireRequest::Push { path, manifest } => (path, manifest),
        };
        manifest.validate()?;
        let _receive_guard = self.receive_admission_lock.lock().await;
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
        let admission = DiskAdmission::new(
            self.state_root.clone(),
            self.destination_root.clone(),
            self.min_free_space_bytes,
            manifest.size,
        );
        admission.check_state(unique_missing_chunk_bytes(&manifest, &missing)?)?;
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
        let materialize_admission = admission.clone();
        let mut writer = ChunkWritePipeline::with_admission(
            Arc::clone(&self.store),
            CHUNK_WRITE_CONCURRENCY,
            CHUNK_WRITE_MAX_QUEUED_BYTES,
            admission,
        );
        let receive_result = async {
            let mut transferred_bytes = 0_u64;
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
                writer
                    .push(VerifiedChunk::validate(descriptor, bytes)?)
                    .await?;
                transferred_bytes = transferred_bytes
                    .checked_add(u64::from(header.length))
                    .context("transferred-byte counter overflow")?;
            }
            Ok(transferred_bytes)
        }
        .await;
        let transferred_bytes = finish_chunk_writes(writer, receive_result).await?;

        let _apply_guard = self.apply_lock.lock().await;
        let store = Arc::clone(&self.store);
        let root = self.destination_root.clone();
        let materialize_manifest = manifest.clone();
        let materialize_path = path.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            prepare_destination_kind(
                &store,
                &materialize_path,
                &root,
                SyncEntryKind::File,
                materialize_manifest.manifest_hash(),
            )?;
            materialize_admission.check_materialization(materialize_manifest.size)?;
            store.materialize(&materialize_manifest, &materialize_path, root)
        })
        .await
        .context("materialization task failed")??;

        let index = Arc::clone(&self.index);
        let adopted_path = path.clone();
        tokio::task::spawn_blocking(move || {
            index.adopt_materialized_file(&adopted_path, &outcome.observation)
        })
        .await
        .context("receiver index adoption task failed")??;

        write_frame(
            send,
            &WireResponse::Complete(TransferReceipt {
                file_hash: manifest.file_hash,
                manifest_hash: manifest.manifest_hash(),
                transferred_bytes,
                reused_extents,
                path: path.clone(),
            }),
        )
        .await?;
        observed_event(
            &self.observer,
            "file_received",
            Some(&path),
            Some("receive"),
            transferred_bytes,
            peer,
        );
        Ok(())
    }
}

#[derive(Clone)]
struct SyncHandler {
    active_handlers: Arc<tokio::sync::RwLock<()>>,
    share_authorization: Option<share::Authorization>,
    _root_lease: Arc<root_admission::RootLease>,
    admission: Arc<OperationAdmission>,
    observer: Option<TransferObserver>,
    store: Arc<Store>,
    index: Arc<LocalIndex>,
    destination_root: PathBuf,
    peer_policy: PeerPolicy,
    apply_lock: Arc<tokio::sync::Mutex<()>>,
    connection_limit: Arc<tokio::sync::Semaphore>,
    min_free_space_bytes: u64,
    state_root: PathBuf,
    receive_admission_lock: Arc<tokio::sync::Mutex<()>>,
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
        let handler = self.clone();
        let active = self.active_handlers.clone().read_owned().await;
        tokio::spawn(async move {
            let _active = active;
            handler.accept_retained(connection).await
        })
        .await
        .map_err(AcceptError::from_err)?
    }
}

impl SyncHandler {
    async fn accept_retained(&self, connection: Connection) -> Result<(), AcceptError> {
        let Ok(_permit) = Arc::clone(&self.connection_limit).try_acquire_owned() else {
            connection.close(0_u8.into(), b"server connection limit reached; retry later");
            return Ok(());
        };
        let peer = connection.remote_id();
        if !self.peer_policy.allows(peer) {
            warn!(%peer, "rejected unauthorized DeltaWeave reconciliation peer");
            connection.close(0_u8.into(), b"endpoint ID is not allow-listed");
            return Ok(());
        }

        let Some(operation) = self.admission.admit() else {
            connection.close(0_u8.into(), b"receiver paused; retry later");
            return Ok(());
        };
        observed_event(&self.observer, "peer_seen", None, None, 0, peer);
        let (mut send, mut receive) = connection.accept_bi().await?;
        info!(%peer, "accepted DeltaWeave reconciliation peer");
        let outcome = async {
            match read_frame::<SyncWireRequest>(&mut receive).await? {
                request @ SyncWireRequest::QueryNode { .. } => {
                    self.handle_query_session(request, &mut send, &mut receive)
                        .await
                }
                SyncWireRequest::PullRecord { record } => {
                    self.handle_pull(record, &mut send, &mut receive, peer)
                        .await
                }
                SyncWireRequest::PushRecord { record, manifest } => {
                    self.handle_push_record(record, manifest, &mut send, &mut receive, peer)
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
            drop(operation);
            observed_event(&self.observer, "error", None, None, 0, peer);
            let message = public_error_message(&error);
            warn!(%peer, error = message, "DeltaWeave reconciliation operation failed");
            let _ = write_frame(
                &mut send,
                &SyncWireResponse::Error {
                    message: message.to_owned(),
                },
            )
            .await;
            let _ = send.finish();
            connection.closed().await;
            return Err(AcceptError::from_err(std::io::Error::other(message)));
        }
        drop(operation);
        send.finish()?;
        connection.closed().await;
        Ok(())
    }
}

impl SyncHandler {
    fn authorize(&self, write: bool) -> Result<()> {
        if let Some(auth) = &self.share_authorization {
            auth.check(write)?;
        }
        Ok(())
    }

    async fn handle_query_session(
        &self,
        first: SyncWireRequest,
        send: &mut SendStream,
        receive: &mut RecvStream,
    ) -> Result<()> {
        self.authorize(false)?;
        let _gate = self.apply_lock.lock().await;
        self.authorize(false)?;
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
            self.authorize(false)?;
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
        peer: EndpointId,
    ) -> Result<()> {
        self.authorize(false)?;
        let _gate = self.apply_lock.lock().await;
        self.authorize(false)?;
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
        self.authorize(false)?;
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
        self.authorize(false)?;
        let (transferred_bytes, reused_extents) = send_requested_chunks(
            send,
            &source,
            &manifest,
            missing,
            self.share_authorization.as_ref(),
        )
        .await?;
        self.authorize(false)?;
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
        observed_event(
            &self.observer,
            "file_sent",
            Some(&expected.path),
            Some("send"),
            transferred_bytes,
            peer,
        );
        Ok(())
    }

    async fn handle_push_record(
        &self,
        record: SyncRecord,
        manifest: FileManifest,
        send: &mut SendStream,
        receive: &mut RecvStream,
        peer: EndpointId,
    ) -> Result<()> {
        self.authorize(true)?;
        record.validate()?;
        manifest.validate()?;
        let _receive_guard = self.receive_admission_lock.lock().await;
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
        let admission = DiskAdmission::new(
            self.state_root.clone(),
            self.destination_root.clone(),
            self.min_free_space_bytes,
            manifest.size,
        );
        admission.check_state(unique_missing_chunk_bytes(&manifest, &missing)?)?;
        self.authorize(true)?;
        write_frame(
            send,
            &SyncWireResponse::NeedChunks {
                hashes: missing.clone(),
            },
        )
        .await?;

        let materialize_admission = admission.clone();
        let transferred_bytes = receive_chunks(
            &self.store,
            receive,
            &manifest,
            missing,
            admission,
            self.share_authorization.clone(),
        )
        .await?;
        let _apply_guard = self.apply_lock.lock().await;
        self.authorize(true)?;
        let index = Arc::clone(&self.index);
        let candidate = record.clone();
        let authorization = self.share_authorization.clone();
        let share_metadata = tokio::task::spawn_blocking(move || {
            ensure_causally_applicable(&index, &candidate)?;
            authorization
                .as_ref()
                .map(|auth| auth.candidate_metadata(&candidate))
                .transpose()
        })
        .await
        .context("causal precondition task failed")??;
        let store = Arc::clone(&self.store);
        let root = self.destination_root.clone();
        let path = record.path.clone();
        let operation_hash = record.logical_hash();
        let materialize_manifest = manifest;
        let authorization = self.share_authorization.clone();
        let observer = self.observer.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            if let Some(auth) = &authorization {
                auth.check(true)?;
                auth.phase("applying", Some(&path));
            } else {
                observed_event(&observer, "applying", Some(&path), Some("receive"), 0, peer);
            }
            prepare_destination_kind(&store, &path, &root, SyncEntryKind::File, operation_hash)?;
            materialize_admission.check_materialization(materialize_manifest.size)?;
            store.materialize(&materialize_manifest, &path, root)
        })
        .await
        .context("causal materialization task failed")??;
        self.authorize(true)?;
        let observation = outcome
            .observation
            .after_readonly_update(&outcome.destination, record.readonly)?;
        let index = Arc::clone(&self.index);
        let adopted = record.clone();
        let authorization = self.share_authorization.clone();
        tokio::task::spawn_blocking(move || {
            if let Some(auth) = &authorization {
                auth.check(true)?;
            }
            if let Some(metadata) = share_metadata {
                index.adopt_materialized_record_with_share_metadata(
                    &adopted,
                    &observation,
                    &metadata,
                )
            } else {
                index.adopt_materialized_record(&adopted, &observation)
            }
        })
        .await
        .context("causal index adoption task failed")??;
        self.authorize(false)?;
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
        observed_event(
            &self.observer,
            "file_received",
            Some(&record.path),
            Some("receive"),
            transferred_bytes,
            peer,
        );
        Ok(())
    }

    async fn handle_metadata(&self, record: SyncRecord, send: &mut SendStream) -> Result<()> {
        self.authorize(true)?;
        record.validate()?;
        ensure!(
            record.tombstone || record.kind == SyncEntryKind::Directory,
            "metadata apply supports only directories and tombstones"
        );
        let _apply_guard = self.apply_lock.lock().await;
        self.authorize(true)?;
        let index = Arc::clone(&self.index);
        let candidate = record.clone();
        let authorization = self.share_authorization.clone();
        let share_metadata = tokio::task::spawn_blocking(move || {
            ensure_causally_applicable(&index, &candidate)?;
            authorization
                .as_ref()
                .map(|auth| auth.candidate_metadata(&candidate))
                .transpose()
        })
        .await
        .context("metadata causal precondition task failed")??;
        let store = Arc::clone(&self.store);
        let root = self.destination_root.clone();
        let apply_record = record.clone();
        let authorization = self.share_authorization.clone();
        tokio::task::spawn_blocking(move || {
            if let Some(auth) = &authorization {
                auth.check(true)?;
                auth.phase("applying", Some(&apply_record.path));
            }
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
        let authorization = self.share_authorization.clone();
        tokio::task::spawn_blocking(move || {
            if let Some(auth) = &authorization {
                auth.check(true)?;
            }
            if let Some(metadata) = share_metadata {
                index.adopt_verified_record_with_share_metadata(&adopted, &metadata)
            } else {
                index.adopt_verified_record(&adopted)
            }
        })
        .await
        .context("metadata index adoption task failed")??;
        self.authorize(false)?;
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

struct InflightWrite {
    bytes: usize,
    task: tokio::task::JoinHandle<Result<usize>>,
}

/// Free-space admission shared by CAS persistence and destination materialization.
#[derive(Clone, Debug)]
pub struct DiskAdmission {
    state_path: PathBuf,
    destination_path: PathBuf,
    reserve_bytes: u64,
    pending_destination_bytes: u64,
}

impl DiskAdmission {
    /// Tracks the reserve and all destination bytes that must remain affordable while staging.
    #[must_use]
    pub fn new(
        state_path: PathBuf,
        destination_path: PathBuf,
        reserve_bytes: u64,
        pending_destination_bytes: u64,
    ) -> Self {
        Self {
            state_path,
            destination_path,
            reserve_bytes,
            pending_destination_bytes,
        }
    }

    /// Rechecks a CAS write while retaining space for all pending destination writes.
    pub fn check_state(&self, write_bytes: u64) -> Result<()> {
        self.check_budget(write_bytes, self.pending_destination_bytes)
    }

    /// Rechecks one destination write immediately before materialization.
    pub fn check_materialization(&self, write_bytes: u64) -> Result<()> {
        self.check_budget(0, write_bytes)
    }

    fn check_budget(&self, state_write_bytes: u64, destination_write_bytes: u64) -> Result<()> {
        let budget = FilesystemBudget {
            state_available: available_space(&self.state_path, "state")?,
            destination_available: available_space(&self.destination_path, "destination")?,
            shared: same_filesystem(&self.state_path, &self.destination_path)?,
        };
        check_filesystem_budget(
            budget,
            state_write_bytes,
            destination_write_bytes,
            self.reserve_bytes,
        )
    }
}

#[derive(Clone, Copy, Debug)]
struct FilesystemBudget {
    state_available: u64,
    destination_available: u64,
    shared: bool,
}

fn check_filesystem_budget(
    budget: FilesystemBudget,
    state_write_bytes: u64,
    destination_write_bytes: u64,
    reserve_bytes: u64,
) -> Result<()> {
    if budget.shared {
        let write_bytes = state_write_bytes
            .checked_add(destination_write_bytes)
            .context("combined disk admission byte count overflow")?;
        let required = reserve_bytes
            .checked_add(write_bytes)
            .context("combined disk admission byte count overflow")?;
        let available = budget.state_available.min(budget.destination_available);
        ensure!(
            available >= required,
            "insufficient shared disk space: writes need {write_bytes} bytes plus {reserve_bytes} reserved bytes, have {available} bytes"
        );
    } else {
        check_available(
            budget.state_available,
            state_write_bytes,
            reserve_bytes,
            "state",
        )?;
        check_available(
            budget.destination_available,
            destination_write_bytes,
            reserve_bytes,
            "destination",
        )?;
    }
    Ok(())
}

fn check_available(
    available: u64,
    write_bytes: u64,
    reserve_bytes: u64,
    label: &str,
) -> Result<()> {
    let required = reserve_bytes
        .checked_add(write_bytes)
        .context("disk admission byte count overflow")?;
    ensure!(
        available >= required,
        "insufficient {label} disk space: write needs {write_bytes} bytes plus {reserve_bytes} reserved bytes, have {available} bytes"
    );
    Ok(())
}

fn available_space(path: &Path, label: &str) -> Result<u64> {
    fs2::available_space(path)
        .with_context(|| format!("failed to query free space for {label} filesystem"))
}

#[cfg(unix)]
fn same_filesystem(left: &Path, right: &Path) -> Result<bool> {
    use std::os::unix::fs::MetadataExt;
    Ok(fs::metadata(left)?.dev() == fs::metadata(right)?.dev())
}

#[cfg(windows)]
fn same_filesystem(left: &Path, right: &Path) -> Result<bool> {
    let left = fs::canonicalize(left)?;
    let right = fs::canonicalize(right)?;
    Ok(left.components().next().map(|part| part.as_os_str())
        == right.components().next().map(|part| part.as_os_str()))
}

#[cfg(not(any(unix, windows)))]
fn same_filesystem(_left: &Path, _right: &Path) -> Result<bool> {
    // Conservatively combine budgets on platforms without a stable filesystem identity API.
    Ok(true)
}

struct ChunkWritePipeline {
    authorization: Option<share::Authorization>,
    store: Arc<Store>,
    admission: Option<DiskAdmission>,
    max_inflight: usize,
    max_queued_bytes: usize,
    inflight: Vec<InflightWrite>,
    pending: Vec<VerifiedChunk>,
}

impl ChunkWritePipeline {
    #[cfg(test)]
    fn new(store: Arc<Store>, max_inflight: usize) -> Self {
        Self::with_limits(store, max_inflight, CHUNK_WRITE_MAX_QUEUED_BYTES)
    }

    #[cfg(test)]
    fn with_limits(store: Arc<Store>, max_inflight: usize, max_queued_bytes: usize) -> Self {
        Self::with_optional_admission(store, max_inflight, max_queued_bytes, None)
    }

    fn with_admission(
        store: Arc<Store>,
        max_inflight: usize,
        max_queued_bytes: usize,
        admission: DiskAdmission,
    ) -> Self {
        Self::with_optional_admission(store, max_inflight, max_queued_bytes, Some(admission))
    }

    fn with_optional_admission(
        store: Arc<Store>,
        max_inflight: usize,
        max_queued_bytes: usize,
        admission: Option<DiskAdmission>,
    ) -> Self {
        Self {
            authorization: None,
            store,
            admission,
            max_inflight: max_inflight.max(1),
            max_queued_bytes: max_queued_bytes.max(1),
            inflight: Vec::new(),
            pending: Vec::new(),
        }
    }

    fn queued_bytes(&self) -> usize {
        self.pending
            .iter()
            .map(|chunk| chunk.bytes().len())
            .sum::<usize>()
            + self.inflight.iter().map(|write| write.bytes).sum::<usize>()
    }

    async fn push(&mut self, chunk: VerifiedChunk) -> Result<()> {
        while self.queued_bytes() + chunk.bytes().len() > self.max_queued_bytes {
            if !self.pending.is_empty() && self.inflight.len() < self.max_inflight {
                self.flush_pending().await?;
                continue;
            }
            if self.inflight.is_empty() {
                break;
            }
            self.join_oldest().await?;
        }
        self.pending.push(chunk);
        if self.pending.len() >= CHUNK_WRITE_BATCH || self.queued_bytes() > self.max_queued_bytes {
            self.flush_pending().await?;
        }
        Ok(())
    }

    async fn finish(mut self) -> Result<()> {
        let mut first_error = None;
        if !self.pending.is_empty()
            && let Err(error) = self.flush_pending().await
        {
            first_error = Some(error);
        }
        let rest = std::mem::take(&mut self.inflight)
            .into_iter()
            .map(|write| write.task)
            .collect();
        if let Err(error) = drain_chunk_tasks(rest).await {
            first_error.get_or_insert(error);
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn join_oldest(&mut self) -> Result<()> {
        let InflightWrite { task, .. } = self.inflight.remove(0);
        if let Err(error) = join_chunk_task(task).await {
            let rest = std::mem::take(&mut self.inflight)
                .into_iter()
                .map(|write| write.task)
                .collect();
            let drain = drain_chunk_tasks(rest).await;
            return match drain {
                Ok(()) => Err(error),
                Err(drain_error) => Err(error.context(drain_error)),
            };
        }
        Ok(())
    }

    async fn flush_pending(&mut self) -> Result<()> {
        if self.inflight.len() >= self.max_inflight {
            self.join_oldest().await?;
        }
        let batch = std::mem::take(&mut self.pending);
        let bytes = batch.iter().map(|chunk| chunk.bytes().len()).sum();
        if let Some(admission) = &self.admission {
            admission.check_state(u64::try_from(bytes).context("write batch size overflow")?)?;
        }
        let store = Arc::clone(&self.store);
        self.inflight.push(InflightWrite {
            bytes,
            task: {
                let authorization = self.authorization.clone();
                tokio::task::spawn_blocking(move || {
                    if let Some(auth) = &authorization {
                        auth.check(true)?;
                        auth.phase("receiving", None);
                    }
                    store.chunks().put_validated_batch(batch)
                })
            },
        });
        Ok(())
    }
}

async fn finish_chunk_writes(writer: ChunkWritePipeline, result: Result<u64>) -> Result<u64> {
    let persist = writer.finish().await;
    match (result, persist) {
        (Ok(bytes), Ok(())) => Ok(bytes),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(persist_error)) => {
            Err(error.context(format!("chunk-store drain also failed: {persist_error:#}")))
        }
    }
}

async fn join_chunk_task(task: tokio::task::JoinHandle<Result<usize>>) -> Result<usize> {
    match task.await.context("chunk-store task failed") {
        Ok(Ok(written)) => Ok(written),
        Ok(Err(error)) | Err(error) => Err(error),
    }
}

async fn drain_chunk_tasks(tasks: Vec<tokio::task::JoinHandle<Result<usize>>>) -> Result<()> {
    let mut first_error = None;
    for task in tasks {
        if let Err(error) = join_chunk_task(task).await {
            first_error.get_or_insert(error);
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn unique_missing_chunk_bytes(manifest: &FileManifest, missing: &[Hash32]) -> Result<u64> {
    let descriptors: HashMap<_, _> = manifest
        .chunks
        .iter()
        .map(|chunk| (chunk.hash, chunk.length))
        .collect();
    let mut seen = HashSet::new();
    missing
        .iter()
        .filter(|hash| seen.insert(**hash))
        .try_fold(0_u64, |total, hash| {
            let length = descriptors
                .get(hash)
                .with_context(|| format!("missing chunk {hash} is absent from manifest"))?;
            total
                .checked_add(u64::from(*length))
                .context("missing chunk byte count overflow")
        })
}

async fn receive_chunks(
    store: &Arc<Store>,
    receive: &mut RecvStream,
    manifest: &FileManifest,
    missing: Vec<Hash32>,
    admission: DiskAdmission,
    authorization: Option<share::Authorization>,
) -> Result<u64> {
    let descriptor_by_hash: HashMap<_, _> = manifest
        .chunks
        .iter()
        .map(|chunk| (chunk.hash, chunk.clone()))
        .collect();
    let mut writer = ChunkWritePipeline::with_admission(
        Arc::clone(store),
        CHUNK_WRITE_CONCURRENCY,
        CHUNK_WRITE_MAX_QUEUED_BYTES,
        admission,
    );
    writer.authorization = authorization;
    let receive_result = async {
        let mut transferred_bytes = 0_u64;
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
            writer
                .push(VerifiedChunk::validate(descriptor, bytes)?)
                .await?;
        }
        Ok(transferred_bytes)
    }
    .await;
    finish_chunk_writes(writer, receive_result).await
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
    ShareError(share::ShareError),
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

async fn read_sync_response(receive: &mut RecvStream) -> Result<SyncWireResponse> {
    match read_frame(receive).await? {
        SyncWireResponse::ShareError(error) => Err(error.into()),
        response => Ok(response),
    }
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

fn public_error_message(error: &anyhow::Error) -> &'static str {
    // Filesystem and database errors may embed private absolute paths. Use the
    // same bounded, static descriptions for the peer, application log, and router.
    for cause in error.chain() {
        if cause.is::<deltaweave_core::WirePathError>() {
            return "Invalid destination path; choose a portable relative path and retry.";
        }
        if cause.is::<deltaweave_core::ManifestError>() {
            return "Invalid file manifest; regenerate it from the source and retry.";
        }
        if cause.is::<deltaweave_core::SyncRecordError>() {
            return "Invalid causal record; refresh the snapshot and reconcile before retrying.";
        }
        if cause.is::<postcard::Error>() {
            return "Malformed protocol message; check peer compatibility before retrying.";
        }
        if let Some(io_error) = cause.downcast_ref::<std::io::Error>() {
            return match io_error.kind() {
                std::io::ErrorKind::DirectoryNotEmpty => {
                    "Destination directory is not empty; synchronize or move its contents before retrying."
                }
                std::io::ErrorKind::PermissionDenied => {
                    "Receiver permission denied; check destination and state permissions before retrying."
                }
                _ => {
                    "Receiver storage or transport failed; check storage, permissions, and connectivity before retrying."
                }
            };
        }
    }
    match error.to_string().as_str() {
        "incoming record is causally stale" => {
            "Incoming record is causally stale; refresh the snapshot and reconcile before retrying."
        }
        "incoming record is concurrent; reconcile it before applying" => {
            "Incoming record is concurrent; refresh the snapshot and reconcile before retrying."
        }
        "incoming record reuses an existing causal version for different state" => {
            "Incoming record reuses a causal version for different state; reconcile before retrying."
        }
        "requested path is absent" | "requested path changed after snapshot" => {
            "Requested path changed or is absent; refresh the snapshot before retrying."
        }
        _ => {
            "Operation rejected; check the destination and receiver storage, then refresh the snapshot and retry."
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, fs};

    use deltaweave_core::{SYNC_RECORD_SCHEMA_V1, VersionVector};
    use tempfile::TempDir;

    use super::*;

    fn regular_files_below(path: &Path) -> usize {
        let Ok(entries) = fs::read_dir(path) else {
            return 0;
        };
        entries
            .filter_map(Result::ok)
            .map(|entry| {
                if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                    regular_files_below(&entry.path())
                } else {
                    usize::from(entry.file_type().is_ok_and(|kind| kind.is_file()))
                }
            })
            .sum()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn observed_transfer_reports_literal_payload_and_inventory() {
        let state = TempDir::new().unwrap();
        let destination = TempDir::new().unwrap();
        let source_dir = TempDir::new().unwrap();
        let source = source_dir.path().join("source.txt");
        fs::write(&source, b"observed bytes").unwrap();
        let client_key = SecretKey::generate();
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = Arc::clone(&events);
        let server = start_server_observed(
            ServerConfig {
                secret_key: SecretKey::generate(),
                destination_root: destination.path().into(),
                state_root: state.path().into(),
                peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
                network_mode: NetworkMode::DirectOnly,
                bind_address: Some("127.0.0.1:0".parse().unwrap()),
                max_connections: 8,
                min_free_space_bytes: 0,
            },
            Some(TransferObserver::new(move |event| {
                captured.lock().unwrap().push(event)
            })),
        )
        .await
        .unwrap();
        push_file(PushOptions {
            secret_key: client_key.clone(),
            source,
            remote_path: WirePath::new("folder/report.txt").unwrap(),
            remote: server.endpoint_addr(),
            profile: ChunkingProfile::DEFAULT,
            network_mode: NetworkMode::DirectOnly,
            state_root: None,
        })
        .await
        .unwrap();
        assert_eq!(
            fs::read(destination.path().join("folder/report.txt")).unwrap(),
            b"observed bytes"
        );
        let inventory = server.inventory().unwrap();
        assert_eq!(
            (inventory.files, inventory.bytes, inventory.retries),
            (1, 14, 0)
        );
        let events = events.lock().unwrap().clone();
        let received = events
            .iter()
            .find(|event| event.phase == "file_received")
            .unwrap();
        assert_eq!(received.path.as_deref(), Some("folder/report.txt"));
        assert_eq!(received.direction.as_deref(), Some("receive"));
        assert_eq!(received.bytes, 14);
        assert_eq!(
            received.peer.as_deref(),
            Some(client_key.public().to_string().as_str())
        );
        server.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pause_drains_admitted_transfer_rejects_new_work_and_resumes_identity() {
        let state = TempDir::new().unwrap();
        let destination = TempDir::new().unwrap();
        let client_key = SecretKey::generate();
        let server = start_server(ServerConfig {
            secret_key: SecretKey::generate(),
            destination_root: destination.path().into(),
            state_root: state.path().into(),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
            network_mode: NetworkMode::DirectOnly,
            bind_address: Some("127.0.0.1:0".parse().unwrap()),
            max_connections: 8,
            min_free_space_bytes: 0,
        })
        .await
        .unwrap();
        let original_id = server.endpoint_addr().id;
        let endpoint = bind_endpoint(client_key.clone(), NetworkMode::DirectOnly, None, None)
            .await
            .unwrap();
        let connection = endpoint
            .connect(server.endpoint_addr(), ALPN_V1)
            .await
            .unwrap();
        let (mut send, mut receive) = connection.open_bi().await.unwrap();
        let manifest = manifest_from_reader(&b"drain me"[..], ChunkingProfile::DEFAULT).unwrap();
        write_frame(
            &mut send,
            &WireRequest::Push {
                path: WirePath::new("drained.txt").unwrap(),
                manifest: manifest.clone(),
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            read_frame::<WireResponse>(&mut receive).await.unwrap(),
            WireResponse::NeedChunks { .. }
        ));
        {
            let pause = server.pause();
            tokio::pin!(pause);
            assert!(
                tokio::time::timeout(Duration::from_millis(75), &mut pause)
                    .await
                    .is_err()
            );
            assert!(!destination.path().join("drained.txt").exists());
            let chunk = &manifest.chunks[0];
            write_frame(
                &mut send,
                &ChunkHeader {
                    hash: chunk.hash,
                    length: chunk.length,
                },
            )
            .await
            .unwrap();
            send.write_all(b"drain me").await.unwrap();
            assert!(matches!(
                read_frame::<WireResponse>(&mut receive).await.unwrap(),
                WireResponse::Complete(_)
            ));
            // Draining means the operation is done; it must not depend on a cooperative peer closing QUIC.
            tokio::time::timeout(Duration::from_secs(5), &mut pause)
                .await
                .unwrap()
                .unwrap();
        }
        assert_eq!(
            fs::read(destination.path().join("drained.txt")).unwrap(),
            b"drain me"
        );
        let client = SyncClient {
            secret_key: client_key,
            remote: server.endpoint_addr(),
            network_mode: NetworkMode::DirectOnly,
        };
        let blocked = client
            .fetch_snapshot(&MerkleTree::from_records(Vec::new()).unwrap())
            .await;
        assert!(blocked.is_err(), "paused receiver must reject new V2 work");
        let directory = SyncRecord {
            schema_version: SYNC_RECORD_SCHEMA_V1,
            path: WirePath::new("after-resume").unwrap(),
            kind: SyncEntryKind::Directory,
            size: 0,
            content_hash: None,
            readonly: false,
            version: version(b"pause-client", 1),
            tombstone: false,
        };
        assert!(client.apply_metadata(directory.clone()).await.is_err());
        assert!(!destination.path().join("after-resume").exists());
        server.resume().await.unwrap();
        client.apply_metadata(directory).await.unwrap();
        assert!(destination.path().join("after-resume").is_dir());
        assert_eq!(server.endpoint_addr().id, original_id);
        let snapshot = client
            .fetch_snapshot(&MerkleTree::from_records(Vec::new()).unwrap())
            .await
            .unwrap();
        assert!(
            snapshot
                .records
                .iter()
                .any(|record| record.path.as_str() == "drained.txt")
        );
        connection.close(0_u8.into(), b"test done");
        endpoint.close().await;
        server.shutdown().await.unwrap();
    }

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

    fn verified_chunk(bytes: Vec<u8>) -> VerifiedChunk {
        let descriptor = deltaweave_core::ChunkDescriptor {
            offset: 0,
            length: u32::try_from(bytes.len()).expect("test chunk length fits in u32"),
            hash: Hash32::digest(&bytes),
        };
        VerifiedChunk::validate(&descriptor, bytes).expect("test chunk validates")
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

    async fn scripted_snapshot(
        summaries: Vec<MerkleNodeSummary>,
    ) -> (Result<RemoteSnapshot>, Vec<SyncWireRequest>) {
        let endpoint = bind_endpoint(
            SecretKey::generate(),
            NetworkMode::DirectOnly,
            Some(vec![ALPN_V2.to_vec()]),
            Some("127.0.0.1:0".parse().expect("loopback address is valid")),
        )
        .await
        .expect("scripted peer can bind");
        let client = SyncClient {
            secret_key: SecretKey::generate(),
            remote: endpoint_addr_with_local_fallback(&endpoint),
            network_mode: NetworkMode::DirectOnly,
        };
        let peer = tokio::spawn(async move {
            let connection = endpoint
                .accept()
                .await
                .expect("client connects")
                .await
                .expect("client authenticates");
            let (mut send, mut receive) =
                connection.accept_bi().await.expect("client opens stream");
            let mut summaries = VecDeque::from(summaries);
            let mut requests = Vec::new();
            while let Ok(request) = read_frame::<SyncWireRequest>(&mut receive).await {
                let response = match &request {
                    SyncWireRequest::QueryNode { .. } => match summaries.pop_front() {
                        Some(summary) => SyncWireResponse::Node {
                            summary: Some(summary),
                        },
                        None => SyncWireResponse::Error {
                            message: "script exhausted".into(),
                        },
                    },
                    SyncWireRequest::Finish => SyncWireResponse::Finished,
                    _ => panic!("unexpected snapshot request"),
                };
                let done = matches!(
                    response,
                    SyncWireResponse::Error { .. } | SyncWireResponse::Finished
                );
                requests.push(request);
                write_frame(&mut send, &response)
                    .await
                    .expect("scripted response can be sent");
                if done {
                    send.finish().expect("scripted response can finish");
                    connection.closed().await;
                    break;
                }
            }
            endpoint.close().await;
            requests
        });
        let local =
            MerkleTree::from_records(Vec::<SyncRecord>::new()).expect("empty tree is valid");
        let result = tokio::time::timeout(Duration::from_secs(15), client.fetch_snapshot(&local))
            .await
            .expect("snapshot validation finishes promptly");
        (result, peer.await.expect("scripted peer completes"))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn snapshot_rejects_malformed_children_before_scheduling_queries() {
        use deltaweave_reconcile::MerkleChildSummary;

        let child = MerkleChildSummary {
            name: "file".into(),
            hash: Hash32::digest(b"child"),
            record_count: 1,
        };
        let root = MerkleNodeSummary {
            prefix: String::new(),
            hash: Hash32::digest(b"root"),
            record_count: 1,
            record: None,
            children: vec![child.clone()],
        };
        let cases = [
            MerkleNodeSummary {
                children: vec![child.clone(), child.clone()],
                record_count: 2,
                ..root.clone()
            },
            MerkleNodeSummary {
                children: vec![MerkleChildSummary {
                    name: "nested/file".into(),
                    ..child.clone()
                }],
                ..root.clone()
            },
            MerkleNodeSummary {
                children: vec![MerkleChildSummary {
                    record_count: 0,
                    ..child.clone()
                }],
                record_count: 0,
                ..root.clone()
            },
            MerkleNodeSummary {
                record_count: 2,
                ..root.clone()
            },
            MerkleNodeSummary {
                record: Some(file_record("unrelated", b"", b"peer", 1)),
                record_count: 2,
                ..root
            },
        ];
        for summary in cases {
            let (result, requests) = scripted_snapshot(vec![summary]).await;
            assert!(result.is_err(), "malformed Merkle summaries must fail");
            assert_eq!(
                requests.len(),
                1,
                "invalid summaries must fail before follow-up requests"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn snapshot_rejects_child_that_changes_its_advertised_commitment() {
        use deltaweave_reconcile::MerkleChildSummary;

        let record = file_record("file", b"", b"peer", 1);
        let root = MerkleNodeSummary {
            prefix: String::new(),
            hash: Hash32::digest(b"root"),
            record_count: 1,
            record: None,
            children: vec![MerkleChildSummary {
                name: "file".into(),
                hash: Hash32::digest(b"advertised child"),
                record_count: 1,
            }],
        };
        let changed_child = MerkleNodeSummary {
            prefix: "file".into(),
            hash: Hash32::digest(b"different child"),
            record_count: 1,
            record: Some(record),
            children: Vec::new(),
        };
        let (result, requests) = scripted_snapshot(vec![root, changed_child]).await;
        assert!(result.is_err());
        assert_eq!(
            requests.len(),
            2,
            "changed child must fail before snapshot completion"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shared_connection_limit_covers_v1_and_v2_until_transfer_finishes() {
        let state = TempDir::new().unwrap();
        let destination = TempDir::new().unwrap();
        let first_key = SecretKey::generate();
        let second_key = SecretKey::generate();
        let server = start_server(ServerConfig {
            secret_key: SecretKey::generate(),
            destination_root: destination.path().into(),
            state_root: state.path().into(),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([
                first_key.public(),
                second_key.public(),
            ])),
            network_mode: NetworkMode::DirectOnly,
            bind_address: Some("127.0.0.1:0".parse().unwrap()),
            max_connections: 1,
            min_free_space_bytes: 0,
        })
        .await
        .unwrap();
        let endpoint = bind_endpoint(first_key, NetworkMode::DirectOnly, None, None)
            .await
            .unwrap();
        let connection = endpoint
            .connect(server.endpoint_addr(), ALPN_V1)
            .await
            .unwrap();
        let (mut send, mut receive) = connection.open_bi().await.unwrap();
        let manifest =
            manifest_from_reader(&b"held payload"[..], ChunkingProfile::DEFAULT).unwrap();
        write_frame(
            &mut send,
            &WireRequest::Push {
                path: WirePath::new("held.txt").unwrap(),
                manifest: manifest.clone(),
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            read_frame::<WireResponse>(&mut receive).await.unwrap(),
            WireResponse::NeedChunks { .. }
        ));

        let client = SyncClient {
            secret_key: second_key,
            remote: server.endpoint_addr(),
            network_mode: NetworkMode::DirectOnly,
        };
        assert!(
            client
                .fetch_snapshot(&MerkleTree::from_records(Vec::new()).unwrap())
                .await
                .is_err(),
            "V1 must consume the shared V1/V2 connection permit"
        );

        let chunk = &manifest.chunks[0];
        write_frame(
            &mut send,
            &ChunkHeader {
                hash: chunk.hash,
                length: chunk.length,
            },
        )
        .await
        .unwrap();
        send.write_all(b"held payload").await.unwrap();
        assert!(matches!(
            read_frame::<WireResponse>(&mut receive).await.unwrap(),
            WireResponse::Complete(_)
        ));
        connection.close(0_u8.into(), b"complete");
        endpoint.close().await;

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if client
                    .fetch_snapshot(&MerkleTree::from_records(Vec::new()).unwrap())
                    .await
                    .is_ok()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("permit is released after V1 transfer finishes");
        server.shutdown().await.unwrap();
    }

    #[test]
    fn disk_admission_checks_state_and_destination_filesystems() {
        let state = TempDir::new().expect("state");
        let destination = TempDir::new().expect("destination");
        let admission = DiskAdmission::new(
            state.path().to_path_buf(),
            destination.path().to_path_buf(),
            0,
            1,
        );
        admission.check_state(1).expect("state admits small write");
        admission
            .check_materialization(1)
            .expect("destination admits small materialization");
        assert!(admission.check_state(u64::MAX).is_err());
        assert!(admission.check_materialization(u64::MAX).is_err());
    }

    #[test]
    fn shared_budget_combines_cas_and_destination_while_distinct_budgets_do_not() {
        let shared = FilesystemBudget {
            state_available: 300,
            destination_available: 300,
            shared: true,
        };
        assert!(check_filesystem_budget(shared, 100, 100, 128).is_err());

        let distinct = FilesystemBudget {
            state_available: 228,
            destination_available: 228,
            shared: false,
        };
        check_filesystem_budget(distinct, 100, 100, 128)
            .expect("distinct filesystems each have their own complete budget");

        let exact_shared = FilesystemBudget {
            state_available: 328,
            destination_available: 328,
            shared: true,
        };
        check_filesystem_budget(exact_shared, 100, 100, 128)
            .expect("combined shared budget admits an exact fit");
        assert_eq!(exact_shared.state_available - 100 - 100, 128);

        let after_cas = FilesystemBudget {
            state_available: 228,
            destination_available: 228,
            shared: true,
        };
        check_filesystem_budget(after_cas, 0, 100, 128)
            .expect("materialization recheck preserves the exact reserve");
        assert!(
            check_filesystem_budget(
                FilesystemBudget {
                    state_available: 227,
                    destination_available: 227,
                    shared: true,
                },
                0,
                100,
                128,
            )
            .is_err(),
            "a changed post-CAS budget must block materialization"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn receiver_impossible_reserve_rejects_before_destination_or_cas_write() {
        let state = TempDir::new().expect("state");
        let destination = TempDir::new().expect("destination");
        let source_dir = TempDir::new().expect("source");
        let source = source_dir.path().join("payload.bin");
        fs::write(&source, fixture(256 * 1024)).expect("source can be written");
        let client_key = SecretKey::generate();
        let server = start_server(ServerConfig {
            secret_key: SecretKey::generate(),
            destination_root: destination.path().to_path_buf(),
            state_root: state.path().to_path_buf(),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
            network_mode: NetworkMode::DirectOnly,
            bind_address: Some("127.0.0.1:0".parse().unwrap()),
            max_connections: 8,
            min_free_space_bytes: u64::MAX,
        })
        .await
        .expect("server can start with a conservative reserve");

        let result = push_file(PushOptions {
            secret_key: client_key,
            source,
            remote_path: WirePath::new("rejected.bin").unwrap(),
            remote: server.endpoint_addr(),
            profile: ChunkingProfile::DEFAULT,
            network_mode: NetworkMode::DirectOnly,
            state_root: None,
        })
        .await;
        assert!(result.is_err());
        assert!(!destination.path().join("rejected.bin").exists());
        assert_eq!(regular_files_below(&state.path().join("chunks")), 0);
        server.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn receiver_successful_shared_transfer_preserves_configured_reserve() {
        const HEADROOM: u64 = 64 * 1024 * 1024;
        let root = TempDir::new().expect("shared filesystem root");
        let state = root.path().join("state");
        let destination = root.path().join("destination");
        let source = root.path().join("source.bin");
        fs::create_dir(&state).unwrap();
        fs::create_dir(&destination).unwrap();
        fs::write(&source, fixture(256 * 1024)).unwrap();
        let available = fs2::available_space(root.path()).unwrap();
        assert!(
            available > HEADROOM,
            "test filesystem needs bounded headroom"
        );
        let reserve = available - HEADROOM;
        let client_key = SecretKey::generate();
        let server = start_server(ServerConfig {
            secret_key: SecretKey::generate(),
            destination_root: destination.clone(),
            state_root: state.clone(),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
            network_mode: NetworkMode::DirectOnly,
            bind_address: Some("127.0.0.1:0".parse().unwrap()),
            max_connections: 8,
            min_free_space_bytes: reserve,
        })
        .await
        .unwrap();

        push_file(PushOptions {
            secret_key: client_key,
            source,
            remote_path: WirePath::new("accepted.bin").unwrap(),
            remote: server.endpoint_addr(),
            profile: ChunkingProfile::DEFAULT,
            network_mode: NetworkMode::DirectOnly,
            state_root: None,
        })
        .await
        .expect("bounded transfer succeeds");
        assert!(destination.join("accepted.bin").is_file());
        assert!(fs2::available_space(root.path()).unwrap() >= reserve);
        server.shutdown().await.unwrap();
    }

    #[test]
    fn missing_chunk_bytes_counts_each_hash_once() {
        let bytes = fixture(128 * 1024);
        let manifest = manifest_from_reader(
            std::io::Cursor::new([bytes.clone(), bytes].concat()),
            ChunkingProfile {
                version: 1,
                min_size: 64 * 1024,
                avg_size: 128 * 1024,
                max_size: 256 * 1024,
            },
        )
        .expect("manifest");
        let missing = vec![manifest.chunks[0].hash];
        assert_eq!(
            unique_missing_chunk_bytes(&manifest, &missing).expect("byte count"),
            u64::from(manifest.chunks[0].length)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn chunk_writer_rechecks_disk_reserve_before_persistence() {
        let state = TempDir::new().expect("state");
        let store = Arc::new(Store::open(state.path()).expect("store"));
        let reserve = fs2::available_space(state.path()).expect("free space");
        let mut writer = ChunkWritePipeline::with_admission(
            Arc::clone(&store),
            1,
            CHUNK_WRITE_MAX_QUEUED_BYTES,
            DiskAdmission::new(
                state.path().to_path_buf(),
                state.path().to_path_buf(),
                reserve,
                0,
            ),
        );
        writer
            .push(verified_chunk(fixture(4096)))
            .await
            .expect("chunk queues");
        assert!(writer.finish().await.is_err());
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
    async fn chunk_writer_flush_drains_remaining_after_oldest_fails() {
        let state = TempDir::new().expect("state directory can be created");
        let store = Arc::new(Store::open(state.path()).expect("store can open"));
        let first_bytes = fixture(64 * 1024);
        let first_hash = Hash32::digest(&first_bytes);
        let blocked_parent = state.path().join("chunks").join(&first_hash.to_hex()[..2]);
        fs::create_dir_all(blocked_parent.parent().expect("chunks directory exists"))
            .expect("chunk store parent can be created");
        fs::write(&blocked_parent, b"not a directory").expect("chunk parent is blocked");
        let mut writer = ChunkWritePipeline::new(Arc::clone(&store), 2);
        writer
            .push(verified_chunk(first_bytes))
            .await
            .expect("failing batch is queued before persistence");
        writer
            .flush_pending()
            .await
            .expect("failing task is spawned");

        let mut good = Vec::new();
        let mut next = 65 * 1024;
        while good.len() < CHUNK_WRITE_BATCH {
            let bytes = fixture(next);
            next += 1;
            let hash = Hash32::digest(&bytes);
            if hash.to_hex()[..2] == first_hash.to_hex()[..2] {
                continue;
            }
            good.push((hash, bytes));
        }
        for (_, bytes) in &good {
            writer
                .push(verified_chunk(bytes.clone()))
                .await
                .expect("second batch is queued before the oldest task is joined");
        }

        let mut result = Ok(());
        for index in 0..CHUNK_WRITE_BATCH {
            let bytes = fixture(70 * 1024 + index);
            if let Err(error) = writer.push(verified_chunk(bytes)).await {
                result = Err(error);
                break;
            }
        }

        assert!(result.is_err(), "oldest failed batch must fail the flush");
        for (hash, bytes) in good {
            assert_eq!(
                store
                    .chunks()
                    .read_verified(hash)
                    .expect("later in-flight batch is drained after the oldest failure"),
                bytes
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn chunk_writer_flushes_when_queued_bytes_exceed_budget() {
        let state = TempDir::new().expect("state directory can be created");
        let store = Arc::new(Store::open(state.path()).expect("store can open"));
        let first = fixture(64 * 1024);
        let first_hash = Hash32::digest(&first);
        let second = fixture(64 * 1024 + 1);
        let second_hash = Hash32::digest(&second);
        let mut writer = ChunkWritePipeline::with_limits(Arc::clone(&store), 1, first.len() + 1);

        writer
            .push(verified_chunk(first.clone()))
            .await
            .expect("first chunk stays pending under the byte budget");
        assert_eq!(writer.inflight.len(), 0);
        assert_eq!(writer.pending.len(), 1);

        writer
            .push(verified_chunk(second.clone()))
            .await
            .expect("second chunk flushes the pending batch to stay in budget");
        assert!(
            writer.queued_bytes() <= first.len() + 1,
            "pipeline stays at or under the configured byte budget after enqueue"
        );
        writer
            .finish()
            .await
            .expect("budget-limited pipeline persists both chunks");

        assert_eq!(
            store
                .chunks()
                .read_verified(first_hash)
                .expect("first budgeted chunk can be read"),
            first
        );
        assert_eq!(
            store
                .chunks()
                .read_verified(second_hash)
                .expect("second budgeted chunk can be read"),
            second
        );
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

        for (_hash, bytes) in &chunks {
            writer
                .push(verified_chunk(bytes.clone()))
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
            bind_address: Some("127.0.0.1:0".parse().expect("ephemeral bind is valid")),
            max_connections: 64,
            min_free_space_bytes: 0,
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
            state_root: None,
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
            state_root: None,
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
            state_root: None,
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
            state_root: None,
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

    #[test]
    fn sender_manifest_cache_reuses_unchanged_files_and_invalidates_on_metadata() {
        let root = TempDir::new().expect("temporary directory can be created");
        let cache = root.path().join("sender-state");
        let source = root.path().join("payload.bin");
        fs::write(&source, fixture(128 * 1024)).expect("source can be written");

        let first = prepare_sender_manifest(&source, ChunkingProfile::DEFAULT, Some(&cache))
            .expect("first manifest can be built");
        let cached = prepare_sender_manifest(&source, ChunkingProfile::DEFAULT, Some(&cache))
            .expect("cached manifest can be reused");
        assert_eq!(first, cached);

        let mut permissions = fs::metadata(&source)
            .expect("source metadata can be read")
            .permissions();
        permissions.set_readonly(true);
        fs::set_permissions(&source, permissions).expect("source can be marked readonly");
        let after_readonly =
            prepare_sender_manifest(&source, ChunkingProfile::DEFAULT, Some(&cache))
                .expect("readonly change rebuilds the manifest");
        assert_eq!(after_readonly.file_hash, first.file_hash);

        let mut permissions = fs::metadata(&source)
            .expect("source metadata can be read")
            .permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            permissions.set_mode(permissions.mode() | 0o200);
        }
        #[cfg(not(unix))]
        permissions.set_readonly(false);
        fs::set_permissions(&source, permissions).expect("source can be made writable");
        let mut mutated = fixture(128 * 1024);
        mutated[0] ^= 1;
        fs::write(&source, mutated).expect("same-size source can be rewritten");
        let rebuilt = prepare_sender_manifest(&source, ChunkingProfile::DEFAULT, Some(&cache))
            .expect("same-size mutation rebuilds the manifest");
        assert_ne!(rebuilt.file_hash, first.file_hash);
        assert_eq!(rebuilt.size, first.size);
    }

    #[test]
    fn cached_manifest_requires_matching_before_and_after_handle_fingerprints() {
        let fingerprint = SourceFingerprint {
            identity: Some((1, 2)),
            size: 128,
            modified_ns: Some(3),
            changed_ns: Some(5),
            readonly: false,
        };
        let changed = SourceFingerprint {
            modified_ns: Some(4),
            ..fingerprint
        };
        let changed_ctime = SourceFingerprint {
            changed_ns: Some(6),
            ..fingerprint
        };
        let without_identity = SourceFingerprint {
            identity: None,
            ..fingerprint
        };
        let without_changed_ns = SourceFingerprint {
            changed_ns: None,
            ..fingerprint
        };
        let manifest = FileManifest {
            schema_version: deltaweave_core::MANIFEST_SCHEMA_V1,
            size: 128,
            file_hash: Hash32::digest(&[0_u8; 128]),
            profile: ChunkingProfile::DEFAULT,
            chunks: vec![deltaweave_core::ChunkDescriptor {
                offset: 0,
                length: 128,
                hash: Hash32::digest(&[0_u8; 128]),
            }],
        };

        assert!(manifest_fingerprints_match(
            &manifest,
            fingerprint,
            fingerprint
        ));
        assert!(!manifest_fingerprints_match(
            &manifest,
            fingerprint,
            changed
        ));
        assert!(!manifest_fingerprints_match(
            &manifest,
            fingerprint,
            changed_ctime
        ));
        assert!(!manifest_fingerprints_match(
            &FileManifest {
                size: 127,
                ..manifest
            },
            fingerprint,
            fingerprint
        ));
        assert!(sender_cache_eligible(&fingerprint));
        assert!(!sender_cache_eligible(&without_identity));
        assert!(!sender_cache_eligible(&without_changed_ns));
    }

    #[test]
    fn sender_manifest_cache_fails_open_when_the_database_cannot_be_locked() {
        let root = TempDir::new().expect("temporary directory can be created");
        let cache = root.path().join("sender-state");
        let source = root.path().join("payload.bin");
        fs::write(&source, fixture(64 * 1024)).expect("source can be written");
        fs::create_dir_all(&cache).expect("cache directory can be created");
        fs::write(cache.join("sender-manifests.redb"), b"not a database")
            .expect("corrupt cache can be written");

        let manifest = prepare_sender_manifest(&source, ChunkingProfile::DEFAULT, Some(&cache))
            .expect("corrupt cache is ignored");
        assert_eq!(manifest.size, 64 * 1024);
    }

    #[cfg(unix)]
    #[test]
    fn sender_manifest_cache_creates_private_database_in_existing_public_root() {
        use std::os::unix::fs::PermissionsExt;

        let root = TempDir::new().expect("sender cache root can be created");
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o755))
            .expect("existing cache parent is searchable");
        drop(SenderManifestCache::open(root.path()).expect("sender cache can be opened"));
        let database = root.path().join("sender-manifests.redb");
        assert_eq!(
            fs::metadata(&database).unwrap().permissions().mode() & 0o077,
            0,
            "a new database must keep source metadata private in an existing public parent"
        );
        assert_eq!(
            fs::metadata(root.path()).unwrap().permissions().mode() & 0o777,
            0o755,
            "existing parent permissions are preserved"
        );

        fs::set_permissions(&database, fs::Permissions::from_mode(0o640))
            .expect("administrator can configure existing database permissions");
        drop(SenderManifestCache::open(root.path()).expect("existing database can be reused"));
        assert_eq!(
            fs::metadata(&database).unwrap().permissions().mode() & 0o777,
            0o640,
            "existing database permissions are preserved"
        );
    }

    #[cfg(unix)]
    #[test]
    fn sender_manifest_cache_creates_private_root_without_changing_existing_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let root = TempDir::new().expect("temporary root can be created");
        let state = root.path().join("sender-state");
        drop(SenderManifestCache::open(&state).expect("sender manifest cache can be opened"));
        assert_eq!(
            fs::metadata(&state)
                .expect("sender cache metadata is readable")
                .permissions()
                .mode()
                & 0o077,
            0,
            "source paths and fingerprints require a private sender cache"
        );

        fs::set_permissions(&state, fs::Permissions::from_mode(0o750))
            .expect("administrator can configure existing permissions");
        drop(SenderManifestCache::open(&state).expect("existing sender cache can be reused"));
        assert_eq!(
            fs::metadata(&state)
                .expect("sender cache metadata is readable")
                .permissions()
                .mode()
                & 0o777,
            0o750
        );
    }

    #[cfg(unix)]
    #[test]
    fn server_creates_private_state_root_without_changing_existing_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let root = TempDir::new().expect("temporary root can be created");
        let state = root.path().join("state");
        prepare_server_roots(&root.path().join("destination"), &state)
            .expect("separate server roots can be created");
        assert_eq!(
            fs::metadata(&state)
                .expect("state metadata is readable")
                .permissions()
                .mode()
                & 0o077,
            0
        );

        fs::set_permissions(&state, fs::Permissions::from_mode(0o750))
            .expect("administrator can configure existing permissions");
        prepare_server_roots(&root.path().join("destination"), &state)
            .expect("existing server roots can be reused");
        assert_eq!(
            fs::metadata(&state)
                .expect("state metadata is readable")
                .permissions()
                .mode()
                & 0o777,
            0o750
        );
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
            bind_address: None,
            max_connections: 64,
            min_free_space_bytes: 0,
        })
        .await;

        assert!(result.is_err());
    }

    fn advertised_socket(server: &Server) -> SocketAddr {
        server
            .address_info()
            .direct_addresses
            .first()
            .expect("server advertises a direct address")
            .parse()
            .expect("advertised address is a socket address")
    }

    #[tokio::test]
    async fn direct_server_reuses_configured_port_after_restart() {
        let state = TempDir::new().expect("state directory can be created");
        let destination = TempDir::new().expect("destination can be created");
        let secret_key = SecretKey::generate();
        let first = start_server(ServerConfig {
            secret_key: secret_key.clone(),
            destination_root: destination.path().to_path_buf(),
            state_root: state.path().to_path_buf(),
            peer_policy: PeerPolicy::AllowListed(HashSet::new()),
            network_mode: NetworkMode::DirectOnly,
            bind_address: Some("127.0.0.1:0".parse().expect("ephemeral bind is valid")),
            max_connections: 64,
            min_free_space_bytes: 0,
        })
        .await
        .expect("server can bind an ephemeral local address");
        let bind_address = advertised_socket(&first);
        assert_eq!(bind_address.ip(), std::net::Ipv4Addr::LOCALHOST);
        assert_ne!(bind_address.port(), 0);
        first.shutdown().await.expect("first server shuts down");

        let second = start_server(ServerConfig {
            secret_key,
            destination_root: destination.path().to_path_buf(),
            state_root: state.path().to_path_buf(),
            peer_policy: PeerPolicy::AllowListed(HashSet::new()),
            network_mode: NetworkMode::DirectOnly,
            bind_address: Some(bind_address),
            max_connections: 64,
            min_free_space_bytes: 0,
        })
        .await
        .expect("server can rebind the previously assigned address");
        assert_eq!(
            second.address_info().direct_addresses,
            vec![bind_address.to_string()]
        );
        second.shutdown().await.expect("second server shuts down");
    }

    #[tokio::test]
    async fn configured_bind_address_fails_when_the_udp_port_is_already_taken() {
        let state = TempDir::new().expect("state directory can be created");
        let destination = TempDir::new().expect("destination can be created");
        let occupied = std::net::UdpSocket::bind("127.0.0.1:0").expect("occupied socket can bind");
        let bind_address = occupied
            .local_addr()
            .expect("occupied socket has a local address");
        let result = start_server(ServerConfig {
            secret_key: SecretKey::generate(),
            destination_root: destination.path().to_path_buf(),
            state_root: state.path().to_path_buf(),
            peer_policy: PeerPolicy::AllowListed(HashSet::new()),
            network_mode: NetworkMode::DirectOnly,
            bind_address: Some(bind_address),
            max_connections: 64,
            min_free_space_bytes: 0,
        })
        .await;
        assert!(result.is_err());
        drop(occupied);
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
            bind_address: None,
            max_connections: 64,
            min_free_space_bytes: 0,
        })
        .await
        .expect("server can start");
        assert!(server.wait_online(Duration::from_millis(250)).await);
        assert!(!server.address_info().direct_addresses.is_empty());
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
            bind_address: None,
            max_connections: 64,
            min_free_space_bytes: 0,
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
            state_root: None,
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn causal_push_rechecks_local_edits_after_chunk_negotiation() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let state = TempDir::new().expect("state directory can be created");
            let destination = TempDir::new().expect("destination can be created");
            let sources = TempDir::new().expect("source directory can be created");
            let source = sources.path().join("source.bin");
            let initial_bytes = b"initial";
            fs::write(&source, initial_bytes).expect("initial source can be written");
            let client_key = SecretKey::from_bytes(&[31; 32]);
            let server_key = SecretKey::from_bytes(&[32; 32]);
            let server_replica = ReplicaId(Hash32::digest(server_key.public().as_bytes()));
            let server = start_server(ServerConfig {
                secret_key: server_key,
                destination_root: destination.path().to_path_buf(),
                state_root: state.path().to_path_buf(),
                peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
                network_mode: NetworkMode::DirectOnly,
                bind_address: Some("127.0.0.1:0".parse().expect("loopback address is valid")),
                max_connections: 64,
                min_free_space_bytes: 0,
            })
            .await
            .expect("server can start");
            let client = SyncClient {
                secret_key: client_key,
                remote: server.endpoint_addr(),
                network_mode: NetworkMode::DirectOnly,
            };
            let initial = file_record("shared.bin", initial_bytes, b"client-a", 1);
            client
                .push_record(&source, initial.clone(), ChunkingProfile::DEFAULT)
                .await
                .expect("initial causal file can be seeded");

            let incoming_bytes = b"incoming upload";
            fs::write(&source, incoming_bytes).expect("incoming source can be written");
            let incoming = file_record("shared.bin", incoming_bytes, b"client-a", 2);
            let manifest = manifest_from_path(&source, ChunkingProfile::DEFAULT)
                .expect("incoming manifest can be built");
            let session = client.open_session().await.expect("session can open");
            let connection = session
                .endpoint
                .connect(server.endpoint_addr(), ALPN_V2)
                .await
                .expect("upload connection can open");
            let (mut send, mut receive) = connection.open_bi().await.expect("stream can open");
            write_frame(
                &mut send,
                &SyncWireRequest::PushRecord {
                    record: incoming.clone(),
                    manifest: manifest.clone(),
                },
            )
            .await
            .expect("causal push request can be sent");
            let SyncWireResponse::NeedChunks { hashes } = read_frame(&mut receive)
                .await
                .expect("receiver negotiates missing chunks")
            else {
                panic!("receiver must request the new content before applying it");
            };
            assert_eq!(hashes, vec![Hash32::digest(incoming_bytes)]);

            // NeedChunks leaves the receiver waiting for payload, making this edit deterministic.
            // Its different length also guarantees the authoritative scan observes the change.
            let local_bytes = b"receiver independently edited this file during the upload";
            fs::write(destination.path().join("shared.bin"), local_bytes)
                .expect("receiver can edit the file before payload arrives");
            send_requested_chunks(&mut send, &source, &manifest, hashes, None)
                .await
                .expect("requested payload can be uploaded");
            send.finish().expect("upload can finish");
            let response: SyncWireResponse = read_frame(&mut receive)
                .await
                .expect("receiver returns a causal decision");
            connection.close(0_u8.into(), b"test upload finished");
            session.close().await;
            assert!(
                matches!(response, SyncWireResponse::Error { .. }),
                "an upload based on the old record must be rejected: {response:?}"
            );
            assert_eq!(
                fs::read(destination.path().join("shared.bin")).expect("local edit remains"),
                local_bytes
            );
            assert_eq!(
                fs::read_dir(state.path().join("trash"))
                    .expect("replacement history can be read")
                    .count(),
                0,
                "a rejected upload must not replace the local edit"
            );
            let empty =
                MerkleTree::from_records(Vec::<SyncRecord>::new()).expect("empty tree is valid");
            let snapshot = client
                .fetch_snapshot(&empty)
                .await
                .expect("fresh receiver snapshot can be verified");
            let mut expected = file_record("shared.bin", local_bytes, b"client-a", 1);
            expected.version.observe(server_replica, 1);
            assert_eq!(snapshot.records, vec![expected]);
            server.shutdown().await.expect("server shuts down");
        })
        .await
        .expect("causal upload race completes within its timeout");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn causal_tombstones_preserve_conflicts_and_allow_idempotent_delete() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let state = TempDir::new().expect("state directory can be created");
            let destination = TempDir::new().expect("destination can be created");
            let sources = TempDir::new().expect("source directory can be created");
            let source = sources.path().join("source.bin");
            let bytes = b"content that must survive a conflicting delete";
            fs::write(&source, bytes).expect("source can be written");
            let client_key = SecretKey::from_bytes(&[33; 32]);
            let server = start_server(ServerConfig {
                secret_key: SecretKey::from_bytes(&[34; 32]),
                destination_root: destination.path().to_path_buf(),
                state_root: state.path().to_path_buf(),
                peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
                network_mode: NetworkMode::DirectOnly,
                bind_address: Some("127.0.0.1:0".parse().expect("loopback address is valid")),
                max_connections: 64,
                min_free_space_bytes: 0,
            })
            .await
            .expect("server can start");
            let client = SyncClient {
                secret_key: client_key,
                remote: server.endpoint_addr(),
                network_mode: NetworkMode::DirectOnly,
            };
            let current = file_record("shared.bin", bytes, b"client-a", 2);
            client
                .push_record(&source, current.clone(), ChunkingProfile::DEFAULT)
                .await
                .expect("current causal file can be seeded");
            let empty =
                MerkleTree::from_records(Vec::<SyncRecord>::new()).expect("empty tree is valid");

            for (case, clock) in [
                ("stale", version(b"client-a", 1)),
                ("equal clock with deleted state", version(b"client-a", 2)),
                ("concurrent", version(b"client-b", 1)),
            ] {
                let tombstone = SyncRecord {
                    version: clock,
                    tombstone: true,
                    ..current.clone()
                };
                let result = client.apply_metadata(tombstone).await;
                assert!(
                    result.is_err(),
                    "{case} deletion must be rejected: {result:?}"
                );
                assert_eq!(
                    fs::read(destination.path().join("shared.bin"))
                        .expect("conflicting delete must preserve the file"),
                    bytes,
                    "{case} deletion changed file contents"
                );
                let snapshot = client
                    .fetch_snapshot(&empty)
                    .await
                    .expect("receiver snapshot remains valid after rejected delete");
                assert_eq!(
                    snapshot.records,
                    vec![current.clone()],
                    "{case} deletion changed causal state"
                );
                assert_eq!(
                    fs::read_dir(state.path().join("trash"))
                        .expect("replacement history can be read")
                        .count(),
                    0,
                    "{case} deletion must not move the file into trash"
                );
            }

            let mut resolved_version = version(b"client-a", 3);
            resolved_version.merge(&version(b"client-b", 1));
            let resolved = SyncRecord {
                version: resolved_version,
                tombstone: true,
                ..current
            };
            let expected_receipt = SyncApplyReceipt {
                path: resolved.path.clone(),
                record_hash: resolved.logical_hash(),
                transferred_bytes: 0,
                reused_extents: 0,
            };
            for attempt in ["resolved delete", "identical retry"] {
                let receipt = client
                    .apply_metadata(resolved.clone())
                    .await
                    .expect("dominating deletion and its retry must succeed");
                assert_eq!(
                    receipt, expected_receipt,
                    "{attempt} receipt must bind the exact record"
                );
                assert!(!destination.path().join("shared.bin").exists());
                let snapshot = client
                    .fetch_snapshot(&empty)
                    .await
                    .expect("deleted record can be independently verified");
                assert_eq!(
                    snapshot.records,
                    vec![resolved.clone()],
                    "{attempt} must retain the exact tombstone"
                );
                let backups: Vec<_> = fs::read_dir(state.path().join("trash"))
                    .expect("replacement history can be read")
                    .map(|entry| entry.expect("trash entry can be read").path())
                    .collect();
                assert_eq!(backups.len(), 1, "{attempt} must preserve exactly one copy");
                assert_eq!(
                    fs::read(backups[0].join("shared.bin")).expect("preserved content can be read"),
                    bytes,
                    "{attempt} must preserve the deleted content"
                );
            }
            server.shutdown().await.expect("server shuts down");
        })
        .await
        .expect("causal tombstone checks complete within their timeout");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn filesystem_errors_hide_receiver_paths_and_allow_recovery() {
        let root = tempfile::Builder::new()
            .prefix("receiver-private-sentinel-")
            .tempdir()
            .expect("synthetic private receiver root can be created");
        let destination = root.path().join("destination");
        fs::create_dir_all(destination.join("occupied"))
            .expect("occupied directory can be created");
        fs::write(destination.join("occupied/keep.txt"), b"keep me")
            .expect("existing contents can be created");
        let source = root.path().join("source.txt");
        fs::write(&source, b"new data").expect("synthetic source can be created");
        let client_key = SecretKey::generate();
        let server = start_server(ServerConfig {
            secret_key: SecretKey::generate(),
            destination_root: destination.clone(),
            state_root: root.path().join("state"),
            peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
            network_mode: NetworkMode::DirectOnly,
            bind_address: Some("127.0.0.1:0".parse().expect("loopback address is valid")),
            max_connections: 64,
            min_free_space_bytes: 0,
        })
        .await
        .expect("server can start");
        let options = PushOptions {
            secret_key: client_key.clone(),
            source,
            remote_path: WirePath::new("occupied").expect("path is portable"),
            remote: server.endpoint_addr(),
            profile: ChunkingProfile::DEFAULT,
            network_mode: NetworkMode::DirectOnly,
            state_root: None,
        };
        let push_error = push_file(options.clone())
            .await
            .expect_err("replacing a nonempty directory must fail")
            .to_string();
        let client = SyncClient {
            secret_key: client_key,
            remote: server.endpoint_addr(),
            network_mode: NetworkMode::DirectOnly,
        };
        let empty =
            MerkleTree::from_records(Vec::<SyncRecord>::new()).expect("empty tree is valid");
        let mut directory = client
            .fetch_snapshot(&empty)
            .await
            .expect("snapshot remains available after rejected push")
            .records
            .into_iter()
            .find(|record| record.path.as_str() == "occupied")
            .expect("snapshot includes occupied directory");
        directory
            .version
            .increment(ReplicaId(Hash32::digest(b"synthetic-client")))
            .expect("new version fits");
        directory.tombstone = true;
        let metadata_error = client
            .apply_metadata(directory)
            .await
            .expect_err("removing a nonempty directory must fail")
            .to_string();

        push_file(PushOptions {
            remote_path: WirePath::new("recovered.txt").expect("path is portable"),
            ..options
        })
        .await
        .expect("a corrected push succeeds after the error");
        client
            .apply_metadata(SyncRecord {
                schema_version: SYNC_RECORD_SCHEMA_V1,
                path: WirePath::new("recovered-directory").expect("path is portable"),
                kind: SyncEntryKind::Directory,
                size: 0,
                content_hash: None,
                readonly: false,
                version: version(b"synthetic-client", 1),
                tombstone: false,
            })
            .await
            .expect("a corrected metadata operation succeeds after the error");
        server.shutdown().await.expect("server shuts down");
        assert_eq!(
            fs::read(destination.join("occupied/keep.txt")).unwrap(),
            b"keep me"
        );
        assert_eq!(
            fs::read(destination.join("recovered.txt")).unwrap(),
            b"new data"
        );
        assert!(destination.join("recovered-directory").is_dir());
        assert!(
            [&push_error, &metadata_error]
                .iter()
                .all(|message| !message.contains("receiver-private-sentinel-")),
            "wire errors disclose receiver paths: V1={push_error:?}, V2={metadata_error:?}"
        );
        for message in [&push_error, &metadata_error] {
            assert!(message.contains("directory is not empty"));
            assert!(message.contains("retry"));
        }
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
            bind_address: None,
            max_connections: 64,
            min_free_space_bytes: 0,
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
    #[test]
    fn legacy_shutdown_drains_blocking_apply_before_releasing_root() {
        if std::env::var_os("DW_LEGACY_DRAIN_CHILD").is_none() {
            let home = tempfile::tempdir().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "tests::legacy_shutdown_drains_blocking_apply_before_releasing_root",
                    "--nocapture",
                ])
                .env("DW_LEGACY_DRAIN_CHILD", "1")
                .env("HOME", home.path())
                .env("USERPROFILE", home.path())
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                use std::{future::Future, task::Poll};
                let temp = tempfile::tempdir().unwrap();
                let root = temp.path().join("root");
                let state = temp.path().join("state");
                let peer = SecretKey::generate();
                let source_root = temp.path().join("source");
                fs::create_dir(&source_root).unwrap();
                fs::write(source_root.join("file"), b"valid payload").unwrap();
                let index = LocalIndex::open(
                    &source_root,
                    temp.path().join("source-state/index.redb"),
                    ReplicaId(Hash32::digest(peer.public().as_bytes())),
                    IndexOptions::default(),
                )
                .unwrap();
                index.scan().unwrap();
                let record = index.sync_records().unwrap()[0].clone();
                let entered = Arc::new(tokio::sync::Notify::new());
                let release = Arc::new(std::sync::Barrier::new(2));
                let e = entered.clone();
                let r = release.clone();
                let observer = TransferObserver::new(move |event| {
                    if event.phase == "applying" {
                        e.notify_one();
                        r.wait();
                    }
                });
                let server = start_server_observed(
                    ServerConfig {
                        secret_key: SecretKey::generate(),
                        destination_root: root.clone(),
                        state_root: state,
                        peer_policy: PeerPolicy::AnyAuthenticated,
                        network_mode: NetworkMode::DirectOnly,
                        bind_address: None,
                        max_connections: 8,
                        min_free_space_bytes: 0,
                    },
                    Some(observer),
                )
                .await
                .unwrap();
                let client = SyncClient {
                    secret_key: peer,
                    remote: server.endpoint_addr(),
                    network_mode: NetworkMode::DirectOnly,
                };
                let pushing = tokio::spawn(async move {
                    client
                        .push_record(source_root.join("file"), record, ChunkingProfile::DEFAULT)
                        .await
                });
                tokio::time::timeout(Duration::from_secs(10), entered.notified())
                    .await
                    .unwrap();
                // Finish iroh cancellation before polling our public shutdown. Its
                // remaining obligation is the retained blocking application task.
                server.router.shutdown().await.unwrap();
                let shutdown = server.shutdown();
                tokio::pin!(shutdown);
                let returned_early = std::future::poll_fn(|cx| {
                    Poll::Ready(match shutdown.as_mut().poll(cx) {
                        Poll::Ready(result) => Some(result),
                        Poll::Pending => None,
                    })
                })
                .await;
                assert!(
                    root_admission::acquire(
                        &root,
                        root_admission::RootUse::Managed {
                            share: [1; 32],
                            owner: [2; 32]
                        }
                    )
                    .is_err()
                );
                tokio::task::spawn_blocking(move || release.wait())
                    .await
                    .unwrap();
                if returned_early.is_none() {
                    shutdown.await.unwrap();
                }
                let _ = pushing.await.unwrap();
                assert!(
                    returned_early.is_none(),
                    "shutdown returned with an outstanding disk task"
                );
                assert_eq!(fs::read(root.join("file")).unwrap(), b"valid payload");
                assert!(
                    root_admission::acquire(
                        &root,
                        root_admission::RootUse::Managed {
                            share: [1; 32],
                            owner: [2; 32]
                        }
                    )
                    .is_ok()
                );
            });
    }
}
