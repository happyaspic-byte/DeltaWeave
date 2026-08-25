//! Authenticated iroh transport and resumable delta transfer protocol.

#![forbid(unsafe_code)]

use std::{
    collections::{HashMap, HashSet},
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
use deltaweave_core::{ChunkDescriptor, ChunkingProfile, FileManifest, Hash32, WirePath};
use deltaweave_store::Store;
use iroh::{
    Endpoint, EndpointAddr, EndpointId, RelayUrl, SecretKey, TransportAddr,
    endpoint::{Connection, RecvStream, SendStream, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::{info, warn};

/// Versioned ALPN identifier for DeltaWeave's initial push protocol.
pub const ALPN_V1: &[u8] = b"deltaweave/sync/1";
const MAX_CONTROL_FRAME: usize = 16 * 1024 * 1024;
const MAX_CHUNKS_PER_FILE: usize = 250_000;
const MAX_FILE_SIZE: u64 = 16 * 1024 * 1024 * 1024 * 1024;

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
    if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
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
        Err(error) => Err(error)
            .with_context(|| format!("failed to create identity file {}", path.display())),
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
        self.router.endpoint().addr()
    }

    /// Returns data suitable for displaying or copying to a client.
    #[must_use]
    pub fn address_info(&self) -> AddressInfo {
        let address = self.endpoint_addr();
        AddressInfo {
            endpoint_id: address.id.to_string(),
            direct_addresses: address.ip_addrs().map(|value| value.to_string()).collect(),
            relay_urls: address
                .relay_urls()
                .map(ToString::to_string)
                .collect(),
        }
    }

    /// Waits for the internet-mode endpoint to establish discovery/relay reachability.
    pub async fn wait_online(&self, timeout: Duration) -> bool {
        if self.network_mode == NetworkMode::DirectOnly {
            return true;
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
    let store = Arc::new(Store::open(&config.state_root)?);
    let endpoint = bind_endpoint(
        config.secret_key,
        config.network_mode,
        Some(ALPN_V1.to_vec()),
    )
    .await?;
    let handler = PushHandler {
        store,
        destination_root: config.destination_root,
        peer_policy: config.peer_policy,
    };
    let router = Router::builder(endpoint).accept(ALPN_V1, handler).spawn();
    Ok(Server {
        router,
        network_mode: config.network_mode,
    })
}

async fn bind_endpoint(
    secret_key: SecretKey,
    mode: NetworkMode,
    alpn: Option<Vec<u8>>,
) -> Result<Endpoint> {
    let mut builder = match mode {
        NetworkMode::Internet => Endpoint::builder(presets::N0),
        NetworkMode::DirectOnly => Endpoint::builder(presets::Minimal),
    }
    .secret_key(secret_key);
    if let Some(alpn) = alpn {
        builder = builder.alpns(vec![alpn]);
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

/// Sends one file, transmitting only chunks the receiver reports missing.
pub async fn push_file(options: PushOptions) -> Result<TransferReceipt> {
    ensure!(options.source.is_file(), "source is not a regular file");
    let source_for_manifest = options.source.clone();
    let profile = options.profile;
    let manifest = tokio::task::spawn_blocking(move || {
        manifest_from_path(source_for_manifest, profile)
    })
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
    let mut source = tokio::fs::File::open(source_path).await?;
    let mut sent = HashSet::new();
    for hash in missing {
        ensure!(sent.insert(hash), "receiver requested duplicate chunk {hash}");
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
    }
    send.finish().context("finish transfer upload")?;

    match read_frame(&mut receive).await? {
        WireResponse::Complete(receipt) => {
            ensure!(receipt.file_hash == manifest.file_hash, "receipt file hash mismatch");
            ensure!(
                receipt.manifest_hash == manifest.manifest_hash(),
                "receipt manifest hash mismatch"
            );
            connection.close(0_u8.into(), b"complete");
            Ok(receipt)
        }
        WireResponse::Error { message } => bail!("receiver failed transfer: {message}"),
        WireResponse::Rejected { message } => bail!("receiver rejected peer: {message}"),
        WireResponse::NeedChunks { .. } => bail!("receiver sent a second chunk request"),
    }
}

#[derive(Clone)]
struct PushHandler {
    store: Arc<Store>,
    destination_root: PathBuf,
    peer_policy: PeerPolicy,
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
        let (mut send, mut receive) = connection.accept_bi().await?;
        if !self.peer_policy.allows(peer) {
            warn!(%peer, "rejected unauthorized DeltaWeave peer");
            let _ = write_frame(
                &mut send,
                &WireResponse::Rejected {
                    message: "endpoint ID is not allow-listed".to_owned(),
                },
            )
            .await;
            let _ = send.finish();
            return Ok(());
        }

        info!(%peer, "accepted DeltaWeave peer");
        if let Err(error) = self.handle_push(&mut send, &mut receive).await {
            warn!(%peer, error = %error, "DeltaWeave transfer failed");
            let message = truncate_error(&error.to_string());
            let _ = write_frame(&mut send, &WireResponse::Error { message }).await;
            let _ = send.finish();
            return Err(AcceptError::from_err(std::io::Error::other(
                error.to_string(),
            )));
        }
        send.finish()?;
        Ok(())
    }
}

impl PushHandler {
    async fn handle_push(
        &self,
        send: &mut SendStream,
        receive: &mut RecvStream,
    ) -> Result<()> {
        let request: WireRequest = read_frame(receive).await?;
        let (path, manifest) = match request {
            WireRequest::Push { path, manifest } => (path, manifest),
        };
        manifest.validate()?;
        ensure!(manifest.size <= MAX_FILE_SIZE, "file exceeds protocol size limit");
        ensure!(
            manifest.chunks.len() <= MAX_CHUNKS_PER_FILE,
            "manifest exceeds chunk-count limit"
        );

        let missing = self.store.missing_chunks(&manifest);
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

            let store = Arc::clone(&self.store);
            tokio::task::spawn_blocking(move || store.chunks().put_verified(header.hash, &bytes))
                .await
                .context("chunk-store task failed")??;
            transferred_bytes = transferred_bytes
                .checked_add(u64::from(header.length))
                .context("transferred-byte counter overflow")?;
        }

        let store = Arc::clone(&self.store);
        let root = self.destination_root.clone();
        let materialize_manifest = manifest.clone();
        let materialize_path = path.clone();
        tokio::task::spawn_blocking(move || {
            store.materialize(&materialize_manifest, &materialize_path, root)
        })
        .await
        .context("materialization task failed")??;

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
            secret_key: client_key,
            source,
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
            peer_policy: PeerPolicy::AllowListed(HashSet::from([
                SecretKey::generate().public()
            ])),
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
        server.shutdown().await.expect("server shuts down");
    }
}
