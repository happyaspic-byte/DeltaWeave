//! DeltaWeave command-line interface.

#![forbid(unsafe_code)]

use std::{
    collections::HashSet,
    fs,
    io::Write,
    net::{IpAddr, SocketAddr, UdpSocket},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use clap::{Args, Parser, Subcommand};
use deltaweave_cdc::manifest_from_path;
use deltaweave_core::{ChunkingProfile, Hash32, ReplicaId, WirePath};
use deltaweave_index::{IndexOptions, LocalIndex, ScanChange, WatchService};
use deltaweave_net::{
    NetworkMode, PeerPolicy, PushOptions, Server, ServerConfig, SyncClient, TransferReceipt,
    access::{AccessStore, PairingTicket, unix_now},
    endpoint_addr, load_or_create_identity, push_file,
    quota::QuotaPolicy,
    redeem_pairing_ticket, start_server,
};
use deltaweave_sync::{SyncConfig, SyncEngine};
use iroh::{EndpointId, SecretKey};
use serde_json::json;
use tracing_subscriber::EnvFilter;

const QUICK_PROFILE_NAME: &str = "profile.json";
const QUICK_SHARE_QUOTA: QuotaPolicy = QuotaPolicy {
    bytes_per_second: 8 * 1024 * 1024,
    burst_bytes: 16 * 1024 * 1024,
    max_concurrent_operations_per_peer: 4,
    max_storage_bytes: 10 * 1024 * 1024 * 1024,
};

#[derive(Debug, Parser)]
#[command(
    name = "deltaweave",
    version,
    about = "Authenticated, content-defined bidirectional P2P file synchronization",
    long_about = None
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create or inspect a persistent node identity.
    Init(InitArgs),
    /// Build and print a deterministic FastCDC/BLAKE3 manifest.
    Manifest(ManifestArgs),
    /// Receive authenticated file pushes.
    Serve(ServeArgs),
    /// Send one file and transfer only chunks missing at the receiver.
    Push(PushArgs),
    /// Build or refresh the authoritative local directory index once.
    Scan(ScanArgs),
    /// Continuously index a directory using native watcher hints and periodic reconciliation.
    Watch(WatchArgs),
    /// Reconcile a local folder with a peer once and verify both Merkle roots.
    SyncOnce(SyncTargetArgs),
    /// Continuously reconcile a local folder with retry/backoff until stopped.
    Sync(SyncArgs),
    /// Manage pairing tickets, authorized peers, and identity rotation.
    Pair(PairArgs),
    /// Attach to or run the local daemon.
    Daemon(DaemonArgs),
    /// Pair this PC using one ticket and remember the peer.
    Connect {
        /// Printable `dwpair1:` ticket.
        ticket: String,
    },
    /// Synchronize one folder using the remembered peer.
    SyncFolder {
        /// Local folder to synchronize.
        folder: PathBuf,
        /// Keep synchronizing until Ctrl-C.
        #[arg(long)]
        watch: bool,
    },
    /// Share one folder and accept paired peers.
    Share {
        /// Local folder to share.
        folder: PathBuf,
        /// Fixed UDP listen address.
        #[arg(long, default_value = "0.0.0.0:17891")]
        bind: SocketAddr,
    },
    /// Run an isolated local end-to-end transfer and delta-reuse check.
    SelfTest,
}

#[derive(Debug, Args)]
struct InitArgs {
    /// Persistent secret-key file.
    #[arg(long, default_value = ".deltaweave/identity.key")]
    identity: PathBuf,
}

#[derive(Debug, Args)]
struct ManifestArgs {
    /// File to chunk and hash.
    file: PathBuf,
    #[command(flatten)]
    chunking: ChunkingArgs,
}

#[derive(Clone, Copy, Debug, Args)]
struct ChunkingArgs {
    /// Minimum FastCDC chunk size in bytes.
    #[arg(long, default_value_t = ChunkingProfile::DEFAULT.min_size)]
    min_chunk: u32,
    /// Average FastCDC chunk size in bytes.
    #[arg(long, default_value_t = ChunkingProfile::DEFAULT.avg_size)]
    avg_chunk: u32,
    /// Maximum FastCDC chunk size in bytes.
    #[arg(long, default_value_t = ChunkingProfile::DEFAULT.max_size)]
    max_chunk: u32,
}

impl ChunkingArgs {
    fn profile(self) -> Result<ChunkingProfile> {
        let profile = ChunkingProfile {
            version: 1,
            min_size: self.min_chunk,
            avg_size: self.avg_chunk,
            max_size: self.max_chunk,
        };
        profile.validate()?;
        Ok(profile)
    }
}

#[derive(Debug, Args)]
struct ServeArgs {
    /// Directory beneath which received files are materialized.
    #[arg(long)]
    root: PathBuf,
    /// Private metadata, chunk, journal, and trash directory.
    #[arg(long, default_value = ".deltaweave/state")]
    state: PathBuf,
    /// Durable peer authorization and stable replica database.
    #[arg(long, default_value = ".deltaweave/state/access.redb")]
    access: PathBuf,
    /// Persistent secret-key file.
    #[arg(long, default_value = ".deltaweave/identity.key")]
    identity: PathBuf,
    /// Endpoint ID authorized to push; repeat for multiple peers.
    #[arg(long = "allow-peer")]
    allowed_peers: Vec<String>,
    /// Accept any cryptographically authenticated peer (unsafe on public networks).
    #[arg(long, conflicts_with = "allowed_peers")]
    allow_any_authenticated: bool,
    /// Disable discovery and relay services; advertise direct addresses only.
    #[arg(long)]
    direct_only: bool,
    /// Resolve peer authorization from durable state instead of CLI flags.
    #[arg(long, conflicts_with_all = ["allowed_peers", "allow_any_authenticated"])]
    durable_access: bool,
    /// Sustained per-peer receive rate in bytes per second; 0 disables pacing.
    #[arg(long, default_value_t = 0)]
    rate_bytes_per_second: u64,
    /// Token-bucket burst above the sustained rate in bytes.
    #[arg(long, default_value_t = 0)]
    burst_bytes: u64,
    /// Simultaneous in-flight operations per peer; 0 means unlimited.
    #[arg(long, default_value_t = 0)]
    max_concurrent_operations: u32,
    /// Maximum unique CAS bytes stored by this node; 0 means unlimited.
    #[arg(long, default_value_t = 0)]
    max_storage_bytes: u64,
    /// Fixed UDP bind address so pairing tickets survive server restart.
    #[arg(long)]
    bind: Option<SocketAddr>,
}

#[derive(Debug, Args)]
struct DaemonArgs {
    #[command(subcommand)]
    command: DaemonCommand,
}

#[derive(Debug, Subcommand)]
enum DaemonCommand {
    /// Run the daemon in the foreground.
    Run,
    /// Print Hello from a live daemon.
    Status,
}

#[derive(Debug, Args)]
struct PairArgs {
    #[command(subcommand)]
    command: PairCommand,
}

#[derive(Debug, Subcommand)]
enum PairCommand {
    /// Issue a single-use pairing ticket bound to this server's address.
    Issue {
        /// Ticket lifetime in seconds.
        #[arg(long, default_value_t = 600)]
        ttl_seconds: u64,
        /// Server direct UDP address embedded in the ticket.
        #[arg(long)]
        direct_address: SocketAddr,
        /// Durable access database.
        #[arg(long, default_value = ".deltaweave/state/access.redb")]
        access: PathBuf,
        /// Server identity whose endpoint ID is embedded in the ticket.
        #[arg(long, default_value = ".deltaweave/identity.key")]
        identity: PathBuf,
    },
    /// Redeem a ticket code, authorizing this endpoint with the server.
    Redeem {
        /// Printable ticket code (dwpair1:...).
        code: String,
        /// Local secret-key file used when redeeming.
        #[arg(long, default_value = ".deltaweave/identity.key")]
        identity: PathBuf,
    },
    /// List authorized peers.
    List {
        /// Durable access database.
        #[arg(long, default_value = ".deltaweave/state/access.redb")]
        access: PathBuf,
    },
    /// Revoke an authorized endpoint.
    Revoke {
        /// Durable access database.
        #[arg(long, default_value = ".deltaweave/state/access.redb")]
        access: PathBuf,
        /// Endpoint ID to revoke.
        endpoint_id: String,
    },
    /// Rotate the transport identity, keeping the stable replica identity.
    Rotate {
        /// Identity file to replace.
        #[arg(long, default_value = ".deltaweave/identity.key")]
        identity: PathBuf,
    },
}

#[derive(Debug, Args)]
struct PushArgs {
    /// Local file to send.
    source: PathBuf,
    /// Portable relative destination path (uses `/`, never `..`).
    #[arg(long)]
    remote_path: WirePath,
    /// Receiver endpoint ID.
    #[arg(long)]
    peer: String,
    /// Receiver direct UDP address; repeat when multiple addresses are advertised.
    #[arg(long = "direct")]
    direct_addresses: Vec<SocketAddr>,
    /// Receiver relay URL; repeat when multiple relays are advertised.
    #[arg(long = "relay")]
    relay_urls: Vec<String>,
    /// Persistent sender secret-key file.
    #[arg(long, default_value = ".deltaweave/identity.key")]
    identity: PathBuf,
    /// Disable discovery and relay services; use supplied direct addresses only.
    #[arg(long)]
    direct_only: bool,
    #[command(flatten)]
    chunking: ChunkingArgs,
}

#[derive(Debug, Args)]
struct ScanArgs {
    #[command(flatten)]
    index: IndexArgs,
    /// Include every persistent path and retry record in the JSON response.
    #[arg(long)]
    include_records: bool,
}

#[derive(Debug, Args)]
struct WatchArgs {
    #[command(flatten)]
    index: IndexArgs,
    /// Quiet period after the latest filesystem event.
    #[arg(long, default_value_t = 750)]
    debounce_ms: u64,
    /// Maximum time an event storm may postpone a scan.
    #[arg(long, default_value_t = 5_000)]
    max_debounce_ms: u64,
    /// Safety-net full rescan interval in seconds.
    #[arg(long, default_value_t = 600)]
    rescan_seconds: u64,
    /// Full-scan interval when native watching is unavailable or reports loss.
    #[arg(long, default_value_t = 5)]
    poll_fallback_seconds: u64,
}

#[derive(Debug, Args)]
struct IndexArgs {
    /// Directory whose local state is indexed.
    #[arg(long)]
    root: PathBuf,
    /// Private redb index file. If beneath root, its parent is excluded automatically.
    #[arg(long, default_value = ".deltaweave/index.redb")]
    state: PathBuf,
    /// Durable peer authorization and stable replica database.
    #[arg(long, default_value = ".deltaweave/state/access.redb")]
    access: PathBuf,
    /// Persistent node identity used to derive a stable replica ID.
    #[arg(long, default_value = ".deltaweave/identity.key")]
    identity: PathBuf,
    /// Maximum simultaneous file hashers; defaults to available CPUs capped at eight.
    #[arg(long)]
    hash_workers: Option<usize>,
    /// Path to exclude from indexing; repeat for multiple paths.
    #[arg(long = "ignore")]
    ignored_paths: Vec<PathBuf>,
}

#[derive(Debug, Args)]
struct SyncTargetArgs {
    /// Local folder participating in bidirectional synchronization.
    #[arg(long)]
    root: PathBuf,
    /// Private local index, CAS, journal, and recovery directory outside `root`.
    #[arg(long, default_value = ".deltaweave/sync-state")]
    state: PathBuf,
    /// Durable peer authorization and stable replica database.
    #[arg(long, default_value = ".deltaweave/state/access.redb")]
    access: PathBuf,
    /// Persistent local endpoint identity outside `root`.
    #[arg(long, default_value = ".deltaweave/identity.key")]
    identity: PathBuf,
    /// Remote receiver endpoint ID.
    #[arg(long)]
    peer: String,
    #[arg(long = "direct")]
    direct_addresses: Vec<SocketAddr>,
    /// Remote relay URL; repeat when multiple relays are advertised.
    #[arg(long = "relay")]
    relay_urls: Vec<String>,
    /// Disable discovery and relay services; use supplied direct addresses only.
    #[arg(long)]
    direct_only: bool,
    #[command(flatten)]
    chunking: ChunkingArgs,
}

#[derive(Debug, Args)]
struct SyncArgs {
    #[command(flatten)]
    target: SyncTargetArgs,
    /// Maximum delay between successful reconciliation passes (also polls remote changes).
    #[arg(long, default_value_t = 5)]
    interval_seconds: u64,
    /// Quiet period after the latest local filesystem event.
    #[arg(long, default_value_t = 750)]
    debounce_ms: u64,
    /// Maximum time a local event storm may postpone synchronization.
    #[arg(long, default_value_t = 5_000)]
    max_debounce_ms: u64,
    /// Maximum exponential retry delay after failures.
    #[arg(long, default_value_t = 300)]
    max_backoff_seconds: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    run(Cli::parse()).await
}

async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Init(args) => initialize(args),
        Command::Manifest(args) => print_manifest(args),
        Command::Serve(args) => serve(args).await,
        Command::Push(args) => push(args).await,
        Command::Scan(args) => scan(args),
        Command::Watch(args) => watch(args).await,
        Command::SyncOnce(args) => sync_once(args).await,
        Command::Sync(args) => sync_forever(args).await,
        Command::Pair(args) => pair(args).await,
        Command::Daemon(args) => daemon(args).await,
        Command::Connect { ticket } => quick_connect(&ticket).await,
        Command::SyncFolder { folder, watch } => quick_sync_folder(folder, watch).await,
        Command::Share { folder, bind } => quick_share(folder, bind).await,
        Command::SelfTest => self_test().await,
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct QuickProfile {
    peer_endpoint_id: String,
    peer_address: String,
}

fn quick_data_dir() -> Result<PathBuf> {
    let dir = deltaweave_daemon::default_data_dir()?.join("quick");
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

fn quick_profile_path() -> Result<PathBuf> {
    Ok(quick_data_dir()?.join(QUICK_PROFILE_NAME))
}

fn save_quick_profile(profile: &QuickProfile) -> Result<()> {
    save_quick_profile_at(&quick_profile_path()?, profile)
}

fn save_quick_profile_at(path: &Path, profile: &QuickProfile) -> Result<()> {
    let nonce =
        Hash32::digest(SecretKey::generate().to_bytes().as_slice()).to_hex()[..12].to_string();
    let temp = path.with_file_name(format!(".profile.{nonce}.tmp"));
    let mut options = fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let write_result = (|| -> Result<()> {
        let mut file = options
            .open(&temp)
            .with_context(|| format!("failed to create {}", temp.display()))?;
        serde_json::to_writer(&mut file, profile)?;
        file.sync_all()?;
        Ok(())
    })();
    if let Err(error) = write_result {
        let _ = fs::remove_file(&temp);
        return Err(error);
    }
    if let Some(parent) = path.parent()
        && let Err(error) = sync_dir(parent)
    {
        let _ = fs::remove_file(&temp);
        return Err(error);
    }
    replace_quick_profile(path, &temp, &nonce)
}

#[cfg(unix)]
fn replace_quick_profile(path: &Path, temp: &Path, _nonce: &str) -> Result<()> {
    if let Err(error) = fs::rename(temp, path) {
        let _ = fs::remove_file(temp);
        return Err(error).with_context(|| format!("failed to replace {}", path.display()));
    }
    if let Some(parent) = path.parent() {
        let _ = sync_dir(parent);
    }
    Ok(())
}

#[cfg(not(unix))]
fn replace_quick_profile(path: &Path, temp: &Path, nonce: &str) -> Result<()> {
    let backup = path.with_file_name(format!(".profile.{nonce}.bak"));
    if backup.exists() {
        fs::remove_file(&backup)?;
    }
    if path.exists()
        && let Err(error) = fs::rename(path, &backup)
    {
        let _ = fs::remove_file(temp);
        return Err(error).with_context(|| format!("failed to back up {}", path.display()));
    }
    if let Err(error) = fs::rename(temp, path) {
        if backup.exists() {
            let _ = fs::rename(&backup, path);
        }
        let _ = fs::remove_file(temp);
        return Err(error).with_context(|| format!("failed to replace {}", path.display()));
    }
    if backup.exists() {
        fs::remove_file(&backup)?;
    }
    Ok(())
}

#[cfg(unix)]
fn sync_dir(path: &Path) -> Result<()> {
    let file = fs::File::open(path)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_dir(_path: &Path) -> Result<()> {
    Ok(())
}

fn quick_folder_id(folder: &Path) -> Result<String> {
    let canonical = fs::canonicalize(folder)?;
    Ok(Hash32::digest(canonical.to_string_lossy().as_bytes()).to_hex()[..16].to_string())
}

fn quick_folder_state(folder: &Path) -> Result<PathBuf> {
    Ok(quick_data_dir()?
        .join("folders")
        .join(quick_folder_id(folder)?))
}

fn quick_lan_ip() -> Result<IpAddr> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.connect("1.1.1.1:53")?;
    Ok(socket.local_addr()?.ip())
}

fn load_quick_profile() -> Result<QuickProfile> {
    let path = quick_profile_path()?;
    let file = fs::File::open(&path).with_context(|| {
        format!(
            "run 'deltaweave connect <dwpair1:...>' first ({})",
            path.display()
        )
    })?;
    Ok(serde_json::from_reader(file)?)
}

fn print_connect_card(endpoint_id: &str, peer_address: &str) -> Result<()> {
    let mut out = std::io::stdout().lock();
    writeln!(out, "\n┌ DeltaWeave 페어링 완료")?;
    writeln!(out, "├ 이 PC ID     {endpoint_id}")?;
    writeln!(out, "├ 서버 주소    {peer_address}")?;
    writeln!(out, "└ 다음: deltaweave sync-folder <폴더>")?;
    Ok(())
}

fn print_sync_card(report: &deltaweave_sync::SyncReport) -> Result<()> {
    let mut out = std::io::stdout().lock();
    writeln!(out, "\n┌ DeltaWeave 동기화 {}", report.status)?;
    writeln!(out, "├ 보낸 바이트      {}", report.pushed_bytes)?;
    writeln!(out, "├ 받은 바이트      {}", report.pulled_bytes)?;
    writeln!(out, "├ 로컬 변경        {}", report.local_actions)?;
    writeln!(out, "├ 원격 변경        {}", report.remote_actions)?;
    writeln!(out, "├ 충돌             {}", report.conflicts.len())?;
    writeln!(out, "└ 검증 루트        {}", report.verified_local_root)?;
    Ok(())
}

async fn quick_connect(ticket_code: &str) -> Result<()> {
    let dir = quick_data_dir()?;
    let identity_path = dir.join("identity.key");
    let identity = load_or_create_identity(&identity_path)?;
    let ticket = PairingTicket::from_code(ticket_code)?;
    let endpoint_id = ticket.server_endpoint_id.clone();
    let address = ticket.server_direct_address.clone();
    redeem_pairing_ticket(identity.secret_key.clone(), ticket, NetworkMode::DirectOnly).await?;
    save_quick_profile(&QuickProfile {
        peer_endpoint_id: endpoint_id,
        peer_address: address.clone(),
    })?;
    print_connect_card(&identity.endpoint_id().to_string(), &address)
}

fn quick_sync_engine_args(folder: &Path) -> Result<SyncTargetArgs> {
    let profile = load_quick_profile()?;
    let dir = quick_data_dir()?;
    let state_root = quick_folder_state(folder)?;
    fs::create_dir_all(&state_root)?;
    Ok(SyncTargetArgs {
        root: folder.to_path_buf(),
        state: state_root,
        access: dir.join("access.redb"),
        identity: dir.join("identity.key"),
        peer: profile.peer_endpoint_id,
        direct_addresses: vec![profile.peer_address.parse()?],
        relay_urls: Vec::new(),
        direct_only: true,
        chunking: ChunkingArgs {
            min_chunk: ChunkingProfile::DEFAULT.min_size,
            avg_chunk: ChunkingProfile::DEFAULT.avg_size,
            max_chunk: ChunkingProfile::DEFAULT.max_size,
        },
    })
}

async fn quick_sync_folder(folder: PathBuf, watch: bool) -> Result<()> {
    let args = quick_sync_engine_args(&folder)?;
    if !watch {
        let engine = open_sync_engine(args)?;
        let report = engine.sync_once().await?;
        return print_sync_card(&report);
    }
    let engine = open_sync_engine(args)?;
    let mut out = std::io::stdout().lock();
    writeln!(out, "\n┌ DeltaWeave 자동 동기화")?;
    writeln!(out, "├ 폴더      {}", folder.display())?;
    writeln!(out, "├ 간격      5초")?;
    writeln!(out, "└ 실행 중… Ctrl-C로 중지")?;
    drop(out);
    loop {
        tokio::select! {
            result = wait_for_shutdown_signal() => {
                result?;
                let mut out = std::io::stdout().lock();
                writeln!(out, "\n└ 자동 동기화 종료")?;
                return Ok(());
            }
            result = engine.sync_once() => {
                match result {
                    Ok(report) => print_sync_card(&report)?,
                    Err(error) => {
                        let mut out = std::io::stdout().lock();
                        writeln!(out, "\n┌ 동기화 재시도")?;
                        writeln!(out, "├ 오류      {error}")?;
                        writeln!(out, "└ 5초 뒤 재시도")?;
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn quick_share(folder: PathBuf, bind: SocketAddr) -> Result<()> {
    let dir = quick_data_dir()?;
    let share_dir = dir.join("share");
    let state_dir = share_dir.join("folders").join(quick_folder_id(&folder)?);
    let access_path = share_dir.join("access.redb");
    let identity_path = share_dir.join("identity.key");
    fs::create_dir_all(&state_dir)?;
    let identity = load_or_create_identity(&identity_path)?;
    ensure_identity_outside_destination(&identity_path, &folder)?;
    let advertised_ip = if bind.ip().is_unspecified() {
        quick_lan_ip()?
    } else {
        bind.ip()
    };
    let advertised = SocketAddr::new(advertised_ip, bind.port());
    let access = Arc::new(AccessStore::open(&access_path)?);
    let server = start_server(ServerConfig {
        secret_key: identity.secret_key.clone(),
        destination_root: folder.clone(),
        state_root: state_dir,
        peer_policy: PeerPolicy::Durable(Arc::clone(&access)),
        network_mode: NetworkMode::DirectOnly,
        quota_policy: Some(QUICK_SHARE_QUOTA),
        bind: Some(bind),
    })
    .await?;
    if !server.wait_online(Duration::from_secs(20)).await {
        server.shutdown().await?;
        bail!("공유 서버를 20초 안에 열지 못했습니다");
    }
    let expires_at = unix_now()
        .checked_add(600)
        .context("pairing ticket expiry overflow")?;
    let ticket =
        access.issue_ticket(&identity.endpoint_id(), &advertised.to_string(), expires_at)?;
    let mut out = std::io::stdout().lock();
    writeln!(out, "\n┌ DeltaWeave 공유 시작")?;
    writeln!(out, "├ 폴더      {}", folder.display())?;
    writeln!(out, "├ 내 ID     {}", identity.endpoint_id())?;
    writeln!(out, "├ 주소      {advertised}")?;
    writeln!(out, "├ 티켓      {}", ticket.to_code())?;
    writeln!(out, "├ 만료      10분 · 1회용")?;
    writeln!(out, "└ 대기 중… Ctrl-C로 중지")?;
    out.flush()?;
    wait_for_shutdown_signal().await?;
    server.shutdown().await
}

fn initialize(args: InitArgs) -> Result<()> {
    let identity = load_or_create_identity(&args.identity)?;
    print_json(&json!({
        "created": identity.created,
        "endpoint_id": identity.endpoint_id().to_string(),
        "identity_file": display_path(&args.identity),
    }))
}

fn print_manifest(args: ManifestArgs) -> Result<()> {
    let manifest = manifest_from_path(&args.file, args.chunking.profile()?)?;
    print_json(&manifest)
}

async fn serve(args: ServeArgs) -> Result<()> {
    if args.allowed_peers.is_empty() && !args.allow_any_authenticated && !args.durable_access {
        bail!("serve requires --allow-peer, --allow-any-authenticated, or --durable-access");
    }
    let identity = load_or_create_identity(&args.identity)?;
    ensure_identity_outside_destination(&args.identity, &args.root)?;
    let peer_policy = if args.durable_access {
        let access = AccessStore::open(&args.access)?;
        PeerPolicy::Durable(Arc::new(access))
    } else if args.allow_any_authenticated {
        PeerPolicy::AnyAuthenticated
    } else {
        let peers = args
            .allowed_peers
            .iter()
            .map(|peer| {
                peer.parse::<EndpointId>()
                    .with_context(|| format!("invalid allowed endpoint ID {peer}"))
            })
            .collect::<Result<HashSet<_>>>()?;
        PeerPolicy::AllowListed(peers)
    };
    let quota_policy = QuotaPolicy {
        bytes_per_second: args.rate_bytes_per_second,
        burst_bytes: args.burst_bytes,
        max_concurrent_operations_per_peer: args.max_concurrent_operations,
        max_storage_bytes: args.max_storage_bytes,
    };

    let server = start_server(ServerConfig {
        secret_key: identity.secret_key,
        destination_root: args.root,
        state_root: args.state,
        peer_policy,
        network_mode: network_mode(args.direct_only),
        quota_policy: if quota_policy == QuotaPolicy::UNLIMITED {
            None
        } else {
            Some(quota_policy)
        },
        bind: args.bind,
    })
    .await?;
    if !server.wait_online(Duration::from_secs(20)).await {
        server.shutdown().await?;
        bail!("endpoint did not become reachable within 20 seconds");
    }

    let address = server.address_info();
    print_json(&json!({
        "status": "ready",
        "endpoint_id": address.endpoint_id,
        "direct_addresses": address.direct_addresses,
        "relay_urls": address.relay_urls,
    }))?;
    wait_for_shutdown_signal().await?;
    server.shutdown().await
}

fn ensure_identity_outside_destination(identity: &Path, destination_root: &Path) -> Result<()> {
    fs::create_dir_all(destination_root).with_context(|| {
        format!(
            "failed to create destination root {}",
            destination_root.display()
        )
    })?;
    let identity = fs::canonicalize(identity)
        .with_context(|| format!("failed to resolve identity file {}", identity.display()))?;
    let destination_root = fs::canonicalize(destination_root).with_context(|| {
        format!(
            "failed to resolve destination root {}",
            destination_root.display()
        )
    })?;
    ensure!(
        !identity.starts_with(&destination_root),
        "identity file {} must be outside destination root {}",
        identity.display(),
        destination_root.display()
    );
    Ok(())
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to register SIGTERM handler")?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result.context("failed to wait for Ctrl-C")?;
        }
        _ = terminate.recv() => {}
    }
    Ok(())
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> Result<()> {
    tokio::signal::ctrl_c()
        .await
        .context("failed to wait for Ctrl-C")
}

async fn daemon(args: DaemonArgs) -> Result<()> {
    match args.command {
        DaemonCommand::Run => deltaweave_daemon::run().await,
        DaemonCommand::Status => daemon_status().await,
    }
}

async fn daemon_status() -> Result<()> {
    let data_dir = deltaweave_daemon::default_data_dir()?;
    let socket = deltaweave_daemon::ipc_path(&data_dir);
    let hello = deltaweave_daemon::connect_and_hello(&socket)
        .await
        .with_context(|| format!("failed to attach to {}", socket.display()))?;
    print_json(&json!({
        "instance_id": hello.instance_id,
        "local_endpoint_id": hello.local_endpoint_id,
        "protocol_version": {
            "major": hello.protocol_version.major,
            "minor": hello.protocol_version.minor,
        },
    }))
}

async fn pair(args: PairArgs) -> Result<()> {
    match args.command {
        PairCommand::Issue {
            ttl_seconds,
            direct_address,
            access,
            identity,
        } => {
            ensure!(ttl_seconds > 0, "--ttl-seconds must be greater than zero");
            let identity = load_or_create_identity(identity)?;
            let expires_at = unix_now()
                .checked_add(ttl_seconds)
                .context("pairing ticket expiry overflow")?;
            let access = AccessStore::open(access)?;
            let ticket = access.issue_ticket(
                &identity.endpoint_id(),
                &direct_address.to_string(),
                expires_at,
            )?;
            print_json(&json!({
                "code": ticket.to_code(),
                "expires_at": ticket.expires_at,
                "server_endpoint_id": ticket.server_endpoint_id,
            }))
        }
        PairCommand::Redeem { code, identity } => {
            let ticket = PairingTicket::from_code(&code)?;
            let identity = load_or_create_identity(identity)?;
            let outcome =
                redeem_pairing_ticket(identity.secret_key, ticket, NetworkMode::DirectOnly).await?;
            print_json(&json!({"outcome": format!("{outcome:?}")}))
        }
        PairCommand::List { access } => {
            let peers = AccessStore::open(access)?.list_peers()?;
            print_json(&peers)
        }
        PairCommand::Revoke {
            access,
            endpoint_id,
        } => {
            let endpoint_id = endpoint_id
                .parse::<EndpointId>()
                .context("invalid endpoint ID")?;
            let revoked = AccessStore::open(access)?.revoke(endpoint_id)?;
            print_json(&json!({"endpoint_id": endpoint_id.to_string(), "revoked": revoked}))
        }
        PairCommand::Rotate { identity } => {
            let rotation = AccessStore::rotate_identity(&identity)?;
            print_json(&json!({
                "new_endpoint_id": rotation.new_endpoint_id.to_string(),
                "previous_endpoint_id": rotation.previous_endpoint_id.to_string(),
            }))
        }
    }
}

async fn push(args: PushArgs) -> Result<()> {
    if args.direct_only && args.direct_addresses.is_empty() {
        bail!("--direct-only requires at least one --direct address");
    }
    let identity = load_or_create_identity(&args.identity)?;
    let remote = endpoint_addr(&args.peer, &args.direct_addresses, &args.relay_urls)?;
    let receipt = push_file(PushOptions {
        secret_key: identity.secret_key,
        source: args.source,
        remote_path: args.remote_path,
        remote,
        profile: args.chunking.profile()?,
        network_mode: network_mode(args.direct_only),
    })
    .await?;
    print_json(&receipt)
}

fn open_sync_engine(args: SyncTargetArgs) -> Result<SyncEngine> {
    if args.direct_only && args.direct_addresses.is_empty() {
        bail!("--direct-only requires at least one --direct address");
    }
    let identity = load_or_create_identity(&args.identity)?;
    ensure_identity_outside_destination(&args.identity, &args.root)?;
    let profile = args.chunking.profile()?;
    let remote = endpoint_addr(&args.peer, &args.direct_addresses, &args.relay_urls)?;
    let access = AccessStore::open(&args.access)?;
    let replica = access.stable_replica_id(&identity.secret_key)?;
    SyncEngine::open(SyncConfig {
        root: args.root,
        state_root: args.state,
        replica,
        client: SyncClient {
            secret_key: identity.secret_key,
            remote,
            network_mode: network_mode(args.direct_only),
        },
        profile,
        ignored_paths: Vec::new(),
    })
}

async fn sync_once(args: SyncTargetArgs) -> Result<()> {
    let engine = open_sync_engine(args)?;
    print_json(&engine.sync_once().await?)
}

async fn sync_forever(args: SyncArgs) -> Result<()> {
    ensure!(
        args.interval_seconds > 0,
        "--interval-seconds must be greater than zero"
    );
    ensure!(
        args.max_backoff_seconds > 0,
        "--max-backoff-seconds must be greater than zero"
    );
    ensure!(
        args.debounce_ms > 0,
        "--debounce-ms must be greater than zero"
    );
    ensure!(
        args.max_debounce_ms >= args.debounce_ms,
        "--max-debounce-ms must be at least --debounce-ms"
    );
    let interval = Duration::from_secs(args.interval_seconds);
    let maximum_backoff = Duration::from_secs(args.max_backoff_seconds);
    let root = args.target.root.clone();
    let debounce = Duration::from_millis(args.debounce_ms);
    let maximum_debounce = Duration::from_millis(args.max_debounce_ms);
    let engine = open_sync_engine(args.target)?;
    let watcher_result = WatchService::new(&root, &[], debounce, maximum_debounce);
    let (mut watcher, watcher_error) = match watcher_result {
        Ok(watcher) => (Some(watcher), None),
        Err(error) => (None, Some(error.to_string())),
    };
    print_json(&json!({
        "event": "sync_started",
        "local_change_detection": if watcher.is_some() { "native_watcher" } else { "polling_fallback" },
        "remote_poll_seconds": interval.as_secs(),
        "watcher_error": watcher_error,
    }))?;
    let shutdown = wait_for_shutdown_signal();
    tokio::pin!(shutdown);
    let mut backoff = Duration::from_secs(1);
    loop {
        let (delay, watch_for_local_changes) = tokio::select! {
            result = &mut shutdown => {
                result?;
                print_json(&json!({"event": "shutdown", "status": "stopped"}))?;
                return Ok(());
            }
            result = engine.sync_once() => {
                match result {
                    Ok(report) => {
                        print_json(&json!({"event": "sync", "report": report}))?;
                        backoff = Duration::from_secs(1);
                        (interval, true)
                    }
                    Err(error) => {
                        print_json(&json!({
                            "event": "sync_error",
                            "error": error.to_string(),
                            "retry_in_seconds": backoff.as_secs(),
                            "status": "retrying",
                        }))?;
                        let delay = backoff;
                        backoff = backoff.saturating_mul(2).min(maximum_backoff);
                        (delay, false)
                    }
                }
            }
        };
        let waiting_since = Instant::now();
        loop {
            let remaining = delay.saturating_sub(waiting_since.elapsed());
            if remaining.is_zero() {
                break;
            }
            tokio::select! {
                result = &mut shutdown => {
                    result?;
                    print_json(&json!({"event": "shutdown", "status": "stopped"}))?;
                    return Ok(());
                }
                _ = tokio::time::sleep(remaining.min(Duration::from_millis(100))) => {}
            }
            if watch_for_local_changes
                && let Some(trigger) = watcher
                    .as_mut()
                    .and_then(|watcher| watcher.poll(Instant::now()))
            {
                print_json(&json!({
                    "event": "local_change",
                    "native_events": trigger.event_count,
                    "rescan_required": trigger.rescan_required,
                    "status": "synchronizing",
                }))?;
                break;
            }
        }
    }
}

fn scan(args: ScanArgs) -> Result<()> {
    let index = open_index(args.index)?;
    let report = index.scan()?;
    if args.include_records {
        print_json(&json!({
            "records": index.records()?,
            "report": report,
            "retries": index.retries()?,
        }))
    } else {
        print_json(&report)
    }
}

async fn watch(args: WatchArgs) -> Result<()> {
    ensure!(
        args.debounce_ms > 0,
        "--debounce-ms must be greater than zero"
    );
    ensure!(
        args.max_debounce_ms >= args.debounce_ms,
        "--max-debounce-ms must be at least --debounce-ms"
    );
    ensure!(
        args.rescan_seconds > 0,
        "--rescan-seconds must be greater than zero"
    );
    ensure!(
        args.poll_fallback_seconds > 0,
        "--poll-fallback-seconds must be greater than zero"
    );

    let index = open_index(args.index)?;
    let debounce = Duration::from_millis(args.debounce_ms);
    let maximum_delay = Duration::from_millis(args.max_debounce_ms);
    let rescan_interval = Duration::from_secs(args.rescan_seconds);
    let fallback_interval = Duration::from_secs(args.poll_fallback_seconds);
    let (mut watcher, watcher_error) =
        match WatchService::new(index.root(), index.ignored_paths(), debounce, maximum_delay) {
            Ok(watcher) => (Some(watcher), None),
            Err(error) => (None, Some(error.to_string())),
        };
    print_json(&json!({
        "event": "initial_scan",
        "report": index.scan()?,
        "root": display_path(index.root()),
        "status": if watcher.is_some() { "watching" } else { "polling_fallback" },
        "watcher_error": watcher_error,
    }))?;

    let started = Instant::now();
    let mut next_periodic = started + rescan_interval;
    let mut next_fallback = started + fallback_interval;
    let mut watcher_degraded = watcher.is_none();
    let shutdown = wait_for_shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            result = &mut shutdown => {
                result?;
                print_json(&json!({"event": "shutdown", "status": "stopped"}))?;
                return Ok(());
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                let now = Instant::now();
                let trigger = watcher.as_mut().and_then(|watcher| watcher.poll(now));
                if trigger.as_ref().is_some_and(|value| value.rescan_required) {
                    watcher_degraded = true;
                }
                let periodic = now >= next_periodic;
                let fallback = watcher_degraded && now >= next_fallback;
                if trigger.is_none() && !periodic && !fallback {
                    continue;
                }
                let authoritative = periodic
                    || fallback
                    || trigger.as_ref().is_some_and(|value| value.rescan_required);
                let report = if authoritative {
                    index.scan()?
                } else {
                    index.scan_incremental(
                        trigger
                            .as_ref()
                            .map_or(&[][..], |value| value.changed_paths.as_slice()),
                    )?
                };
                print_json(&json!({
                    "event": if periodic {
                        "periodic_scan"
                    } else if fallback {
                        "fallback_scan"
                    } else {
                        "watch_scan"
                    },
                    "native_events": trigger.as_ref().map_or(0, |value| value.event_count),
                    "report": report,
                    "rescan_required": authoritative,
                    "watcher_degraded": watcher_degraded,
                }))?;
                if periodic {
                    next_periodic = now + rescan_interval;
                }
                if fallback {
                    next_fallback = now + fallback_interval;
                }
            }
        }
    }
}

fn open_index(mut args: IndexArgs) -> Result<LocalIndex> {
    let identity = load_or_create_identity(&args.identity)?;
    let replica = AccessStore::open(&args.access)?.stable_replica_id(&identity.secret_key)?;
    args.ignored_paths.push(args.identity.clone());
    let mut options = IndexOptions {
        ignored_paths: args.ignored_paths,
        ..IndexOptions::default()
    };
    if let Some(workers) = args.hash_workers {
        ensure!(workers > 0, "--hash-workers must be greater than zero");
        options.hash_workers = workers;
    }
    LocalIndex::open(args.root, args.state, replica, options)
}

async fn self_test() -> Result<()> {
    let workspace = tempfile::tempdir().context("failed to create self-test workspace")?;
    let destination = workspace.path().join("received");
    let state = workspace.path().join("state");
    let source = workspace.path().join("source.bin");
    fs::create_dir_all(&destination)?;

    let client_key = SecretKey::generate();
    let server = start_server(ServerConfig {
        secret_key: SecretKey::generate(),
        destination_root: destination.clone(),
        state_root: state,
        peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
        network_mode: NetworkMode::DirectOnly,
        quota_policy: None,
        bind: None,
    })
    .await
    .context("self-test receiver failed to start")?;

    let outcome = async {
        let transfer =
            exercise_self_test(&server, client_key.clone(), &source, &destination).await?;
        let synchronization =
            exercise_sync_self_test(workspace.path(), &server, client_key, &destination).await?;
        Ok::<_, anyhow::Error>((transfer, synchronization))
    }
    .await;
    let shutdown = server.shutdown().await;
    let ((first, second, final_size), synchronization) = match (outcome, shutdown) {
        (Err(error), _) => return Err(error),
        (Ok(_), Err(error)) => return Err(error.context("self-test receiver shutdown failed")),
        (Ok(report), Ok(())) => report,
    };
    let index = exercise_index_self_test(workspace.path())?;

    print_json(&json!({
        "architecture": std::env::consts::ARCH,
        "final_size": final_size,
        "first_transfer_bytes": first.transferred_bytes,
        "index_initial_records": index.initial_records,
        "index_rename_detected": index.rename_detected,
        "index_restart_verified": index.restart_verified,
        "index_tombstones": index.tombstones,
        "operating_system": std::env::consts::OS,
        "reused_extents": second.reused_extents,
        "second_transfer_bytes": second.transferred_bytes,
        "status": "pass",
        "sync_bidirectional_verified": synchronization.bidirectional_verified,
        "sync_conflicts_preserved": synchronization.conflicts_preserved,
        "sync_delete_verified": synchronization.delete_verified,
        "sync_restart_actions": synchronization.restart_actions,
        "sync_verified_root": synchronization.verified_root,
        "temporary_data": "cleaned on exit",
    }))
}

struct IndexSelfTestReport {
    initial_records: usize,
    rename_detected: bool,
    restart_verified: bool,
    tombstones: usize,
}

struct SyncSelfTestReport {
    bidirectional_verified: bool,
    conflicts_preserved: usize,
    delete_verified: bool,
    restart_actions: usize,
    verified_root: Hash32,
}

async fn exercise_sync_self_test(
    workspace: &Path,
    server: &Server,
    client_key: SecretKey,
    remote_root: &Path,
) -> Result<SyncSelfTestReport> {
    let local_root = workspace.join("sync-local");
    let local_state = workspace.join("sync-local-state");
    fs::create_dir_all(local_root.join("bidirectional"))?;
    fs::create_dir_all(remote_root.join("bidirectional"))?;
    fs::write(
        local_root.join("bidirectional/local.txt"),
        b"from local self-test",
    )?;
    fs::write(
        remote_root.join("bidirectional/remote.txt"),
        b"from remote self-test",
    )?;
    fs::write(local_root.join("bidirectional/shared.txt"), b"common")?;
    fs::write(remote_root.join("bidirectional/shared.txt"), b"common")?;

    let config = SyncConfig {
        root: local_root.clone(),
        state_root: local_state,
        replica: ReplicaId(Hash32::digest(client_key.public().as_bytes())),
        client: SyncClient {
            secret_key: client_key,
            remote: server.endpoint_addr(),
            network_mode: NetworkMode::DirectOnly,
        },
        profile: ChunkingProfile::DEFAULT,
        ignored_paths: Vec::new(),
    };
    let engine = SyncEngine::open(config.clone())?;
    let initial = engine
        .sync_once()
        .await
        .context("self-test bidirectional initial merge failed")?;
    let bidirectional_verified = local_root.join("bidirectional/remote.txt").is_file()
        && remote_root.join("bidirectional/local.txt").is_file()
        && initial.verified_local_root == initial.verified_remote_root;
    ensure!(
        bidirectional_verified,
        "self-test did not exchange files in both directions"
    );

    fs::write(
        local_root.join("bidirectional/shared.txt"),
        b"local concurrent edit",
    )?;
    fs::write(
        remote_root.join("bidirectional/shared.txt"),
        b"remote concurrent edit",
    )?;
    let conflict = engine
        .sync_once()
        .await
        .context("self-test conflict reconciliation failed")?;
    ensure!(
        conflict.conflicts.len() == 1
            && conflict.conflicts[0]
                .conflict_path
                .as_ref()
                .is_some_and(|path| {
                    local_root.join(path.as_str()).is_file()
                        && remote_root.join(path.as_str()).is_file()
                }),
        "self-test did not preserve both concurrent edits"
    );

    fs::remove_file(local_root.join("bidirectional/local.txt"))?;
    let deletion = engine
        .sync_once()
        .await
        .context("self-test deletion reconciliation failed")?;
    let delete_verified = !remote_root.join("bidirectional/local.txt").exists()
        && deletion.verified_local_root == deletion.verified_remote_root;
    ensure!(delete_verified, "self-test deletion did not converge");
    let verified_root = deletion.verified_local_root;

    drop(engine);
    let restarted = SyncEngine::open(config)?;
    let restart = restarted
        .sync_once()
        .await
        .context("self-test restart reconciliation failed")?;
    let restart_actions = restart.local_actions.saturating_add(restart.remote_actions);
    ensure!(
        restart_actions == 0 && restart.verified_local_root == verified_root,
        "self-test durable restart was not an idempotent no-op"
    );
    Ok(SyncSelfTestReport {
        bidirectional_verified,
        conflicts_preserved: conflict.conflicts.len(),
        delete_verified,
        restart_actions,
        verified_root,
    })
}

fn exercise_index_self_test(workspace: &Path) -> Result<IndexSelfTestReport> {
    let root = workspace.join("index-root");
    let state = workspace.join("index-state/index.redb");
    fs::create_dir_all(&root)?;
    let original = root.join("before.bin");
    let renamed = root.join("after.bin");
    fs::write(&original, b"local index self-test")?;
    let replica = ReplicaId(Hash32::digest(b"deltaweave packaged self-test replica"));
    let index = LocalIndex::open(&root, &state, replica, IndexOptions::default())?;
    let initial = index
        .scan()
        .context("self-test initial index scan failed")?;
    ensure!(
        initial.live_records == 1,
        "self-test indexed an unexpected path count"
    );
    ensure!(
        initial.files_hashed == 1,
        "self-test did not hash its fixture"
    );

    fs::rename(&original, &renamed)?;
    let rename = index.scan().context("self-test rename scan failed")?;
    let rename_detected = rename.changes.iter().any(|change| {
        matches!(
            change,
            ScanChange::Renamed { from, to }
                if from.as_str() == "before.bin" && to.as_str() == "after.bin"
        )
    });
    ensure!(
        rename_detected,
        "self-test failed to correlate a stable-identity rename"
    );

    fs::remove_file(&renamed)?;
    let deleted = index.scan().context("self-test deletion scan failed")?;
    ensure!(
        deleted.tombstones == 2,
        "self-test did not preserve rename/delete tombstones"
    );
    drop(index);

    let reopened = LocalIndex::open(&root, &state, replica, IndexOptions::default())?;
    let records = reopened
        .records()
        .context("self-test index restart failed")?;
    let restart_verified = records.len() == 2 && records.iter().all(|record| record.tombstone);
    ensure!(
        restart_verified,
        "self-test index state did not survive restart"
    );

    Ok(IndexSelfTestReport {
        initial_records: initial.live_records,
        rename_detected,
        restart_verified,
        tombstones: deleted.tombstones,
    })
}

async fn exercise_self_test(
    server: &Server,
    client_key: SecretKey,
    source: &Path,
    destination: &Path,
) -> Result<(TransferReceipt, TransferReceipt, usize)> {
    let original = self_test_fixture(4 * 1024 * 1024);
    fs::write(source, &original)?;
    let remote_path = WirePath::new("self-test/payload.bin")?;

    let first = push_file(PushOptions {
        secret_key: client_key.clone(),
        source: source.to_path_buf(),
        remote_path: remote_path.clone(),
        remote: server.endpoint_addr(),
        profile: ChunkingProfile::DEFAULT,
        network_mode: NetworkMode::DirectOnly,
    })
    .await
    .context("self-test initial transfer failed")?;
    ensure!(first.transferred_bytes > 0, "initial transfer sent no data");
    ensure!(
        fs::read(destination.join(remote_path.as_str()))? == original,
        "initial reconstructed file differs from its source"
    );

    let mut modified = original;
    modified.splice(
        700_000..700_000,
        b"DeltaWeave self-test insertion\n".iter().copied(),
    );
    fs::write(source, &modified)?;
    let second = push_file(PushOptions {
        secret_key: client_key,
        source: source.to_path_buf(),
        remote_path: remote_path.clone(),
        remote: server.endpoint_addr(),
        profile: ChunkingProfile::DEFAULT,
        network_mode: NetworkMode::DirectOnly,
    })
    .await
    .context("self-test delta transfer failed")?;
    ensure!(
        second.reused_extents > 0,
        "delta transfer reused no extents"
    );
    ensure!(
        second.transferred_bytes < modified.len() as u64,
        "delta transfer sent the complete modified file"
    );
    ensure!(
        fs::read(destination.join(remote_path.as_str()))? == modified,
        "delta reconstructed file differs from its source"
    );

    Ok((first, second, modified.len()))
}

fn self_test_fixture(length: usize) -> Vec<u8> {
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

const fn network_mode(direct_only: bool) -> NetworkMode {
    if direct_only {
        NetworkMode::DirectOnly
    } else {
        NetworkMode::Internet
    }
}

fn init_tracing() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn,netwatch=error"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
}

fn print_json(value: &impl serde::Serialize) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer_pretty(&mut stdout, value)?;
    writeln!(stdout)?;
    Ok(())
}

fn display_path(path: &Path) -> String {
    path.display().to_string()
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn quick_share_quota_bounds_disk_and_bandwidth() {
        let quota = QUICK_SHARE_QUOTA;
        assert!(
            quota.max_storage_bytes > 0,
            "quick share must cap stored bytes instead of unlimited"
        );
        assert!(
            quota.bytes_per_second > 0,
            "quick share must cap receive rate instead of unlimited"
        );
        assert!(
            quota.max_concurrent_operations_per_peer > 0,
            "quick share must cap per-peer concurrency instead of unlimited"
        );
        assert_ne!(quota, QuotaPolicy::UNLIMITED);
        assert!(
            quota.max_storage_bytes <= 16 * 1024 * 1024 * 1024,
            "quick share storage cap must stay small enough to protect a laptop disk"
        );
        assert!(
            quota.bytes_per_second <= 16 * 1024 * 1024,
            "quick share rate must stay below LAN saturation"
        );
    }

    #[test]
    fn parses_quick_user_commands() {
        let connect = Cli::try_parse_from(["deltaweave", "connect", "dwpair1:abc"])
            .expect("quick connect parses");
        assert!(matches!(connect.command, Command::Connect { ticket } if ticket == "dwpair1:abc"));

        let sync = Cli::try_parse_from(["deltaweave", "sync-folder", r"C:\Sync"])
            .expect("quick sync parses");
        assert!(
            matches!(sync.command, Command::SyncFolder { folder, watch: false } if folder == Path::new(r"C:\Sync"))
        );

        let share =
            Cli::try_parse_from(["deltaweave", "share", r"C:\Shared"]).expect("quick share parses");
        assert!(
            matches!(share.command, Command::Share { folder, bind } if folder == Path::new(r"C:\Shared") && bind == "0.0.0.0:17891".parse().unwrap())
        );
    }

    #[test]
    fn quick_profile_round_trip_never_stores_ticket_secret() {
        let dir = tempfile::tempdir().unwrap();
        let profile_path = dir.path().join("profile.json");
        let profile = QuickProfile {
            peer_endpoint_id: "ab".repeat(32),
            peer_address: "172.30.1.22:17892".into(),
        };
        let encoded = serde_json::to_string(&profile).unwrap();
        assert!(!encoded.contains("dwpair1:"));
        std::fs::write(&profile_path, &encoded).unwrap();
        let decoded: QuickProfile =
            serde_json::from_reader(std::fs::File::open(&profile_path).unwrap()).unwrap();
        assert_eq!(decoded.peer_endpoint_id, profile.peer_endpoint_id);
        assert_eq!(decoded.peer_address, profile.peer_address);
    }

    #[test]
    fn quick_profile_replaces_existing_file_like_windows() {
        let dir = tempfile::tempdir().unwrap();
        let profile_path = dir.path().join("profile.json");
        let first = QuickProfile {
            peer_endpoint_id: "aa".repeat(32),
            peer_address: "172.30.1.22:17891".into(),
        };
        save_quick_profile_at(&profile_path, &first).unwrap();
        let second = QuickProfile {
            peer_endpoint_id: "bb".repeat(32),
            peer_address: "172.30.1.22:17892".into(),
        };
        save_quick_profile_at(&profile_path, &second).unwrap();
        let decoded: QuickProfile =
            serde_json::from_reader(std::fs::File::open(&profile_path).unwrap()).unwrap();
        assert_eq!(decoded.peer_endpoint_id, second.peer_endpoint_id);
        assert_eq!(decoded.peer_address, second.peer_address);
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.file_name())
            .filter(|name| name != "profile.json")
            .collect();
        assert!(
            leftovers.is_empty(),
            "temporary profile files must be gone, leftover={leftovers:?}"
        );
    }

    #[test]
    fn quick_folder_state_separates_roots() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("folder-a");
        let second = dir.path().join("folder-b");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();
        let first_id = super::quick_folder_id(&first).unwrap();
        let second_id = super::quick_folder_id(&second).unwrap();
        assert_ne!(first_id, second_id);
        assert_eq!(first_id, super::quick_folder_id(&first).unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn quick_share_issues_a_ticket_the_client_can_redeem() {
        let base = tempfile::tempdir().unwrap();
        let shared = base.path().join("shared");
        let client_root = base.path().join("client");
        std::fs::create_dir_all(&shared).unwrap();
        std::fs::create_dir_all(&client_root).unwrap();
        std::fs::write(shared.join("from-server.txt"), "server").unwrap();

        let server_state = base.path().join("server-state");
        let client_state = base.path().join("client-state");
        std::fs::create_dir_all(&server_state).unwrap();
        std::fs::create_dir_all(&client_state).unwrap();

        let server_identity_path = base.path().join("server.key");
        let client_identity_path = base.path().join("client.key");
        let server_identity = load_or_create_identity(&server_identity_path).unwrap();
        let client_identity = load_or_create_identity(&client_identity_path).unwrap();

        let access = Arc::new(AccessStore::open(base.path().join("access.redb")).unwrap());
        let server = start_server(ServerConfig {
            secret_key: server_identity.secret_key.clone(),
            destination_root: shared.clone(),
            state_root: server_state,
            peer_policy: PeerPolicy::Durable(Arc::clone(&access)),
            network_mode: NetworkMode::DirectOnly,
            quota_policy: None,
            bind: Some("127.0.0.1:0".parse().unwrap()),
        })
        .await
        .unwrap();
        assert!(
            server.wait_online(Duration::from_secs(20)).await,
            "share server never advertised an address"
        );
        let address = server
            .endpoint_addr()
            .ip_addrs()
            .next()
            .unwrap()
            .to_string();

        let expires_at = unix_now().checked_add(600).unwrap();
        let ticket = access
            .issue_ticket(&server_identity.endpoint_id(), &address, expires_at)
            .unwrap();

        let outcome = redeem_pairing_ticket(
            client_identity.secret_key.clone(),
            ticket.clone(),
            NetworkMode::DirectOnly,
        )
        .await
        .unwrap();
        assert_eq!(outcome, deltaweave_net::access::RedeemOutcome::Paired);

        let client_access = base.path().join("client-access.redb");
        let engine = open_sync_engine(SyncTargetArgs {
            root: client_root.clone(),
            state: client_state,
            access: client_access,
            identity: client_identity_path,
            peer: ticket.server_endpoint_id,
            direct_addresses: vec![address.parse().unwrap()],
            relay_urls: Vec::new(),
            direct_only: true,
            chunking: ChunkingArgs {
                min_chunk: ChunkingProfile::DEFAULT.min_size,
                avg_chunk: ChunkingProfile::DEFAULT.avg_size,
                max_chunk: ChunkingProfile::DEFAULT.max_size,
            },
        })
        .unwrap();
        let report = engine.sync_once().await.unwrap();
        assert_eq!(report.status, "pass");
        assert_eq!(report.conflicts.len(), 0);
        let copied = client_root.join("from-server.txt");
        assert_eq!(std::fs::read(&copied).unwrap(), b"server");
        server.shutdown().await.unwrap();
    }

    #[test]
    fn parses_manifest_defaults() {
        let cli = Cli::try_parse_from(["deltaweave", "manifest", "input.bin"])
            .expect("default manifest command parses");
        let Command::Manifest(args) = cli.command else {
            panic!("manifest command expected");
        };
        assert_eq!(
            args.chunking.profile().expect("default profile is valid"),
            ChunkingProfile::DEFAULT
        );
    }

    #[test]
    fn remote_path_rejects_traversal_during_parse() {
        assert!(
            Cli::try_parse_from([
                "deltaweave",
                "push",
                "input.bin",
                "--remote-path",
                "../escape.bin",
                "--peer",
                "invalid"
            ])
            .is_err()
        );
    }

    #[test]
    fn allow_any_conflicts_with_allow_list() {
        assert!(
            Cli::try_parse_from([
                "deltaweave",
                "serve",
                "--root",
                "output",
                "--allow-peer",
                "peer",
                "--allow-any-authenticated"
            ])
            .is_err()
        );
    }

    #[test]
    fn parses_self_test_command() {
        let cli =
            Cli::try_parse_from(["deltaweave", "self-test"]).expect("self-test command parses");
        assert!(matches!(cli.command, Command::SelfTest));
    }

    #[test]
    fn parses_scan_and_watch_safety_defaults() {
        let scan = Cli::try_parse_from(["deltaweave", "scan", "--root", "data"])
            .expect("scan command parses");
        assert!(matches!(scan.command, Command::Scan(_)));

        let watch = Cli::try_parse_from(["deltaweave", "watch", "--root", "data"])
            .expect("watch command parses");
        let Command::Watch(args) = watch.command else {
            panic!("watch command expected");
        };
        assert_eq!(args.debounce_ms, 750);
        assert_eq!(args.rescan_seconds, 600);
        assert_eq!(args.poll_fallback_seconds, 5);

        let scan =
            Cli::try_parse_from(["deltaweave", "scan", "--root", "data", "--include-records"])
                .expect("detailed scan command parses");
        let Command::Scan(args) = scan.command else {
            panic!("scan command expected");
        };
        assert!(args.include_records);
    }

    #[test]
    fn parses_one_shot_and_continuous_sync_commands() {
        let once = Cli::try_parse_from([
            "deltaweave",
            "sync-once",
            "--root",
            "data",
            "--peer",
            "peer-id",
        ])
        .expect("sync-once command parses");
        assert!(matches!(once.command, Command::SyncOnce(_)));

        let continuous = Cli::try_parse_from([
            "deltaweave",
            "sync",
            "--root",
            "data",
            "--peer",
            "peer-id",
            "--interval-seconds",
            "2",
        ])
        .expect("continuous sync command parses");
        let Command::Sync(args) = continuous.command else {
            panic!("sync command expected");
        };
        assert_eq!(args.interval_seconds, 2);
        assert_eq!(args.debounce_ms, 750);
        assert_eq!(args.max_debounce_ms, 5_000);
        assert_eq!(args.max_backoff_seconds, 300);
    }

    #[test]
    fn daemon_status_parses() {
        let cli = Cli::try_parse_from(["deltaweave", "daemon", "status"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Daemon(DaemonArgs {
                command: DaemonCommand::Status
            })
        ));
    }

    #[test]
    fn parses_pairing_lifecycle_commands() {
        let issue = Cli::try_parse_from([
            "deltaweave",
            "pair",
            "issue",
            "--ttl-seconds",
            "90",
            "--direct-address",
            "127.0.0.1:4000",
            "--access",
            "access.redb",
            "--identity",
            "server.key",
        ])
        .expect("pair issue parses");
        let Command::Pair(PairArgs {
            command:
                PairCommand::Issue {
                    ttl_seconds,
                    direct_address,
                    access,
                    identity,
                },
        }) = issue.command
        else {
            panic!("pair issue expected");
        };
        assert_eq!(ttl_seconds, 90);
        assert_eq!(
            direct_address,
            "127.0.0.1:4000".parse().expect("valid address")
        );
        assert_eq!(access, PathBuf::from("access.redb"));
        assert_eq!(identity, PathBuf::from("server.key"));

        let redeem = Cli::try_parse_from([
            "deltaweave",
            "pair",
            "redeem",
            "dwpair1:00",
            "--identity",
            "client.key",
        ])
        .expect("pair redeem parses");
        let Command::Pair(PairArgs {
            command: PairCommand::Redeem { code, identity },
        }) = redeem.command
        else {
            panic!("pair redeem expected");
        };
        assert_eq!(code, "dwpair1:00");
        assert_eq!(identity, PathBuf::from("client.key"));

        let list = Cli::try_parse_from(["deltaweave", "pair", "list", "--access", "access.redb"])
            .expect("pair list parses");
        assert!(matches!(
            list.command,
            Command::Pair(PairArgs {
                command: PairCommand::List { access }
            }) if access == *"access.redb"
        ));

        let peer = SecretKey::generate().public().to_string();
        let revoke = Cli::try_parse_from([
            "deltaweave",
            "pair",
            "revoke",
            "--access",
            "access.redb",
            &peer,
        ])
        .expect("pair revoke parses");
        assert!(matches!(
            revoke.command,
            Command::Pair(PairArgs {
                command: PairCommand::Revoke { access, endpoint_id }
            }) if access == *"access.redb" && endpoint_id == peer
        ));

        let rotate =
            Cli::try_parse_from(["deltaweave", "pair", "rotate", "--identity", "node.key"])
                .expect("pair rotate parses");
        assert!(matches!(
            rotate.command,
            Command::Pair(PairArgs {
                command: PairCommand::Rotate { identity }
            }) if identity == *"node.key"
        ));
    }

    #[test]
    fn serve_parses_fixed_udp_bind() {
        let cli = Cli::try_parse_from([
            "deltaweave",
            "serve",
            "--root",
            "output",
            "--direct-only",
            "--bind",
            "127.0.0.1:4433",
        ])
        .expect("serve --bind parses");
        let Command::Serve(args) = cli.command else {
            panic!("serve command expected");
        };
        assert_eq!(
            args.bind,
            Some("127.0.0.1:4433".parse().expect("valid address"))
        );
    }

    #[test]
    fn serve_and_sync_accept_a_shared_access_database_path() {
        let serve = Cli::try_parse_from([
            "deltaweave",
            "serve",
            "--root",
            "output",
            "--durable-access",
            "--access",
            "access.redb",
        ])
        .expect("durable serve parses");
        assert!(matches!(
            serve.command,
            Command::Serve(ServeArgs { access, durable_access: true, bind: None, .. })
                if access == *"access.redb"
        ));

        let sync = Cli::try_parse_from([
            "deltaweave",
            "sync-once",
            "--root",
            "data",
            "--peer",
            "peer-id",
            "--access",
            "access.redb",
        ])
        .expect("sync-once parses");
        assert!(matches!(
            sync.command,
            Command::SyncOnce(SyncTargetArgs { access, .. })
                if access == *"access.redb"
        ));
    }

    #[test]
    fn identity_inside_destination_is_rejected() {
        let temp = tempfile::tempdir().expect("temporary directory can be created");
        let root = temp.path().join("received");
        fs::create_dir_all(&root).expect("destination can be created");
        let identity = root.join("receiver.key");
        load_or_create_identity(&identity).expect("identity can be created");

        assert!(ensure_identity_outside_destination(&identity, &root).is_err());
    }
}
