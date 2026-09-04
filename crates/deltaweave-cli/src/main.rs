//! DeltaWeave command-line interface.

#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, HashSet},
    fs,
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command as ProcessCommand, Stdio},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use clap::{Args, Parser, Subcommand};
use deltaweave_cdc::manifest_from_path;
use deltaweave_core::{ChunkingProfile, Hash32, ReplicaId, WirePath};
use deltaweave_index::{IndexOptions, LocalIndex, ScanChange, WatchService};
use deltaweave_net::{
    NetworkMode, PeerPolicy, PushOptions, Server, ServerConfig, SyncClient, TransferReceipt,
    endpoint_addr, load_or_create_identity, push_file, start_server,
};
use deltaweave_sync::{SyncConfig, SyncEngine};
use iroh::{EndpointId, SecretKey};
use serde::Serialize;
use serde_json::json;
use tracing_subscriber::EnvFilter;

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
    /// Run an isolated local end-to-end transfer and delta-reuse check.
    SelfTest,
    /// Run the deterministic restart and network fault-injection scenario.
    FaultTest(FaultTestArgs),
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
    /// Persistent secret-key file.
    #[arg(long, default_value = ".deltaweave/identity.key")]
    identity: PathBuf,
    /// Endpoint ID authorized to push; repeat for multiple peers.
    #[arg(long = "allow-peer")]
    allowed_peers: Vec<String>,
    /// Accept any cryptographically authenticated peer (unsafe on public networks).
    #[arg(long, conflicts_with = "allowed_peers")]
    allow_any_authenticated: bool,
    /// Bind the receiver to a stable local UDP socket address.
    #[arg(long)]
    bind: Option<SocketAddr>,
    /// Disable discovery and relay services; advertise direct addresses only.
    #[arg(long)]
    direct_only: bool,
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
    /// Optional private sender manifest-cache directory.
    #[arg(long)]
    state: Option<PathBuf>,
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
    /// Persistent local endpoint identity outside `root`.
    #[arg(long, default_value = ".deltaweave/identity.key")]
    identity: PathBuf,
    /// Remote receiver endpoint ID.
    #[arg(long)]
    peer: String,
    /// Remote direct UDP address; repeat when multiple addresses are advertised.
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
struct FaultTestArgs {
    /// Seed controlling identities, file bytes, and operation order.
    #[arg(long, default_value_t = 424_242)]
    seed: u64,
    /// Durable evidence directory. Removed after success unless explicitly supplied.
    #[arg(long)]
    workspace: Option<PathBuf>,
    /// Payload size used to keep active-transfer barriers observable.
    #[arg(long, default_value_t = 16)]
    payload_mib: usize,
    /// Deliberately fail after writing the complete reproduction bundle.
    #[arg(long)]
    force_failure: bool,
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
        Command::SelfTest => self_test().await,
        Command::FaultTest(args) => fault_test(args).await,
    }
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
    if args.allowed_peers.is_empty() && !args.allow_any_authenticated {
        bail!("serve requires at least one --allow-peer, or explicit --allow-any-authenticated");
    }
    let identity = load_or_create_identity(&args.identity)?;
    ensure_identity_outside_destination(&args.identity, &args.root)?;
    let peer_policy = if args.allow_any_authenticated {
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

    let server = start_server(ServerConfig {
        secret_key: identity.secret_key,
        destination_root: args.root,
        state_root: args.state,
        peer_policy,
        network_mode: network_mode(args.direct_only),
        bind_address: args.bind,
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
        state_root: args.state,
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
    let replica = ReplicaId(Hash32::digest(identity.endpoint_id().as_bytes()));
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
    let replica = ReplicaId(Hash32::digest(identity.endpoint_id().as_bytes()));
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

#[derive(Clone, Debug, Serialize)]
struct FaultOperation {
    sequence: usize,
    peer: &'static str,
    kind: &'static str,
    path: String,
    destination: Option<String>,
    content_hash: Option<Hash32>,
    status: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct FaultEvidence {
    barrier: &'static str,
    killed_process: &'static str,
    pid: u32,
}

#[derive(Debug, Serialize)]
struct FaultTestReport {
    status: String,
    seed: u64,
    operations: Vec<FaultOperation>,
    faults: Vec<FaultEvidence>,
    peer_logs: BTreeMap<&'static str, String>,
    roots: BTreeMap<&'static str, String>,
    states: BTreeMap<&'static str, String>,
    final_merkle_root: Option<Hash32>,
    restart_local_actions: Option<usize>,
    restart_remote_actions: Option<usize>,
    bundle: String,
    error: Option<String>,
}

fn seeded_bytes(seed: u64, label: &str, length: usize) -> Vec<u8> {
    let digest = Hash32::digest(label.as_bytes());
    let mut word = [0_u8; 8];
    word.copy_from_slice(&digest.as_bytes()[..8]);
    let mut value = seed ^ u64::from_le_bytes(word);
    (0..length)
        .map(|index| {
            value = value
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (value >> 32) as u8 ^ index as u8
        })
        .collect()
}

fn seeded_key(seed: u64, label: &str) -> SecretKey {
    let mut material = seed.to_le_bytes().to_vec();
    material.extend_from_slice(label.as_bytes());
    SecretKey::from_bytes(Hash32::digest(&material).as_bytes())
}

fn write_identity(path: &Path, key: &SecretKey) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, format!("{}\n", hex::encode(key.to_bytes())))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn executable() -> Result<PathBuf> {
    std::env::current_exe().context("failed to locate shipped CLI executable")
}

fn append_operation(log: &Path, operation: &FaultOperation) -> Result<()> {
    let mut file = fs::OpenOptions::new().create(true).append(true).open(log)?;
    serde_json::to_writer(&mut file, operation)?;
    writeln!(file)?;
    file.sync_all()?;
    Ok(())
}

fn filesystem_snapshot(root: &Path) -> Result<BTreeMap<String, Hash32>> {
    fn visit(base: &Path, current: &Path, output: &mut BTreeMap<String, Hash32>) -> Result<()> {
        let mut entries = fs::read_dir(current)?.collect::<std::io::Result<Vec<_>>>()?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)?;
            ensure!(
                !metadata.file_type().is_symlink(),
                "fault-test root contains a symlink"
            );
            if metadata.is_dir() {
                visit(base, &path, output)?;
            } else if metadata.is_file() {
                let relative = path
                    .strip_prefix(base)?
                    .to_string_lossy()
                    .replace('\\', "/");
                output.insert(relative, Hash32::digest(&fs::read(path)?));
            }
        }
        Ok(())
    }
    let mut output = BTreeMap::new();
    visit(root, root, &mut output)?;
    Ok(output)
}

fn reserve_udp_port() -> Result<u16> {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0")?;
    Ok(socket.local_addr()?.port())
}

fn spawn_server(
    binary: &Path,
    root: &Path,
    state: &Path,
    identity: &Path,
    allowed_peer: &str,
    port: u16,
    log: &Path,
) -> Result<Child> {
    let stdout = fs::OpenOptions::new().create(true).append(true).open(log)?;
    let stderr = stdout.try_clone()?;
    ProcessCommand::new(binary)
        .args(["serve", "--root"])
        .arg(root)
        .arg("--state")
        .arg(state)
        .arg("--identity")
        .arg(identity)
        .arg("--allow-peer")
        .arg(allowed_peer)
        .arg("--bind")
        .arg(format!("127.0.0.1:{port}"))
        .arg("--direct-only")
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .context("failed to spawn shipped serve process")
}

fn sync_command(
    binary: &Path,
    root: &Path,
    state: &Path,
    identity: &Path,
    peer: &str,
    port: u16,
    log: &Path,
) -> Result<ProcessCommand> {
    let stdout = fs::OpenOptions::new().create(true).append(true).open(log)?;
    let stderr = stdout.try_clone()?;
    let mut command = ProcessCommand::new(binary);
    command
        .args(["sync-once", "--root"])
        .arg(root)
        .arg("--state")
        .arg(state)
        .arg("--identity")
        .arg(identity)
        .arg("--peer")
        .arg(peer)
        .arg("--direct")
        .arg(format!("127.0.0.1:{port}"))
        .arg("--direct-only")
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    Ok(command)
}

fn wait_for_server(child: &mut Child) -> Result<()> {
    for _ in 0..100 {
        ensure!(
            child.try_wait()?.is_none(),
            "serve process exited before readiness"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    Ok(())
}

fn chunk_file_count(path: &Path) -> Result<usize> {
    if !path.exists() {
        return Ok(0);
    }
    let mut count = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        count += if entry.file_type()?.is_dir() {
            chunk_file_count(&entry.path())?
        } else if entry.metadata()?.len() > 0 {
            1
        } else {
            0
        };
    }
    Ok(count)
}

fn chunk_exists_while_destination_absent(
    chunks: &Path,
    destination: &Path,
    baseline_chunks: usize,
) -> Result<bool> {
    fn chunk_count(path: &Path) -> Result<usize> {
        if !path.exists() {
            return Ok(0);
        }
        let mut count = 0;
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            count += if entry.file_type()?.is_dir() {
                chunk_count(&entry.path())?
            } else if entry.metadata()?.len() > 0 {
                1
            } else {
                0
            };
        }
        Ok(count)
    }
    Ok(!destination.exists() && chunk_count(chunks)? > baseline_chunks)
}

fn wait_active_transfer(
    child: &mut Child,
    state: &Path,
    destination: &Path,
    baseline_chunks: usize,
) -> Result<()> {
    for _ in 0..3_000 {
        ensure!(
            child.try_wait()?.is_none(),
            "transfer process exited before active-transfer barrier"
        );
        if chunk_exists_while_destination_absent(state, destination, baseline_chunks)? {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    bail!("active payload barrier was not observed")
}

fn run_sync(command: &mut ProcessCommand) -> Result<()> {
    let status = command.status()?;
    ensure!(
        status.success(),
        "shipped sync-once process failed with {status}"
    );
    Ok(())
}

fn write_fault_report(path: &Path, report: &FaultTestReport) -> Result<()> {
    fs::write(path, serde_json::to_vec_pretty(report)?)?;
    Ok(())
}

async fn fault_test(args: FaultTestArgs) -> Result<()> {
    let temporary = if args.workspace.is_none() {
        Some(tempfile::tempdir()?)
    } else {
        None
    };
    let workspace = args.workspace.clone().unwrap_or_else(|| {
        temporary
            .as_ref()
            .expect("temporary workspace")
            .path()
            .to_path_buf()
    });
    fs::create_dir_all(&workspace)?;
    let report_path = workspace.join("report.json");
    let roots = BTreeMap::from([
        ("windows", display_path(&workspace.join("roots/windows"))),
        ("synology", display_path(&workspace.join("roots/synology"))),
    ]);
    let states = BTreeMap::from([
        ("windows", display_path(&workspace.join("states/windows"))),
        ("synology", display_path(&workspace.join("states/synology"))),
    ]);
    let logs = BTreeMap::from([
        ("windows", display_path(&workspace.join("logs/windows.log"))),
        (
            "synology",
            display_path(&workspace.join("logs/synology.log")),
        ),
    ]);
    let mut report = FaultTestReport {
        status: "running".into(),
        seed: args.seed,
        operations: Vec::new(),
        faults: Vec::new(),
        peer_logs: logs,
        roots,
        states,
        final_merkle_root: None,
        restart_local_actions: None,
        restart_remote_actions: None,
        bundle: display_path(&workspace),
        error: None,
    };
    write_fault_report(&report_path, &report)?;
    let result = fault_test_scenario(&args, &workspace, &mut report);
    if let Err(error) = result {
        report.status = "failed".into();
        report.error = Some(format!("{error:#}"));
        write_fault_report(&report_path, &report)?;
        print_json(&report)?;
        return Err(error);
    }
    report.status = if args.force_failure {
        "forced_failure"
    } else {
        "pass"
    }
    .into();
    write_fault_report(&report_path, &report)?;
    print_json(&report)?;
    if args.force_failure {
        bail!(
            "forced fault-test failure; reproduction bundle preserved at {}",
            workspace.display()
        );
    }
    Ok(())
}

fn fault_test_scenario(
    args: &FaultTestArgs,
    workspace: &Path,
    report: &mut FaultTestReport,
) -> Result<()> {
    let binary = executable()?;
    let local_root = workspace.join("roots/windows");
    let remote_root = workspace.join("roots/synology");
    let local_state = workspace.join("states/windows");
    let remote_state = workspace.join("states/synology");
    let local_log = workspace.join("logs/windows.log");
    let remote_log = workspace.join("logs/synology.log");
    fs::create_dir_all(&local_root)?;
    fs::create_dir_all(&remote_root)?;
    fs::create_dir_all(workspace.join("logs"))?;
    let client_key = seeded_key(args.seed, "windows");
    let server_key = seeded_key(args.seed, "synology");
    let client_identity = workspace.join("identities/windows.key");
    let server_identity = workspace.join("identities/synology.key");
    write_identity(&client_identity, &client_key)?;
    write_identity(&server_identity, &server_key)?;
    let port = reserve_udp_port()?;
    let mut server = spawn_server(
        &binary,
        &remote_root,
        &remote_state,
        &server_identity,
        &client_key.public().to_string(),
        port,
        &remote_log,
    )?;
    wait_for_server(&mut server)?;
    let peer = server_key.public().to_string();

    fs::write(
        local_root.join("shared.bin"),
        seeded_bytes(args.seed, "shared", 1024),
    )?;
    fs::write(
        local_root.join("obsolete.bin"),
        seeded_bytes(args.seed, "obsolete", 1024),
    )?;
    fs::write(
        remote_root.join("before.bin"),
        seeded_bytes(args.seed, "rename", 1024),
    )?;
    run_sync(&mut sync_command(
        &binary,
        &local_root,
        &local_state,
        &client_identity,
        &peer,
        port,
        &local_log,
    )?)?;
    let mut execute = |peer_name,
                       kind,
                       path: &str,
                       destination: Option<&str>,
                       bytes: Option<Vec<u8>>|
     -> Result<()> {
        let root = if peer_name == "windows" {
            &local_root
        } else {
            &remote_root
        };
        match kind {
            "create" | "modify" => fs::write(root.join(path), bytes.as_ref().expect("bytes"))?,
            "delete" => fs::remove_file(root.join(path))?,
            "rename" => fs::rename(
                root.join(path),
                root.join(destination.expect("destination")),
            )?,
            _ => bail!("unknown operation"),
        }
        let operation = FaultOperation {
            sequence: report.operations.len() + 1,
            peer: peer_name,
            kind,
            path: path.into(),
            destination: destination.map(str::to_owned),
            content_hash: bytes.as_deref().map(Hash32::digest),
            status: "executed",
        };
        append_operation(
            if peer_name == "windows" {
                &local_log
            } else {
                &remote_log
            },
            &operation,
        )?;
        report.operations.push(operation);
        write_fault_report(&workspace.join("report.json"), report)
    };
    execute(
        "windows",
        "create",
        "created.bin",
        None,
        Some(seeded_bytes(args.seed, "create", 4096)),
    )?;
    execute(
        "synology",
        "modify",
        "shared.bin",
        None,
        Some(seeded_bytes(args.seed, "modify", 8192)),
    )?;
    execute("windows", "delete", "obsolete.bin", None, None)?;
    execute("synology", "rename", "before.bin", Some("after.bin"), None)?;

    let network_path = local_root.join("network-fault.bin");
    fs::write(
        &network_path,
        seeded_bytes(args.seed, "network", args.payload_mib * 1024 * 1024),
    )?;
    let remote_destination = remote_root.join("network-fault.bin");
    let remote_baseline = chunk_file_count(&remote_state.join("chunks"))?;
    let mut network_sync = sync_command(
        &binary,
        &local_root,
        &local_state,
        &client_identity,
        &peer,
        port,
        &local_log,
    )?
    .spawn()?;
    wait_active_transfer(
        &mut network_sync,
        &remote_state,
        &remote_destination,
        remote_baseline,
    )?;
    let server_pid = server.id();
    server.kill()?;
    let _ = server.wait();
    let _ = network_sync.wait();
    report.faults.push(FaultEvidence {
        barrier: "remote_chunk_persisted_destination_absent",
        killed_process: "serve",
        pid: server_pid,
    });
    write_fault_report(&workspace.join("report.json"), report)?;

    let mut server = spawn_server(
        &binary,
        &remote_root,
        &remote_state,
        &server_identity,
        &client_key.public().to_string(),
        port,
        &remote_log,
    )?;
    wait_for_server(&mut server)?;
    run_sync(&mut sync_command(
        &binary,
        &local_root,
        &local_state,
        &client_identity,
        &peer,
        port,
        &local_log,
    )?)?;
    let process_path = local_root.join("process-fault.bin");
    fs::write(
        &process_path,
        seeded_bytes(args.seed, "process", args.payload_mib * 1024 * 1024),
    )?;
    let remote_process_destination = remote_root.join("process-fault.bin");
    let remote_process_baseline = chunk_file_count(&remote_state.join("chunks"))?;
    let mut process_sync = sync_command(
        &binary,
        &local_root,
        &local_state,
        &client_identity,
        &peer,
        port,
        &local_log,
    )?
    .spawn()?;
    wait_active_transfer(
        &mut process_sync,
        &remote_state,
        &remote_process_destination,
        remote_process_baseline,
    )?;
    let sync_pid = process_sync.id();
    process_sync.kill()?;
    let _ = process_sync.wait();
    report.faults.push(FaultEvidence {
        barrier: "remote_chunk_persisted_destination_absent",
        killed_process: "sync-once",
        pid: sync_pid,
    });
    write_fault_report(&workspace.join("report.json"), report)?;
    run_sync(&mut sync_command(
        &binary,
        &local_root,
        &local_state,
        &client_identity,
        &peer,
        port,
        &local_log,
    )?)?;

    let mut final_command = sync_command(
        &binary,
        &local_root,
        &local_state,
        &client_identity,
        &peer,
        port,
        &local_log,
    )?;
    final_command.stdout(Stdio::piped());
    let output = final_command.output()?;
    ensure!(output.status.success(), "zero-action sync failed");
    let text = String::from_utf8(output.stdout)?;
    let final_report: serde_json::Value = serde_json::from_str(&text)?;
    let local_actions = final_report["local_actions"]
        .as_u64()
        .context("missing local_actions")? as usize;
    let remote_actions = final_report["remote_actions"]
        .as_u64()
        .context("missing remote_actions")? as usize;
    let local_root_hash: Hash32 =
        serde_json::from_value(final_report["verified_local_root"].clone())?;
    let remote_root_hash: Hash32 =
        serde_json::from_value(final_report["verified_remote_root"].clone())?;
    ensure!(
        local_actions == 0 && remote_actions == 0,
        "unchanged restart was not zero-action"
    );
    ensure!(
        filesystem_snapshot(&local_root)? == filesystem_snapshot(&remote_root)?,
        "peer filesystem paths or bytes differ"
    );
    ensure!(
        local_root_hash == remote_root_hash,
        "peer Merkle roots differ"
    );
    server.kill()?;
    let _ = server.wait();
    report.final_merkle_root = Some(local_root_hash);
    report.restart_local_actions = Some(local_actions);
    report.restart_remote_actions = Some(remote_actions);
    Ok(())
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
        bind_address: None,
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
        state_root: None,
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
        state_root: None,
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
    fn parses_push_state_directory() {
        let cli = Cli::try_parse_from([
            "deltaweave",
            "push",
            "input.bin",
            "--remote-path",
            "docs/input.bin",
            "--peer",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--state",
            "./private/sender-state",
        ])
        .expect("push state command parses");
        let Command::Push(args) = cli.command else {
            panic!("push command expected");
        };
        assert_eq!(args.state, Some(PathBuf::from("./private/sender-state")));
    }

    #[test]
    fn omits_push_state_directory() {
        let cli = Cli::try_parse_from([
            "deltaweave",
            "push",
            "input.bin",
            "--remote-path",
            "docs/input.bin",
            "--peer",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ])
        .expect("push without --state parses");
        let Command::Push(args) = cli.command else {
            panic!("push command expected");
        };
        assert_eq!(args.state, None);
    }

    #[test]
    fn parses_serve_bind_address() {
        let cli = Cli::try_parse_from([
            "deltaweave",
            "serve",
            "--root",
            "output",
            "--allow-peer",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--bind",
            "172.30.1.21:49170",
            "--direct-only",
        ])
        .expect("serve bind command parses");
        let Command::Serve(args) = cli.command else {
            panic!("serve command expected");
        };
        assert_eq!(
            args.bind,
            Some("172.30.1.21:49170".parse().expect("bind address is valid"))
        );
        assert!(args.direct_only);
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
    fn parses_fault_test_reproduction_options() {
        let cli = Cli::try_parse_from([
            "deltaweave",
            "fault-test",
            "--seed",
            "424242",
            "--workspace",
            "fault-evidence",
            "--force-failure",
        ])
        .expect("fault-test command parses");
        let Command::FaultTest(args) = cli.command else {
            panic!("fault-test command selected");
        };
        assert_eq!(args.seed, 424242);
        assert_eq!(args.workspace, Some(PathBuf::from("fault-evidence")));
        assert!(args.force_failure);
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
    fn identity_inside_destination_is_rejected() {
        let temp = tempfile::tempdir().expect("temporary directory can be created");
        let root = temp.path().join("received");
        fs::create_dir_all(&root).expect("destination can be created");
        let identity = root.join("receiver.key");
        load_or_create_identity(&identity).expect("identity can be created");

        assert!(ensure_identity_outside_destination(&identity, &root).is_err());
    }
}
