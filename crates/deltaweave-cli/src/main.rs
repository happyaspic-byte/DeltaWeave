//! DeltaWeave command-line interface.

#![forbid(unsafe_code)]

use std::{
    collections::HashSet,
    fs,
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use clap::{Args, Parser, Subcommand};
use deltaweave_cdc::manifest_from_path;
use deltaweave_core::{ChunkingProfile, Hash32, ReplicaId, WirePath};
use deltaweave_daemon::{
    AuthToken, Command as DaemonCommand, ControlConfig, Daemon, DaemonConfig, IpcClient, Snapshot,
    SyncLoop, SyncLoopConfig, SyncLoopEvent, SyncTask,
};
use deltaweave_index::{IndexOptions, LocalIndex, ScanChange, WatchService};
use deltaweave_net::{
    NetworkMode, PeerPolicy, PushOptions, Server, ServerConfig, SyncClient, TransferReceipt,
    endpoint_addr, load_or_create_identity, push_file, start_server,
};
use deltaweave_sync::{SyncConfig, SyncEngine};
use iroh::{EndpointId, SecretKey};
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
    /// Run synchronization under the authenticated local daemon control plane.
    Daemon(DaemonArgs),
    /// Send one authenticated command to a local daemon.
    Ctl(CtlArgs),
    /// Render or run an operating-system service entry point.
    #[command(subcommand)]
    Service(ServiceArgs),
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

#[derive(Clone, Debug, Args)]
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

#[derive(Debug, Args)]
struct DaemonArgs {
    #[command(flatten)]
    sync: SyncArgs,
    /// Directory holding daemon lock, owner token, and IPC socket.
    #[arg(long)]
    control_state: PathBuf,
}

#[derive(Debug, Subcommand)]
enum CtlCommand {
    /// Print the current daemon snapshot.
    Status,
    /// Pause the synchronization loop.
    Pause,
    /// Resume a paused synchronization loop.
    Resume,
    /// Request a graceful shutdown.
    Stop,
}

impl CtlCommand {
    fn daemon_command(&self) -> DaemonCommand {
        match self {
            Self::Status => DaemonCommand::Status,
            Self::Pause => DaemonCommand::Pause,
            Self::Resume => DaemonCommand::Resume,
            Self::Stop => DaemonCommand::Stop,
        }
    }
}

#[derive(Debug, Args)]
struct CtlArgs {
    /// Directory holding daemon lock, owner token, and IPC socket.
    #[arg(long)]
    control_state: PathBuf,
    #[command(subcommand)]
    command: CtlCommand,
}

#[derive(Debug, Subcommand)]
enum ServiceArgs {
    /// Print a hardened systemd unit for the given absolute paths and user.
    SystemdUnit(SystemdUnitArgs),
    /// Windows service entry point invoked by the Service Control Manager.
    #[cfg(windows)]
    Run(DaemonArgs),
}

#[derive(Debug, Args)]
struct SystemdUnitArgs {
    /// Absolute path to the deltaweave executable.
    #[arg(long)]
    executable: PathBuf,
    /// System user that runs the daemon.
    #[arg(long)]
    user: String,
    #[command(flatten)]
    daemon: DaemonArgs,
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
        Command::Daemon(args) => run_daemon(args).await,
        Command::Ctl(args) => run_ctl(args).await,
        Command::Service(ServiceArgs::SystemdUnit(args)) => {
            print!("{}", render_systemd_unit(&args)?);
            Ok(())
        }
        #[cfg(windows)]
        Command::Service(ServiceArgs::Run(args)) => run_daemon(args).await,
        Command::SelfTest => self_test().await,
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

struct EngineTask(SyncEngine);

impl SyncTask for EngineTask {
    async fn sync_once(&self) -> anyhow::Result<serde_json::Value> {
        Ok(serde_json::to_value(self.0.sync_once().await?)?)
    }
}

fn loop_config(args: &SyncArgs) -> SyncLoopConfig {
    SyncLoopConfig {
        interval: Duration::from_secs(args.interval_seconds),
        debounce: Duration::from_millis(args.debounce_ms),
        maximum_debounce: Duration::from_millis(args.max_debounce_ms),
        maximum_backoff: Duration::from_secs(args.max_backoff_seconds),
        watch_root: Some(args.target.root.clone()),
        ignored_paths: Vec::new(),
    }
}

async fn run_daemon(args: DaemonArgs) -> Result<()> {
    let engine = open_sync_engine(args.sync.target.clone())?;
    let config = DaemonConfig {
        control: ControlConfig::new(&args.control_state),
        sync: loop_config(&args.sync),
        endpoint: Some(args.sync.target.peer.clone()),
    };
    let daemon = Arc::new(Daemon::new(config, Arc::new(EngineTask(engine)))?);
    let token = AuthToken::load_or_create(&daemon.config().control.token_path())?;
    let running = Arc::clone(&daemon).spawn().await?;
    let ipc = Arc::clone(&daemon).spawn_ipc(token).await?;
    let stopped = running.wait();
    tokio::pin!(stopped);
    tokio::select! {
        result = wait_for_shutdown_signal() => {
            result?;
            let _response = daemon.execute(DaemonCommand::Stop).await?;
            stopped.await?;
        }
        result = &mut stopped => {
            result?;
        }
    }
    ipc.shutdown().await?;
    Ok(())
}

async fn run_ctl(args: CtlArgs) -> Result<()> {
    let control = ControlConfig::new(args.control_state);
    ensure!(
        control.token_path().is_file(),
        "daemon owner token not found at {}",
        control.token_path().display()
    );
    let token = AuthToken::load_or_create(&control.token_path())?;
    let client = IpcClient::new(control.ipc_path(), token);
    let response = client.send(args.command.daemon_command()).await?;
    print_json(&response)?;
    ensure!(
        response.ok,
        "{}",
        response.message.as_deref().unwrap_or("ctl command failed")
    );
    Ok(())
}

fn require_absolute(path: &Path, name: &str) -> Result<()> {
    ensure!(
        path.is_absolute(),
        "{name} must be an absolute path, got {}",
        path.display()
    );
    Ok(())
}

fn systemd_quote(value: &str) -> String {
    let mut out = String::from("\"");
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            _ => out.push(ch),
        }
    }
    out.push('"');
    out
}

fn render_systemd_unit(args: &SystemdUnitArgs) -> Result<String> {
    require_absolute(&args.executable, "executable")?;
    require_absolute(&args.daemon.control_state, "control-state")?;
    require_absolute(&args.daemon.sync.target.root, "root")?;
    require_absolute(&args.daemon.sync.target.state, "state")?;
    require_absolute(&args.daemon.sync.target.identity, "identity")?;
    ensure!(!args.user.is_empty(), "user must not be empty");
    ensure!(
        !args.user.contains(['\n', '\r']),
        "user must not contain line breaks"
    );

    let mut exec = format!(
        "ExecStart={} daemon --root {} --state {} --identity {} --peer {} --control-state {} --interval-seconds {} --debounce-ms {} --max-debounce-ms {} --max-backoff-seconds {}",
        systemd_quote(&args.executable.display().to_string()),
        systemd_quote(&args.daemon.sync.target.root.display().to_string()),
        systemd_quote(&args.daemon.sync.target.state.display().to_string()),
        systemd_quote(&args.daemon.sync.target.identity.display().to_string()),
        systemd_quote(&args.daemon.sync.target.peer),
        systemd_quote(&args.daemon.control_state.display().to_string()),
        args.daemon.sync.interval_seconds,
        args.daemon.sync.debounce_ms,
        args.daemon.sync.max_debounce_ms,
        args.daemon.sync.max_backoff_seconds,
    );
    for address in &args.daemon.sync.target.direct_addresses {
        exec.push_str(" --direct ");
        exec.push_str(&systemd_quote(&address.to_string()));
    }
    for url in &args.daemon.sync.target.relay_urls {
        exec.push_str(" --relay ");
        exec.push_str(&systemd_quote(url));
    }
    if args.daemon.sync.target.direct_only {
        exec.push_str(" --direct-only");
    }

    Ok(format!(
        "[Unit]\n\
         Description=DeltaWeave synchronization daemon\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         User={}\n\
         {}\n\
         Restart=on-failure\n\
         RestartSec=5\n\
         NoNewPrivileges=true\n\
         ProtectSystem=strict\n\
         ProtectHome=read-only\n\
         PrivateTmp=true\n\
         ReadWritePaths={} {} {}\n\
         \n\
         [Install]\n\
         WantedBy=multi-user.target\n",
        args.user,
        exec,
        systemd_quote(&args.daemon.sync.target.root.display().to_string()),
        systemd_quote(&args.daemon.sync.target.state.display().to_string()),
        systemd_quote(&args.daemon.control_state.display().to_string()),
    ))
}

async fn sync_forever(args: SyncArgs) -> Result<()> {
    let loop_config = loop_config(&args);
    loop_config.validate()?;
    let task = Arc::new(EngineTask(open_sync_engine(args.target)?));
    let sync = Arc::new(SyncLoop::from_config(task, &loop_config));
    sync.set_event_hook(move |event| {
        let value = match event {
            SyncLoopEvent::Started {
                watch_state,
                watcher_error,
            } => json!({
                "event": "sync_started",
                "local_change_detection": match watch_state {
                    deltaweave_daemon::WatchState::PollingFallback => "polling_fallback",
                    _ => "native_watcher",
                },
                "remote_poll_seconds": loop_config.interval.as_secs(),
                "watcher_error": watcher_error,
            }),
            SyncLoopEvent::Success { report } => json!({"event": "sync", "report": report}),
            SyncLoopEvent::Failure { error, retry } => json!({
                "event": "sync_error",
                "error": error,
                "retry_in_seconds": retry.as_secs(),
                "status": "retrying",
            }),
            SyncLoopEvent::LocalChange {
                event_count,
                rescan_required,
            } => json!({
                "event": "local_change",
                "native_events": event_count,
                "rescan_required": rescan_required,
                "status": "synchronizing",
            }),
            SyncLoopEvent::Stopped => json!({"event": "shutdown", "status": "stopped"}),
        };
        let _ = print_json(&value);
    })
    .await;
    let snapshot = Arc::new(tokio::sync::Mutex::new(Snapshot::default()));
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(Arc::clone(&sync).run_forever(snapshot, receiver));
    wait_for_shutdown_signal().await?;
    let _ = shutdown.send(true);
    sync.resume().await;
    task.await?;
    Ok(())
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
    fn identity_inside_destination_is_rejected() {
        let temp = tempfile::tempdir().expect("temporary directory can be created");
        let root = temp.path().join("received");
        fs::create_dir_all(&root).expect("destination can be created");
        let identity = root.join("receiver.key");
        load_or_create_identity(&identity).expect("identity can be created");

        assert!(ensure_identity_outside_destination(&identity, &root).is_err());
    }
}
