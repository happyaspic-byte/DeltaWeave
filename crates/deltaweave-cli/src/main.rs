//! DeltaWeave command-line interface.

#![forbid(unsafe_code)]

use std::{
    collections::HashSet,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use deltaweave_cdc::manifest_from_path;
use deltaweave_core::{ChunkingProfile, WirePath};
use deltaweave_net::{
    NetworkMode, PeerPolicy, PushOptions, ServerConfig, endpoint_addr,
    load_or_create_identity, push_file, start_server,
};
use iroh::EndpointId;
use serde_json::json;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "deltaweave",
    version,
    about = "Authenticated, content-defined P2P file transfer",
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
    tokio::signal::ctrl_c().await.context("failed to wait for Ctrl-C")?;
    server.shutdown().await
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

const fn network_mode(direct_only: bool) -> NetworkMode {
    if direct_only {
        NetworkMode::DirectOnly
    } else {
        NetworkMode::Internet
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}

fn print_json(value: &impl serde::Serialize) -> Result<()> {
    serde_json::to_writer_pretty(std::io::stdout().lock(), value)?;
    println!();
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
}
