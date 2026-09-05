use anyhow::Result;
use clap::Parser;
use deltaweave_web::{Config, WebApp};
use std::{net::SocketAddr, path::PathBuf};

#[derive(Parser)]
#[command(
    version,
    about = "Local browser interface for DeltaWeave",
    after_help = "Open the private URL printed at startup. Keep its session token private. HTTP management binds only to loopback; peer traffic uses --peer-bind. Root must exist and must not overlap private state or identity."
)]
struct Args {
    /// Existing synchronized directory.
    #[arg(long)]
    root: PathBuf,
    /// Private owner-only state directory outside root.
    #[arg(long, default_value = ".deltaweave-web")]
    state: PathBuf,
    /// Persistent identity file (defaults to STATE/identity.key).
    #[arg(long)]
    identity: Option<PathBuf>,
    /// Loopback HTTP management listener.
    #[arg(long, default_value = "127.0.0.1:7840")]
    bind: SocketAddr,
    /// Direct QUIC receiving listener; select a fixed port for LAN peers.
    #[arg(long, default_value = "0.0.0.0:0")]
    peer_bind: SocketAddr,
}
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    anyhow::ensure!(
        args.bind.ip().is_loopback(),
        "HTTP bind must be a loopback IP address"
    );
    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    let app = WebApp::new(Config {
        root: args.root,
        state: args.state,
        identity: args.identity,
        bind: args.bind,
        peer_bind: args.peer_bind,
    })
    .await?;
    let authority = listener.local_addr()?.to_string();
    let state = app.snapshot().await;
    println!(
        "{}",
        serde_json::json!({"url":format!("http://{authority}/#token={}",app.token()),"root":state.root,"endpoint_id":state.endpoint_id})
    );
    let shutting_down = app.clone();
    axum::serve(listener, app.router(&authority))
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            if let Err(error) = shutting_down.shutdown().await {
                eprintln!("shutdown failed: {error:#}");
            }
        })
        .await?;
    app.shutdown().await?;
    Ok(())
}
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install termination signal handler");
        tokio::select! {_=tokio::signal::ctrl_c()=>{},_=terminate.recv()=>{}}
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
