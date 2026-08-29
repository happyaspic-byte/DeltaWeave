//! Authenticated local IPC over Unix sockets or Windows loopback TCP.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::watch,
    task::JoinHandle,
};
use tracing::{info, warn};

use crate::{
    Daemon,
    auth::AuthToken,
    frame::{read_frame, write_frame},
    state::{Command, CommandResponse},
};

/// JSON envelope wrapping an authenticated command.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IpcRequest {
    /// Hex-encoded owner token.
    pub token: String,
    /// Requested command.
    pub command: Command,
}

/// Handle to a listening IPC server.
pub struct IpcServer {
    endpoint: PathBuf,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl IpcServer {
    /// Filesystem path of the Unix socket, or the Windows endpoint file.
    #[must_use]
    pub fn endpoint(&self) -> &Path {
        &self.endpoint
    }

    /// Stops accepting connections and waits for the accept loop to exit.
    ///
    /// # Errors
    ///
    /// Returns an error if the accept task panicked.
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown.send(true);
        self.task.await.context("IPC accept loop panicked")?;
        let _ = fs::remove_file(&self.endpoint);
        Ok(())
    }
}

/// Client that authenticates every request with the owner token.
pub struct IpcClient {
    endpoint: PathBuf,
    token: AuthToken,
}

impl IpcClient {
    /// Constructs a client for the given endpoint and owner token.
    #[must_use]
    pub fn new(endpoint: PathBuf, token: AuthToken) -> Self {
        Self { endpoint, token }
    }

    /// Sends one authenticated command and returns the daemon response.
    ///
    /// # Errors
    ///
    /// Returns an error on connect, framing, authentication, or I/O failure.
    pub async fn send(&self, command: Command) -> Result<CommandResponse> {
        let request = IpcRequest {
            token: self.token.to_hex(),
            command,
        };
        let mut stream = connect(&self.endpoint).await?;
        write_frame(&mut stream, &request).await?;
        read_frame(&mut stream).await
    }
}

/// Starts an authenticated IPC listener for `daemon`.
///
/// Unix: binds `runtime_dir/control.sock` at mode `0600`. A socket left by a
/// dead instance is recovered only after connect fails, so a live peer's
/// endpoint is never hijacked.
/// Windows: binds `127.0.0.1:0` and writes the loopback address to
/// `runtime_dir/control.sock`. The 32-byte owner token remains the
/// authentication secret.
///
/// # Errors
///
/// Returns an error when the listener cannot bind or the endpoint file cannot
/// be written.
pub async fn spawn_ipc<T>(daemon: Arc<Daemon<T>>, token: AuthToken) -> Result<IpcServer>
where
    T: crate::sync_loop::SyncTask,
{
    let (shutdown, rx) = watch::channel(false);
    spawn_platform(daemon, token, shutdown, rx).await
}

#[cfg(unix)]
async fn spawn_platform<T>(
    daemon: Arc<Daemon<T>>,
    token: AuthToken,
    shutdown: watch::Sender<bool>,
    rx: watch::Receiver<bool>,
) -> Result<IpcServer>
where
    T: crate::sync_loop::SyncTask,
{
    use std::os::unix::fs::PermissionsExt;
    use tokio::net::UnixListener;

    let path = daemon.config().control.ipc_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
        crate::lock::restrict_owner_dir(parent)?;
    }
    if path.exists() {
        match tokio::net::UnixStream::connect(&path).await {
            Ok(_) => bail!("IPC endpoint {} is already active", path.display()),
            Err(_) => fs::remove_file(&path)
                .with_context(|| format!("failed to remove stale IPC socket {}", path.display()))?,
        }
    }
    let listener = UnixListener::bind(&path)
        .with_context(|| format!("failed to bind Unix IPC socket {}", path.display()))?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    info!(path = %path.display(), "listening on owner-only Unix IPC socket");
    let task = tokio::spawn(unix_accept_loop(listener, daemon, token, rx));
    Ok(IpcServer {
        endpoint: path,
        shutdown,
        task,
    })
}

#[cfg(windows)]
async fn spawn_platform<T>(
    daemon: Arc<Daemon<T>>,
    token: AuthToken,
    shutdown: watch::Sender<bool>,
    rx: watch::Receiver<bool>,
) -> Result<IpcServer>
where
    T: crate::sync_loop::SyncTask,
{
    use std::io::Write;
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind Windows loopback IPC socket")?;
    let addr = listener.local_addr()?;
    anyhow::ensure!(
        addr.ip().is_loopback(),
        "Windows IPC must bind loopback only"
    );
    let endpoint_text = addr.to_string();
    let path = daemon.config().control.ipc_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    if path.exists() {
        let live = fs::read_to_string(&path)
            .ok()
            .and_then(|text| text.trim().parse::<std::net::SocketAddr>().ok())
            .filter(|addr| addr.ip().is_loopback());
        if let Some(existing) = live {
            if tokio::net::TcpStream::connect(existing).await.is_ok() {
                bail!("IPC endpoint {} is already active", path.display());
            }
        }
        fs::remove_file(&path)
            .with_context(|| format!("failed to remove stale IPC endpoint {}", path.display()))?;
    }
    let mut options = fs::OpenOptions::new();
    options.create_new(true).write(true);
    let mut file = options
        .open(&path)
        .with_context(|| format!("failed to create IPC endpoint file {}", path.display()))?;
    file.write_all(format!("{endpoint_text}\n").as_bytes())?;
    file.sync_all()?;
    info!(endpoint = %endpoint_text, "listening on Windows loopback IPC");
    let task = tokio::spawn(tcp_accept_loop(listener, daemon, token, rx));
    Ok(IpcServer {
        endpoint: path,
        shutdown,
        task,
    })
}

#[cfg(unix)]
async fn unix_accept_loop<T>(
    listener: tokio::net::UnixListener,
    daemon: Arc<Daemon<T>>,
    token: AuthToken,
    mut shutdown: watch::Receiver<bool>,
) where
    T: crate::sync_loop::SyncTask,
{
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _)) => {
                        let daemon = Arc::clone(&daemon);
                        let token = token.clone();
                        tokio::spawn(async move {
                            let _ = serve_connection(stream, daemon, token).await;
                        });
                    }
                    Err(error) => {
                        warn!(%error, "IPC accept failed; stopping accept loop");
                        break;
                    }
                }
            }
        }
    }
}

#[cfg(windows)]
async fn tcp_accept_loop<T>(
    listener: tokio::net::TcpListener,
    daemon: Arc<Daemon<T>>,
    token: AuthToken,
    mut shutdown: watch::Receiver<bool>,
) where
    T: crate::sync_loop::SyncTask,
{
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer)) => {
                        if !peer.ip().is_loopback() {
                            warn!(%peer, "refusing non-loopback IPC connection");
                            continue;
                        }
                        let daemon = Arc::clone(&daemon);
                        let token = token.clone();
                        tokio::spawn(async move {
                            let _ = serve_connection(stream, daemon, token).await;
                        });
                    }
                    Err(error) => {
                        warn!(%error, "IPC accept failed; stopping accept loop");
                        break;
                    }
                }
            }
        }
    }
}

async fn serve_connection<S, T>(
    mut stream: S,
    daemon: Arc<Daemon<T>>,
    token: AuthToken,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: crate::sync_loop::SyncTask,
{
    let request: IpcRequest = match read_frame(&mut stream).await {
        Ok(request) => request,
        Err(error) => {
            let _ = write_frame(
                &mut stream,
                &CommandResponse::rejected(format!("malformed request: {error}")),
            )
            .await;
            return Ok(());
        }
    };
    let authenticated = AuthToken::from_hex(&request.token)
        .map(|presented| token.verify(&presented.expose()))
        .unwrap_or(false);
    if !authenticated {
        write_frame(&mut stream, &CommandResponse::rejected("unauthenticated")).await?;
        return Ok(());
    }
    let response = daemon.execute(request.command).await?;
    write_frame(&mut stream, &response).await
}

#[cfg(unix)]
async fn connect(endpoint: &Path) -> Result<tokio::net::UnixStream> {
    tokio::net::UnixStream::connect(endpoint)
        .await
        .with_context(|| format!("failed to connect to {}", endpoint.display()))
}

#[cfg(windows)]
async fn connect(endpoint: &Path) -> Result<tokio::net::TcpStream> {
    let text = fs::read_to_string(endpoint)
        .with_context(|| format!("failed to read IPC endpoint {}", endpoint.display()))?;
    let addr: std::net::SocketAddr = text
        .trim()
        .parse()
        .with_context(|| format!("invalid IPC endpoint {}", endpoint.display()))?;
    anyhow::ensure!(
        addr.ip().is_loopback(),
        "refusing non-loopback IPC endpoint"
    );
    tokio::net::TcpStream::connect(addr)
        .await
        .with_context(|| format!("failed to connect to {addr}"))
}
