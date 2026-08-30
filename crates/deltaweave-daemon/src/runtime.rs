//! Foreground daemon process: data dir, config, IPC, ready line.

use std::{
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde_json::json;

/// Per-user application state directory.
pub fn default_data_dir() -> Result<PathBuf> {
    #[cfg(windows)]
    {
        let local = std::env::var_os("LOCALAPPDATA").context("LOCALAPPDATA is not set")?;
        Ok(PathBuf::from(local).join("DeltaWeave"))
    }
    #[cfg(not(windows))]
    {
        if let Some(xdg) = std::env::var_os("XDG_STATE_HOME") {
            return Ok(PathBuf::from(xdg).join("deltaweave"));
        }
        let home = std::env::var_os("HOME").context("HOME is not set")?;
        Ok(PathBuf::from(home).join(".local/state/deltaweave"))
    }
}

/// IPC endpoint path for this data directory.
#[must_use]
pub fn ipc_path(data_dir: impl AsRef<Path>) -> PathBuf {
    #[cfg(unix)]
    {
        data_dir.as_ref().join("daemon.sock")
    }
    #[cfg(windows)]
    {
        let _ = data_dir;
        PathBuf::from(r"\\.\pipe\deltaweave")
    }
    #[cfg(not(any(unix, windows)))]
    {
        data_dir.as_ref().join("daemon.sock")
    }
}

/// Opens config, prints one JSON ready line, then serves IPC until interrupted.
pub async fn run() -> Result<()> {
    let data_dir = default_data_dir()?;
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("failed to create daemon data dir {}", data_dir.display()))?;
    if let Some(instance_id) = try_attach(&data_dir).await? {
        print_ready(&instance_id)?;
        return Ok(());
    }
    let store = std::sync::Arc::new(crate::ConfigStore::open(data_dir.join("config.redb"))?);
    let pairing = crate::PairingService::start(crate::PairingConfig {
        state_root: data_dir.join("pairing"),
        destination_root: data_dir.join("pairing-inbox"),
        identity_path: data_dir.join("identity.key"),
        bind: None,
    })
    .await?;
    #[cfg(unix)]
    {
        run_unix(&data_dir, store, pairing).await
    }
    #[cfg(windows)]
    {
        run_windows(&data_dir, store, pairing).await
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (data_dir, store, pairing);
        anyhow::bail!("daemon IPC is not implemented on this platform")
    }
}

async fn try_attach(data_dir: &Path) -> Result<Option<String>> {
    use crate::connect_and_hello;

    let socket = ipc_path(data_dir);
    match connect_and_hello(&socket).await {
        Ok(hello) => Ok(Some(hello.instance_id)),
        Err(_) => Ok(None),
    }
}

#[cfg(unix)]
async fn run_unix(
    data_dir: &Path,
    store: std::sync::Arc<crate::ConfigStore>,
    pairing: crate::PairingService,
) -> Result<()> {
    use std::time::{Duration, Instant};

    use anyhow::ensure;

    use crate::{DaemonInstance, JobSupervisor, serve_unix};

    let socket = ipc_path(data_dir);
    let instance = DaemonInstance::with_pairing(Some(store), JobSupervisor::new(), pairing);
    instance.start_continuous_loop();
    let instance_id = instance.instance_id.clone();
    let serve_socket = socket.clone();
    let mut server = tokio::spawn(async move { serve_unix(instance, serve_socket).await });
    let deadline = Instant::now() + Duration::from_secs(5);
    while !socket.exists() {
        if server.is_finished() {
            return server.await?;
        }
        ensure!(
            Instant::now() < deadline,
            "socket {} did not appear",
            socket.display()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    if server.is_finished() {
        return server.await?;
    }
    print_ready(&instance_id)?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            server.abort();
        }
        result = &mut server => {
            return result?;
        }
    }
    let _ = server.await;
    Ok(())
}

#[cfg(windows)]
async fn run_windows(
    data_dir: &Path,
    store: std::sync::Arc<crate::ConfigStore>,
    pairing: crate::PairingService,
) -> Result<()> {
    use crate::{DaemonInstance, JobSupervisor, serve_windows};

    let socket = ipc_path(data_dir);
    let instance = DaemonInstance::with_pairing(Some(store), JobSupervisor::new(), pairing);
    instance.start_continuous_loop();
    let instance_id = instance.instance_id.clone();
    let serve_socket = socket.clone();
    let mut server = tokio::spawn(async move { serve_windows(instance, serve_socket).await });
    print_ready(&instance_id)?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            server.abort();
        }
        result = &mut server => {
            return result?;
        }
    }
    let _ = server.await;
    Ok(())
}

fn print_ready(instance_id: &str) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(
        &mut stdout,
        &json!({
            "status": "ready",
            "instance_id": instance_id,
        }),
    )?;
    writeln!(stdout)?;
    stdout.flush()?;
    Ok(())
}
