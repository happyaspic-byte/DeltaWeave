use crate::{Config, Shared, now_ms};
use anyhow::{Context, Result, bail, ensure};
use deltaweave_core::{ChunkingProfile, Hash32, ReplicaId};
use deltaweave_index::{IndexOptions, LocalIndex, RetryRecord, ScanReport};
use deltaweave_net::{
    NetworkMode, PeerPolicy, Server, ServerConfig, SyncClient, endpoint_addr,
    load_or_create_identity, start_server,
};
use deltaweave_store::Store;
use deltaweave_sync::{SyncConfig, SyncEngine};
use iroh::{EndpointId, SecretKey};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    fs,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

pub(crate) struct Prepared {
    pub root: PathBuf,
    pub state: PathBuf,
    pub secret: SecretKey,
    pub peer_bind: SocketAddr,
}
impl Prepared {
    pub fn new(config: Config) -> Result<Self> {
        ensure!(
            config.bind.ip().is_loopback(),
            "HTTP bind must be a loopback IP address"
        );
        let root = fs::canonicalize(&config.root).context("root must be an existing directory")?;
        ensure!(root.is_dir(), "root must be an existing directory");
        let state = resolve_path(&config.state)?;
        let identity = resolve_path(
            &config
                .identity
                .unwrap_or_else(|| state.join("identity.key")),
        )?;
        ensure!(
            !root.starts_with(&state) && !state.starts_with(&root),
            "public root and private state must not overlap"
        );
        ensure!(
            !identity.starts_with(&root) && !root.starts_with(&identity),
            "identity must be outside the public root"
        );
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&state)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            ensure!(
                fs::metadata(&state)?.permissions().mode() & 0o077 == 0,
                "private state directory must be owner-only; run chmod 700 on {}",
                state.display()
            );
        }
        let identity = load_or_create_identity(identity)?;
        Ok(Self {
            root,
            state,
            secret: identity.secret_key,
            peer_bind: config.peer_bind,
        })
    }
    fn replica(&self) -> ReplicaId {
        ReplicaId(Hash32::digest(self.secret.public().as_bytes()))
    }
    fn index(&self) -> Result<LocalIndex> {
        LocalIndex::open(
            &self.root,
            self.state.join("index.redb"),
            self.replica(),
            IndexOptions::default(),
        )
    }
}
// Resolve existing symlinks before each subsequent component, including `..`.
fn resolve_path(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut resolved = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(_) => {
                resolved.push(component.as_os_str());
                continue;
            }
            Component::ParentDir => {
                resolved.pop();
            }
            Component::CurDir => {}
            part => resolved.push(part.as_os_str()),
        }
        match fs::symlink_metadata(&resolved) {
            Ok(_) => {
                resolved =
                    fs::canonicalize(&resolved).context("failed to resolve configured path")?
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(resolved)
}

pub(crate) enum Command {
    Scan,
    Sync {
        peer: EndpointId,
        address: SocketAddr,
    },
    Start {
        peer: EndpointId,
    },
    Stop,
    Shutdown,
}
impl Command {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Sync { .. } => "sync",
            Self::Start { .. } => "receiver_start",
            Self::Stop | Self::Shutdown => "receiver_stop",
        }
    }
    pub fn phase(&self) -> &'static str {
        match self {
            Self::Scan => "scanning",
            Self::Sync { .. } => "synchronizing",
            Self::Start { .. } => "starting_receiver",
            Self::Stop | Self::Shutdown => "stopping_receiver",
        }
    }
}

pub(crate) async fn worker(
    config: Arc<Prepared>,
    shared: Arc<Shared>,
    mut commands: mpsc::UnboundedReceiver<Command>,
) -> Result<()> {
    let mut receiver: Option<Server> = None;
    while let Some(command) = commands.recv().await {
        if matches!(command, Command::Shutdown) {
            if let Some(server) = receiver.take() {
                server.shutdown().await?;
            }
            drain_receiver(Arc::clone(&config)).await?;
            return Ok(());
        }
        let kind = command.kind();
        let result: Result<Option<Value>> = match command {
            Command::Scan => {
                let config = Arc::clone(&config);
                flatten(tokio::task::spawn_blocking(move || {
                    let index = config.index()?;
                    let report = index.scan()?;
                    if !report.issues.is_empty() || !report.collisions.is_empty() || report.retries_queued != 0 {
                        let retries = index.retries().context("failed to read rejected scan retry details")?;
                        bail!(scan_diagnostics(&report, &retries));
                    }
                    let records = index.records()?;
                    Ok(Some(json!({"report": report,"total_records":records.len(),"records":records.into_iter().take(200).collect::<Vec<_>>()})))
                }).await)
            }
            Command::Sync { peer, address } => {
                let config = Arc::clone(&config);
                flatten(
                    tokio::spawn(async move {
                        let engine = tokio::task::spawn_blocking(move || {
                            SyncEngine::open(SyncConfig {
                                root: config.root.clone(),
                                state_root: config.state.clone(),
                                replica: config.replica(),
                                client: SyncClient {
                                    secret_key: config.secret.clone(),
                                    remote: endpoint_addr(&peer.to_string(), &[address], &[])?,
                                    network_mode: NetworkMode::DirectOnly,
                                },
                                profile: ChunkingProfile::default(),
                                ignored_paths: Vec::new(),
                            })
                        })
                        .await??;
                        let report = engine.sync_once().await?;
                        Ok(Some(serde_json::to_value(report)?))
                    })
                    .await,
                )
            }
            Command::Start { peer } => {
                match start_server(ServerConfig {
                    secret_key: config.secret.clone(),
                    destination_root: config.root.clone(),
                    state_root: config.state.clone(),
                    peer_policy: PeerPolicy::AllowListed(HashSet::from([peer])),
                    network_mode: NetworkMode::DirectOnly,
                    bind_address: Some(config.peer_bind),
                    max_connections: 64,
                    min_free_space_bytes: 0,
                })
                .await
                {
                    Ok(server) => {
                        let info = serde_json::to_value(server.address_info());
                        receiver = Some(server);
                        info.map(Some).map_err(Into::into)
                    }
                    Err(error) => Err(error),
                }
            }
            Command::Stop => {
                let stopped = match receiver.take() {
                    Some(server) => server.shutdown().await,
                    None => Ok(()),
                };
                match stopped {
                    Ok(()) => drain_receiver(Arc::clone(&config)).await.map(|()| None),
                    Err(error) => Err(error),
                }
            }
            Command::Shutdown => unreachable!(),
        };
        let mut inner = shared.inner.lock().await;
        inner.busy = false;
        inner.state.phase = if receiver.is_some() {
            "receiving"
        } else if kind == "receiver_stop" && result.is_err() {
            "stopping_receiver"
        } else {
            "idle"
        }
        .into();
        let activity = inner
            .state
            .activity
            .first_mut()
            .expect("accepted operation has activity");
        activity.finished_at_ms = Some(now_ms());
        match result {
            Ok(value) => {
                activity.status = "success".into();
                match kind {
                    "scan" => inner.state.scan = value,
                    "sync" => inner.state.sync = value,
                    "receiver_start" => inner.state.receiver = value,
                    "receiver_stop" => inner.state.receiver = None,
                    _ => unreachable!(),
                }
            }
            Err(error) => {
                activity.status = "error".into();
                activity.error = Some(format!("{error:#}"));
            }
        }
    }
    if let Some(server) = receiver {
        server.shutdown().await?;
    }
    drain_receiver(config).await
}
fn flatten<T>(result: std::result::Result<Result<T>, tokio::task::JoinError>) -> Result<T> {
    result.context("operation worker failed")?
}

// iroh cancels protocol futures at shutdown; their already-started blocking jobs
// can still own redb handles. Never advertise idle until those owners release.
async fn drain_receiver(config: Arc<Prepared>) -> Result<()> {
    flatten(
        tokio::task::spawn_blocking(move || {
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let probe = (|| -> Result<()> {
                    let _index = config.index()?;
                    if config.state.join("metadata.redb").exists() {
                        let _store = Store::open(&config.state)?;
                    }
                    Ok(())
                })();
                match probe {
                    Ok(()) => return Ok(()),
                    Err(error)
                        if matches!(
                            error.downcast_ref::<redb::DatabaseError>(),
                            Some(redb::DatabaseError::DatabaseAlreadyOpen)
                        ) && Instant::now() < deadline =>
                    {
                        std::thread::sleep(Duration::from_millis(25))
                    }
                    Err(error) => {
                        return Err(error.context(
                            "receiver storage could not be released; stop again to retry",
                        ));
                    }
                }
            }
        })
        .await,
    )
}

// A rejected scan must remain actionable without replacing the last good report.
// Bound both entry counts and individual fields so one bad tree cannot fill the
// activity history with unbounded path/error text.
fn scan_diagnostics(report: &ScanReport, retries: &[RetryRecord]) -> String {
    let mut lines = vec![format!(
        "scan incomplete: {} issues, {} collisions, {} queued retries; previous successful result retained",
        report.issues.len(),
        report.collisions.len(),
        report.retries_queued
    )];
    append_diagnostics(
        &mut lines,
        "issues",
        report.issues.iter().map(|issue| {
            format!(
                "issue: {}: {:?}: {}",
                diagnostic_field(&issue.path),
                issue.kind,
                diagnostic_field(&issue.message)
            )
        }),
    );
    append_diagnostics(
        &mut lines,
        "collision paths",
        report.collisions.iter().flat_map(|group| {
            group.paths.iter().map(move |path| {
                format!(
                    "collision: {}: shares portable comparison key {}",
                    diagnostic_field(path.as_str()),
                    diagnostic_field(&group.collision_key)
                )
            })
        }),
    );
    append_diagnostics(
        &mut lines,
        "retries",
        retries.iter().map(|retry| {
            format!(
                "retry: {}: {} (attempts: {})",
                diagnostic_field(retry.path.as_str()),
                diagnostic_field(&retry.last_error),
                retry.attempts
            )
        }),
    );
    lines.join("\n")
}

fn append_diagnostics(lines: &mut Vec<String>, label: &str, entries: impl Iterator<Item = String>) {
    let mut omitted = 0;
    for (index, entry) in entries.enumerate() {
        if index < 8 {
            lines.push(entry);
        } else {
            omitted += 1;
        }
    }
    if omitted > 0 {
        lines.push(format!("... {omitted} additional {label} omitted"));
    }
}

fn diagnostic_field(value: &str) -> String {
    let mut characters = value.chars();
    let prefix: String = characters.by_ref().take(256).collect();
    let omitted = characters.count();
    if omitted == 0 {
        prefix
    } else {
        format!("{prefix}... [{omitted} characters omitted]")
    }
}

#[cfg(test)]
mod path_tests {
    use super::*;

    #[test]
    fn canonical_paths_resolve_existing_directories_and_missing_suffixes() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        assert_eq!(resolve_path(&root).unwrap(), root);
        let missing = root.join("missing/nested");
        assert_eq!(resolve_path(&missing).unwrap(), missing);
        assert!(!missing.exists(), "validation must not create directories");
    }
}
