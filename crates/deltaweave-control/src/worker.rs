use crate::{FolderCommand, FolderView, Runtime, now};
use anyhow::{Context, Result};
use deltaweave_core::{ChunkingProfile, Hash32, ReplicaId};
use deltaweave_net::{
    Inventory, NetworkMode, PeerPolicy, Server, ServerConfig, SyncClient, TransferObserver,
    endpoint_addr, load_or_create_identity, start_server_observed,
};
use deltaweave_sync::{SyncConfig, SyncEngine, SyncReport};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};

pub(crate) struct Worker {
    sender: mpsc::Sender<Message>,
    task: tokio::task::JoinHandle<()>,
}
struct Message {
    command: Option<FolderCommand>,
    reply: oneshot::Sender<Result<()>>,
}
impl Worker {
    pub async fn start(mut view: FolderView, shared: Arc<Mutex<Runtime>>) -> Result<Self> {
        let id = view.id.clone();
        let identity_path = view
            .input
            .identity_path
            .clone()
            .context("identity path missing")?;
        // Server and SyncEngine hold the common hierarchy-aware root lease.
        let identity =
            tokio::task::spawn_blocking(move || load_or_create_identity(identity_path)).await??;
        let event_id = id.clone();
        let event_shared = shared.clone();
        let observer = TransferObserver::new(move |event| {
            let mut state = event_shared.lock().expect("snapshot mutex");
            if let Some(folder) = state.folders.get_mut(&event_id) {
                folder.phase = Some(event.phase.clone());
                folder.current_path = event.path.clone();
            }
            if matches!(
                event.phase.as_str(),
                "peer_seen" | "file_received" | "file_sent" | "complete"
            ) && let Some(peer) = event.peer.as_ref()
            {
                for device in &mut state.config.devices {
                    if device.input.endpoint_id == *peer {
                        device.last_seen_at = Some(now());
                    }
                }
            }
            if matches!(event.phase.as_str(), "file_received" | "file_sent") {
                state.activity(
                    Some(event_id.clone()),
                    &event.phase,
                    event.path.clone().unwrap_or_default(),
                    event.path.clone(),
                );
                if let Some(activity) = state.activities.last_mut() {
                    match event.direction.as_deref() {
                        Some("push" | "send") => activity.pushed_bytes = event.bytes,
                        Some("pull" | "receive") => activity.pulled_bytes = event.bytes,
                        _ => {}
                    }
                }
            }
            state.revision += 1;
        });
        let engine = if view.input.role == "receive" {
            let requested_bind: std::net::SocketAddr =
                view.input.bind.as_deref().unwrap_or("0.0.0.0:0").parse()?;
            let server = start_server_observed(
                ServerConfig {
                    secret_key: identity.secret_key,
                    destination_root: view.input.root.clone().into(),
                    state_root: view
                        .input
                        .state_path
                        .clone()
                        .context("state path missing")?
                        .into(),
                    peer_policy: PeerPolicy::AllowListed(
                        view.input
                            .allowed_peers
                            .iter()
                            .map(|id| id.parse())
                            .collect::<std::result::Result<_, _>>()?,
                    ),
                    network_mode: NetworkMode::DirectOnly,
                    bind_address: Some(requested_bind),
                    max_connections: view.input.max_connections.unwrap_or(8),
                    min_free_space_bytes: view.input.min_free_space_mib.unwrap_or(64) * 1024 * 1024,
                },
                Some(observer.clone()),
            )
            .await?;
            if view.input.enabled == Some(false) {
                server.pause().await?;
            }
            let addresses = server.address_info().direct_addresses;
            let allocated = addresses
                .iter()
                .filter_map(|address| address.parse::<std::net::SocketAddr>().ok())
                .find(|address| address.is_ipv4() == requested_bind.is_ipv4())
                .context("receiver has no allocated direct address")?;
            view.input.bind =
                Some(std::net::SocketAddr::new(requested_bind.ip(), allocated.port()).to_string());
            {
                let mut state = shared.lock().expect("snapshot mutex");
                if let Some(folder) = state.folders.get_mut(&id) {
                    folder.addresses = addresses;
                    folder.input.bind = view.input.bind.clone();
                }
            }
            Engine::Receive(server)
        } else {
            let input = view.input.clone();
            let engine = tokio::task::spawn_blocking(move || -> Result<SyncEngine> {
                let min_free_space_bytes = input
                    .min_free_space_mib
                    .context("minimum free space missing")?
                    .checked_mul(1024 * 1024)
                    .context("minimum free space byte count overflow")?;
                SyncEngine::open_with_min_free_space(
                    SyncConfig {
                        swarm_sources: Vec::new(),
                        root: input.root.into(),
                        state_root: input.state_path.context("state path missing")?.into(),
                        replica: ReplicaId(Hash32::digest(identity.endpoint_id().as_bytes())),
                        client: SyncClient {
                            secret_key: identity.secret_key,
                            remote: endpoint_addr(
                                input.peer_endpoint_id.as_deref().context("peer missing")?,
                                &input
                                    .direct_addresses
                                    .iter()
                                    .map(|a| a.parse())
                                    .collect::<std::result::Result<Vec<_>, _>>()?,
                                &[],
                            )?,
                            network_mode: NetworkMode::DirectOnly,
                        },
                        profile: ChunkingProfile::default(),
                        ignored_paths: Vec::new(),
                    },
                    min_free_space_bytes,
                )
            })
            .await??;
            Engine::Sync(Arc::new(engine))
        };
        let paused = view.input.enabled == Some(false);
        set_status(
            &shared,
            &id,
            if paused {
                "paused"
            } else if view.input.role == "receive" {
                "listening"
            } else {
                "idle"
            },
        );
        let (sender, receiver) = mpsc::channel(8);
        let task = tokio::spawn(run(engine, view, observer, shared, receiver));
        Ok(Self { sender, task })
    }
    pub async fn command(&self, command: FolderCommand) -> Result<()> {
        let (reply, response) = oneshot::channel();
        self.sender
            .try_send(Message {
                command: Some(command),
                reply,
            })
            .context("folder is busy or stopped; retry after its current command")?;
        response.await.context("folder worker stopped")?
    }
    pub async fn stop(self) -> Result<()> {
        let (reply, response) = oneshot::channel();
        let sent = self
            .sender
            .send(Message {
                command: None,
                reply,
            })
            .await
            .is_ok();
        let result = if sent {
            response
                .await
                .context("folder stopped without acknowledgment")?
        } else {
            Ok(())
        };
        self.task.await.context("folder worker task failed")?;
        result
    }
}
enum Engine {
    Receive(Server),
    Sync(Arc<SyncEngine>),
}
fn set_status(shared: &Arc<Mutex<Runtime>>, id: &str, status: &str) {
    let mut state = shared.lock().expect("snapshot mutex");
    if let Some(folder) = state.folders.get_mut(id) {
        folder.status = status.into();
        folder.phase = None;
        folder.current_path = None;
    }
    state.revision += 1;
}
async fn inventory(engine: &Engine) -> Result<Inventory> {
    match engine {
        Engine::Receive(server) => server.inventory(),
        Engine::Sync(engine) => {
            let engine = engine.clone();
            tokio::task::spawn_blocking(move || engine.inventory()).await?
        }
    }
}
async fn cycle(
    engine: &Engine,
    id: &str,
    observer: &TransferObserver,
    shared: &Arc<Mutex<Runtime>>,
) -> Result<()> {
    let Engine::Sync(engine) = engine else {
        anyhow::bail!(
            "receive folders accept remote sync requests; run sync on the sending connection"
        )
    };
    set_status(shared, id, "syncing");
    let report = engine.sync_once_observed(Some(observer.clone())).await?;
    let inventory = inventory(&Engine::Sync(engine.clone())).await?;
    let mut state = shared.lock().expect("snapshot mutex");
    if let Some(folder) = state.folders.get_mut(id) {
        folder.status = "idle".into();
        folder.phase = None;
        folder.current_path = None;
        folder.last_sync_at = Some(now());
        folder.last_error = None;
        folder.retry_at = None;
        folder.files_count = inventory.files;
        folder.total_bytes = inventory.bytes;
        folder.last_report = Some(serde_json::to_value(&report)?);
    }
    state.history.push(crate::HistoryPoint {
        timestamp: now(),
        folder_id: id.into(),
        pushed_bytes: report.pushed_bytes,
        pulled_bytes: report.pulled_bytes,
    });
    state.activity(
        Some(id.into()),
        "complete",
        format!(
            "{} local, {} remote actions",
            report.local_actions, report.remote_actions
        ),
        None,
    );
    if let Some(activity) = state.activities.last_mut() {
        activity.pushed_bytes = report.pushed_bytes;
        activity.pulled_bytes = report.pulled_bytes;
    }
    record_conflicts(&mut state, id, &report)?;
    if let Some(peer) = state
        .folders
        .get(id)
        .and_then(|f| f.input.peer_endpoint_id.clone())
    {
        for device in &mut state.config.devices {
            if device.input.endpoint_id == peer {
                device.last_seen_at = Some(now());
            }
        }
    }
    state.trim();
    state.revision += 1;
    Ok(())
}
fn record_conflicts(state: &mut Runtime, id: &str, report: &SyncReport) -> Result<()> {
    for conflict in &report.conflicts {
        state.activity(
            Some(id.into()),
            if conflict.conflict_path.is_some() {
                "conflict_preserved"
            } else {
                "conflict_decision"
            },
            serde_json::to_string(conflict)?,
            conflict
                .conflict_path
                .as_ref()
                .map(|path| path.as_str().to_string()),
        );
    }
    Ok(())
}

async fn run(
    engine: Engine,
    view: FolderView,
    observer: TransferObserver,
    shared: Arc<Mutex<Runtime>>,
    mut receiver: mpsc::Receiver<Message>,
) {
    let id = view.id;
    let mut paused = view.input.enabled == Some(false);
    let interval = Duration::from_secs(view.input.interval_seconds.unwrap_or(30));
    let mut next = Instant::now() + interval;
    let mut failures = 0u32;
    let mut watcher = if matches!(&engine, Engine::Sync(_)) {
        deltaweave_index::WatchService::new(
            &view.input.root,
            &[],
            Duration::from_millis(300),
            Duration::from_secs(2),
        )
        .ok()
    } else {
        None
    };
    let mut ticker = tokio::time::interval(Duration::from_millis(250));
    loop {
        let mut resume_sync = false;
        let (result, reply, stop) = tokio::select! {
            message = receiver.recv() => {
                match message {
                    None => break,
                    Some(Message { command: None, reply }) => (Ok(()), Some(reply), true),
                    Some(Message { command: Some(command), reply }) => {
                        let result = match command {
                            FolderCommand::Sync => {
                                if paused { Err(anyhow::anyhow!("folder is paused; resume before syncing")) } else { cycle(&engine, &id, &observer, &shared).await }
                            },
                            FolderCommand::Pause => {
                                set_status(&shared, &id, "pausing");
                                let result = match &engine { Engine::Receive(server) => server.pause().await, Engine::Sync(_) => Ok(()) };
                                if result.is_ok() { paused = true; set_status(&shared, &id, "paused"); }
                                result
                            },
                            FolderCommand::Resume => {
                                let result = match &engine { Engine::Receive(server) => server.resume().await, Engine::Sync(_) => Ok(()) };
                                if result.is_ok() { paused = false; resume_sync = matches!(&engine, Engine::Sync(_)); set_status(&shared, &id, if matches!(&engine, Engine::Receive(_)) { "listening" } else { "idle" }); }
                                result
                            },
                        };
                        (result, Some(reply), false)
                    }
                }
            },
            _ = ticker.tick() => {
                let changed = watcher.as_mut().and_then(|watcher| watcher.poll(Instant::now())).is_some();
                if !paused && matches!(&engine, Engine::Sync(_)) && (Instant::now() >= next || (changed && failures == 0)) {
                    (cycle(&engine, &id, &observer, &shared).await, None, false)
                } else {
                    if let Ok(inventory) = inventory(&engine).await {
                        let mut state = shared.lock().expect("snapshot mutex");
                        if let Some(folder) = state.folders.get_mut(&id)
                            && (folder.files_count != inventory.files || folder.total_bytes != inventory.bytes) {
                            folder.files_count = inventory.files; folder.total_bytes = inventory.bytes; state.revision += 1;
                        }
                    }
                    continue;
                }
            }
        };
        if stop {
            let result = match engine {
                Engine::Receive(server) => {
                    set_status(&shared, &id, "pausing");
                    match server.pause().await {
                        Ok(()) => server.shutdown().await,
                        Err(error) => Err(error),
                    }
                }
                Engine::Sync(_) => Ok(()),
            };
            set_status(&shared, &id, "stopped");
            if let Some(reply) = reply {
                let _ = reply.send(result);
            }
            return;
        }
        match &result {
            Ok(()) => {
                failures = 0;
                next = if resume_sync {
                    Instant::now()
                } else {
                    Instant::now() + interval
                };
            }
            Err(error) => {
                failures = failures.saturating_add(1);
                let delay = 2u64.saturating_pow(failures.min(6));
                next = Instant::now() + Duration::from_secs(delay);
                let mut state = shared.lock().expect("snapshot mutex");
                if let Some(folder) = state.folders.get_mut(&id) {
                    folder.status = if paused { "paused" } else { "error" }.into();
                    folder.phase = None;
                    folder.current_path = None;
                    folder.last_error = Some(format!("{error:#}"));
                    folder.retry_at = if paused {
                        None
                    } else {
                        Some(now() + delay * 1000)
                    };
                }
                state.activity(Some(id.clone()), "error", format!("{error:#}"), None);
            }
        }
        if let Some(reply) = reply {
            let _ = reply.send(result);
        }
    }
    if let Engine::Receive(server) = engine {
        let _ = server.shutdown().await;
    }
    set_status(&shared, &id, "stopped");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_conflict_does_not_invent_a_preserved_copy() {
        let hash = Hash32::digest(b"metadata-only conflict");
        let report = SyncReport {
            status: "complete",
            local_before_root: hash,
            remote_before_root: hash,
            desired_root: hash,
            verified_local_root: hash,
            verified_remote_root: hash,
            merkle_queries: 0,
            local_actions: 0,
            remote_actions: 0,
            staged_local_files: 0,
            pulled_remote_files: 0,
            pulled_bytes: 0,
            pushed_bytes: 0,
            reused_extents: 0,
            swarm_sources_used: 0,
            conflicts: vec![
                serde_json::from_value(serde_json::json!({
                    "path": "directory", "conflict_path": null, "winner_hash": hash,
                    "loser_hash": hash, "reason": "concurrent_edit"
                }))
                .unwrap(),
            ],
        };
        let mut state = Runtime {
            config: crate::config::Config::default(),
            folders: Default::default(),
            activities: Vec::new(),
            history: Vec::new(),
            revision: 0,
        };
        record_conflicts(&mut state, "folder", &report).unwrap();
        let activity = &state.activities[0];
        assert_eq!(activity.kind, "conflict_decision");
        assert!(
            activity.path.is_none(),
            "no file copy exists for this decision"
        );
        let record: serde_json::Value = serde_json::from_str(&activity.detail).unwrap();
        assert_eq!(record["path"], "directory");
        assert!(record["conflict_path"].is_null());
    }
}
