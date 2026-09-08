//! Persistent local administration and serialized folder workers.
pub mod model;
pub use model::*;
mod config;
mod worker;

use anyhow::{Context, Result, ensure};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex as AsyncMutex, RwLock};
use worker::Worker;

pub(crate) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
static IDS: AtomicU64 = AtomicU64::new(1);
fn new_id() -> String {
    format!(
        "{:x}-{:x}-{:x}",
        now(),
        std::process::id(),
        IDS.fetch_add(1, Ordering::Relaxed)
    )
}

pub(crate) struct Runtime {
    config: config::Config,
    folders: BTreeMap<String, FolderView>,
    activities: Vec<Activity>,
    history: Vec<HistoryPoint>,
    revision: u64,
}
impl Runtime {
    fn activity(
        &mut self,
        folder_id: Option<String>,
        kind: &str,
        detail: String,
        path: Option<String>,
    ) {
        self.activities.push(Activity {
            id: new_id(),
            folder_id,
            kind: kind.into(),
            title: kind.into(),
            detail,
            timestamp: now(),
            pushed_bytes: 0,
            pulled_bytes: 0,
            path,
        });
        self.trim();
        self.revision += 1;
    }
    fn trim(&mut self) {
        let limit = self.config.settings.history_limit;
        if self.activities.len() > limit {
            self.activities.drain(..self.activities.len() - limit);
        }
        if self.history.len() > limit {
            self.history.drain(..self.history.len() - limit);
        }
    }
}
struct Slot {
    operation: AsyncMutex<Option<Worker>>,
}
/// Owns local configuration and one serialized worker for each managed root.
/// Snapshot locks protect only in-memory copies; engine and filesystem work run outside them.
pub struct Manager {
    data_dir: PathBuf,
    shared: Arc<Mutex<Runtime>>,
    slots: Mutex<BTreeMap<String, Arc<Slot>>>,
    persistence: AsyncMutex<()>,
    lifecycle: RwLock<()>,
    ownership: Mutex<Option<File>>,
    stopped: AtomicBool,
    started_at: u64,
}
impl Manager {
    pub async fn open(data_dir: PathBuf) -> Result<Arc<Self>> {
        let (data_dir, ownership, config) = tokio::task::spawn_blocking(move || -> Result<_> {
            let mut directories = std::fs::DirBuilder::new();
            directories.recursive(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                directories.mode(0o700);
            }
            directories.create(&data_dir)?;
            let data_dir = std::fs::canonicalize(data_dir)?;
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(data_dir.join("manager.lock"))?;
            fs2::FileExt::try_lock_exclusive(&file)
                .context("management data directory is already open by another process")?;
            let mut config = config::read(&data_dir)?;
            config::validate_name(&config.settings.node_name)?;
            ensure!(
                (1..=86400).contains(&config.settings.poll_interval_seconds),
                "saved poll interval must be 1–86400 seconds"
            );
            ensure!(
                (1..=10000).contains(&config.settings.history_limit),
                "saved history limit must be 1–10000"
            );
            let mut endpoints = std::collections::HashSet::new();
            let mut ids = std::collections::HashSet::new();
            for folder in &config.folders {
                ensure!(
                    ids.insert(&folder.id),
                    "duplicate folder ID in saved configuration"
                );
                ensure!(
                    endpoints.insert(&folder.endpoint_id),
                    "duplicate endpoint identity in saved configuration"
                );
            }
            for device in &config.devices {
                validate_device(&device.input)?;
            }
            // Resolve actual identities before starting any worker, including copied key files.
            let saved_folders = config.folders.clone();
            let mut actual_endpoints = std::collections::HashSet::new();
            for folder in &mut config.folders {
                folder.input = config::normalize(
                    folder.input.clone(),
                    &folder.id,
                    &data_dir,
                    &saved_folders,
                    config.settings.poll_interval_seconds,
                )?;
                folder.endpoint_id = deltaweave_net::load_or_create_identity(
                    folder
                        .input
                        .identity_path
                        .as_ref()
                        .context("identity path missing")?,
                )?
                .endpoint_id()
                .to_string();
                ensure!(
                    actual_endpoints.insert(folder.endpoint_id.clone()),
                    "duplicate endpoint identity in saved configuration"
                );
            }
            Ok((data_dir, file, config))
        })
        .await??;
        let manager = Arc::new(Self {
            data_dir,
            shared: Arc::new(Mutex::new(Runtime {
                folders: BTreeMap::new(),
                config: config.clone(),
                activities: config.activities.clone(),
                history: config.history.clone(),
                revision: 1,
            })),
            slots: Mutex::new(BTreeMap::new()),
            persistence: AsyncMutex::new(()),
            lifecycle: RwLock::new(()),
            ownership: Mutex::new(Some(ownership)),
            stopped: AtomicBool::new(false),
            started_at: now(),
        });
        for mut view in config.folders {
            let others = manager
                .shared
                .lock()
                .expect("snapshot mutex")
                .folders
                .values()
                .cloned()
                .collect::<Vec<_>>();
            view.input = config::normalize(
                view.input,
                &view.id,
                &manager.data_dir,
                &others,
                config.settings.poll_interval_seconds,
            )?;
            view.status = "starting".into();
            view.addresses.clear();
            view.phase = None;
            view.current_path = None;
            manager
                .shared
                .lock()
                .expect("snapshot mutex")
                .folders
                .insert(view.id.clone(), view.clone());
            let slot = Arc::new(Slot {
                operation: AsyncMutex::new(None),
            });
            manager
                .slots
                .lock()
                .expect("slots mutex")
                .insert(view.id.clone(), slot.clone());
            match Worker::start(view.clone(), manager.shared.clone()).await {
                Ok(worker) => *slot.operation.lock().await = Some(worker),
                Err(error) => {
                    let mut state = manager.shared.lock().expect("snapshot mutex");
                    if let Some(folder) = state.folders.get_mut(&view.id) {
                        folder.status = "error".into();
                        folder.last_error = Some(format!("{error:#}"));
                    }
                    state.activity(Some(view.id), "error", format!("{error:#}"), None);
                }
            }
        }
        // Persist dynamically allocated receiver ports before callers can copy connection details.
        let bound = manager
            .shared
            .lock()
            .expect("snapshot mutex")
            .folders
            .clone();
        manager
            .persist(|config| {
                for folder in &mut config.folders {
                    if let Some(observed) = bound.get(&folder.id) {
                        folder.input.bind = observed.input.bind.clone();
                    }
                }
                Ok(())
            })
            .await?;
        let weak = Arc::downgrade(&manager);
        tokio::spawn(async move {
            let mut saved_revision = 0;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                let Some(manager) = weak.upgrade() else { break };
                let _active = manager.lifecycle.read().await;
                if manager.stopped.load(Ordering::Acquire) {
                    break;
                }
                let revision = manager.shared.lock().expect("snapshot mutex").revision;
                if revision == saved_revision {
                    continue;
                }
                if let Err(error) = manager.persist(|_| Ok(())).await {
                    manager.shared.lock().expect("snapshot mutex").activity(
                        None,
                        "error",
                        format!("Unable to save activity history: {error:#}"),
                        None,
                    );
                } else {
                    saved_revision = revision;
                }
            }
        });
        Ok(manager)
    }

    /// Lists directories while excluding management and per-folder private state.
    pub async fn browse(&self, requested: Option<PathBuf>) -> Result<DirectoryListing> {
        let _active = self.lifecycle.read().await;
        ensure!(
            !self.stopped.load(Ordering::Acquire),
            "manager is shut down"
        );
        let mut private = vec![self.data_dir.clone()];
        {
            let state = self.shared.lock().expect("snapshot mutex");
            for folder in state.folders.values() {
                if let Some(path) = &folder.input.state_path {
                    private.push(config::candidate(std::path::Path::new(path))?);
                }
                if let Some(path) = &folder.input.identity_path {
                    private.push(config::candidate(std::path::Path::new(path))?);
                }
            }
        }
        tokio::task::spawn_blocking(move || {
            let requested = requested.unwrap_or(std::env::current_dir()?);
            let path = std::fs::canonicalize(requested)?;
            ensure!(path.is_dir(), "path is not a directory");
            ensure!(
                !private.iter().any(|denied| path.starts_with(denied)),
                "private management path is not browseable"
            );
            let mut entries = Vec::new();
            for entry in std::fs::read_dir(&path)? {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let entry_path = entry.path();
                if private
                    .iter()
                    .any(|denied| entry_path.starts_with(denied) || denied.starts_with(&entry_path))
                {
                    continue;
                }
                entries.push(DirectoryEntry {
                    name: entry.file_name().to_string_lossy().into_owned(),
                    path: entry_path.to_string_lossy().into_owned(),
                });
                ensure!(
                    entries.len() <= 10_000,
                    "directory contains too many subdirectories"
                );
            }
            entries.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(DirectoryListing {
                path: path.to_string_lossy().into_owned(),
                parent: path
                    .parent()
                    .map(|path| path.to_string_lossy().into_owned()),
                entries,
            })
        })
        .await
        .context("directory browse task failed")?
    }
    fn running(&self) -> Result<()> {
        ensure!(
            !self.stopped.load(Ordering::Acquire),
            "manager is shut down"
        );
        Ok(())
    }
    fn slot(&self, id: &str) -> Result<Arc<Slot>> {
        self.running()?;
        self.slots
            .lock()
            .expect("slots mutex")
            .get(id)
            .cloned()
            .context("folder not found")
    }
    async fn persist(&self, mutate: impl FnOnce(&mut config::Config) -> Result<()>) -> Result<()> {
        let _writer = self.persistence.lock().await;
        let mut config = {
            let state = self.shared.lock().expect("snapshot mutex");
            let mut config = state.config.clone();
            for saved in &mut config.folders {
                if let Some(observed) = state.folders.get(&saved.id) {
                    saved.last_sync_at = observed.last_sync_at;
                    saved.last_report = observed.last_report.clone();
                    saved.files_count = observed.files_count;
                    saved.total_bytes = observed.total_bytes;
                }
            }
            config.activities = state.activities.clone();
            config.history = state.history.clone();
            config
        };
        let before = serde_json::to_value(&config)?;
        mutate(&mut config)?;
        let changed = serde_json::to_value(&config)? != before;
        let dir = self.data_dir.clone();
        let saved = config.clone();
        tokio::task::spawn_blocking(move || config::save(&dir, &saved)).await??;
        let mut state = self.shared.lock().expect("snapshot mutex");
        state.config = config;
        state.trim();
        if changed {
            state.revision += 1;
        }
        Ok(())
    }
    fn resolve_device(&self, mut input: FolderInput) -> Result<FolderInput> {
        if let Some(id) = input.device_id.as_ref() {
            let state = self.shared.lock().expect("snapshot mutex");
            let device = state
                .config
                .devices
                .iter()
                .find(|device| &device.id == id)
                .context("selected device does not exist")?;
            if input.role == "sync" {
                if input.peer_endpoint_id.as_deref().is_none_or(str::is_empty) {
                    input.peer_endpoint_id = Some(device.input.endpoint_id.clone());
                }
                if input.direct_addresses.is_empty() {
                    input.direct_addresses.push(device.input.address.clone());
                }
                ensure!(
                    input.peer_endpoint_id.as_deref() == Some(device.input.endpoint_id.as_str()),
                    "peer endpoint ID does not match selected device"
                );
            }
        }
        Ok(input)
    }
    pub async fn snapshot(&self) -> AppSnapshot {
        let state = self.shared.lock().expect("snapshot mutex");
        let folders: Vec<_> = state.folders.values().cloned().collect();
        let totals = Totals {
            folders: folders.len(),
            active_folders: folders
                .iter()
                .filter(|f| !matches!(f.status.as_str(), "paused" | "stopped" | "error"))
                .count(),
            files: folders.iter().map(|f| f.files_count).sum(),
            bytes: folders.iter().map(|f| f.total_bytes).sum(),
            pushed_bytes: state.history.iter().map(|h| h.pushed_bytes).sum(),
            pulled_bytes: state.history.iter().map(|h| h.pulled_bytes).sum(),
            conflicts: folders
                .iter()
                .filter_map(|f| f.last_report.as_ref()?.get("conflicts")?.as_array())
                .map(Vec::len)
                .sum(),
        };
        AppSnapshot {
            node: Node {
                name: state.config.settings.node_name.clone(),
                version: env!("CARGO_PKG_VERSION").into(),
                platform: std::env::consts::OS.into(),
                started_at: self.started_at,
                uptime_seconds: now().saturating_sub(self.started_at) / 1000,
            },
            folders,
            devices: state.config.devices.clone(),
            activities: state.activities.iter().rev().cloned().collect(),
            history: state.history.clone(),
            totals,
            settings: state.config.settings.clone(),
            revision: state.revision,
            shares: Vec::new(),
            pending: Vec::new(),
        }
    }
    pub async fn add_folder(&self, input: FolderInput) -> Result<FolderView> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        let input = self.resolve_device(input)?;
        let id = new_id();
        // Reserve all canonical paths before opening any engine. Concurrent adds see this reservation.
        let input = {
            let (others, poll) = {
                let state = self.shared.lock().expect("snapshot mutex");
                (
                    state.folders.values().cloned().collect::<Vec<_>>(),
                    state.config.settings.poll_interval_seconds,
                )
            };
            let dir = self.data_dir.clone();
            let id = id.clone();
            tokio::task::spawn_blocking(move || config::normalize(input, &id, &dir, &others, poll))
                .await??
        };
        let identity_path = input
            .identity_path
            .clone()
            .context("identity path missing")?;
        let endpoint_id = tokio::task::spawn_blocking(move || {
            deltaweave_net::load_or_create_identity(identity_path)
                .map(|identity| identity.endpoint_id().to_string())
        })
        .await??;
        let view = FolderView {
            input,
            id: id.clone(),
            endpoint_id,
            addresses: Vec::new(),
            status: "starting".into(),
            phase: None,
            current_path: None,
            last_sync_at: None,
            last_error: None,
            retry_at: None,
            files_count: 0,
            total_bytes: 0,
            last_report: None,
        };
        {
            let mut state = self.shared.lock().expect("snapshot mutex");
            ensure!(
                state
                    .folders
                    .values()
                    .chain(state.config.folders.iter())
                    .all(|other| !paths_conflict(&view.input, &other.input)
                        && view.endpoint_id != other.endpoint_id),
                "paths or endpoint identity overlap another managed folder"
            );
            state.folders.insert(id.clone(), view.clone());
            state.revision += 1;
        }
        let slot = Arc::new(Slot {
            operation: AsyncMutex::new(None),
        });
        let mut operation = slot.operation.lock().await;
        self.slots
            .lock()
            .expect("slots mutex")
            .insert(id.clone(), slot.clone());
        let result = Worker::start(view.clone(), self.shared.clone()).await;
        match result {
            Ok(worker) => {
                *operation = Some(worker);
                let view = self.shared.lock().expect("snapshot mutex").folders[&id].clone();
                if let Err(error) = self
                    .persist(|config| {
                        validate_device_reference(config, &view.input)?;
                        config.folders.push(view.clone());
                        Ok(())
                    })
                    .await
                {
                    if let Some(worker) = operation.take() {
                        worker.stop().await?;
                    }
                    self.shared
                        .lock()
                        .expect("snapshot mutex")
                        .folders
                        .remove(&id);
                    self.slots.lock().expect("slots mutex").remove(&id);
                    return Err(error);
                }
            }
            Err(error) => {
                self.shared
                    .lock()
                    .expect("snapshot mutex")
                    .folders
                    .remove(&id);
                self.slots.lock().expect("slots mutex").remove(&id);
                return Err(error);
            }
        }
        let mut state = self.shared.lock().expect("snapshot mutex");
        state.activity(Some(id.clone()), "folder_added", view.input.name, None);
        Ok(state.folders[&id].clone())
    }
    pub async fn update_folder(&self, id: &str, input: FolderInput) -> Result<FolderView> {
        let _active = self.lifecycle.read().await;
        let input = self.resolve_device(input)?;
        let slot = self.slot(id)?;
        let mut operation = slot.operation.lock().await;
        let (old, others, poll) = {
            let state = self.shared.lock().expect("snapshot mutex");
            (
                state.folders.get(id).cloned().context("folder not found")?,
                state.folders.values().cloned().collect::<Vec<_>>(),
                state.config.settings.poll_interval_seconds,
            )
        };
        let input = FolderInput {
            state_path: input.state_path.or_else(|| old.input.state_path.clone()),
            identity_path: input
                .identity_path
                .or_else(|| old.input.identity_path.clone()),
            ..input
        };
        ensure!(
            input.role == old.input.role,
            "changing an existing state role requires a new folder connection"
        );
        let owned_id = id.to_string();
        let dir = self.data_dir.clone();
        let input = tokio::task::spawn_blocking(move || {
            config::normalize(input, &owned_id, &dir, &others, poll)
        })
        .await??;
        let identity_path = input
            .identity_path
            .clone()
            .context("identity path missing")?;
        let endpoint_id = tokio::task::spawn_blocking(move || {
            deltaweave_net::load_or_create_identity(identity_path)
                .map(|identity| identity.endpoint_id().to_string())
        })
        .await??;
        let new = FolderView {
            input,
            endpoint_id,
            status: "starting".into(),
            addresses: Vec::new(),
            ..old.clone()
        };
        {
            let mut state = self.shared.lock().expect("snapshot mutex");
            ensure!(
                state
                    .folders
                    .values()
                    .chain(state.config.folders.iter())
                    .filter(|f| f.id != id)
                    .all(|f| !paths_conflict(&new.input, &f.input)
                        && new.endpoint_id != f.endpoint_id),
                "paths or endpoint identity overlap another managed folder"
            );
            state.folders.insert(id.into(), new.clone());
        }
        if let Some(worker) = operation.take() {
            worker.stop().await?;
        }
        match Worker::start(new.clone(), self.shared.clone()).await {
            Ok(worker) => {
                *operation = Some(worker);
                let new = self.shared.lock().expect("snapshot mutex").folders[id].clone();
                if let Err(error) = self
                    .persist(|config| {
                        validate_device_reference(config, &new.input)?;
                        let folder = config
                            .folders
                            .iter_mut()
                            .find(|f| f.id == id)
                            .context("folder not found")?;
                        *folder = new.clone();
                        Ok(())
                    })
                    .await
                {
                    if let Some(worker) = operation.take() {
                        worker.stop().await?;
                    }
                    self.shared
                        .lock()
                        .expect("snapshot mutex")
                        .folders
                        .insert(id.into(), old.clone());
                    *operation = Some(Worker::start(old, self.shared.clone()).await.context(
                        "configuration save failed and previous worker could not restart",
                    )?);
                    return Err(error);
                }
            }
            Err(error) => {
                self.shared
                    .lock()
                    .expect("snapshot mutex")
                    .folders
                    .insert(id.into(), old.clone());
                match Worker::start(old, self.shared.clone()).await {
                    Ok(worker) => *operation = Some(worker),
                    Err(restore) => {
                        let mut state = self.shared.lock().expect("snapshot mutex");
                        if let Some(folder) = state.folders.get_mut(id) {
                            folder.status = "error".into();
                            folder.last_error = Some(format!(
                                "update failed: {error:#}; restoring worker failed: {restore:#}"
                            ));
                        }
                    }
                }
                return Err(error);
            }
        }
        Ok(self.shared.lock().expect("snapshot mutex").folders[id].clone())
    }
    pub async fn remove_folder(&self, id: &str) -> Result<()> {
        let _active = self.lifecycle.read().await;
        let slot = self.slot(id)?;
        let mut operation = slot.operation.lock().await;
        let old = self
            .shared
            .lock()
            .expect("snapshot mutex")
            .folders
            .get(id)
            .cloned()
            .context("folder not found")?;
        if let Some(worker) = operation.take() {
            worker.stop().await?;
        }
        if let Err(error) = self
            .persist(|config| {
                config.folders.retain(|folder| folder.id != id);
                Ok(())
            })
            .await
        {
            *operation = Some(Worker::start(old, self.shared.clone()).await?);
            return Err(error);
        }
        self.shared
            .lock()
            .expect("snapshot mutex")
            .folders
            .remove(id);
        self.slots.lock().expect("slots mutex").remove(id);
        self.shared.lock().expect("snapshot mutex").activity(
            Some(id.into()),
            "folder_removed",
            "Connection removed; local files retained".into(),
            None,
        );
        Ok(())
    }
    pub async fn command(&self, id: &str, command: FolderCommand) -> Result<()> {
        let _active = self.lifecycle.read().await;
        let slot = self.slot(id)?;
        if matches!(command, FolderCommand::Sync) {
            let state = self.shared.lock().expect("snapshot mutex");
            let folder = state.folders.get(id).context("folder not found")?;
            ensure!(
                folder.input.role == "sync",
                "receive folders accept remote requests; run sync on the sending connection"
            );
            ensure!(
                folder.input.enabled != Some(false),
                "folder is paused; resume before syncing"
            );
            ensure!(
                !matches!(folder.status.as_str(), "syncing" | "pausing"),
                "folder is busy; retry after its current command"
            );
        }
        if matches!(command, FolderCommand::Pause) {
            let mut state = self.shared.lock().expect("snapshot mutex");
            if let Some(folder) = state.folders.get_mut(id) {
                folder.status = "pausing".into();
            }
            state.revision += 1;
        }
        let operation = if matches!(command, FolderCommand::Sync) {
            slot.operation
                .try_lock()
                .context("folder is busy; retry after its current command")?
        } else {
            slot.operation.lock().await
        };
        let worker = operation
            .as_ref()
            .context("folder worker is stopped; edit the folder to retry startup")?;
        worker.command(command).await?;
        if !matches!(command, FolderCommand::Sync) {
            let enabled = matches!(command, FolderCommand::Resume);
            if let Err(error) = self
                .persist(|config| {
                    config
                        .folders
                        .iter_mut()
                        .find(|f| f.id == id)
                        .context("folder not found")?
                        .input
                        .enabled = Some(enabled);
                    Ok(())
                })
                .await
            {
                worker
                    .command(if enabled {
                        FolderCommand::Pause
                    } else {
                        FolderCommand::Resume
                    })
                    .await?;
                return Err(error);
            }
            let mut state = self.shared.lock().expect("snapshot mutex");
            if let Some(folder) = state.folders.get_mut(id) {
                folder.input.enabled = Some(enabled);
            }
            state.revision += 1;
        }
        Ok(())
    }
    pub async fn add_device(&self, input: DeviceInput) -> Result<DeviceView> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        validate_device(&input)?;
        let device = DeviceView {
            input,
            id: new_id(),
            added_at: now(),
            last_seen_at: None,
        };
        self.persist(|config| {
            ensure!(
                !config
                    .devices
                    .iter()
                    .any(|d| d.input.endpoint_id == device.input.endpoint_id),
                "device endpoint is already registered"
            );
            config.devices.push(device.clone());
            Ok(())
        })
        .await?;
        Ok(device)
    }
    pub async fn update_device(&self, id: &str, input: DeviceInput) -> Result<DeviceView> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        validate_device(&input)?;
        self.persist(|config| {
            ensure!(
                !config
                    .devices
                    .iter()
                    .any(|d| d.id != id && d.input.endpoint_id == input.endpoint_id),
                "device endpoint is already registered"
            );
            config
                .devices
                .iter_mut()
                .find(|d| d.id == id)
                .context("device not found")?
                .input = input;
            Ok(())
        })
        .await?;
        self.shared
            .lock()
            .expect("snapshot mutex")
            .config
            .devices
            .iter()
            .find(|d| d.id == id)
            .cloned()
            .context("device not found")
    }
    pub async fn remove_device(&self, id: &str) -> Result<()> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        self.persist(|config| {
            ensure!(
                config.devices.iter().any(|d| d.id == id),
                "device not found"
            );
            ensure!(
                !config
                    .folders
                    .iter()
                    .any(|f| f.input.device_id.as_deref() == Some(id)),
                "device is used by a folder; update that folder first"
            );
            config.devices.retain(|d| d.id != id);
            Ok(())
        })
        .await
    }
    pub async fn update_settings(&self, settings: Settings) -> Result<Settings> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        config::validate_name(&settings.node_name)?;
        ensure!(
            (1..=86400).contains(&settings.poll_interval_seconds),
            "poll interval must be 1–86400 seconds"
        );
        ensure!(
            (1..=10000).contains(&settings.history_limit),
            "history limit must be 1–10000"
        );
        self.persist(|config| {
            config.settings = settings.clone();
            Ok(())
        })
        .await?;
        Ok(settings)
    }
    pub async fn shutdown(&self) -> Result<()> {
        let _exclusive = self.lifecycle.write().await;
        if self.stopped.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let slots = self
            .slots
            .lock()
            .expect("slots mutex")
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut error = None;
        for slot in slots {
            if let Some(worker) = slot.operation.lock().await.take()
                && let Err(failure) = worker.stop().await
            {
                error = Some(failure);
            }
        }
        if let Err(failure) = self.persist(|_| Ok(())).await {
            error = Some(failure);
        }
        self.ownership.lock().expect("ownership mutex").take();
        if let Some(error) = error {
            return Err(error);
        }
        Ok(())
    }
}
fn validate_device_reference(config: &config::Config, input: &FolderInput) -> Result<()> {
    if let Some(id) = &input.device_id {
        let device = config
            .devices
            .iter()
            .find(|device| &device.id == id)
            .context("selected device was removed; choose a current device")?;
        if input.role == "sync" {
            ensure!(
                input.peer_endpoint_id.as_deref() == Some(device.input.endpoint_id.as_str()),
                "selected device endpoint changed; refresh its connection information"
            );
        }
    }
    Ok(())
}
fn validate_device(input: &DeviceInput) -> Result<()> {
    config::validate_name(&input.name)?;
    input
        .endpoint_id
        .parse::<iroh::EndpointId>()
        .context("invalid device endpoint ID")?;
    input
        .address
        .parse::<std::net::SocketAddr>()
        .context("device address must be IP:port")?;
    Ok(())
}
fn paths_conflict(a: &FolderInput, b: &FolderInput) -> bool {
    [
        Some(&a.root),
        a.state_path.as_ref(),
        a.identity_path.as_ref(),
    ]
    .into_iter()
    .flatten()
    .any(|a| {
        [
            Some(&b.root),
            b.state_path.as_ref(),
            b.identity_path.as_ref(),
        ]
        .into_iter()
        .flatten()
        .any(|b| config::overlaps(std::path::Path::new(a), std::path::Path::new(b)))
    })
}
