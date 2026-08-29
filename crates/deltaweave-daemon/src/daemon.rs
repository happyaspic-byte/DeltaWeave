//! Daemon lifecycle, command dispatch, and IPC ownership.

use std::sync::Arc;

use anyhow::{Result, ensure};
use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
};

use crate::{
    auth::AuthToken,
    config::DaemonConfig,
    ipc::{IpcServer, spawn_ipc},
    lock::DaemonLock,
    state::{Command, CommandResponse, LifecycleState, Snapshot, WatchState},
    sync_loop::{SyncLoop, SyncTask},
};

/// Supervisory process owning config, lock, snapshot, and synchronization loop.
pub struct Daemon<T: SyncTask> {
    config: DaemonConfig,
    snapshot: Arc<Mutex<Snapshot>>,
    sync: Arc<SyncLoop<T>>,
    lock: Mutex<Option<DaemonLock>>,
    shutdown: watch::Sender<bool>,
}

/// Join handle for a running daemon loop.
pub struct RunningDaemon {
    task: JoinHandle<()>,
}

impl RunningDaemon {
    /// Waits for the daemon to reach `Stopped`.
    pub async fn wait(self) -> Result<()> {
        self.task.await?;
        Ok(())
    }
}

impl<T: SyncTask> Daemon<T> {
    /// Validates configuration and constructs a daemon in `Starting`.
    pub fn new(config: DaemonConfig, task: Arc<T>) -> Result<Self> {
        config.validate()?;
        std::fs::create_dir_all(&config.control.runtime_dir)?;
        let sync = Arc::new(SyncLoop::from_config(task, &config.sync));
        let (shutdown, _) = watch::channel(false);
        Ok(Self {
            snapshot: Arc::new(Mutex::new(Snapshot {
                state: LifecycleState::Starting,
                watch_state: WatchState::Starting,
                endpoint: config.endpoint.clone(),
                ..Snapshot::default()
            })),
            config,
            sync,
            lock: Mutex::new(None),
            shutdown,
        })
    }

    /// Active daemon configuration.
    #[must_use]
    pub fn config(&self) -> &DaemonConfig {
        &self.config
    }

    /// Current operator-visible snapshot.
    pub async fn snapshot(&self) -> Snapshot {
        self.snapshot.lock().await.clone()
    }

    /// Claims the single-instance lock and enters `Running`.
    pub async fn start(&self) -> Result<()> {
        let mut lock = self.lock.lock().await;
        if lock.is_none() {
            *lock = Some(DaemonLock::acquire(self.config.control.lock_path())?);
        }
        drop(lock);
        let watch_state = self.sync.ensure_watch().await;
        let mut snapshot = self.snapshot.lock().await;
        if snapshot.state == LifecycleState::Starting {
            snapshot.state = LifecycleState::Running;
            snapshot.watch_state = watch_state;
        }
        Ok(())
    }

    /// Starts the reusable synchronization loop.
    pub async fn spawn(self: Arc<Self>) -> Result<RunningDaemon> {
        self.start().await?;
        let sync = Arc::clone(&self.sync);
        let snapshot = Arc::clone(&self.snapshot);
        let shutdown = self.shutdown.subscribe();
        Ok(RunningDaemon {
            task: tokio::spawn(async move { sync.run_forever(snapshot, shutdown).await }),
        })
    }

    /// Dispatches one authenticated control command.
    pub async fn execute(&self, command: Command) -> Result<CommandResponse> {
        match command {
            Command::Status => Ok(CommandResponse::ok(self.snapshot().await)),
            Command::Pause => {
                self.sync.pause().await;
                let mut snapshot = self.snapshot.lock().await;
                if !matches!(
                    snapshot.state,
                    LifecycleState::Stopping | LifecycleState::Stopped
                ) {
                    snapshot.state = LifecycleState::Paused;
                    snapshot.watch_state = WatchState::Paused;
                }
                Ok(CommandResponse::ok(snapshot.clone()))
            }
            Command::Resume => {
                self.sync.resume().await;
                let watch_state = self.sync.watch_state().await;
                let mut snapshot = self.snapshot.lock().await;
                if snapshot.state == LifecycleState::Paused {
                    snapshot.state = LifecycleState::Running;
                    snapshot.watch_state = watch_state;
                }
                Ok(CommandResponse::ok(snapshot.clone()))
            }
            Command::Stop => {
                {
                    let mut snapshot = self.snapshot.lock().await;
                    if snapshot.state != LifecycleState::Stopped {
                        snapshot.state = LifecycleState::Stopping;
                        snapshot.watch_state = WatchState::Stopped;
                    }
                }
                let _ = self.shutdown.send(true);
                self.sync.resume().await;
                Ok(CommandResponse::ok(self.snapshot().await))
            }
        }
    }

    /// Starts authenticated local IPC after the instance lock is held.
    pub async fn spawn_ipc(self: Arc<Self>, token: AuthToken) -> Result<IpcServer> {
        ensure!(
            self.lock.lock().await.is_some(),
            "IPC requires the single-instance lock; call start() first"
        );
        spawn_ipc(Arc::clone(&self), token).await
    }

    /// Shutdown receiver for integration entry points.
    #[must_use]
    pub fn shutdown_receiver(&self) -> watch::Receiver<bool> {
        self.shutdown.subscribe()
    }

    /// Shared snapshot handle.
    #[must_use]
    pub fn snapshot_handle(&self) -> Arc<Mutex<Snapshot>> {
        Arc::clone(&self.snapshot)
    }

    /// Shared synchronization loop handle.
    #[must_use]
    pub fn sync_loop(&self) -> Arc<SyncLoop<T>> {
        Arc::clone(&self.sync)
    }
}
