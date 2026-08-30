//! Identity and lifecycle of one running daemon process.

use std::{fmt, path::PathBuf, sync::Arc};

use anyhow::{Result, bail};
use deltaweave_daemon_api::{CommandResult, ConflictAction, Direction as ApiDirection, JobInfo};

use crate::{ConfigStore, Direction, JobSupervisor, PairingService};

/// Unique live daemon process.
#[derive(Clone)]
pub struct DaemonInstance {
    /// Opaque instance identifier returned by Hello.
    pub instance_id: String,
    stop: tokio::sync::watch::Sender<bool>,
    config: Option<Arc<ConfigStore>>,
    supervisor: Arc<JobSupervisor>,
    pairing: Option<Arc<PairingService>>,
}

impl fmt::Debug for DaemonInstance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DaemonInstance")
            .field("instance_id", &self.instance_id)
            .finish_non_exhaustive()
    }
}

impl PartialEq for DaemonInstance {
    fn eq(&self, other: &Self) -> bool {
        self.instance_id == other.instance_id
    }
}

impl Eq for DaemonInstance {}

impl Default for DaemonInstance {
    fn default() -> Self {
        Self::new()
    }
}

impl DaemonInstance {
    /// Creates a new instance id for this process.
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(None)
    }

    /// Creates an instance that can resolve jobs from `config`.
    #[must_use]
    pub fn with_config(config: Option<Arc<ConfigStore>>) -> Self {
        Self::with_runtime(config, JobSupervisor::new())
    }

    /// Creates an instance with durable config and live job supervision.
    #[must_use]
    pub fn with_runtime(config: Option<Arc<ConfigStore>>, supervisor: JobSupervisor) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let (stop, _) = tokio::sync::watch::channel(false);
        Self {
            instance_id: format!("{nanos:x}-{}", std::process::id()),
            stop,
            config,
            supervisor: Arc::new(supervisor),
            pairing: None,
        }
    }

    /// Creates an instance that can issue and redeem pairing tickets.
    #[must_use]
    pub fn with_pairing(
        config: Option<Arc<ConfigStore>>,
        supervisor: JobSupervisor,
        pairing: PairingService,
    ) -> Self {
        let mut instance = Self::with_runtime(config, supervisor);
        instance.pairing = Some(Arc::new(pairing));
        instance
    }

    pub(crate) fn request_stop(&self) {
        self.stop.send_replace(true);
    }

    pub(crate) fn subscribe_stop(&self) -> tokio::sync::watch::Receiver<bool> {
        self.stop.subscribe()
    }

    pub(crate) fn resolve_job_conflict(
        &self,
        id: &str,
        path: &str,
        action: ConflictAction,
    ) -> Result<CommandResult> {
        let Some(store) = self.config.as_ref() else {
            bail!("config store unavailable");
        };
        let job = store
            .list_jobs()?
            .into_iter()
            .find(|job| job.id == id)
            .ok_or_else(|| anyhow::anyhow!("unknown job"))?;
        crate::resolve_conflict(&job.local_root, path, action)
    }

    pub(crate) fn list_jobs(&self) -> Result<CommandResult> {
        let store = self.config_store()?;
        let jobs = store
            .list_jobs()?
            .into_iter()
            .map(|job| {
                self.supervisor.ensure_job_with_pause(&job.id, job.paused);
                JobInfo {
                    id: job.id,
                    name: job.name,
                    local_root: job.local_root.to_string_lossy().into(),
                    peer_endpoint_id: job.peer_endpoint_id,
                    direction: match job.direction {
                        Direction::Bidirectional => ApiDirection::Bidirectional,
                        Direction::SendOnly => ApiDirection::SendOnly,
                        Direction::ReceiveOnly => ApiDirection::ReceiveOnly,
                    },
                    continuous: job.continuous,
                    paused: job.paused,
                }
            })
            .collect();
        Ok(CommandResult::Jobs { jobs })
    }

    pub(crate) fn create_job(
        &self,
        name: String,
        local_root: String,
        peer_endpoint_id: String,
        peer_address: Option<String>,
        direction: ApiDirection,
    ) -> Result<CommandResult> {
        let store = self.config_store()?;
        let direction = match direction {
            ApiDirection::Bidirectional => Direction::Bidirectional,
            ApiDirection::SendOnly => Direction::SendOnly,
            ApiDirection::ReceiveOnly => Direction::ReceiveOnly,
        };
        let job = store.create_job(
            name,
            PathBuf::from(local_root),
            peer_endpoint_id,
            peer_address,
            direction,
        )?;
        self.supervisor.ensure_job(&job.id);
        Ok(CommandResult::Accepted { id: job.id })
    }

    pub(crate) fn set_job_paused(&self, id: &str, paused: bool) -> Result<CommandResult> {
        let store = self.config_store()?;
        store.set_paused(id, paused)?;
        self.supervisor.ensure_job(id);
        if paused {
            self.supervisor.pause(id)?;
        } else {
            self.supervisor.resume(id)?;
        }
        Ok(CommandResult::Accepted { id: id.into() })
    }

    pub(crate) fn cancel_job(&self, id: &str) -> Result<CommandResult> {
        self.supervisor.ensure_job(id);
        self.supervisor.cancel(id)?;
        Ok(CommandResult::Accepted { id: id.into() })
    }

    pub(crate) async fn sync_now(&self, id: &str) -> Result<CommandResult> {
        let _ = self.job(id)?;
        self.supervisor.ensure_job(id);
        let cancel = self.supervisor.begin_sync(id)?;
        let _guard = self.supervisor.sync_guard(id);
        async {
            let job = self.job(id)?;
            let address = job
                .peer_address
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("peer direct address unavailable"))?;
            let pairing = self.pairing_service()?;
            let engine = deltaweave_sync::SyncEngine::open(deltaweave_sync::SyncConfig {
                root: job.local_root,
                state_root: job.state_root,
                replica: pairing.replica_id()?,
                client: pairing.sync_client(&job.peer_endpoint_id, address)?,
                profile: deltaweave_core::ChunkingProfile::DEFAULT,
                ignored_paths: Vec::new(),
            })?;
            let direction = match job.direction {
                Direction::Bidirectional => deltaweave_sync::SyncDirection::Bidirectional,
                Direction::SendOnly => deltaweave_sync::SyncDirection::SendOnly,
                Direction::ReceiveOnly => deltaweave_sync::SyncDirection::ReceiveOnly,
            };
            engine.sync_once_with(None, Some(cancel), direction).await?;
            Ok(CommandResult::Accepted { id: id.into() })
        }
        .await
    }

    pub(crate) async fn preview_job(
        &self,
        local_root: String,
        peer_endpoint_id: String,
        peer_address: String,
    ) -> Result<CommandResult> {
        let pairing = self.pairing_service()?;
        let store = self.config_store()?;
        let digest = deltaweave_core::Hash32::digest(
            format!("{local_root}:{peer_endpoint_id}:{peer_address}").as_bytes(),
        );
        let state_root = store
            .data_root()
            .join("preview")
            .join(&digest.to_hex()[..16]);
        let engine = deltaweave_sync::SyncEngine::open(deltaweave_sync::SyncConfig {
            root: PathBuf::from(local_root),
            state_root,
            replica: pairing.replica_id()?,
            client: pairing.sync_client(&peer_endpoint_id, &peer_address)?,
            profile: deltaweave_core::ChunkingProfile::DEFAULT,
            ignored_paths: Vec::new(),
        })?;
        let (local, remote) = engine.preview_once().await?;
        crate::preview_snapshots(&local, &remote)
    }

    pub(crate) fn list_job_conflicts(&self, id: &str) -> Result<CommandResult> {
        let job = self.job(id)?;
        crate::list_conflicts(&job.local_root)
    }

    pub(crate) async fn redeem_ticket(&self, code: &str) -> Result<CommandResult> {
        self.pairing_service()?.redeem_ticket(code).await
    }

    pub(crate) fn issue_ticket(&self, ttl_seconds: Option<u64>) -> Result<CommandResult> {
        self.pairing_service()?.issue_ticket(ttl_seconds)
    }

    pub(crate) fn revoke_peer(&self, endpoint_id: &str) -> Result<CommandResult> {
        self.pairing_service()?.revoke_peer(endpoint_id)?;
        Ok(CommandResult::Accepted {
            id: endpoint_id.into(),
        })
    }

    fn pairing_service(&self) -> Result<&PairingService> {
        self.pairing
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("pairing service unavailable"))
    }

    fn job(&self, id: &str) -> Result<crate::JobConfig> {
        self.config_store()?
            .list_jobs()?
            .into_iter()
            .find(|job| job.id == id)
            .ok_or_else(|| anyhow::anyhow!("unknown job"))
    }

    fn config_store(&self) -> Result<&Arc<ConfigStore>> {
        self.config
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("config store unavailable"))
    }
}
