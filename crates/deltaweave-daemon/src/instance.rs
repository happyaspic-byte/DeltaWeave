//! Identity and lifecycle of one running daemon process.

use std::sync::Arc;

use anyhow::{Result, bail};
use deltaweave_daemon_api::{CommandResult, ConflictAction};

use crate::ConfigStore;

/// Unique live daemon process.
#[derive(Clone, Debug)]
pub struct DaemonInstance {
    /// Opaque instance identifier returned by Hello.
    pub instance_id: String,
    stop: tokio::sync::watch::Sender<bool>,
    config: Option<Arc<ConfigStore>>,
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
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let (stop, _) = tokio::sync::watch::channel(false);
        Self {
            instance_id: format!("{nanos:x}-{}", std::process::id()),
            stop,
            config,
        }
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
}
