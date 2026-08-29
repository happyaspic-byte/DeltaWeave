//! Daemon configuration wrapping control-plane and loop timings.

use std::{path::PathBuf, time::Duration};

/// Bounded synchronization loop inputs extracted from the CLI.
#[derive(Clone, Debug)]
pub struct SyncLoopConfig {
    /// Maximum delay between two successful synchronization passes.
    pub interval: Duration,
    /// Quiet period after the latest local filesystem event.
    pub debounce: Duration,
    /// Maximum time a local event storm may postpone synchronization.
    pub maximum_debounce: Duration,
    /// Cap applied to exponential retry backoff.
    pub maximum_backoff: Duration,
    /// Local root watched for native filesystem events, if any.
    pub watch_root: Option<PathBuf>,
    /// Paths excluded from native watch notifications.
    pub ignored_paths: Vec<PathBuf>,
}

impl SyncLoopConfig {
    /// Validation defaults used by tests and CLI defaults.
    #[must_use]
    pub const fn default_config() -> Self {
        Self {
            interval: Duration::from_secs(5),
            debounce: Duration::from_millis(750),
            maximum_debounce: Duration::from_millis(5_000),
            maximum_backoff: Duration::from_secs(300),
            watch_root: None,
            ignored_paths: Vec::new(),
        }
    }

    /// Rejects unusable loop timings before any resource is opened.
    ///
    /// # Errors
    ///
    /// Returns an error when any interval is zero or debounce exceeds its cap.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.interval.is_zero(),
            "sync interval must be greater than zero"
        );
        anyhow::ensure!(
            !self.debounce.is_zero(),
            "debounce must be greater than zero"
        );
        anyhow::ensure!(
            self.maximum_debounce >= self.debounce,
            "maximum debounce must be at least debounce"
        );
        anyhow::ensure!(
            !self.maximum_backoff.is_zero(),
            "maximum backoff must be greater than zero"
        );
        Ok(())
    }
}

impl Default for SyncLoopConfig {
    fn default() -> Self {
        Self::default_config()
    }
}

/// Where the daemon stores its lock, token, and IPC endpoint.
#[derive(Clone, Debug)]
pub struct ControlConfig {
    /// Directory holding daemon runtime files; created before startup.
    pub runtime_dir: PathBuf,
}

impl ControlConfig {
    /// Constructs control-plane paths under `runtime_dir`.
    #[must_use]
    pub fn new(runtime_dir: impl Into<PathBuf>) -> Self {
        Self {
            runtime_dir: runtime_dir.into(),
        }
    }

    /// Lock-file path derived from the runtime directory.
    #[must_use]
    pub fn lock_path(&self) -> PathBuf {
        self.runtime_dir.join("daemon.lock")
    }

    /// Owner-token path derived from the runtime directory.
    #[must_use]
    pub fn token_path(&self) -> PathBuf {
        self.runtime_dir.join("owner.token")
    }

    /// IPC endpoint path derived from the runtime directory.
    #[must_use]
    pub fn ipc_path(&self) -> PathBuf {
        self.runtime_dir.join("control.sock")
    }
}

/// Combined daemon configuration: control plane plus loop timings.
#[derive(Clone, Debug)]
pub struct DaemonConfig {
    /// Lock, token, and IPC paths.
    pub control: ControlConfig,
    /// Reusable synchronization loop settings, including debounce and watch root.
    pub sync: SyncLoopConfig,
    /// Optional remote endpoint identity recorded in snapshots.
    pub endpoint: Option<String>,
}

impl DaemonConfig {
    /// Rejects unusable settings before the daemon opens any resource.
    ///
    /// # Errors
    ///
    /// Returns an error when any interval is zero or debounce exceeds its cap.
    pub fn validate(&self) -> anyhow::Result<()> {
        self.sync.validate()
    }
}
