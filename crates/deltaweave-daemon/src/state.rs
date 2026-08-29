//! Lifecycle states, snapshots, and command dispatch.

use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

/// Supervisory lifecycle of a running daemon.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleState {
    /// Resources are being claimed; IPC may already be listening.
    #[default]
    Starting,
    /// Synchronization loop is eligible to run.
    Running,
    /// Synchronization is held; status queries still succeed.
    Paused,
    /// The last pass failed; the loop retries with exponential backoff.
    Retrying,
    /// Shutdown has been requested; the process is draining.
    Stopping,
    /// The process has fully stopped.
    Stopped,
}

/// Health of native local-change detection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchState {
    /// The watcher is being initialized.
    #[default]
    Starting,
    /// Native events are available and may wake synchronization early.
    Watching,
    /// Native watching is unavailable; periodic polling preserves correctness.
    PollingFallback,
    /// Watcher activity is held while the daemon is paused.
    Paused,
    /// The daemon has stopped watching.
    Stopped,
}

/// Operator-visible snapshot of daemon health. Never contains secrets.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    /// Current lifecycle state.
    pub state: LifecycleState,
    /// Filesystem watcher health feeding the synchronization loop.
    pub watch_state: WatchState,
    /// Successful verified synchronization passes.
    pub successful_syncs: u64,
    /// Consecutive failed synchronization attempts.
    pub failed_syncs: u64,
    /// Last error string, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Last successful synchronization report, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_report: Option<serde_json::Value>,
    /// Wall-clock time of the last successful pass.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_success_at: Option<SystemTime>,
    /// Wall-clock time the next retry will be attempted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_retry: Option<SystemTime>,
    /// Delay before the next retry attempt.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_retry_delay: Option<Duration>,
    /// IPC endpoint advertised to authenticated clients.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
}

impl Snapshot {
    /// Records a successful pass, clearing retry state.
    #[must_use]
    pub fn with_success(mut self, report: serde_json::Value) -> Self {
        self.state = LifecycleState::Running;
        self.successful_syncs = self.successful_syncs.saturating_add(1);
        self.failed_syncs = 0;
        self.last_error = None;
        self.last_report = Some(report);
        self.last_success_at = Some(SystemTime::now());
        self.next_retry = None;
        self.next_retry_delay = None;
        self
    }

    /// Records a failed pass and schedules the next retry.
    #[must_use]
    pub fn with_failure(mut self, error: String, retry_delay: Duration) -> Self {
        self.failed_syncs = self.failed_syncs.saturating_add(1);
        self.last_error = Some(error);
        self.state = LifecycleState::Retrying;
        self.next_retry_delay = Some(retry_delay);
        self.next_retry = Some(SystemTime::now() + retry_delay);
        self
    }

    /// Moves the watcher to the given state without touching sync state.
    #[must_use]
    pub const fn with_watch_state(mut self, watch_state: WatchState) -> Self {
        self.watch_state = watch_state;
        self
    }
}

/// Authenticated local control-plane commands.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Command {
    /// Return the current snapshot without changing state.
    Status,
    /// Pause the reusable synchronization loop.
    Pause,
    /// Resume a paused loop.
    Resume,
    /// Request a graceful stop; the daemon transitions to Stopped.
    Stop,
}

/// JSON response envelope returned to `ctl`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommandResponse {
    /// Whether the command was accepted.
    pub ok: bool,
    /// Snapshot after the command was applied, when authenticated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<Snapshot>,
    /// Optional human-readable note.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl CommandResponse {
    /// Successful response carrying the latest snapshot.
    #[must_use]
    pub fn ok(snapshot: Snapshot) -> Self {
        Self {
            ok: true,
            snapshot: Some(snapshot),
            message: None,
        }
    }

    /// Rejection that never carries internal state to the caller.
    #[must_use]
    pub fn rejected(message: impl Into<String>) -> Self {
        Self {
            ok: false,
            snapshot: None,
            message: Some(message.into()),
        }
    }
}
