//! Reusable bounded synchronization loop driving any injectable sync backend.

use std::{future::Future, path::PathBuf, sync::Arc, time::Duration};

use deltaweave_index::WatchService;
use serde_json::Value;
use tokio::sync::{Mutex, Notify, watch};

use crate::{
    config::SyncLoopConfig,
    state::{LifecycleState, Snapshot, WatchState},
};

/// One verified synchronization pass, provided by the sync layer.
pub trait SyncTask: Send + Sync + 'static {
    /// Executes one complete verified pass and returns its JSON report.
    fn sync_once(&self) -> impl Future<Output = anyhow::Result<Value>> + Send;
}

/// Operator-visible events emitted by the reusable loop for JSON presentation.
#[derive(Clone, Debug)]
pub enum SyncLoopEvent {
    /// Loop is about to run its first pass.
    Started {
        /// Native watch health after initialization.
        watch_state: WatchState,
        /// Watcher constructor error, if native watching is unavailable.
        watcher_error: Option<String>,
    },
    /// A verified pass completed.
    Success {
        /// JSON report from the injectable task.
        report: Value,
    },
    /// A pass failed and a retry was scheduled.
    Failure {
        /// Display representation of the failure.
        error: String,
        /// Delay before the next attempt.
        retry: Duration,
    },
    /// Native filesystem events woke the loop early.
    LocalChange {
        /// Number of coalesced native events.
        event_count: usize,
        /// Whether the watcher asked for a full rescan.
        rescan_required: bool,
    },
    /// The loop reached `Stopped`.
    Stopped,
}

enum PreparedWatch {
    Uninit,
    Disabled,
    Native(WatchService),
    Polling,
}

type EventHook = Arc<dyn Fn(SyncLoopEvent) + Send + Sync>;

/// Reusable loop core shared by the daemon and its command handlers.
pub struct SyncLoop<T> {
    task: Arc<T>,
    interval: Duration,
    debounce: Duration,
    maximum_debounce: Duration,
    maximum_backoff: Duration,
    watch_root: Option<PathBuf>,
    ignored_paths: Vec<PathBuf>,
    paused: Mutex<bool>,
    wake: Notify,
    watch: Mutex<PreparedWatch>,
    watcher_error: Mutex<Option<String>>,
    event_hook: Mutex<Option<EventHook>>,
}

impl<T: SyncTask> SyncLoop<T> {
    /// Creates a loop from validated daemon loop configuration.
    #[must_use]
    pub fn from_config(task: Arc<T>, config: &SyncLoopConfig) -> Self {
        Self {
            task,
            interval: config.interval,
            debounce: config.debounce,
            maximum_debounce: config.maximum_debounce,
            maximum_backoff: config.maximum_backoff,
            watch_root: config.watch_root.clone(),
            ignored_paths: config.ignored_paths.clone(),
            paused: Mutex::new(false),
            wake: Notify::new(),
            watch: Mutex::new(PreparedWatch::Uninit),
            watcher_error: Mutex::new(None),
            event_hook: Mutex::new(None),
        }
    }

    /// Installs a presentation hook used by the CLI `sync` command.
    pub async fn set_event_hook(&self, hook: impl Fn(SyncLoopEvent) + Send + Sync + 'static) {
        *self.event_hook.lock().await = Some(Arc::new(hook));
    }

    /// Interval between successful passes.
    #[must_use]
    pub const fn interval(&self) -> Duration {
        self.interval
    }

    /// Backoff cap applied after failures.
    #[must_use]
    pub const fn maximum_backoff(&self) -> Duration {
        self.maximum_backoff
    }

    /// Holds the loop between passes.
    pub async fn pause(&self) {
        *self.paused.lock().await = true;
        self.wake.notify_waiters();
    }

    /// Releases a pause and wakes the loop immediately.
    pub async fn resume(&self) {
        *self.paused.lock().await = false;
        self.wake.notify_waiters();
    }

    /// Whether the loop is currently holding passes.
    pub async fn is_paused(&self) -> bool {
        *self.paused.lock().await
    }

    /// Opens the native watcher once and returns the resulting watch state.
    pub async fn ensure_watch(&self) -> WatchState {
        let mut slot = self.watch.lock().await;
        match &*slot {
            PreparedWatch::Native(_) => return WatchState::Watching,
            PreparedWatch::Polling => return WatchState::PollingFallback,
            PreparedWatch::Disabled => return WatchState::Watching,
            PreparedWatch::Uninit => {}
        }
        let (prepared, error) = match &self.watch_root {
            None => (PreparedWatch::Disabled, None),
            Some(root) => match WatchService::new(
                root,
                &self.ignored_paths,
                self.debounce,
                self.maximum_debounce,
            ) {
                Ok(watcher) => (PreparedWatch::Native(watcher), None),
                Err(error) => (PreparedWatch::Polling, Some(error.to_string())),
            },
        };
        let state = match &prepared {
            PreparedWatch::Native(_) | PreparedWatch::Disabled => WatchState::Watching,
            PreparedWatch::Polling => WatchState::PollingFallback,
            PreparedWatch::Uninit => WatchState::Starting,
        };
        *self.watcher_error.lock().await = error;
        *slot = prepared;
        state
    }

    /// Watch health implied by the currently prepared watcher.
    pub async fn watch_state(&self) -> WatchState {
        match *self.watch.lock().await {
            PreparedWatch::Native(_) | PreparedWatch::Disabled => WatchState::Watching,
            PreparedWatch::Polling => WatchState::PollingFallback,
            PreparedWatch::Uninit => WatchState::Starting,
        }
    }

    /// Executes one pass without holding shared snapshot state.
    pub async fn run_once(&self) -> anyhow::Result<Value> {
        self.task.sync_once().await
    }

    /// Runs passes until shutdown, honouring pause, watch, debounce, and backoff.
    pub async fn run_forever(
        self: Arc<Self>,
        snapshot: Arc<Mutex<Snapshot>>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let watch_state = self.ensure_watch().await;
        let watcher_error = self.watcher_error.lock().await.clone();
        {
            let mut current = snapshot.lock().await;
            if current.state != LifecycleState::Stopping {
                current.watch_state = watch_state;
            }
        }
        self.emit(SyncLoopEvent::Started {
            watch_state,
            watcher_error,
        })
        .await;

        let mut backoff = Duration::from_secs(1).min(self.maximum_backoff);
        loop {
            if *shutdown.borrow() {
                break;
            }
            if self.is_paused().await {
                {
                    let mut current = snapshot.lock().await;
                    if current.state != LifecycleState::Stopping {
                        current.state = LifecycleState::Paused;
                        current.watch_state = WatchState::Paused;
                    }
                }
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { break; }
                    }
                    () = self.wake.notified() => {}
                }
                continue;
            }

            let outcome = self.run_once().await;
            if *shutdown.borrow() {
                break;
            }
            let stopping = snapshot.lock().await.state == LifecycleState::Stopping;
            if stopping {
                break;
            }
            let (delay, watch_for_local_changes) = match outcome {
                Ok(report) => {
                    self.emit(SyncLoopEvent::Success {
                        report: report.clone(),
                    })
                    .await;
                    let mut current = snapshot.lock().await;
                    if current.state == LifecycleState::Stopping {
                        break;
                    }
                    *current = current.clone().with_success(report);
                    backoff = Duration::from_secs(1).min(self.maximum_backoff);
                    (self.interval, true)
                }
                Err(error) => {
                    let delay = backoff;
                    let message = error.to_string();
                    self.emit(SyncLoopEvent::Failure {
                        error: message.clone(),
                        retry: delay,
                    })
                    .await;
                    let mut current = snapshot.lock().await;
                    if current.state == LifecycleState::Stopping {
                        break;
                    }
                    *current = current.clone().with_failure(message, delay);
                    backoff = backoff.saturating_mul(2).min(self.maximum_backoff);
                    (delay, false)
                }
            };

            if !self
                .wait_for_next(delay, watch_for_local_changes, &snapshot, &mut shutdown)
                .await
            {
                break;
            }
        }
        let mut current = snapshot.lock().await;
        current.state = LifecycleState::Stopped;
        current.watch_state = WatchState::Stopped;
        current.next_retry = None;
        current.next_retry_delay = None;
        drop(current);
        self.emit(SyncLoopEvent::Stopped).await;
    }

    async fn wait_for_next(
        &self,
        delay: Duration,
        watch_for_local_changes: bool,
        snapshot: &Arc<Mutex<Snapshot>>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> bool {
        let native = matches!(*self.watch.lock().await, PreparedWatch::Native(_));
        if !native || !watch_for_local_changes {
            let sleep = tokio::time::sleep(delay);
            tokio::pin!(sleep);
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { return false; }
                }
                () = self.wake.notified() => {}
                () = &mut sleep => {}
            }
            return !*shutdown.borrow();
        }

        let waiting_since = tokio::time::Instant::now();
        loop {
            if *shutdown.borrow() {
                return false;
            }
            let remaining = delay.saturating_sub(waiting_since.elapsed());
            if remaining.is_zero() {
                return true;
            }
            let slice = remaining.min(Duration::from_millis(100));
            let sleep = tokio::time::sleep(slice);
            tokio::pin!(sleep);
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { return false; }
                }
                () = self.wake.notified() => {
                    return !*shutdown.borrow();
                }
                () = &mut sleep => {}
            }
            if let Some((event_count, rescan_required)) = self.poll_watch().await {
                if rescan_required {
                    *self.watch.lock().await = PreparedWatch::Polling;
                    snapshot.lock().await.watch_state = WatchState::PollingFallback;
                }
                self.emit(SyncLoopEvent::LocalChange {
                    event_count,
                    rescan_required,
                })
                .await;
                return true;
            }
        }
    }

    async fn poll_watch(&self) -> Option<(usize, bool)> {
        let mut slot = self.watch.lock().await;
        let PreparedWatch::Native(watcher) = &mut *slot else {
            return None;
        };
        watcher
            .poll(std::time::Instant::now())
            .map(|trigger| (trigger.event_count, trigger.rescan_required))
    }

    async fn emit(&self, event: SyncLoopEvent) {
        let hook = self.event_hook.lock().await.clone();
        if let Some(hook) = hook {
            hook(event);
        }
    }
}
