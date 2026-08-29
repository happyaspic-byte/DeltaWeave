//! Job handles, pause/cancel, and coalesced progress.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Result, bail};
use deltaweave_sync::SyncCancel;
use tokio::sync::mpsc::UnboundedSender;

/// Drops inner updates so subscribers see at most one sample per interval.
pub struct ProgressCoalescer<T> {
    interval: Duration,
    tx: UnboundedSender<T>,
    last_sent: Mutex<Option<Instant>>,
    pending: Mutex<Option<T>>,
    flush: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl<T: Clone + Send + 'static> ProgressCoalescer<T> {
    /// Creates a coalescer that publishes at most once per `interval`.
    #[must_use]
    pub fn new(interval: Duration, tx: UnboundedSender<T>) -> Arc<Self> {
        Arc::new(Self {
            interval,
            tx,
            last_sent: Mutex::new(None),
            pending: Mutex::new(None),
            flush: Mutex::new(None),
        })
    }

    /// Records a sample. Immediate if the interval has elapsed; otherwise the latest value is deferred.
    pub fn emit(self: &Arc<Self>, value: T) {
        let now = Instant::now();
        let mut last_sent = self.last_sent.lock().expect("progress lock");
        let due = last_sent.is_none_or(|previous| now.duration_since(previous) >= self.interval);
        if due {
            *last_sent = Some(now);
            drop(last_sent);
            let _ = self.tx.send(value);
            return;
        }
        drop(last_sent);
        *self.pending.lock().expect("progress lock") = Some(value);
        let mut flush = self.flush.lock().expect("progress lock");
        if flush.is_some() {
            return;
        }
        let this = Arc::clone(self);
        *flush = Some(tokio::spawn(async move {
            tokio::time::sleep(this.interval).await;
            this.flush_pending();
        }));
    }

    fn flush_pending(&self) {
        *self.flush.lock().expect("progress lock") = None;
        if let Some(value) = self.pending.lock().expect("progress lock").take() {
            *self.last_sent.lock().expect("progress lock") = Some(Instant::now());
            let _ = self.tx.send(value);
        }
    }
}

/// Runtime state for one job.
struct JobHandle {
    paused: bool,
    cancel: SyncCancel,
    running: bool,
}

/// Owns live job tasks. One handle per job id; never two indexes on one state root.
pub struct JobSupervisor {
    jobs: Mutex<HashMap<String, JobHandle>>,
}

impl Default for JobSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl JobSupervisor {
    /// Empty supervisor.
    #[must_use]
    pub fn new() -> Self {
        Self {
            jobs: Mutex::new(HashMap::new()),
        }
    }

    /// Registers a job if it is not already present.
    pub fn ensure_job(&self, id: &str) {
        let mut jobs = self.jobs.lock().expect("job lock");
        jobs.entry(id.to_owned()).or_insert_with(|| JobHandle {
            paused: false,
            cancel: SyncCancel::new(),
            running: false,
        });
    }

    /// Pauses the job and cancels the current pass at the next gate.
    pub fn pause(&self, id: &str) -> Result<()> {
        let mut jobs = self.jobs.lock().expect("job lock");
        let handle = jobs
            .get_mut(id)
            .ok_or_else(|| anyhow::anyhow!("unknown job"))?;
        handle.paused = true;
        handle.cancel.cancel();
        Ok(())
    }

    /// Clears the pause flag and installs a fresh cancel token.
    pub fn resume(&self, id: &str) -> Result<()> {
        let mut jobs = self.jobs.lock().expect("job lock");
        let handle = jobs
            .get_mut(id)
            .ok_or_else(|| anyhow::anyhow!("unknown job"))?;
        handle.paused = false;
        handle.cancel = SyncCancel::new();
        Ok(())
    }

    /// Cancels the current pass without changing paused.
    pub fn cancel(&self, id: &str) -> Result<()> {
        let jobs = self.jobs.lock().expect("job lock");
        let handle = jobs.get(id).ok_or_else(|| anyhow::anyhow!("unknown job"))?;
        handle.cancel.cancel();
        Ok(())
    }

    /// Returns a clone of the current cancel token.
    pub fn cancel_token(&self, id: &str) -> Result<SyncCancel> {
        let jobs = self.jobs.lock().expect("job lock");
        let handle = jobs.get(id).ok_or_else(|| anyhow::anyhow!("unknown job"))?;
        Ok(handle.cancel.clone())
    }

    /// Marks a sync pass as running, or errors if one is already in flight.
    pub fn begin_sync(&self, id: &str) -> Result<SyncCancel> {
        let mut jobs = self.jobs.lock().expect("job lock");
        let handle = jobs
            .get_mut(id)
            .ok_or_else(|| anyhow::anyhow!("unknown job"))?;
        if handle.paused {
            bail!("job is paused");
        }
        if handle.running {
            bail!("sync already running");
        }
        handle.running = true;
        Ok(handle.cancel.clone())
    }

    /// Clears the in-flight flag after a pass finishes.
    pub fn end_sync(&self, id: &str) {
        if let Some(handle) = self.jobs.lock().expect("job lock").get_mut(id) {
            handle.running = false;
        }
    }

    /// Whether the job is paused.
    pub fn is_paused(&self, id: &str) -> bool {
        self.jobs
            .lock()
            .expect("job lock")
            .get(id)
            .is_some_and(|handle| handle.paused)
    }
}
