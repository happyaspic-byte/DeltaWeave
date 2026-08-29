//! Single-owner daemon that owns indexes, stores, and access databases.

#![forbid(unsafe_code)]

mod config;
mod instance;
#[cfg(unix)]
mod ipc;
mod jobs;

pub use config::{ConfigStore, Direction, JobConfig};
pub use instance::DaemonInstance;
#[cfg(unix)]
pub use ipc::{connect_and_hello, serve_unix, try_bind_unix, wait_until_exists};
pub use jobs::{JobSupervisor, ProgressCoalescer};

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn rejects_second_job_on_same_canonical_root() {
        let dir = tempfile::tempdir().unwrap();
        let store = ConfigStore::open(dir.path().join("config.redb")).unwrap();
        let root = dir.path().join("sync");
        std::fs::create_dir_all(&root).unwrap();
        let job = JobConfig {
            id: "job-a".into(),
            name: "A".into(),
            local_root: root.clone(),
            state_root: dir.path().join("state-a"),
            peer_endpoint_id: "aa".repeat(32),
            direction: Direction::Bidirectional,
            continuous: true,
            paused: false,
        };
        store.insert_job(&job).unwrap();
        let mut job2 = job.clone();
        job2.id = "job-b".into();
        job2.state_root = dir.path().join("state-b");
        let err = store.insert_job(&job2).unwrap_err();
        assert!(format!("{err:#}").contains("already has a job"));
    }

    #[tokio::test]
    async fn second_bind_connects_to_existing_instance() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("dw.sock");
        let instance = DaemonInstance::new();
        let server = tokio::spawn(serve_unix(instance.clone(), socket.clone()));
        wait_until_exists(&socket).await;
        let hello = connect_and_hello(&socket).await.unwrap();
        assert_eq!(hello.instance_id, instance.instance_id);
        let err = try_bind_unix(&socket).unwrap_err();
        assert!(format!("{err:#}").contains("already running"));
        server.abort();
    }

    #[tokio::test]
    async fn progress_is_coalesced_to_ten_hertz() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let sink = ProgressCoalescer::new(Duration::from_millis(100), tx);
        let start = Instant::now();
        for i in 0..1000 {
            sink.emit(dummy_progress(i));
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
        let mut n = 0;
        while rx.try_recv().is_ok() {
            n += 1;
        }
        assert!(
            n <= 2,
            "got {n} events in {elapsed:?}",
            elapsed = start.elapsed()
        );
    }

    fn dummy_progress(i: u64) -> u64 {
        i
    }
}
