//! Single-owner daemon that owns indexes, stores, and access databases.

#![forbid(unsafe_code)]

mod config;

pub use config::{ConfigStore, Direction, JobConfig};

#[cfg(test)]
mod tests {
    use super::*;

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
}
