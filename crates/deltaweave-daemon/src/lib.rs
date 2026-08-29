//! Single-owner daemon that owns indexes, stores, and access databases.

#![forbid(unsafe_code)]

mod config;
mod diagnostics;
mod instance;
mod ipc;
mod jobs;
mod pair;
mod preview;
mod runtime;

pub use config::{ConfigStore, Direction, JobConfig};
pub use diagnostics::{diagnostic_bundle_json, redact_diagnostics};
pub use instance::DaemonInstance;
#[cfg(windows)]
pub use ipc::serve_windows;
pub use ipc::{connect_and_hello, send_command};
#[cfg(unix)]
pub use ipc::{serve_unix, try_bind_unix, wait_until_exists};
pub use jobs::{JobSupervisor, ProgressCoalescer};
pub use pair::{PairingConfig, PairingService};
pub use preview::{list_conflicts, preview_snapshots, resolve_conflict};
pub use runtime::{default_data_dir, ipc_path, run};

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
            peer_address: None,
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
    async fn stop_command_replies_before_server_exits() {
        use deltaweave_daemon_api::{Command, CommandResult};

        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("dw.sock");
        let server = tokio::spawn(serve_unix(DaemonInstance::new(), socket.clone()));
        wait_until_exists(&socket).await;

        let result = send_command(&socket, Command::Stop).await.unwrap();
        assert_eq!(
            result,
            CommandResult::Accepted {
                id: "daemon".into()
            }
        );
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("server did not stop")
            .unwrap()
            .unwrap();
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn consumed_ticket_cannot_be_redeemed_twice_through_daemon() {
        use deltaweave_daemon_api::CommandResult;

        let server_dir = tempfile::tempdir().unwrap();
        let client_dir = tempfile::tempdir().unwrap();
        let dest = tempfile::tempdir().unwrap();
        let reserved = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let bind = reserved.local_addr().unwrap();
        drop(reserved);

        let server = PairingService::start(PairingConfig {
            state_root: server_dir.path().to_path_buf(),
            destination_root: dest.path().to_path_buf(),
            identity_path: server_dir.path().join("node.key"),
            bind: Some(bind),
        })
        .await
        .unwrap();
        let issued = server.issue_ticket(None).unwrap();
        let CommandResult::TicketIssued { code, .. } = issued else {
            panic!("expected TicketIssued, got {issued:?}");
        };
        assert!(code.starts_with("dwpair1:"));

        let client = PairingService::start(PairingConfig {
            state_root: client_dir.path().join("state"),
            destination_root: client_dir.path().join("dest"),
            identity_path: client_dir.path().join("node.key"),
            bind: None,
        })
        .await
        .unwrap();
        let first = client.redeem_ticket(&code).await.unwrap();
        match first {
            CommandResult::TicketRedeemed { outcome, .. } => {
                assert_eq!(outcome, "paired");
            }
            other => panic!("expected TicketRedeemed, got {other:?}"),
        }
        let second = client.redeem_ticket(&code).await;
        assert!(
            second.is_err(),
            "a consumed ticket cannot be redeemed twice"
        );
        server.shutdown().await.unwrap();
        client.shutdown().await.unwrap();
    }
}
