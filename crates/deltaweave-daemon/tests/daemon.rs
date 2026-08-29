use std::{fs, sync::Arc, time::Duration};

use deltaweave_daemon::{
    AuthToken, Command, ControlConfig, Daemon, DaemonConfig, DaemonLock, IpcClient, LifecycleState,
    MAX_FRAME_BYTES, Snapshot, SyncLoopConfig, SyncTask, WatchState, read_frame, write_frame,
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncWriteExt, duplex},
    sync::Mutex,
};

#[derive(Debug, Default)]
struct TestSync {
    calls: Mutex<u64>,
    fail_until: Mutex<u64>,
}

impl TestSync {
    async fn calls(&self) -> u64 {
        *self.calls.lock().await
    }
}

impl SyncTask for TestSync {
    async fn sync_once(&self) -> anyhow::Result<Value> {
        let mut calls = self.calls.lock().await;
        *calls += 1;
        let current = *calls;
        if current <= *self.fail_until.lock().await {
            anyhow::bail!("injected failure {current}");
        }
        Ok(json!({"status": "pass", "call": current}))
    }
}

fn config(directory: &std::path::Path) -> DaemonConfig {
    DaemonConfig {
        control: ControlConfig::new(directory),
        sync: SyncLoopConfig {
            interval: Duration::from_secs(60),
            debounce: Duration::from_millis(10),
            maximum_debounce: Duration::from_millis(50),
            maximum_backoff: Duration::from_millis(40),
            watch_root: None,
            ignored_paths: Vec::new(),
        },
        endpoint: Some("peer-endpoint".to_owned()),
    }
}

#[test]
fn config_rejects_invalid_intervals() {
    let temp = tempfile::tempdir().unwrap();
    let mut value = config(temp.path());
    value.sync.interval = Duration::ZERO;
    assert!(value.validate().is_err());
    value.sync.interval = Duration::from_secs(1);
    value.sync.debounce = Duration::ZERO;
    assert!(value.validate().is_err());
    value.sync.debounce = Duration::from_secs(2);
    value.sync.maximum_debounce = Duration::from_secs(1);
    assert!(value.validate().is_err());
}

#[test]
fn token_comparison_accepts_only_exact_owner_token() {
    let token = AuthToken::from_bytes([7; 32]);
    assert!(token.verify(&token.expose()));
    let mut different = token.expose();
    different[31] ^= 1;
    assert!(!token.verify(&different));
    assert!(!token.verify(&different[..31]));
}

#[tokio::test]
async fn frame_round_trip_and_malformed_json_rejection() {
    let (mut writer, mut reader) = duplex(1024);
    let request = json!({"token": "owner", "command": "status"});
    write_frame(&mut writer, &request).await.unwrap();
    assert_eq!(read_frame::<_, Value>(&mut reader).await.unwrap(), request);

    let (mut writer, mut reader) = duplex(1024);
    writer.write_u32(1).await.unwrap();
    writer.write_all(b"{").await.unwrap();
    assert!(read_frame::<_, Value>(&mut reader).await.is_err());
}

#[tokio::test]
async fn oversized_frame_is_rejected_before_allocation() {
    let (mut writer, mut reader) = duplex(16);
    writer
        .write_u32((MAX_FRAME_BYTES + 1) as u32)
        .await
        .unwrap();
    let error = read_frame::<_, Value>(&mut reader).await.unwrap_err();
    assert!(error.to_string().contains("exceeds"));
}

#[test]
fn lock_is_exclusive_and_recovers_stale_pid() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("daemon.lock");
    let first = DaemonLock::acquire(&path).unwrap();
    assert!(DaemonLock::acquire(&path).is_err());
    drop(first);
    fs::write(&path, "4294967295\n").unwrap();
    let recovered = DaemonLock::acquire(&path).unwrap();
    assert_eq!(recovered.path(), path);
}

#[tokio::test(start_paused = true)]
async fn lifecycle_pause_resume_retry_and_graceful_stop() {
    let temp = tempfile::tempdir().unwrap();
    let task = Arc::new(TestSync::default());
    *task.fail_until.lock().await = 1;
    let daemon = Arc::new(Daemon::new(config(temp.path()), Arc::clone(&task)).unwrap());
    assert_eq!(daemon.snapshot().await.state, LifecycleState::Starting);
    let running = Arc::clone(&daemon).spawn().await.unwrap();
    tokio::task::yield_now().await;
    assert_eq!(daemon.snapshot().await.state, LifecycleState::Retrying);
    assert!(daemon.snapshot().await.next_retry.is_some());

    daemon.execute(Command::Pause).await.unwrap();
    let paused_calls = task.calls().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(task.calls().await, paused_calls);
    assert_eq!(daemon.snapshot().await.state, LifecycleState::Paused);

    daemon.execute(Command::Resume).await.unwrap();
    tokio::task::yield_now().await;
    assert!(task.calls().await > paused_calls);
    assert_eq!(daemon.snapshot().await.state, LifecycleState::Running);

    daemon.execute(Command::Stop).await.unwrap();
    assert_eq!(daemon.snapshot().await.state, LifecycleState::Stopping);
    running.wait().await.unwrap();
    assert_eq!(daemon.snapshot().await.state, LifecycleState::Stopped);
}

#[cfg(unix)]
#[tokio::test]
async fn unix_ipc_is_owner_only_authenticated_and_not_replaced() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().unwrap();
    let daemon = Arc::new(Daemon::new(config(temp.path()), Arc::new(TestSync::default())).unwrap());
    daemon.start().await.unwrap();
    let token = AuthToken::from_bytes([9; 32]);
    let server = Arc::clone(&daemon).spawn_ipc(token.clone()).await.unwrap();
    let mode = fs::metadata(server.endpoint())
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);

    let client = IpcClient::new(server.endpoint().to_owned(), token.clone());
    let response = client.send(Command::Status).await.unwrap();
    assert_eq!(response.snapshot.unwrap().state, LifecycleState::Running);

    let bad = IpcClient::new(server.endpoint().to_owned(), AuthToken::from_bytes([1; 32]));
    let rejected = bad.send(Command::Status).await.unwrap();
    assert!(!rejected.ok);
    assert!(rejected.snapshot.is_none());

    assert!(Arc::clone(&daemon).spawn_ipc(token).await.is_err());
    server.shutdown().await.unwrap();
}

#[test]
fn snapshot_is_serializable_without_secrets() {
    let snapshot = Snapshot::default();
    let encoded = serde_json::to_string(&snapshot).unwrap();
    assert!(!encoded.contains("token"));
    assert!(encoded.contains("\"watch_state\":\"starting\""));
    let watching = Snapshot::default().with_watch_state(WatchState::Watching);
    assert_eq!(watching.watch_state, WatchState::Watching);
}

#[cfg(unix)]
#[test]
fn token_file_is_created_owner_only() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("owner.token");
    let token = AuthToken::load_or_create(&path).unwrap();
    assert_eq!(token.expose().len(), 32);
    assert_eq!(
        fs::metadata(path).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[cfg(unix)]
#[test]
fn token_file_rejects_group_or_world_access() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("owner.token");
    let token = AuthToken::load_or_create(&path).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
    assert!(AuthToken::load_or_create(&path).is_err());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(AuthToken::load_or_create(&path).unwrap(), token);
}

#[cfg(unix)]
#[tokio::test]
async fn stale_unix_endpoint_is_replaced() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("control.sock");
    fs::write(&path, b"stale").unwrap();
    let daemon = Arc::new(Daemon::new(config(temp.path()), Arc::new(TestSync::default())).unwrap());
    daemon.start().await.unwrap();
    let token = AuthToken::from_bytes([3; 32]);
    let server = Arc::clone(&daemon).spawn_ipc(token.clone()).await.unwrap();
    assert_eq!(server.endpoint(), path.as_path());
    let client = IpcClient::new(path, token);
    let response = client.send(Command::Status).await.unwrap();
    assert!(response.ok);
    server.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn ipc_commands_pause_resume_and_stop() {
    let temp = tempfile::tempdir().unwrap();
    let daemon = Arc::new(Daemon::new(config(temp.path()), Arc::new(TestSync::default())).unwrap());
    daemon.start().await.unwrap();
    let token = AuthToken::from_bytes([4; 32]);
    let server = Arc::clone(&daemon).spawn_ipc(token.clone()).await.unwrap();
    let client = IpcClient::new(server.endpoint().to_owned(), token);

    let paused = client.send(Command::Pause).await.unwrap();
    assert_eq!(paused.snapshot.unwrap().state, LifecycleState::Paused);
    let resumed = client.send(Command::Resume).await.unwrap();
    assert_eq!(resumed.snapshot.unwrap().state, LifecycleState::Running);
    let stopped = client.send(Command::Stop).await.unwrap();
    assert_eq!(stopped.snapshot.unwrap().state, LifecycleState::Stopping);
    server.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn ipc_malformed_frame_is_rejected() {
    use tokio::net::UnixStream;

    let temp = tempfile::tempdir().unwrap();
    let daemon = Arc::new(Daemon::new(config(temp.path()), Arc::new(TestSync::default())).unwrap());
    daemon.start().await.unwrap();
    let server = Arc::clone(&daemon)
        .spawn_ipc(AuthToken::from_bytes([5; 32]))
        .await
        .unwrap();

    let mut stream = UnixStream::connect(server.endpoint()).await.unwrap();
    stream.write_u32(1).await.unwrap();
    stream.write_all(b"{").await.unwrap();
    stream.flush().await.unwrap();
    let rejected: deltaweave_daemon::CommandResponse = read_frame(&mut stream).await.unwrap();
    assert!(!rejected.ok);
    assert!(rejected.snapshot.is_none());

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_watch_wakes_sync_before_interval() {
    let temp = tempfile::tempdir().unwrap();
    let watch_root = temp.path().join("watched");
    fs::create_dir_all(&watch_root).unwrap();
    let task = Arc::new(TestSync::default());
    let mut daemon_config = config(temp.path());
    daemon_config.sync.watch_root = Some(watch_root.clone());
    daemon_config.sync.debounce = Duration::from_millis(20);
    daemon_config.sync.maximum_debounce = Duration::from_millis(100);
    let daemon = Arc::new(Daemon::new(daemon_config, Arc::clone(&task)).unwrap());
    let running = Arc::clone(&daemon).spawn().await.unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        while task.calls().await < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    fs::write(watch_root.join("change.txt"), b"wake").unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while task.calls().await < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(daemon.snapshot().await.watch_state, WatchState::Watching);

    daemon.execute(Command::Stop).await.unwrap();
    running.wait().await.unwrap();
}

#[tokio::test]
async fn unavailable_native_watch_uses_polling_fallback() {
    let temp = tempfile::tempdir().unwrap();
    let mut daemon_config = config(temp.path());
    daemon_config.sync.watch_root = Some(temp.path().join("missing"));
    let daemon = Daemon::new(daemon_config, Arc::new(TestSync::default())).unwrap();

    daemon.start().await.unwrap();

    assert_eq!(
        daemon.snapshot().await.watch_state,
        WatchState::PollingFallback
    );
}
