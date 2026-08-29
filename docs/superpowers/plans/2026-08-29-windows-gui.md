# Windows GUI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a light-only Windows Tauri 2 GUI plus a single-owner Rust daemon so this PC can add folders, pair, sync, pause, and resolve conflicts without opening redb from more than one process.

**Architecture:** `deltaweave-daemon` is the only process that opens indexes, stores, and access DBs. `deltaweave-daemon-api` defines versioned JSON commands/events. Tauri GUI and later CLI talk over a per-user named pipe (Windows) or Unix socket (Linux tests). Existing `deltaweave-sync` / `deltaweave-net` remain the transfer authorities; the GUI never hashes, never opens redb, and never talks iroh.

**Tech Stack:** Rust 1.91, edition 2024, `#![forbid(unsafe_code)]`, serde JSON IPC, redb config DB, tokio, iroh 1.1, Tauri 2 (`tray-icon`), `@tauri-apps/api/tray`, `tauri-plugin-autostart` 2, `tauri-plugin-shell` sidecar, NSIS current-user installer.

**Spec:** `docs/superpowers/specs/2026-08-29-windows-gui-design.md`

## Global Constraints

- Light theme only. No dark CSS, no `prefers-color-scheme` dark palette.
- GUI never opens a redb database.
- One daemon process per Windows user; second invocation attaches.
- Pairing uses existing `dwpair1` tickets, 10-minute default TTL, one successful redemption.
- No automatic node-key rotation; replacement is revoke-then-reissue.
- Closing the window must not stop the daemon.
- Events coalesced to ≤10 Hz per job.
- Credentials, ticket secrets, and private keys never appear in events, logs, or diagnostic bundles.
- Do not invent methods; extend existing `SyncEngine`, `AccessStore`, `ServerConfig.bind`.
- Preserve uncommitted `ServerConfig.bind` / CLI `--bind` work on this branch; commit it in Task 0 before GUI crates land.
- Do not store SSH passwords in repo, logs, or commits.
- Windows 10/11 x86-64 is the first GUI target; Linux remains the daemon unit-test host.
- `unsafe_code = forbid` remains workspace-wide.

---

## File Structure

- `crates/deltaweave-cli/src/main.rs` — keep `--bind` and pair commands; later add `daemon` subcommand.
- `crates/deltaweave-net/src/lib.rs` — keep `ServerConfig.bind`; pairing stays here.
- `crates/deltaweave-sync/src/lib.rs` — add `SyncProgress` + cooperative `SyncCancel`.
- Create: `crates/deltaweave-daemon-api/` — protocol types only.
- Create: `crates/deltaweave-daemon/` — config, jobs, IPC server, Windows lifecycle.
- Create: `apps/deltaweave-gui/` — Tauri 2 app (`src-tauri` + Vite/TypeScript UI).
- Create: `apps/deltaweave-gui/src-tauri/binaries/` — sidecar naming for `deltaweave-daemon`.
- Modify: `Cargo.toml` workspace members.
- Modify: `.github/workflows/release.yml` — Windows NSIS + daemon sidecar.

---

### Task 0: Commit the pairing fixed-bind work already on this branch

**Files:**
- Modify: `crates/deltaweave-net/src/lib.rs`
- Modify: `crates/deltaweave-cli/src/main.rs`
- Modify: `crates/deltaweave-sync/src/lib.rs`
- Modify: `crates/deltaweave-sync/tests/chaos.rs`
- Modify: `.gitignore` (keep `.superpowers/`)

**Interfaces:**
- Consumes: existing uncommitted `ServerConfig.bind: Option<SocketAddr>`
- Produces: committed baseline so GUI work does not mix with pairing-port diffs

- [ ] **Step 1: Confirm the bind test exists**

```text
rg -n "bound_port_survives_restart_so_issued_ticket_still_redeems" crates/deltaweave-net/src/lib.rs
```

Expected: one match.

- [ ] **Step 2: Format, clippy, and test the net/cli packages**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p deltaweave-net bound_port_survives_restart_so_issued_ticket_still_redeems -- --nocapture
cargo test -p deltaweave --lib
```

Expected: all pass. Do not claim the pairing-port fix is done if the named test is missing or fails.

- [ ] **Step 3: Commit only pairing-bind and gitignore**

```bash
git add .gitignore crates/deltaweave-net/src/lib.rs crates/deltaweave-cli/src/main.rs crates/deltaweave-sync/src/lib.rs crates/deltaweave-sync/tests/chaos.rs
git commit -m "$(cat <<'EOF'
feat: keep pairing UDP bind across serve restarts

Tickets encode a concrete host:port. Restarting serve without --bind
invalidated outstanding tickets; ServerConfig.bind and CLI --bind keep
the advertised address stable.

Co-Authored-By: Claude <noreply@anthropic.com>
EOF
)"
```

Do not add `docs/superpowers/` in this commit.

---

### Task 1: Daemon API crate — versioned envelope

**Files:**
- Modify: `Cargo.toml` workspace `members`
- Create: `crates/deltaweave-daemon-api/Cargo.toml`
- Create: `crates/deltaweave-daemon-api/src/lib.rs`
- Test: same `lib.rs` `#[cfg(test)]` module

**Interfaces:**
- Consumes: none
- Produces:
  - `pub const PROTOCOL_VERSION_MAJOR: u16 = 1;`
  - `pub const PROTOCOL_VERSION_MINOR: u16 = 0;`
  - `pub struct Request { pub protocol_version: ProtocolVersion, pub request_id: String, pub command: Command }`
  - `pub struct Response { pub request_id: String, pub result: Result<CommandResult, ErrorBody> }`
  - `pub struct Event { pub event_id: u64, pub state_revision: u64, pub body: EventBody }`
  - `Command::Hello`, `CommandResult::Hello { protocol_version, instance_id }`
  - `ErrorBody { code: ErrorCode, message: String }`
  - `ErrorCode::UpgradeRequired | Unauthorized | InvalidRequest | Internal`

- [ ] **Step 1: Write the failing test first**

Add crate files with only the test compiling against missing types, then run:

```rust
#[test]
fn hello_round_trips_and_rejects_incompatible_major() {
    let request = Request {
        protocol_version: ProtocolVersion { major: 1, minor: 0 },
        request_id: "r1".into(),
        command: Command::Hello,
    };
    let json = serde_json::to_string(&request).unwrap();
    let back: Request = serde_json::from_str(&json).unwrap();
    assert!(matches!(back.command, Command::Hello));

    let too_new = ProtocolVersion { major: 2, minor: 0 };
    assert_eq!(too_new.negotiate(1, 0).unwrap_err().code, ErrorCode::UpgradeRequired);
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p deltaweave-daemon-api hello_round_trips_and_rejects_incompatible_major
```

Expected: FAIL compiling `Request` / `ProtocolVersion`.

- [ ] **Step 3: Write minimal types**

```rust
#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION_MAJOR: u16 = 1;
pub const PROTOCOL_VERSION_MINOR: u16 = 0;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProtocolVersion {
    pub major: u16,
    pub minor: u16,
}

impl ProtocolVersion {
    pub fn negotiate(self, server_major: u16, server_minor: u16) -> Result<Self, ErrorBody> {
        if self.major != server_major {
            return Err(ErrorBody {
                code: ErrorCode::UpgradeRequired,
                message: "incompatible daemon protocol".into(),
            });
        }
        Ok(Self {
            major: server_major,
            minor: server_minor.min(self.minor),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Request {
    pub protocol_version: ProtocolVersion,
    pub request_id: String,
    pub command: Command,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    Hello,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Response {
    pub request_id: String,
    pub result: Result<CommandResult, ErrorBody>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CommandResult {
    Hello {
        protocol_version: ProtocolVersion,
        instance_id: String,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    UpgradeRequired,
    Unauthorized,
    InvalidRequest,
    Internal,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ErrorBody {
    pub code: ErrorCode,
    pub message: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Event {
    pub event_id: u64,
    pub state_revision: u64,
    pub body: EventBody,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventBody {
    DaemonReady { instance_id: String },
}
```

Workspace `Cargo.toml` members add `"crates/deltaweave-daemon-api"`. Crate depends on `serde` with `derive` and `serde_json` as a normal dependency (tests need it; production IPC uses it later).

- [ ] **Step 4: Run test to verify it passes**

```bash
cargo test -p deltaweave-daemon-api
cargo fmt --all -- --check
```

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/deltaweave-daemon-api
git commit -m "feat: add versioned daemon IPC envelope"
```

---

### Task 2: SyncEngine progress and cooperative cancel

**Files:**
- Modify: `crates/deltaweave-sync/src/lib.rs`
- Modify: `crates/deltaweave-sync/Cargo.toml` if a new test helper is needed
- Test: `crates/deltaweave-sync/src/lib.rs` `#[cfg(test)]` or `crates/deltaweave-sync/tests/progress.rs`

**Interfaces:**
- Consumes: existing `SyncEngine::sync_once(&self) -> Result<SyncReport>`
- Produces:
  - `pub struct SyncCancel(tokio::sync::watch::Sender<bool>);` with `fn cancel(&self)` and `fn is_cancelled(&self) -> bool`
  - `pub struct SyncProgress { pub phase: SyncPhase, pub current_path: Option<String>, pub pulled_bytes: u64, pub pushed_bytes: u64, pub reused_extents: usize }`
  - `pub enum SyncPhase { Scan, FetchRemote, Stage, Apply, Verify }`
  - `pub async fn sync_once_with(&self, progress: Option<ProgressSink>, cancel: Option<SyncCancel>) -> Result<SyncReport>`
  - `pub type ProgressSink = Arc<dyn Fn(SyncProgress) + Send + Sync>;`
  - existing `sync_once` calls `sync_once_with(None, None)`

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn cancelled_sync_returns_before_apply_and_is_safe_to_retry() {
    let cancel = SyncCancel::new();
    cancel.cancel();
    let engine = open_temp_engine(); // copy the existing test helper that builds SyncEngine against a local server
    let err = engine
        .sync_once_with(None, Some(cancel))
        .await
        .expect_err("cancelled before work");
    assert!(format!("{err:#}").contains("cancelled"));
}
```

If no local-server helper exists in-crate, put the test next to the existing `deltaweave-sync` integration test that already starts `start_server`, and cancel immediately after `open`.

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p deltaweave-sync cancelled_sync_returns_before_apply_and_is_safe_to_retry
```

Expected: FAIL, `sync_once_with` / `SyncCancel` not found.

- [ ] **Step 3: Minimal implementation**

```rust
#[derive(Clone, Debug)]
pub struct SyncCancel {
    inner: tokio::sync::watch::Sender<bool>,
}

impl SyncCancel {
    #[must_use]
    pub fn new() -> Self {
        let (inner, _) = tokio::sync::watch::channel(false);
        Self { inner }
    }
    pub fn cancel(&self) {
        let _ = self.inner.send(true);
    }
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        *self.inner.borrow()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncPhase {
    Scan,
    FetchRemote,
    Stage,
    Apply,
    Verify,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct SyncProgress {
    pub phase: SyncPhase,
    pub current_path: Option<String>,
    pub pulled_bytes: u64,
    pub pushed_bytes: u64,
    pub reused_extents: usize,
}

pub type ProgressSink = std::sync::Arc<dyn Fn(SyncProgress) + Send + Sync>;

fn throw_if_cancelled(cancel: Option<&SyncCancel>) -> Result<()> {
    if cancel.is_some_and(SyncCancel::is_cancelled) {
        bail!("sync cancelled");
    }
    Ok(())
}
```

Call `throw_if_cancelled` at the start of `sync_once_with` and before stage/apply/verify. Emit progress at those same points. Do not interrupt an in-flight `apply_local` / metadata commit; cancellation is checked at those four gates only.

Keep `pub async fn sync_once(&self) -> Result<SyncReport> { self.sync_once_with(None, None).await }`.

- [ ] **Step 4: Run tests**

```bash
cargo test -p deltaweave-sync
```

Expected: PASS, including existing sync tests.

- [ ] **Step 5: Commit**

```bash
git add crates/deltaweave-sync
git commit -m "feat: add sync progress snapshots and cooperative cancel"
```

---

### Task 3: Daemon configuration store

**Files:**
- Create: `crates/deltaweave-daemon/Cargo.toml`
- Create: `crates/deltaweave-daemon/src/lib.rs`
- Create: `crates/deltaweave-daemon/src/config.rs`
- Modify: workspace `Cargo.toml` members

**Interfaces:**
- Consumes: none from Task 1 except later job IDs as `String`
- Produces:
  - `pub struct ConfigStore`
  - `pub fn ConfigStore::open(path: impl AsRef<Path>) -> Result<Self>`
  - `pub struct JobConfig { id: String, name: String, local_root: PathBuf, state_root: PathBuf, peer_endpoint_id: String, direction: Direction, continuous: bool, paused: bool }`
  - `pub enum Direction { Bidirectional, SendOnly, ReceiveOnly }`
  - `pub fn insert_job(&self, job: &JobConfig) -> Result<()>`
  - `pub fn list_jobs(&self) -> Result<Vec<JobConfig>>`
  - rejects overlapping `local_root`/`state_root` and two enabled jobs on the same canonical `local_root`

- [ ] **Step 1: Failing test**

```rust
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
```

- [ ] **Step 2: Run to verify fail**

```bash
cargo test -p deltaweave-daemon rejects_second_job_on_same_canonical_root
```

- [ ] **Step 3: Minimal ConfigStore**

Use redb with tables `meta` (schema version `1`) and `jobs` (key = job id, value = postcard `JobConfig`). Canonicalize roots before insert. `state_root` must not overlap `local_root`. Identity path checks stay at job-start time, not here.

Crate deps: `anyhow`, `postcard`, `redb`, `serde`, `thiserror`, `tempfile` (dev).

- [ ] **Step 4: Tests pass**

```bash
cargo test -p deltaweave-daemon
```

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/deltaweave-daemon
git commit -m "feat: add daemon job configuration store"
```

---

### Task 4: Per-user IPC listener and Hello

**Files:**
- Create: `crates/deltaweave-daemon/src/ipc.rs`
- Create: `crates/deltaweave-daemon/src/instance.rs`
- Modify: `crates/deltaweave-daemon/src/lib.rs`

**Interfaces:**
- Consumes: Task 1 `Request`/`Response`/`Command::Hello`
- Produces:
  - `pub struct DaemonInstance { pub instance_id: String }`
  - `pub async fn serve_ipc(instance: DaemonInstance, listener: impl IpcListener) -> Result<()>`
  - Windows: named pipe `\\.\pipe\deltaweave-<sid>` with current-user DACL
  - Non-Windows tests: Unix socket under a temp dir
  - Second `try_bind` fails with `already running`; client `connect` then `Hello` succeeds against the first instance

- [ ] **Step 1: Failing test**

```rust
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
```

- [ ] **Step 2: Run to verify fail**

```bash
cargo test -p deltaweave-daemon second_bind_connects_to_existing_instance
```

- [ ] **Step 3: Minimal server**

Length-delimited JSON: 4-byte little-endian length + UTF-8 JSON, max 1 MiB. On `Hello`, negotiate version and return `CommandResult::Hello`. Unknown major → `ErrorCode::UpgradeRequired`. Do not implement job commands yet.

- [ ] **Step 4: Tests pass**

```bash
cargo test -p deltaweave-daemon
```

- [ ] **Step 5: Commit**

```bash
git add crates/deltaweave-daemon
git commit -m "feat: add per-user daemon IPC hello and single-instance bind"
```

---

### Task 5: Job supervisor with pause, sync-now, and 10 Hz progress

**Files:**
- Create: `crates/deltaweave-daemon/src/jobs.rs`
- Modify: `crates/deltaweave-daemon-api/src/lib.rs` add job commands/events
- Modify: `crates/deltaweave-daemon/src/ipc.rs` dispatch

**Interfaces:**
- Consumes: `ConfigStore`, `SyncEngine::sync_once_with`, `SyncCancel`, `SyncProgress`
- Produces API:
  - `Command::ListJobs`
  - `Command::CreateJob { name, local_root, peer_endpoint_id, direction }`
  - `Command::PauseJob { id }` / `Command::ResumeJob { id }` / `Command::SyncNow { id }` / `Command::CancelJob { id }`
  - `EventBody::JobProgress { id, phase, current_path, pulled_bytes, pushed_bytes, reused_extents, bytes_per_second, eta_seconds }`
  - `EventBody::JobState { id, severity, summary }` where `severity` is `normal | attention | action_required`
- Coalesce progress so a test that emits 1000 inner updates publishes ≤10 events in any 1-second window.

- [ ] **Step 1: Failing test**

```rust
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
    while rx.try_recv().is_ok() { n += 1; }
    assert!(n <= 2, "got {n} events in {elapsed:?}", elapsed = start.elapsed());
}
```

- [ ] **Step 2: Run to verify fail**

```bash
cargo test -p deltaweave-daemon progress_is_coalesced_to_ten_hertz
```

- [ ] **Step 3: Implement coalescer + JobSupervisor**

`JobSupervisor` holds `HashMap<String, JobHandle>`. Each handle has `paused: bool`, `cancel: SyncCancel`, and a worker task. `PauseJob` sets paused and calls `cancel` so the current pass stops at the next gate. `SyncNow` runs one `sync_once_with` if not already running. Do not open a second `LocalIndex` for the same state root.

Extend API enums with the commands above; keep serde tag `type` / `snake_case`.

- [ ] **Step 4: Tests pass**

```bash
cargo test -p deltaweave-daemon-api
cargo test -p deltaweave-daemon
```

- [ ] **Step 5: Commit**

```bash
git add crates/deltaweave-daemon-api crates/deltaweave-daemon
git commit -m "feat: supervise jobs with pause, cancel, and coalesced progress"
```

---

### Task 6: Pairing, preview, and conflict commands on the daemon

**Files:**
- Modify: `crates/deltaweave-daemon/src/pair.rs` (create)
- Modify: `crates/deltaweave-daemon/src/preview.rs` (create)
- Modify: `crates/deltaweave-daemon-api/src/lib.rs`
- Modify: `crates/deltaweave-net` only if a public wrapper is required; prefer calling `AccessStore::issue_ticket`, `redeem_pairing_ticket`, `AccessStore::revoke` from the daemon

**Interfaces:**
- Consumes: `PairingTicket::to_code` / `from_code`, `issue_ticket`, `redeem_pairing_ticket`, `ServerConfig.bind`
- Produces:
  - `Command::IssueTicket { ttl_seconds: Option<u64> }` → `{ code, expires_at, server_endpoint_id, server_fingerprint }`
  - `Command::RedeemTicket { code }` → `{ outcome, peer_endpoint_id, peer_fingerprint }`
  - `Command::RevokePeer { endpoint_id }`
  - `Command::PreviewJob { id }` → `{ sends, receives, deletes, conflicts }` without applying
  - `Command::ListConflicts { id }` / `Command::ResolveConflict { id, path, action }` where `action` is `keep_local | keep_remote | keep_both`
- Ticket default TTL 600 seconds, `uses_remaining = 1` via existing `issue_ticket`.
- Events never include `code` or ticket secrets.

- [ ] **Step 1: Failing tests**

```rust
#[test]
fn ticket_response_omits_secret_bytes() {
    let json = serde_json::to_string(&CommandResult::TicketIssued {
        code: "dwpair1:ab".into(),
        expires_at: 1,
        server_endpoint_id: "aa".repeat(32),
        server_fingerprint: "abcd".into(),
    }).unwrap();
    assert!(!json.contains("secret"));
}

#[tokio::test]
async fn consumed_ticket_cannot_be_redeemed_twice_through_daemon() {
    // start daemon with temp access store + bound server
    // IssueTicket, RedeemTicket once -> Paired, second RedeemTicket errors
}
```

- [ ] **Step 2: Run to verify fail**

```bash
cargo test -p deltaweave-daemon-api ticket_response_omits_secret_bytes
cargo test -p deltaweave-daemon consumed_ticket_cannot_be_redeemed_twice_through_daemon
```

- [ ] **Step 3: Implement daemon pairing**

Issue tickets using the daemon's live `Server::address_info()` direct address, never a stale CLI flag. If no direct address is bound, fail `IssueTicket` with `InvalidRequest` "server has no direct address". Redeem calls `redeem_pairing_ticket`. Revoke calls `AccessStore::revoke` and marks jobs for that peer `action_required`.

Preview: run scan + remote snapshot + `merge_snapshots` / `actions_to_reach` without `apply_local` / `apply_remote`. Reuse reconcile functions; do not duplicate merge rules.

Conflict resolve: map UI actions onto existing conflict copies. `keep_local` / `keep_remote` replace the canonical path with the chosen verified hash after both contents are in CAS. `keep_both` leaves the portable `.conflict-<hash>` copy. Do not auto-overwrite.

- [ ] **Step 4: Tests pass**

```bash
cargo test -p deltaweave-daemon-api
cargo test -p deltaweave-daemon
cargo test -p deltaweave-net
```

- [ ] **Step 5: Commit**

```bash
git add crates/deltaweave-daemon-api crates/deltaweave-daemon
git commit -m "feat: expose pairing, preview, and conflict resolve on the daemon"
```

---

### Task 7: `deltaweave daemon` binary and CLI attach

**Files:**
- Create: `crates/deltaweave-daemon/src/main.rs`
- Modify: `crates/deltaweave-daemon/Cargo.toml` `[[bin]] name = "deltaweave-daemon"`
- Modify: `crates/deltaweave-cli/src/main.rs` add `Daemon` subcommand `run | status`
- Modify: `crates/deltaweave-cli/Cargo.toml` depend on `deltaweave-daemon-api` and a small IPC client module shared from daemon crate (`deltaweave-daemon` lib)

**Interfaces:**
- Consumes: `serve_ipc`, `ConfigStore::open`
- Produces:
  - `deltaweave-daemon` listens, prints one JSON ready line `{ "status": "ready", "instance_id": ... }` to stdout, then serves
  - `deltaweave daemon status` connects and prints Hello result
  - data dir: Windows `%LOCALAPPDATA%\DeltaWeave\`, Unix `$XDG_STATE_HOME/deltaweave` or `~/.local/state/deltaweave`

- [ ] **Step 1: Failing CLI parse test** in `crates/deltaweave-cli/src/main.rs` tests:

```rust
#[test]
fn daemon_status_parses() {
    let cli = Cli::try_parse_from(["deltaweave", "daemon", "status"]).unwrap();
    assert!(matches!(cli.command, Command::Daemon(DaemonArgs { command: DaemonCommand::Status })));
}
```

- [ ] **Step 2: Run to verify fail**

```bash
cargo test -p deltaweave daemon_status_parses
```

- [ ] **Step 3: Implement binary + subcommand**

`deltaweave daemon run` execs the same `run()` as `deltaweave-daemon`. Status is attach-only.

- [ ] **Step 4: Tests pass**

```bash
cargo test -p deltaweave
cargo test -p deltaweave-daemon
```

- [ ] **Step 5: Commit**

```bash
git add crates/deltaweave-daemon crates/deltaweave-cli
git commit -m "feat: add deltaweave-daemon binary and CLI status attach"
```

---

### Task 8: Tauri 2 shell, tray, light window

**Files:**
- Create: `apps/deltaweave-gui/package.json`
- Create: `apps/deltaweave-gui/src-tauri/Cargo.toml` (package `deltaweave-gui`)
- Create: `apps/deltaweave-gui/src-tauri/tauri.conf.json`
- Create: `apps/deltaweave-gui/src-tauri/src/lib.rs`
- Create: `apps/deltaweave-gui/src-tauri/capabilities/default.json`
- Create: `apps/deltaweave-gui/ui/index.html`
- Create: `apps/deltaweave-gui/ui/src/main.ts`
- Create: `apps/deltaweave-gui/ui/src/styles.css`
- Modify: workspace members to include `apps/deltaweave-gui/src-tauri`

**Interfaces:**
- Consumes: daemon IPC client
- Produces:
  - window title `DeltaWeave`
  - light CSS only (`background: #f3f5f7; color: #1b2430`)
  - tray via `tauri = { version = "2", features = ["tray-icon"] }` and `TrayIcon::new`
  - tray menu: Open DeltaWeave, Sync all now, Pause all, Quit Sync Engine
  - close-to-tray: `on_window_event` hide on CloseRequested, do not exit
  - Quit Sync Engine sends `Command::Stop` then `app.exit`
  - sidecar: `bundle.externalBin = ["binaries/deltaweave-daemon"]` and `shell:allow-spawn` for that name
  - autostart: `tauri-plugin-autostart` `Builder::new().args(["--hidden"]).build()` plus capabilities `autostart:allow-enable`, `allow-disable`, `allow-is-enabled`

- [ ] **Step 1: Failing UI unit test** (Vitest, no Tauri runtime):

```ts
import { describe, it, expect } from "vitest";
import { renderDashboard } from "./dashboard";

describe("dashboard", () => {
  it("shows transfer and conflict panes together", () => {
    const html = renderDashboard({
      bytesPerSecond: 86_000_000,
      currentPath: "VMware.iso",
      percent: 62,
      conflicts: [{ path: "VMware.iso", localNewer: true }],
    });
    expect(html).toContain("충돌");
    expect(html).toContain("86");
    expect(html.toLowerCase()).not.toContain("prefers-color-scheme: dark");
  });
});
```

- [ ] **Step 2: Run to verify fail**

```bash
cd apps/deltaweave-gui && npm test
```

Expected: FAIL missing module.

- [ ] **Step 3: Minimal light dashboard + tray bootstrap**

Do not add a dark stylesheet. Window starts hidden when argv contains `--hidden`. Sidecar spawn happens in `setup` if Hello connect fails.

`tauri.conf.json` windows NSIS `installMode: currentUser`. Product name `DeltaWeave`. Identifier `byte.happyaspic.deltaweave`.

- [ ] **Step 4: Tests pass**

```bash
cd apps/deltaweave-gui && npm test
cargo test -p deltaweave-gui
```

GUI crate tests can be empty except compile. Frontend tests must pass.

- [ ] **Step 5: Commit**

```bash
git add apps/deltaweave-gui Cargo.toml
git commit -m "feat: add light Tauri shell with tray and close-to-tray"
```

---

### Task 9: Add Folder wizard (four stages)

**Files:**
- Create: `apps/deltaweave-gui/ui/src/wizard.ts`
- Create: `apps/deltaweave-gui/ui/src/wizard.test.ts`
- Modify: daemon `CreateJob` to require `preview_confirmed: true`

**Interfaces:**
- Consumes: `Command::CreateJob`, `Command::PreviewJob`, `Command::IssueTicket`, `Command::RedeemTicket`
- Produces wizard stages: folder → peer (discover or paste `dwpair1:`) → direction → preview confirm
- CreateJob without preview confirmation returns `InvalidRequest`

- [ ] **Step 1: Failing tests**

Frontend: wizard cannot jump to start without confirming preview.

Daemon:

```rust
#[test]
fn create_job_requires_preview_confirmation() {
    let cmd = Command::CreateJob {
        name: "ISOs".into(),
        local_root: "/tmp/x".into(),
        peer_endpoint_id: "aa".repeat(32),
        direction: Direction::Bidirectional,
        preview_confirmed: false,
    };
    // dispatch against supervisor
    let err = dispatch(cmd).unwrap_err();
    assert_eq!(err.code, ErrorCode::InvalidRequest);
}
```

- [ ] **Step 2: Run to verify fail**

```bash
cargo test -p deltaweave-daemon create_job_requires_preview_confirmation
cd apps/deltaweave-gui && npm test
```

- [ ] **Step 3: Implement**

Keep advanced manual IP/port behind a disclosure labeled 고급. Default peer path is LAN discovery list or ticket paste.

- [ ] **Step 4: Tests pass** then commit

```bash
git commit -m "feat: add four-stage folder pairing wizard with preview gate"
```

---

### Task 10: Conflict pane and diagnostics export

**Files:**
- Create: `apps/deltaweave-gui/ui/src/conflicts.ts`
- Create: `crates/deltaweave-daemon/src/diagnostics.rs`

**Interfaces:**
- Consumes: `ListConflicts`, `ResolveConflict`
- Produces redacted zip/json diagnostic bundle: job names, hashes, error codes, OS, version. Strip `dwpair1:` payloads, secret keys, ticket hex.

- [ ] **Step 1: Failing test**

```rust
#[test]
fn diagnostic_bundle_redacts_ticket_codes() {
    let raw = "issued dwpair1:deadbeef identity=ffffffffffffffff";
    let redacted = redact_diagnostics(raw);
    assert!(!redacted.contains("dwpair1:"));
    assert!(!redacted.contains("ffff"));
}
```

- [ ] **Step 2: Fail then implement redact + Keep this PC / Keep peer / Keep both buttons wired to `ResolveConflict`**

- [ ] **Step 3: Tests pass and commit**

```bash
git commit -m "feat: add conflict resolution UI and redacted diagnostics"
```

---

### Task 11: Windows installer and release workflow

**Files:**
- Modify: `.github/workflows/release.yml`
- Modify: `apps/deltaweave-gui/src-tauri/tauri.conf.json` bundle NSIS
- Create: `scripts/package-windows-gui.sh` copies `deltaweave-daemon` to `apps/deltaweave-gui/src-tauri/binaries/deltaweave-daemon-x86_64-pc-windows-msvc.exe`

**Interfaces:**
- Consumes: `cargo build --release -p deltaweave-daemon --target x86_64-pc-windows-gnu` or `pc-windows-msvc` on the Windows runner
- Produces: NSIS current-user installer artifact plus existing ZIP. Unsigned is allowed for the hardware soak; release notes must say unsigned.

- [ ] **Step 1: Add a workflow job `windows-gui` that fails if sidecar binary missing**

```yaml
- run: test -f apps/deltaweave-gui/src-tauri/binaries/deltaweave-daemon-x86_64-pc-windows-msvc.exe
```

- [ ] **Step 2: Implement packaging script and wire `tauri build --bundles nsis`**

- [ ] **Step 3: Commit**

```bash
git commit -m "chore: package Windows GUI with daemon sidecar"
```

---

### Task 12: Hardware gate on the real Windows PC and 172.30.1.22

**Files:**
- Create: `docs/superpowers/plans/2026-08-29-windows-gui-soak-checklist.md` only if evidence needs a checklist; otherwise record results in the PR body
- Do not claim completion in this task without fresh hashes from both sides

**Evidence required:**

1. Installer launch, tray icon, `--hidden` login start.
2. Add `C:\DeltaWeave-Sync` to `172.30.1.22` within 60 seconds.
3. Small-file bidirectional hash match.
4. Four ISO files; GUI remains clickable; daemon progress events ≤10 Hz.
5. Pause / cancel / daemon restart / reconnect; verified roots match.
6. One conflict resolved via Keep this PC and hash-verified.
7. Idle CPU/RAM vs budget (daemon ≤100 MB, GUI ≤150 MB, CPU <1% idle).
8. Throughput within 5% of headless `deltaweave sync-once` on the same pair.

Commands on 22 (do not embed SSH passwords):

```bash
find /home/ubuntu/deltaweave-windows/root -maxdepth 1 -type f -printf '%f %s\n'
sha256sum /home/ubuntu/deltaweave-windows/root/*
```

Windows:

```powershell
Get-FileHash C:\DeltaWeave-Sync\*
```

If any item fails, this task is not done.

---

## Spec coverage

| Spec section | Task |
| --- | --- |
| Process model, single redb owner | 4, 5, 7, 8 |
| Versioned IPC, Hello, reconnect | 1, 4 |
| Job config, overlap rules | 3, 5 |
| Progress ≤10 Hz, pause/cancel | 2, 5 |
| Pairing dwpair1, 10 min, one-use, revoke | 6, 9 |
| Preview before first sync | 6, 9 |
| Light dashboard transfer+conflict | 8, 10 |
| Tray, close-to-tray, quit engine | 8 |
| Autostart hidden | 8, 11 |
| Diagnostics redaction | 10 |
| NSIS installer | 11 |
| Hardware soak | 12 |
| Preserve bind/pairing port | 0 |

## Type names (locked)

`ProtocolVersion`, `Request`, `Response`, `Command`, `CommandResult`, `Event`, `EventBody`, `ErrorBody`, `ErrorCode`, `ConfigStore`, `JobConfig`, `Direction`, `DaemonInstance`, `SyncCancel`, `SyncProgress`, `SyncPhase`, `ProgressSink`, `ProgressCoalescer`, `JobSupervisor`. Later tasks must not rename these.
