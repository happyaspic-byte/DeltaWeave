//! Persistent local administration and serialized folder workers.
pub mod model;
pub use model::*;
mod config;
mod private;
mod worker;

use anyhow::{Context, Result, ensure};
use deltaweave_net::{
    root_admission::{self, RootLease, RootUse},
    share::{Membership as NetMembership, OwnerShare, ShareError, ShareService, ShareTicket},
};
use deltaweave_sync::{ManagedSyncConfig, ManagedSyncEngine, ManagedSyncReport};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex as AsyncMutex, RwLock},
    task::JoinHandle,
};
use worker::Worker;

/// Stable, credential-free errors emitted by managed control operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedErrorKind {
    IdempotencyConflict,
    KeyResponseExpired,
    InvalidInput,
    InvalidPath,
    OwnerOnly,
    NotFound,
    Busy,
    ShuttingDown,
    IdempotencyCapacity,
    PendingExpired,
    ClockRollback,
}

#[derive(Debug)]
pub struct ManagedError {
    kind: ManagedErrorKind,
}

impl ManagedError {
    pub const fn new(kind: ManagedErrorKind) -> Self {
        Self { kind }
    }

    pub const fn kind(&self) -> ManagedErrorKind {
        self.kind
    }
}

impl std::fmt::Display for ManagedError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.kind().code())
    }
}

impl std::error::Error for ManagedError {}

impl ManagedErrorKind {
    const fn code(self) -> &'static str {
        match self {
            Self::IdempotencyConflict => "idempotency_conflict",
            Self::KeyResponseExpired => "key_response_expired",
            Self::InvalidInput => "invalid_input",
            Self::InvalidPath => "invalid_path",
            Self::OwnerOnly => "owner_only",
            Self::NotFound => "not_found",
            Self::Busy => "busy",
            Self::ShuttingDown => "shutting_down",
            Self::IdempotencyCapacity => "idempotency_capacity",
            Self::PendingExpired => "pending_expired",
            Self::ClockRollback => "clock_rollback",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::IdempotencyConflict => "request ID was already used for another request",
            Self::KeyResponseExpired => "the one-time key response is no longer available",
            Self::InvalidInput => "managed request input is invalid",
            Self::InvalidPath => "the selected path is invalid or unavailable",
            Self::OwnerOnly => "the operation is restricted to the share owner",
            Self::NotFound => "managed share or membership was not found",
            Self::Busy => "managed share is busy; retry later",
            Self::ShuttingDown => "manager is shutting down",
            Self::IdempotencyCapacity => "managed request journal has no available capacity",
            Self::PendingExpired => "pending enrollment has expired",
            Self::ClockRollback => "managed clock moved backwards; state is temporarily locked",
        }
    }
}

/// Classifies an internal error without exposing its chain, credentials, or paths.
pub fn classify_managed_error(error: &anyhow::Error) -> ErrorSummary {
    if let Some(managed) = error.downcast_ref::<ManagedError>() {
        return ErrorSummary {
            code: managed.kind().code().into(),
            message: managed.kind().message().into(),
        };
    }
    if let Some(share) = error.downcast_ref::<deltaweave_net::share::ShareError>() {
        let (code, message) = match share {
            deltaweave_net::share::ShareError::InvalidTicket => {
                ("invalid_ticket", "invalid share key")
            }
            deltaweave_net::share::ShareError::UnsupportedVersion => {
                ("unsupported_version", "unsupported share key version")
            }
            deltaweave_net::share::ShareError::Expired => ("expired", "share key expired"),
            deltaweave_net::share::ShareError::InvitationRevoked => {
                ("invitation_revoked", "share key revoked")
            }
            deltaweave_net::share::ShareError::MemberRevoked => {
                ("member_revoked", "membership revoked")
            }
            deltaweave_net::share::ShareError::PermissionDenied => {
                ("permission_denied", "share permission denied")
            }
            deltaweave_net::share::ShareError::UnknownShare => ("not_found", "share unavailable"),
            deltaweave_net::share::ShareError::NotMember => {
                ("not_member", "share enrollment required")
            }
            deltaweave_net::share::ShareError::ReplicaClaimRejected => (
                "replica_claim_rejected",
                "stored replica binding was rejected",
            ),
            deltaweave_net::share::ShareError::InvalidRecord => {
                ("invalid_record", "invalid causal record")
            }
            deltaweave_net::share::ShareError::Busy => ("busy", "share busy; retry"),
            deltaweave_net::share::ShareError::Offline => ("offline", "share owner unavailable"),
            deltaweave_net::share::ShareError::StateUnavailable => {
                ("state_unavailable", "private share state unavailable")
            }
            deltaweave_net::share::ShareError::Protocol => ("protocol", "invalid share protocol"),
            deltaweave_net::share::ShareError::OwnerMismatch => {
                ("owner_mismatch", "share owner mismatch")
            }
            deltaweave_net::share::ShareError::TransferFailed => {
                ("transfer_failed", "share transfer failed")
            }
            deltaweave_net::share::ShareError::HeartbeatExpired => {
                ("heartbeat_expired", "share heartbeat expired")
            }
            deltaweave_net::share::ShareError::HeartbeatReplay => {
                ("heartbeat_replay", "share heartbeat replayed")
            }
            deltaweave_net::share::ShareError::EpochMismatch => {
                ("epoch_mismatch", "share permission epoch mismatch")
            }
            deltaweave_net::share::ShareError::EndpointMismatch => {
                ("endpoint_mismatch", "share endpoint mismatch")
            }
            deltaweave_net::share::ShareError::ClockRollback => (
                "clock_rollback",
                "managed clock moved backwards; state is temporarily locked",
            ),
            deltaweave_net::share::ShareError::RosterStale => {
                ("roster_stale", "share roster is stale")
            }
            deltaweave_net::share::ShareError::GrantExpired => {
                ("grant_expired", "share grant expired")
            }
            deltaweave_net::share::ShareError::GrantReplay => {
                ("grant_replay", "share grant replayed")
            }
            deltaweave_net::share::ShareError::ManifestMismatch => {
                ("manifest_mismatch", "share manifest mismatch")
            }
            deltaweave_net::share::ShareError::CasUnavailable => {
                ("cas_unavailable", "share content unavailable")
            }
            deltaweave_net::share::ShareError::RevocationPending => {
                ("revocation_pending", "share revocation is pending")
            }
        };
        return ErrorSummary {
            code: code.into(),
            message: message.into(),
        };
    }
    if let Some(io) = error.downcast_ref::<std::io::Error>() {
        let kind = match io.kind() {
            std::io::ErrorKind::NotFound => ManagedErrorKind::NotFound,
            std::io::ErrorKind::PermissionDenied
            | std::io::ErrorKind::InvalidInput
            | std::io::ErrorKind::InvalidData => ManagedErrorKind::InvalidInput,
            std::io::ErrorKind::AlreadyExists => ManagedErrorKind::Busy,
            _ => {
                return ErrorSummary {
                    code: "internal_error".into(),
                    message: "managed operation failed".into(),
                };
            }
        };
        return ErrorSummary {
            code: kind.code().into(),
            message: kind.message().into(),
        };
    }
    ErrorSummary {
        code: "internal_error".into(),
        message: "managed operation failed".into(),
    }
}

pub(crate) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn managed_now() -> u64 {
    now() / 1000
}

fn managed_clock_required(managed: &config::ManagedConfig) -> bool {
    !managed.shares.is_empty()
        || !managed.pending.is_empty()
        || !managed.intents.is_empty()
        || !managed.requests.is_empty()
        || !managed.tombstones.is_empty()
        || !managed.revocations.is_empty()
        || !managed.key_intents.is_empty()
        || !managed.removals.is_empty()
}

fn validate_request_id(request_id: &str) -> Result<()> {
    ensure!(
        !request_id.is_empty()
            && request_id.len() <= 128
            && !request_id.chars().any(char::is_control)
            && !request_id.contains(['/', '\\']),
        ManagedError::new(ManagedErrorKind::InvalidInput)
    );
    Ok(())
}

fn parse_share_id(value: &str) -> Result<ShareId> {
    ensure!(
        value.len() == 64
            && value.as_bytes().iter().all(u8::is_ascii_hexdigit)
            && value == value.to_ascii_lowercase(),
        ManagedError::new(ManagedErrorKind::InvalidInput)
    );
    let bytes = hex::decode(value)
        .map_err(|_| anyhow::Error::new(ManagedError::new(ManagedErrorKind::InvalidInput)))?;
    ensure!(
        bytes.len() == 32,
        ManagedError::new(ManagedErrorKind::InvalidInput)
    );
    let mut id = [0_u8; 32];
    id.copy_from_slice(&bytes);
    Ok(ShareId(id))
}

fn parse_invitation_id(value: &str) -> Result<InvitationId> {
    ensure!(
        value.len() == 64
            && value.as_bytes().iter().all(u8::is_ascii_hexdigit)
            && value == value.to_ascii_lowercase(),
        ManagedError::new(ManagedErrorKind::InvalidInput)
    );
    let bytes = hex::decode(value)
        .map_err(|_| anyhow::Error::new(ManagedError::new(ManagedErrorKind::InvalidInput)))?;
    ensure!(
        bytes.len() == 32,
        ManagedError::new(ManagedErrorKind::InvalidInput)
    );
    let mut id = [0_u8; 32];
    id.copy_from_slice(&bytes);
    Ok(InvitationId(id))
}

fn share_id_string(id: ShareId) -> String {
    hex::encode(id.0)
}

fn invitation_id_string(id: InvitationId) -> String {
    hex::encode(id.0)
}

fn endpoint_id(value: &str) -> Result<iroh::EndpointId> {
    value
        .parse()
        .map_err(|_| anyhow::Error::new(ManagedError::new(ManagedErrorKind::InvalidInput)))
}

fn request_hash<T: serde::Serialize>(operation: &str, input: &T) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"deltaweave/control/managed-request/v1\0");
    hasher.update(operation.as_bytes());
    hasher.update(b"\0");
    hasher.update(&serde_json::to_vec(input)?);
    Ok(hasher.finalize().to_hex().to_string())
}

fn is_share_error(error: &anyhow::Error, expected: ShareError) -> bool {
    error
        .downcast_ref::<ShareError>()
        .is_some_and(|actual| *actual == expected)
}

fn is_terminal_pending_error(error: &anyhow::Error) -> bool {
    [
        ShareError::Expired,
        ShareError::InvitationRevoked,
        ShareError::MemberRevoked,
        ShareError::InvalidTicket,
        ShareError::UnsupportedVersion,
    ]
    .into_iter()
    .any(|expected| is_share_error(error, expected))
}

fn is_terminal_pending_storage_error(error: &anyhow::Error) -> bool {
    error.downcast_ref::<ManagedError>().is_some_and(|error| {
        matches!(
            error.kind(),
            ManagedErrorKind::NotFound
                | ManagedErrorKind::InvalidPath
                | ManagedErrorKind::InvalidInput
        )
    })
}

fn pending_terminal_ref(error: Option<&anyhow::Error>, share_id: &str) -> String {
    let kind = error
        .and_then(|error| {
            error
                .downcast_ref::<ShareError>()
                .map(|error| match error {
                    ShareError::InvitationRevoked | ShareError::MemberRevoked => "revoked",
                    ShareError::InvalidTicket | ShareError::UnsupportedVersion => "invalid",
                    ShareError::Expired => "expired",
                    _ => "expired",
                })
                .or_else(|| {
                    error
                        .downcast_ref::<ManagedError>()
                        .map(|error| match error.kind() {
                            ManagedErrorKind::NotFound
                            | ManagedErrorKind::InvalidPath
                            | ManagedErrorKind::InvalidInput => "invalid",
                            _ => "expired",
                        })
                })
        })
        .unwrap_or("expired");
    format!("{kind}:{share_id}")
}

static IDS: AtomicU64 = AtomicU64::new(1);
fn new_id() -> String {
    format!(
        "{:x}-{:x}-{:x}",
        now(),
        std::process::id(),
        IDS.fetch_add(1, Ordering::Relaxed)
    )
}

pub(crate) struct Runtime {
    config: config::Config,
    folders: BTreeMap<String, FolderView>,
    activities: Vec<Activity>,
    history: Vec<HistoryPoint>,
    revision: u64,
}
impl Runtime {
    fn activity(
        &mut self,
        folder_id: Option<String>,
        kind: &str,
        detail: String,
        path: Option<String>,
    ) {
        self.activities.push(Activity {
            id: new_id(),
            folder_id,
            kind: kind.into(),
            title: kind.into(),
            detail,
            timestamp: now(),
            pushed_bytes: 0,
            pulled_bytes: 0,
            path,
        });
        self.trim();
        self.revision += 1;
    }
    fn trim(&mut self) {
        let limit = self.config.settings.history_limit;
        if self.activities.len() > limit {
            self.activities.drain(..self.activities.len() - limit);
        }
        if self.history.len() > limit {
            self.history.drain(..self.history.len() - limit);
        }
    }
}
struct Slot {
    operation: AsyncMutex<Option<Worker>>,
}

struct ManagedSlot {
    operation: AsyncMutex<Option<ManagedWorker>>,
}

enum ManagedWorker {
    Owner(OwnerShare),
    Member(ManagedSyncEngine),
    Pending(PendingWorker),
}

struct PendingWorker {
    /// The durable request that owns this share slot.  Share slots are
    /// intentionally single-binding; a different join request may not
    /// silently inherit or discard this lease.
    request_id: String,
    lease: Option<Arc<RootLease>>,
}

struct RevocationTask {
    pending: config::PendingRevocation,
    task: JoinHandle<Result<()>>,
}

struct IssueKeyRequest<'a> {
    request_id: String,
    share: ShareId,
    permission: Permission,
    expires_at: Option<u64>,
    operation: &'a str,
    hash: String,
    rotate_invitation: Option<InvitationId>,
}

struct ManagedObserverState {
    last_event_at: u64,
}

impl ManagedWorker {
    async fn stop(self) -> Result<()> {
        match self {
            Self::Owner(owner) => {
                owner.pause().await;
                Ok(())
            }
            Self::Member(engine) => engine.shutdown().await,
            Self::Pending(mut pending) => {
                pending.lease.take();
                Ok(())
            }
        }
    }
}

const MANAGED_REQUEST_CAPACITY: usize = 1024;
const MANAGED_PENDING_MAX_SECONDS: u64 = 7 * 24 * 60 * 60;
const MANAGED_KEY_RESPONSE_SECONDS: u64 = 5 * 60;
const MANAGED_TICK_SECONDS: u64 = 2;
/// Owns local configuration and one serialized worker for each managed root.
/// Snapshot locks protect only in-memory copies; engine and filesystem work run outside them.
pub struct Manager {
    data_dir: PathBuf,
    shared: Arc<Mutex<Runtime>>,
    slots: Mutex<BTreeMap<String, Arc<Slot>>>,
    managed_slots: Mutex<BTreeMap<String, Arc<ManagedSlot>>>,
    managed_service: AsyncMutex<Option<Arc<ShareService>>>,
    member_handle_key: AsyncMutex<Option<[u8; 32]>>,
    managed_mutations: AsyncMutex<()>,
    background: AsyncMutex<Vec<JoinHandle<()>>>,
    revocation_tasks: AsyncMutex<Vec<RevocationTask>>,
    managed_options: ManagerOptions,
    persistence: AsyncMutex<()>,
    lifecycle: RwLock<()>,
    ownership: Mutex<Option<File>>,
    stopped: AtomicBool,
    started_at: u64,
}
impl Manager {
    pub async fn open(data_dir: PathBuf) -> Result<Arc<Self>> {
        Self::open_with_options(data_dir, ManagerOptions::default()).await
    }

    /// Opens the manager with managed-network settings supplied by a harness.
    /// These settings are intentionally not persisted and do not affect legacy
    /// folder identities or their direct-only workers.
    pub async fn open_with_options(
        data_dir: PathBuf,
        managed_options: ManagerOptions,
    ) -> Result<Arc<Self>> {
        let (data_dir, ownership, config) = tokio::task::spawn_blocking(move || -> Result<_> {
            let mut directories = std::fs::DirBuilder::new();
            directories.recursive(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                directories.mode(0o700);
            }
            directories.create(&data_dir)?;
            let data_dir = std::fs::canonicalize(data_dir)?;
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(data_dir.join("manager.lock"))?;
            fs2::FileExt::try_lock_exclusive(&file)
                .context("management data directory is already open by another process")?;
            let mut config = config::read(&data_dir)?;
            config::validate_name(&config.settings.node_name)?;
            ensure!(
                (1..=86400).contains(&config.settings.poll_interval_seconds),
                "saved poll interval must be 1–86400 seconds"
            );
            ensure!(
                (1..=10000).contains(&config.settings.history_limit),
                "saved history limit must be 1–10000"
            );
            let mut endpoints = std::collections::HashSet::new();
            let mut ids = std::collections::HashSet::new();
            for folder in &config.folders {
                ensure!(
                    ids.insert(&folder.id),
                    "duplicate folder ID in saved configuration"
                );
                ensure!(
                    endpoints.insert(&folder.endpoint_id),
                    "duplicate endpoint identity in saved configuration"
                );
            }
            for device in &config.devices {
                validate_device(&device.input)?;
            }
            // Resolve actual identities before starting any worker, including copied key files.
            let saved_folders = config.folders.clone();
            let mut actual_endpoints = std::collections::HashSet::new();
            for folder in &mut config.folders {
                folder.input = config::normalize(
                    folder.input.clone(),
                    &folder.id,
                    &data_dir,
                    &saved_folders,
                    config.settings.poll_interval_seconds,
                )?;
                folder.endpoint_id = deltaweave_net::load_or_create_identity(
                    folder
                        .input
                        .identity_path
                        .as_ref()
                        .context("identity path missing")?,
                )?
                .endpoint_id()
                .to_string();
                ensure!(
                    actual_endpoints.insert(folder.endpoint_id.clone()),
                    "duplicate endpoint identity in saved configuration"
                );
            }
            Ok((data_dir, file, config))
        })
        .await??;
        let manager = Arc::new(Self {
            data_dir,
            shared: Arc::new(Mutex::new(Runtime {
                folders: BTreeMap::new(),
                config: config.clone(),
                activities: config.activities.clone(),
                history: config.history.clone(),
                revision: 1,
            })),
            slots: Mutex::new(BTreeMap::new()),
            managed_slots: Mutex::new(BTreeMap::new()),
            managed_service: AsyncMutex::new(None),
            member_handle_key: AsyncMutex::new(None),
            managed_mutations: AsyncMutex::new(()),
            background: AsyncMutex::new(Vec::new()),
            revocation_tasks: AsyncMutex::new(Vec::new()),
            managed_options,
            persistence: AsyncMutex::new(()),
            lifecycle: RwLock::new(()),
            ownership: Mutex::new(Some(ownership)),
            stopped: AtomicBool::new(false),
            started_at: now(),
        });
        let startup = async {
            for mut view in config.folders {
                let others = manager
                    .shared
                    .lock()
                    .expect("snapshot mutex")
                    .folders
                    .values()
                    .cloned()
                    .collect::<Vec<_>>();
                view.input = config::normalize(
                    view.input,
                    &view.id,
                    &manager.data_dir,
                    &others,
                    config.settings.poll_interval_seconds,
                )?;
                view.status = "starting".into();
                view.addresses.clear();
                view.phase = None;
                view.current_path = None;
                manager
                    .shared
                    .lock()
                    .expect("snapshot mutex")
                    .folders
                    .insert(view.id.clone(), view.clone());
                let slot = Arc::new(Slot {
                    operation: AsyncMutex::new(None),
                });
                manager
                    .slots
                    .lock()
                    .expect("slots mutex")
                    .insert(view.id.clone(), slot.clone());
                match Worker::start(view.clone(), manager.shared.clone()).await {
                    Ok(worker) => *slot.operation.lock().await = Some(worker),
                    Err(error) => {
                        let mut state = manager.shared.lock().expect("snapshot mutex");
                        if let Some(folder) = state.folders.get_mut(&view.id) {
                            folder.status = "error".into();
                            folder.last_error = Some(format!("{error:#}"));
                        }
                        state.activity(Some(view.id), "error", format!("{error:#}"), None);
                    }
                }
            }
            // Persist dynamically allocated receiver ports before callers can copy connection details.
            let bound = manager
                .shared
                .lock()
                .expect("snapshot mutex")
                .folders
                .clone();
            manager
                .persist(|config| {
                    for folder in &mut config.folders {
                        if let Some(observed) = bound.get(&folder.id) {
                            folder.input.bind = observed.input.bind.clone();
                        }
                    }
                    Ok(())
                })
                .await?;
            manager.recover_managed().await
        }
        .await;
        if let Err(error) = startup {
            // Recovery runs after legacy workers have started.  Treat it as a
            // startup transaction so an error cannot leave a detached watcher,
            // listener, root lease, or managed endpoint behind the failed open.
            let cleanup_failed = manager.shutdown().await.is_err();
            if cleanup_failed {
                return Err(anyhow::anyhow!(
                    "managed recovery failed and startup cleanup failed"
                ));
            }
            return Err(error);
        }
        let weak = Arc::downgrade(&manager);
        let persistence_task = tokio::spawn(async move {
            let mut saved_revision = 0;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                let Some(manager) = weak.upgrade() else { break };
                let _active = manager.lifecycle.read().await;
                if manager.stopped.load(Ordering::Acquire) {
                    break;
                }
                let revision = manager.shared.lock().expect("snapshot mutex").revision;
                if revision == saved_revision {
                    continue;
                }
                if let Err(error) = manager.persist(|_| Ok(())).await {
                    manager.shared.lock().expect("snapshot mutex").activity(
                        None,
                        "error",
                        format!("Unable to save activity history: {error:#}"),
                        None,
                    );
                } else {
                    saved_revision = revision;
                }
            }
        });
        manager.background.lock().await.push(persistence_task);
        let weak = Arc::downgrade(&manager);
        let managed_task = tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval(std::time::Duration::from_secs(MANAGED_TICK_SECONDS));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let Some(manager) = weak.upgrade() else { break };
                let _active = manager.lifecycle.read().await;
                if manager.stopped.load(Ordering::Acquire) {
                    break;
                }
                if let Err(error) = manager.tick_managed().await {
                    manager.record_managed_error(error);
                }
            }
        });
        manager.background.lock().await.push(managed_task);
        Ok(manager)
    }

    /// Lists directories while excluding management and per-folder private state.
    pub async fn browse(&self, requested: Option<PathBuf>) -> Result<DirectoryListing> {
        let _active = self.lifecycle.read().await;
        ensure!(
            !self.stopped.load(Ordering::Acquire),
            "manager is shut down"
        );
        let mut private = vec![self.data_dir.clone()];
        {
            let state = self.shared.lock().expect("snapshot mutex");
            for folder in state.folders.values() {
                if let Some(path) = &folder.input.state_path {
                    private.push(config::candidate(std::path::Path::new(path))?);
                }
                if let Some(path) = &folder.input.identity_path {
                    private.push(config::candidate(std::path::Path::new(path))?);
                }
            }
        }
        tokio::task::spawn_blocking(move || {
            let requested = requested.unwrap_or(std::env::current_dir()?);
            let path = std::fs::canonicalize(requested)?;
            ensure!(path.is_dir(), "path is not a directory");
            ensure!(
                !private.iter().any(|denied| path.starts_with(denied)),
                "private management path is not browseable"
            );
            let mut entries = Vec::new();
            for entry in std::fs::read_dir(&path)? {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let entry_path = entry.path();
                if private
                    .iter()
                    .any(|denied| entry_path.starts_with(denied) || denied.starts_with(&entry_path))
                {
                    continue;
                }
                entries.push(DirectoryEntry {
                    name: entry.file_name().to_string_lossy().into_owned(),
                    path: entry_path.to_string_lossy().into_owned(),
                });
                ensure!(
                    entries.len() <= 10_000,
                    "directory contains too many subdirectories"
                );
            }
            entries.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(DirectoryListing {
                path: path.to_string_lossy().into_owned(),
                parent: path
                    .parent()
                    .map(|path| path.to_string_lossy().into_owned()),
                entries,
            })
        })
        .await
        .context("directory browse task failed")?
    }
    fn running(&self) -> Result<()> {
        ensure!(
            !self.stopped.load(Ordering::Acquire),
            ManagedError::new(ManagedErrorKind::ShuttingDown)
        );
        Ok(())
    }
    fn slot(&self, id: &str) -> Result<Arc<Slot>> {
        self.running()?;
        self.slots
            .lock()
            .expect("slots mutex")
            .get(id)
            .cloned()
            .context("folder not found")
    }
    async fn persist(&self, mutate: impl FnOnce(&mut config::Config) -> Result<()>) -> Result<()> {
        let _writer = self.persistence.lock().await;
        let mut config = {
            let state = self.shared.lock().expect("snapshot mutex");
            let mut config = state.config.clone();
            for saved in &mut config.folders {
                if let Some(observed) = state.folders.get(&saved.id) {
                    saved.last_sync_at = observed.last_sync_at;
                    saved.last_report = observed.last_report.clone();
                    saved.files_count = observed.files_count;
                    saved.total_bytes = observed.total_bytes;
                }
            }
            config.activities = state.activities.clone();
            config.history = state.history.clone();
            config
        };
        let before = serde_json::to_value(&config)?;
        let before_managed = serde_json::to_value(&config.managed)?;
        mutate(&mut config)?;
        let managed_changed = serde_json::to_value(&config.managed)? != before_managed;
        if managed_changed {
            let wall_now = managed_now();
            ensure!(
                config.managed.clock_last == 0 || wall_now >= config.managed.clock_last,
                ManagedError::new(ManagedErrorKind::ClockRollback)
            );
            config.managed.clock_last = wall_now;
        }
        let changed = serde_json::to_value(&config)? != before;
        let dir = self.data_dir.clone();
        let saved = config.clone();
        tokio::task::spawn_blocking(move || config::save(&dir, &saved)).await??;
        let mut state = self.shared.lock().expect("snapshot mutex");
        state.config = config;
        state.trim();
        if changed {
            state.revision += 1;
        }
        Ok(())
    }

    /// Returns the durable managed time and rejects a wall-clock rollback.
    /// A rollback must never turn an expired ticket or response into a valid one.
    fn managed_time(&self) -> Result<u64> {
        let wall_now = managed_now();
        let state = self.shared.lock().expect("snapshot mutex");
        if managed_clock_required(&state.config.managed) {
            ensure!(
                state.config.managed.clock_last == 0 || wall_now >= state.config.managed.clock_last,
                ManagedError::new(ManagedErrorKind::ClockRollback)
            );
        }
        Ok(wall_now)
    }

    async fn ensure_managed_service(&self) -> Result<Arc<ShareService>> {
        let mut service = self.managed_service.lock().await;
        if let Some(service) = service.as_ref() {
            return Ok(service.clone());
        }
        let managed_root = self.data_dir.join("managed");
        root_admission::reserve_private(&managed_root)?;
        private::prepare_directory(&managed_root)?;
        let state = self.data_dir.join("managed").join("service");
        root_admission::reserve_private(&state)?;
        private::prepare_directory(&state)?;
        let opened = ShareService::open(
            state,
            self.managed_options.managed_network,
            self.managed_options.managed_bind,
        )
        .await?;
        let opened = Arc::new(opened);
        *service = Some(opened.clone());
        Ok(opened)
    }

    async fn member_handle(&self, share: ShareId, endpoint: iroh::EndpointId) -> Result<String> {
        let mut key = self.member_handle_key.lock().await;
        let secret = if let Some(secret) = *key {
            secret
        } else {
            let managed_root = self.data_dir.join("managed");
            root_admission::reserve_private(&managed_root)?;
            private::prepare_directory(&managed_root)?;
            let path =
                root_admission::reserve_private_file(managed_root.join("member-handle.key"))?;
            let secret = if path.exists() {
                let bytes = std::fs::read(&path)?;
                ensure!(
                    bytes.len() == 32,
                    ManagedError::new(ManagedErrorKind::InvalidInput)
                );
                let mut secret = [0_u8; 32];
                secret.copy_from_slice(&bytes);
                secret
            } else {
                let secret = iroh::SecretKey::generate().to_bytes();
                let mut options = OpenOptions::new();
                options.create_new(true).write(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.mode(0o600);
                }
                let mut file = options.open(&path)?;
                std::io::Write::write_all(&mut file, &secret)?;
                file.sync_all()?;
                secret
            };
            *key = Some(secret);
            secret
        };
        let mut hasher = blake3::Hasher::new_keyed(&secret);
        hasher.update(b"deltaweave/control/member-handle/v1\0");
        hasher.update(&share.0);
        hasher.update(endpoint.as_bytes());
        Ok(format!("member-{}", &hasher.finalize().to_hex()[..32]))
    }

    fn private_text_path(&self, namespace: &str, hash: &str, suffix: &str) -> Result<PathBuf> {
        ensure!(
            hash.len() == 64 && hash.chars().all(|ch| ch.is_ascii_hexdigit()),
            ManagedError::new(ManagedErrorKind::InvalidInput)
        );
        let path = self
            .data_dir
            .join("managed")
            .join(namespace)
            .join(format!("{hash}.{suffix}"));
        Ok(path)
    }

    fn write_private_text(path: &Path, value: &str, max_bytes: usize) -> Result<()> {
        ensure!(
            value.len() <= max_bytes,
            ManagedError::new(ManagedErrorKind::InvalidInput)
        );
        let parent = path
            .parent()
            .context("private file parent is unavailable")?;
        root_admission::reserve_private(parent)?;
        private::prepare_directory(parent)?;
        if path.exists() {
            ensure!(
                !path.is_symlink(),
                ManagedError::new(ManagedErrorKind::InvalidPath)
            );
            let metadata = std::fs::metadata(path)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                ensure!(
                    metadata.permissions().mode() & 0o077 == 0,
                    ManagedError::new(ManagedErrorKind::InvalidPath)
                );
            }
            ensure!(
                metadata.len() <= max_bytes as u64,
                ManagedError::new(ManagedErrorKind::InvalidInput)
            );
            return Ok(());
        }
        let reserved = root_admission::reserve_private_file(path)?;
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(reserved)?;
        std::io::Write::write_all(&mut file, value.as_bytes())?;
        file.sync_all()?;
        Ok(())
    }

    fn read_private_text(path: &Path, max_bytes: usize) -> Result<String> {
        ensure!(
            path.exists() && !path.is_symlink(),
            ManagedError::new(ManagedErrorKind::NotFound)
        );
        if let Some(parent) = path.parent() {
            private::prepare_directory(parent)?;
        }
        let metadata = std::fs::metadata(path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            ensure!(
                metadata.permissions().mode() & 0o077 == 0,
                ManagedError::new(ManagedErrorKind::InvalidPath)
            );
        }
        ensure!(
            metadata.len() <= max_bytes as u64,
            ManagedError::new(ManagedErrorKind::InvalidInput)
        );
        let bytes = std::fs::read(path)?;
        String::from_utf8(bytes)
            .map_err(|_| anyhow::Error::new(ManagedError::new(ManagedErrorKind::InvalidInput)))
    }

    fn remove_private_file(path: &Path) -> Result<()> {
        if path.exists() {
            ensure!(
                !path.is_symlink(),
                ManagedError::new(ManagedErrorKind::InvalidPath)
            );
            std::fs::remove_file(path)?;
            if let Some(parent) = path.parent()
                && let Ok(file) = File::open(parent)
            {
                let _ = file.sync_all();
            }
        }
        Ok(())
    }

    fn pending_deadline(pending: &config::PendingRecord) -> u64 {
        pending.expires_at.unwrap_or_else(|| {
            pending
                .created_at
                .saturating_add(MANAGED_PENDING_MAX_SECONDS)
        })
    }

    fn is_generated_private_path(root: &Path, path: &Path, suffix: &str) -> bool {
        let Some(parent) = path.parent() else {
            return false;
        };
        if parent != root {
            return false;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            return false;
        };
        let Some(hash) = name.strip_suffix(suffix) else {
            return false;
        };
        hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
    }

    fn pending_ticket_path(&self, pending: &config::PendingRecord) -> Result<PathBuf> {
        let root = self.data_dir.join("managed").join("pending");
        private::prepare_directory(&root)?;
        let path = PathBuf::from(&pending.ticket_file);
        ensure!(
            Self::is_generated_private_path(&root, &path, ".ticket"),
            ManagedError::new(ManagedErrorKind::InvalidPath)
        );
        Ok(path)
    }

    async fn install_pending_slot_with_lease(
        &self,
        pending: &config::PendingRecord,
        lease: Arc<RootLease>,
    ) -> Result<()> {
        let id = pending.share_id.clone();
        let existing = self
            .managed_slots
            .lock()
            .expect("managed slots mutex")
            .get(&id)
            .cloned();
        if let Some(slot) = existing {
            let mut operation = slot.operation.lock().await;
            match operation.as_ref() {
                None => {
                    *operation = Some(ManagedWorker::Pending(PendingWorker {
                        request_id: pending.request_id.clone(),
                        lease: Some(lease),
                    }));
                    return Ok(());
                }
                Some(ManagedWorker::Pending(existing))
                    if existing.request_id == pending.request_id =>
                {
                    // The existing worker owns the admission lease for this
                    // request.  The newly acquired duplicate is dropped.
                    return Ok(());
                }
                Some(ManagedWorker::Pending(_))
                | Some(ManagedWorker::Owner(_))
                | Some(ManagedWorker::Member(_)) => {
                    return Err(ManagedError::new(ManagedErrorKind::Busy).into());
                }
            }
        }
        self.managed_slots
            .lock()
            .expect("managed slots mutex")
            .insert(
                id,
                Arc::new(ManagedSlot {
                    operation: AsyncMutex::new(Some(ManagedWorker::Pending(PendingWorker {
                        request_id: pending.request_id.clone(),
                        lease: Some(lease),
                    }))),
                }),
            );
        Ok(())
    }

    async fn install_pending_slot(&self, pending: &config::PendingRecord) -> Result<()> {
        let id = pending.share_id.clone();
        let existing = self
            .managed_slots
            .lock()
            .expect("managed slots mutex")
            .get(&id)
            .cloned();
        if let Some(slot) = existing {
            let mut operation = slot.operation.lock().await;
            if let Some(existing) = operation.as_ref() {
                match existing {
                    ManagedWorker::Pending(existing)
                        if existing.request_id == pending.request_id =>
                    {
                        return Ok(());
                    }
                    ManagedWorker::Pending(_)
                    | ManagedWorker::Owner(_)
                    | ManagedWorker::Member(_) => {
                        return Err(ManagedError::new(ManagedErrorKind::Busy).into());
                    }
                }
            }
            let share = parse_share_id(&pending.share_id)?;
            let owner = endpoint_id(&pending.owner)?;
            let lease = Arc::new(root_admission::acquire_with_private(
                Path::new(&pending.root),
                RootUse::Managed {
                    share: share.0,
                    owner: *owner.as_bytes(),
                },
                std::slice::from_ref(&PathBuf::from(&pending.state_root)),
            )?);
            *operation = Some(ManagedWorker::Pending(PendingWorker {
                request_id: pending.request_id.clone(),
                lease: Some(lease),
            }));
            return Ok(());
        }
        let share = parse_share_id(&pending.share_id)?;
        let owner = endpoint_id(&pending.owner)?;
        let lease = Arc::new(root_admission::acquire_with_private(
            Path::new(&pending.root),
            RootUse::Managed {
                share: share.0,
                owner: *owner.as_bytes(),
            },
            std::slice::from_ref(&PathBuf::from(&pending.state_root)),
        )?);
        self.managed_slots
            .lock()
            .expect("managed slots mutex")
            .insert(
                id,
                Arc::new(ManagedSlot {
                    operation: AsyncMutex::new(Some(ManagedWorker::Pending(PendingWorker {
                        request_id: pending.request_id.clone(),
                        lease: Some(lease),
                    }))),
                }),
            );
        Ok(())
    }

    /// Removes only a pending worker.  If a completed worker replaced the
    /// pending operation concurrently, it is put back into the slot.
    async fn remove_pending_slot(&self, share_id: &str) {
        let slot = self
            .managed_slots
            .lock()
            .expect("managed slots mutex")
            .remove(share_id);
        let Some(slot) = slot else { return };
        let operation = slot.operation.lock().await.take();
        match operation {
            None | Some(ManagedWorker::Pending(_)) => {}
            Some(worker) => {
                self.managed_slots
                    .lock()
                    .expect("managed slots mutex")
                    .insert(share_id.to_owned(), slot.clone());
                *slot.operation.lock().await = Some(worker);
            }
        }
    }

    async fn take_pending_lease(&self, share_id: &str) -> Result<Option<Arc<RootLease>>> {
        let slot = self
            .managed_slots
            .lock()
            .expect("managed slots mutex")
            .get(share_id)
            .cloned();
        let Some(slot) = slot else { return Ok(None) };
        let operation = { slot.operation.lock().await.take() };
        match operation {
            Some(ManagedWorker::Pending(pending)) => Ok(pending.lease),
            None => Ok(None),
            Some(worker) => {
                *slot.operation.lock().await = Some(worker);
                Err(ManagedError::new(ManagedErrorKind::Busy).into())
            }
        }
    }

    /// Commits terminal pending state before releasing the lease and raw ticket.
    /// The metadata remains available until the owner has authenticated the
    /// caller as NotMember; an offline owner never causes an active membership
    /// to be discarded.
    async fn terminalize_pending(
        &self,
        pending: &config::PendingRecord,
        result_ref: String,
    ) -> Result<()> {
        let ticket_path = self.pending_ticket_path(pending)?;
        self.persist(|config| {
            config
                .managed
                .pending
                .retain(|item| item.request_id != pending.request_id);
            if let Some(request) = config
                .managed
                .requests
                .iter_mut()
                .find(|request| request.request_id == pending.request_id)
            {
                request.result_ref = result_ref.clone();
            }
            Ok(())
        })
        .await?;
        self.remove_pending_slot(&pending.share_id).await;
        Self::remove_private_file(&ticket_path)
    }

    /// Removes unreferenced ticket files left by a crash between writing the
    /// secret and committing its PendingRecord.  Referenced expired secrets are
    /// removed while their trusted owner/share metadata is retained for resume.
    async fn gc_pending_tickets(&self) -> Result<()> {
        // Join writes the raw ticket before committing its PendingRecord.
        // Serializing this scan with the mutation lock prevents the background
        // tick from mistaking that in-flight ticket for an orphan.
        let _mutation = self.managed_mutations.lock().await;
        let root = self.data_dir.join("managed").join("pending");
        if !root.is_dir() {
            return Ok(());
        }
        // Validate every existing component before reading or removing a
        // child.  A symlink/reparse ancestor must never redirect cleanup
        // outside the managed private namespace.
        private::prepare_directory(&root)?;
        let now = self.managed_time()?;
        let referenced = {
            let state = self.shared.lock().expect("snapshot mutex");
            let mut referenced = BTreeMap::new();
            for pending in &state.config.managed.pending {
                let path = PathBuf::from(&pending.ticket_file);
                ensure!(
                    Self::is_generated_private_path(&root, &path, ".ticket"),
                    ManagedError::new(ManagedErrorKind::InvalidPath)
                );
                referenced.insert(path, Self::pending_deadline(pending) <= now);
            }
            referenced
        };
        for entry in std::fs::read_dir(&root)? {
            let entry = entry?;
            let path = entry.path();
            if !entry.file_type()?.is_file() || path.is_symlink() {
                continue;
            }
            if !Self::is_generated_private_path(&root, &path, ".ticket") {
                continue;
            }
            if referenced
                .get(&path)
                .copied()
                .is_none_or(|is_expired| is_expired)
            {
                Self::remove_private_file(&path)?;
            }
        }
        Ok(())
    }

    fn gc_key_responses(&self) -> Result<()> {
        let root = self.data_dir.join("managed").join("responses");
        if !root.is_dir() {
            return Ok(());
        }
        private::prepare_directory(&root)?;
        let now = self.managed_time()?;
        let cutoff = UNIX_EPOCH
            + std::time::Duration::from_secs(now.saturating_sub(MANAGED_KEY_RESPONSE_SECONDS));
        for entry in std::fs::read_dir(&root)? {
            let entry = entry?;
            let path = entry.path();
            if !entry.file_type()?.is_file() || path.is_symlink() {
                continue;
            }
            if !Self::is_generated_private_path(&root, &path, ".ticket") {
                continue;
            }
            if entry
                .metadata()?
                .modified()
                .is_ok_and(|modified| modified <= cutoff)
            {
                Self::remove_private_file(&path)?;
            }
        }
        Ok(())
    }

    fn request_lookup(
        &self,
        request_id: &str,
        operation: &str,
        hash: &str,
    ) -> Result<Option<String>> {
        let state = self.shared.lock().expect("snapshot mutex");
        let Some(record) = state
            .config
            .managed
            .requests
            .iter()
            .find(|record| record.request_id == request_id)
        else {
            return Ok(None);
        };
        ensure!(
            record.operation == operation && record.request_hash == hash,
            ManagedError::new(ManagedErrorKind::IdempotencyConflict)
        );
        Ok(Some(record.result_ref.clone()))
    }

    fn request_record(&self, request_id: &str) -> Option<config::RequestRecord> {
        self.shared
            .lock()
            .expect("snapshot mutex")
            .config
            .managed
            .requests
            .iter()
            .find(|record| record.request_id == request_id)
            .cloned()
    }

    fn removal_intent(&self, request_id: &str) -> Option<config::RemovalIntent> {
        self.shared
            .lock()
            .expect("snapshot mutex")
            .config
            .managed
            .removals
            .iter()
            .find(|intent| intent.request_id == request_id)
            .cloned()
    }

    fn key_intent(&self, request_id: &str) -> Option<config::KeyIntent> {
        self.shared
            .lock()
            .expect("snapshot mutex")
            .config
            .managed
            .key_intents
            .iter()
            .find(|intent| intent.request_id == request_id)
            .cloned()
    }

    fn key_intent_path(&self, intent: &config::KeyIntent) -> Result<PathBuf> {
        let root = self.data_dir.join("managed").join("responses");
        private::prepare_directory(&root)?;
        let path = PathBuf::from(&intent.response_file);
        ensure!(
            Self::is_generated_private_path(&root, &path, ".ticket"),
            ManagedError::new(ManagedErrorKind::InvalidPath)
        );
        Ok(path)
    }

    fn validate_key_intent(
        intent: &config::KeyIntent,
        request: &IssueKeyRequest<'_>,
    ) -> Result<InvitationId> {
        ensure!(
            intent.operation == request.operation
                && intent.request_hash == request.hash
                && intent.share_id == share_id_string(request.share)
                && intent.permission == request.permission
                && intent.expires_at == request.expires_at
                && intent
                    .rotate_invitation
                    .as_ref()
                    .map(|id| id.to_ascii_lowercase())
                    == request.rotate_invitation.map(invitation_id_string),
            ManagedError::new(ManagedErrorKind::IdempotencyConflict)
        );
        parse_invitation_id(&intent.invitation_id)
    }

    async fn remove_key_intent(&self, request_id: &str) -> Result<()> {
        self.persist(|config| {
            config
                .managed
                .key_intents
                .retain(|intent| intent.request_id != request_id);
            Ok(())
        })
        .await
    }

    async fn expire_key_intent(
        &self,
        intent: &config::KeyIntent,
        path: &Path,
        owner: &OwnerShare,
    ) -> Result<IssuedKey> {
        let invitation = parse_invitation_id(&intent.invitation_id)?;
        if let Some(existing) = owner
            .keys()?
            .into_iter()
            .find(|existing| existing.id == invitation)
            && existing.revoked_at.is_none()
        {
            owner.revoke_key(invitation)?;
        }
        self.record_request(
            intent.request_id.clone(),
            &intent.operation,
            intent.request_hash.clone(),
            path.to_string_lossy().into_owned(),
        )
        .await?;
        Self::remove_private_file(path)?;
        self.remove_key_intent(&intent.request_id).await?;
        Err(ManagedError::new(ManagedErrorKind::KeyResponseExpired).into())
    }

    fn ensure_request_capacity(&self, request_id: &str) -> Result<()> {
        let state = self.shared.lock().expect("snapshot mutex");
        if state
            .config
            .managed
            .requests
            .iter()
            .any(|record| record.request_id == request_id)
        {
            return Ok(());
        }
        ensure!(
            state.config.managed.requests.len() < MANAGED_REQUEST_CAPACITY,
            ManagedError::new(ManagedErrorKind::IdempotencyCapacity)
        );
        Ok(())
    }

    async fn record_request(
        &self,
        request_id: String,
        operation: &str,
        request_hash: String,
        result_ref: String,
    ) -> Result<()> {
        self.persist(|config| {
            if let Some(existing) = config
                .managed
                .requests
                .iter()
                .find(|record| record.request_id == request_id)
            {
                ensure!(
                    existing.operation == operation && existing.request_hash == request_hash,
                    ManagedError::new(ManagedErrorKind::IdempotencyConflict)
                );
                return Ok(());
            }
            ensure!(
                config.managed.requests.len() < MANAGED_REQUEST_CAPACITY,
                ManagedError::new(ManagedErrorKind::IdempotencyCapacity)
            );
            config.managed.requests.push(config::RequestRecord {
                request_id,
                operation: operation.into(),
                request_hash,
                result_ref,
                recorded_at: managed_now(),
            });
            Ok(())
        })
        .await
    }

    async fn complete_removed_request(
        &self,
        request_id: &str,
        request_hash: &str,
        share_id: &str,
    ) -> Result<()> {
        self.persist(|config| {
            if let Some(existing) = config
                .managed
                .requests
                .iter()
                .find(|record| record.request_id == request_id)
            {
                ensure!(
                    existing.operation == "remove_share" && existing.request_hash == request_hash,
                    ManagedError::new(ManagedErrorKind::IdempotencyConflict)
                );
            } else {
                ensure!(
                    config.managed.requests.len() < MANAGED_REQUEST_CAPACITY,
                    ManagedError::new(ManagedErrorKind::IdempotencyCapacity)
                );
                config.managed.requests.push(config::RequestRecord {
                    request_id: request_id.into(),
                    operation: "remove_share".into(),
                    request_hash: request_hash.into(),
                    result_ref: format!("mutation:{share_id}"),
                    recorded_at: managed_now(),
                });
            }
            config
                .managed
                .removals
                .retain(|intent| intent.request_id != request_id);
            Ok(())
        })
        .await
    }

    fn record_managed_error(&self, error: anyhow::Error) {
        let summary = classify_managed_error(&error);
        let mut state = self.shared.lock().expect("snapshot mutex");
        state.activity(None, "managed_error", summary.message, None);
    }

    fn mark_managed_memory_failure(&self, share: ShareId, error: &anyhow::Error) {
        let summary = classify_managed_error(error);
        let id = share_id_string(share);
        let mut state = self.shared.lock().expect("snapshot mutex");
        if let Some(record) = state
            .config
            .managed
            .shares
            .iter_mut()
            .find(|record| record.share_id == id)
        {
            record.status = match summary.code.as_str() {
                "member_revoked" => ManagedStatus::Revoked,
                "offline" => ManagedStatus::Offline,
                _ => ManagedStatus::Error,
            };
            record.phase = Some(
                if summary.code == "member_revoked" {
                    "revoked"
                } else {
                    "error"
                }
                .into(),
            );
            record.retry_at = if summary.code == "member_revoked" {
                None
            } else {
                Some(managed_now().saturating_add(MANAGED_TICK_SECONDS))
            };
            record.last_error = Some(summary);
            state.revision += 1;
        }
    }

    fn managed_record(&self, share: ShareId) -> Result<config::ManagedShareRecord> {
        let share = share_id_string(share);
        self.shared
            .lock()
            .expect("snapshot mutex")
            .config
            .managed
            .shares
            .iter()
            .find(|record| record.share_id == share)
            .cloned()
            .ok_or_else(|| anyhow::Error::new(ManagedError::new(ManagedErrorKind::NotFound)))
    }

    fn managed_view(record: &config::ManagedShareRecord) -> ManagedShareView {
        ManagedShareView {
            share_id: record.share_id.clone(),
            name: record.name.clone(),
            role: record.role,
            permission: record.permission,
            root: record.root.clone(),
            status: record.status,
            phase: record.phase.clone(),
            last_sync_at: record.last_sync_at,
            retry_at: record.retry_at,
            files_count: record.files_count,
            total_bytes: record.total_bytes,
            transferred_bytes: record.transferred_bytes,
            speed_bps: record.speed_bps,
            active_peer_count: record.active_peer_count,
            connected_devices: record.connected_devices.clone(),
            last_error: record.last_error.clone(),
        }
    }

    fn managed_view_for(&self, share: ShareId) -> Result<ManagedShareView> {
        Ok(Self::managed_view(&self.managed_record(share)?))
    }

    fn managed_paths_conflict(&self, root: &Path, state_root: &Path) -> Result<()> {
        let state = self.shared.lock().expect("snapshot mutex");
        ensure!(
            !config::overlaps(root, &self.data_dir),
            ManagedError::new(ManagedErrorKind::InvalidPath)
        );
        for folder in state.folders.values().chain(state.config.folders.iter()) {
            let candidates = [
                Some(Path::new(folder.input.root.as_str())),
                folder.input.state_path.as_deref().map(Path::new),
                folder.input.identity_path.as_deref().map(Path::new),
            ];
            for candidate in candidates.into_iter().flatten() {
                let candidate = config::candidate(candidate)?;
                ensure!(
                    !config::overlaps(root, &candidate)
                        && !config::overlaps(state_root, &candidate),
                    ManagedError::new(ManagedErrorKind::InvalidPath)
                );
            }
        }
        for record in &state.config.managed.shares {
            let other_root = Path::new(&record.root);
            let other_state = Path::new(&record.state_root);
            ensure!(
                !config::overlaps(root, other_root)
                    && !config::overlaps(root, other_state)
                    && !config::overlaps(state_root, other_root)
                    && !config::overlaps(state_root, other_state),
                ManagedError::new(ManagedErrorKind::InvalidPath)
            );
        }
        Ok(())
    }

    /// Enforces the one-device/one-share binding before writing a new pending
    /// ticket or reserving a second pair of roots.  The pending/active rows
    /// are durable even when their worker is temporarily missing, so a new
    /// request must never replace that binding.
    fn ensure_new_join_binding(&self, share: ShareId, request_id: &str) -> Result<()> {
        let id = share_id_string(share);
        let state = self.shared.lock().expect("snapshot mutex");
        if state
            .config
            .managed
            .tombstones
            .iter()
            .any(|tombstone| tombstone == &id)
        {
            return Err(ManagedError::new(ManagedErrorKind::NotFound).into());
        }
        if state
            .config
            .managed
            .pending
            .iter()
            .any(|pending| pending.share_id == id && pending.request_id != request_id)
        {
            return Err(ManagedError::new(ManagedErrorKind::Busy).into());
        }
        if state
            .config
            .managed
            .shares
            .iter()
            .any(|record| record.share_id == id)
        {
            return Err(ManagedError::new(ManagedErrorKind::Busy).into());
        }
        Ok(())
    }

    async fn recover_managed(&self) -> Result<()> {
        // A rollback in managed time quarantines managed recovery while still
        // allowing the legacy manager and its manual workers to open.  The
        // durable managed rows remain untouched; once the wall clock catches
        // up, the normal background tick can recover them.
        if let Err(error) = self.managed_time() {
            if error
                .downcast_ref::<ManagedError>()
                .is_some_and(|error| error.kind() == ManagedErrorKind::ClockRollback)
            {
                self.record_managed_error(error);
                return Ok(());
            }
            return Err(error);
        }
        self.gc_pending_tickets().await?;
        let has_managed = {
            let state = self.shared.lock().expect("snapshot mutex");
            !state.config.managed.shares.is_empty()
                || !state.config.managed.pending.is_empty()
                || !state.config.managed.intents.is_empty()
                || !state.config.managed.tombstones.is_empty()
                || !state.config.managed.revocations.is_empty()
                || !state.config.managed.key_intents.is_empty()
                || !state.config.managed.removals.is_empty()
        };
        if !has_managed {
            return Ok(());
        }
        let service = self.ensure_managed_service().await?;
        let owned = service.owned_configs()?;
        let (tombstones, configured, intents) = {
            let state = self.shared.lock().expect("snapshot mutex");
            (
                state.config.managed.tombstones.clone(),
                state.config.managed.shares.clone(),
                state.config.managed.intents.clone(),
            )
        };
        self.cleanup_tombstones(&service, &tombstones, &configured)
            .await?;
        let mut recovered = Vec::new();
        let mut recovered_requests = Vec::new();
        for owned_config in owned {
            let id = share_id_string(owned_config.share_id);
            if tombstones.iter().any(|tombstone| tombstone == &id) {
                continue;
            }
            if let Some(existing) = configured.iter().find(|record| record.share_id == id) {
                ensure!(
                    existing.role == ShareRole::Owner
                        && existing.name == owned_config.name
                        && existing.root == owned_config.root.to_string_lossy()
                        && existing.state_root == owned_config.state_root.to_string_lossy()
                        && existing.owner == owned_config.owner.to_string(),
                    ShareError::StateUnavailable
                );
                continue;
            }
            let root = owned_config.root.to_string_lossy().into_owned();
            let state_root = owned_config.state_root.to_string_lossy().into_owned();
            let matches: Vec<_> = intents
                .iter()
                .filter(|intent| {
                    intent.name == owned_config.name
                        && intent.root == root
                        && intent.state_root == state_root
                        && intent
                            .owner
                            .as_deref()
                            .is_none_or(|owner| owner == owned_config.owner.to_string().as_str())
                })
                .collect();
            if matches.len() != 1 {
                // A catalog record without one exact durable create intent is not
                // safe to publish. Leave it in the net catalog for repair.
                continue;
            }
            let intent = matches[0];
            recovered.push(config::ManagedShareRecord {
                share_id: id,
                role: ShareRole::Owner,
                permission: None,
                name: owned_config.name.clone(),
                root,
                state_root,
                owner: owned_config.owner.to_string(),
                min_free_space_bytes: owned_config.min_free_space_bytes,
                owner_address: Some(service.endpoint_addr()),
                member_id: None,
                replica: Some(owned_config.replica),
                enrolled_at: None,
                membership_epoch: None,
                revocation_pending: false,
                status: ManagedStatus::InitialSync,
                phase: None,
                last_sync_at: None,
                retry_at: None,
                files_count: 0,
                total_bytes: 0,
                transferred_bytes: 0,
                speed_bps: 0,
                active_peer_count: 0,
                connected_devices: Vec::new(),
                last_error: None,
            });
            recovered_requests.push((intent.request_id.clone(), intent.request_hash.clone()));
        }
        if !recovered.is_empty() {
            self.persist(|config| {
                for (record, (request_id, request_hash)) in
                    recovered.iter().zip(recovered_requests.iter())
                {
                    config.managed.intents.retain(|intent| {
                        !(intent.name == record.name
                            && intent.root == record.root
                            && intent.state_root == record.state_root)
                    });
                    config.managed.shares.push(record.clone());
                    if !config
                        .managed
                        .requests
                        .iter()
                        .any(|request| request.request_id == *request_id)
                    {
                        config.managed.requests.push(config::RequestRecord {
                            request_id: request_id.clone(),
                            operation: "create_share".into(),
                            request_hash: request_hash.clone(),
                            result_ref: format!("share:{}", record.share_id),
                            recorded_at: managed_now(),
                        });
                    }
                }
                Ok(())
            })
            .await?;
        }
        self.restore_managed_workers(service).await
    }

    async fn cleanup_tombstones(
        &self,
        service: &Arc<ShareService>,
        tombstones: &[String],
        configured: &[config::ManagedShareRecord],
    ) -> Result<()> {
        for id in tombstones {
            let share = parse_share_id(id)?;
            if let Some(record) = configured.iter().find(|record| record.share_id == *id)
                && record.role == ShareRole::Member
            {
                let owner = endpoint_id(&record.owner)?;
                service.forget_membership(owner, share)?;
                continue;
            }
            match service.unload_owned_share(share).await {
                Ok(()) => {}
                Err(error) if is_share_error(&error, ShareError::UnknownShare) => {}
                Err(error) => return Err(error),
            }
        }
        self.persist(|config| {
            config
                .managed
                .shares
                .retain(|record| !tombstones.iter().any(|id| id == &record.share_id));
            config
                .managed
                .pending
                .retain(|pending| !tombstones.iter().any(|id| id == &pending.share_id));
            config
                .managed
                .revocations
                .retain(|pending| !tombstones.iter().any(|id| id == &pending.share_id));
            Ok(())
        })
        .await
    }

    async fn restore_managed_workers(&self, service: Arc<ShareService>) -> Result<()> {
        let (records, tombstones) = {
            let state = self.shared.lock().expect("snapshot mutex");
            (
                state.config.managed.shares.clone(),
                state
                    .config
                    .managed
                    .tombstones
                    .iter()
                    .cloned()
                    .collect::<std::collections::BTreeSet<_>>(),
            )
        };
        // A completed row and a pending row for the same share would give
        // recovery two competing immutable roots. Refuse the configuration
        // before opening either worker instead of silently choosing one.
        let mut seen_bindings = std::collections::BTreeSet::new();
        for record in &records {
            ensure!(
                seen_bindings.insert(record.share_id.clone()),
                ShareError::StateUnavailable
            );
        }
        let pending_snapshot = self
            .shared
            .lock()
            .expect("snapshot mutex")
            .config
            .managed
            .pending
            .clone();
        for pending in &pending_snapshot {
            ensure!(
                seen_bindings.insert(pending.share_id.clone()),
                ShareError::StateUnavailable
            );
        }
        for record in records {
            let id = record.share_id.clone();
            if tombstones.contains(&id) {
                continue;
            }
            let share = parse_share_id(&id)?;
            let worker = match record.role {
                ShareRole::Owner => {
                    let load = if matches!(
                        record.status,
                        ManagedStatus::Paused | ManagedStatus::Revoked
                    ) {
                        service.load_owned_share_paused(share).await
                    } else {
                        service.load_owned_share(share).await
                    };
                    match load {
                        Ok(owner) => {
                            self.install_owner_observer(share, &owner).await?;
                            if matches!(
                                record.status,
                                ManagedStatus::Paused | ManagedStatus::Revoked
                            ) {
                                owner.pause().await;
                            } else {
                                self.update_owner_inventory(share, &owner).await?;
                            }
                            Some(ManagedWorker::Owner(owner))
                        }
                        Err(error) => {
                            self.update_managed_failure(share, &error).await?;
                            None
                        }
                    }
                }
                ShareRole::Member => {
                    if record.status == ManagedStatus::Revoked {
                        continue;
                    }
                    let owner = record
                        .owner_address
                        .as_ref()
                        .map(|address| address.id)
                        .ok_or_else(|| {
                            anyhow::Error::new(ManagedError::new(ManagedErrorKind::InvalidInput))
                        })?;
                    let sync_config = ManagedSyncConfig {
                        root: PathBuf::from(&record.root),
                        state_root: PathBuf::from(&record.state_root),
                        profile: deltaweave_core::ChunkingProfile::default(),
                        min_free_space_bytes: record.min_free_space_bytes,
                    };
                    match ManagedSyncEngine::resume(&service, owner, share, sync_config) {
                        Ok(engine) => Some(ManagedWorker::Member(engine)),
                        Err(error) => {
                            self.update_managed_failure(share, &error).await?;
                            None
                        }
                    }
                }
            };
            if let Some(worker) = worker {
                self.managed_slots
                    .lock()
                    .expect("managed slots mutex")
                    .insert(
                        id,
                        Arc::new(ManagedSlot {
                            operation: AsyncMutex::new(Some(worker)),
                        }),
                    );
            }
        }
        for item in pending_snapshot {
            self.install_pending_slot(&item).await?;
        }
        self.restore_pending_revocations().await?;
        Ok(())
    }

    async fn update_owner_inventory(&self, share: ShareId, owner: &OwnerShare) -> Result<()> {
        let inventory = match owner.refresh_inventory().await {
            Ok(inventory) => inventory,
            Err(error) => {
                self.update_managed_failure(share, &error).await?;
                return Err(error);
            }
        };
        let id = share_id_string(share);
        self.persist(|config| {
            if let Some(record) = config
                .managed
                .shares
                .iter_mut()
                .find(|record| record.share_id == id)
            {
                if matches!(
                    record.status,
                    ManagedStatus::Paused | ManagedStatus::Revoked
                ) {
                    return Ok(());
                }
                record.status = ManagedStatus::Complete;
                record.phase = Some("complete".into());
                record.last_sync_at = Some(managed_now());
                record.retry_at = None;
                record.files_count = inventory.files;
                record.total_bytes = inventory.bytes;
                record.last_error = None;
            }
            Ok(())
        })
        .await
    }

    fn clear_managed_observation(&self, share: ShareId) {
        self.clear_managed_observation_id(&share_id_string(share));
    }

    fn clear_managed_observation_id(&self, id: &str) {
        let mut state = self.shared.lock().expect("snapshot mutex");
        if let Some(record) = state
            .config
            .managed
            .shares
            .iter_mut()
            .find(|record| record.share_id == id)
        {
            record.active_peer_count = 0;
            record.connected_devices.clear();
            record.speed_bps = 0;
            state.revision += 1;
        }
    }

    fn managed_observer(
        &self,
        share: ShareId,
        peer_handles: BTreeMap<String, (String, Permission)>,
    ) -> deltaweave_net::TransferObserver {
        let shared = Arc::downgrade(&self.shared);
        let id = share_id_string(share);
        let timing = Arc::new(Mutex::new(ManagedObserverState { last_event_at: 0 }));
        deltaweave_net::TransferObserver::new(move |event| {
            let Some(shared) = shared.upgrade() else {
                return;
            };
            let now_millis = now();
            let speed = {
                let mut timing = timing.lock().expect("managed observer mutex");
                let elapsed = now_millis.saturating_sub(timing.last_event_at);
                let speed = if event.bytes > 0 && elapsed > 0 {
                    event.bytes.saturating_mul(1000) / elapsed
                } else {
                    0
                };
                timing.last_event_at = now_millis;
                speed
            };
            let mut state = shared.lock().expect("snapshot mutex");
            if let Some(record) = state
                .config
                .managed
                .shares
                .iter_mut()
                .find(|record| record.share_id == id)
            {
                let completed = matches!(event.phase.as_str(), "complete" | "error");
                record.phase = Some(event.phase);
                record.transferred_bytes = record.transferred_bytes.saturating_add(event.bytes);
                if event.bytes > 0 {
                    record.speed_bps = speed;
                }
                if completed {
                    record.active_peer_count = 0;
                    record.connected_devices.clear();
                    record.speed_bps = 0;
                } else if let Some(peer) = event.peer {
                    record.active_peer_count = 1;
                    if let Some((member_id, permission)) = peer_handles.get(&peer) {
                        record.connected_devices = vec![ConnectedDeviceView {
                            member_id: member_id.clone(),
                            permission: *permission,
                            active_operations: 1,
                            last_seen_at: Some(managed_now()),
                        }];
                    }
                }
                state.revision += 1;
            }
        })
    }

    async fn install_owner_observer(&self, share: ShareId, owner: &OwnerShare) -> Result<()> {
        let mut peer_handles = BTreeMap::new();
        for member in owner.members()? {
            peer_handles.insert(
                member.endpoint.to_string(),
                (
                    self.member_handle(share, member.endpoint).await?,
                    member.permission,
                ),
            );
        }
        owner.set_observer(Some(self.managed_observer(share, peer_handles)));
        Ok(())
    }

    async fn schedule_revocation_drain(
        &self,
        pending: config::PendingRevocation,
        owner: OwnerShare,
    ) -> Result<()> {
        let peer = endpoint_id(&pending.endpoint)?;
        ensure!(
            owner.config().share_id == parse_share_id(&pending.share_id)?,
            ShareError::StateUnavailable
        );
        let mut tasks = self.revocation_tasks.lock().await;
        if tasks
            .iter()
            .any(|task| task.pending.request_id == pending.request_id)
        {
            return Ok(());
        }
        let task_owner = owner.clone();
        let task = tokio::spawn(async move { task_owner.drain_member(peer).await });
        tasks.push(RevocationTask { pending, task });
        Ok(())
    }

    async fn complete_revocation(&self, pending: &config::PendingRevocation) -> Result<()> {
        self.persist(|config| {
            config.managed.revocations.retain(|item| {
                !(item.request_id == pending.request_id
                    && item.share_id == pending.share_id
                    && item.member_id == pending.member_id)
            });
            Ok(())
        })
        .await
    }

    async fn retry_revocation(&self, pending: &config::PendingRevocation) -> Result<()> {
        let retry_at = self.managed_time()?.saturating_add(MANAGED_TICK_SECONDS);
        self.persist(|config| {
            if let Some(item) = config
                .managed
                .revocations
                .iter_mut()
                .find(|item| item.request_id == pending.request_id)
            {
                item.retry_at = Some(retry_at);
            }
            Ok(())
        })
        .await
    }

    async fn restore_pending_revocations(&self) -> Result<()> {
        let pending = self
            .shared
            .lock()
            .expect("snapshot mutex")
            .config
            .managed
            .revocations
            .clone();
        for item in pending {
            let share = parse_share_id(&item.share_id)?;
            let owner = self.owner_share(share).await?;
            self.schedule_revocation_drain(item, owner).await?;
        }
        Ok(())
    }

    async fn tick_revocations(&self) -> Result<()> {
        let _mutation = self.managed_mutations.lock().await;
        let mut finished = Vec::new();
        {
            let mut tasks = self.revocation_tasks.lock().await;
            let mut active = Vec::with_capacity(tasks.len());
            for task in tasks.drain(..) {
                if task.task.is_finished() {
                    finished.push(task);
                } else {
                    active.push(task);
                }
            }
            *tasks = active;
        }
        for item in finished {
            match item.task.await {
                Ok(Ok(())) => {
                    if let Err(error) = self.complete_revocation(&item.pending).await {
                        self.record_managed_error(error);
                    }
                }
                Ok(Err(error)) => {
                    if let Err(error) = self.retry_revocation(&item.pending).await {
                        self.record_managed_error(error);
                    }
                    self.record_managed_error(error);
                }
                Err(error) => {
                    let error = anyhow::anyhow!("revocation drain task failed: {error}");
                    if let Err(retry_error) = self.retry_revocation(&item.pending).await {
                        self.record_managed_error(retry_error);
                    }
                    self.record_managed_error(error);
                }
            }
        }
        let now = self.managed_time()?;
        let pending = self
            .shared
            .lock()
            .expect("snapshot mutex")
            .config
            .managed
            .revocations
            .clone();
        for item in pending {
            if item.retry_at.is_some_and(|retry_at| retry_at > now) {
                continue;
            }
            let share = match parse_share_id(&item.share_id) {
                Ok(share) => share,
                Err(error) => {
                    self.record_managed_error(error);
                    continue;
                }
            };
            let owner = match self.owner_share(share).await {
                Ok(owner) => owner,
                Err(error) => {
                    if let Err(retry_error) = self.retry_revocation(&item).await {
                        self.record_managed_error(retry_error);
                    }
                    self.record_managed_error(error);
                    continue;
                }
            };
            if let Err(error) = self.schedule_revocation_drain(item.clone(), owner).await {
                if let Err(retry_error) = self.retry_revocation(&item).await {
                    self.record_managed_error(retry_error);
                }
                self.record_managed_error(error);
            }
        }
        Ok(())
    }

    async fn await_revocation_tasks(&self, share_id: Option<&str>) -> Result<()> {
        let selected = {
            let mut tasks = self.revocation_tasks.lock().await;
            let mut selected = Vec::new();
            let mut remaining = Vec::with_capacity(tasks.len());
            for task in tasks.drain(..) {
                if share_id.is_none_or(|share| task.pending.share_id == share) {
                    selected.push(task);
                } else {
                    remaining.push(task);
                }
            }
            *tasks = remaining;
            selected
        };
        let mut failure = None;
        for item in selected {
            match item.task.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failure = Some(error),
                Err(error) => {
                    failure = Some(anyhow::anyhow!("revocation drain task failed: {error}"))
                }
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        Ok(())
    }

    fn member_observer(&self, share: ShareId) -> deltaweave_net::TransferObserver {
        self.managed_observer(share, BTreeMap::new())
    }

    async fn restore_missing_managed_workers(&self) -> Result<()> {
        // Worker construction performs network/session and filesystem awaits.
        // Keep the same mutation lock through the snapshot and publication so
        // remove_share cannot commit a tombstone and then have this stale
        // worker reappear after its await points.
        let _mutation = self.managed_mutations.lock().await;
        let now = self.managed_time()?;
        let records = self
            .shared
            .lock()
            .expect("snapshot mutex")
            .config
            .managed
            .shares
            .clone();
        let tombstones = self
            .shared
            .lock()
            .expect("snapshot mutex")
            .config
            .managed
            .tombstones
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        for record in records {
            if tombstones.contains(&record.share_id)
                || matches!(
                    record.status,
                    ManagedStatus::Paused | ManagedStatus::Revoked
                )
                || record.retry_at.is_some_and(|retry_at| retry_at > now)
                || self
                    .managed_slots
                    .lock()
                    .expect("managed slots mutex")
                    .contains_key(&record.share_id)
            {
                continue;
            }
            let share = parse_share_id(&record.share_id)?;
            let service = self.ensure_managed_service().await?;
            let worker = match record.role {
                ShareRole::Owner => match service.load_owned_share(share).await {
                    Ok(owner) => {
                        if let Err(error) = self.install_owner_observer(share, &owner).await {
                            owner.pause().await;
                            Err(error)
                        } else {
                            Ok(ManagedWorker::Owner(owner))
                        }
                    }
                    Err(error) => Err(error),
                },
                ShareRole::Member => {
                    let owner = record
                        .owner_address
                        .as_ref()
                        .map(|address| address.id)
                        .ok_or_else(|| {
                            anyhow::Error::new(ManagedError::new(ManagedErrorKind::InvalidInput))
                        });
                    match owner {
                        Ok(owner) => ManagedSyncEngine::resume(
                            &service,
                            owner,
                            share,
                            ManagedSyncConfig {
                                root: PathBuf::from(&record.root),
                                state_root: PathBuf::from(&record.state_root),
                                profile: deltaweave_core::ChunkingProfile::default(),
                                min_free_space_bytes: record.min_free_space_bytes,
                            },
                        )
                        .map(ManagedWorker::Member),
                        Err(error) => Err(error),
                    }
                }
            };
            match worker {
                Ok(worker) => {
                    self.managed_slots
                        .lock()
                        .expect("managed slots mutex")
                        .entry(record.share_id)
                        .or_insert_with(|| {
                            Arc::new(ManagedSlot {
                                operation: AsyncMutex::new(Some(worker)),
                            })
                        });
                }
                Err(error) => {
                    self.update_managed_failure(share, &error).await?;
                }
            }
        }
        Ok(())
    }

    async fn tick_managed(&self) -> Result<()> {
        // See recover_managed: preserve manual workers during a managed-only
        // clock quarantine and retry recovery after the durable high-water
        // value becomes reachable again.
        if let Err(error) = self.managed_time() {
            if error
                .downcast_ref::<ManagedError>()
                .is_some_and(|error| error.kind() == ManagedErrorKind::ClockRollback)
            {
                return Ok(());
            }
            return Err(error);
        }
        self.gc_pending_tickets().await?;
        self.gc_key_responses()?;
        self.tick_revocations().await?;
        self.restore_missing_managed_workers().await?;
        let slots = self
            .managed_slots
            .lock()
            .expect("managed slots mutex")
            .iter()
            .map(|(id, slot)| (id.clone(), slot.clone()))
            .collect::<Vec<_>>();
        for (id, slot) in slots {
            let share = match parse_share_id(&id) {
                Ok(share) => share,
                Err(error) => {
                    self.record_managed_error(error);
                    continue;
                }
            };
            let pending = self
                .shared
                .lock()
                .expect("snapshot mutex")
                .config
                .managed
                .pending
                .iter()
                .find(|pending| pending.share_id == id)
                .cloned();
            if pending.is_some() {
                drop(pending);
                drop(slot);
                if let Err(error) = self.retry_pending(&id, None, false).await {
                    let device_wide = error.downcast_ref::<ManagedError>().is_some_and(|error| {
                        matches!(
                            error.kind(),
                            ManagedErrorKind::ClockRollback | ManagedErrorKind::ShuttingDown
                        )
                    });
                    if device_wide {
                        return Err(error);
                    }
                    self.record_managed_error(error);
                }
                continue;
            }
            let status = self
                .managed_record(share)
                .map(|record| record.status)
                .unwrap_or(ManagedStatus::Error);
            if matches!(status, ManagedStatus::Paused | ManagedStatus::Revoked) {
                continue;
            }
            let mut operation = slot.operation.lock().await;
            let Some(worker) = operation.as_mut() else {
                continue;
            };
            match worker {
                ManagedWorker::Owner(owner) => {
                    if let Err(error) = self.update_owner_inventory(share, owner).await {
                        self.mark_managed_memory_failure(share, &error);
                        self.record_managed_error(error);
                    }
                }
                ManagedWorker::Member(engine) => {
                    match engine.sync_once(Some(self.member_observer(share))).await {
                        Ok(report) => match engine.inventory() {
                            Ok(inventory) => {
                                if let Err(error) =
                                    self.persist_member_report(share, &report, inventory).await
                                {
                                    self.mark_managed_memory_failure(share, &error);
                                    self.record_managed_error(error);
                                }
                            }
                            Err(error) => {
                                self.mark_managed_memory_failure(share, &error);
                                self.record_managed_error(error);
                            }
                        },
                        Err(error) => {
                            if let Err(error) = self.persist_member_error(share, error).await {
                                self.mark_managed_memory_failure(share, &error);
                                self.record_managed_error(error);
                            }
                        }
                    }
                }
                ManagedWorker::Pending(_) => {}
            }
        }
        Ok(())
    }

    async fn retry_pending(
        &self,
        id: &str,
        expected_request_id: Option<&str>,
        // An authenticated user retry may bypass the background backoff;
        // ticker calls keep it so an offline owner cannot be hammered.
        force: bool,
    ) -> Result<()> {
        let _mutation = self.managed_mutations.lock().await;
        let pending = {
            let state = self.shared.lock().expect("snapshot mutex");
            state
                .config
                .managed
                .pending
                .iter()
                .find(|pending| pending.share_id == id)
                .cloned()
        };
        let Some(pending) = pending else {
            return Ok(());
        };
        if let Some(expected_request_id) = expected_request_id {
            ensure!(
                pending.request_id == expected_request_id,
                ManagedError::new(ManagedErrorKind::IdempotencyConflict)
            );
        }
        let now = self.managed_time()?;
        if !force && pending.retry_at.is_some_and(|retry_at| retry_at > now) {
            return Ok(());
        }
        let address = pending
            .owner_address
            .clone()
            .ok_or_else(|| anyhow::Error::new(ManagedError::new(ManagedErrorKind::InvalidInput)))?;
        let share = parse_share_id(id)?;
        self.install_pending_slot(&pending).await?;
        let service = self.ensure_managed_service().await?;
        let member = match service
            .resume_membership(address.id, share, address.clone())
            .await
        {
            Ok(member) => member,
            Err(error) if is_share_error(&error, ShareError::NotMember) => {
                if Self::pending_deadline(&pending) <= now {
                    self.terminalize_pending(&pending, pending_terminal_ref(None, id))
                        .await?;
                    return Ok(());
                }
                let ticket_path = self.pending_ticket_path(&pending)?;
                let encoded = match Self::read_private_text(&ticket_path, 32 * 1024) {
                    Ok(encoded) => encoded,
                    Err(error) => {
                        if is_terminal_pending_storage_error(&error) {
                            self.terminalize_pending(
                                &pending,
                                pending_terminal_ref(Some(&error), id),
                            )
                            .await?;
                        }
                        return Err(error);
                    }
                };
                let ticket = match ShareTicket::parse(&encoded) {
                    Ok(ticket) if Self::pending_deadline(&pending) > now => ticket,
                    Ok(_) | Err(ShareError::Expired) => {
                        self.terminalize_pending(&pending, pending_terminal_ref(None, id))
                            .await?;
                        return Ok(());
                    }
                    Err(error) => {
                        let error = anyhow::Error::new(error);
                        if is_terminal_pending_error(&error) {
                            self.terminalize_pending(
                                &pending,
                                pending_terminal_ref(Some(&error), id),
                            )
                            .await?;
                        }
                        return Err(error);
                    }
                };
                match service.enroll(&ticket, None).await {
                    Ok(member) => member,
                    Err(error) if is_share_error(&error, ShareError::Offline) => {
                        self.update_pending_retry(&pending).await?;
                        return Ok(());
                    }
                    Err(error) if is_terminal_pending_error(&error) => {
                        self.terminalize_pending(&pending, pending_terminal_ref(Some(&error), id))
                            .await?;
                        return Err(error);
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(error) if is_share_error(&error, ShareError::Offline) => {
                self.update_pending_retry(&pending).await?;
                return Ok(());
            }
            Err(error) if is_terminal_pending_error(&error) => {
                self.terminalize_pending(&pending, pending_terminal_ref(Some(&error), id))
                    .await?;
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        let slot = self
            .managed_slots
            .lock()
            .expect("managed slots mutex")
            .get(id)
            .cloned()
            .ok_or_else(|| anyhow::Error::new(ManagedError::new(ManagedErrorKind::Busy)))?;
        let taken = { slot.operation.lock().await.take() };
        let lease = match taken {
            Some(ManagedWorker::Pending(pending_worker)) => pending_worker.lease,
            Some(other) => {
                *slot.operation.lock().await = Some(other);
                return Ok(());
            }
            None => None,
        };
        match self
            .complete_pending_membership(pending.clone(), member, service, lease)
            .await
        {
            Ok(worker) => {
                *slot.operation.lock().await = Some(worker);
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    /// Retries the durable join request identified by `request_id`.
    ///
    /// A pending join is intentionally different from `resume_membership`:
    /// the latter only applies to a member row which is already present in
    /// the local managed catalog, while this operation also covers the
    /// response-loss window where only the durable pending row remains.  The
    /// bearer is read from the private pending-ticket file by
    /// `retry_pending`; callers never submit it again and a retry never
    /// allocates a new replica or invitation.
    pub async fn retry_pending_join(&self, input: RetryPendingJoinInput) -> Result<JoinResult> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        validate_request_id(&input.request_id)?;
        let share_id = share_id_string(input.share);
        let request = self
            .request_record(&input.request_id)
            .ok_or_else(|| anyhow::Error::new(ManagedError::new(ManagedErrorKind::NotFound)))?;
        ensure!(
            request.operation == "join_share",
            ManagedError::new(ManagedErrorKind::IdempotencyConflict)
        );

        let (kind, recorded_share) = request
            .result_ref
            .split_once(':')
            .ok_or_else(|| anyhow::Error::new(ShareError::StateUnavailable))?;
        ensure!(
            recorded_share == share_id,
            ManagedError::new(ManagedErrorKind::IdempotencyConflict)
        );

        match kind {
            "share" => {
                // The request already completed before the response arrived.
                // Reconstruct the stable result from the persisted managed row.
                let record = self.managed_record(input.share)?;
                Ok(Self::join_result_from_record(input.request_id, record))
            }
            "pending" => {
                // Let the helper take the mutation lock and validate the
                // expected request ID.  Do not preflight the pending row under
                // a separate snapshot lock: the background ticker may finish
                // this request between that read and the helper.  The journal
                // and managed row below are the authoritative post-operation
                // state in either ordering.
                if let Err(error) = self
                    .retry_pending(&share_id, Some(&input.request_id), true)
                    .await
                {
                    // Terminalization records the stable result before the
                    // helper returns the protocol error.  Prefer that
                    // journal value so invitation revocation and expiry use
                    // the same public mapping as a retry after response loss.
                    if is_terminal_pending_error(&error)
                        && let Some(terminal) = self.request_record(&input.request_id)
                        && !terminal.result_ref.starts_with("pending:")
                    {
                        return Self::pending_result_error(&terminal.result_ref, &share_id);
                    }
                    return Err(error);
                }

                let state = self.shared.lock().expect("snapshot mutex");
                if let Some(record) = state
                    .config
                    .managed
                    .shares
                    .iter()
                    .find(|record| record.share_id == share_id)
                    .cloned()
                {
                    return Ok(Self::join_result_from_record(input.request_id, record));
                }
                if let Some(pending) = state
                    .config
                    .managed
                    .pending
                    .iter()
                    .find(|pending| {
                        pending.request_id == input.request_id && pending.share_id == share_id
                    })
                    .cloned()
                {
                    return Ok(Self::waiting_join_result(&pending));
                }
                drop(state);
                // `retry_pending` may have terminalized an expired or
                // revoked ticket while returning success.  Read the journal
                // again and expose its stable, credential-free error.
                let terminal = self
                    .request_record(&input.request_id)
                    .ok_or_else(|| anyhow::Error::new(ShareError::StateUnavailable))?;
                Self::pending_result_error(&terminal.result_ref, &share_id)
            }
            "revoked" => Err(ShareError::MemberRevoked.into()),
            "invalid" => Err(ShareError::InvalidTicket.into()),
            "expired" => Err(ManagedError::new(ManagedErrorKind::PendingExpired).into()),
            _ => Err(ShareError::StateUnavailable.into()),
        }
    }

    async fn update_pending_retry(&self, pending: &config::PendingRecord) -> Result<()> {
        let retry_at = self.managed_time()?.saturating_add(MANAGED_TICK_SECONDS);
        self.persist(|config| {
            if let Some(item) = config
                .managed
                .pending
                .iter_mut()
                .find(|item| item.request_id == pending.request_id)
            {
                item.status = ManagedStatus::Waiting;
                item.retry_at = Some(retry_at);
            }
            Ok(())
        })
        .await
    }

    async fn update_managed_failure(&self, share: ShareId, error: &anyhow::Error) -> Result<()> {
        let id = share_id_string(share);
        let summary = classify_managed_error(error);
        let offline = summary.code == "offline";
        self.persist(|config| {
            if let Some(record) = config
                .managed
                .shares
                .iter_mut()
                .find(|record| record.share_id == id)
            {
                record.status = if summary.code == "member_revoked" {
                    ManagedStatus::Revoked
                } else if offline {
                    ManagedStatus::Offline
                } else {
                    ManagedStatus::Error
                };
                record.phase = Some(
                    if summary.code == "member_revoked" {
                        "revoked"
                    } else {
                        "error"
                    }
                    .into(),
                );
                record.retry_at = if summary.code == "member_revoked" {
                    None
                } else {
                    Some(managed_now().saturating_add(MANAGED_TICK_SECONDS))
                };
                record.last_error = Some(summary.clone());
            }
            Ok(())
        })
        .await
    }
    fn resolve_device(&self, mut input: FolderInput) -> Result<FolderInput> {
        if let Some(id) = input.device_id.as_ref() {
            let state = self.shared.lock().expect("snapshot mutex");
            let device = state
                .config
                .devices
                .iter()
                .find(|device| &device.id == id)
                .context("selected device does not exist")?;
            if input.role == "sync" {
                if input.peer_endpoint_id.as_deref().is_none_or(str::is_empty) {
                    input.peer_endpoint_id = Some(device.input.endpoint_id.clone());
                }
                if input.direct_addresses.is_empty() {
                    input.direct_addresses.push(device.input.address.clone());
                }
                ensure!(
                    input.peer_endpoint_id.as_deref() == Some(device.input.endpoint_id.as_str()),
                    "peer endpoint ID does not match selected device"
                );
            }
        }
        Ok(input)
    }
    pub async fn snapshot(&self) -> AppSnapshot {
        let state = self.shared.lock().expect("snapshot mutex");
        let folders: Vec<_> = state.folders.values().cloned().collect();
        let shares = state
            .config
            .managed
            .shares
            .iter()
            .map(Self::managed_view)
            .collect();
        let pending = state
            .config
            .managed
            .pending
            .iter()
            .map(|pending| PendingView {
                request_id: pending.request_id.clone(),
                share_id: pending.share_id.clone(),
                status: pending.status,
                created_at: pending.created_at,
                retry_at: pending.retry_at,
            })
            .collect();
        let totals = Totals {
            folders: folders.len(),
            active_folders: folders
                .iter()
                .filter(|f| !matches!(f.status.as_str(), "paused" | "stopped" | "error"))
                .count(),
            files: folders.iter().map(|f| f.files_count).sum(),
            bytes: folders.iter().map(|f| f.total_bytes).sum(),
            pushed_bytes: state.history.iter().map(|h| h.pushed_bytes).sum(),
            pulled_bytes: state.history.iter().map(|h| h.pulled_bytes).sum(),
            conflicts: folders
                .iter()
                .filter_map(|f| f.last_report.as_ref()?.get("conflicts")?.as_array())
                .map(Vec::len)
                .sum(),
        };
        AppSnapshot {
            node: Node {
                name: state.config.settings.node_name.clone(),
                version: env!("CARGO_PKG_VERSION").into(),
                platform: std::env::consts::OS.into(),
                started_at: self.started_at,
                uptime_seconds: now().saturating_sub(self.started_at) / 1000,
            },
            folders,
            devices: state.config.devices.clone(),
            activities: state.activities.iter().rev().cloned().collect(),
            history: state.history.clone(),
            totals,
            settings: state.config.settings.clone(),
            revision: state.revision,
            shares,
            pending,
        }
    }

    async fn owner_share(&self, share: ShareId) -> Result<OwnerShare> {
        let record = self.managed_record(share)?;
        ensure!(
            record.role == ShareRole::Owner,
            ManagedError::new(ManagedErrorKind::OwnerOnly)
        );
        let id = share_id_string(share);
        let slot = self
            .managed_slots
            .lock()
            .expect("managed slots mutex")
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow::Error::new(ManagedError::new(ManagedErrorKind::Busy)))?;
        let operation = slot.operation.lock().await;
        match operation.as_ref() {
            Some(ManagedWorker::Owner(owner)) => Ok(owner.clone()),
            Some(ManagedWorker::Pending(_) | ManagedWorker::Member(_)) | None => {
                Err(ManagedError::new(ManagedErrorKind::OwnerOnly).into())
            }
        }
    }

    fn mutation_result(&self, request_id: String, share: ShareId) -> Result<MutationResult> {
        let record = self.managed_record(share)?;
        let (pending, member_revoke) = {
            let state = self.shared.lock().expect("snapshot mutex");
            let pending = state
                .config
                .managed
                .revocations
                .iter()
                .find(|pending| pending.request_id == request_id)
                .cloned();
            let member_revoke = state
                .config
                .managed
                .requests
                .iter()
                .find(|request| request.request_id == request_id)
                .is_some_and(|request| request.operation == "revoke_member");
            (pending, member_revoke)
        };
        let completion_pending = pending.is_some();
        let retry_at = pending.as_ref().and_then(|pending| pending.retry_at);
        Ok(MutationResult {
            request_id,
            accepted: true,
            completion: if completion_pending {
                MutationCompletion::Pending
            } else {
                MutationCompletion::Complete
            },
            retry_at,
            status: if completion_pending {
                ManagedStatus::Waiting
            } else if member_revoke {
                ManagedStatus::Revoked
            } else {
                record.status
            },
        })
    }

    fn removed_mutation_result(request_id: String) -> MutationResult {
        MutationResult {
            request_id,
            accepted: true,
            completion: MutationCompletion::Complete,
            retry_at: None,
            status: ManagedStatus::Complete,
        }
    }

    async fn persist_member_report(
        &self,
        share: ShareId,
        report: &ManagedSyncReport,
        inventory: deltaweave_net::Inventory,
    ) -> Result<()> {
        let id = share_id_string(share);
        let (transferred, conflict, status) = match report {
            ManagedSyncReport::ReadWrite(report) => (
                report.pulled_bytes.saturating_add(report.pushed_bytes),
                !report.conflicts.is_empty(),
                report.status,
            ),
            ManagedSyncReport::ReadOnly(report) => (
                report.pulled_bytes,
                !report.preserved.is_empty(),
                report.status,
            ),
        };
        self.persist(|config| {
            let record = config
                .managed
                .shares
                .iter_mut()
                .find(|record| record.share_id == id)
                .ok_or_else(|| anyhow::Error::new(ManagedError::new(ManagedErrorKind::NotFound)))?;
            if record.status == ManagedStatus::Paused || record.status == ManagedStatus::Revoked {
                return Ok(());
            }
            record.status = if conflict {
                ManagedStatus::Conflict
            } else if matches!(status, "complete" | "pass") {
                ManagedStatus::Complete
            } else {
                ManagedStatus::InitialSync
            };
            record.phase = Some(status.into());
            record.last_sync_at = Some(managed_now());
            record.retry_at = None;
            record.files_count = inventory.files;
            record.total_bytes = inventory.bytes;
            record.transferred_bytes = record.transferred_bytes.saturating_add(transferred);
            record.last_error = None;
            Ok(())
        })
        .await
    }

    async fn persist_member_error(&self, share: ShareId, error: anyhow::Error) -> Result<()> {
        let summary = classify_managed_error(&error);
        let id = share_id_string(share);
        self.persist(|config| {
            if let Some(record) = config
                .managed
                .shares
                .iter_mut()
                .find(|record| record.share_id == id)
                && record.status != ManagedStatus::Paused
                && record.status != ManagedStatus::Revoked
            {
                record.status = if summary.code == "member_revoked" {
                    ManagedStatus::Revoked
                } else if summary.code == "offline" {
                    ManagedStatus::Offline
                } else {
                    ManagedStatus::Error
                };
                record.phase = Some(
                    if summary.code == "member_revoked" {
                        "revoked"
                    } else {
                        "error"
                    }
                    .into(),
                );
                record.retry_at = if summary.code == "member_revoked" {
                    None
                } else {
                    Some(managed_now().saturating_add(MANAGED_TICK_SECONDS))
                };
                record.last_error = Some(summary);
            }
            Ok(())
        })
        .await
    }

    pub async fn create_share(&self, input: CreateShareInput) -> Result<ShareView> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        validate_request_id(&input.request_id)?;
        config::validate_name(&input.name)
            .map_err(|_| anyhow::Error::new(ManagedError::new(ManagedErrorKind::InvalidInput)))?;
        let hash = request_hash("create_share", &input)?;
        if let Some(result_ref) = self.request_lookup(&input.request_id, "create_share", &hash)? {
            let share = parse_share_id(result_ref.strip_prefix("share:").unwrap_or(&result_ref))?;
            return self.managed_view_for(share);
        }
        let _mutation = self.managed_mutations.lock().await;
        if let Some(result_ref) = self.request_lookup(&input.request_id, "create_share", &hash)? {
            let share = parse_share_id(result_ref.strip_prefix("share:").unwrap_or(&result_ref))?;
            return self.managed_view_for(share);
        }
        self.managed_time()?;
        self.ensure_request_capacity(&input.request_id)?;
        let root = config::candidate(&input.root)
            .map_err(|_| anyhow::Error::new(ManagedError::new(ManagedErrorKind::InvalidPath)))?;
        ensure!(
            root != self.data_dir && !root.starts_with(&self.data_dir),
            ManagedError::new(ManagedErrorKind::InvalidPath)
        );
        if root.exists() {
            ensure!(
                root.is_dir(),
                ManagedError::new(ManagedErrorKind::InvalidPath)
            );
        }
        let state_root = self
            .data_dir
            .join("managed")
            .join("share-state")
            .join(&hash);
        self.managed_paths_conflict(&root, &state_root)?;
        root_admission::reserve_private(&state_root).context("reserve managed owner state")?;
        private::prepare_directory(&state_root)?;
        let min_free_space_bytes = input
            .min_free_space_mib
            .unwrap_or(0)
            .checked_mul(1024 * 1024)
            .ok_or_else(|| anyhow::Error::new(ManagedError::new(ManagedErrorKind::InvalidInput)))?;
        let root_string = root.to_string_lossy().into_owned();
        let state_root_string = state_root.to_string_lossy().into_owned();
        let existing_intent = {
            let state = self.shared.lock().expect("snapshot mutex");
            state
                .config
                .managed
                .intents
                .iter()
                .find(|intent| intent.request_id == input.request_id)
                .cloned()
        };
        if let Some(intent) = &existing_intent {
            ensure!(
                intent.request_hash == hash
                    && intent.name == input.name
                    && intent.root == root_string
                    && intent.state_root == state_root_string,
                ManagedError::new(ManagedErrorKind::IdempotencyConflict)
            );
        } else {
            self.persist(|config| {
                config.managed.intents.push(config::CreateIntent {
                    request_id: input.request_id.clone(),
                    request_hash: hash.clone(),
                    name: input.name.clone(),
                    root: root_string.clone(),
                    state_root: state_root_string.clone(),
                    owner: None,
                    created_at: managed_now(),
                });
                Ok(())
            })
            .await
            .context("persist managed create intent")?;
        }
        let service = self
            .ensure_managed_service()
            .await
            .context("open managed share service")?;
        let owner_id = service.endpoint_id();
        self.persist(|config| {
            let intent = config
                .managed
                .intents
                .iter_mut()
                .find(|intent| intent.request_id == input.request_id)
                .ok_or_else(|| anyhow::Error::new(ManagedError::new(ManagedErrorKind::NotFound)))?;
            if let Some(previous) = intent.owner.as_deref() {
                ensure!(previous == owner_id.to_string(), ShareError::OwnerMismatch);
            } else {
                intent.owner = Some(owner_id.to_string());
            }
            Ok(())
        })
        .await
        .context("persist managed owner identity")?;
        let candidates = service
            .owned_configs()
            .context("read managed owner catalog")?
            .into_iter()
            .filter(|candidate| {
                candidate.name == input.name
                    && candidate.root == root
                    && candidate.state_root == state_root
                    && candidate.owner == owner_id
                    && candidate.min_free_space_bytes == min_free_space_bytes
            })
            .collect::<Vec<_>>();
        ensure!(candidates.len() <= 1, ShareError::StateUnavailable);
        let owner = match if let Some(candidate) = candidates.into_iter().next() {
            service.load_owned_share(candidate.share_id).await
        } else {
            service
                .create_owned_share(
                    input.name.clone(),
                    root.clone(),
                    state_root.clone(),
                    None,
                    min_free_space_bytes,
                )
                .await
        }
        .context("create or recover managed owner share")
        {
            Ok(owner) => owner,
            Err(error) => return Err(error),
        };
        let inventory = owner.inventory().context("read managed owner inventory")?;
        let id = owner.config().share_id;
        let record = config::ManagedShareRecord {
            share_id: share_id_string(id),
            role: ShareRole::Owner,
            permission: None,
            name: owner.config().name.clone(),
            root: owner.config().root.to_string_lossy().into_owned(),
            state_root: owner.config().state_root.to_string_lossy().into_owned(),
            owner: owner.config().owner.to_string(),
            min_free_space_bytes: owner.config().min_free_space_bytes,
            owner_address: Some(service.endpoint_addr()),
            member_id: None,
            replica: Some(owner.config().replica),
            enrolled_at: None,
            membership_epoch: None,
            revocation_pending: false,
            status: ManagedStatus::Complete,
            phase: Some("complete".into()),
            last_sync_at: Some(managed_now()),
            retry_at: None,
            files_count: inventory.files,
            total_bytes: inventory.bytes,
            transferred_bytes: 0,
            speed_bps: 0,
            active_peer_count: 0,
            connected_devices: Vec::new(),
            last_error: None,
        };
        self.persist(|config| {
            config.managed.shares.push(record.clone());
            config
                .managed
                .intents
                .retain(|intent| intent.request_id != input.request_id);
            config.managed.requests.push(config::RequestRecord {
                request_id: input.request_id.clone(),
                operation: "create_share".into(),
                request_hash: hash.clone(),
                result_ref: format!("share:{}", record.share_id),
                recorded_at: managed_now(),
            });
            Ok(())
        })
        .await
        .context("persist managed owner record")?;
        self.install_owner_observer(id, &owner).await?;
        self.managed_slots
            .lock()
            .expect("managed slots mutex")
            .insert(
                record.share_id.clone(),
                Arc::new(ManagedSlot {
                    operation: AsyncMutex::new(Some(ManagedWorker::Owner(owner))),
                }),
            );
        Ok(Self::managed_view(&record))
    }

    pub async fn preview_share_key(&self, input: PreviewKeyInput) -> Result<KeyPreview> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        validate_request_id(&input.request_id)?;
        let hash = request_hash("preview_share_key", &input)?;
        if self
            .request_lookup(&input.request_id, "preview_share_key", &hash)?
            .is_some()
        {
            // Re-parse the caller's credential to reconstruct the stable,
            // bearer-free result; no enrollment or network service is opened.
        } else {
            let _mutation = self.managed_mutations.lock().await;
            if self
                .request_lookup(&input.request_id, "preview_share_key", &hash)?
                .is_some()
            {
                // Another caller committed this request while this call was
                // waiting for the mutation lock. Reconstruct the redacted
                // metadata below without parsing or storing a second result.
            } else {
                self.ensure_request_capacity(&input.request_id)?;
                let ticket = ShareTicket::parse(&input.encoded_key)?;
                let preview = ticket.preview();
                let result = KeyPreview {
                    share_id: share_id_string(preview.share_id),
                    name: preview.name,
                    permission: preview.permission,
                    invitation_id: invitation_id_string(preview.invitation_id),
                    expires_at: preview.expires_at,
                    signature_valid: true,
                    issuance: KeyIssuance::NotChecked,
                };
                self.record_request(
                    input.request_id,
                    "preview_share_key",
                    hash,
                    format!("preview:{}:{}", result.share_id, result.invitation_id),
                )
                .await?;
                return Ok(result);
            }
        }
        let ticket = ShareTicket::parse(&input.encoded_key)?;
        let preview = ticket.preview();
        Ok(KeyPreview {
            share_id: share_id_string(preview.share_id),
            name: preview.name,
            permission: preview.permission,
            invitation_id: invitation_id_string(preview.invitation_id),
            expires_at: preview.expires_at,
            signature_valid: true,
            issuance: KeyIssuance::NotChecked,
        })
    }

    pub async fn validate_share_key(&self, input: ValidateKeyInput) -> Result<KeyPreview> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        validate_request_id(&input.request_id)?;
        let hash = request_hash("validate_share_key", &input)?;
        if self
            .request_lookup(&input.request_id, "validate_share_key", &hash)?
            .is_some()
        {
            let ticket = ShareTicket::parse(&input.encoded_key)?;
            let preview = ticket.preview();
            return Ok(KeyPreview {
                share_id: share_id_string(preview.share_id),
                name: preview.name,
                permission: preview.permission,
                invitation_id: invitation_id_string(preview.invitation_id),
                expires_at: preview.expires_at,
                signature_valid: true,
                issuance: KeyIssuance::Validated,
            });
        }
        let _mutation = self.managed_mutations.lock().await;
        if self
            .request_lookup(&input.request_id, "validate_share_key", &hash)?
            .is_some()
        {
            let ticket = ShareTicket::parse(&input.encoded_key)?;
            let preview = ticket.preview();
            return Ok(KeyPreview {
                share_id: share_id_string(preview.share_id),
                name: preview.name,
                permission: preview.permission,
                invitation_id: invitation_id_string(preview.invitation_id),
                expires_at: preview.expires_at,
                signature_valid: true,
                issuance: KeyIssuance::Validated,
            });
        }
        self.managed_time()?;
        self.ensure_request_capacity(&input.request_id)?;
        let ticket = ShareTicket::parse(&input.encoded_key)?;
        let service = self.ensure_managed_service().await?;
        let preview = service.validate_ticket(&ticket).await?;
        let result = KeyPreview {
            share_id: share_id_string(preview.share_id),
            name: preview.name,
            permission: preview.permission,
            invitation_id: invitation_id_string(preview.invitation_id),
            expires_at: preview.expires_at,
            signature_valid: true,
            issuance: KeyIssuance::Validated,
        };
        self.record_request(
            input.request_id,
            "validate_share_key",
            hash,
            format!("validated:{}:{}", result.share_id, result.invitation_id),
        )
        .await?;
        Ok(result)
    }

    async fn complete_pending_membership(
        &self,
        pending: config::PendingRecord,
        member: NetMembership,
        service: Arc<ShareService>,
        lease: Option<Arc<RootLease>>,
    ) -> Result<ManagedWorker> {
        // A pending transition must already own the admission lease.  A
        // missing lease is corrupt/legacy state; opening a fresh lease here
        // would recreate the pending-to-active TOCTOU this handoff closes.
        let Some(lease) = lease else {
            return Err(ShareError::StateUnavailable.into());
        };
        let share = match parse_share_id(&pending.share_id) {
            Ok(share) => share,
            Err(error) => {
                if let Err(reinstall) = self
                    .install_pending_slot_with_lease(&pending, Arc::clone(&lease))
                    .await
                {
                    self.record_managed_error(reinstall);
                }
                return Err(error);
            }
        };
        let owner = match endpoint_id(&pending.owner) {
            Ok(owner) => owner,
            Err(error) => {
                if let Err(reinstall) = self
                    .install_pending_slot_with_lease(&pending, Arc::clone(&lease))
                    .await
                {
                    self.record_managed_error(reinstall);
                }
                return Err(error);
            }
        };
        if member.share_id != share || member.owner != owner || member.revoked_at.is_some() {
            let error = anyhow::Error::new(ShareError::MemberRevoked);
            if let Err(reinstall) = self
                .install_pending_slot_with_lease(&pending, Arc::clone(&lease))
                .await
            {
                self.record_managed_error(reinstall);
            }
            return Err(error);
        }
        let member_id = match self.member_handle(member.share_id, member.endpoint).await {
            Ok(member_id) => member_id,
            Err(error) => {
                if let Err(reinstall) = self
                    .install_pending_slot_with_lease(&pending, Arc::clone(&lease))
                    .await
                {
                    self.record_managed_error(reinstall);
                }
                return Err(error);
            }
        };
        let record = config::ManagedShareRecord {
            share_id: pending.share_id.clone(),
            role: ShareRole::Member,
            permission: Some(member.permission),
            name: pending.name.clone(),
            root: pending.root.clone(),
            state_root: pending.state_root.clone(),
            owner: member.owner.to_string(),
            min_free_space_bytes: pending.min_free_space_bytes,
            owner_address: pending.owner_address.clone(),
            member_id: Some(member_id),
            replica: Some(member.replica),
            enrolled_at: Some(member.enrolled_at),
            membership_epoch: Some(member.epoch),
            revocation_pending: false,
            status: ManagedStatus::InitialSync,
            phase: Some("initial_sync".into()),
            last_sync_at: None,
            retry_at: None,
            files_count: 0,
            total_bytes: 0,
            transferred_bytes: 0,
            speed_bps: 0,
            active_peer_count: 0,
            connected_devices: Vec::new(),
            last_error: None,
        };
        let request_hash = {
            let state = self.shared.lock().expect("snapshot mutex");
            state
                .config
                .managed
                .requests
                .iter()
                .find(|request| request.request_id == pending.request_id)
                .map(|request| request.request_hash.clone())
        };
        let share = member.share_id;
        let sync_config = ManagedSyncConfig {
            root: PathBuf::from(&pending.root),
            state_root: PathBuf::from(&pending.state_root),
            profile: deltaweave_core::ChunkingProfile::default(),
            min_free_space_bytes: pending.min_free_space_bytes,
        };
        // Transfer the PendingWorker lease directly into ReplicaState.  The
        // engine must not drop and reacquire it between enrollment and opening
        // its index/store: that gap would let another process claim either
        // root before the active member owns the binding.
        let engine_result = ManagedSyncEngine::open_with_lease(
            &service,
            owner,
            share,
            sync_config,
            Arc::clone(&lease),
        );
        let engine = match engine_result {
            Ok(engine) => engine,
            Err(error) => {
                // Keep the exact Arc-backed lease in the pending slot when
                // opening fails.  Reacquiring here would recreate the same
                // handoff gap this path is designed to close.
                if let Err(reinstall) = self
                    .install_pending_slot_with_lease(&pending, Arc::clone(&lease))
                    .await
                {
                    self.record_managed_error(reinstall);
                }
                return Err(error);
            }
        };
        match self
            .persist(|config| {
                if let Some(existing) = config
                    .managed
                    .shares
                    .iter_mut()
                    .find(|existing| existing.share_id == record.share_id)
                {
                    ensure!(
                        existing.role == record.role
                            && existing.owner == record.owner
                            && existing.replica == record.replica
                            && existing.permission == record.permission
                            && existing.enrolled_at == record.enrolled_at
                            && existing.membership_epoch == record.membership_epoch
                            && existing.name == record.name
                            && existing.root == record.root
                            && existing.state_root == record.state_root
                            && existing.min_free_space_bytes == record.min_free_space_bytes
                            && existing.owner_address == record.owner_address,
                        ManagedError::new(ManagedErrorKind::IdempotencyConflict)
                    );
                    *existing = record.clone();
                } else {
                    config.managed.shares.push(record.clone());
                }
                config
                    .managed
                    .pending
                    .retain(|item| item.request_id != pending.request_id);
                if let Some(request) = config
                    .managed
                    .requests
                    .iter_mut()
                    .find(|request| request.request_id == pending.request_id)
                {
                    request.result_ref = format!("share:{}", record.share_id);
                } else if let Some(request_hash) = request_hash.as_ref() {
                    config.managed.requests.push(config::RequestRecord {
                        request_id: pending.request_id.clone(),
                        operation: "join_share".into(),
                        request_hash: request_hash.clone(),
                        result_ref: format!("share:{}", record.share_id),
                        recorded_at: managed_now(),
                    });
                }
                Ok(())
            })
            .await
        {
            Ok(()) => {}
            Err(error) => {
                if let Err(shutdown_error) = engine.shutdown().await {
                    self.record_managed_error(shutdown_error);
                }
                if let Err(reinstall) = self
                    .install_pending_slot_with_lease(&pending, Arc::clone(&lease))
                    .await
                {
                    self.record_managed_error(reinstall);
                }
                return Err(error);
            }
        }
        if let Err(error) = self
            .pending_ticket_path(&pending)
            .and_then(|path| Self::remove_private_file(&path))
        {
            self.record_managed_error(error);
        }
        Ok(ManagedWorker::Member(engine))
    }

    fn join_result_from_record(
        request_id: String,
        record: config::ManagedShareRecord,
    ) -> JoinResult {
        JoinResult {
            request_id,
            share_id: record.share_id,
            enrollment: if record.status == ManagedStatus::Revoked {
                EnrollmentState::Revoked
            } else {
                EnrollmentState::Enrolled
            },
            status: record.status,
            permission: record.permission,
            member_id: record.member_id,
        }
    }

    fn pending_result_error(result_ref: &str, share_id: &str) -> Result<JoinResult> {
        let (kind, recorded_share) = result_ref
            .split_once(':')
            .ok_or_else(|| anyhow::Error::new(ShareError::StateUnavailable))?;
        ensure!(
            recorded_share == share_id,
            ManagedError::new(ManagedErrorKind::IdempotencyConflict)
        );
        match kind {
            "revoked" => Err(ShareError::MemberRevoked.into()),
            "invalid" => Err(ShareError::InvalidTicket.into()),
            "expired" => Err(ManagedError::new(ManagedErrorKind::PendingExpired).into()),
            _ => Err(ShareError::StateUnavailable.into()),
        }
    }

    fn waiting_join_result(pending: &config::PendingRecord) -> JoinResult {
        JoinResult {
            request_id: pending.request_id.clone(),
            share_id: pending.share_id.clone(),
            enrollment: EnrollmentState::Waiting,
            status: ManagedStatus::Waiting,
            permission: pending.permission,
            member_id: None,
        }
    }

    fn issued_key(request_id: String, encoded: String) -> Result<IssuedKey> {
        let ticket = ShareTicket::parse(&encoded)?;
        let preview = ticket.preview();
        Ok(IssuedKey {
            request_id,
            share_id: share_id_string(preview.share_id),
            invitation_id: invitation_id_string(preview.invitation_id),
            permission: preview.permission,
            expires_at: preview.expires_at,
            key: encoded,
        })
    }

    fn check_expiry(&self, expires_at: Option<u64>) -> Result<()> {
        if let Some(expires_at) = expires_at {
            let now = self.managed_time()?;
            ensure!(
                expires_at > now && expires_at <= now.saturating_add(30 * 24 * 60 * 60),
                ManagedError::new(ManagedErrorKind::InvalidInput)
            );
        }
        Ok(())
    }

    pub async fn join_share(&self, input: JoinShareInput) -> Result<JoinResult> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        validate_request_id(&input.request_id)?;
        let hash = request_hash("join_share", &input)?;
        let pending_existing = {
            let state = self.shared.lock().expect("snapshot mutex");
            state
                .config
                .managed
                .pending
                .iter()
                .find(|pending| pending.request_id == input.request_id)
                .cloned()
        };
        if let Some(result_ref) = self.request_lookup(&input.request_id, "join_share", &hash)?
            && pending_existing.is_none()
        {
            let Some(share_ref) = result_ref.strip_prefix("share:") else {
                return Err(if result_ref.starts_with("revoked:") {
                    ShareError::MemberRevoked.into()
                } else if result_ref.starts_with("invalid:") {
                    ShareError::InvalidTicket.into()
                } else {
                    ManagedError::new(ManagedErrorKind::PendingExpired).into()
                });
            };
            let share = parse_share_id(share_ref)?;
            let record = self.managed_record(share)?;
            return Ok(JoinResult {
                request_id: input.request_id,
                share_id: record.share_id,
                enrollment: if record.status == ManagedStatus::Revoked {
                    EnrollmentState::Revoked
                } else {
                    EnrollmentState::Enrolled
                },
                status: record.status,
                permission: record.permission,
                member_id: record.member_id,
            });
        }
        let _mutation = self.managed_mutations.lock().await;
        let pending_existing = {
            let state = self.shared.lock().expect("snapshot mutex");
            state
                .config
                .managed
                .pending
                .iter()
                .find(|pending| pending.request_id == input.request_id)
                .cloned()
        };
        if let Some(result_ref) = self.request_lookup(&input.request_id, "join_share", &hash)? {
            if pending_existing.is_none()
                && let Some(share_ref) = result_ref.strip_prefix("share:")
            {
                let share = parse_share_id(share_ref)?;
                let record = self.managed_record(share)?;
                return Ok(JoinResult {
                    request_id: input.request_id.clone(),
                    share_id: record.share_id,
                    enrollment: if record.status == ManagedStatus::Revoked {
                        EnrollmentState::Revoked
                    } else {
                        EnrollmentState::Enrolled
                    },
                    status: record.status,
                    permission: record.permission,
                    member_id: record.member_id,
                });
            }
            if pending_existing.is_none() {
                return Err(if result_ref.starts_with("revoked:") {
                    ShareError::MemberRevoked.into()
                } else if result_ref.starts_with("invalid:") {
                    ShareError::InvalidTicket.into()
                } else {
                    ManagedError::new(ManagedErrorKind::PendingExpired).into()
                });
            }
        }
        self.managed_time()?;
        self.ensure_request_capacity(&input.request_id)?;
        let (ticket, pending) = if let Some(pending) = pending_existing.clone() {
            let path = self.pending_ticket_path(&pending)?;
            let service = self.ensure_managed_service().await?;
            if let Some(address) = pending.owner_address.clone() {
                match service
                    .resume_membership(address.id, parse_share_id(&pending.share_id)?, address)
                    .await
                {
                    Ok(member) => {
                        let id = parse_share_id(&pending.share_id)?;
                        let lease = self.take_pending_lease(&pending.share_id).await?;
                        let worker = self
                            .complete_pending_membership(
                                pending.clone(),
                                member.clone(),
                                service,
                                lease,
                            )
                            .await;
                        let worker = match worker {
                            Ok(worker) => worker,
                            Err(error) => return Err(error),
                        };
                        let slot = self
                            .managed_slots
                            .lock()
                            .expect("managed slots mutex")
                            .entry(pending.share_id.clone())
                            .or_insert_with(|| {
                                Arc::new(ManagedSlot {
                                    operation: AsyncMutex::new(None),
                                })
                            })
                            .clone();
                        *slot.operation.lock().await = Some(worker);
                        return Ok(JoinResult {
                            request_id: input.request_id,
                            share_id: pending.share_id,
                            enrollment: EnrollmentState::Enrolled,
                            status: ManagedStatus::InitialSync,
                            permission: Some(member.permission),
                            member_id: Some(self.member_handle(id, member.endpoint).await?),
                        });
                    }
                    Err(error) if is_share_error(&error, ShareError::Offline) => {
                        self.install_pending_slot(&pending).await?;
                        return Ok(Self::waiting_join_result(&pending));
                    }
                    Err(error) if is_terminal_pending_error(&error) => {
                        self.terminalize_pending(
                            &pending,
                            pending_terminal_ref(Some(&error), &pending.share_id),
                        )
                        .await?;
                        return Err(error);
                    }
                    Err(error) if !is_share_error(&error, ShareError::NotMember) => {
                        self.install_pending_slot(&pending).await?;
                        return Err(error);
                    }
                    Err(_) => {}
                }
            }
            if Self::pending_deadline(&pending) <= self.managed_time()? {
                self.terminalize_pending(&pending, pending_terminal_ref(None, &pending.share_id))
                    .await?;
                return Err(ManagedError::new(ManagedErrorKind::PendingExpired).into());
            }
            let encoded = match Self::read_private_text(&path, 32 * 1024) {
                Ok(encoded) => encoded,
                Err(error) => {
                    if is_terminal_pending_storage_error(&error) {
                        self.terminalize_pending(
                            &pending,
                            pending_terminal_ref(Some(&error), &pending.share_id),
                        )
                        .await?;
                    }
                    return Err(error);
                }
            };
            let ticket = match ShareTicket::parse(&encoded) {
                Ok(ticket) => ticket,
                Err(error) => {
                    let error = anyhow::Error::new(error);
                    if is_terminal_pending_error(&error) {
                        self.terminalize_pending(
                            &pending,
                            pending_terminal_ref(Some(&error), &pending.share_id),
                        )
                        .await?;
                    }
                    return Err(error);
                }
            };
            (ticket, pending)
        } else {
            let ticket = ShareTicket::parse(&input.encoded_key)?;
            self.ensure_new_join_binding(ticket.preview().share_id, &input.request_id)?;
            let root = config::candidate(&input.destination_root).map_err(|_| {
                anyhow::Error::new(ManagedError::new(ManagedErrorKind::InvalidPath))
            })?;
            ensure!(
                root != self.data_dir && !root.starts_with(&self.data_dir),
                ManagedError::new(ManagedErrorKind::InvalidPath)
            );
            if root.exists() {
                ensure!(
                    root.is_dir(),
                    ManagedError::new(ManagedErrorKind::InvalidPath)
                );
            }
            let state_root = self
                .data_dir
                .join("managed")
                .join("member-state")
                .join(&hash);
            self.managed_paths_conflict(&root, &state_root)?;
            root_admission::reserve_private(&state_root)?;
            private::prepare_directory(&state_root)?;
            let lease = Arc::new(root_admission::acquire_with_private(
                &root,
                RootUse::Managed {
                    share: ticket.preview().share_id.0,
                    owner: *ticket.preview().owner.as_bytes(),
                },
                std::slice::from_ref(&state_root),
            )?);
            let ticket_path = self.private_text_path("pending", &hash, "ticket")?;
            Self::write_private_text(&ticket_path, &ticket.encode(), 32 * 1024)?;
            let now = self.managed_time()?;
            let expires_at = Some(
                ticket
                    .preview()
                    .expires_at
                    .unwrap_or(now.saturating_add(MANAGED_PENDING_MAX_SECONDS))
                    .min(now.saturating_add(MANAGED_PENDING_MAX_SECONDS)),
            );
            let pending = config::PendingRecord {
                request_id: input.request_id.clone(),
                share_id: share_id_string(ticket.preview().share_id),
                owner: ticket.preview().owner.to_string(),
                owner_address: Some(ticket.address()),
                name: ticket.preview().name.clone(),
                permission: Some(ticket.preview().permission),
                root: root.to_string_lossy().into_owned(),
                state_root: state_root.to_string_lossy().into_owned(),
                ticket_file: ticket_path.to_string_lossy().into_owned(),
                created_at: now,
                expires_at,
                status: ManagedStatus::Waiting,
                retry_at: Some(now),
                min_free_space_bytes: 0,
            };
            if let Err(error) = self
                .persist(|config| {
                    config
                        .managed
                        .pending
                        .retain(|item| item.request_id != input.request_id);
                    config.managed.pending.push(pending.clone());
                    if let Some(request) = config
                        .managed
                        .requests
                        .iter_mut()
                        .find(|request| request.request_id == input.request_id)
                    {
                        ensure!(
                            request.operation == "join_share" && request.request_hash == hash,
                            ManagedError::new(ManagedErrorKind::IdempotencyConflict)
                        );
                        request.result_ref = format!("pending:{}", pending.share_id);
                    } else {
                        config.managed.requests.push(config::RequestRecord {
                            request_id: input.request_id.clone(),
                            operation: "join_share".into(),
                            request_hash: hash.clone(),
                            result_ref: format!("pending:{}", pending.share_id),
                            recorded_at: now,
                        });
                    }
                    Ok(())
                })
                .await
            {
                let _ = Self::remove_private_file(&ticket_path);
                drop(lease);
                return Err(error);
            }
            let service = match self.ensure_managed_service().await {
                Ok(service) => service,
                Err(error) => {
                    if let Err(reinstall) = self
                        .install_pending_slot_with_lease(&pending, Arc::clone(&lease))
                        .await
                    {
                        self.record_managed_error(reinstall);
                    }
                    return Err(error);
                }
            };
            let member = match service
                .resume_membership(
                    ticket.preview().owner,
                    ticket.preview().share_id,
                    ticket.address(),
                )
                .await
            {
                Ok(member) => member,
                Err(error) if is_share_error(&error, ShareError::NotMember) => {
                    match service.enroll(&ticket, None).await {
                        Ok(member) => member,
                        Err(error) if is_share_error(&error, ShareError::Offline) => {
                            self.install_pending_slot_with_lease(&pending, lease)
                                .await?;
                            return Ok(Self::waiting_join_result(&pending));
                        }
                        Err(error) if is_terminal_pending_error(&error) => {
                            self.terminalize_pending(
                                &pending,
                                pending_terminal_ref(Some(&error), &pending.share_id),
                            )
                            .await?;
                            drop(lease);
                            return Err(error);
                        }
                        Err(error) => {
                            self.update_pending_retry(&pending).await?;
                            self.install_pending_slot_with_lease(&pending, lease)
                                .await?;
                            return Err(error);
                        }
                    }
                }
                Err(error) if is_share_error(&error, ShareError::Offline) => {
                    self.install_pending_slot_with_lease(&pending, lease)
                        .await?;
                    return Ok(Self::waiting_join_result(&pending));
                }
                Err(error) if is_terminal_pending_error(&error) => {
                    self.terminalize_pending(
                        &pending,
                        pending_terminal_ref(Some(&error), &pending.share_id),
                    )
                    .await?;
                    drop(lease);
                    return Err(error);
                }
                Err(error) => {
                    self.update_pending_retry(&pending).await?;
                    self.install_pending_slot_with_lease(&pending, lease)
                        .await?;
                    return Err(error);
                }
            };
            let worker = self
                .complete_pending_membership(pending.clone(), member, service, Some(lease))
                .await;
            let worker = match worker {
                Ok(worker) => worker,
                Err(error) => return Err(error),
            };
            self.managed_slots
                .lock()
                .expect("managed slots mutex")
                .insert(
                    pending.share_id.clone(),
                    Arc::new(ManagedSlot {
                        operation: AsyncMutex::new(Some(worker)),
                    }),
                );
            let record = self.managed_record(ticket.preview().share_id)?;
            return Ok(JoinResult {
                request_id: input.request_id,
                share_id: record.share_id,
                enrollment: EnrollmentState::Enrolled,
                status: record.status,
                permission: record.permission,
                member_id: record.member_id,
            });
        };
        let service = self.ensure_managed_service().await?;
        let member = match service
            .resume_membership(
                ticket.preview().owner,
                ticket.preview().share_id,
                ticket.address(),
            )
            .await
        {
            Ok(member) => member,
            Err(error) if is_share_error(&error, ShareError::NotMember) => {
                service.enroll(&ticket, None).await?
            }
            Err(error) if is_share_error(&error, ShareError::Offline) => {
                let now = self.managed_time()?;
                self.persist(|config| {
                    if let Some(item) = config
                        .managed
                        .pending
                        .iter_mut()
                        .find(|item| item.request_id == input.request_id)
                    {
                        item.status = ManagedStatus::Waiting;
                        item.retry_at = Some(now.saturating_add(MANAGED_TICK_SECONDS));
                    }
                    Ok(())
                })
                .await?;
                return Ok(Self::waiting_join_result(&pending));
            }
            Err(error) => return Err(error),
        };
        let share = member.share_id;
        let lease = self.take_pending_lease(&pending.share_id).await?;
        let worker = match self
            .complete_pending_membership(pending.clone(), member.clone(), service, lease)
            .await
        {
            Ok(worker) => worker,
            Err(error) => return Err(error),
        };
        self.managed_slots
            .lock()
            .expect("managed slots mutex")
            .insert(
                pending.share_id.clone(),
                Arc::new(ManagedSlot {
                    operation: AsyncMutex::new(Some(worker)),
                }),
            );
        let record = self.managed_record(share)?;
        Ok(JoinResult {
            request_id: input.request_id,
            share_id: record.share_id,
            enrollment: EnrollmentState::Enrolled,
            status: record.status,
            permission: record.permission,
            member_id: record.member_id,
        })
    }

    pub async fn resume_membership(&self, input: ResumeMembershipInput) -> Result<JoinResult> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        validate_request_id(&input.request_id)?;
        let hash = request_hash("resume_membership", &input)?;
        if let Some(result_ref) =
            self.request_lookup(&input.request_id, "resume_membership", &hash)?
        {
            let share = parse_share_id(result_ref.strip_prefix("share:").unwrap_or(&result_ref))?;
            let record = self.managed_record(share)?;
            return Ok(JoinResult {
                request_id: input.request_id,
                share_id: record.share_id,
                enrollment: if record.status == ManagedStatus::Revoked {
                    EnrollmentState::Revoked
                } else {
                    EnrollmentState::Enrolled
                },
                status: record.status,
                permission: record.permission,
                member_id: record.member_id,
            });
        }
        let _mutation = self.managed_mutations.lock().await;
        if let Some(result_ref) =
            self.request_lookup(&input.request_id, "resume_membership", &hash)?
        {
            let share = parse_share_id(result_ref.strip_prefix("share:").unwrap_or(&result_ref))?;
            let record = self.managed_record(share)?;
            return Ok(JoinResult {
                request_id: input.request_id.clone(),
                share_id: record.share_id,
                enrollment: if record.status == ManagedStatus::Revoked {
                    EnrollmentState::Revoked
                } else {
                    EnrollmentState::Enrolled
                },
                status: record.status,
                permission: record.permission,
                member_id: record.member_id,
            });
        }
        self.managed_time()?;
        self.ensure_request_capacity(&input.request_id)?;
        let record = self.managed_record(input.share)?;
        ensure!(
            record.role == ShareRole::Member,
            ManagedError::new(ManagedErrorKind::OwnerOnly)
        );
        let address = record
            .owner_address
            .clone()
            .ok_or_else(|| anyhow::Error::new(ManagedError::new(ManagedErrorKind::InvalidInput)))?;
        let service = self.ensure_managed_service().await?;
        let member = match service
            .resume_membership(address.id, input.share, address.clone())
            .await
        {
            Ok(member) => member,
            Err(error) if is_share_error(&error, ShareError::MemberRevoked) => {
                let id = share_id_string(input.share);
                self.persist(|config| {
                    if let Some(record) = config
                        .managed
                        .shares
                        .iter_mut()
                        .find(|record| record.share_id == id)
                    {
                        record.status = ManagedStatus::Revoked;
                        record.phase = Some("revoked".into());
                        record.last_error = Some(classify_managed_error(&error));
                    }
                    Ok(())
                })
                .await?;
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        ensure!(
            member.share_id == input.share
                && member.owner == address.id
                && record.permission == Some(member.permission)
                && record.replica == Some(member.replica),
            ShareError::ReplicaClaimRejected
        );
        ensure!(
            record
                .enrolled_at
                .is_none_or(|enrolled_at| enrolled_at == member.enrolled_at)
                && record
                    .membership_epoch
                    .is_none_or(|epoch| epoch == member.epoch),
            ShareError::ReplicaClaimRejected
        );
        let id = share_id_string(input.share);
        let slot = self
            .managed_slots
            .lock()
            .expect("managed slots mutex")
            .get(&id)
            .cloned();
        if slot.is_none() {
            let sync_config = ManagedSyncConfig {
                root: PathBuf::from(&record.root),
                state_root: PathBuf::from(&record.state_root),
                profile: deltaweave_core::ChunkingProfile::default(),
                min_free_space_bytes: record.min_free_space_bytes,
            };
            let engine =
                ManagedSyncEngine::resume(&service, member.owner, input.share, sync_config)?;
            self.managed_slots
                .lock()
                .expect("managed slots mutex")
                .insert(
                    id.clone(),
                    Arc::new(ManagedSlot {
                        operation: AsyncMutex::new(Some(ManagedWorker::Member(engine))),
                    }),
                );
        }
        self.persist(|config| {
            if let Some(record) = config
                .managed
                .shares
                .iter_mut()
                .find(|record| record.share_id == id)
            {
                record.status = ManagedStatus::InitialSync;
                record.phase = Some("initial_sync".into());
                record.retry_at = None;
                record.last_error = None;
                record.enrolled_at = Some(member.enrolled_at);
                record.membership_epoch = Some(member.epoch);
            }
            config.managed.requests.push(config::RequestRecord {
                request_id: input.request_id.clone(),
                operation: "resume_membership".into(),
                request_hash: hash.clone(),
                result_ref: format!("share:{id}"),
                recorded_at: managed_now(),
            });
            Ok(())
        })
        .await?;
        let record = self.managed_record(input.share)?;
        Ok(JoinResult {
            request_id: input.request_id,
            share_id: record.share_id,
            enrollment: EnrollmentState::Enrolled,
            status: record.status,
            permission: record.permission,
            member_id: record.member_id,
        })
    }

    pub async fn list_shares(&self) -> Result<Vec<ShareView>> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        Ok(self
            .shared
            .lock()
            .expect("snapshot mutex")
            .config
            .managed
            .shares
            .iter()
            .map(Self::managed_view)
            .collect())
    }

    pub async fn list_keys(&self, share: ShareId) -> Result<Vec<KeySummary>> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        self.managed_time()?;
        let owner = self.owner_share(share).await?;
        Ok(owner
            .keys()?
            .into_iter()
            .map(|invitation| KeySummary {
                invitation_id: invitation_id_string(invitation.id),
                share_id: share_id_string(invitation.share_id),
                permission: invitation.permission,
                issued_at: None,
                expires_at: invitation.expires_at,
                revoked_at: invitation.revoked_at,
            })
            .collect())
    }

    async fn read_key_response(
        &self,
        request_id: &str,
        hash: &str,
        result_ref: &str,
    ) -> Result<IssuedKey> {
        let record = self.request_record(request_id).ok_or_else(|| {
            anyhow::Error::new(ManagedError::new(ManagedErrorKind::KeyResponseExpired))
        })?;
        ensure!(
            record.request_hash == hash,
            ManagedError::new(ManagedErrorKind::IdempotencyConflict)
        );
        let current = self.managed_time()?;
        ensure!(
            current >= record.recorded_at
                && current - record.recorded_at <= MANAGED_KEY_RESPONSE_SECONDS,
            ManagedError::new(ManagedErrorKind::KeyResponseExpired)
        );
        let response_root = self.data_dir.join("managed").join("responses");
        let path = PathBuf::from(result_ref);
        ensure!(
            Self::is_generated_private_path(&response_root, &path, ".ticket"),
            ManagedError::new(ManagedErrorKind::InvalidPath)
        );
        let encoded = Self::read_private_text(&path, 32 * 1024).map_err(|_| {
            anyhow::Error::new(ManagedError::new(ManagedErrorKind::KeyResponseExpired))
        })?;
        Self::issued_key(request_id.into(), encoded)
    }

    async fn issue_key_inner(&self, request: IssueKeyRequest<'_>) -> Result<IssuedKey> {
        let IssueKeyRequest {
            request_id,
            share,
            permission,
            expires_at,
            operation,
            hash,
            rotate_invitation,
        } = request;
        self.check_expiry(expires_at)?;
        if let Some(result_ref) = self.request_lookup(&request_id, operation, &hash)? {
            return self
                .read_key_response(&request_id, &hash, &result_ref)
                .await;
        }
        self.managed_time()?;
        let owner = self.owner_share(share).await?;
        // Rotation inherits the permission of the invitation being rotated.
        // The public RotateKeyInput has no permission field, and forcing a
        // default here would silently downgrade an RW grant to RO.
        let permission = if let Some(invitation) = rotate_invitation {
            owner
                .keys()?
                .into_iter()
                .find(|existing| existing.id == invitation)
                .ok_or(ShareError::InvalidTicket)?
                .permission
        } else {
            permission
        };
        let service = self.ensure_managed_service().await?;
        let request_spec = IssueKeyRequest {
            request_id: request_id.clone(),
            share,
            permission,
            expires_at,
            operation,
            hash: hash.clone(),
            rotate_invitation,
        };
        let mut intent = self.key_intent(&request_id);
        let mut staged = None;
        if let Some(existing) = intent.as_ref() {
            let invitation = Self::validate_key_intent(existing, &request_spec)?;
            let path = self.key_intent_path(existing)?;
            let current = self.managed_time()?;
            ensure!(
                current >= existing.created_at,
                ManagedError::new(ManagedErrorKind::ClockRollback)
            );
            let existing_invitation = owner
                .keys()?
                .into_iter()
                .find(|existing| existing.id == invitation);
            if current - existing.created_at > MANAGED_KEY_RESPONSE_SECONDS {
                return self.expire_key_intent(existing, &path, &owner).await;
            }
            if !path.exists() {
                if existing_invitation.is_some() {
                    return self.expire_key_intent(existing, &path, &owner).await;
                }
                self.remove_key_intent(&request_id).await?;
                intent = None;
            } else {
                let encoded = Self::read_private_text(&path, 32 * 1024)?;
                let parsed = match ShareTicket::parse(&encoded) {
                    Ok(ticket) => Some(ticket),
                    Err(error) => {
                        let error = anyhow::Error::new(error);
                        if !is_terminal_pending_error(&error) {
                            return Err(error);
                        }
                        if existing_invitation.is_some() {
                            return self.expire_key_intent(existing, &path, &owner).await;
                        }
                        // Remove the staged file first.  If that cleanup
                        // fails, retain the intent so a retry cannot write a
                        // different ticket through write_private_text's
                        // existing-file no-op path.  A crash after the file
                        // removal but before intent removal is safe: the next
                        // retry sees the missing file and regenerates only
                        // after the old staged artifact is gone.
                        Self::remove_private_file(&path)?;
                        self.remove_key_intent(&request_id).await?;
                        intent = None;
                        None
                    }
                };
                if let Some(ticket) = parsed {
                    let preview = ticket.preview();
                    ensure!(
                        preview.share_id == share
                            && preview.permission == permission
                            && preview.expires_at == expires_at
                            && preview.invitation_id == invitation,
                        ManagedError::new(ManagedErrorKind::IdempotencyConflict)
                    );
                    staged = Some((ticket, path));
                }
            }
        }
        if staged.is_none() {
            self.ensure_request_capacity(&request_id)?;
            let path = self.private_text_path("responses", &hash, "ticket")?;
            let reserved = root_admission::reserve_private_file(&path)?;
            if let Some(existing) = intent.as_ref() {
                ensure!(
                    existing.response_file == reserved.to_string_lossy(),
                    ManagedError::new(ManagedErrorKind::IdempotencyConflict)
                );
            }
            let parent = reserved
                .parent()
                .context("private response parent is unavailable")?;
            private::prepare_directory(parent)?;
            // A crash may leave the deterministic response path without its
            // durable KeyIntent.  Never let write_private_text's existing-file
            // no-op pair a newly generated invitation with that old bytes:
            // parse and bind a valid orphan to this request, or remove an
            // invalid orphan successfully before generating anything.
            let orphan = if reserved.exists() {
                let encoded = Self::read_private_text(&reserved, 32 * 1024)?;
                match ShareTicket::parse(&encoded) {
                    Ok(ticket) => {
                        let preview = ticket.preview();
                        ensure!(
                            preview.share_id == share
                                && preview.owner == owner.config().owner
                                && preview.permission == permission
                                && preview.expires_at == expires_at,
                            ManagedError::new(ManagedErrorKind::IdempotencyConflict)
                        );
                        Some(ticket)
                    }
                    Err(error) => {
                        let error = anyhow::Error::new(error);
                        if !is_terminal_pending_error(&error) {
                            return Err(error);
                        }
                        // Keep the request uncommitted until the old secret is
                        // actually removed.  A failed removal must be retried
                        // with the same path and cannot mint a replacement.
                        Self::remove_private_file(&reserved)?;
                        None
                    }
                }
            } else {
                None
            };
            let ticket = match orphan {
                Some(ticket) => ticket,
                None => owner.prepare_key(permission, expires_at, service.endpoint_addr())?,
            };
            let preview = ticket.preview();
            let staged_intent = config::KeyIntent {
                request_id: request_id.clone(),
                operation: operation.into(),
                request_hash: hash.clone(),
                share_id: share_id_string(share),
                permission,
                expires_at,
                invitation_id: invitation_id_string(preview.invitation_id),
                response_file: reserved.to_string_lossy().into_owned(),
                rotate_invitation: rotate_invitation.map(invitation_id_string),
                created_at: self.managed_time()?,
            };
            self.persist(|config| {
                if let Some(existing) = config
                    .managed
                    .key_intents
                    .iter()
                    .find(|existing| existing.request_id == request_id)
                {
                    ensure!(
                        existing.request_hash == staged_intent.request_hash
                            && existing.response_file == staged_intent.response_file
                            && existing.invitation_id == staged_intent.invitation_id,
                        ManagedError::new(ManagedErrorKind::IdempotencyConflict)
                    );
                } else {
                    config.managed.key_intents.push(staged_intent.clone());
                }
                Ok(())
            })
            .await?;
            Self::write_private_text(&reserved, &ticket.encode(), 32 * 1024)?;
            intent = Some(staged_intent);
            staged = Some((ticket, reserved));
        }
        let (ticket, response_path) = staged.expect("staged issuance ticket");
        owner.commit_key(&ticket, rotate_invitation)?;
        // Keep the intent and response file when the journal write fails. A
        // retry will replay this exact signed ticket and registry invitation
        // after a crash or durable-journal failure.
        self.record_request(
            request_id.clone(),
            operation,
            hash,
            response_path.to_string_lossy().into_owned(),
        )
        .await?;
        if let Some(intent) = intent
            && let Err(error) = self.remove_key_intent(&intent.request_id).await
        {
            self.record_managed_error(error);
        }
        let encoded = Self::read_private_text(&response_path, 32 * 1024)?;
        Self::issued_key(request_id, encoded)
    }

    pub async fn issue_key(&self, input: IssueKeyInput) -> Result<IssuedKey> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        validate_request_id(&input.request_id)?;
        let hash = request_hash("issue_key", &input)?;
        let _mutation = self.managed_mutations.lock().await;
        self.issue_key_inner(IssueKeyRequest {
            request_id: input.request_id,
            share: input.share,
            permission: input.permission,
            expires_at: input.expires_at,
            operation: "issue_key",
            hash,
            rotate_invitation: None,
        })
        .await
    }

    pub async fn rotate_key(&self, input: RotateKeyInput) -> Result<IssuedKey> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        validate_request_id(&input.request_id)?;
        let hash = request_hash("rotate_key", &input)?;
        let _mutation = self.managed_mutations.lock().await;
        self.issue_key_inner(IssueKeyRequest {
            request_id: input.request_id,
            share: input.share,
            permission: Permission::ReadOnly,
            expires_at: input.expires_at,
            operation: "rotate_key",
            hash,
            rotate_invitation: Some(input.invitation),
        })
        .await
    }

    pub async fn list_members(&self, share: ShareId) -> Result<Vec<MemberView>> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        self.managed_time()?;
        let owner = self.owner_share(share).await?;
        let mut members = Vec::new();
        for member in owner.members()? {
            let member_id = self.member_handle(share, member.endpoint).await?;
            let revocation_pending = self
                .shared
                .lock()
                .expect("snapshot mutex")
                .config
                .managed
                .revocations
                .iter()
                .any(|pending| {
                    pending.share_id == share_id_string(share) && pending.member_id == member_id
                });
            members.push(MemberView {
                member_id,
                permission: member.permission,
                revocation_pending,
                enrolled_at: member.enrolled_at,
                revoked_at: member.revoked_at,
                active_operations: 0,
                last_seen_at: None,
            });
        }
        Ok(members)
    }

    pub async fn revoke_key(&self, input: RevokeKeyInput) -> Result<MutationResult> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        validate_request_id(&input.request_id)?;
        let hash = request_hash("revoke_key", &input)?;
        if self
            .request_lookup(&input.request_id, "revoke_key", &hash)?
            .is_some()
        {
            return self.mutation_result(input.request_id, input.share);
        }
        let _mutation = self.managed_mutations.lock().await;
        if self
            .request_lookup(&input.request_id, "revoke_key", &hash)?
            .is_some()
        {
            return self.mutation_result(input.request_id, input.share);
        }
        self.managed_time()?;
        self.ensure_request_capacity(&input.request_id)?;
        let owner = self.owner_share(input.share).await?;
        owner.revoke_key(input.invitation)?;
        self.record_request(
            input.request_id.clone(),
            "revoke_key",
            hash,
            format!("mutation:{}", share_id_string(input.share)),
        )
        .await?;
        self.mutation_result(input.request_id, input.share)
    }

    pub async fn revoke_member(&self, input: RevokeMemberInput) -> Result<MutationResult> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        validate_request_id(&input.request_id)?;
        ensure!(
            !input.member_id.is_empty()
                && input.member_id.len() <= 128
                && !input.member_id.chars().any(char::is_control),
            ManagedError::new(ManagedErrorKind::InvalidInput)
        );
        let hash = request_hash("revoke_member", &input)?;
        if self
            .request_lookup(&input.request_id, "revoke_member", &hash)?
            .is_some()
        {
            return self.mutation_result(input.request_id, input.share);
        }
        let _mutation = self.managed_mutations.lock().await;
        if self
            .request_lookup(&input.request_id, "revoke_member", &hash)?
            .is_some()
        {
            return self.mutation_result(input.request_id, input.share);
        }
        self.managed_time()?;
        self.ensure_request_capacity(&input.request_id)?;
        let owner = self.owner_share(input.share).await?;
        let mut found = None;
        for member in owner.members()? {
            if self.member_handle(input.share, member.endpoint).await? == input.member_id {
                found = Some(member);
                break;
            }
        }
        let member = found
            .ok_or_else(|| anyhow::Error::new(ManagedError::new(ManagedErrorKind::NotFound)))?;
        let now = self.managed_time()?;
        owner.deny_member(member.endpoint)?;
        let share_id = share_id_string(input.share);
        let pending = config::PendingRevocation {
            request_id: input.request_id.clone(),
            share_id: share_id.clone(),
            member_id: input.member_id.clone(),
            endpoint: member.endpoint.to_string(),
            created_at: now,
            retry_at: None,
        };
        let revocation_pending = !owner.member_drain_ready(member.endpoint);
        self.persist(|config| {
            if revocation_pending {
                if let Some(existing) = config
                    .managed
                    .revocations
                    .iter()
                    .find(|existing| existing.request_id == input.request_id)
                {
                    ensure!(
                        existing.share_id == pending.share_id
                            && existing.member_id == pending.member_id
                            && existing.endpoint == pending.endpoint,
                        ManagedError::new(ManagedErrorKind::IdempotencyConflict)
                    );
                } else {
                    config.managed.revocations.push(pending.clone());
                }
            }
            if !config
                .managed
                .requests
                .iter()
                .any(|request| request.request_id == input.request_id)
            {
                config.managed.requests.push(config::RequestRecord {
                    request_id: input.request_id.clone(),
                    operation: "revoke_member".into(),
                    request_hash: hash.clone(),
                    result_ref: format!("mutation:{share_id}:{}", input.member_id),
                    recorded_at: managed_now(),
                });
            }
            Ok(())
        })
        .await?;
        if revocation_pending
            && let Err(error) = self.schedule_revocation_drain(pending, owner).await
        {
            self.record_managed_error(error);
        }
        self.mutation_result(input.request_id, input.share)
    }

    pub async fn remove_share(&self, input: RemoveShareInput) -> Result<MutationResult> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        validate_request_id(&input.request_id)?;
        let hash = request_hash("remove_share", &input)?;
        if self
            .request_lookup(&input.request_id, "remove_share", &hash)?
            .is_some()
        {
            return Ok(Self::removed_mutation_result(input.request_id));
        }
        let _mutation = self.managed_mutations.lock().await;
        if self
            .request_lookup(&input.request_id, "remove_share", &hash)?
            .is_some()
        {
            return Ok(Self::removed_mutation_result(input.request_id));
        }
        self.managed_time()?;
        let id = share_id_string(input.share);
        if let Some(intent) = self.removal_intent(&input.request_id) {
            ensure!(
                intent.request_hash == hash && intent.share_id == id,
                ManagedError::new(ManagedErrorKind::IdempotencyConflict)
            );
            let (tombstoned, configured) = {
                let state = self.shared.lock().expect("snapshot mutex");
                (
                    state
                        .config
                        .managed
                        .tombstones
                        .iter()
                        .any(|tombstone| tombstone == &id),
                    state
                        .config
                        .managed
                        .shares
                        .iter()
                        .any(|record| record.share_id == id),
                )
            };
            ensure!(
                tombstoned,
                ManagedError::new(ManagedErrorKind::InvalidInput)
            );
            if !configured {
                self.ensure_request_capacity(&input.request_id)?;
                self.complete_removed_request(&input.request_id, &hash, &id)
                    .await?;
                return Ok(Self::removed_mutation_result(input.request_id));
            }
        }
        self.ensure_request_capacity(&input.request_id)?;
        let record = self.managed_record(input.share)?;
        self.persist(|config| {
            if !config
                .managed
                .tombstones
                .iter()
                .any(|tombstone| tombstone == &id)
            {
                config.managed.tombstones.push(id.clone());
            }
            if let Some(existing) = config
                .managed
                .removals
                .iter()
                .find(|intent| intent.request_id == input.request_id)
            {
                ensure!(
                    existing.request_hash == hash && existing.share_id == id,
                    ManagedError::new(ManagedErrorKind::IdempotencyConflict)
                );
            } else {
                config.managed.removals.push(config::RemovalIntent {
                    request_id: input.request_id.clone(),
                    request_hash: hash.clone(),
                    share_id: id.clone(),
                    created_at: managed_now(),
                });
            }
            Ok(())
        })
        .await?;
        self.await_revocation_tasks(Some(&id)).await?;
        let slot = self
            .managed_slots
            .lock()
            .expect("managed slots mutex")
            .remove(&id);
        if let Some(slot) = slot {
            let worker = {
                let mut operation = slot.operation.lock().await;
                operation.take()
            };
            if let Some(worker) = worker {
                worker.stop().await?;
            }
        }
        if let Some(service) = self.managed_service.lock().await.as_ref().cloned() {
            match record.role {
                ShareRole::Owner => {
                    if let Err(error) = service.unload_owned_share(input.share).await {
                        self.update_managed_failure(input.share, &error).await?;
                        return Err(error);
                    }
                }
                ShareRole::Member => {
                    let owner = endpoint_id(&record.owner)?;
                    service.forget_membership(owner, input.share)?;
                }
            }
        }
        self.persist(|config| {
            config.managed.shares.retain(|record| record.share_id != id);
            config
                .managed
                .pending
                .retain(|pending| pending.share_id != id);
            config
                .managed
                .revocations
                .retain(|pending| pending.share_id != id);
            config
                .managed
                .removals
                .retain(|intent| intent.request_id != input.request_id);
            if let Some(existing) = config
                .managed
                .requests
                .iter()
                .find(|record| record.request_id == input.request_id)
            {
                ensure!(
                    existing.operation == "remove_share" && existing.request_hash == hash,
                    ManagedError::new(ManagedErrorKind::IdempotencyConflict)
                );
            } else {
                config.managed.requests.push(config::RequestRecord {
                    request_id: input.request_id.clone(),
                    operation: "remove_share".into(),
                    request_hash: hash.clone(),
                    result_ref: format!("mutation:{id}"),
                    recorded_at: managed_now(),
                });
            }
            Ok(())
        })
        .await?;
        Ok(Self::removed_mutation_result(input.request_id))
    }

    async fn set_managed_status(
        &self,
        share: ShareId,
        status: ManagedStatus,
        phase: &str,
    ) -> Result<()> {
        let id = share_id_string(share);
        self.persist(|config| {
            let record = config
                .managed
                .shares
                .iter_mut()
                .find(|record| record.share_id == id)
                .ok_or_else(|| anyhow::Error::new(ManagedError::new(ManagedErrorKind::NotFound)))?;
            record.status = status;
            record.phase = Some(phase.into());
            if status != ManagedStatus::Error && status != ManagedStatus::Offline {
                record.last_error = None;
            }
            if status != ManagedStatus::Offline {
                record.retry_at = None;
            }
            Ok(())
        })
        .await
    }

    pub async fn share_command(&self, input: ShareCommandInput) -> Result<ShareView> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        validate_request_id(&input.request_id)?;
        let hash = request_hash("share_command", &input)?;
        if self
            .request_lookup(&input.request_id, "share_command", &hash)?
            .is_some()
        {
            return self.managed_view_for(input.share);
        }
        let _mutation = self.managed_mutations.lock().await;
        if self
            .request_lookup(&input.request_id, "share_command", &hash)?
            .is_some()
        {
            return self.managed_view_for(input.share);
        }
        self.managed_time()?;
        self.ensure_request_capacity(&input.request_id)?;
        let id = share_id_string(input.share);
        let slot = self
            .managed_slots
            .lock()
            .expect("managed slots mutex")
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow::Error::new(ManagedError::new(ManagedErrorKind::NotFound)))?;
        let mut operation = slot.operation.lock().await;
        let worker = operation
            .as_mut()
            .ok_or_else(|| anyhow::Error::new(ManagedError::new(ManagedErrorKind::Busy)))?;
        match (worker, input.command) {
            (ManagedWorker::Owner(owner), ShareCommand::Pause) => {
                self.set_managed_status(input.share, ManagedStatus::Paused, "paused")
                    .await?;
                owner.pause().await;
                self.clear_managed_observation(input.share);
            }
            (ManagedWorker::Owner(owner), ShareCommand::Resume) => {
                self.set_managed_status(input.share, ManagedStatus::InitialSync, "initial_sync")
                    .await?;
                owner.resume();
                self.update_owner_inventory(input.share, owner).await?;
            }
            (ManagedWorker::Owner(owner), ShareCommand::Sync) => {
                self.update_owner_inventory(input.share, owner).await?;
            }
            (ManagedWorker::Member(_), ShareCommand::Pause) => {
                self.set_managed_status(input.share, ManagedStatus::Paused, "paused")
                    .await?;
                self.clear_managed_observation(input.share);
            }
            (ManagedWorker::Member(engine), ShareCommand::Resume)
            | (ManagedWorker::Member(engine), ShareCommand::Sync) => {
                self.set_managed_status(input.share, ManagedStatus::InitialSync, "initial_sync")
                    .await?;
                match engine
                    .sync_once(Some(self.member_observer(input.share)))
                    .await
                {
                    Ok(report) => {
                        let inventory = engine.inventory()?;
                        self.persist_member_report(input.share, &report, inventory)
                            .await?;
                    }
                    Err(error) => {
                        self.persist_member_error(input.share, error).await?;
                    }
                }
            }
            (ManagedWorker::Pending(_), _) => {
                return Err(ManagedError::new(ManagedErrorKind::Busy).into());
            }
        }
        self.record_request(
            input.request_id.clone(),
            "share_command",
            hash,
            format!("share:{id}"),
        )
        .await?;
        self.managed_view_for(input.share)
    }
    pub async fn add_folder(&self, input: FolderInput) -> Result<FolderView> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        let input = self.resolve_device(input)?;
        let id = new_id();
        // Reserve all canonical paths before opening any engine. Concurrent adds see this reservation.
        let input = {
            let (others, poll) = {
                let state = self.shared.lock().expect("snapshot mutex");
                (
                    state.folders.values().cloned().collect::<Vec<_>>(),
                    state.config.settings.poll_interval_seconds,
                )
            };
            let dir = self.data_dir.clone();
            let id = id.clone();
            tokio::task::spawn_blocking(move || config::normalize(input, &id, &dir, &others, poll))
                .await??
        };
        let identity_path = input
            .identity_path
            .clone()
            .context("identity path missing")?;
        let endpoint_id = tokio::task::spawn_blocking(move || {
            deltaweave_net::load_or_create_identity(identity_path)
                .map(|identity| identity.endpoint_id().to_string())
        })
        .await??;
        let view = FolderView {
            input,
            id: id.clone(),
            endpoint_id,
            addresses: Vec::new(),
            status: "starting".into(),
            phase: None,
            current_path: None,
            last_sync_at: None,
            last_error: None,
            retry_at: None,
            files_count: 0,
            total_bytes: 0,
            last_report: None,
        };
        {
            let mut state = self.shared.lock().expect("snapshot mutex");
            ensure!(
                state
                    .folders
                    .values()
                    .chain(state.config.folders.iter())
                    .all(|other| !paths_conflict(&view.input, &other.input)
                        && view.endpoint_id != other.endpoint_id),
                "paths or endpoint identity overlap another managed folder"
            );
            state.folders.insert(id.clone(), view.clone());
            state.revision += 1;
        }
        let slot = Arc::new(Slot {
            operation: AsyncMutex::new(None),
        });
        let mut operation = slot.operation.lock().await;
        self.slots
            .lock()
            .expect("slots mutex")
            .insert(id.clone(), slot.clone());
        let result = Worker::start(view.clone(), self.shared.clone()).await;
        match result {
            Ok(worker) => {
                *operation = Some(worker);
                let view = self.shared.lock().expect("snapshot mutex").folders[&id].clone();
                if let Err(error) = self
                    .persist(|config| {
                        validate_device_reference(config, &view.input)?;
                        config.folders.push(view.clone());
                        Ok(())
                    })
                    .await
                {
                    if let Some(worker) = operation.take() {
                        worker.stop().await?;
                    }
                    self.shared
                        .lock()
                        .expect("snapshot mutex")
                        .folders
                        .remove(&id);
                    self.slots.lock().expect("slots mutex").remove(&id);
                    return Err(error);
                }
            }
            Err(error) => {
                self.shared
                    .lock()
                    .expect("snapshot mutex")
                    .folders
                    .remove(&id);
                self.slots.lock().expect("slots mutex").remove(&id);
                return Err(error);
            }
        }
        let mut state = self.shared.lock().expect("snapshot mutex");
        state.activity(Some(id.clone()), "folder_added", view.input.name, None);
        Ok(state.folders[&id].clone())
    }
    pub async fn update_folder(&self, id: &str, input: FolderInput) -> Result<FolderView> {
        let _active = self.lifecycle.read().await;
        let input = self.resolve_device(input)?;
        let slot = self.slot(id)?;
        let mut operation = slot.operation.lock().await;
        let (old, others, poll) = {
            let state = self.shared.lock().expect("snapshot mutex");
            (
                state.folders.get(id).cloned().context("folder not found")?,
                state.folders.values().cloned().collect::<Vec<_>>(),
                state.config.settings.poll_interval_seconds,
            )
        };
        let input = FolderInput {
            state_path: input.state_path.or_else(|| old.input.state_path.clone()),
            identity_path: input
                .identity_path
                .or_else(|| old.input.identity_path.clone()),
            ..input
        };
        ensure!(
            input.role == old.input.role,
            "changing an existing state role requires a new folder connection"
        );
        let owned_id = id.to_string();
        let dir = self.data_dir.clone();
        let input = tokio::task::spawn_blocking(move || {
            config::normalize(input, &owned_id, &dir, &others, poll)
        })
        .await??;
        let identity_path = input
            .identity_path
            .clone()
            .context("identity path missing")?;
        let endpoint_id = tokio::task::spawn_blocking(move || {
            deltaweave_net::load_or_create_identity(identity_path)
                .map(|identity| identity.endpoint_id().to_string())
        })
        .await??;
        let new = FolderView {
            input,
            endpoint_id,
            status: "starting".into(),
            addresses: Vec::new(),
            ..old.clone()
        };
        {
            let mut state = self.shared.lock().expect("snapshot mutex");
            ensure!(
                state
                    .folders
                    .values()
                    .chain(state.config.folders.iter())
                    .filter(|f| f.id != id)
                    .all(|f| !paths_conflict(&new.input, &f.input)
                        && new.endpoint_id != f.endpoint_id),
                "paths or endpoint identity overlap another managed folder"
            );
            state.folders.insert(id.into(), new.clone());
        }
        if let Some(worker) = operation.take() {
            worker.stop().await?;
        }
        match Worker::start(new.clone(), self.shared.clone()).await {
            Ok(worker) => {
                *operation = Some(worker);
                let new = self.shared.lock().expect("snapshot mutex").folders[id].clone();
                if let Err(error) = self
                    .persist(|config| {
                        validate_device_reference(config, &new.input)?;
                        let folder = config
                            .folders
                            .iter_mut()
                            .find(|f| f.id == id)
                            .context("folder not found")?;
                        *folder = new.clone();
                        Ok(())
                    })
                    .await
                {
                    if let Some(worker) = operation.take() {
                        worker.stop().await?;
                    }
                    self.shared
                        .lock()
                        .expect("snapshot mutex")
                        .folders
                        .insert(id.into(), old.clone());
                    *operation = Some(Worker::start(old, self.shared.clone()).await.context(
                        "configuration save failed and previous worker could not restart",
                    )?);
                    return Err(error);
                }
            }
            Err(error) => {
                self.shared
                    .lock()
                    .expect("snapshot mutex")
                    .folders
                    .insert(id.into(), old.clone());
                match Worker::start(old, self.shared.clone()).await {
                    Ok(worker) => *operation = Some(worker),
                    Err(restore) => {
                        let mut state = self.shared.lock().expect("snapshot mutex");
                        if let Some(folder) = state.folders.get_mut(id) {
                            folder.status = "error".into();
                            folder.last_error = Some(format!(
                                "update failed: {error:#}; restoring worker failed: {restore:#}"
                            ));
                        }
                    }
                }
                return Err(error);
            }
        }
        Ok(self.shared.lock().expect("snapshot mutex").folders[id].clone())
    }
    pub async fn remove_folder(&self, id: &str) -> Result<()> {
        let _active = self.lifecycle.read().await;
        let slot = self.slot(id)?;
        let mut operation = slot.operation.lock().await;
        let old = self
            .shared
            .lock()
            .expect("snapshot mutex")
            .folders
            .get(id)
            .cloned()
            .context("folder not found")?;
        if let Some(worker) = operation.take() {
            worker.stop().await?;
        }
        if let Err(error) = self
            .persist(|config| {
                config.folders.retain(|folder| folder.id != id);
                Ok(())
            })
            .await
        {
            *operation = Some(Worker::start(old, self.shared.clone()).await?);
            return Err(error);
        }
        self.shared
            .lock()
            .expect("snapshot mutex")
            .folders
            .remove(id);
        self.slots.lock().expect("slots mutex").remove(id);
        self.shared.lock().expect("snapshot mutex").activity(
            Some(id.into()),
            "folder_removed",
            "Connection removed; local files retained".into(),
            None,
        );
        Ok(())
    }
    pub async fn command(&self, id: &str, command: FolderCommand) -> Result<()> {
        let _active = self.lifecycle.read().await;
        let slot = self.slot(id)?;
        if matches!(command, FolderCommand::Sync) {
            let state = self.shared.lock().expect("snapshot mutex");
            let folder = state.folders.get(id).context("folder not found")?;
            ensure!(
                folder.input.role == "sync",
                "receive folders accept remote requests; run sync on the sending connection"
            );
            ensure!(
                folder.input.enabled != Some(false),
                "folder is paused; resume before syncing"
            );
            ensure!(
                !matches!(folder.status.as_str(), "syncing" | "pausing"),
                "folder is busy; retry after its current command"
            );
        }
        if matches!(command, FolderCommand::Pause) {
            let mut state = self.shared.lock().expect("snapshot mutex");
            if let Some(folder) = state.folders.get_mut(id) {
                folder.status = "pausing".into();
            }
            state.revision += 1;
        }
        let operation = if matches!(command, FolderCommand::Sync) {
            slot.operation
                .try_lock()
                .context("folder is busy; retry after its current command")?
        } else {
            slot.operation.lock().await
        };
        let worker = operation
            .as_ref()
            .context("folder worker is stopped; edit the folder to retry startup")?;
        worker.command(command).await?;
        if !matches!(command, FolderCommand::Sync) {
            let enabled = matches!(command, FolderCommand::Resume);
            if let Err(error) = self
                .persist(|config| {
                    config
                        .folders
                        .iter_mut()
                        .find(|f| f.id == id)
                        .context("folder not found")?
                        .input
                        .enabled = Some(enabled);
                    Ok(())
                })
                .await
            {
                worker
                    .command(if enabled {
                        FolderCommand::Pause
                    } else {
                        FolderCommand::Resume
                    })
                    .await?;
                return Err(error);
            }
            let mut state = self.shared.lock().expect("snapshot mutex");
            if let Some(folder) = state.folders.get_mut(id) {
                folder.input.enabled = Some(enabled);
            }
            state.revision += 1;
        }
        Ok(())
    }
    pub async fn add_device(&self, input: DeviceInput) -> Result<DeviceView> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        validate_device(&input)?;
        let device = DeviceView {
            input,
            id: new_id(),
            added_at: now(),
            last_seen_at: None,
        };
        self.persist(|config| {
            ensure!(
                !config
                    .devices
                    .iter()
                    .any(|d| d.input.endpoint_id == device.input.endpoint_id),
                "device endpoint is already registered"
            );
            config.devices.push(device.clone());
            Ok(())
        })
        .await?;
        Ok(device)
    }
    pub async fn update_device(&self, id: &str, input: DeviceInput) -> Result<DeviceView> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        validate_device(&input)?;
        self.persist(|config| {
            ensure!(
                !config
                    .devices
                    .iter()
                    .any(|d| d.id != id && d.input.endpoint_id == input.endpoint_id),
                "device endpoint is already registered"
            );
            config
                .devices
                .iter_mut()
                .find(|d| d.id == id)
                .context("device not found")?
                .input = input;
            Ok(())
        })
        .await?;
        self.shared
            .lock()
            .expect("snapshot mutex")
            .config
            .devices
            .iter()
            .find(|d| d.id == id)
            .cloned()
            .context("device not found")
    }
    pub async fn remove_device(&self, id: &str) -> Result<()> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        self.persist(|config| {
            ensure!(
                config.devices.iter().any(|d| d.id == id),
                "device not found"
            );
            ensure!(
                !config
                    .folders
                    .iter()
                    .any(|f| f.input.device_id.as_deref() == Some(id)),
                "device is used by a folder; update that folder first"
            );
            config.devices.retain(|d| d.id != id);
            Ok(())
        })
        .await
    }
    pub async fn update_settings(&self, settings: Settings) -> Result<Settings> {
        let _active = self.lifecycle.read().await;
        self.running()?;
        config::validate_name(&settings.node_name)?;
        ensure!(
            (1..=86400).contains(&settings.poll_interval_seconds),
            "poll interval must be 1–86400 seconds"
        );
        ensure!(
            (1..=10000).contains(&settings.history_limit),
            "history limit must be 1–10000"
        );
        self.persist(|config| {
            config.settings = settings.clone();
            Ok(())
        })
        .await?;
        Ok(settings)
    }
    pub async fn shutdown(&self) -> Result<()> {
        let _exclusive = self.lifecycle.write().await;
        if self.stopped.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let background = std::mem::take(&mut *self.background.lock().await);
        let mut error = None;
        for task in background {
            task.abort();
            if let Err(failure) = task.await
                && !failure.is_cancelled()
            {
                error = Some(anyhow::anyhow!("background task failed"));
            }
        }
        if let Err(failure) = self.await_revocation_tasks(None).await {
            error = Some(failure);
        }
        let slots = self
            .slots
            .lock()
            .expect("slots mutex")
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for slot in slots {
            if let Some(worker) = slot.operation.lock().await.take()
                && let Err(failure) = worker.stop().await
            {
                error = Some(failure);
            }
        }
        let managed_slots = self
            .managed_slots
            .lock()
            .expect("managed slots mutex")
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let managed_ids = self
            .shared
            .lock()
            .expect("snapshot mutex")
            .config
            .managed
            .shares
            .iter()
            .map(|record| record.share_id.clone())
            .collect::<Vec<_>>();
        for share in managed_ids {
            self.clear_managed_observation_id(&share);
        }
        for slot in managed_slots {
            if let Some(worker) = slot.operation.lock().await.take()
                && let Err(failure) = worker.stop().await
            {
                error = Some(failure);
            }
        }
        self.managed_slots
            .lock()
            .expect("managed slots mutex")
            .clear();
        if let Some(service) = self.managed_service.lock().await.take() {
            match Arc::try_unwrap(service) {
                Ok(service) => {
                    if let Err(failure) = service.shutdown().await {
                        error = Some(failure);
                    }
                }
                Err(_) => {
                    error = Some(anyhow::Error::new(ManagedError::new(
                        ManagedErrorKind::Busy,
                    )));
                }
            }
        }
        if let Err(failure) = self.persist(|_| Ok(())).await {
            error = Some(failure);
        }
        self.ownership.lock().expect("ownership mutex").take();
        if let Some(error) = error {
            return Err(error);
        }
        Ok(())
    }
}
fn validate_device_reference(config: &config::Config, input: &FolderInput) -> Result<()> {
    if let Some(id) = &input.device_id {
        let device = config
            .devices
            .iter()
            .find(|device| &device.id == id)
            .context("selected device was removed; choose a current device")?;
        if input.role == "sync" {
            ensure!(
                input.peer_endpoint_id.as_deref() == Some(device.input.endpoint_id.as_str()),
                "selected device endpoint changed; refresh its connection information"
            );
        }
    }
    Ok(())
}
fn validate_device(input: &DeviceInput) -> Result<()> {
    config::validate_name(&input.name)?;
    input
        .endpoint_id
        .parse::<iroh::EndpointId>()
        .context("invalid device endpoint ID")?;
    input
        .address
        .parse::<std::net::SocketAddr>()
        .context("device address must be IP:port")?;
    Ok(())
}
fn paths_conflict(a: &FolderInput, b: &FolderInput) -> bool {
    [
        Some(&a.root),
        a.state_path.as_ref(),
        a.identity_path.as_ref(),
    ]
    .into_iter()
    .flatten()
    .any(|a| {
        [
            Some(&b.root),
            b.state_path.as_ref(),
            b.identity_path.as_ref(),
        ]
        .into_iter()
        .flatten()
        .any(|b| config::overlaps(std::path::Path::new(a), std::path::Path::new(b)))
    })
}

#[cfg(test)]
mod managed_error_tests {
    use super::*;

    #[test]
    fn managed_error_classification_uses_stable_codes_and_safe_messages() {
        let error = anyhow::Error::new(ManagedError::new(ManagedErrorKind::IdempotencyConflict))
            .context("request contained a bearer=secret and /private/root");
        let summary = classify_managed_error(&error);
        assert_eq!(summary.code, "idempotency_conflict");
        assert_eq!(
            summary.message,
            "request ID was already used for another request"
        );
        assert!(!summary.message.contains("secret"));
        assert!(!summary.message.contains("/private/root"));
    }

    #[test]
    fn share_error_classification_preserves_only_the_safe_variant() {
        let error = anyhow::Error::new(deltaweave_net::share::ShareError::MemberRevoked)
            .context("bearer=secret");
        let summary = classify_managed_error(&error);
        assert_eq!(summary.code, "member_revoked");
        assert_eq!(summary.message, "membership revoked");
        assert!(!summary.message.contains("secret"));
    }

    #[test]
    fn newly_added_share_errors_have_stable_safe_codes() {
        let cases = [
            (ShareError::HeartbeatExpired, "heartbeat_expired"),
            (ShareError::HeartbeatReplay, "heartbeat_replay"),
            (ShareError::EpochMismatch, "epoch_mismatch"),
            (ShareError::EndpointMismatch, "endpoint_mismatch"),
            (ShareError::ClockRollback, "clock_rollback"),
            (ShareError::RosterStale, "roster_stale"),
            (ShareError::GrantExpired, "grant_expired"),
            (ShareError::GrantReplay, "grant_replay"),
            (ShareError::ManifestMismatch, "manifest_mismatch"),
            (ShareError::CasUnavailable, "cas_unavailable"),
            (ShareError::RevocationPending, "revocation_pending"),
        ];
        for (error, expected_code) in cases {
            let summary = classify_managed_error(&anyhow::Error::new(error));
            assert_eq!(summary.code, expected_code);
            assert!(!summary.message.contains("/"));
            assert!(!summary.message.contains("key="));
        }
    }

    #[test]
    fn manual_only_manager_ignores_stale_managed_clock_high_water() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let temp = tempfile::tempdir().unwrap();
            let data_dir = temp.path().join("admin");
            let manager = Manager::open(data_dir.clone()).await.unwrap();
            {
                let mut state = manager.shared.lock().expect("snapshot mutex");
                state.config.managed.clock_last = managed_now().saturating_add(3600);
            }
            manager
                .update_settings(Settings {
                    node_name: "Manual".into(),
                    poll_interval_seconds: 30,
                    history_limit: 300,
                })
                .await
                .unwrap();
            manager.shutdown().await.unwrap();

            let reopened = Manager::open(data_dir).await.unwrap();
            assert_eq!(reopened.snapshot().await.settings.node_name, "Manual");
            reopened.shutdown().await.unwrap();
        });
    }

    #[test]
    fn managed_clock_rollback_quarantines_managed_state_without_losing_manual_open() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let temp = tempfile::tempdir().unwrap();
            let data_dir = temp.path().join("admin");
            let manager = Manager::open(data_dir.clone()).await.unwrap();
            {
                let mut state = manager.shared.lock().expect("snapshot mutex");
                state.config.managed.clock_last = managed_now().saturating_add(3600);
                state.config.managed.pending.push(config::PendingRecord {
                    request_id: "rollback-test".into(),
                    share_id: "00".repeat(32),
                    owner: "owner".into(),
                    owner_address: None,
                    name: "test".into(),
                    permission: Some(Permission::ReadOnly),
                    root: temp.path().join("root").to_string_lossy().into_owned(),
                    state_root: temp.path().join("state").to_string_lossy().into_owned(),
                    ticket_file: data_dir
                        .join("managed/pending/")
                        .join(format!("{}.ticket", "00".repeat(32)))
                        .to_string_lossy()
                        .into_owned(),
                    created_at: managed_now(),
                    expires_at: None,
                    status: ManagedStatus::Waiting,
                    retry_at: None,
                    min_free_space_bytes: 0,
                });
            }
            manager.shutdown().await.unwrap();
            let reopened = Manager::open(data_dir).await.unwrap();
            assert_eq!(reopened.snapshot().await.pending.len(), 1);
            reopened
                .update_settings(Settings {
                    node_name: "Manual after rollback".into(),
                    poll_interval_seconds: 30,
                    history_limit: 300,
                })
                .await
                .unwrap();
            reopened.shutdown().await.unwrap();
        });
    }

    #[test]
    fn pending_preengine_failure_reattaches_the_exact_admission_lease() {
        if std::env::var_os("DELTAWEAVE_PREENGINE_LEASE_CHILD").is_none() {
            let home = tempfile::tempdir().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "managed_error_tests::pending_preengine_failure_reattaches_the_exact_admission_lease",
                    "--nocapture",
                ])
                .env("DELTAWEAVE_PREENGINE_LEASE_CHILD", "1")
                .env("HOME", home.path())
                .env("USERPROFILE", home.path())
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let temp = tempfile::tempdir().unwrap();
            let data_dir = temp.path().join("admin");
            let root = temp.path().join("member-root");
            let state_root = temp.path().join("member-state");
            std::fs::create_dir_all(&root).unwrap();

            let manager = Manager::open_with_options(
                data_dir.clone(),
                ManagerOptions {
                    managed_network: NetworkMode::DirectOnly,
                    managed_bind: None,
                },
            )
            .await
            .unwrap();
            let owner_secret = iroh::SecretKey::generate();
            let member_secret = iroh::SecretKey::generate();
            let owner = owner_secret.public();
            let share = ShareId([1; 32]);
            root_admission::reserve_private(&state_root).unwrap();
            private::prepare_directory(&state_root).unwrap();
            let lease = Arc::new(
                root_admission::acquire_with_private(
                    &root,
                    RootUse::Managed {
                        share: share.0,
                        owner: *owner.as_bytes(),
                    },
                    std::slice::from_ref(&state_root),
                )
                .unwrap(),
            );
            let lease_weak = Arc::downgrade(&lease);
            let pending = config::PendingRecord {
                request_id: "preengine-lease".into(),
                share_id: share_id_string(share),
                owner: owner.to_string(),
                owner_address: None,
                name: "preengine".into(),
                permission: Some(Permission::ReadWrite),
                root: root.to_string_lossy().into_owned(),
                state_root: state_root.to_string_lossy().into_owned(),
                ticket_file: data_dir
                    .join("managed/pending")
                    .join(format!("{}.ticket", "aa".repeat(32)))
                    .to_string_lossy()
                    .into_owned(),
                created_at: managed_now(),
                expires_at: None,
                status: ManagedStatus::Waiting,
                retry_at: None,
                min_free_space_bytes: 0,
            };
            manager
                .managed_slots
                .lock()
                .expect("managed slots mutex")
                .insert(
                    pending.share_id.clone(),
                    Arc::new(ManagedSlot {
                        operation: AsyncMutex::new(Some(ManagedWorker::Pending(PendingWorker {
                            request_id: pending.request_id.clone(),
                            lease: Some(Arc::clone(&lease)),
                        }))),
                    }),
                );
            drop(lease);
            let taken_lease = manager
                .take_pending_lease(&pending.share_id)
                .await
                .unwrap()
                .expect("pending caller must own the lease");
            let service = Arc::new(
                ShareService::open(
                    data_dir.join("standalone-service"),
                    NetworkMode::DirectOnly,
                    None,
                )
                .await
                .unwrap(),
            );
            let malformed = NetMembership {
                share_id: ShareId([2; 32]),
                owner,
                endpoint: member_secret.public(),
                permission: Permission::ReadWrite,
                replica: deltaweave_core::ReplicaId(deltaweave_core::Hash32::from_bytes([3; 32])),
                enrolled_at: managed_now(),
                revoked_at: None,
                epoch: 1,
            };
            let error = match manager
                .complete_pending_membership(
                    pending.clone(),
                    malformed,
                    Arc::clone(&service),
                    Some(taken_lease),
                )
                .await
            {
                Ok(_) => panic!("malformed membership must fail before engine open"),
                Err(error) => error,
            };
            assert!(is_share_error(&error, ShareError::MemberRevoked));

            let slot = manager
                .managed_slots
                .lock()
                .expect("managed slots mutex")
                .get(&pending.share_id)
                .cloned()
                .expect("pending slot reattached");
            let operation = slot.operation.lock().await;
            let Some(ManagedWorker::Pending(restored)) = operation.as_ref() else {
                panic!("pre-engine failure must restore pending worker");
            };
            let Some(restored_lease) = restored.lease.as_ref() else {
                panic!("pending worker lost its admission lease");
            };
            let original_lease = lease_weak
                .upgrade()
                .expect("the original lease allocation must remain alive");
            assert!(Arc::ptr_eq(restored_lease, &original_lease));
            drop(original_lease);
            drop(operation);
            assert!(
                root_admission::acquire_with_private(
                    &root,
                    RootUse::Managed {
                        share: share.0,
                        owner: *owner.as_bytes(),
                    },
                    std::slice::from_ref(&state_root),
                )
                .is_err(),
                "a competing admission attempt must not acquire the pending binding"
            );
            std::fs::create_dir_all(data_dir.join("managed/member-handle.key")).unwrap();
            let valid_member = NetMembership {
                share_id: share,
                owner,
                endpoint: member_secret.public(),
                permission: Permission::ReadWrite,
                replica: deltaweave_core::ReplicaId(deltaweave_core::Hash32::from_bytes([3; 32])),
                enrolled_at: managed_now(),
                revoked_at: None,
                epoch: 1,
            };
            let handle_error = match manager
                .complete_pending_membership(
                    pending.clone(),
                    valid_member,
                    Arc::clone(&service),
                    Some(
                        manager
                            .take_pending_lease(&pending.share_id)
                            .await
                            .unwrap()
                            .expect("pending caller must own the lease"),
                    ),
                )
                .await
            {
                Ok(_) => panic!("member handle failure must stop before engine open"),
                Err(error) => error,
            };
            assert!(!classify_managed_error(&handle_error).code.is_empty());
            let slot = manager
                .managed_slots
                .lock()
                .expect("managed slots mutex")
                .get(&pending.share_id)
                .cloned()
                .expect("pending slot survives member handle failure");
            let operation = slot.operation.lock().await;
            let Some(ManagedWorker::Pending(restored)) = operation.as_ref() else {
                panic!("member handle failure must restore pending worker");
            };
            let Some(restored_lease) = restored.lease.as_ref() else {
                panic!("member handle failure lost admission lease");
            };
            let original_lease = lease_weak
                .upgrade()
                .expect("the original lease allocation must remain alive");
            assert!(Arc::ptr_eq(restored_lease, &original_lease));
            drop(original_lease);
            drop(operation);
            manager.shutdown().await.unwrap();
            Arc::try_unwrap(service).unwrap().shutdown().await.unwrap();
        });
    }

    #[cfg(unix)]
    #[test]
    fn pending_gc_rejects_symlinked_ancestor_and_preserves_unrecognized_files() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            use std::os::unix::fs::symlink;

            let temp = tempfile::tempdir().unwrap();
            let data_dir = temp.path().join("admin");
            let manager = Manager::open(data_dir.clone()).await.unwrap();
            let external = temp.path().join("external");
            std::fs::create_dir_all(&external).unwrap();
            let sentinel = external.join("sentinel.txt");
            std::fs::write(&sentinel, b"must survive").unwrap();
            std::fs::create_dir_all(data_dir.join("managed")).unwrap();
            symlink(&external, data_dir.join("managed/pending")).unwrap();
            assert!(manager.gc_pending_tickets().await.is_err());
            assert_eq!(std::fs::read(&sentinel).unwrap(), b"must survive");
            std::fs::remove_file(data_dir.join("managed/pending")).unwrap();

            let pending = data_dir.join("managed/pending");
            private::prepare_directory(&pending).unwrap();
            let unrecognized = pending.join("user.ticket");
            let generated = pending.join(format!("{}.ticket", "cd".repeat(32)));
            std::fs::write(&unrecognized, b"user data").unwrap();
            std::fs::write(&generated, b"generated data").unwrap();
            manager.gc_pending_tickets().await.unwrap();
            assert!(unrecognized.is_file());
            assert!(!generated.exists());
            manager.shutdown().await.unwrap();
        });
    }
}
