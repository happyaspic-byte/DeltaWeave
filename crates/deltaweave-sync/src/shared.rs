//! Managed participants retain their logical replica, endpoint session, and root lease.
use super::*;
use deltaweave_net::{
    root_admission,
    share::{Permission, ShareError, ShareId, ShareService, ShareSession},
};
use serde::Serialize;
use std::{future::Future, pin::Pin};

/// Credential- and path-free error fields suitable for a management API.
#[derive(Clone, Copy, Debug, serde::Deserialize, Serialize)]
#[serde(tag = "kind", content = "share_error", rename_all = "snake_case")]
pub enum ManagedSyncFailure {
    /// Preserves the authenticated session's specific permission/offline/error classification.
    Share(ShareError),
    /// Safe private placement could not be established before local mutation.
    RecoveryUnavailable,
    /// A concurrent local filesystem edit requires a fresh round.
    LocalChanged,
    /// Durable recovery history is unavailable; automatic history reset is forbidden.
    StateUnavailable,
}
impl ManagedSyncFailure {
    /// Converts internal errors without exposing credentials, paths, or raw error chains.
    pub fn classify(error: &anyhow::Error) -> Self {
        match error.downcast_ref::<deltaweave_store::PreservationError>() {
            Some(deltaweave_store::PreservationError::RecoveryUnavailable) => {
                Self::RecoveryUnavailable
            }
            Some(deltaweave_store::PreservationError::LocalChanged) => Self::LocalChanged,
            Some(deltaweave_store::PreservationError::StateUnavailable) => Self::StateUnavailable,
            None => Self::Share(ShareError::classify(error)),
        }
    }
}

/// Private state and folder selected for an enrolled member.
#[derive(Clone, Debug)]
pub struct ManagedSyncConfig {
    /// Public synchronized namespace.
    pub root: PathBuf,
    /// Private index, checkpoint, CAS and recovery journal location.
    pub state_root: PathBuf,
    /// CDC profile used by read/write reconciliation.
    pub profile: ChunkingProfile,
    /// Free-space reserve checked before incoming staging and CAS writes.
    pub min_free_space_bytes: u64,
}

/// Role-specific result; RO never contains remote mutation counts.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "permission", content = "report", rename_all = "snake_case")]
pub enum ManagedSyncReport {
    /// Existing causal reconciliation and convergence semantics.
    ReadWrite(SyncReport),
    /// Authoritative owner application and durable local-work recovery locations.
    ReadOnly(ReadOnlyReport),
}

/// One enrolled participant. It never opens an owner runtime's index or another endpoint.
pub struct ManagedSyncEngine {
    inner: Arc<ManagedInner>,
    pending: std::sync::Mutex<Vec<tokio::sync::oneshot::Receiver<()>>>,
}
struct ManagedInner {
    local: Arc<ReplicaState>,
    session: ShareSession,
    gate: tokio::sync::Mutex<()>,
    recovery_only: bool,
    heartbeat_task: Option<tokio::task::JoinHandle<()>>,
    /// Owns the concrete E2 supplier guard without exposing its net-module
    /// type through this public sync API.  Calling it closes and unregisters
    /// the exact generation before the local root lease is released.
    supplier_drain: Option<SupplierDrain>,
}

type SupplierDrain =
    Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

async fn finish_managed_shutdown(
    session: ShareSession,
    heartbeat_task: Option<tokio::task::JoinHandle<()>>,
    supplier_drain: Option<SupplierDrain>,
) -> Result<()> {
    let mut first_error = None;
    if let Some(drain) = supplier_drain
        && let Err(error) = drain().await
    {
        // Supplier cleanup must not skip heartbeat/session cleanup. Keep
        // the original error and finish every independent local action.
        first_error = Some(error);
    }
    if let Some(task) = heartbeat_task {
        task.abort();
        if let Err(error) = task.await
            && !error.is_cancelled()
            && first_error.is_none()
        {
            first_error = Some(anyhow::anyhow!(
                "managed heartbeat task failed during shutdown"
            ));
        }
    }
    session.close().await;
    first_error.map_or(Ok(()), Err)
}

impl ManagedSyncEngine {
    /// Opens or resumes a persisted enrollment using its issuing owner and assigned replica.
    /// Keep the device service alive until `shutdown` drains this engine.
    pub fn open(
        service: &ShareService,
        owner: iroh::EndpointId,
        share: ShareId,
        config: ManagedSyncConfig,
    ) -> Result<Self> {
        Self::open_inner(service, owner, share, config, false, None, false)
    }

    /// Opens a newly enrolled member while transferring an already acquired
    /// admission lease into the engine.  The caller acquired this lease for
    /// the exact public root and private state root before contacting the
    /// owner; retaining it here closes the pending-to-active TOCTOU window.
    pub fn open_with_lease(
        service: &ShareService,
        owner: iroh::EndpointId,
        share: ShareId,
        config: ManagedSyncConfig,
        lease: Arc<root_admission::RootLease>,
    ) -> Result<Self> {
        Self::open_inner(service, owner, share, config, false, Some(lease), false)
    }

    /// Resumes an existing member only if both the index and recovery journal still exist.
    /// Controllers must use this for retained configurations; missing history is never reset.
    pub fn resume(
        service: &ShareService,
        owner: iroh::EndpointId,
        share: ShareId,
        config: ManagedSyncConfig,
    ) -> Result<Self> {
        Self::open_inner(service, owner, share, config, true, None, false)
    }

    /// Opens only the retained local state for exact ApplyStart/recovery
    /// queries. This path is valid for paused or revoked memberships: it does
    /// not register a supplier, start a heartbeat, request a fresh snapshot,
    /// or permit normal sync/public filesystem work.
    pub fn open_recovery(
        service: &ShareService,
        owner: iroh::EndpointId,
        share: ShareId,
        config: ManagedSyncConfig,
    ) -> Result<Self> {
        Self::open_inner(service, owner, share, config, true, None, true)
    }

    fn open_inner(
        service: &ShareService,
        owner: iroh::EndpointId,
        share: ShareId,
        config: ManagedSyncConfig,
        resume: bool,
        transferred_lease: Option<Arc<root_admission::RootLease>>,
        recovery_only: bool,
    ) -> Result<Self> {
        if recovery_only {
            // `acquire_with_private` is intentionally create-on-open for a
            // first enrollment.  A recovery process must never turn a lost
            // public namespace into a newly admitted one: the retained index
            // and journal are only meaningful with the exact old directory.
            validate_existing_managed_directory(&config.root)?;
            validate_existing_managed_directory(&config.state_root)?;
        }
        let index_exists = config.state_root.join("index.redb").is_file();
        let store_exists = config.state_root.join("store/metadata.redb").is_file();
        ensure!(
            !resume || (index_exists && store_exists),
            ShareError::StateUnavailable
        );
        ensure!(index_exists == store_exists, ShareError::StateUnavailable);
        let session = service.open_session(owner, share)?;
        let member = session.membership();
        config.profile.validate()?;
        let lease = if let Some(lease) = transferred_lease {
            // A transferred lease is meaningful only for the binding that
            // was admitted by the controller.  Path equality alone would
            // allow a Legacy or another share's lease to be transplanted, so
            // validate the complete public/private admission binding before
            // opening the index/store.
            validate_transferred_lease(&lease, owner, share, &config)?;
            lease
        } else {
            Arc::new(root_admission::acquire_with_private(
                &config.root,
                root_admission::RootUse::Managed {
                    share: share.0,
                    owner: *owner.as_bytes(),
                },
                std::slice::from_ref(&config.state_root),
            )?)
        };
        let root = lease.root().to_path_buf();
        let state = fs::canonicalize(&config.state_root)?;
        let index = Arc::new(LocalIndex::open(
            &root,
            state.join("index.redb"),
            member.replica,
            IndexOptions::default(),
        )?);
        let store = Arc::new(Store::open_with_recovery_reserver(
            state.join("store"),
            |path| root_admission::reserve_private(path),
        )?);
        // Managed recovery is authority-gated and must happen only after a
        // fresh owner snapshot in the role-specific sync path.  The legacy
        // SyncEngine keeps its existing generic recovery contract.
        let local = Arc::new(ReplicaState {
            _root_lease: lease,
            root,
            index,
            store,
            swarm_sources: Vec::new(),
            profile: config.profile,
            min_free_space_bytes: config.min_free_space_bytes,
            peer: owner.to_string(),
        });
        if member.permission == Permission::ReadOnly {
            ensure!(
                !resume || local.index.share_metadata()?.is_some(),
                ShareError::StateUnavailable
            );
            read_only::initialize(&local, member)?;
        }
        // Register the same Arc-backed index/store/lease used by this engine.
        // A member's roster row alone is only a discovery hint; the net
        // handler will serve chunks only while this exact guard is live.
        let supplier_drain = if recovery_only {
            None
        } else {
            let guard = service.register_supplier_storage(
                owner,
                share,
                member,
                &local.root,
                Arc::clone(&local._root_lease),
                Arc::clone(&local.index),
                Arc::clone(&local.store),
            )?;
            Some(Box::new(move || {
                let drain: Pin<Box<dyn Future<Output = Result<()>> + Send>> =
                    Box::pin(async move { guard.drain().await });
                drain
            }) as SupplierDrain)
        };
        let heartbeat_task = (!recovery_only).then(|| session.start_heartbeat());
        Ok(Self {
            pending: std::sync::Mutex::new(Vec::new()),
            inner: Arc::new(ManagedInner {
                local,
                session,
                gate: tokio::sync::Mutex::new(()),
                recovery_only,
                heartbeat_task,
                supplier_drain,
            }),
        })
    }

    /// Runs the member's durable role. Work retains the lease even if this future is cancelled.
    pub async fn sync_once(&self, observer: Option<TransferObserver>) -> Result<ManagedSyncReport> {
        ensure!(!self.inner.recovery_only, ShareError::PermissionDenied);
        let inner = self.inner.clone();
        let (finished, completion) = tokio::sync::oneshot::channel();
        {
            let mut pending = self
                .pending
                .lock()
                .map_err(|_| anyhow::anyhow!("managed lifetime lock poisoned"))?;
            pending.retain_mut(|receiver| {
                matches!(
                    receiver.try_recv(),
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty)
                )
            });
            pending.push(completion);
        }
        tokio::spawn(async move {
            let result = async {
                let _guard = inner.gate.lock().await;
                // Resolve response-lost/restarted swarm intents before any
                // new snapshot, provider, fallback, or public operation.
                // A nonterminal owner result remains a durable blocker.
                recover_managed_durable_state(&inner).await?;
                // Liveness is checked on the managed session before local
                // reconciliation. A separate supervisor repeats this work
                // every 30 seconds, so a long transfer cannot age the roster
                // past its 90-second freshness window.
                inner.session.ensure_roster_heartbeat().await?;
                inner.local.observe(&observer, "scanning", None, None, 0);
                match inner.session.membership().permission {
                    Permission::ReadWrite => inner
                        .local
                        .sync_managed_rw(&inner.session, &observer)
                        .await
                        .map(ManagedSyncReport::ReadWrite),
                    Permission::ReadOnly => {
                        read_only::sync(&inner.local, &inner.session, &observer)
                            .await
                            .map(ManagedSyncReport::ReadOnly)
                    }
                }
            }
            .await;
            // Heartbeat/control failures happen before the role-specific sync
            // can emit an event. Publish the terminal observer event outside
            // the fallible body so those failures are visible to management
            // callers and do not look like a silent cancelled round.
            inner.local.observe(
                &observer,
                if result.is_ok() { "complete" } else { "error" },
                None,
                None,
                0,
            );
            // Release every lease-owning clone before acknowledging drain completion.
            drop(inner);
            let _ = finished.send(());
            result
        })
        .await
        .context("managed sync task failed")?
    }

    /// Closes only durable, already-started work for a paused, revoked, or
    /// temporarily offline membership.  This endpoint deliberately skips
    /// heartbeat, fresh snapshots, grants, and public filesystem work so a
    /// lifecycle controller can make progress on an old receipt even when
    /// normal admission is closed.  The owner-side status/cancel/drain result
    /// and the local apply journal are both retained on an unknown response.
    pub async fn recover_pending(&self) -> Result<()> {
        let inner = self.inner.clone();
        let (finished, completion) = tokio::sync::oneshot::channel();
        {
            let mut pending = self
                .pending
                .lock()
                .map_err(|_| anyhow::anyhow!("managed lifetime lock poisoned"))?;
            pending.retain_mut(|receiver| {
                matches!(
                    receiver.try_recv(),
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty)
                )
            });
            pending.push(completion);
        }
        tokio::spawn(async move {
            let result = async {
                let _guard = inner.gate.lock().await;
                recover_managed_durable_state(&inner).await
            }
            .await;
            drop(inner);
            let _ = finished.send(());
            result
        })
        .await
        .context("managed recovery task failed")?
    }

    /// Runs a causal read/write round; using an RO grant fails before any work.
    pub async fn sync_read_write(&self, observer: Option<TransferObserver>) -> Result<SyncReport> {
        ensure!(
            self.inner.session.membership().permission == Permission::ReadWrite,
            ShareError::PermissionDenied
        );
        match self.sync_once(observer).await? {
            ManagedSyncReport::ReadWrite(report) => Ok(report),
            _ => unreachable!(),
        }
    }

    /// Applies only authenticated owner records and retains displaced local work privately.
    pub async fn sync_read_only(
        &self,
        observer: Option<TransferObserver>,
    ) -> Result<ReadOnlyReport> {
        ensure!(
            self.inner.session.membership().permission == Permission::ReadOnly,
            ShareError::PermissionDenied
        );
        match self.sync_once(observer).await? {
            ManagedSyncReport::ReadOnly(report) => Ok(report),
            _ => unreachable!(),
        }
    }

    /// Reads the retained index; registration or inventory does not establish online status.
    pub fn inventory(&self) -> Result<Inventory> {
        Inventory::from_index(&self.inner.local.index)
    }

    /// Lists durable recovery objects; incoming staging is never included.
    pub fn preserved_changes(&self) -> Result<Vec<PreservedLocalChange>> {
        read_only::preserved(&self.inner.local)
    }

    /// Drains active work before releasing the admission lease and shared endpoint session.
    pub async fn shutdown(self) -> Result<()> {
        for completion in self
            .pending
            .into_inner()
            .map_err(|_| anyhow::anyhow!("managed lifetime lock poisoned"))?
        {
            let _ = completion.await;
        }
        let inner = Arc::try_unwrap(self.inner)
            .map_err(|_| anyhow::anyhow!("managed work is still active"))?;
        let ManagedInner {
            session,
            heartbeat_task,
            supplier_drain,
            ..
        } = inner;
        finish_managed_shutdown(session, heartbeat_task, supplier_drain).await
    }
}

fn ensure_managed_swarm_recovery_complete(
    rows: &[deltaweave_net::share::ClientIntentRow],
) -> Result<()> {
    for row in rows {
        ensure!(
            matches!(
                row.phase,
                deltaweave_net::share::ClientIntentPhase::Drained
                    | deltaweave_net::share::ClientIntentPhase::Cancelled
            ),
            ShareError::RevocationPending
        );
    }
    Ok(())
}

async fn recover_managed_durable_state(inner: &ManagedInner) -> Result<()> {
    // Service-owned swarm intents are recovered first.  This closes the
    // exact activation/receipt state before any local apply journal is
    // considered complete, while still allowing both operations when the
    // owner has paused or revoked new admission.
    let recovered = inner.session.recover_swarm_intents(false).await?;
    let mut terminal = Vec::with_capacity(recovered.len());
    for row in recovered {
        if matches!(
            row.phase,
            deltaweave_net::share::ClientIntentPhase::Drained
                | deltaweave_net::share::ClientIntentPhase::Cancelled
        ) {
            terminal.push(row);
            continue;
        }

        // The compatibility boolean is intentionally not accepted as drain
        // evidence.  Close this exact operation in the service-owned task
        // registry, bind it to the same managed lease, and only then permit
        // the owner receipt to terminalize the durable row.  A paused or
        // revoked owner may still answer this recovery query; no heartbeat or
        // new payload admission is needed here.
        let state_root = inner
            .local
            .store
            .state_root()
            .parent()
            .context(ShareError::StateUnavailable)?
            .to_path_buf();
        let proof = inner
            .session
            .prove_local_io_drained(
                &row.grant,
                row.operation_id,
                Arc::clone(&inner.local._root_lease),
                &inner.local.root,
                state_root,
            )
            .await?;
        let recovered = inner
            .session
            .recover_swarm_intent_with_proof(&row.grant, row.operation_id, &proof)
            .await?;
        terminal.push(recovered);
    }
    ensure_managed_swarm_recovery_complete(&terminal)?;
    // A managed ApplyStart can also be left in the member's private index
    // after response loss.  Recover it before roster liveness: heartbeat is
    // an admission check and may correctly fail for a paused/revoked owner.
    inner
        .local
        .recover_managed_apply_before_liveness(&inner.session)
        .await
}

fn validate_transferred_lease(
    lease: &root_admission::RootLease,
    owner: iroh::EndpointId,
    share: ShareId,
    config: &ManagedSyncConfig,
) -> Result<()> {
    ensure!(
        fs::canonicalize(&config.root)? == lease.root(),
        ShareError::StateUnavailable
    );
    ensure!(
        lease.kind()
            == &root_admission::RootUse::Managed {
                share: share.0,
                owner: *owner.as_bytes(),
            },
        ShareError::StateUnavailable
    );
    let state = fs::canonicalize(&config.state_root)?;
    ensure!(
        lease
            .private_roots()
            .iter()
            .any(|private| private == &state),
        ShareError::StateUnavailable
    );
    Ok(())
}

/// Validates an already-created managed directory without following an alias
/// in any component.  Normal enrollment may create missing roots through the
/// admission layer; recovery uses this stricter preflight so an absent or
/// replaced public/state directory cannot be silently recreated or rebound.
fn validate_existing_managed_directory(path: &Path) -> Result<()> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut current = PathBuf::new();
    for component in absolute.components() {
        match component {
            std::path::Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            std::path::Component::ParentDir => {
                current.pop();
            }
            std::path::Component::CurDir => {}
            std::path::Component::RootDir | std::path::Component::Normal(_) => {
                current.push(component.as_os_str());
                let metadata = fs::symlink_metadata(&current)
                    .map_err(|_| anyhow::Error::new(ShareError::StateUnavailable))?;
                ensure!(
                    metadata.is_dir() && !metadata.file_type().is_symlink(),
                    ShareError::StateUnavailable
                );
                #[cfg(windows)]
                ensure!(
                    !managed_path_is_reparse_point(&current)?,
                    ShareError::StateUnavailable
                );
            }
        }
    }
    Ok(())
}

#[cfg(windows)]
fn managed_path_is_reparse_point(path: &Path) -> Result<bool> {
    let handle = winapi_util::Handle::from_path_any(path)
        .map_err(|_| anyhow::Error::new(ShareError::StateUnavailable))?;
    let information = winapi_util::file::information(&handle)
        .map_err(|_| anyhow::Error::new(ShareError::StateUnavailable))?;
    const FILE_ATTRIBUTE_REPARSE_POINT: u64 = 0x0400;
    Ok(information.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaweave_net::root_admission::{self, RootUse};

    #[test]
    fn transferred_lease_rejects_wrong_role_share_and_private_root() {
        if std::env::var_os("DW_MANAGED_LEASE_BINDING_CHILD").is_none() {
            let home = tempfile::tempdir().expect("isolated home");
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "shared::tests::transferred_lease_rejects_wrong_role_share_and_private_root",
                    "--nocapture",
                ])
                .env("DW_MANAGED_LEASE_BINDING_CHILD", "1")
                .env("HOME", home.path())
                .env("USERPROFILE", home.path())
                .status()
                .expect("run isolated lease test");
            assert!(status.success());
            return;
        }

        let temp = tempfile::tempdir().expect("test root");
        let owner = iroh::SecretKey::generate().public();
        let other_owner = iroh::SecretKey::generate().public();
        let share = ShareId([1; 32]);
        let other_share = ShareId([2; 32]);

        let root = temp.path().join("managed-root");
        let state = temp.path().join("managed-state");
        let wrong_state = temp.path().join("wrong-state");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&state).unwrap();
        std::fs::create_dir_all(&wrong_state).unwrap();
        root_admission::reserve_private(&state).unwrap();
        let lease = root_admission::acquire_with_private(
            &root,
            RootUse::Managed {
                share: share.0,
                owner: *owner.as_bytes(),
            },
            std::slice::from_ref(&state),
        )
        .unwrap();
        let config = ManagedSyncConfig {
            root: root.clone(),
            state_root: state.clone(),
            profile: ChunkingProfile::default(),
            min_free_space_bytes: 0,
        };

        assert!(validate_transferred_lease(&lease, owner, share, &config).is_ok());

        let legacy_root = temp.path().join("legacy-root");
        let legacy_state = temp.path().join("legacy-state");
        std::fs::create_dir_all(&legacy_root).unwrap();
        std::fs::create_dir_all(&legacy_state).unwrap();
        root_admission::reserve_private(&legacy_state).unwrap();
        let legacy_lease = root_admission::acquire_with_private(
            &legacy_root,
            RootUse::Legacy,
            std::slice::from_ref(&legacy_state),
        )
        .unwrap();
        let legacy_config = ManagedSyncConfig {
            root: legacy_root,
            state_root: legacy_state,
            profile: ChunkingProfile::default(),
            min_free_space_bytes: 0,
        };
        assert!(validate_transferred_lease(&legacy_lease, owner, share, &legacy_config).is_err());

        let other_root = temp.path().join("other-root");
        let other_state = temp.path().join("other-state");
        std::fs::create_dir_all(&other_root).unwrap();
        std::fs::create_dir_all(&other_state).unwrap();
        root_admission::reserve_private(&other_state).unwrap();
        let other_lease = root_admission::acquire_with_private(
            &other_root,
            RootUse::Managed {
                share: other_share.0,
                owner: *other_owner.as_bytes(),
            },
            std::slice::from_ref(&other_state),
        )
        .unwrap();
        let other_config = ManagedSyncConfig {
            root: other_root,
            state_root: other_state,
            profile: ChunkingProfile::default(),
            min_free_space_bytes: 0,
        };
        assert!(validate_transferred_lease(&other_lease, owner, share, &other_config).is_err());

        let wrong_private_config = ManagedSyncConfig {
            root,
            state_root: wrong_state,
            profile: ChunkingProfile::default(),
            min_free_space_bytes: 0,
        };
        assert!(validate_transferred_lease(&lease, owner, share, &wrong_private_config).is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn heartbeat_failure_reaches_managed_observer() {
        let name = "shared::tests::heartbeat_failure_reaches_managed_observer";
        if std::env::var("DW_MANAGED_HEARTBEAT_FAILURE_CHILD")
            .ok()
            .as_deref()
            != Some(name)
        {
            let profile = tempfile::tempdir().expect("isolated home");
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", name, "--nocapture"])
                .env("DW_MANAGED_HEARTBEAT_FAILURE_CHILD", name)
                .env("HOME", profile.path())
                .env("USERPROFILE", profile.path())
                .status()
                .expect("run isolated heartbeat test");
            assert!(status.success());
            return;
        }

        let temp = tempfile::tempdir().expect("test root");
        let owner = ShareService::open(
            temp.path().join("owner-service"),
            deltaweave_net::NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let member = ShareService::open(
            temp.path().join("member-service"),
            deltaweave_net::NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let owner_root = temp.path().join("owner-root");
        std::fs::create_dir_all(&owner_root).unwrap();
        std::fs::write(owner_root.join("heartbeat.txt"), b"heartbeat").unwrap();
        let owner_share = owner
            .create_owned_share(
                "Heartbeat".into(),
                owner_root,
                temp.path().join("owner-state"),
                None,
                0,
            )
            .await
            .unwrap();
        let share = owner_share.config().share_id;
        let ticket = owner_share
            .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
            .unwrap();
        member.enroll(&ticket, None).await.unwrap();
        let engine = ManagedSyncEngine::open(
            &member,
            owner.endpoint_id(),
            share,
            ManagedSyncConfig {
                root: temp.path().join("member-root"),
                state_root: temp.path().join("member-state"),
                profile: ChunkingProfile::DEFAULT,
                min_free_space_bytes: 0,
            },
        )
        .unwrap();
        drop(owner_share);
        owner.shutdown().await.unwrap();

        let phases = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let observed = phases.clone();
        let observer = TransferObserver::new(move |event| {
            observed.lock().expect("observer lock").push(event.phase);
        });
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            engine.sync_once(Some(observer)),
        )
        .await
        .expect("heartbeat failure must remain bounded");
        assert!(
            result.is_err(),
            "an offline owner must fail the managed round"
        );
        assert!(
            phases
                .lock()
                .expect("observer lock")
                .iter()
                .any(|phase| phase == "error"),
            "heartbeat failure must emit the terminal observer error event"
        );

        engine.shutdown().await.unwrap();
        member.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_finishes_cleanup_after_supplier_drain_error() {
        let name = "shared::tests::shutdown_finishes_cleanup_after_supplier_drain_error";
        if std::env::var("DW_MANAGED_SHUTDOWN_DRAIN_ERROR_CHILD")
            .ok()
            .as_deref()
            != Some(name)
        {
            let home = tempfile::tempdir().expect("isolated home");
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", name, "--nocapture"])
                .env("DW_MANAGED_SHUTDOWN_DRAIN_ERROR_CHILD", name)
                .env("HOME", home.path())
                .env("USERPROFILE", home.path())
                .status()
                .expect("run isolated shutdown test");
            assert!(status.success());
            return;
        }

        let temp = tempfile::tempdir().expect("test root");
        let owner = ShareService::open(
            temp.path().join("owner-service"),
            deltaweave_net::NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let member = ShareService::open(
            temp.path().join("member-service"),
            deltaweave_net::NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let owned = owner
            .create_owned_share(
                "Shutdown drain".into(),
                temp.path().join("owner-root"),
                temp.path().join("owner-state"),
                None,
                0,
            )
            .await
            .unwrap();
        let share = owned.config().share_id;
        member
            .enroll(
                &owned
                    .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
                    .unwrap(),
                None,
            )
            .await
            .unwrap();
        let session = member.open_session(owner.endpoint_id(), share).unwrap();

        struct DropMarker(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for DropMarker {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let heartbeat_stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let marker = DropMarker(Arc::clone(&heartbeat_stopped));
        let heartbeat_task = tokio::spawn(async move {
            let _marker = marker;
            std::future::pending::<()>().await;
        });
        let drain_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let drain_called_by_task = Arc::clone(&drain_called);
        let supplier_drain: SupplierDrain = Box::new(move || {
            Box::pin(async move {
                drain_called_by_task.store(true, std::sync::atomic::Ordering::SeqCst);
                Err(ShareError::StateUnavailable.into())
            })
        });

        let result =
            finish_managed_shutdown(session, Some(heartbeat_task), Some(supplier_drain)).await;
        assert!(matches!(
            result.as_ref().err().map(ShareError::classify),
            Some(ShareError::StateUnavailable)
        ));
        assert!(drain_called.load(std::sync::atomic::Ordering::SeqCst));
        assert!(heartbeat_stopped.load(std::sync::atomic::Ordering::SeqCst));

        drop(owned);
        member.shutdown().await.unwrap();
        owner.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recovery_open_skips_supplier_and_heartbeat_for_revoked_member() {
        let name = "shared::tests::recovery_open_skips_supplier_and_heartbeat_for_revoked_member";
        if std::env::var("DW_MANAGED_RECOVERY_OPEN_CHILD")
            .ok()
            .as_deref()
            != Some(name)
        {
            let home = tempfile::tempdir().expect("isolated home");
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", name, "--nocapture"])
                .env("DW_MANAGED_RECOVERY_OPEN_CHILD", name)
                .env("HOME", home.path())
                .env("USERPROFILE", home.path())
                .status()
                .expect("run isolated recovery-open test");
            assert!(status.success());
            return;
        }

        let temp = tempfile::tempdir().expect("test root");
        let owner = ShareService::open(
            temp.path().join("owner-service"),
            deltaweave_net::NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let member = ShareService::open(
            temp.path().join("member-service"),
            deltaweave_net::NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let owned = owner
            .create_owned_share(
                "Recovery open".into(),
                temp.path().join("owner-root"),
                temp.path().join("owner-state"),
                None,
                0,
            )
            .await
            .unwrap();
        let share = owned.config().share_id;
        member
            .enroll(
                &owned
                    .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
                    .unwrap(),
                None,
            )
            .await
            .unwrap();
        let config = ManagedSyncConfig {
            root: temp.path().join("member-root"),
            state_root: temp.path().join("member-state"),
            profile: ChunkingProfile::DEFAULT,
            min_free_space_bytes: 0,
        };
        let active =
            ManagedSyncEngine::open(&member, owner.endpoint_id(), share, config.clone()).unwrap();
        active.shutdown().await.unwrap();
        owned
            .revoke_member_strong(member.endpoint_id())
            .await
            .unwrap();

        let normal = ManagedSyncEngine::open(&member, owner.endpoint_id(), share, config.clone())
            .expect("the retained local relationship can still open before heartbeat refresh");
        let normal_error = normal
            .sync_once(None)
            .await
            .expect_err("normal sync must fail closed after owner revocation");
        assert!(matches!(
            ShareError::classify(&normal_error),
            ShareError::MemberRevoked | ShareError::Offline | ShareError::Busy
        ));
        normal.shutdown().await.unwrap();

        let recovery =
            ManagedSyncEngine::open_recovery(&member, owner.endpoint_id(), share, config.clone())
                .expect("recovery open must retain exact local state after revoke");
        assert!(recovery.recover_pending().await.is_ok());
        let normal_sync_error = recovery
            .sync_once(None)
            .await
            .expect_err("recovery-only engine must reject normal sync");
        assert_eq!(
            ShareError::classify(&normal_sync_error),
            ShareError::PermissionDenied
        );
        recovery.shutdown().await.unwrap();

        // Recovery must not recreate a missing public namespace merely because
        // the private index and CAS still exist.
        std::fs::remove_dir_all(&config.root).unwrap();
        assert!(
            ManagedSyncEngine::open_recovery(&member, owner.endpoint_id(), share, config).is_err()
        );
        assert!(!temp.path().join("member-root").exists());

        drop(owned);
        member.shutdown().await.unwrap();
        owner.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recovery_open_drains_persisted_apply_after_restart_and_revoke() {
        let name = "shared::tests::recovery_open_drains_persisted_apply_after_restart_and_revoke";
        if std::env::var("DW_MANAGED_APPLY_RECOVERY_CHILD")
            .ok()
            .as_deref()
            != Some(name)
        {
            let home = tempfile::tempdir().expect("isolated home");
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", name, "--nocapture"])
                .env("DW_MANAGED_APPLY_RECOVERY_CHILD", name)
                .env("HOME", home.path())
                .env("USERPROFILE", home.path())
                .status()
                .expect("run isolated apply recovery test");
            assert!(status.success());
            return;
        }

        let temp = tempfile::tempdir().expect("test root");
        let owner_path = temp.path().join("owner-service");
        let member_path = temp.path().join("member-service");
        let owner = ShareService::open(&owner_path, deltaweave_net::NetworkMode::DirectOnly, None)
            .await
            .unwrap();
        let member =
            ShareService::open(&member_path, deltaweave_net::NetworkMode::DirectOnly, None)
                .await
                .unwrap();
        let owned = owner
            .create_owned_share(
                "Apply recovery".into(),
                temp.path().join("owner-root"),
                temp.path().join("owner-state"),
                None,
                0,
            )
            .await
            .unwrap();
        let share = owned.config().share_id;
        member
            .enroll(
                &owned
                    .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
                    .unwrap(),
                None,
            )
            .await
            .unwrap();

        let config = ManagedSyncConfig {
            root: temp.path().join("member-root"),
            state_root: temp.path().join("member-state"),
            profile: ChunkingProfile::DEFAULT,
            min_free_space_bytes: 0,
        };
        let engine =
            ManagedSyncEngine::open(&member, owner.endpoint_id(), share, config.clone()).unwrap();
        let session = member.open_session(owner.endpoint_id(), share).unwrap();
        let empty = MerkleTree::from_records(Vec::new()).unwrap();
        let snapshot = session.fetch_authoritative_snapshot(&empty).await.unwrap();
        let permit = session
            .revalidate_before_apply(&snapshot.token)
            .await
            .unwrap();
        let operation_id = [0x6a; 16];
        session.apply_start(&permit, operation_id).await.unwrap();
        let started = session
            .apply_status(&permit, Some(operation_id))
            .await
            .unwrap();
        assert!(matches!(started.state, ApplyStateView::Started));

        // The local journal is written after ApplyStart to model response loss
        // at the boundary where a managed process has to preserve the exact
        // permit and operation for its next generation.
        let mut journal = engine
            .inner
            .local
            .load_managed_rw_journal(&session)
            .unwrap();
        journal.apply = Some(ManagedApplyJournal {
            permit: permit.clone(),
            operation_id,
            committed: false,
        });
        engine
            .inner
            .local
            .save_managed_rw_journal(&journal)
            .unwrap();
        engine.shutdown().await.unwrap();
        drop(session);

        let first_revoke = owned
            .revoke_member_strong(member.endpoint_id())
            .await
            .unwrap();
        assert!(matches!(
            first_revoke,
            deltaweave_net::share::RevocationReceipt::Pending { blockers, .. }
                if blockers > 0
        ));
        member.shutdown().await.unwrap();

        // Reopen the same service and state paths. Recovery is allowed to use
        // the old exact status/drain binding despite the now-revoked member;
        // it must not start a heartbeat, supplier, snapshot, or public write.
        let member =
            ShareService::open(&member_path, deltaweave_net::NetworkMode::DirectOnly, None)
                .await
                .unwrap();
        let recovery =
            ManagedSyncEngine::open_recovery(&member, owner.endpoint_id(), share, config).unwrap();
        recovery.recover_pending().await.unwrap();
        let reopened_session = member.open_session(owner.endpoint_id(), share).unwrap();
        let reopened = recovery
            .inner
            .local
            .load_managed_rw_journal(&reopened_session)
            .unwrap();
        assert!(
            reopened.apply.is_none(),
            "exact drain must clear the local journal"
        );
        drop(reopened_session);
        let completed = owned
            .revoke_member_strong(member.endpoint_id())
            .await
            .unwrap();
        assert!(matches!(
            completed,
            deltaweave_net::share::RevocationReceipt::Complete { .. }
        ));

        recovery.shutdown().await.unwrap();
        member.shutdown().await.unwrap();
        drop(owned);
        owner.shutdown().await.unwrap();
    }
}
