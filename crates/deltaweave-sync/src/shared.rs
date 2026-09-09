//! Managed participants retain their logical replica, endpoint session, and root lease.
use super::*;
use deltaweave_net::{
    root_admission,
    share::{Permission, ShareError, ShareId, ShareService, ShareSession},
};
use serde::Serialize;

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
    heartbeat_task: Option<tokio::task::JoinHandle<()>>,
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
        Self::open_inner(service, owner, share, config, false, None)
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
        Self::open_inner(service, owner, share, config, false, Some(lease))
    }

    /// Resumes an existing member only if both the index and recovery journal still exist.
    /// Controllers must use this for retained configurations; missing history is never reset.
    pub fn resume(
        service: &ShareService,
        owner: iroh::EndpointId,
        share: ShareId,
        config: ManagedSyncConfig,
    ) -> Result<Self> {
        Self::open_inner(service, owner, share, config, true, None)
    }

    fn open_inner(
        service: &ShareService,
        owner: iroh::EndpointId,
        share: ShareId,
        config: ManagedSyncConfig,
        resume: bool,
        transferred_lease: Option<Arc<root_admission::RootLease>>,
    ) -> Result<Self> {
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
        if member.permission == Permission::ReadWrite {
            deltaweave_net::recover_causal_index(&store, &index, &root)?;
        }
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
        let heartbeat_task = session.start_heartbeat();
        Ok(Self {
            pending: std::sync::Mutex::new(Vec::new()),
            inner: Arc::new(ManagedInner {
                local,
                session,
                gate: tokio::sync::Mutex::new(()),
                heartbeat_task: Some(heartbeat_task),
            }),
        })
    }

    /// Runs the member's durable role. Work retains the lease even if this future is cancelled.
    pub async fn sync_once(&self, observer: Option<TransferObserver>) -> Result<ManagedSyncReport> {
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
                // Liveness is checked on the managed session before local
                // reconciliation. A separate supervisor repeats this work
                // every 30 seconds, so a long transfer cannot age the roster
                // past its 90-second freshness window.
                inner.session.ensure_roster_heartbeat().await?;
                inner.local.observe(&observer, "scanning", None, None, 0);
                match inner.session.membership().permission {
                    Permission::ReadWrite => {
                        inner.local.recover_pending()?;
                        let scan = scan_index(inner.local.index.clone()).await?;
                        ensure_scan_is_safe(&scan, "local")?;
                        let records = read_records(inner.local.index.clone()).await?;
                        let tree = MerkleTree::from_records(records.clone())?;
                        inner
                            .local
                            .sync_with_session(&inner.session, records, tree, &observer)
                            .await
                            .map(ManagedSyncReport::ReadWrite)
                    }
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
            ..
        } = inner;
        if let Some(task) = heartbeat_task {
            task.abort();
            let _ = task.await;
        }
        session.close().await;
        Ok(())
    }
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
}
