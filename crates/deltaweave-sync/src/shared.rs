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
        Self::open_inner(service, owner, share, config, false)
    }

    /// Resumes an existing member only if both the index and recovery journal still exist.
    /// Controllers must use this for retained configurations; missing history is never reset.
    pub fn resume(
        service: &ShareService,
        owner: iroh::EndpointId,
        share: ShareId,
        config: ManagedSyncConfig,
    ) -> Result<Self> {
        Self::open_inner(service, owner, share, config, true)
    }

    fn open_inner(
        service: &ShareService,
        owner: iroh::EndpointId,
        share: ShareId,
        config: ManagedSyncConfig,
        resume: bool,
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
        let lease = root_admission::acquire_with_private(
            &config.root,
            root_admission::RootUse::Managed {
                share: share.0,
                owner: *owner.as_bytes(),
            },
            std::slice::from_ref(&config.state_root),
        )?;
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
        Ok(Self {
            pending: std::sync::Mutex::new(Vec::new()),
            inner: Arc::new(ManagedInner {
                local,
                session,
                gate: tokio::sync::Mutex::new(()),
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
                inner.local.observe(&observer, "scanning", None, None, 0);
                let result = match inner.session.membership().permission {
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
                };
                inner.local.observe(
                    &observer,
                    if result.is_ok() { "complete" } else { "error" },
                    None,
                    None,
                    0,
                );
                result
            }
            .await;
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
        inner.session.close().await;
        Ok(())
    }
}
