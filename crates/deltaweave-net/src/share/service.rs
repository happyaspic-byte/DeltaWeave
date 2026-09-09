use super::authority::{
    ApplyDrained, ApplyPermit, ApplyStart, AuthoritativeSnapshot, ManifestAttestation, ShareGrant,
    SnapshotToken, request_hash,
};
use super::roster::random_nonce;
use super::{
    ALPN_SWARM_V1, ALPN_V3, GrantNonce, LegacyProof, MemberRelationship, Membership,
    OwnedShareConfig, RosterHeartbeat, ShareError, ShareId, ShareTicket, SignedRoster,
    TicketPreview,
};
use super::{
    registry::Registry,
    runtime::{Authorization, OwnedRuntime, OwnerShare},
    wire::{self, Hello, Operation, Reply},
};
use crate::{
    NetworkMode, OperationAdmission, SyncClient, SyncHandler, SyncSession, SyncWireRequest,
    SyncWireResponse, bind_endpoint, endpoint_addr_with_local_fallback, load_or_create_identity,
    prepare_server_roots, read_frame,
    root_admission::{self, RootLease, RootUse},
    write_frame,
};
use anyhow::{Result, ensure};
use deltaweave_cdc::manifest_from_path;
use deltaweave_core::{ChunkingProfile, Hash32, ReplicaId, SyncRecord};
use deltaweave_index::{IndexOptions, LocalIndex};
use deltaweave_reconcile::MerkleTree;
use deltaweave_store::Store;
use iroh::{
    EndpointAddr, EndpointId, SecretKey,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler, Router},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock, atomic::Ordering},
    time::{Duration, Instant},
};

const CONTROL_DEADLINE: std::time::Duration = std::time::Duration::from_secs(15);

/// The provider-side activation lease returned after a signed owner reply.
///
/// `deadline` is derived from the instant immediately before the activation
/// request was sent.  Keeping that local monotonic deadline next to the reply
/// prevents a caller from accidentally starting a fresh lease when a reply is
/// received late.  The type is intentionally local to this process and is not
/// serialized onto the wire.
#[derive(Clone, Debug)]
pub struct ActivationLease {
    pub reply: super::ActivateGrantReply,
    pub deadline: Instant,
}

impl ActivationLease {
    fn from_reply_at(
        reply: super::ActivateGrantReply,
        request_started: Instant,
        now: Instant,
    ) -> Result<Self> {
        ensure!(reply.accepted, ShareError::GrantReplay);
        let duration = Duration::from_secs(u64::from(
            reply
                .max_duration_secs
                .min(super::authority::MAX_ACTIVATE_TTL_SECONDS),
        ));
        let deadline = request_started
            .checked_add(duration)
            .ok_or(ShareError::GrantExpired)?;
        ensure!(now < deadline, ShareError::GrantExpired);
        Ok(Self { reply, deadline })
    }

    fn from_reply(reply: super::ActivateGrantReply, request_started: Instant) -> Result<Self> {
        Self::from_reply_at(reply, request_started, Instant::now())
    }

    /// Returns the remaining monotonic lifetime without exposing wall-clock
    /// expiry or allowing a late response to extend this lease.
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    /// Whether this local activation lease can still admit provider work.
    pub fn is_active(&self) -> bool {
        Instant::now() < self.deadline
    }
}

/// One device-wide persistent endpoint. Clone its endpoint for all outbound shares;
/// legacy per-folder identities remain separate and are never rebound here.
#[derive(Debug)]
pub struct ShareService {
    router: Router,
    key: SecretKey,
    registry: Arc<Registry>,
    runtimes: Arc<RwLock<BTreeMap<ShareId, Arc<OwnedRuntime>>>>,
    lifecycle: tokio::sync::Mutex<()>,
    mode: NetworkMode,
    active: Arc<tokio::sync::RwLock<()>>,
}

/// Opaque ownership proof for the already-bound device endpoint.  It exposes
/// no bind or legacy protocol registration operation; E may use it only when
/// constructing the separate grant-gated adapter.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct ShareEndpointOwnership {
    endpoint: crate::Endpoint,
    id: EndpointId,
}
impl ShareEndpointOwnership {
    #[allow(dead_code)]
    pub(crate) fn endpoint_id(&self) -> EndpointId {
        self.id
    }
    #[allow(dead_code)]
    pub(crate) fn endpoint(&self) -> &crate::Endpoint {
        &self.endpoint
    }
}

/// Holds the exact admission/index/store Arcs used by a member runtime.  A
/// later swarm handler must retain this guard for every supplier operation and
/// drain it before releasing the engine's root lease.
#[allow(dead_code)]
#[derive(Debug)]
pub struct SupplierRegistrationGuard {
    owner: EndpointId,
    share: ShareId,
    membership: Membership,
    root_lease: Arc<RootLease>,
    index: Arc<LocalIndex>,
    store: Arc<Store>,
    drained: Arc<std::sync::atomic::AtomicBool>,
}
impl SupplierRegistrationGuard {
    #[allow(dead_code)]
    pub(crate) fn owner(&self) -> EndpointId {
        self.owner
    }
    #[allow(dead_code)]
    pub(crate) fn share(&self) -> ShareId {
        self.share
    }
    #[allow(dead_code)]
    pub(crate) fn membership(&self) -> &Membership {
        &self.membership
    }
    #[allow(dead_code)]
    pub(crate) fn root_lease(&self) -> &Arc<RootLease> {
        &self.root_lease
    }
    #[allow(dead_code)]
    pub(crate) fn index(&self) -> &Arc<LocalIndex> {
        &self.index
    }
    #[allow(dead_code)]
    pub(crate) fn store(&self) -> &Arc<Store> {
        &self.store
    }
    pub async fn drain(&mut self) -> Result<()> {
        self.drained.store(true, Ordering::SeqCst);
        Ok(())
    }
}
impl ShareService {
    pub async fn open(
        state: impl AsRef<Path>,
        mode: NetworkMode,
        bind: Option<SocketAddr>,
    ) -> Result<Self> {
        let state = root_admission::reserve_private(state)?;
        root_admission::private_directory(&state)?;
        let key = load_or_create_identity(state.join("device.key"))?.secret_key;
        let registry = Arc::new(Registry::open(&state, key.public())?);
        let endpoint = bind_endpoint(
            key.clone(),
            mode,
            Some(vec![ALPN_V3.to_vec(), ALPN_SWARM_V1.to_vec()]),
            bind,
        )
        .await?;
        let runtimes = Arc::new(RwLock::new(BTreeMap::new()));
        let active = Arc::new(tokio::sync::RwLock::new(()));
        let handler = Handler {
            registry: registry.clone(),
            key: key.clone(),
            runtimes: runtimes.clone(),
            limit: Arc::new(tokio::sync::Semaphore::new(64)),
            active: active.clone(),
        };
        let router = Router::builder(endpoint)
            .accept(ALPN_V3, handler)
            .accept(ALPN_SWARM_V1, SwarmAdmissionHandler)
            .spawn();
        Ok(Self {
            router,
            key,
            registry,
            runtimes,
            lifecycle: tokio::sync::Mutex::new(()),
            mode,
            active,
        })
    }
    pub fn endpoint_id(&self) -> EndpointId {
        self.key.public()
    }
    #[allow(dead_code)]
    pub(crate) fn endpoint_ownership(&self) -> ShareEndpointOwnership {
        ShareEndpointOwnership {
            endpoint: self.router.endpoint().clone(),
            id: self.endpoint_id(),
        }
    }
    pub fn endpoint_addr(&self) -> EndpointAddr {
        endpoint_addr_with_local_fallback(self.router.endpoint())
    }
    /// E's provider handler uses the existing registry and endpoint rather
    /// than opening a second authority/store.  This wrapper keeps the
    /// registry private while exposing the exact grant admission primitive.
    #[allow(dead_code)]
    pub(crate) fn validate_provider_grant(
        &self,
        grant: &ShareGrant,
        remote_consumer: EndpointId,
    ) -> Result<Instant> {
        self.registry
            .validate_provider_grant(grant, self.endpoint_id(), remote_consumer)
    }
    #[allow(dead_code)]
    pub(crate) fn drain_grant(
        &self,
        share: ShareId,
        nonce: GrantNonce,
        activation_id: [u8; 16],
        remote_peer: EndpointId,
    ) -> Result<()> {
        self.registry
            .drain_grant(share, nonce, activation_id, remote_peer)
    }
    pub async fn wait_online(&self, timeout: std::time::Duration) -> bool {
        if self.mode == NetworkMode::DirectOnly {
            return crate::wait_for_direct_address(self.router.endpoint(), timeout)
                .await
                .is_ok();
        }
        tokio::time::timeout(timeout, self.router.endpoint().online())
            .await
            .is_ok()
    }
    pub fn owned_configs(&self) -> Result<Vec<OwnedShareConfig>> {
        self.registry.configs()
    }
    /// For import, supply the exact retained DB-bound logical replica. A wrong
    /// replica fails LocalIndex's unchanged binding check without resetting state.
    pub async fn create_owned_share(
        &self,
        name: String,
        root: PathBuf,
        state_root: PathBuf,
        replica: Option<ReplicaId>,
        min_free_space_bytes: u64,
    ) -> Result<OwnerShare> {
        let _lifecycle = self.lifecycle.lock().await;
        ensure!(
            !name.is_empty() && name.len() <= 255 && !name.chars().any(char::is_control),
            ShareError::InvalidTicket
        );
        let share_id = ShareId(super::ticket::random_bytes());
        let (lease, config) = root_admission::admit_with_private(
            &root,
            RootUse::Managed {
                share: share_id.0,
                owner: *self.endpoint_id().as_bytes(),
            },
            &[state_root],
            |root, private| {
                let state_root = &private[0];
                let retained = LocalIndex::read_bound_replica(root, state_root.join("index.redb"))?;
                if let (Some(requested), Some(retained)) = (replica, retained) {
                    ensure!(requested == retained, ShareError::ReplicaClaimRejected);
                }
                let config = OwnedShareConfig {
                    share_id,
                    owner: self.endpoint_id(),
                    name,
                    root: root.to_path_buf(),
                    state_root: state_root.clone(),
                    replica: retained.or(replica).unwrap_or_else(|| {
                        ReplicaId(Hash32::from_bytes(super::ticket::random_bytes()))
                    }),
                    min_free_space_bytes,
                };
                // Short synchronous intent commit under admission serialization;
                // no runtime lock or disk handler may be acquired here.
                self.registry
                    .insert_share(config.clone(), BTreeSet::new())?;
                Ok(config)
            },
        )?;
        self.load_config_with_lease(config, lease, false).await
    }
    pub async fn load_owned_share(&self, share: ShareId) -> Result<OwnerShare> {
        let _lifecycle = self.lifecycle.lock().await;
        if let Some(runtime) = self.runtimes.read().expect("runtime map").get(&share) {
            return Ok(OwnerShare {
                runtime: runtime.clone(),
                key: self.key.clone(),
            });
        }
        self.load_config(self.registry.config(share)?).await
    }

    /// Loads an owner runtime with admission disabled before it enters the
    /// endpoint map.  Restart recovery uses this for a durable Paused state so
    /// no peer can observe a transient enabled window.
    pub async fn load_owned_share_paused(&self, share: ShareId) -> Result<OwnerShare> {
        let _lifecycle = self.lifecycle.lock().await;
        if let Some(runtime) = self.runtimes.read().expect("runtime map").get(&share) {
            runtime.disable();
            return Ok(OwnerShare {
                runtime: runtime.clone(),
                key: self.key.clone(),
            });
        }
        let config = self.registry.config(share)?;
        let lease = root_admission::acquire_with_private(
            &config.root,
            RootUse::Managed {
                share: share.0,
                owner: *config.owner.as_bytes(),
            },
            std::slice::from_ref(&config.state_root),
        )?;
        self.load_config_with_lease(config, lease, true).await
    }

    /// Pauses and drains a managed owner runtime before deleting only its
    /// transport catalog entry. The manager retains local files and state.
    pub async fn unload_owned_share(&self, share: ShareId) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        let runtime = self.runtimes.write().expect("runtime map").remove(&share);
        if let Some(runtime) = runtime {
            runtime.pause().await;
        }
        self.registry.remove_share(share)
    }

    pub fn forget_membership(&self, owner: EndpointId, share: ShareId) -> Result<()> {
        self.registry.forget_relationship(owner, share)
    }

    /// Registers the already-open member storage handles for a future swarm
    /// supplier.  The persisted relationship is authoritative; caller-supplied
    /// permission, epoch, replica, endpoint, or root metadata cannot replace
    /// it, and this function never opens a second root/index/store.
    #[allow(clippy::too_many_arguments)]
    pub fn register_supplier_storage(
        &self,
        owner: EndpointId,
        share: ShareId,
        membership: &Membership,
        root: &Path,
        root_lease: Arc<RootLease>,
        index: Arc<LocalIndex>,
        store: Arc<Store>,
    ) -> Result<SupplierRegistrationGuard> {
        let persisted = self.registry.relationship(owner, share)?;
        ensure!(
            persisted.membership == *membership
                && membership.endpoint == self.endpoint_id()
                && membership.owner == owner
                && membership.share_id == share
                && membership.revoked_at.is_none(),
            ShareError::ReplicaClaimRejected
        );
        let canonical_root = fs::canonicalize(root)?;
        ensure!(
            root_lease.root() == canonical_root.as_path(),
            ShareError::StateUnavailable
        );
        ensure!(
            matches!(
                root_lease.kind(),
                RootUse::Managed {
                    share: admitted_share,
                    owner: admitted_owner
                } if admitted_share == &share.0 && admitted_owner == owner.as_bytes()
            ),
            ShareError::OwnerMismatch
        );
        ensure!(
            fs::canonicalize(index.root())? == canonical_root,
            ShareError::StateUnavailable
        );
        let store_state = fs::canonicalize(store.state_root())?;
        ensure!(
            root_lease
                .private_roots()
                .iter()
                .any(|private| store_state.starts_with(private)),
            ShareError::StateUnavailable
        );
        Ok(SupplierRegistrationGuard {
            owner,
            share,
            membership: persisted.membership,
            root_lease,
            index,
            store,
            drained: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }
    async fn load_config(&self, config: OwnedShareConfig) -> Result<OwnerShare> {
        let lease = root_admission::acquire_with_private(
            &config.root,
            RootUse::Managed {
                share: config.share_id.0,
                owner: *config.owner.as_bytes(),
            },
            std::slice::from_ref(&config.state_root),
        )?;
        self.load_config_with_lease(config, lease, false).await
    }
    async fn load_config_with_lease(
        &self,
        config: OwnedShareConfig,
        lease: RootLease,
        paused: bool,
    ) -> Result<OwnerShare> {
        let registry = self.registry.clone();
        let ready = registry.is_ready(config.share_id)?;
        let runtime = tokio::task::spawn_blocking(move || -> Result<_> {
            let lease = Arc::new(lease);
            if ready {
                ensure!(
                    config.state_root.join("index.redb").is_file()
                        && config.state_root.join("metadata.redb").is_file(),
                    ShareError::StateUnavailable
                );
            }
            let (root, state_root) = prepare_server_roots(&config.root, &config.state_root)?;
            ensure!(
                root == config.root && state_root == config.state_root,
                ShareError::StateUnavailable
            );
            let index = Arc::new(LocalIndex::open(
                &root,
                state_root.join("index.redb"),
                config.replica,
                IndexOptions::default(),
            )?);
            if ready {
                ensure!(
                    index.share_metadata()?.is_some(),
                    ShareError::StateUnavailable
                );
            }
            let store = Arc::new(Store::open_with_recovery_reserver(&state_root, |path| {
                crate::root_admission::reserve_private(path)
            })?);
            store.recover_path_changes(&root)?;
            let runtime = OwnedRuntime::new(config, registry, index, store, lease)?;
            if paused {
                runtime.disable();
            }
            let report = runtime.index.scan()?;
            crate::ensure_index_report_safe(&report)?;
            runtime.refresh_causal_state()?;
            Ok(Arc::new(runtime))
        })
        .await??;
        self.registry.mark_ready(runtime.config.share_id)?;
        self.runtimes
            .write()
            .expect("runtime map")
            .insert(runtime.config.share_id, runtime.clone());
        Ok(OwnerShare {
            runtime,
            key: self.key.clone(),
        })
    }
    pub fn relationships(&self) -> Result<Vec<MemberRelationship>> {
        self.registry.relationships()
    }
    pub async fn validate_ticket(&self, ticket: &ShareTicket) -> Result<TicketPreview> {
        ticket.verify_at(super::now())?;
        let connection = self.connect_ticket(ticket).await?;
        let result = self
            .exchange_bounded(
                &connection,
                Hello {
                    version: 3,
                    share_id: ticket.preview().share_id,
                    operation: Operation::Validate(ticket.clone()),
                },
            )
            .await;
        connection.close(0u8.into(), b"validation complete");
        match result? {
            Reply::Validated(preview) => {
                ensure!(preview == ticket.preview(), ShareError::InvalidTicket);
                Ok(preview)
            }
            _ => Err(ShareError::Protocol.into()),
        }
    }
    pub async fn enroll(
        &self,
        ticket: &ShareTicket,
        proof: Option<LegacyProof>,
    ) -> Result<Membership> {
        ticket.verify_at(super::now())?;
        let connection = self.connect_ticket(ticket).await?;
        let result = self
            .exchange_bounded(
                &connection,
                Hello {
                    version: 3,
                    share_id: ticket.preview().share_id,
                    operation: Operation::Enroll {
                        ticket: ticket.clone(),
                        proof: proof.map(Box::new),
                    },
                },
            )
            .await;
        connection.close(0u8.into(), b"enrollment complete");
        match result? {
            Reply::Enrolled(member) => {
                ensure!(
                    member.owner == ticket.preview().owner
                        && member.share_id == ticket.preview().share_id
                        && member.endpoint == self.endpoint_id(),
                    ShareError::OwnerMismatch
                );
                self.registry.store_relationship(MemberRelationship {
                    membership: member.clone(),
                    address: ticket.address(),
                })?;
                Ok(member)
            }
            _ => Err(ShareError::Protocol.into()),
        }
    }

    /// Restores a durable membership after a lost enrollment response.
    ///
    /// This path intentionally has no ticket and never allocates a membership or
    /// logical replica. The owner authenticates the caller from the QUIC peer ID and
    /// returns only the currently active binding for the requested share.
    pub async fn resume_membership(
        &self,
        owner: EndpointId,
        share: ShareId,
        address: EndpointAddr,
    ) -> Result<Membership> {
        ensure!(owner != self.endpoint_id(), ShareError::OwnerMismatch);
        ensure!(address.id == owner, ShareError::OwnerMismatch);
        let connection = self.connect_address(address.clone()).await?;
        let result = self
            .exchange_bounded(
                &connection,
                Hello {
                    version: 3,
                    share_id: share,
                    operation: Operation::Resume,
                },
            )
            .await;
        connection.close(0u8.into(), b"membership resume complete");
        match result? {
            Reply::Resumed(member) => {
                ensure!(
                    member.owner == owner
                        && member.share_id == share
                        && member.endpoint == self.endpoint_id(),
                    ShareError::OwnerMismatch
                );
                self.registry.store_relationship(MemberRelationship {
                    membership: member.clone(),
                    address,
                })?;
                ensure!(member.revoked_at.is_none(), ShareError::MemberRevoked);
                Ok(member)
            }
            _ => Err(ShareError::Protocol.into()),
        }
    }

    async fn connect_ticket(&self, ticket: &ShareTicket) -> Result<Connection> {
        ensure!(
            ticket.preview().owner != self.endpoint_id(),
            ShareError::OwnerMismatch
        );
        self.connect_address(ticket.address()).await
    }

    async fn connect_address(&self, address: EndpointAddr) -> Result<Connection> {
        tokio::time::timeout(
            CONTROL_DEADLINE,
            self.router.endpoint().connect(address, ALPN_V3),
        )
        .await
        .map_err(|_| anyhow::Error::new(ShareError::Offline))?
        .map_err(|_| ShareError::Offline.into())
    }

    async fn exchange_bounded(&self, connection: &Connection, hello: Hello) -> Result<Reply> {
        tokio::time::timeout(CONTROL_DEADLINE, wire::exchange(connection, hello))
            .await
            .map_err(|_| anyhow::Error::new(ShareError::Offline))?
    }
    /// Opens an issuer-pinned session from persisted enrollment. No caller-supplied
    /// role is accepted, and the existing endpoint is cloned, never rebound.
    pub fn open_session(&self, owner: EndpointId, share: ShareId) -> Result<ShareSession> {
        let relationship = self.registry.relationship(owner, share)?;
        ensure!(
            relationship.membership.owner == owner
                && relationship.address.id == owner
                && owner != self.endpoint_id(),
            ShareError::OwnerMismatch
        );
        Ok(ShareSession {
            membership: relationship.membership,
            roster_challenge: Mutex::new(None),
            inner: SyncSession {
                client: SyncClient {
                    secret_key: self.key.clone(),
                    remote: relationship.address,
                    network_mode: self.mode,
                },
                endpoint: self.router.endpoint().clone(),
                share: Some(share),
            },
        })
    }
    /// A managed member engine must retain this lease for its entire lifetime.
    pub fn admit_member_root(
        &self,
        owner: EndpointId,
        share: ShareId,
        root: impl AsRef<Path>,
    ) -> Result<RootLease> {
        self.registry.relationship(owner, share)?;
        root_admission::acquire(
            root,
            RootUse::Managed {
                share: share.0,
                owner: *owner.as_bytes(),
            },
        )
    }
    pub async fn shutdown(self) -> Result<()> {
        let runtimes: Vec<_> = self
            .runtimes
            .read()
            .expect("runtime map")
            .values()
            .cloned()
            .collect();
        for runtime in runtimes {
            runtime.pause().await;
        }
        self.runtimes.write().expect("runtime map").clear();
        self.router.shutdown().await?;
        let _drained = self.active.write().await;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ShareSession {
    membership: Membership,
    roster_challenge: Mutex<Option<GrantNonce>>,
    inner: SyncSession,
}
impl ShareSession {
    pub fn membership(&self) -> &Membership {
        &self.membership
    }

    /// Fetches the owner-signed member roster and arms one heartbeat challenge
    /// for the next address update. Roster addresses are transport hints only;
    /// persisted membership remains the authorization authority.
    pub async fn refresh_roster(&self) -> Result<SignedRoster> {
        let result = self.control_exchange(Operation::Roster).await?;
        let (roster, challenge) = match result {
            Reply::Roster { roster, challenge } => (roster, challenge),
            _ => return Err(ShareError::Protocol.into()),
        };
        roster.verify_for(
            self.membership.owner,
            self.membership.share_id,
            super::now(),
        )?;
        ensure!(
            roster
                .member(self.membership.endpoint)
                .is_some_and(|entry| {
                    entry.permission == self.membership.permission
                        && entry.member_epoch == self.membership.epoch
                }),
            ShareError::EpochMismatch
        );
        *self
            .roster_challenge
            .lock()
            .expect("roster challenge mutex") = Some(challenge);
        Ok(roster)
    }

    /// Returns the most recently issued challenge for callers that need to
    /// schedule an explicit heartbeat.
    #[must_use]
    pub fn roster_challenge(&self) -> Option<GrantNonce> {
        *self
            .roster_challenge
            .lock()
            .expect("roster challenge mutex")
    }

    /// Signs the current endpoint address and submits it to the owner. The
    /// owner authenticates both the QUIC peer identity and this signature
    /// before updating the durable roster address.
    pub async fn heartbeat(&self, challenge: GrantNonce) -> Result<()> {
        let address = crate::endpoint_addr_with_local_fallback(&self.inner.endpoint);
        let heartbeat = RosterHeartbeat::sign(
            &self.inner.client.secret_key,
            self.membership.owner,
            self.membership.share_id,
            address,
            challenge,
            super::now(),
        );
        let result = self
            .control_exchange(Operation::Heartbeat(heartbeat))
            .await?;
        let roster = match result {
            Reply::Heartbeat(roster) => roster,
            _ => return Err(ShareError::Protocol.into()),
        };
        roster.verify_for(
            self.membership.owner,
            self.membership.share_id,
            super::now(),
        )?;
        ensure!(
            roster
                .member(self.membership.endpoint)
                .is_some_and(|entry| {
                    entry.permission == self.membership.permission
                        && entry.member_epoch == self.membership.epoch
                }),
            ShareError::EpochMismatch
        );
        let mut stored = self
            .roster_challenge
            .lock()
            .expect("roster challenge mutex");
        if stored.as_ref() == Some(&challenge) {
            *stored = None;
        }
        Ok(())
    }

    async fn control_exchange(&self, operation: Operation) -> Result<Reply> {
        let connection = self.connect_control().await?;
        let result = tokio::time::timeout(
            CONTROL_DEADLINE,
            wire::exchange(
                &connection,
                Hello {
                    version: 3,
                    share_id: self.membership.share_id,
                    operation,
                },
            ),
        )
        .await;
        connection.close(0u8.into(), b"share control complete");
        result.map_err(|_| anyhow::Error::new(ShareError::Offline))?
    }

    async fn connect_control(&self) -> Result<Connection> {
        tokio::time::timeout(
            CONTROL_DEADLINE,
            self.inner
                .endpoint
                .connect(self.inner.client.remote.clone(), ALPN_V3),
        )
        .await
        .map_err(|_| anyhow::Error::new(ShareError::Offline))?
        .map_err(|_| ShareError::Offline.into())
    }

    /// Requests the complete owner-signed snapshot used by managed apply.
    /// `local` is retained for the caller's reconciliation API symmetry; the
    /// owner response is always independently complete and Merkle-verified.
    pub async fn fetch_authoritative_snapshot(
        &self,
        _local: &MerkleTree,
    ) -> Result<AuthoritativeSnapshot> {
        let result = self.control_exchange(Operation::Snapshot).await?;
        let snapshot = match result {
            Reply::Snapshot(snapshot) => snapshot,
            _ => return Err(ShareError::Protocol.into()),
        };
        snapshot.verify_complete(
            self.membership.owner,
            self.membership.share_id,
            super::now(),
        )?;
        ensure!(
            snapshot.token.epoch == self.membership.epoch,
            ShareError::EpochMismatch
        );
        Ok(snapshot)
    }

    /// Requests an owner attestation for one exact snapshot record.
    pub async fn request_manifest(
        &self,
        snapshot: &SnapshotToken,
        record: &SyncRecord,
    ) -> Result<ManifestAttestation> {
        ensure!(
            snapshot.epoch == self.membership.epoch,
            ShareError::EpochMismatch
        );
        let result = self
            .control_exchange(Operation::Manifest {
                snapshot: snapshot.clone(),
                record: record.clone(),
            })
            .await?;
        let attestation = match result {
            Reply::Manifest(attestation) => attestation,
            _ => return Err(ShareError::Protocol.into()),
        };
        attestation.verify_for(
            self.membership.owner,
            self.membership.share_id,
            super::now(),
        )?;
        attestation.verify_record(snapshot, record)?;
        Ok(attestation)
    }

    /// Requests one owner-signed grant for a sorted, duplicate-free subset of
    /// manifest chunks.  A grant is bound to this authenticated consumer and
    /// cannot be reused for a different provider or subset.
    pub async fn request_swarm_grant(
        &self,
        provider: EndpointId,
        snapshot: &SnapshotToken,
        manifest: &ManifestAttestation,
        hashes: &[Hash32],
    ) -> Result<ShareGrant> {
        ensure!(
            snapshot.epoch == self.membership.epoch,
            ShareError::EpochMismatch
        );
        ensure!(
            snapshot.share == self.membership.share_id,
            ShareError::OwnerMismatch
        );
        ensure!(
            manifest.share == snapshot.share,
            ShareError::ManifestMismatch
        );
        ensure!(manifest.epoch == snapshot.epoch, ShareError::EpochMismatch);
        super::authority::validate_hash_subset(hashes)?;
        let result = self
            .control_exchange(Operation::SwarmGrant {
                provider,
                snapshot: snapshot.clone(),
                manifest: manifest.clone(),
                hashes: hashes.to_vec(),
            })
            .await?;
        let grant = match result {
            Reply::Grant(grant) => grant,
            _ => return Err(ShareError::Protocol.into()),
        };
        grant.verify_for(
            self.membership.owner,
            self.membership.share_id,
            super::now(),
        )?;
        ensure!(
            grant.consumer == self.membership.endpoint,
            ShareError::EndpointMismatch
        );
        ensure!(grant.provider == provider, ShareError::EndpointMismatch);
        ensure!(
            grant.epoch == self.membership.epoch,
            ShareError::EpochMismatch
        );
        ensure!(
            grant.request_hash
                == request_hash(
                    self.membership.share_id,
                    snapshot.snapshot,
                    manifest.manifest_hash,
                    hashes,
                )?,
            ShareError::ManifestMismatch
        );
        Ok(grant)
    }

    /// Activates one member-provider grant at the owner.  The provider's
    /// monotonic start is captured before the control exchange; the owner
    /// applies its own receive-side deadline and never extends this lease from
    /// a wall-clock expiry.
    pub async fn activate_grant(&self, grant: &ShareGrant) -> Result<ActivationLease> {
        ensure!(
            grant.provider == self.membership.endpoint,
            ShareError::EndpointMismatch
        );
        ensure!(
            grant.share == self.membership.share_id
                && grant.owner == self.membership.owner
                && grant.provider_epoch == self.membership.epoch,
            ShareError::EpochMismatch
        );
        let started = Instant::now();
        let result = self
            .control_exchange(Operation::Activate(grant.activate_request()))
            .await?;
        ensure!(
            started.elapsed() <= CONTROL_DEADLINE,
            ShareError::GrantExpired
        );
        let reply = match result {
            Reply::Activate(reply) => reply,
            _ => return Err(ShareError::Protocol.into()),
        };
        reply.verify_for(self.membership.owner, self.membership.share_id)?;
        ensure!(
            reply.provider == self.membership.endpoint,
            ShareError::EndpointMismatch
        );
        ensure!(reply.nonce == grant.nonce, ShareError::GrantReplay);
        ActivationLease::from_reply(reply, started)
    }

    /// Sends one authenticated endpoint drain acknowledgement for an active
    /// grant.  The owner retains the grant as Active until its other endpoint
    /// sends the same activation-bound acknowledgement.
    pub async fn grant_drained(&self, grant: &ShareGrant, activation_id: [u8; 16]) -> Result<()> {
        ensure!(
            grant.share == self.membership.share_id,
            ShareError::OwnerMismatch
        );
        ensure!(
            grant.consumer == self.membership.endpoint
                || grant.provider == self.membership.endpoint,
            ShareError::EndpointMismatch
        );
        let result = self
            .control_exchange(Operation::GrantDrained {
                nonce: grant.nonce,
                activation_id,
            })
            .await?;
        ensure!(matches!(result, Reply::GrantDrained), ShareError::Protocol);
        Ok(())
    }

    /// Revalidates the owner root and obtains a short-lived apply permit.
    pub async fn revalidate_before_apply(&self, snapshot: &SnapshotToken) -> Result<ApplyPermit> {
        ensure!(
            snapshot.epoch == self.membership.epoch,
            ShareError::EpochMismatch
        );
        let result = self
            .control_exchange(Operation::Revalidate {
                snapshot: snapshot.clone(),
            })
            .await?;
        let permit = match result {
            Reply::ApplyPermit(permit) => permit,
            _ => return Err(ShareError::Protocol.into()),
        };
        permit.verify_for(
            self.membership.owner,
            self.membership.share_id,
            self.membership.endpoint,
            super::now(),
        )?;
        ensure!(
            permit.epoch == self.membership.epoch,
            ShareError::EpochMismatch
        );
        ensure!(
            permit.snapshot == snapshot.snapshot,
            ShareError::ManifestMismatch
        );
        ensure!(
            permit.root_hash == snapshot.root_hash,
            ShareError::ManifestMismatch
        );
        Ok(permit)
    }

    /// Alias retained for the network contract and managed engine adapters.
    pub async fn revalidate(&self, snapshot: &SnapshotToken) -> Result<ApplyPermit> {
        self.revalidate_before_apply(snapshot).await
    }

    /// Records the beginning of a permit-scoped local apply at the owner.
    pub async fn apply_start(&self, permit: &ApplyPermit, operation_id: [u8; 16]) -> Result<()> {
        ensure!(
            permit.consumer == self.membership.endpoint,
            ShareError::EndpointMismatch
        );
        let result = self
            .control_exchange(Operation::ApplyStart(ApplyStart {
                operation_id,
                permit_nonce: permit.nonce,
            }))
            .await?;
        ensure!(matches!(result, Reply::ApplyAccepted), ShareError::Protocol);
        Ok(())
    }

    /// Records the durable writer drain acknowledgement for a permit.
    pub async fn apply_drained(
        &self,
        permit: &ApplyPermit,
        operation_id: [u8; 16],
        committed: bool,
    ) -> Result<()> {
        ensure!(
            permit.consumer == self.membership.endpoint,
            ShareError::EndpointMismatch
        );
        let result = self
            .control_exchange(Operation::ApplyDrained(ApplyDrained {
                operation_id,
                permit_nonce: permit.nonce,
                committed,
            }))
            .await?;
        ensure!(matches!(result, Reply::ApplyAccepted), ShareError::Protocol);
        Ok(())
    }

    pub async fn fetch_snapshot(&self, local: &MerkleTree) -> Result<crate::RemoteSnapshot> {
        self.inner.fetch_snapshot(local).await
    }
    pub async fn pull_record(
        &self,
        record: SyncRecord,
        store: Arc<Store>,
    ) -> Result<crate::PullReceipt> {
        self.inner.pull_record(record, store).await
    }
    pub async fn pull_record_to(
        &self,
        record: SyncRecord,
        store: Arc<Store>,
        destination_root: PathBuf,
    ) -> Result<crate::PullReceipt> {
        self.inner
            .pull_record_to(record, store, destination_root)
            .await
    }
    pub async fn pull_record_to_with_budget(
        &self,
        record: SyncRecord,
        store: Arc<Store>,
        destination_root: PathBuf,
        min_free_space_bytes: u64,
        pending_destination_bytes: u64,
    ) -> Result<crate::PullReceipt> {
        self.inner
            .pull_record_to_with_budget(
                record,
                store,
                destination_root,
                min_free_space_bytes,
                pending_destination_bytes,
            )
            .await
    }
    pub async fn push_record(
        &self,
        source: impl AsRef<Path>,
        record: SyncRecord,
        profile: ChunkingProfile,
    ) -> Result<crate::SyncApplyReceipt> {
        self.inner.push_record(source, record, profile).await
    }
    pub async fn apply_metadata(&self, record: SyncRecord) -> Result<crate::SyncApplyReceipt> {
        self.inner.apply_metadata(record).await
    }
    /// Releases only this session; the shared device endpoint remains alive.
    pub async fn close(self) {
        self.inner.close().await;
    }
}

#[derive(Clone, Debug)]
struct Handler {
    registry: Arc<Registry>,
    key: SecretKey,
    runtimes: Arc<RwLock<BTreeMap<ShareId, Arc<OwnedRuntime>>>>,
    limit: Arc<tokio::sync::Semaphore>,
    active: Arc<tokio::sync::RwLock<()>>,
}

/// D2 owns the ALPN registration but deliberately exposes no data success
/// path.  E replaces this rejection handler with the grant/manifest/endpoint
/// adapter once its provider-side verification is complete.
#[derive(Clone, Copy, Debug)]
struct SwarmAdmissionHandler;
impl ProtocolHandler for SwarmAdmissionHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        connection.close(0u8.into(), b"share swarm grant required");
        Ok(())
    }
}

impl ProtocolHandler for Handler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let Ok(permit) = self.limit.clone().try_acquire_owned() else {
            connection.close(0u8.into(), b"share busy");
            return Ok(());
        };
        let active = self.active.clone().read_owned().await;
        let handler = self.clone();
        // Router cancellation must not drop a future that owns blocking disk work.
        // The spawned handler retains its runtime/lease and drains every join handle.
        let _ = tokio::spawn(async move {
            let _permit = permit;
            let _active = active;
            let outcome = handler.run(connection.clone()).await;
            if outcome.is_err() {
                connection.close(0u8.into(), b"share operation ended");
            }
        })
        .await;
        Ok(())
    }
}
impl Handler {
    async fn run(&self, connection: Connection) -> Result<()> {
        let (mut send, mut receive) = connection.accept_bi().await?;
        let hello = wire::read_hello(&mut receive).await?;
        let runtime = self
            .runtimes
            .read()
            .expect("runtime map")
            .get(&hello.share_id)
            .cloned();
        let Some(runtime) = runtime else {
            write_frame(&mut send, &Reply::Error(ShareError::UnknownShare)).await?;
            send.finish()?;
            connection.closed().await;
            return Ok(());
        };
        let peer = connection.remote_id();
        let operation_started = Instant::now();
        let operation = hello.operation;
        let result = match operation {
            authority @ (Operation::Snapshot
            | Operation::Manifest { .. }
            | Operation::SwarmGrant { .. }
            | Operation::Revalidate { .. }
            | Operation::ApplyStart(_)
            | Operation::ApplyDrained(_)
            | Operation::Activate(_)
            | Operation::GrantDrained { .. }) => {
                self.run_authority(&runtime, peer, hello.share_id, authority, operation_started)
                    .await
            }
            operation => (|| -> Result<Reply> {
                ensure!(runtime.enabled.load(Ordering::SeqCst), ShareError::Busy);
                match operation {
                    Operation::Validate(ticket) => {
                        ensure!(
                            ticket.preview().share_id == hello.share_id,
                            ShareError::InvalidTicket
                        );
                        self.registry.validate(&ticket)?;
                        Ok(Reply::Validated(ticket.preview()))
                    }
                    Operation::Enroll { ticket, proof } => {
                        ensure!(
                            ticket.preview().share_id == hello.share_id,
                            ShareError::InvalidTicket
                        );
                        Ok(Reply::Enrolled(self.registry.enroll(
                            &ticket,
                            peer,
                            proof.as_deref(),
                        )?))
                    }
                    Operation::Session => {
                        self.registry.authorize(hello.share_id, peer, false)?;
                        Ok(Reply::Accepted)
                    }
                    Operation::Resume => Ok(Reply::Resumed(self.registry.authorize(
                        hello.share_id,
                        peer,
                        false,
                    )?)),
                    Operation::Roster => {
                        let (roster, challenge) = self.registry.issue_roster_challenge(
                            &self.key,
                            hello.share_id,
                            peer,
                        )?;
                        Ok(Reply::Roster { roster, challenge })
                    }
                    Operation::Heartbeat(heartbeat) => {
                        ensure!(heartbeat.share == hello.share_id, ShareError::OwnerMismatch);
                        Ok(Reply::Heartbeat(
                            self.registry
                                .accept_roster_heartbeat(&self.key, &heartbeat, peer)?,
                        ))
                    }
                    _ => Err(ShareError::Protocol.into()),
                }
            })(),
        };
        let reply = result.unwrap_or_else(|error| Reply::Error(safe_error(&error)));
        let session = matches!(reply, Reply::Accepted);
        // Denied and preview-only connections cannot join a drain after revoke
        // took its close snapshot. An accepted session registers before its final
        // authorization check, so every operation is either closed or denied.
        let _tracked = session.then(|| runtime.track(connection.clone()));
        if session {
            ensure!(runtime.enabled.load(Ordering::SeqCst), ShareError::Busy);
            self.registry.authorize(hello.share_id, peer, false)?;
        }
        write_frame(&mut send, &reply).await?;
        send.finish()?;
        if !session {
            connection.closed().await;
            return Ok(());
        }
        let member = self.registry.authorize(hello.share_id, peer, false)?;
        let auth = Authorization {
            runtime: runtime.clone(),
            peer,
            epoch: member.epoch,
        };
        let handler = SyncHandler {
            active_handlers: self.active.clone(),
            _root_lease: runtime.lease.clone(),
            share_authorization: Some(auth.clone()),
            admission: Arc::new(OperationAdmission::default()),
            observer: runtime.observer.lock().expect("observer mutex").clone(),
            store: runtime.store.clone(),
            index: runtime.index.clone(),
            destination_root: runtime.config.root.clone(),
            peer_policy: crate::PeerPolicy::AllowListed([peer].into_iter().collect()),
            apply_lock: runtime.gate.clone(),
            connection_limit: self.limit.clone(),
            min_free_space_bytes: runtime.config.min_free_space_bytes,
            state_root: runtime.config.state_root.clone(),
            receive_admission_lock: runtime.receive_gate.clone(),
        };
        let (mut send, mut receive) = connection.accept_bi().await?;
        let result = async {
            auth.check(false)?;
            match read_frame::<SyncWireRequest>(&mut receive).await? {
                request @ SyncWireRequest::QueryNode { .. } => {
                    handler
                        .handle_query_session(request, &mut send, &mut receive)
                        .await
                }
                SyncWireRequest::PullRecord { record } => {
                    handler
                        .handle_pull(record, &mut send, &mut receive, peer)
                        .await
                }
                SyncWireRequest::PushRecord { record, manifest } => {
                    handler
                        .handle_push_record(record, manifest, &mut send, &mut receive, peer)
                        .await
                }
                SyncWireRequest::ApplyMetadata { record } => {
                    handler.handle_metadata(record, &mut send).await
                }
                _ => Err(ShareError::Protocol.into()),
            }
        }
        .await;
        if let Err(error) = result {
            let _ = write_frame(&mut send, &SyncWireResponse::ShareError(safe_error(&error))).await;
        }
        let _ = send.finish();
        // All disk/chunk work has completed before the tracked guard can disappear.
        connection.closed().await;
        Ok(())
    }

    /// Handles the owner-authoritative v1 swarm control records.  Each branch
    /// rechecks the authenticated QUIC peer and the live catalog binding; the
    /// serialized wire values are never treated as a caller-selected role.
    async fn run_authority(
        &self,
        runtime: &Arc<OwnedRuntime>,
        peer: EndpointId,
        share: ShareId,
        operation: Operation,
        operation_started: Instant,
    ) -> Result<Reply> {
        // Admission and new writes stop as soon as a runtime is paused or
        // revoked, but an already-started operation must still be able to
        // record its authenticated drain acknowledgement.  Otherwise a
        // normal shutdown would turn a known writer into a permanent Pending
        // blocker merely because the runtime was disabled first.
        if !matches!(
            operation,
            Operation::ApplyDrained(_) | Operation::GrantDrained { .. }
        ) {
            ensure!(runtime.enabled.load(Ordering::SeqCst), ShareError::Busy);
        }
        ensure!(runtime.config.share_id == share, ShareError::UnknownShare);
        match operation {
            Operation::Snapshot => {
                let member = self.registry.authorize(share, peer, false)?;
                let _gate = runtime.gate.lock().await;
                let report = runtime.index.scan()?;
                crate::ensure_index_report_safe(&report)?;
                runtime.refresh_causal_state()?;
                let records = runtime.index.sync_records()?;
                let tree = MerkleTree::from_records(records.clone())?;
                let token = SnapshotToken::sign(
                    &self.key,
                    share,
                    member.epoch,
                    random_nonce(),
                    tree.root_hash(),
                    records.len(),
                    super::now(),
                )?;
                let snapshot = AuthoritativeSnapshot { token, records };
                snapshot.verify_complete(self.key.public(), share, super::now())?;
                self.registry.store_snapshot(&snapshot)?;
                Ok(Reply::Snapshot(snapshot))
            }
            Operation::Manifest { snapshot, record } => {
                let _member = self.registry.authorize(share, peer, false)?;
                ensure!(snapshot.share == share, ShareError::OwnerMismatch);
                snapshot.verify_for(self.key.public(), share, super::now())?;
                let _gate = runtime.gate.lock().await;
                let stored = self
                    .registry
                    .snapshot(snapshot.snapshot)?
                    .ok_or(ShareError::ManifestMismatch)?;
                ensure!(stored.token == snapshot, ShareError::ManifestMismatch);
                let stored_record = stored
                    .records
                    .iter()
                    .find(|candidate| candidate.logical_hash() == record.logical_hash())
                    .ok_or(ShareError::ManifestMismatch)?;
                ensure!(stored_record == &record, ShareError::ManifestMismatch);
                ensure!(!record.tombstone, ShareError::ManifestMismatch);
                ensure!(
                    record.kind == deltaweave_core::SyncEntryKind::File,
                    ShareError::ManifestMismatch
                );
                let path = runtime.config.root.join(record.path.as_str());
                let manifest = tokio::task::spawn_blocking(move || {
                    manifest_from_path(path, ChunkingProfile::DEFAULT)
                })
                .await??;
                let attestation = ManifestAttestation::sign(
                    &self.key,
                    &snapshot,
                    &record,
                    manifest,
                    super::now(),
                )?;
                attestation.verify_for(self.key.public(), share, super::now())?;
                Ok(Reply::Manifest(attestation))
            }
            Operation::SwarmGrant {
                provider,
                snapshot,
                manifest,
                hashes,
            } => {
                ensure!(snapshot.share == share, ShareError::OwnerMismatch);
                ensure!(manifest.share == share, ShareError::OwnerMismatch);
                ensure!(manifest.epoch == snapshot.epoch, ShareError::EpochMismatch);
                ensure!(peer != provider, ShareError::EndpointMismatch);
                if provider == self.key.public() {
                    ensure!(runtime.config.share_id == share, ShareError::UnknownShare);
                    ensure!(runtime.enabled.load(Ordering::SeqCst), ShareError::Busy);
                }
                let grant = self
                    .registry
                    .issue_swarm_grant(&self.key, peer, provider, &snapshot, &manifest, &hashes)?;
                Ok(Reply::Grant(grant))
            }
            Operation::Revalidate { snapshot } => {
                let _member = self.registry.authorize(share, peer, false)?;
                let _gate = runtime.gate.lock().await;
                let report = runtime.index.scan()?;
                crate::ensure_index_report_safe(&report)?;
                runtime.refresh_causal_state()?;
                let records = runtime.index.sync_records()?;
                let root = MerkleTree::from_records(records)?.root_hash();
                let permit = self
                    .registry
                    .issue_apply_permit(&self.key, peer, &snapshot, root)?;
                Ok(Reply::ApplyPermit(permit))
            }
            Operation::ApplyStart(start) => {
                let _member = self.registry.authorize(share, peer, false)?;
                let _gate = runtime.gate.lock().await;
                let records = runtime.index.sync_records()?;
                let root = MerkleTree::from_records(records)?.root_hash();
                self.registry
                    .apply_start(self.key.public(), share, peer, root, &start)?;
                Ok(Reply::ApplyAccepted)
            }
            Operation::ApplyDrained(drained) => {
                self.registry
                    .apply_drained(self.key.public(), share, peer, &drained)?;
                Ok(Reply::ApplyAccepted)
            }
            Operation::GrantDrained {
                nonce,
                activation_id,
            } => {
                self.registry
                    .drain_grant(share, nonce, activation_id, peer)?;
                Ok(Reply::GrantDrained)
            }
            Operation::Activate(request) => {
                ensure!(request.share == share, ShareError::OwnerMismatch);
                let reply = self.registry.activate_grant_at(
                    &self.key,
                    &request,
                    peer,
                    operation_started,
                )?;
                Ok(Reply::Activate(reply))
            }
            _ => Err(ShareError::Protocol.into()),
        }
    }
}
fn safe_error(error: &anyhow::Error) -> ShareError {
    ShareError::classify(error)
}

#[cfg(test)]
mod tests {
    use super::super::Permission;
    use super::*;

    async fn roster_exchange(
        client: &ShareService,
        owner: &ShareService,
        share: ShareId,
        operation: Operation,
    ) -> Result<Reply> {
        let connection = client
            .router
            .endpoint()
            .connect(owner.endpoint_addr(), ALPN_V3)
            .await?;
        let result = wire::exchange(
            &connection,
            Hello {
                version: 3,
                share_id: share,
                operation,
            },
        )
        .await;
        connection.close(0u8.into(), b"roster test complete");
        result
    }

    #[test]
    fn activation_lease_keeps_request_start_deadline_and_reports_reduced_time() {
        let owner = SecretKey::generate();
        let provider = SecretKey::generate();
        let reply = super::super::authority::ActivateGrantReply::sign(
            &owner,
            ShareId([0x31; 32]),
            provider.public(),
            [0x32; 32],
            [0x33; 16],
            true,
            10,
        )
        .unwrap();
        let request_started = Instant::now();
        let reply_received = request_started + Duration::from_secs(4);
        let lease = ActivationLease::from_reply_at(reply, request_started, reply_received).unwrap();

        assert_eq!(
            lease.deadline,
            request_started + Duration::from_secs(10),
            "the response must not start a fresh activation lease"
        );
        assert_eq!(
            lease.deadline.saturating_duration_since(reply_received),
            Duration::from_secs(6),
            "a delayed response must retain only the remaining request lifetime"
        );
    }

    #[test]
    fn activation_lease_rejects_a_reply_that_arrived_after_request_deadline() {
        let owner = SecretKey::generate();
        let provider = SecretKey::generate();
        let reply = super::super::authority::ActivateGrantReply::sign(
            &owner,
            ShareId([0x41; 32]),
            provider.public(),
            [0x42; 32],
            [0x43; 16],
            true,
            15,
        )
        .unwrap();
        let request_started = Instant::now();
        let error = ActivationLease::from_reply_at(
            reply,
            request_started,
            request_started + Duration::from_secs(16),
        )
        .expect_err("a delayed activation reply must not be revived");

        assert_eq!(
            error.downcast_ref::<ShareError>(),
            Some(&ShareError::GrantExpired)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn authenticated_roster_heartbeat_updates_address_and_rejects_replay() {
        let name = "share::service::tests::authenticated_roster_heartbeat_updates_address_and_rejects_replay";
        if std::env::var("DW_ROSTER_CHILD").ok().as_deref() != Some(name) {
            let profile = tempfile::tempdir().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", name, "--nocapture"])
                .env("DW_ROSTER_CHILD", name)
                .env("HOME", profile.path())
                .env("USERPROFILE", profile.path())
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }

        let temp = tempfile::tempdir().unwrap();
        let owner = ShareService::open(temp.path().join("owner"), NetworkMode::DirectOnly, None)
            .await
            .unwrap();
        let member = ShareService::open(temp.path().join("member"), NetworkMode::DirectOnly, None)
            .await
            .unwrap();
        let outsider =
            ShareService::open(temp.path().join("outsider"), NetworkMode::DirectOnly, None)
                .await
                .unwrap();
        let owner_share = owner
            .create_owned_share(
                "Files".into(),
                temp.path().join("root"),
                temp.path().join("state"),
                None,
                0,
            )
            .await
            .unwrap();
        let share = owner_share.config().share_id;
        let ticket = owner_share
            .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
            .unwrap();
        let membership = member.enroll(&ticket, None).await.unwrap();
        let session = member.open_session(owner.endpoint_id(), share).unwrap();

        let roster = session.refresh_roster().await.unwrap();
        roster
            .verify_for(owner.endpoint_id(), share, super::super::now())
            .unwrap();
        let entry = roster.member(member.endpoint_id()).unwrap();
        assert_eq!(entry.permission, membership.permission);
        assert_eq!(entry.member_epoch, membership.epoch);
        assert_eq!(entry.heartbeat_at, 0);
        assert_eq!(entry.heartbeat_expires_at, 0);
        assert!(!roster.member_is_fresh(member.endpoint_id(), super::super::now()));
        let first_challenge = session.roster_challenge().unwrap();
        session.heartbeat(first_challenge).await.unwrap();

        let stored = owner.registry.stored_roster(share).unwrap().unwrap();
        stored
            .verify_for(owner.endpoint_id(), share, super::super::now())
            .unwrap();
        let first_entry = stored.member(member.endpoint_id()).unwrap();
        assert_eq!(first_entry.address.id, member.endpoint_id());
        assert!(first_entry.heartbeat_at > 0);
        assert!(first_entry.heartbeat_expires_at > first_entry.heartbeat_at);
        assert!(stored.member_is_fresh(member.endpoint_id(), super::super::now()));

        let _ = session.refresh_roster().await.unwrap();
        let second_challenge = session.roster_challenge().unwrap();
        let receive_floor = super::super::now();
        let changed_address = EndpointAddr::from_parts(
            member.endpoint_id(),
            [iroh::TransportAddr::Ip("127.0.0.1:39999".parse().unwrap())],
        );
        let changed = RosterHeartbeat::sign(
            &member.key,
            owner.endpoint_id(),
            share,
            changed_address.clone(),
            second_challenge,
            receive_floor.saturating_sub(4),
        );
        let reply = roster_exchange(
            &member,
            &owner,
            share,
            Operation::Heartbeat(changed.clone()),
        )
        .await
        .unwrap();
        assert!(matches!(reply, Reply::Heartbeat(_)));
        let changed_stored = owner.registry.stored_roster(share).unwrap().unwrap();
        assert!(
            changed_stored
                .member(member.endpoint_id())
                .unwrap()
                .heartbeat_at
                >= receive_floor,
            "owner receive time must anchor heartbeat liveness"
        );
        assert_eq!(
            changed_stored.member(member.endpoint_id()).unwrap().address,
            changed_address
        );

        let replay =
            match roster_exchange(&member, &owner, share, Operation::Heartbeat(changed)).await {
                Ok(_) => panic!("replayed heartbeat was accepted"),
                Err(error) => error,
            };
        assert_eq!(
            replay.downcast_ref::<ShareError>(),
            Some(&ShareError::HeartbeatReplay)
        );

        let bad_share = ShareId([0x55; 32]);
        let cross_share = RosterHeartbeat::sign(
            &member.key,
            owner.endpoint_id(),
            bad_share,
            member.endpoint_addr(),
            [7; 32],
            super::super::now(),
        );
        let cross_share_error = match roster_exchange(
            &member,
            &owner,
            share,
            Operation::Heartbeat(cross_share),
        )
        .await
        {
            Ok(_) => panic!("cross-share heartbeat was accepted"),
            Err(error) => error,
        };
        assert_eq!(
            cross_share_error.downcast_ref::<ShareError>(),
            Some(&ShareError::OwnerMismatch)
        );

        let outsider_error =
            match roster_exchange(&outsider, &owner, share, Operation::Roster).await {
                Ok(_) => panic!("non-member roster request was accepted"),
                Err(error) => error,
            };
        assert_eq!(
            outsider_error.downcast_ref::<ShareError>(),
            Some(&ShareError::NotMember)
        );

        drop(session);
        drop(owner_share);
        outsider.shutdown().await.unwrap();
        member.shutdown().await.unwrap();
        owner.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn authority_control_round_trip_binds_snapshot_manifest_grant_and_apply() {
        let name = "share::service::tests::authority_control_round_trip_binds_snapshot_manifest_grant_and_apply";
        if std::env::var("DW_AUTHORITY_CHILD").ok().as_deref() != Some(name) {
            let profile = tempfile::tempdir().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", name, "--nocapture"])
                .env("DW_AUTHORITY_CHILD", name)
                .env("HOME", profile.path())
                .env("USERPROFILE", profile.path())
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let owner = ShareService::open(
            temp.path().join("owner-service"),
            NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let member = ShareService::open(
            temp.path().join("member-service"),
            NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let root = temp.path().join("shared-root");
        let state = temp.path().join("shared-state");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("authority.txt"), b"authority payload").unwrap();
        let owner_share = owner
            .create_owned_share("Authority".into(), root.clone(), state, None, 0)
            .await
            .unwrap();
        let share = owner_share.config().share_id;
        let ticket = owner_share
            .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
            .unwrap();
        let membership = member.enroll(&ticket, None).await.unwrap();
        let session = member.open_session(owner.endpoint_id(), share).unwrap();

        let empty = MerkleTree::from_records(Vec::new()).unwrap();
        let snapshot = session.fetch_authoritative_snapshot(&empty).await.unwrap();
        assert_eq!(snapshot.token.epoch, membership.epoch);
        let record = snapshot
            .records
            .iter()
            .find(|record| record.path.as_str() == "authority.txt")
            .cloned()
            .unwrap();
        let manifest = session
            .request_manifest(&snapshot.token, &record)
            .await
            .unwrap();
        let hashes: Vec<_> = manifest
            .manifest
            .chunks
            .iter()
            .map(|chunk| chunk.hash)
            .collect();
        let grant = session
            .request_swarm_grant(owner.endpoint_id(), &snapshot.token, &manifest, &hashes)
            .await
            .unwrap();
        assert_eq!(grant.provider, owner.endpoint_id());
        assert_eq!(grant.consumer, member.endpoint_id());
        assert_eq!(grant.provider_epoch, 0);
        let permit = session.revalidate(&snapshot.token).await.unwrap();
        let operation_id = [0x71; 16];
        session.apply_start(&permit, operation_id).await.unwrap();
        session
            .apply_drained(&permit, operation_id, true)
            .await
            .unwrap();
        assert!(matches!(
            owner_share
                .revoke_member_strong(member.endpoint_id())
                .await
                .unwrap(),
            super::super::RevocationReceipt::Complete { .. }
        ));

        session.close().await;
        drop(owner_share);
        member.shutdown().await.unwrap();
        owner.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn read_only_provider_grant_activation_requires_bilateral_drain() {
        let name =
            "share::service::tests::read_only_provider_grant_activation_requires_bilateral_drain";
        if std::env::var("DW_RO_PROVIDER_CHILD").ok().as_deref() != Some(name) {
            let profile = tempfile::tempdir().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", name, "--nocapture"])
                .env("DW_RO_PROVIDER_CHILD", name)
                .env("HOME", profile.path())
                .env("USERPROFILE", profile.path())
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }

        let temp = tempfile::tempdir().unwrap();
        let owner = ShareService::open(
            temp.path().join("owner-service"),
            NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let consumer = ShareService::open(
            temp.path().join("consumer-service"),
            NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let provider = ShareService::open(
            temp.path().join("provider-service"),
            NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let root = temp.path().join("shared-root");
        let state = temp.path().join("shared-state");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("provider.txt"), b"provider payload").unwrap();
        let owner_share = owner
            .create_owned_share("Provider".into(), root, state, None, 0)
            .await
            .unwrap();
        let share = owner_share.config().share_id;
        let consumer_ticket = owner_share
            .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
            .unwrap();
        let provider_ticket = owner_share
            .issue_key(Permission::ReadOnly, None, owner.endpoint_addr())
            .unwrap();
        let consumer_membership = consumer.enroll(&consumer_ticket, None).await.unwrap();
        let provider_membership = provider.enroll(&provider_ticket, None).await.unwrap();
        assert_eq!(provider_membership.permission, Permission::ReadOnly);

        let consumer_session = consumer.open_session(owner.endpoint_id(), share).unwrap();
        let provider_session = provider.open_session(owner.endpoint_id(), share).unwrap();
        let _ = provider_session.refresh_roster().await.unwrap();
        let provider_challenge = provider_session.roster_challenge().unwrap();
        provider_session
            .heartbeat(provider_challenge)
            .await
            .unwrap();

        let empty = MerkleTree::from_records(Vec::new()).unwrap();
        let snapshot = consumer_session
            .fetch_authoritative_snapshot(&empty)
            .await
            .unwrap();
        assert_eq!(snapshot.token.epoch, consumer_membership.epoch);
        let record = snapshot
            .records
            .iter()
            .find(|record| record.path.as_str() == "provider.txt")
            .cloned()
            .unwrap();
        let manifest = consumer_session
            .request_manifest(&snapshot.token, &record)
            .await
            .unwrap();
        let hashes: Vec<_> = manifest
            .manifest
            .chunks
            .iter()
            .map(|chunk| chunk.hash)
            .collect();
        let grant = consumer_session
            .request_swarm_grant(provider.endpoint_id(), &snapshot.token, &manifest, &hashes)
            .await
            .unwrap();
        assert_eq!(grant.provider_epoch, provider_membership.epoch);
        let activation = provider_session.activate_grant(&grant).await.unwrap();

        let first_revoke = owner_share
            .revoke_member_strong(provider.endpoint_id())
            .await
            .unwrap();
        assert!(matches!(
            first_revoke,
            super::super::RevocationReceipt::Pending { blockers: 1, .. }
        ));
        // Drain acknowledgements remain admissible after the owner closes new
        // runtime admission during revoke/pause.
        owner_share.pause().await;

        consumer_session
            .grant_drained(&grant, activation.reply.activation_id)
            .await
            .unwrap();
        assert!(matches!(
            owner
                .registry
                .revocation_receipt(share, provider.endpoint_id())
                .unwrap(),
            super::super::RevocationReceipt::Pending { blockers: 1, .. }
        ));
        provider_session
            .grant_drained(&grant, activation.reply.activation_id)
            .await
            .unwrap();
        assert!(matches!(
            owner_share
                .revoke_member_strong(provider.endpoint_id())
                .await
                .unwrap(),
            super::super::RevocationReceipt::Complete { .. }
        ));

        consumer_session.close().await;
        provider_session.close().await;
        drop(owner_share);
        provider.shutdown().await.unwrap();
        consumer.shutdown().await.unwrap();
        owner.shutdown().await.unwrap();
    }

    #[test]
    fn denied_session_does_not_join_revocation_drain_after_connections_close() {
        if std::env::var_os("DW_DENIED_DRAIN_CHILD").is_none() {
            let home = tempfile::tempdir().unwrap();
            let result=std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact","share::service::tests::denied_session_does_not_join_revocation_drain_after_connections_close","--nocapture"])
                .env("DW_DENIED_DRAIN_CHILD","1").env("HOME",home.path()).env("USERPROFILE",home.path()).status().unwrap();
            assert!(result.success());
            return;
        }
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let temp = tempfile::tempdir().unwrap();
            let owner =
                ShareService::open(temp.path().join("owner"), NetworkMode::DirectOnly, None)
                    .await
                    .unwrap();
            let share = owner
                .create_owned_share(
                    "Files".into(),
                    temp.path().join("root"),
                    temp.path().join("state"),
                    None,
                    0,
                )
                .await
                .unwrap();
            let ticket = share
                .issue_key(
                    super::super::Permission::ReadOnly,
                    None,
                    owner.endpoint_addr(),
                )
                .unwrap();
            let peer = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
                .bind()
                .await
                .unwrap();
            owner.registry.enroll(&ticket, peer.id(), None).unwrap();
            let gate = share.runtime.gate.lock().await;
            let revoking = share.clone();
            let id = peer.id();
            let mut revoke = tokio::spawn(async move { revoking.revoke_member(id).await });
            while share.members().unwrap()[0].revoked_at.is_none() {
                tokio::task::yield_now().await;
            }
            let connection = peer.connect(owner.endpoint_addr(), ALPN_V3).await.unwrap();
            let result = wire::exchange(
                &connection,
                Hello {
                    version: 3,
                    share_id: share.config().share_id,
                    operation: Operation::Session,
                },
            )
            .await;
            assert_eq!(
                result.err().unwrap().downcast_ref::<ShareError>(),
                Some(&ShareError::MemberRevoked)
            );
            drop(gate);
            let completed =
                tokio::time::timeout(std::time::Duration::from_secs(2), &mut revoke).await;
            connection.close(0u8.into(), b"test complete");
            let finished = completed.is_ok();
            if let Ok(result) = completed {
                result.unwrap().unwrap();
            } else {
                revoke.await.unwrap().unwrap();
            }
            peer.close().await;
            drop(share);
            owner.shutdown().await.unwrap();
            assert!(
                finished,
                "an already-denied idle connection kept revocation waiting"
            );
        });
    }
}

#[cfg(test)]
mod capacity_tests {
    use super::super::{Permission, registry::MAX_REPLICAS};
    use super::*;

    fn catalog_snapshot(registry: &Registry) -> Vec<u8> {
        // Read every persisted share field through redb transactions. A second
        // file handle cannot read a live redb file under Windows byte-range locks.
        let shares: Vec<_> = registry
            .configs()
            .unwrap()
            .into_iter()
            .map(|config| {
                let id = config.share_id;
                (
                    registry.is_ready(id).unwrap(),
                    config,
                    registry.invitations(id).unwrap(),
                    registry.members(id).unwrap(),
                    registry.known(id).unwrap(),
                )
            })
            .collect();
        postcard::to_stdvec(&(shares, registry.relationships().unwrap())).unwrap()
    }

    #[test]
    fn proof_at_replica_capacity_is_atomic_and_existing_writer_survives_restart() {
        let name = "share::service::capacity_tests::proof_at_replica_capacity_is_atomic_and_existing_writer_survives_restart";
        if std::env::var("DW_CAPACITY_CHILD").ok().as_deref() != Some(name) {
            let profile = tempfile::tempdir().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", name, "--nocapture"])
                .env("DW_CAPACITY_CHILD", name)
                .env("HOME", profile.path())
                .env("USERPROFILE", profile.path())
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let temp = tempfile::tempdir().unwrap();
            let device = temp.path().join("device");
            let owner = ShareService::open(&device, NetworkMode::DirectOnly, None)
                .await
                .unwrap();
            let member =
                ShareService::open(temp.path().join("member"), NetworkMode::DirectOnly, None)
                    .await
                    .unwrap();
            let root = temp.path().join("root");
            let share = owner
                .create_owned_share(
                    "Files".into(),
                    root.clone(),
                    temp.path().join("state"),
                    None,
                    0,
                )
                .await
                .unwrap();
            std::fs::write(root.join("file"), b"original").unwrap();
            let ticket = share
                .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
                .unwrap();
            let writer =
                ShareService::open(temp.path().join("writer"), NetworkMode::DirectOnly, None)
                    .await
                    .unwrap();
            let writer_grant = writer.enroll(&ticket, None).await.unwrap();
            let known_key = SecretKey::generate();
            let known_id = ReplicaId(Hash32::digest(known_key.public().as_bytes()));
            let mut known = owner.registry.known(share.config().share_id).unwrap();
            known.insert(known_id);
            for n in 0u64.. {
                if known.len() == MAX_REPLICAS {
                    break;
                }
                known.insert(ReplicaId(Hash32::digest(&n.to_le_bytes())));
            }
            owner
                .registry
                .remember_replicas(share.config().share_id, known.clone())
                .unwrap();
            let unknown_key = SecretKey::generate();
            let unknown_id = ReplicaId(Hash32::digest(unknown_key.public().as_bytes()));
            assert!(!known.contains(&unknown_id));
            let proof =
                LegacyProof::create(&ticket, &unknown_key, member.endpoint_id(), unknown_id)
                    .unwrap();
            let catalog_before = catalog_snapshot(&owner.registry);
            assert!(
                member.enroll(&ticket, Some(proof)).await.is_err(),
                "unknown retained replica exceeded capacity"
            );
            assert!(
                catalog_snapshot(&owner.registry) == catalog_before,
                "denied enrollment wrote catalog"
            );
            assert_eq!(
                owner.registry.known(share.config().share_id).unwrap(),
                known
            );
            assert_eq!(share.members().unwrap(), vec![writer_grant.clone()]);
            let config = share.config().clone();
            drop(share);
            owner.shutdown().await.unwrap();
            let owner = ShareService::open(&device, NetworkMode::DirectOnly, None)
                .await
                .unwrap();
            let share = owner.load_owned_share(config.share_id).await.unwrap();
            assert!(
                catalog_snapshot(&owner.registry) == catalog_before,
                "denied enrollment changed the persisted catalog after restart"
            );
            assert_eq!(owner.registry.known(config.share_id).unwrap(), known);
            assert_eq!(share.members().unwrap(), vec![writer_grant.clone()]);
            let ticket = share
                .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
                .unwrap();
            let proof =
                LegacyProof::create(&ticket, &known_key, member.endpoint_id(), known_id).unwrap();
            let grant = member.enroll(&ticket, Some(proof)).await.unwrap();
            assert_eq!(grant.replica, known_id);
            assert_eq!(owner.registry.known(config.share_id).unwrap(), known);
            assert_eq!(writer.enroll(&ticket, None).await.unwrap(), writer_grant);
            let incumbent = writer
                .open_session(writer_grant.owner, writer_grant.share_id)
                .unwrap();
            let empty = MerkleTree::from_records(Vec::new()).unwrap();
            let mut incumbent_record = incumbent
                .fetch_snapshot(&empty)
                .await
                .unwrap()
                .records
                .remove(0);
            incumbent_record.path = deltaweave_core::WirePath::new("incumbent-created").unwrap();
            incumbent_record.kind = deltaweave_core::SyncEntryKind::Directory;
            incumbent_record.content_hash = None;
            incumbent_record.size = 0;
            incumbent_record
                .version
                .increment(writer_grant.replica)
                .unwrap();
            incumbent.apply_metadata(incumbent_record).await.unwrap();
            assert!(root.join("incumbent-created").is_dir());
            drop(incumbent);
            let session = member.open_session(grant.owner, grant.share_id).unwrap();
            let empty = MerkleTree::from_records(Vec::new()).unwrap();
            let mut record = session
                .fetch_snapshot(&empty)
                .await
                .unwrap()
                .records
                .remove(0);
            record.path = deltaweave_core::WirePath::new("created").unwrap();
            record.kind = deltaweave_core::SyncEntryKind::Directory;
            record.content_hash = None;
            record.size = 0;
            record.version.increment(grant.replica).unwrap();
            session.apply_metadata(record.clone()).await.unwrap();
            assert_eq!(
                session
                    .fetch_snapshot(&empty)
                    .await
                    .unwrap()
                    .records
                    .into_iter()
                    .find(|item| item.path == record.path)
                    .unwrap(),
                record
            );
            assert_eq!(std::fs::read(root.join("file")).unwrap(), b"original");
            drop(session);
            drop(share);
            writer.shutdown().await.unwrap();
            member.shutdown().await.unwrap();
            owner.shutdown().await.unwrap();
        });
    }
}
