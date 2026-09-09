use super::{
    ALPN_V3, GrantNonce, LegacyProof, MemberRelationship, Membership, OwnedShareConfig,
    RosterHeartbeat, ShareError, ShareId, ShareTicket, SignedRoster, TicketPreview,
};
use super::authority::{
    ActivateGrantReply, ActivateGrantRequest, ApplyDrained, ApplyPermit, ApplyStart,
    AuthoritativeSnapshot, ManifestAttestation, ShareGrant, SnapshotToken, request_hash,
};
use super::roster::random_nonce;
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
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock, atomic::Ordering},
    time::Duration,
};

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
        let endpoint = bind_endpoint(key.clone(), mode, Some(vec![ALPN_V3.to_vec()]), bind).await?;
        let runtimes = Arc::new(RwLock::new(BTreeMap::new()));
        let active = Arc::new(tokio::sync::RwLock::new(()));
        let handler = Handler {
            registry: registry.clone(),
            key: key.clone(),
            runtimes: runtimes.clone(),
            limit: Arc::new(tokio::sync::Semaphore::new(64)),
            active: active.clone(),
        };
        let router = Router::builder(endpoint).accept(ALPN_V3, handler).spawn();
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
    pub fn endpoint_addr(&self) -> EndpointAddr {
        endpoint_addr_with_local_fallback(self.router.endpoint())
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
        let result = wire::exchange(
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
        let result = wire::exchange(
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
        let connection = self
            .router
            .endpoint()
            .connect(address.clone(), ALPN_V3)
            .await
            .map_err(|_| ShareError::Offline)?;
        let result = wire::exchange(
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
        Ok(self
            .router
            .endpoint()
            .connect(ticket.address(), ALPN_V3)
            .await
            .map_err(|_| ShareError::Offline)?)
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
        let connection = self
            .inner
            .endpoint
            .connect(self.inner.client.remote.clone(), ALPN_V3)
            .await
            .map_err(|_| ShareError::Offline)?;
        let result = wire::exchange(
            &connection,
            Hello {
                version: 3,
                share_id: self.membership.share_id,
                operation,
            },
        )
        .await;
        connection.close(0u8.into(), b"share control complete");
        result
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
        let result = (|| -> Result<Reply> {
            ensure!(runtime.enabled.load(Ordering::SeqCst), ShareError::Busy);
            match hello.operation {
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
                    let (roster, challenge) =
                        self.registry
                            .issue_roster_challenge(&self.key, hello.share_id, peer)?;
                    Ok(Reply::Roster { roster, challenge })
                }
                Operation::Heartbeat(heartbeat) => {
                    ensure!(heartbeat.share == hello.share_id, ShareError::OwnerMismatch);
                    Ok(Reply::Heartbeat(
                        self.registry
                            .accept_roster_heartbeat(&self.key, &heartbeat, peer)?,
                    ))
                }
            }
        })();
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
