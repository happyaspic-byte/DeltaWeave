use super::registry::{MAX_REPLICAS, Registry, resolver};
use super::{
    Invitation, InvitationId, Membership, OwnedShareConfig, Permission, ShareError, ShareId,
    ShareTicket, now,
};
use crate::{TransferEvent, TransferObserver, root_admission::RootLease};
use anyhow::{Result, ensure};
use deltaweave_core::{Hash32, ReplicaId, SyncRecord, WirePath};
use deltaweave_index::LocalIndex;
use deltaweave_store::Store;
use iroh::{EndpointAddr, EndpointId, SecretKey, endpoint::Connection};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::sync::Notify;

/// The authenticated immediate peer, never an inferred version-vector author.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MutationProvenance {
    pub peer: EndpointId,
    pub membership_epoch: u64,
    pub path: WirePath,
    pub operation: String,
    pub record_hash: Hash32,
    pub accepted_at: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct CausalState {
    version: u8,
    share: ShareId,
    owner_replica: ReplicaId,
    ceilings: BTreeMap<ReplicaId, u64>,
    audit: Vec<MutationProvenance>,
}

pub(crate) struct OwnedRuntime {
    pub config: OwnedShareConfig,
    pub registry: Arc<Registry>,
    pub index: Arc<LocalIndex>,
    pub store: Arc<Store>,
    pub lease: Arc<RootLease>,
    pub gate: Arc<tokio::sync::Mutex<()>>,
    pub receive_gate: Arc<tokio::sync::Mutex<()>>,
    pub enabled: AtomicBool,
    pub observer: Mutex<Option<TransferObserver>>,
    connections: Mutex<BTreeMap<u64, (EndpointId, Connection)>>,
    next_connection: AtomicU64,
    changed: Notify,
}
impl std::fmt::Debug for OwnedRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedRuntime")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
impl OwnedRuntime {
    pub fn new(
        config: OwnedShareConfig,
        registry: Arc<Registry>,
        index: Arc<LocalIndex>,
        store: Arc<Store>,
        lease: Arc<RootLease>,
    ) -> Result<Self> {
        let runtime = Self {
            config,
            registry,
            index,
            store,
            lease,
            gate: Arc::new(tokio::sync::Mutex::new(())),
            receive_gate: Arc::new(tokio::sync::Mutex::new(())),
            enabled: AtomicBool::new(true),
            observer: Mutex::new(None),
            connections: Mutex::new(BTreeMap::new()),
            next_connection: AtomicU64::new(1),
            changed: Notify::new(),
        };
        runtime.refresh_causal_state()?;
        Ok(runtime)
    }
    fn causal_state(&self) -> Result<CausalState> {
        let state = match self.index.share_metadata()? {
            Some(bytes) => postcard::from_bytes(&bytes)?,
            None => CausalState {
                version: 3,
                share: self.config.share_id,
                owner_replica: self.config.replica,
                ceilings: BTreeMap::new(),
                audit: Vec::new(),
            },
        };
        ensure!(
            state.version == 3
                && state.share == self.config.share_id
                && state.owner_replica == self.config.replica
                && state.ceilings.len() <= MAX_REPLICAS
                && state.audit.len() <= 512,
            ShareError::StateUnavailable
        );
        Ok(state)
    }
    /// Caller holds the mutation gate, or runtime has not been published yet.
    pub fn refresh_causal_state(&self) -> Result<()> {
        let mut state = self.causal_state()?;
        state.ceilings.entry(self.config.replica).or_insert(0);
        state.ceilings.entry(resolver()).or_insert(0);
        for record in self.index.sync_records()? {
            ensure!(
                record.version.iter().count() <= MAX_REPLICAS,
                ShareError::InvalidRecord
            );
            for (replica, counter) in record.version.iter() {
                let ceiling = state.ceilings.entry(replica).or_insert(0);
                *ceiling = (*ceiling).max(counter);
            }
        }
        ensure!(
            state.ceilings.len() <= MAX_REPLICAS,
            ShareError::InvalidRecord
        );
        self.registry
            .remember_replicas(self.config.share_id, state.ceilings.keys().copied())?;
        self.index
            .set_share_metadata(&postcard::to_stdvec(&state)?)?;
        Ok(())
    }
    pub fn track(self: &Arc<Self>, connection: Connection) -> TrackedConnection {
        let id = self.next_connection.fetch_add(1, Ordering::Relaxed);
        self.connections
            .lock()
            .expect("connection mutex")
            .insert(id, (connection.remote_id(), connection));
        TrackedConnection {
            runtime: self.clone(),
            id,
        }
    }
    fn close_connections(&self, peer: Option<EndpointId>) {
        for (endpoint, connection) in self.connections.lock().expect("connection mutex").values() {
            if peer.is_none_or(|peer| *endpoint == peer) {
                connection.close(0u8.into(), b"share authorization changed");
            }
        }
    }
    async fn drain_connections(&self, peer: Option<EndpointId>) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self
                .connections
                .lock()
                .expect("connection mutex")
                .values()
                .any(|(endpoint, _)| peer.is_none_or(|peer| peer == *endpoint))
            {
                return;
            }
            notified.await;
        }
    }
    pub async fn pause(&self) {
        self.enabled.store(false, Ordering::SeqCst);
        self.close_connections(None);
        {
            let _gate = self.gate.lock().await;
        }
        self.drain_connections(None).await;
    }
}
pub(crate) struct TrackedConnection {
    runtime: Arc<OwnedRuntime>,
    id: u64,
}
impl Drop for TrackedConnection {
    fn drop(&mut self) {
        self.runtime
            .connections
            .lock()
            .expect("connection mutex")
            .remove(&self.id);
        self.runtime.changed.notify_waiters();
    }
}

/// Local owner capability. It is never serialized or offered over v3.
#[derive(Clone, Debug)]
pub struct OwnerShare {
    pub(crate) runtime: Arc<OwnedRuntime>,
    pub(crate) key: SecretKey,
}
impl OwnerShare {
    pub fn config(&self) -> &OwnedShareConfig {
        &self.runtime.config
    }
    /// Keys are display-once: the issuance database retains only their digest.
    pub fn issue_key(
        &self,
        permission: Permission,
        expires_at: Option<u64>,
        address: EndpointAddr,
    ) -> Result<ShareTicket> {
        ensure!(
            self.runtime.enabled.load(Ordering::SeqCst),
            ShareError::Busy
        );
        let ticket = ShareTicket::issue(
            &self.key,
            self.config().share_id,
            self.config().name.clone(),
            permission,
            expires_at,
            address,
            now(),
        )?;
        self.runtime.registry.issue(&ticket)?;
        Ok(ticket)
    }
    pub fn keys(&self) -> Result<Vec<Invitation>> {
        self.runtime.registry.invitations(self.config().share_id)
    }
    pub fn revoke_key(&self, id: InvitationId) -> Result<()> {
        self.runtime.registry.revoke_key(self.config().share_id, id)
    }
    pub fn rotate_key(
        &self,
        id: InvitationId,
        expires_at: Option<u64>,
        address: EndpointAddr,
    ) -> Result<ShareTicket> {
        let old = self
            .keys()?
            .into_iter()
            .find(|key| key.id == id)
            .ok_or(ShareError::InvalidTicket)?;
        self.revoke_key(id)?;
        self.issue_key(old.permission, expires_at, address)
    }
    pub fn members(&self) -> Result<Vec<Membership>> {
        self.runtime.registry.members(self.config().share_id)
    }
    /// Persist denial first, close QUIC, then drain the gate and every retained handler.
    /// Lock order: gate -> short registry transaction; revocation releases registry
    /// before waiting for gate. Blocking work is always awaited by retained handlers.
    pub async fn revoke_member(&self, peer: EndpointId) -> Result<()> {
        self.runtime
            .registry
            .revoke_member(self.config().share_id, peer)?;
        self.runtime.close_connections(Some(peer));
        {
            let _gate = self.runtime.gate.lock().await;
        }
        self.runtime.drain_connections(Some(peer)).await;
        Ok(())
    }
    pub async fn pause(&self) {
        self.runtime.pause().await;
    }
    pub fn resume(&self) {
        self.runtime.enabled.store(true, Ordering::SeqCst);
    }
    pub fn set_observer(&self, observer: Option<TransferObserver>) {
        *self.runtime.observer.lock().expect("observer mutex") = observer;
    }
    pub fn inventory(&self) -> Result<crate::Inventory> {
        crate::Inventory::from_index(&self.runtime.index)
    }
    pub fn provenance(&self) -> Result<Vec<MutationProvenance>> {
        Ok(self.runtime.causal_state()?.audit)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Authorization {
    pub runtime: Arc<OwnedRuntime>,
    pub peer: EndpointId,
    pub epoch: u64,
}
impl Authorization {
    pub fn check(&self, write: bool) -> Result<Membership> {
        ensure!(
            self.runtime.enabled.load(Ordering::SeqCst),
            ShareError::Busy
        );
        let member =
            self.runtime
                .registry
                .authorize(self.runtime.config.share_id, self.peer, write)?;
        ensure!(member.epoch == self.epoch, ShareError::MemberRevoked);
        Ok(member)
    }
    pub fn phase(&self, phase: &str, path: Option<&WirePath>) {
        let observer = self
            .runtime
            .observer
            .lock()
            .expect("observer mutex")
            .clone();
        if let Some(observer) = observer {
            observer.emit(TransferEvent {
                phase: phase.into(),
                path: path.map(|p| p.as_str().into()),
                direction: None,
                bytes: 0,
                peer: Some(self.peer.to_string()),
            });
        }
    }
    /// Runs under the share gate after the existing authoritative pre-apply rescan.
    pub fn candidate_metadata(&self, record: &SyncRecord) -> Result<Vec<u8>> {
        let member = self.check(true)?;
        self.runtime.refresh_causal_state()?;
        let mut state = self.runtime.causal_state()?;
        let known = self.runtime.registry.known(record_share(self))?;
        ensure!(
            record.version.iter().count() <= MAX_REPLICAS
                && postcard::to_stdvec(&record.version)?.len() <= 256 * 1024,
            ShareError::InvalidRecord
        );
        for (replica, counter) in record.version.iter() {
            ensure!(known.contains(&replica), ShareError::InvalidRecord);
            let ceiling = state.ceilings.get(&replica).copied().unwrap_or(0);
            if replica == resolver() {
                ensure!(
                    counter <= ceiling.checked_add(1).ok_or(ShareError::InvalidRecord)?,
                    ShareError::InvalidRecord
                );
            } else if replica != member.replica {
                ensure!(counter <= ceiling, ShareError::InvalidRecord);
            }
        }
        for (replica, counter) in record.version.iter() {
            let ceiling = state.ceilings.entry(replica).or_insert(0);
            *ceiling = (*ceiling).max(counter);
        }
        state.audit.push(MutationProvenance {
            peer: self.peer,
            membership_epoch: member.epoch,
            path: record.path.clone(),
            operation: if record.tombstone { "delete" } else { "adopt" }.into(),
            record_hash: record.logical_hash(),
            accepted_at: now(),
        });
        if state.audit.len() > 512 {
            state.audit.remove(0);
        }
        let metadata = postcard::to_stdvec(&state)?;
        ensure!(metadata.len() <= 2 * 1024 * 1024, ShareError::InvalidRecord);
        Ok(metadata)
    }
}
fn record_share(auth: &Authorization) -> ShareId {
    auth.runtime.config.share_id
}
