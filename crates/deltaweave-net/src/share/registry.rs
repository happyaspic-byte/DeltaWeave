use super::ticket::random_bytes;
use super::{InvitationId, LegacyProof, Permission, ShareError, ShareId, ShareTicket, now};
use anyhow::{Result, ensure};
use deltaweave_core::{Hash32, ReplicaId};
use iroh::{EndpointAddr, EndpointId};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Mutex,
};

const CATALOG: TableDefinition<u8, &[u8]> = TableDefinition::new("owner_share_catalog_v3");
const MAX_MEMBERS: usize = 4096;
pub(crate) const MAX_REPLICAS: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OwnedShareConfig {
    pub share_id: ShareId,
    pub owner: EndpointId,
    pub name: String,
    pub root: PathBuf,
    pub state_root: PathBuf,
    /// Existing DB-bound replica on import; independent of the managed transport.
    pub replica: ReplicaId,
    pub min_free_space_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Membership {
    pub share_id: ShareId,
    pub owner: EndpointId,
    pub endpoint: EndpointId,
    pub permission: Permission,
    pub replica: ReplicaId,
    pub enrolled_at: u64,
    pub revoked_at: Option<u64>,
    pub epoch: u64,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Invitation {
    pub id: InvitationId,
    pub share_id: ShareId,
    pub permission: Permission,
    pub expires_at: Option<u64>,
    pub revoked_at: Option<u64>,
    /// Hash of the entire canonical signed issuance, including a distinct random bearer.
    digest: Hash32,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemberRelationship {
    pub membership: Membership,
    pub address: EndpointAddr,
}
#[derive(Serialize, Deserialize)]
struct ShareEntry {
    ready: bool,
    config: OwnedShareConfig,
    invitations: BTreeMap<InvitationId, Invitation>,
    members: BTreeMap<EndpointId, Membership>,
    known: BTreeSet<ReplicaId>,
}
#[derive(Serialize, Deserialize)]
struct Catalog {
    version: u8,
    owner: EndpointId,
    shares: BTreeMap<ShareId, ShareEntry>,
    relationships: BTreeMap<(EndpointId, ShareId), MemberRelationship>,
}
#[derive(Debug)]
pub(crate) struct Registry {
    db: Database,
    serial: Mutex<()>,
}

pub(crate) fn resolver() -> ReplicaId {
    ReplicaId(Hash32::digest(
        b"deltaweave deterministic conflict resolver v1",
    ))
}

impl Registry {
    pub fn open(path: &Path, owner: EndpointId) -> Result<Self> {
        crate::root_admission::private_directory(path)?;
        let db = Database::create(path.join("shares.redb"))?;
        let tx = db.begin_write()?;
        {
            let mut table = tx.open_table(CATALOG)?;
            if table.get(0)?.is_none() {
                let bytes = postcard::to_stdvec(&Catalog {
                    version: 3,
                    owner,
                    shares: BTreeMap::new(),
                    relationships: BTreeMap::new(),
                })?;
                table.insert(0, bytes.as_slice())?;
            }
        }
        tx.commit()?;
        let registry = Self {
            db,
            serial: Mutex::new(()),
        };
        let catalog = registry.read()?;
        ensure!(
            catalog.version == 3 && catalog.owner == owner,
            ShareError::StateUnavailable
        );
        Ok(registry)
    }
    fn read(&self) -> Result<Catalog> {
        let read = self.db.begin_read()?;
        let table = read.open_table(CATALOG)?;
        let bytes = table.get(0)?.ok_or(ShareError::StateUnavailable)?;
        ensure!(
            bytes.value().len() <= 16 * 1024 * 1024,
            ShareError::StateUnavailable
        );
        Ok(postcard::from_bytes(bytes.value())?)
    }
    fn update<T>(&self, f: impl FnOnce(&mut Catalog) -> Result<T>) -> Result<T> {
        let _serial = self
            .serial
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        let mut catalog = self.read()?;
        let result = f(&mut catalog)?;
        let bytes = postcard::to_stdvec(&catalog)?;
        ensure!(
            bytes.len() <= 16 * 1024 * 1024,
            ShareError::StateUnavailable
        );
        let tx = self.db.begin_write()?;
        tx.open_table(CATALOG)?.insert(0, bytes.as_slice())?;
        tx.commit()?;
        Ok(result)
    }
    pub fn insert_share(
        &self,
        config: OwnedShareConfig,
        mut known: BTreeSet<ReplicaId>,
    ) -> Result<()> {
        self.update(|catalog| {
            ensure!(
                config.owner == catalog.owner && config.replica != resolver(),
                ShareError::ReplicaClaimRejected
            );
            ensure!(
                catalog.shares.len() < 256 && !catalog.shares.contains_key(&config.share_id),
                ShareError::Busy
            );
            known.insert(config.replica);
            known.insert(resolver());
            catalog.shares.insert(
                config.share_id,
                ShareEntry {
                    ready: false,
                    config,
                    invitations: BTreeMap::new(),
                    members: BTreeMap::new(),
                    known,
                },
            );
            Ok(())
        })
    }
    pub fn is_ready(&self, share: ShareId) -> Result<bool> {
        Ok(self
            .read()?
            .shares
            .get(&share)
            .ok_or(ShareError::UnknownShare)?
            .ready)
    }
    pub fn mark_ready(&self, share: ShareId) -> Result<()> {
        self.update(|catalog| {
            catalog
                .shares
                .get_mut(&share)
                .ok_or(ShareError::UnknownShare)?
                .ready = true;
            Ok(())
        })
    }
    pub fn configs(&self) -> Result<Vec<OwnedShareConfig>> {
        Ok(self
            .read()?
            .shares
            .into_values()
            .map(|entry| entry.config)
            .collect())
    }
    pub fn config(&self, id: ShareId) -> Result<OwnedShareConfig> {
        Ok(self
            .read()?
            .shares
            .get(&id)
            .ok_or(ShareError::UnknownShare)?
            .config
            .clone())
    }
    pub fn remove_share(&self, id: ShareId) -> Result<()> {
        self.update(|catalog| {
            ensure!(
                catalog.shares.remove(&id).is_some(),
                ShareError::UnknownShare
            );
            catalog.relationships.retain(|(_, share), _| *share != id);
            Ok(())
        })
    }
    pub fn remember_replicas(
        &self,
        id: ShareId,
        known: impl IntoIterator<Item = ReplicaId>,
    ) -> Result<()> {
        self.update(|catalog| {
            let entry = catalog
                .shares
                .get_mut(&id)
                .ok_or(ShareError::UnknownShare)?;
            entry.known.extend(known);
            ensure!(entry.known.len() <= MAX_REPLICAS, ShareError::InvalidRecord);
            Ok(())
        })
    }
    pub fn known(&self, id: ShareId) -> Result<BTreeSet<ReplicaId>> {
        Ok(self
            .read()?
            .shares
            .get(&id)
            .ok_or(ShareError::UnknownShare)?
            .known
            .clone())
    }
    pub fn issue(&self, ticket: &ShareTicket) -> Result<()> {
        ticket.verify_at(now())?;
        self.update(|catalog| {
            let entry = catalog
                .shares
                .get_mut(&ticket.body.share_id)
                .ok_or(ShareError::UnknownShare)?;
            ensure!(
                entry.config.owner == ticket.body.owner && entry.config.name == ticket.body.name,
                ShareError::InvalidTicket
            );
            let digest = ticket.issuance_digest()?;
            if let Some(existing) = entry.invitations.get(&ticket.body.invitation_id) {
                // A recovered issuance intent may replay the same signed
                // ticket.  It must be an exact replay and can never resurrect
                // a revoked invitation or overwrite its durable role/expiry.
                ensure!(
                    existing.share_id == ticket.body.share_id
                        && existing.permission == ticket.body.permission
                        && existing.expires_at == ticket.body.expires_at
                        && existing.digest == digest
                        && existing.revoked_at.is_none(),
                    ShareError::InvalidTicket
                );
                return Ok(());
            }
            ensure!(entry.invitations.len() < 4096, ShareError::Busy);
            entry.invitations.insert(
                ticket.body.invitation_id,
                Invitation {
                    id: ticket.body.invitation_id,
                    share_id: ticket.body.share_id,
                    permission: ticket.body.permission,
                    expires_at: ticket.body.expires_at,
                    revoked_at: None,
                    digest,
                },
            );
            Ok(())
        })
    }
    fn validate_entry(entry: &ShareEntry, ticket: &ShareTicket) -> Result<()> {
        ticket.verify_at(now())?;
        let issuance = entry
            .invitations
            .get(&ticket.body.invitation_id)
            .ok_or(ShareError::InvalidTicket)?;
        ensure!(issuance.revoked_at.is_none(), ShareError::InvitationRevoked);
        use subtle::ConstantTimeEq;
        ensure!(
            bool::from(
                issuance
                    .digest
                    .as_bytes()
                    .ct_eq(ticket.issuance_digest()?.as_bytes())
            ) && issuance.share_id == ticket.body.share_id
                && issuance.permission == ticket.body.permission
                && issuance.expires_at == ticket.body.expires_at
                && entry.config.owner == ticket.body.owner,
            ShareError::InvalidTicket
        );
        Ok(())
    }
    pub fn validate(&self, ticket: &ShareTicket) -> Result<()> {
        let catalog = self.read()?;
        Self::validate_entry(
            catalog
                .shares
                .get(&ticket.body.share_id)
                .ok_or(ShareError::UnknownShare)?,
            ticket,
        )
    }
    pub fn enroll(
        &self,
        ticket: &ShareTicket,
        peer: EndpointId,
        proof: Option<&LegacyProof>,
    ) -> Result<Membership> {
        self.update(|catalog| {
            ensure!(peer != catalog.owner, ShareError::OwnerMismatch);
            let entry = catalog
                .shares
                .get_mut(&ticket.body.share_id)
                .ok_or(ShareError::UnknownShare)?;
            Self::validate_entry(entry, ticket)?;
            if let Some(member) = entry.members.get(&peer) {
                ensure!(member.revoked_at.is_none(), ShareError::MemberRevoked);
                if let Some(proof) = proof {
                    ensure!(
                        proof.verify(ticket, peer)? == member.replica,
                        ShareError::ReplicaClaimRejected
                    );
                }
                return Ok(member.clone());
            }
            ensure!(entry.members.len() < MAX_MEMBERS, ShareError::Busy);
            let replica = if let Some(proof) = proof {
                let replica = proof.verify(ticket, peer)?;
                ensure!(
                    replica != entry.config.replica
                        && replica != resolver()
                        && !entry
                            .members
                            .values()
                            .any(|member| member.replica == replica),
                    ShareError::ReplicaClaimRejected
                );
                replica
            } else {
                ensure!(entry.known.len() < MAX_REPLICAS, ShareError::Busy);
                loop {
                    let replica = ReplicaId(Hash32::from_bytes(random_bytes()));
                    if !entry.known.contains(&replica) {
                        break replica;
                    }
                }
            };
            ensure!(
                entry.known.contains(&replica) || entry.known.len() < MAX_REPLICAS,
                ShareError::Busy
            );
            entry.known.insert(replica);
            let member = Membership {
                share_id: entry.config.share_id,
                owner: catalog.owner,
                endpoint: peer,
                permission: ticket.body.permission,
                replica,
                enrolled_at: now(),
                revoked_at: None,
                epoch: 1,
            };
            entry.members.insert(peer, member.clone());
            Ok(member)
        })
    }
    pub fn authorize(&self, share: ShareId, peer: EndpointId, write: bool) -> Result<Membership> {
        let catalog = self.read()?;
        let member = catalog
            .shares
            .get(&share)
            .ok_or(ShareError::UnknownShare)?
            .members
            .get(&peer)
            .ok_or(ShareError::NotMember)?;
        ensure!(member.revoked_at.is_none(), ShareError::MemberRevoked);
        ensure!(
            !write || member.permission == Permission::ReadWrite,
            ShareError::PermissionDenied
        );
        Ok(member.clone())
    }
    pub fn members(&self, share: ShareId) -> Result<Vec<Membership>> {
        Ok(self
            .read()?
            .shares
            .get(&share)
            .ok_or(ShareError::UnknownShare)?
            .members
            .values()
            .cloned()
            .collect())
    }
    pub fn invitations(&self, share: ShareId) -> Result<Vec<Invitation>> {
        Ok(self
            .read()?
            .shares
            .get(&share)
            .ok_or(ShareError::UnknownShare)?
            .invitations
            .values()
            .cloned()
            .collect())
    }
    pub fn revoke_key(&self, share: ShareId, invitation: InvitationId) -> Result<()> {
        self.update(|catalog| {
            let entry = catalog
                .shares
                .get_mut(&share)
                .ok_or(ShareError::UnknownShare)?;
            entry
                .invitations
                .get_mut(&invitation)
                .ok_or(ShareError::InvalidTicket)?
                .revoked_at
                .get_or_insert(now());
            Ok(())
        })
    }
    pub fn revoke_member(&self, share: ShareId, peer: EndpointId) -> Result<()> {
        self.update(|catalog| {
            let entry = catalog
                .shares
                .get_mut(&share)
                .ok_or(ShareError::UnknownShare)?;
            let member = entry.members.get_mut(&peer).ok_or(ShareError::NotMember)?;
            if member.revoked_at.is_none() {
                member.revoked_at = Some(now());
                member.epoch = member
                    .epoch
                    .checked_add(1)
                    .ok_or(ShareError::StateUnavailable)?;
            }
            Ok(())
        })
    }
    pub fn store_relationship(&self, relationship: MemberRelationship) -> Result<()> {
        self.update(|catalog| {
            let member = &relationship.membership;
            ensure!(
                member.endpoint == catalog.owner
                    && member.owner == relationship.address.id
                    && member.owner != catalog.owner,
                ShareError::OwnerMismatch
            );
            let key = (member.owner, member.share_id);
            if let Some(existing) = catalog.relationships.get(&key) {
                ensure!(
                    existing.membership.replica == member.replica
                        && existing.membership.permission == member.permission
                        && existing.membership.enrolled_at == member.enrolled_at
                        && existing.membership.endpoint == member.endpoint
                        && existing.membership.owner == member.owner,
                    ShareError::ReplicaClaimRejected
                );
                if existing.membership.revoked_at.is_none() && member.revoked_at.is_some() {
                    ensure!(
                        member.epoch >= existing.membership.epoch,
                        ShareError::ReplicaClaimRejected
                    );
                } else {
                    ensure!(
                        existing.membership.epoch == member.epoch
                            && existing.membership.revoked_at == member.revoked_at,
                        ShareError::ReplicaClaimRejected
                    );
                }
            }
            catalog.relationships.insert(key, relationship);
            Ok(())
        })
    }
    pub fn relationships(&self) -> Result<Vec<MemberRelationship>> {
        Ok(self.read()?.relationships.into_values().collect())
    }
    pub fn relationship(&self, owner: EndpointId, share: ShareId) -> Result<MemberRelationship> {
        self.read()?
            .relationships
            .remove(&(owner, share))
            .ok_or_else(|| ShareError::NotMember.into())
    }
    pub fn forget_relationship(&self, owner: EndpointId, share: ShareId) -> Result<()> {
        self.update(|catalog| {
            catalog.relationships.remove(&(owner, share));
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::SecretKey;
    #[test]
    fn issuance_membership_and_tombstones_are_distinct_durable_authorities() {
        let temp = tempfile::tempdir().unwrap();
        let owner = SecretKey::generate();
        let share = ShareId([1; 32]);
        let registry = Registry::open(&temp.path().join("private"), owner.public()).unwrap();
        let config = OwnedShareConfig {
            share_id: share,
            owner: owner.public(),
            name: "Files".into(),
            root: temp.path().join("root"),
            state_root: temp.path().join("state"),
            replica: ReplicaId(Hash32::digest(b"owner")),
            min_free_space_bytes: 0,
        };
        registry.insert_share(config, BTreeSet::new()).unwrap();
        let ticket = ShareTicket::issue(
            &owner,
            share,
            "Files".into(),
            Permission::ReadOnly,
            None,
            EndpointAddr::new(owner.public()),
            now(),
        )
        .unwrap();
        assert!(registry.validate(&ticket).is_err());
        registry.issue(&ticket).unwrap();
        registry
            .update(|catalog| {
                catalog
                    .shares
                    .get_mut(&share)
                    .unwrap()
                    .invitations
                    .get_mut(&ticket.body.invitation_id)
                    .unwrap()
                    .permission = Permission::ReadWrite;
                Ok(())
            })
            .unwrap();
        assert!(
            registry.validate(&ticket).is_err(),
            "signed ticket role did not exactly match durable issuance"
        );
        registry
            .update(|catalog| {
                catalog
                    .shares
                    .get_mut(&share)
                    .unwrap()
                    .invitations
                    .get_mut(&ticket.body.invitation_id)
                    .unwrap()
                    .permission = Permission::ReadOnly;
                Ok(())
            })
            .unwrap();
        let peer = SecretKey::generate().public();
        let first = registry.enroll(&ticket, peer, None).unwrap();
        assert_eq!(first.permission, Permission::ReadOnly);
        assert_ne!(first.replica, ReplicaId(Hash32::digest(peer.as_bytes())));
        assert_eq!(registry.enroll(&ticket, peer, None).unwrap(), first);
        registry
            .revoke_key(share, ticket.preview().invitation_id)
            .unwrap();
        assert!(registry.validate(&ticket).is_err());
        assert!(
            registry.issue(&ticket).is_err(),
            "replaying a revoked issuance must never resurrect its invitation"
        );
        assert!(registry.authorize(share, peer, false).is_ok());
        assert!(registry.authorize(share, peer, true).is_err());
        registry.revoke_member(share, peer).unwrap();
        drop(registry);
        let registry = Registry::open(&temp.path().join("private"), owner.public()).unwrap();
        assert!(registry.authorize(share, peer, false).is_err());
        let active = ShareTicket::issue(
            &owner,
            share,
            "Files".into(),
            Permission::ReadWrite,
            None,
            EndpointAddr::new(owner.public()),
            now(),
        )
        .unwrap();
        registry.issue(&active).unwrap();
        assert!(registry.enroll(&active, peer, None).is_err());
        assert!(
            registry
                .enroll(&active, SecretKey::generate().public(), None)
                .is_ok()
        );
    }
    #[test]
    fn competing_legacy_claims_have_one_durable_binding_and_owner_is_reserved() {
        use std::sync::{Arc, Barrier};
        let temp = tempfile::tempdir().unwrap();
        let owner = SecretKey::generate();
        let old = SecretKey::generate();
        let share = ShareId([1; 32]);
        let registry =
            Arc::new(Registry::open(&temp.path().join("private"), owner.public()).unwrap());
        let owner_replica = ReplicaId(Hash32::digest(owner.public().as_bytes()));
        registry
            .insert_share(
                OwnedShareConfig {
                    share_id: share,
                    owner: owner.public(),
                    name: "Files".into(),
                    root: temp.path().join("root"),
                    state_root: temp.path().join("state"),
                    replica: owner_replica,
                    min_free_space_bytes: 0,
                },
                BTreeSet::new(),
            )
            .unwrap();
        let ticket = ShareTicket::issue(
            &owner,
            share,
            "Files".into(),
            Permission::ReadWrite,
            None,
            EndpointAddr::new(owner.public()),
            now(),
        )
        .unwrap();
        registry.issue(&ticket).unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let replica = ReplicaId(Hash32::digest(old.public().as_bytes()));
        let mut threads = Vec::new();
        for _ in 0..2 {
            let peer = SecretKey::generate().public();
            let proof = LegacyProof::create(&ticket, &old, peer, replica).unwrap();
            let ticket = ticket.clone();
            let registry = registry.clone();
            let barrier = barrier.clone();
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                registry.enroll(&ticket, peer, Some(&proof))
            }));
        }
        barrier.wait();
        let results: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        let peer = SecretKey::generate().public();
        let owner_claim = LegacyProof::create(&ticket, &owner, peer, owner_replica).unwrap();
        assert!(registry.enroll(&ticket, peer, Some(&owner_claim)).is_err());
        let winner = results.into_iter().find_map(Result::ok).unwrap();
        drop(registry);
        let registry = Registry::open(&temp.path().join("private"), owner.public()).unwrap();
        assert_eq!(
            registry.authorize(share, winner.endpoint, true).unwrap(),
            winner
        );
        let retry_proof = LegacyProof::create(&ticket, &old, winner.endpoint, replica).unwrap();
        assert_eq!(
            registry
                .enroll(&ticket, winner.endpoint, Some(&retry_proof))
                .unwrap(),
            winner
        );
    }

    #[test]
    fn resume_relationship_requires_exact_binding_and_only_monotonic_revoke() {
        let temp = tempfile::tempdir().unwrap();
        let owner = SecretKey::generate();
        let peer = SecretKey::generate().public();
        let share = ShareId([9; 32]);
        let owner_registry =
            Registry::open(&temp.path().join("owner-private"), owner.public()).unwrap();
        owner_registry
            .insert_share(
                OwnedShareConfig {
                    share_id: share,
                    owner: owner.public(),
                    name: "Files".into(),
                    root: temp.path().join("root"),
                    state_root: temp.path().join("state"),
                    replica: ReplicaId(Hash32::digest(b"owner")),
                    min_free_space_bytes: 0,
                },
                BTreeSet::new(),
            )
            .unwrap();
        let ticket = ShareTicket::issue(
            &owner,
            share,
            "Files".into(),
            Permission::ReadWrite,
            None,
            EndpointAddr::new(owner.public()),
            now(),
        )
        .unwrap();
        owner_registry.issue(&ticket).unwrap();
        let membership = owner_registry.enroll(&ticket, peer, None).unwrap();
        let address = EndpointAddr::new(owner.public());
        let registry = Registry::open(&temp.path().join("member-private"), peer).unwrap();
        registry
            .store_relationship(MemberRelationship {
                membership: membership.clone(),
                address: address.clone(),
            })
            .unwrap();

        for mutate in [
            |candidate: &mut Membership| candidate.permission = Permission::ReadOnly,
            |candidate: &mut Membership| candidate.enrolled_at += 1,
            |candidate: &mut Membership| candidate.epoch += 1,
        ] as [fn(&mut Membership); 3]
        {
            let mut changed = membership.clone();
            mutate(&mut changed);
            assert_eq!(
                registry
                    .store_relationship(MemberRelationship {
                        membership: changed,
                        address: address.clone(),
                    })
                    .unwrap_err()
                    .downcast_ref::<ShareError>(),
                Some(&ShareError::ReplicaClaimRejected)
            );
        }

        let mut revoked = membership.clone();
        revoked.revoked_at = Some(now());
        revoked.epoch += 1;
        registry
            .store_relationship(MemberRelationship {
                membership: revoked.clone(),
                address: address.clone(),
            })
            .unwrap();
        let mut revived = revoked;
        revived.revoked_at = None;
        assert_eq!(
            registry
                .store_relationship(MemberRelationship {
                    membership: revived,
                    address,
                })
                .unwrap_err()
                .downcast_ref::<ShareError>(),
            Some(&ShareError::ReplicaClaimRejected)
        );
    }
}
