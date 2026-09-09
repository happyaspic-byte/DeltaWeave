use super::authority::{
    ActivationBinding, ActivationCancel, ActivationReceipt, ActivationStateView,
    ActivationStatusQuery, ApplyCancel, ApplyDrained, ApplyPermit, ApplyReceipt, ApplyStart,
    ApplyStateView, ApplyStatusQuery, AuthoritativeSnapshot, ClientIntentPhase, ClientIntentRow,
    ClientSide, ManifestAttestation, RevocationReceipt, ShareGrant, SnapshotToken, request_hash,
    validate_hash_subset,
};
use super::roster::{
    MAX_ROSTER_ENTRIES, MAX_ROSTER_FRAME_BYTES, ROSTER_STALE_AFTER_SECONDS, ROSTER_TTL_SECONDS,
    ROSTER_VERSION, heartbeat_expiry, random_nonce,
};
use super::ticket::random_bytes;
use super::{
    GrantNonce, InvitationId, LegacyProof, Permission, ShareError, ShareId, ShareTicket,
    SnapshotId, now,
};
use super::{RosterEntry, RosterHeartbeat, SignedRoster};
use anyhow::{Result, ensure};
use deltaweave_core::{Hash32, ReplicaId, SyncRecord};
use iroh::{EndpointAddr, EndpointId, SecretKey};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Mutex,
    time::Instant,
};

const CATALOG: TableDefinition<u8, &[u8]> = TableDefinition::new("owner_share_catalog_v3");
const ROSTERS: TableDefinition<&str, &[u8]> = TableDefinition::new("share_roster_v1");
const HEARTBEATS: TableDefinition<&str, &[u8]> = TableDefinition::new("share_roster_heartbeat_v1");
const SNAPSHOTS: TableDefinition<&str, &[u8]> = TableDefinition::new("share-swarm-snapshot-v1");
const GRANTS: TableDefinition<&str, &[u8]> = TableDefinition::new("share-swarm-grant-v1");
/// Endpoint drain acknowledgements are additive to the grant journal.  They
/// live in a separate table so the original GrantRow postcard remains
/// decodable by older deployments and by rows written before bilateral drain
/// tracking was introduced.
const GRANT_DRAINS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("share-swarm-grant-drain-v1");
const APPLIES: TableDefinition<&str, &[u8]> = TableDefinition::new("share-swarm-apply-v1");
/// Endpoint-local grant operation intent. This is separate from the owner
/// GrantRow so a member provider can persist its send guard without creating
/// a second owner authority journal.
const CLIENT_INTENTS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("share-swarm-client-intent-v1");
/// Durable high-water mark for authority timestamps.  This table is separate
/// from the legacy catalog so a managed clock quarantine never prevents
/// opening manual shares.
const CLOCK: TableDefinition<u8, &[u8]> = TableDefinition::new("share-swarm-clock-v1");
const MAX_MEMBERS: usize = 4096;
pub(crate) const MAX_REPLICAS: usize = 4096;
const MAX_AUTHORITY_ROWS: usize = 4096;
const MAX_AUTHORITY_BYTES: usize = 16 * 1024 * 1024;
const AUTHORITY_RETENTION_SECONDS: u64 = 60 * 60;
const CLIENT_INTENT_RETENTION_SECONDS: u64 = 60 * 60;

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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct HeartbeatChallenge {
    version: u8,
    owner: EndpointId,
    share: ShareId,
    member: EndpointId,
    challenge: [u8; 32],
    issued_at: u64,
    expires_at: u64,
    used: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum GrantState {
    Issued,
    Active,
    Drained,
    Denied,
    /// An active grant observed after process restart.  It remains a drain
    /// blocker, but cannot be reactivated or silently promoted to complete.
    Restarted,
    /// An unactivated grant whose bounded wall lifetime elapsed. It is
    /// terminal for replay/error purposes and is retained only for the
    /// finite authority journal retention window.
    Expired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct GrantRow {
    grant: ShareGrant,
    state: GrantState,
    activation_id: Option<[u8; 16]>,
    activation_deadline: Option<u64>,
    /// Revocation is retained separately from the lifecycle state so an
    /// already admitted activation remains a durable drain blocker.
    #[serde(default)]
    revoked: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
struct GrantDrainState {
    provider_drained: bool,
    consumer_drained: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum ApplyState {
    Prepared,
    Started,
    Drained,
    Denied,
    /// An in-flight apply observed after process restart.  The peer must send
    /// a fresh authenticated drain acknowledgement before it can complete.
    Restarted,
    /// A permit that was never started before its bounded lifetime elapsed.
    Expired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ApplyRow {
    permit: ApplyPermit,
    state: ApplyState,
    operation_id: Option<[u8; 16]>,
    committed: bool,
    /// A started apply remains visible after revocation until its drain ACK.
    #[serde(default)]
    revoked: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ClockAnchor {
    version: u8,
    last_wall: u64,
    boot: [u8; 16],
    quarantined: bool,
}

#[derive(Debug)]
pub(crate) struct Registry {
    db: Database,
    serial: Mutex<()>,
    /// One process-generation identifier shared by every endpoint-local
    /// client intent written through this registry.
    boot_id: [u8; 16],
    /// Live monotonic activation deadlines.  Durable rows retain the wall
    /// value for audit/display; this map prevents a late response from
    /// extending a process-local 15 second lease.
    activation_deadlines: Mutex<BTreeMap<GrantNonce, Instant>>,
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
        let boot_id;
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
        // Authority journals are additive tables.  Opening them here keeps a
        // newly-created registry deterministic and gives restart recovery one
        // transaction in which to quarantine a clock and retain drain
        // blockers.  Legacy catalog bytes are never rewritten by this path.
        {
            let wall = now();
            let mut clock_table = tx.open_table(CLOCK)?;
            let previous = clock_table
                .get(0)?
                .map(|value| postcard::from_bytes::<ClockAnchor>(value.value()))
                .transpose()?;
            let mut anchor = previous.unwrap_or(ClockAnchor {
                version: 1,
                last_wall: wall,
                boot: [0; 16],
                quarantined: false,
            });
            ensure!(anchor.version == 1, ShareError::StateUnavailable);
            if wall.saturating_add(super::authority::MAX_CLOCK_SKEW_SECONDS) < anchor.last_wall {
                anchor.quarantined = true;
            } else {
                anchor.last_wall = anchor.last_wall.max(wall);
            }
            let bytes = random_bytes();
            anchor.boot.copy_from_slice(&bytes[..16]);
            let encoded = postcard::to_stdvec(&anchor)?;
            clock_table.insert(0, encoded.as_slice())?;
            boot_id = anchor.boot;
        }
        {
            // Existing Active/Started rows are unsafe to reactivate after a
            // restart.  Preserve the row and mark it as an unknown-drain
            // blocker; a fresh authenticated drain is required to finish it.
            let rows = {
                let table = tx.open_table(GRANTS)?;
                let mut rows = Vec::new();
                for item in table.iter()? {
                    let (key, value) = item?;
                    let row: GrantRow = postcard::from_bytes(value.value())?;
                    rows.push((key.value().to_owned(), row));
                }
                rows
            };
            let mut table = tx.open_table(GRANTS)?;
            for (key, mut row) in rows {
                if row.state == GrantState::Active {
                    row.state = GrantState::Restarted;
                    let bytes = postcard::to_stdvec(&row)?;
                    table.insert(key.as_str(), bytes.as_slice())?;
                }
            }
        }
        {
            let rows = {
                let table = tx.open_table(APPLIES)?;
                let mut rows = Vec::new();
                for item in table.iter()? {
                    let (key, value) = item?;
                    let row: ApplyRow = postcard::from_bytes(value.value())?;
                    rows.push((key.value().to_owned(), row));
                }
                rows
            };
            let mut table = tx.open_table(APPLIES)?;
            for (key, mut row) in rows {
                if row.state == ApplyState::Started {
                    row.state = ApplyState::Restarted;
                    let bytes = postcard::to_stdvec(&row)?;
                    table.insert(key.as_str(), bytes.as_slice())?;
                }
            }
        }
        {
            // Endpoint-local payload intents belong to the process generation
            // that admitted them.  A restart cannot prove that an old stream
            // or lease drained, so quarantine every nonterminal row as
            // Unknown before publishing the reopened registry.  Recovery may
            // query/cancel that exact binding, but no old nonce is reopened
            // for payload admission.
            let rows = {
                let table = tx.open_table(CLIENT_INTENTS)?;
                let mut rows = Vec::new();
                for item in table.iter()? {
                    let (key, value) = item?;
                    let row: ClientIntentRow = postcard::from_bytes(value.value())?;
                    rows.push((key.value().to_owned(), row));
                }
                rows
            };
            let mut table = tx.open_table(CLIENT_INTENTS)?;
            for (key, mut row) in rows {
                if row.boot_id != boot_id
                    && !matches!(
                        row.phase,
                        ClientIntentPhase::Drained | ClientIntentPhase::Cancelled
                    )
                {
                    row.phase = ClientIntentPhase::Unknown;
                    row.boot_id = boot_id;
                    let bytes = postcard::to_stdvec(&row)?;
                    table.insert(key.as_str(), bytes.as_slice())?;
                } else if matches!(
                    row.phase,
                    ClientIntentPhase::Drained | ClientIntentPhase::Cancelled
                ) && row.terminal_at_wall.is_none()
                {
                    // Rows written before terminal timestamps were added are
                    // retained conservatively from this migration point; an
                    // old start time must never cause immediate cleanup of a
                    // terminal record whose confirmation age is unknown.
                    row.terminal_at_wall = Some(now());
                    let bytes = postcard::to_stdvec(&row)?;
                    table.insert(key.as_str(), bytes.as_slice())?;
                }
            }
        }
        // Opening all tables is also an additive migration marker.  No rows
        // are removed here; cleanup is explicit and bounded below.
        tx.open_table(ROSTERS)?;
        tx.open_table(HEARTBEATS)?;
        tx.open_table(SNAPSHOTS)?;
        tx.open_table(GRANT_DRAINS)?;
        tx.commit()?;
        let registry = Self {
            db,
            serial: Mutex::new(()),
            boot_id,
            activation_deadlines: Mutex::new(BTreeMap::new()),
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
        self.update_locked(f)
    }
    fn update_locked<T>(&self, f: impl FnOnce(&mut Catalog) -> Result<T>) -> Result<T> {
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
        let _serial = self
            .serial
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        // A share cannot be forgotten while an authority row still describes
        // a possible remote writer.  Keeping these rows is what lets a later
        // status/drain recovery distinguish Pending from a completed remove;
        // deleting them would manufacture a false terminal result.
        let has_nonterminal_authority =
            self.read_grant_rows()?.into_iter().any(|(_, row)| {
                matches!(
                    row.state,
                    GrantState::Issued | GrantState::Active | GrantState::Restarted
                ) && row.grant.share == id
            }) || self.read_apply_rows()?.into_iter().any(|(_, row)| {
                matches!(
                    row.state,
                    ApplyState::Prepared | ApplyState::Started | ApplyState::Restarted
                ) && row.permit.share == id
            });
        ensure!(!has_nonterminal_authority, ShareError::RevocationPending);
        let mut catalog = self.read()?;
        ensure!(
            catalog.shares.remove(&id).is_some(),
            ShareError::UnknownShare
        );
        // Relationships are keyed by the remote owner and share.  Removing a
        // locally owned share must not erase a foreign owner's relationship
        // that happens to reuse the same ShareId.
        let local_owner = catalog.owner;
        catalog
            .relationships
            .retain(|(owner, share), _| *share != id || *owner != local_owner);
        let catalog_bytes = postcard::to_stdvec(&catalog)?;
        ensure!(
            catalog_bytes.len() <= 16 * 1024 * 1024,
            ShareError::StateUnavailable
        );

        let snapshot_remove: Vec<_> = self
            .read_snapshot_rows()?
            .into_iter()
            .filter(|(_, row)| row.token.share == id)
            .map(|(key, _)| key)
            .collect();
        let grant_rows = self
            .read_grant_rows()?
            .into_iter()
            .filter(|(_, row)| row.grant.share == id)
            .collect::<Vec<_>>();
        let grant_remove: Vec<_> = grant_rows.iter().map(|(key, _)| key.clone()).collect();
        let grant_nonces: Vec<_> = grant_rows.iter().map(|(_, row)| row.grant.nonce).collect();
        let apply_remove: Vec<_> = self
            .read_apply_rows()?
            .into_iter()
            .filter(|(_, row)| row.permit.share == id)
            .map(|(key, _)| key)
            .collect();
        let heartbeat_prefix = format!("{}:", roster_key(id));
        let heartbeat_remove = {
            let read = self.db.begin_read()?;
            let table = read.open_table(HEARTBEATS)?;
            let mut keys = Vec::new();
            for item in table.iter()? {
                let (key, _) = item?;
                if key.value().starts_with(&heartbeat_prefix) {
                    keys.push(key.value().to_owned());
                }
            }
            keys
        };
        let tx = self.db.begin_write()?;
        tx.open_table(CATALOG)?
            .insert(0, catalog_bytes.as_slice())?;
        {
            let mut table = tx.open_table(ROSTERS)?;
            table.remove(roster_key(id).as_str())?;
        }
        {
            let mut table = tx.open_table(HEARTBEATS)?;
            for key in heartbeat_remove {
                table.remove(key.as_str())?;
            }
        }
        {
            let mut table = tx.open_table(SNAPSHOTS)?;
            for key in snapshot_remove {
                table.remove(key.as_str())?;
            }
        }
        {
            let mut table = tx.open_table(GRANTS)?;
            for key in &grant_remove {
                table.remove(key.as_str())?;
            }
        }
        {
            let mut table = tx.open_table(GRANT_DRAINS)?;
            for key in &grant_remove {
                table.remove(key.as_str())?;
            }
        }
        {
            let mut table = tx.open_table(APPLIES)?;
            for key in apply_remove {
                table.remove(key.as_str())?;
            }
        }
        tx.commit()?;
        self.activation_deadlines
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?
            .retain(|nonce, _| !grant_nonces.contains(nonce));
        Ok(())
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
        self.issue_with_revocation(ticket, None)
    }

    /// Commits one signed issuance and, for rotation, revokes the old
    /// invitation in the same catalog transaction. Replaying an already
    /// committed signed ticket is exact and never changes revocation state.
    pub fn issue_with_revocation(
        &self,
        ticket: &ShareTicket,
        revoke: Option<InvitationId>,
    ) -> Result<()> {
        ticket.verify_at(now())?;
        let _serial = self
            .serial
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        // Invitation issuance is a managed authority mutation.  A durable
        // wall-clock rollback quarantine must stop creating a new bearer;
        // deny/revoke and legacy catalog reads remain available separately.
        self.observe_authority_clock(now())?;
        self.update_locked(|catalog| {
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
                if let Some(revoke) = revoke {
                    ensure!(
                        revoke != ticket.body.invitation_id,
                        ShareError::InvalidTicket
                    );
                    let old = entry
                        .invitations
                        .get_mut(&revoke)
                        .ok_or(ShareError::InvalidTicket)?;
                    old.revoked_at.get_or_insert(now());
                }
                return Ok(());
            }
            if let Some(revoke) = revoke {
                ensure!(
                    revoke != ticket.body.invitation_id,
                    ShareError::InvalidTicket
                );
                let old = entry
                    .invitations
                    .get_mut(&revoke)
                    .ok_or(ShareError::InvalidTicket)?;
                old.revoked_at.get_or_insert(now());
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
    #[allow(dead_code)]
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

    /// Issues one owner-authenticated challenge and stores its replay state in
    /// a separate v1 table.  The challenge does not grant enrollment or
    /// change the catalog membership.
    pub(crate) fn issue_roster_challenge(
        &self,
        owner_key: &SecretKey,
        share: ShareId,
        member: EndpointId,
    ) -> Result<(SignedRoster, [u8; 32])> {
        let _serial = self
            .serial
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        let catalog = self.read()?;
        ensure!(
            catalog.owner == owner_key.public(),
            ShareError::OwnerMismatch
        );
        let member_record = catalog
            .shares
            .get(&share)
            .ok_or(ShareError::UnknownShare)?
            .members
            .get(&member)
            .ok_or(ShareError::NotMember)?;
        ensure!(
            member_record.revoked_at.is_none(),
            ShareError::MemberRevoked
        );
        let now = now();
        self.observe_authority_clock(now)?;
        let previous = self.read_roster(share)?;
        let roster = self.sign_roster(owner_key, &catalog, share, previous.as_ref(), now)?;
        let challenge = random_nonce();
        let record = HeartbeatChallenge {
            version: ROSTER_VERSION,
            owner: catalog.owner,
            share,
            member,
            challenge,
            issued_at: now,
            expires_at: now.saturating_add(ROSTER_STALE_AFTER_SECONDS),
            used: false,
        };
        self.write_roster_and_challenge(&roster, &record)?;
        Ok((roster, challenge))
    }

    /// Accepts a member-signed address update only after checking the
    /// authenticated QUIC peer, the one-use durable challenge, and the live
    /// catalog epoch.  Address freshness is liveness metadata; it never
    /// creates or changes membership.
    pub(crate) fn accept_roster_heartbeat(
        &self,
        owner_key: &SecretKey,
        heartbeat: &RosterHeartbeat,
        remote_peer: EndpointId,
    ) -> Result<SignedRoster> {
        let _serial = self
            .serial
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        ensure!(
            heartbeat.owner == owner_key.public() && heartbeat.member == remote_peer,
            ShareError::EndpointMismatch
        );
        let now = now();
        self.observe_authority_clock(now)?;
        heartbeat.verify_at(now)?;
        let catalog = self.read()?;
        ensure!(
            catalog.owner == owner_key.public(),
            ShareError::OwnerMismatch
        );
        let member_record = catalog
            .shares
            .get(&heartbeat.share)
            .ok_or(ShareError::UnknownShare)?
            .members
            .get(&heartbeat.member)
            .ok_or(ShareError::NotMember)?;
        ensure!(
            member_record.revoked_at.is_none(),
            ShareError::MemberRevoked
        );
        let mut challenge = self
            .read_challenge(heartbeat.share, heartbeat.member)?
            .ok_or(ShareError::HeartbeatReplay)?;
        ensure!(
            challenge.version == ROSTER_VERSION
                && challenge.owner == catalog.owner
                && challenge.share == heartbeat.share
                && challenge.member == heartbeat.member
                && challenge.challenge == heartbeat.challenge
                && !challenge.used,
            ShareError::HeartbeatReplay
        );
        ensure!(
            now < challenge.expires_at
                && heartbeat.sent_at >= challenge.issued_at.saturating_sub(5)
                && heartbeat.sent_at <= challenge.expires_at,
            ShareError::HeartbeatExpired
        );
        let previous = self.read_roster(heartbeat.share)?;
        let mut entries = self.roster_entries(&catalog, heartbeat.share, previous.as_ref())?;
        let entry = entries
            .iter_mut()
            .find(|entry| entry.member == heartbeat.member)
            .ok_or(ShareError::NotMember)?;
        entry.address = heartbeat.address.clone();
        // `sent_at` is only a bounded freshness proof. The owner receive
        // timestamp is authoritative so a delayed heartbeat cannot extend
        // liveness by claiming a future or otherwise shifted wall time.
        entry.heartbeat_at = now;
        entry.heartbeat_expires_at = heartbeat_expiry(now);
        let roster = SignedRoster::sign(
            owner_key,
            heartbeat.share,
            random_nonce(),
            entries,
            now,
            now.saturating_add(ROSTER_TTL_SECONDS),
        )?;
        challenge.used = true;
        self.write_roster_and_challenge(&roster, &challenge)?;
        Ok(roster)
    }

    #[cfg(test)]
    pub(crate) fn stored_roster(&self, share: ShareId) -> Result<Option<SignedRoster>> {
        self.read_roster(share)
    }

    fn sign_roster(
        &self,
        owner_key: &SecretKey,
        catalog: &Catalog,
        share: ShareId,
        previous: Option<&SignedRoster>,
        now: u64,
    ) -> Result<SignedRoster> {
        let entries = self.roster_entries(catalog, share, previous)?;
        SignedRoster::sign(
            owner_key,
            share,
            random_nonce(),
            entries,
            now,
            now.saturating_add(ROSTER_TTL_SECONDS),
        )
    }

    fn roster_entries(
        &self,
        catalog: &Catalog,
        share: ShareId,
        previous: Option<&SignedRoster>,
    ) -> Result<Vec<RosterEntry>> {
        let share_entry = catalog.shares.get(&share).ok_or(ShareError::UnknownShare)?;
        if let Some(previous) = previous {
            ensure!(
                previous.owner == catalog.owner && previous.share == share,
                ShareError::StateUnavailable
            );
            previous.verify_signature()?;
        }
        let mut entries = Vec::new();
        for member in share_entry.members.values() {
            if member.revoked_at.is_some() {
                continue;
            }
            let old = previous.and_then(|roster| roster.member(member.endpoint));
            let (address, heartbeat_at, heartbeat_expires_at) = old
                .filter(|entry| {
                    entry.owner == catalog.owner
                        && entry.share == share
                        && entry.member == member.endpoint
                        && entry.address.id == member.endpoint
                        && entry.member_epoch == member.epoch
                })
                .map(|entry| {
                    (
                        entry.address.clone(),
                        entry.heartbeat_at,
                        entry.heartbeat_expires_at,
                    )
                })
                .unwrap_or_else(|| (EndpointAddr::new(member.endpoint), 0, 0));
            entries.push(RosterEntry {
                owner: catalog.owner,
                share,
                member: member.endpoint,
                address,
                permission: member.permission,
                member_epoch: member.epoch,
                heartbeat_at,
                heartbeat_expires_at,
            });
        }
        ensure!(entries.len() <= MAX_ROSTER_ENTRIES, ShareError::Busy);
        ensure!(
            postcard::to_stdvec(&entries)?.len() <= MAX_ROSTER_FRAME_BYTES,
            ShareError::Busy
        );
        Ok(entries)
    }

    fn read_roster(&self, share: ShareId) -> Result<Option<SignedRoster>> {
        let read = self.db.begin_read()?;
        let table = match read.open_table(ROSTERS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let key = roster_key(share);
        let Some(value) = table.get(key.as_str())? else {
            return Ok(None);
        };
        ensure!(
            value.value().len() <= MAX_ROSTER_FRAME_BYTES,
            ShareError::StateUnavailable
        );
        Ok(Some(postcard::from_bytes(value.value())?))
    }

    fn read_challenge(
        &self,
        share: ShareId,
        member: EndpointId,
    ) -> Result<Option<HeartbeatChallenge>> {
        let read = self.db.begin_read()?;
        let table = match read.open_table(HEARTBEATS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let key = heartbeat_key(share, member);
        let Some(value) = table.get(key.as_str())? else {
            return Ok(None);
        };
        ensure!(value.value().len() <= 4096, ShareError::StateUnavailable);
        Ok(Some(postcard::from_bytes(value.value())?))
    }

    fn write_roster_and_challenge(
        &self,
        roster: &SignedRoster,
        challenge: &HeartbeatChallenge,
    ) -> Result<()> {
        let roster_bytes = postcard::to_stdvec(roster)?;
        let challenge_bytes = postcard::to_stdvec(challenge)?;
        ensure!(
            roster_bytes.len() <= MAX_ROSTER_FRAME_BYTES && challenge_bytes.len() <= 4096,
            ShareError::StateUnavailable
        );
        let roster_key = roster_key(roster.share);
        let challenge_key = heartbeat_key(roster.share, challenge.member);
        let tx = self.db.begin_write()?;
        tx.open_table(ROSTERS)?
            .insert(roster_key.as_str(), roster_bytes.as_slice())?;
        tx.open_table(HEARTBEATS)?
            .insert(challenge_key.as_str(), challenge_bytes.as_slice())?;
        tx.commit()?;
        Ok(())
    }

    /// Rejects managed authority mutations after a durable wall-clock
    /// regression.  The legacy catalog remains readable/openable while this
    /// namespace is quarantined.  Monotonic operation deadlines are enforced
    /// by the live service; this high-water mark protects restart/replay
    /// decisions that necessarily cross a process boundary.
    fn observe_authority_clock(&self, wall: u64) -> Result<()> {
        let read = self.db.begin_read()?;
        let table = read.open_table(CLOCK)?;
        let value = table.get(0)?.ok_or(ShareError::StateUnavailable)?;
        let mut anchor: ClockAnchor = postcard::from_bytes(value.value())?;
        ensure!(anchor.version == 1, ShareError::StateUnavailable);
        if anchor.quarantined
            || wall.saturating_add(super::authority::MAX_CLOCK_SKEW_SECONDS) < anchor.last_wall
        {
            drop(value);
            drop(table);
            drop(read);
            anchor.quarantined = true;
            let bytes = postcard::to_stdvec(&anchor)?;
            let tx = self.db.begin_write()?;
            tx.open_table(CLOCK)?.insert(0, bytes.as_slice())?;
            tx.commit()?;
            return Err(ShareError::ClockRollback.into());
        }
        if wall > anchor.last_wall {
            anchor.last_wall = wall;
            let bytes = postcard::to_stdvec(&anchor)?;
            drop(value);
            drop(table);
            drop(read);
            let tx = self.db.begin_write()?;
            tx.open_table(CLOCK)?.insert(0, bytes.as_slice())?;
            tx.commit()?;
        }
        Ok(())
    }

    fn read_snapshot_rows(&self) -> Result<Vec<(String, AuthoritativeSnapshot)>> {
        let read = self.db.begin_read()?;
        let table = read.open_table(SNAPSHOTS)?;
        let mut rows = Vec::new();
        for item in table.iter()? {
            let (key, value) = item?;
            ensure!(
                value.value().len() <= super::authority::MAX_AUTHORITY_FRAME_BYTES,
                ShareError::StateUnavailable
            );
            rows.push((key.value().to_owned(), postcard::from_bytes(value.value())?));
        }
        Ok(rows)
    }

    /// Deletes only terminal, expired authority rows.  In-flight or
    /// post-restart rows are deliberately retained because removing them would
    /// turn an unknown remote writer into a false Complete result.
    fn gc_authority(&self, wall: u64) -> Result<()> {
        let snapshots = self.read_snapshot_rows()?;
        let mut grants = self.read_grant_rows()?;
        let mut applies = self.read_apply_rows()?;
        // Unactivated rows cannot later become writers after their bounded
        // lease elapsed. Terminalizing them keeps repeated offline requests
        // from exhausting the authority cap while preserving a stable
        // GrantExpired result until retention cleanup.
        for (_, row) in &mut grants {
            if row.state == GrantState::Issued && row.grant.expires_at <= wall {
                row.state = GrantState::Expired;
            }
        }
        for (_, row) in &mut applies {
            if row.state == ApplyState::Prepared && row.permit.expires_at <= wall {
                row.state = ApplyState::Expired;
            }
        }
        let mut referenced = BTreeSet::new();
        for (_, row) in &grants {
            referenced.insert(row.grant.snapshot);
        }
        for (_, row) in &applies {
            referenced.insert(row.permit.snapshot);
        }
        let snapshot_remove: Vec<_> = snapshots
            .iter()
            .filter(|(_, snapshot)| {
                snapshot
                    .token
                    .expires_at
                    .saturating_add(AUTHORITY_RETENTION_SECONDS)
                    <= wall
                    && !referenced.contains(&snapshot.token.snapshot)
            })
            .map(|(key, _)| key.clone())
            .collect();
        let grant_remove: Vec<_> = grants
            .iter()
            .filter(|(_, row)| {
                matches!(
                    row.state,
                    GrantState::Drained | GrantState::Denied | GrantState::Expired
                ) && row
                    .grant
                    .expires_at
                    .saturating_add(AUTHORITY_RETENTION_SECONDS)
                    <= wall
            })
            .map(|(key, _)| key.clone())
            .collect();
        let apply_remove: Vec<_> = applies
            .iter()
            .filter(|(_, row)| {
                matches!(
                    row.state,
                    ApplyState::Drained | ApplyState::Denied | ApplyState::Expired
                ) && row
                    .permit
                    .expires_at
                    .saturating_add(AUTHORITY_RETENTION_SECONDS)
                    <= wall
            })
            .map(|(key, _)| key.clone())
            .collect();
        let changed_grants: Vec<_> = grants
            .iter()
            .filter(|(key, row)| {
                row.state == GrantState::Expired
                    && !grant_remove.iter().any(|candidate| candidate == key)
            })
            .map(|(key, row)| (key.clone(), row.clone()))
            .collect();
        let changed_applies: Vec<_> = applies
            .iter()
            .filter(|(key, row)| {
                row.state == ApplyState::Expired
                    && !apply_remove.iter().any(|candidate| candidate == key)
            })
            .map(|(key, row)| (key.clone(), row.clone()))
            .collect();
        if snapshot_remove.is_empty()
            && grant_remove.is_empty()
            && apply_remove.is_empty()
            && changed_grants.is_empty()
            && changed_applies.is_empty()
        {
            return Ok(());
        }
        let tx = self.db.begin_write()?;
        {
            let mut table = tx.open_table(SNAPSHOTS)?;
            for key in snapshot_remove {
                table.remove(key.as_str())?;
            }
        }
        {
            let mut table = tx.open_table(GRANTS)?;
            for (key, row) in changed_grants {
                let bytes = postcard::to_stdvec(&row)?;
                table.insert(key.as_str(), bytes.as_slice())?;
            }
            for key in &grant_remove {
                table.remove(key.as_str())?;
            }
        }
        {
            let mut table = tx.open_table(GRANT_DRAINS)?;
            for key in &grant_remove {
                table.remove(key.as_str())?;
            }
        }
        {
            let mut table = tx.open_table(APPLIES)?;
            for (key, row) in changed_applies {
                let bytes = postcard::to_stdvec(&row)?;
                table.insert(key.as_str(), bytes.as_slice())?;
            }
            for key in apply_remove {
                table.remove(key.as_str())?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn ensure_authority_capacity(
        &self,
        additional_rows: usize,
        additional_bytes: usize,
    ) -> Result<()> {
        let snapshots = self.read_snapshot_rows()?;
        let grants = self.read_grant_rows()?;
        let applies = self.read_apply_rows()?;
        let rows = snapshots.len() + grants.len() + applies.len();
        let bytes = snapshots
            .iter()
            .map(|(_, row)| postcard::to_stdvec(row).map(|bytes| bytes.len()))
            .chain(
                grants
                    .iter()
                    .map(|(_, row)| postcard::to_stdvec(row).map(|bytes| bytes.len())),
            )
            .chain(
                applies
                    .iter()
                    .map(|(_, row)| postcard::to_stdvec(row).map(|bytes| bytes.len())),
            )
            .try_fold(0usize, |total, size| {
                Ok::<usize, postcard::Error>(total.saturating_add(size?))
            })?;
        ensure!(
            rows.saturating_add(additional_rows) <= MAX_AUTHORITY_ROWS,
            ShareError::Busy
        );
        ensure!(
            bytes.saturating_add(additional_bytes) <= MAX_AUTHORITY_BYTES,
            ShareError::Busy
        );
        Ok(())
    }

    fn read_client_intents(&self) -> Result<Vec<(String, ClientIntentRow)>> {
        let read = self.db.begin_read()?;
        let table = match read.open_table(CLIENT_INTENTS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let mut rows = Vec::new();
        let mut total_bytes = 0usize;
        for item in table.iter()? {
            let (key, value) = item?;
            ensure!(
                rows.len() < MAX_AUTHORITY_ROWS,
                ShareError::StateUnavailable
            );
            ensure!(
                value.value().len() <= super::authority::MAX_AUTHORITY_FRAME_BYTES,
                ShareError::StateUnavailable
            );
            total_bytes = total_bytes
                .checked_add(value.value().len())
                .ok_or(ShareError::StateUnavailable)?;
            ensure!(
                total_bytes <= MAX_AUTHORITY_BYTES,
                ShareError::StateUnavailable
            );
            rows.push((key.value().to_owned(), postcard::from_bytes(value.value())?));
        }
        Ok(rows)
    }

    fn read_client_intent(&self, key: &str) -> Result<Option<ClientIntentRow>> {
        let read = self.db.begin_read()?;
        let table = match read.open_table(CLIENT_INTENTS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let Some(value) = table.get(key)? else {
            return Ok(None);
        };
        ensure!(
            value.value().len() <= super::authority::MAX_AUTHORITY_FRAME_BYTES,
            ShareError::StateUnavailable
        );
        Ok(Some(postcard::from_bytes(value.value())?))
    }

    fn write_client_intent(&self, key: &str, row: &ClientIntentRow) -> Result<()> {
        let bytes = postcard::to_stdvec(row)?;
        ensure!(
            bytes.len() <= super::authority::MAX_AUTHORITY_FRAME_BYTES,
            ShareError::StateUnavailable
        );
        let tx = self.db.begin_write()?;
        tx.open_table(CLIENT_INTENTS)?
            .insert(key, bytes.as_slice())?;
        tx.commit()?;
        Ok(())
    }

    fn gc_client_intents(&self, wall: u64) -> Result<()> {
        let remove: Vec<_> = self
            .read_client_intents()?
            .into_iter()
            .filter(|(_, row)| {
                matches!(
                    row.phase,
                    ClientIntentPhase::Drained | ClientIntentPhase::Cancelled
                ) && row
                    .terminal_at_wall
                    .unwrap_or(row.started_at_wall)
                    .saturating_add(CLIENT_INTENT_RETENTION_SECONDS)
                    <= wall
            })
            .map(|(key, _)| key)
            .collect();
        if remove.is_empty() {
            return Ok(());
        }
        let tx = self.db.begin_write()?;
        let mut table = tx.open_table(CLIENT_INTENTS)?;
        for key in remove {
            table.remove(key.as_str())?;
        }
        drop(table);
        tx.commit()?;
        Ok(())
    }

    /// Persists the endpoint-local guard before a share-swarm request is
    /// opened. Exact grant replay is idempotent for the same operation ID;
    /// another operation cannot claim the same nonce in parallel.
    pub(crate) fn prepare_client_intent(
        &self,
        grant: &ShareGrant,
        side: ClientSide,
        operation_id: [u8; 16],
    ) -> Result<ClientIntentRow> {
        let _serial = self
            .serial
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        let wall = now();
        grant.verify_for(grant.owner, grant.share, wall)?;
        self.gc_client_intents(wall)?;
        let key = grant_key(grant.nonce);
        if let Some(existing) = self.read_client_intent(&key)? {
            ensure!(
                existing.grant == *grant
                    && existing.binding == ActivationBinding::from_grant(grant),
                ShareError::GrantReplay
            );
            ensure!(existing.side == side, ShareError::GrantReplay);
            if existing.operation_id == operation_id {
                return Ok(existing);
            }
            return Err(ShareError::Busy.into());
        }
        let row = ClientIntentRow {
            grant: grant.clone(),
            binding: ActivationBinding::from_grant(grant),
            side,
            activation_id: None,
            operation_id,
            phase: ClientIntentPhase::Prepared,
            boot_id: self.boot_id,
            started_at_wall: wall,
            terminal_at_wall: None,
        };
        let intents = self.read_client_intents()?;
        let row_bytes = postcard::to_stdvec(&row)?.len();
        let total_bytes = intents.iter().try_fold(row_bytes, |total, (_, intent)| {
            Ok::<_, postcard::Error>(total.saturating_add(postcard::to_stdvec(intent)?.len()))
        })?;
        ensure!(
            intents.len() < MAX_AUTHORITY_ROWS && total_bytes <= MAX_AUTHORITY_BYTES,
            ShareError::Busy
        );
        self.write_client_intent(&key, &row)?;
        Ok(row)
    }

    /// Advances one endpoint intent monotonically. A repeated exact phase is
    /// idempotent; terminal phases cannot be reopened by a retry.
    pub(crate) fn transition_client_intent(
        &self,
        grant: &ShareGrant,
        side: ClientSide,
        operation_id: [u8; 16],
        phase: ClientIntentPhase,
        activation_id: Option<[u8; 16]>,
    ) -> Result<ClientIntentRow> {
        let _serial = self
            .serial
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        let key = grant_key(grant.nonce);
        let mut row = self
            .read_client_intent(&key)?
            .ok_or(ShareError::GrantReplay)?;
        ensure!(
            row.grant == *grant
                && row.binding == ActivationBinding::from_grant(grant)
                && row.side == side
                && row.operation_id == operation_id,
            ShareError::GrantReplay
        );
        if let Some(id) = activation_id {
            if let Some(existing) = row.activation_id {
                ensure!(existing == id, ShareError::GrantReplay);
            } else {
                row.activation_id = Some(id);
            }
        }
        let valid = match (row.phase, phase) {
            (current, next) if current == next => true,
            (ClientIntentPhase::Prepared, ClientIntentPhase::AwaitingActivation)
            | (ClientIntentPhase::Prepared, ClientIntentPhase::Unknown)
            | (ClientIntentPhase::Prepared, ClientIntentPhase::Cancelled)
            | (ClientIntentPhase::AwaitingActivation, ClientIntentPhase::Active)
            | (ClientIntentPhase::AwaitingActivation, ClientIntentPhase::Unknown)
            | (ClientIntentPhase::AwaitingActivation, ClientIntentPhase::Draining)
            | (ClientIntentPhase::AwaitingActivation, ClientIntentPhase::Cancelled)
            | (ClientIntentPhase::Active, ClientIntentPhase::Draining)
            | (ClientIntentPhase::Active, ClientIntentPhase::Unknown)
            | (ClientIntentPhase::Active, ClientIntentPhase::Drained)
            | (ClientIntentPhase::Draining, ClientIntentPhase::Drained)
            | (ClientIntentPhase::Draining, ClientIntentPhase::Unknown)
            // An Unknown operation may only be terminalized by a recovery
            // control path. It must never restart payload admission with the
            // old nonce after a crash or a cancelled stream.
            | (ClientIntentPhase::Unknown, ClientIntentPhase::Draining)
            | (ClientIntentPhase::Unknown, ClientIntentPhase::Drained)
            | (ClientIntentPhase::Unknown, ClientIntentPhase::Cancelled) => true,
            (ClientIntentPhase::Drained, ClientIntentPhase::Drained)
            | (ClientIntentPhase::Cancelled, ClientIntentPhase::Cancelled) => true,
            _ => false,
        };
        ensure!(valid, ShareError::GrantReplay);
        row.phase = phase;
        if matches!(
            phase,
            ClientIntentPhase::Drained | ClientIntentPhase::Cancelled
        ) && row.terminal_at_wall.is_none()
        {
            row.terminal_at_wall = Some(now());
        }
        self.write_client_intent(&key, &row)?;
        Ok(row)
    }

    #[allow(dead_code)]
    pub(crate) fn client_intent(&self, grant: &ShareGrant) -> Result<Option<ClientIntentRow>> {
        self.read_client_intent(&grant_key(grant.nonce))
    }

    /// Returns the bounded endpoint-local journal without exposing its
    /// storage keys. Callers must still validate the exact grant binding and
    /// operation before acting on a row; enumeration is for restart
    /// recovery only and never grants payload permission.
    pub(crate) fn client_intents(&self) -> Result<Vec<ClientIntentRow>> {
        Ok(self
            .read_client_intents()?
            .into_iter()
            .map(|(_, row)| row)
            .collect())
    }

    /// Reads one exact journal row for recovery. A nonce collision with a
    /// different grant, side, or operation is a replay/conflict rather than
    /// an invitation to create a replacement intent.
    pub(crate) fn client_intent_exact(
        &self,
        grant: &ShareGrant,
        side: ClientSide,
        operation_id: [u8; 16],
    ) -> Result<Option<ClientIntentRow>> {
        // Recovery may run after the grant's wall-clock expiry, so validate
        // its immutable owner signature without treating expiry as permission
        // to start payload work.
        grant.verify_signature_for(grant.owner, grant.share)?;
        let Some(row) = self.client_intent(grant)? else {
            return Ok(None);
        };
        ensure!(
            row.grant == *grant
                && row.binding == super::authority::ActivationBinding::from_grant(grant)
                && row.side == side
                && row.operation_id == operation_id,
            ShareError::GrantReplay
        );
        Ok(Some(row))
    }

    /// Persists one complete owner snapshot in the separate swarm namespace.
    /// The catalog postcard remains untouched; the signed token is the only
    /// authority a later manifest or grant request may reference.
    pub(crate) fn store_snapshot(&self, snapshot: &AuthoritativeSnapshot) -> Result<()> {
        let _serial = self
            .serial
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        let wall = now();
        self.observe_authority_clock(wall)?;
        let catalog = self.read()?;
        snapshot.verify_complete(catalog.owner, snapshot.token.share, wall)?;
        ensure!(
            catalog.shares.contains_key(&snapshot.token.share),
            ShareError::UnknownShare
        );
        let bytes = postcard::to_stdvec(snapshot)?;
        ensure!(
            bytes.len() <= super::authority::MAX_AUTHORITY_FRAME_BYTES,
            ShareError::StateUnavailable
        );
        self.gc_authority(wall)?;
        let key = snapshot_key(snapshot.token.snapshot);
        let existing = self.snapshot(snapshot.token.snapshot)?;
        if let Some(existing) = existing {
            ensure!(existing == *snapshot, ShareError::GrantReplay);
            return Ok(());
        }
        self.ensure_authority_capacity(1, bytes.len())?;
        let tx = self.db.begin_write()?;
        tx.open_table(SNAPSHOTS)?
            .insert(key.as_str(), bytes.as_slice())?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn snapshot(&self, snapshot: SnapshotId) -> Result<Option<AuthoritativeSnapshot>> {
        let read = self.db.begin_read()?;
        let table = match read.open_table(SNAPSHOTS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let key = snapshot_key(snapshot);
        let Some(value) = table.get(key.as_str())? else {
            return Ok(None);
        };
        ensure!(
            value.value().len() <= super::authority::MAX_AUTHORITY_FRAME_BYTES,
            ShareError::StateUnavailable
        );
        Ok(Some(postcard::from_bytes(value.value())?))
    }

    /// Issues one exact provider grant after rechecking both memberships and
    /// the durable owner snapshot. A member provider must have a fresh signed
    /// roster heartbeat; the owner provider is the explicit epoch-zero case.
    pub(crate) fn issue_swarm_grant(
        &self,
        owner_key: &SecretKey,
        consumer: EndpointId,
        provider: EndpointId,
        snapshot: &SnapshotToken,
        manifest: &ManifestAttestation,
        hashes: &[Hash32],
    ) -> Result<ShareGrant> {
        let _serial = self
            .serial
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        let now = now();
        self.observe_authority_clock(now)?;
        self.gc_authority(now)?;
        snapshot.verify_for(owner_key.public(), snapshot.share, now)?;
        manifest.verify_for(owner_key.public(), snapshot.share, now)?;
        validate_hash_subset(hashes)?;
        let stored = self
            .snapshot(snapshot.snapshot)?
            .ok_or(ShareError::ManifestMismatch)?;
        ensure!(stored.token == *snapshot, ShareError::ManifestMismatch);
        let stored_record = stored
            .records
            .iter()
            .find(|record| record.logical_hash() == manifest.record_hash)
            .ok_or(ShareError::ManifestMismatch)?;
        manifest.verify_record(snapshot, stored_record)?;
        for hash in hashes {
            ensure!(
                manifest
                    .manifest
                    .chunks
                    .iter()
                    .any(|chunk| chunk.hash == *hash),
                ShareError::ManifestMismatch
            );
        }
        let catalog = self.read()?;
        ensure!(
            catalog.owner == owner_key.public(),
            ShareError::OwnerMismatch
        );
        let share_entry = catalog
            .shares
            .get(&snapshot.share)
            .ok_or(ShareError::UnknownShare)?;
        let consumer_member = share_entry
            .members
            .get(&consumer)
            .ok_or(ShareError::NotMember)?;
        ensure!(
            consumer_member.revoked_at.is_none(),
            ShareError::MemberRevoked
        );
        ensure!(
            consumer_member.epoch == snapshot.epoch,
            ShareError::EpochMismatch
        );
        ensure!(provider != consumer, ShareError::EndpointMismatch);
        let provider_epoch = if provider == catalog.owner {
            0
        } else {
            let provider_member = share_entry
                .members
                .get(&provider)
                .ok_or(ShareError::NotMember)?;
            ensure!(
                provider_member.revoked_at.is_none(),
                ShareError::MemberRevoked
            );
            let roster = self
                .read_roster(snapshot.share)?
                .ok_or(ShareError::RosterStale)?;
            roster.verify_for(catalog.owner, snapshot.share, now)?;
            ensure!(
                roster.member_is_fresh(provider, now),
                ShareError::RosterStale
            );
            ensure!(
                roster.member(provider).is_some_and(|entry| {
                    entry.member_epoch == provider_member.epoch
                        && entry.permission == provider_member.permission
                }),
                ShareError::EpochMismatch
            );
            provider_member.epoch
        };
        let request_hash = request_hash(
            snapshot.share,
            snapshot.snapshot,
            manifest.manifest_hash,
            hashes,
        )?;
        let grant = ShareGrant::sign(
            owner_key,
            snapshot.share,
            consumer,
            provider,
            consumer_member.epoch,
            provider_epoch,
            snapshot.snapshot,
            manifest.manifest_hash,
            request_hash,
            random_nonce(),
            now,
        )?;
        let bytes = postcard::to_stdvec(&GrantRow {
            grant: grant.clone(),
            state: GrantState::Issued,
            activation_id: None,
            activation_deadline: None,
            revoked: false,
        })?;
        ensure!(
            bytes.len() <= super::authority::MAX_AUTHORITY_FRAME_BYTES,
            ShareError::StateUnavailable
        );
        self.ensure_authority_capacity(1, bytes.len())?;
        let key = grant_key(grant.nonce);
        let tx = self.db.begin_write()?;
        let mut table = tx.open_table(GRANTS)?;
        ensure!(table.get(key.as_str())?.is_none(), ShareError::GrantReplay);
        table.insert(key.as_str(), bytes.as_slice())?;
        drop(table);
        let drain = postcard::to_stdvec(&GrantDrainState::default())?;
        tx.open_table(GRANT_DRAINS)?
            .insert(key.as_str(), drain.as_slice())?;
        tx.commit()?;
        Ok(grant)
    }

    /// Revalidates a complete snapshot against the owner's current root and
    /// records a bounded apply permit in the separate apply journal.
    pub(crate) fn issue_apply_permit(
        &self,
        owner_key: &SecretKey,
        consumer: EndpointId,
        snapshot: &SnapshotToken,
        current_root: Hash32,
    ) -> Result<ApplyPermit> {
        let _serial = self
            .serial
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        let now = now();
        self.observe_authority_clock(now)?;
        self.gc_authority(now)?;
        snapshot.verify_for(owner_key.public(), snapshot.share, now)?;
        ensure!(
            snapshot.root_hash == current_root,
            ShareError::ManifestMismatch
        );
        let stored = self
            .snapshot(snapshot.snapshot)?
            .ok_or(ShareError::ManifestMismatch)?;
        ensure!(stored.token == *snapshot, ShareError::ManifestMismatch);
        let member = self.authorize(snapshot.share, consumer, false)?;
        ensure!(member.epoch == snapshot.epoch, ShareError::EpochMismatch);
        let permit = ApplyPermit::sign(
            owner_key,
            snapshot.share,
            consumer,
            member.epoch,
            snapshot.snapshot,
            current_root,
            now,
            random_nonce(),
        )?;
        let row = ApplyRow {
            permit: permit.clone(),
            state: ApplyState::Prepared,
            operation_id: None,
            committed: false,
            revoked: false,
        };
        let bytes = postcard::to_stdvec(&row)?;
        ensure!(
            bytes.len() <= super::authority::MAX_AUTHORITY_FRAME_BYTES,
            ShareError::StateUnavailable
        );
        self.ensure_authority_capacity(1, bytes.len())?;
        let key = grant_key(permit.nonce);
        let tx = self.db.begin_write()?;
        let mut table = tx.open_table(APPLIES)?;
        ensure!(table.get(key.as_str())?.is_none(), ShareError::GrantReplay);
        table.insert(key.as_str(), bytes.as_slice())?;
        drop(table);
        tx.commit()?;
        Ok(permit)
    }

    pub(crate) fn apply_start(
        &self,
        owner: EndpointId,
        share: ShareId,
        peer: EndpointId,
        current_root: Hash32,
        start: &ApplyStart,
    ) -> Result<()> {
        let _serial = self
            .serial
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        self.observe_authority_clock(now())?;
        let key = grant_key(start.permit_nonce);
        let mut row = self
            .read_apply(key.as_str())?
            .ok_or(ShareError::GrantReplay)?;
        ensure!(
            row.permit.owner == owner && row.permit.share == share && row.permit.consumer == peer,
            ShareError::EndpointMismatch
        );
        ensure!(!row.revoked, ShareError::MemberRevoked);
        let member = self.authorize(share, peer, false)?;
        ensure!(member.epoch == row.permit.epoch, ShareError::EpochMismatch);
        ensure!(
            row.permit.root_hash == current_root,
            ShareError::ManifestMismatch
        );
        row.permit.verify_for(owner, share, peer, now())?;
        match row.state {
            ApplyState::Prepared => {
                row.state = ApplyState::Started;
                row.operation_id = Some(start.operation_id);
            }
            ApplyState::Started if row.operation_id == Some(start.operation_id) => return Ok(()),
            ApplyState::Drained if row.operation_id == Some(start.operation_id) => return Ok(()),
            ApplyState::Denied => return Err(ShareError::MemberRevoked.into()),
            ApplyState::Restarted => return Err(ShareError::GrantReplay.into()),
            ApplyState::Expired => return Err(ShareError::GrantExpired.into()),
            _ => return Err(ShareError::GrantReplay.into()),
        }
        self.write_apply(&key, &row)
    }

    pub(crate) fn apply_drained(
        &self,
        owner: EndpointId,
        share: ShareId,
        peer: EndpointId,
        drained: &ApplyDrained,
    ) -> Result<()> {
        let _serial = self
            .serial
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        let key = grant_key(drained.permit_nonce);
        let mut row = self
            .read_apply(key.as_str())?
            .ok_or(ShareError::GrantReplay)?;
        ensure!(
            row.permit.owner == owner && row.permit.share == share && row.permit.consumer == peer,
            ShareError::EndpointMismatch
        );
        ensure!(
            row.operation_id == Some(drained.operation_id),
            ShareError::GrantReplay
        );
        match row.state {
            ApplyState::Started => {
                row.state = ApplyState::Drained;
                row.committed = drained.committed;
                self.write_apply(&key, &row)
            }
            ApplyState::Drained if row.committed == drained.committed => Ok(()),
            ApplyState::Restarted => {
                row.state = ApplyState::Drained;
                row.committed = drained.committed;
                self.write_apply(&key, &row)
            }
            ApplyState::Denied => Err(ShareError::MemberRevoked.into()),
            _ => Err(ShareError::GrantReplay.into()),
        }
    }

    fn apply_receipt_from_row(row: &ApplyRow) -> ApplyReceipt {
        let state = match row.state {
            ApplyState::Prepared => ApplyStateView::Prepared,
            ApplyState::Started => ApplyStateView::Started,
            ApplyState::Restarted => ApplyStateView::Restarted,
            ApplyState::Drained => ApplyStateView::Drained,
            ApplyState::Denied => ApplyStateView::Denied,
            ApplyState::Expired => ApplyStateView::Expired,
        };
        ApplyReceipt {
            owner: row.permit.owner,
            share: row.permit.share,
            consumer: row.permit.consumer,
            permit_nonce: row.permit.nonce,
            operation_id: row.operation_id,
            state,
            committed: row.committed,
            revoked: row.revoked,
            admission_open: true,
        }
    }

    fn validate_apply_status_binding(
        row: &ApplyRow,
        owner: EndpointId,
        share: ShareId,
        consumer: EndpointId,
        permit_nonce: GrantNonce,
        operation_id: Option<[u8; 16]>,
        authenticated_peer: EndpointId,
    ) -> Result<()> {
        ensure!(owner == row.permit.owner, ShareError::OwnerMismatch);
        ensure!(share == row.permit.share, ShareError::OwnerMismatch);
        ensure!(
            consumer == row.permit.consumer,
            ShareError::EndpointMismatch
        );
        ensure!(permit_nonce == row.permit.nonce, ShareError::GrantReplay);
        ensure!(authenticated_peer == consumer, ShareError::EndpointMismatch);
        match (row.operation_id, operation_id) {
            (Some(expected), Some(actual)) => ensure!(expected == actual, ShareError::GrantReplay),
            (None, None) => {}
            (None, Some(_)) => ensure!(row.state == ApplyState::Prepared, ShareError::GrantReplay),
            (Some(_), None) => return Err(ShareError::GrantReplay.into()),
        }
        row.permit.verify_signature_for(owner, share, consumer)
    }

    /// Reads the exact apply journal row in a single owner-authenticated
    /// operation. Expiry and restart states are returned as stored; status
    /// never extends a lease or silently terminalizes an unknown row.
    pub(crate) fn apply_status(
        &self,
        query: &ApplyStatusQuery,
        authenticated_peer: EndpointId,
    ) -> Result<ApplyReceipt> {
        let _serial = self
            .serial
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        let catalog = self.read()?;
        ensure!(query.owner == catalog.owner, ShareError::OwnerMismatch);
        let key = grant_key(query.permit_nonce);
        let row = self
            .read_apply(key.as_str())?
            .ok_or(ShareError::GrantReplay)?;
        Self::validate_apply_status_binding(
            &row,
            query.owner,
            query.share,
            query.consumer,
            query.permit_nonce,
            query.operation_id,
            authenticated_peer,
        )?;
        Ok(Self::apply_receipt_from_row(&row))
    }

    /// Atomically cancels a Prepared apply. Started/Restarted rows retain their
    /// exact operation and writer-drain blocker; a late cancel cannot turn an
    /// active write into a false Denied completion.
    pub(crate) fn cancel_apply(
        &self,
        cancel: &ApplyCancel,
        authenticated_peer: EndpointId,
    ) -> Result<ApplyReceipt> {
        let _serial = self
            .serial
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        let catalog = self.read()?;
        ensure!(cancel.owner == catalog.owner, ShareError::OwnerMismatch);
        let key = grant_key(cancel.permit_nonce);
        let mut row = self
            .read_apply(key.as_str())?
            .ok_or(ShareError::GrantReplay)?;
        ensure!(
            row.permit.owner == cancel.owner
                && row.permit.share == cancel.share
                && row.permit.consumer == cancel.consumer
                && row.permit.nonce == cancel.permit_nonce
                && authenticated_peer == cancel.consumer,
            ShareError::EndpointMismatch
        );
        row.permit
            .verify_signature_for(cancel.owner, cancel.share, cancel.consumer)?;
        if let Some(existing) = row.operation_id {
            ensure!(existing == cancel.operation_id, ShareError::GrantReplay);
        }
        match row.state {
            ApplyState::Prepared => {
                row.operation_id = Some(cancel.operation_id);
                row.state = ApplyState::Denied;
                self.write_apply(&key, &row)?;
            }
            ApplyState::Started
            | ApplyState::Restarted
            | ApplyState::Drained
            | ApplyState::Denied
            | ApplyState::Expired => {}
        }
        Ok(Self::apply_receipt_from_row(&row))
    }

    #[allow(dead_code)]
    pub(crate) fn active_apply_blockers(&self, share: ShareId, peer: EndpointId) -> Result<u32> {
        let mut blockers = 0_u32;
        for (_, row) in self.read_apply_rows()? {
            if row.permit.share == share
                && row.permit.consumer == peer
                && matches!(row.state, ApplyState::Started | ApplyState::Restarted)
            {
                blockers = blockers.saturating_add(1);
            }
        }
        Ok(blockers)
    }

    #[allow(dead_code)]
    pub(crate) fn active_grant_blockers(&self, share: ShareId, peer: EndpointId) -> Result<u32> {
        let mut blockers = 0_u32;
        for (_, row) in self.read_grant_rows()? {
            if row.grant.share == share
                && (row.grant.consumer == peer || row.grant.provider == peer)
                && matches!(row.state, GrantState::Active | GrantState::Restarted)
            {
                blockers = blockers.saturating_add(1);
            }
        }
        Ok(blockers)
    }

    /// Validates the provider side of an already activated grant and returns
    /// the process-local monotonic stream deadline.  E's data handler must
    /// still perform its own bounded chunk/CAS checks, but it can use this
    /// primitive to ensure the authenticated connection direction is exact:
    /// the local endpoint is the signed provider and the remote endpoint is
    /// the signed consumer.  A restarted, revoked, drained-side, or unknown
    /// activation is never admitted by this path.
    #[allow(dead_code)]
    pub(crate) fn validate_provider_grant(
        &self,
        grant: &ShareGrant,
        local_provider: EndpointId,
        remote_consumer: EndpointId,
    ) -> Result<Instant> {
        let _serial = self
            .serial
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        ensure!(
            grant.provider == local_provider,
            ShareError::EndpointMismatch
        );
        ensure!(
            grant.consumer == remote_consumer,
            ShareError::EndpointMismatch
        );
        let row = self
            .read_grant(grant_key(grant.nonce).as_str())?
            .ok_or(ShareError::GrantReplay)?;
        ensure!(row.grant == *grant, ShareError::GrantReplay);
        ensure!(!row.revoked, ShareError::MemberRevoked);
        ensure!(row.state == GrantState::Active, ShareError::GrantReplay);
        let drain = self.read_grant_drain(grant_key(grant.nonce).as_str())?;
        ensure!(
            !drain.provider_drained && !drain.consumer_drained,
            ShareError::GrantReplay
        );
        let deadline = self
            .activation_deadlines
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?
            .get(&grant.nonce)
            .copied()
            .ok_or(ShareError::GrantReplay)?;
        ensure!(Instant::now() < deadline, ShareError::GrantExpired);
        grant.verify_for(grant.owner, grant.share, now())?;
        Ok(deadline)
    }

    /// Commits membership revocation and grant denial in one redb transaction.
    /// Started apply rows remain visible so the caller cannot report false
    /// completion while a remote writer is still draining.
    pub(crate) fn revoke_member_durable(
        &self,
        share: ShareId,
        peer: EndpointId,
    ) -> Result<Membership> {
        let _serial = self
            .serial
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        let mut catalog = self.read()?;
        let member = catalog
            .shares
            .get_mut(&share)
            .ok_or(ShareError::UnknownShare)?
            .members
            .get_mut(&peer)
            .ok_or(ShareError::NotMember)?;
        if member.revoked_at.is_none() {
            member.revoked_at = Some(now());
            member.epoch = member
                .epoch
                .checked_add(1)
                .ok_or(ShareError::StateUnavailable)?;
        }
        let result = member.clone();
        let catalog_bytes = postcard::to_stdvec(&catalog)?;
        let mut grants = self.read_grant_rows()?;
        for (_, row) in &mut grants {
            if row.grant.share == share
                && (row.grant.consumer == peer || row.grant.provider == peer)
                && matches!(
                    row.state,
                    GrantState::Issued | GrantState::Active | GrantState::Restarted
                )
            {
                row.revoked = true;
                if row.state == GrantState::Issued {
                    row.state = GrantState::Denied;
                }
            }
        }
        let mut applies = self.read_apply_rows()?;
        for (_, row) in &mut applies {
            if row.permit.share == share && row.permit.consumer == peer {
                row.revoked = true;
                if row.state == ApplyState::Prepared {
                    row.state = ApplyState::Denied;
                }
            }
        }
        let tx = self.db.begin_write()?;
        tx.open_table(CATALOG)?
            .insert(0, catalog_bytes.as_slice())?;
        {
            let mut table = tx.open_table(GRANTS)?;
            for (key, row) in grants {
                let bytes = postcard::to_stdvec(&row)?;
                table.insert(key.as_str(), bytes.as_slice())?;
            }
        }
        {
            let mut table = tx.open_table(APPLIES)?;
            for (key, row) in applies {
                let bytes = postcard::to_stdvec(&row)?;
                table.insert(key.as_str(), bytes.as_slice())?;
            }
        }
        tx.commit()?;
        Ok(result)
    }

    /// Returns the durable revoke result without confusing a closed local
    /// connection with a remote writer drain acknowledgement.  Active and
    /// post-restart rows remain blockers until their explicit drain records
    /// are committed.
    pub(crate) fn revocation_receipt(
        &self,
        share: ShareId,
        peer: EndpointId,
    ) -> Result<RevocationReceipt> {
        let member = self
            .read()?
            .shares
            .get(&share)
            .ok_or(ShareError::UnknownShare)?
            .members
            .get(&peer)
            .ok_or(ShareError::NotMember)?
            .clone();
        ensure!(member.revoked_at.is_some(), ShareError::NotMember);
        let grants = self.read_grant_rows()?;
        let applies = self.read_apply_rows()?;
        let mut blockers = 0_u32;
        let mut deadline = now();
        for (_, row) in grants {
            if row.grant.share == share
                && (row.grant.consumer == peer || row.grant.provider == peer)
                && matches!(row.state, GrantState::Active | GrantState::Restarted)
            {
                blockers = blockers.saturating_add(1);
                deadline = deadline.max(row.activation_deadline.unwrap_or_else(|| {
                    now().saturating_add(u64::from(super::authority::MAX_ACTIVATE_TTL_SECONDS))
                }));
            }
        }
        for (_, row) in applies {
            if row.permit.share == share
                && row.permit.consumer == peer
                && matches!(row.state, ApplyState::Started | ApplyState::Restarted)
            {
                blockers = blockers.saturating_add(1);
                deadline = deadline.max(row.permit.expires_at);
            }
        }
        if blockers == 0 {
            Ok(RevocationReceipt::Complete {
                member_epoch: member.epoch,
                completed_at: now(),
            })
        } else {
            Ok(RevocationReceipt::Pending {
                member_epoch: member.epoch,
                deadline,
                blockers,
            })
        }
    }

    /// Commits one authenticated endpoint drain acknowledgement.  Expiry
    /// alone is never a completion proof, and one endpoint's acknowledgement
    /// cannot stand in for the other endpoint's writer/reader drain.  An
    /// Active/Restarted row becomes Drained only after the exact activation
    /// identifier has been acknowledged by both consumer and provider.
    #[allow(dead_code)]
    pub(crate) fn drain_grant(
        &self,
        share: ShareId,
        nonce: GrantNonce,
        activation_id: [u8; 16],
        remote_peer: EndpointId,
    ) -> Result<()> {
        let _serial = self
            .serial
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        let key = grant_key(nonce);
        let mut row = self
            .read_grant(key.as_str())?
            .ok_or(ShareError::GrantReplay)?;
        ensure!(row.grant.share == share, ShareError::OwnerMismatch);
        ensure!(
            row.grant.consumer == remote_peer || row.grant.provider == remote_peer,
            ShareError::EndpointMismatch
        );
        ensure!(
            row.activation_id == Some(activation_id),
            ShareError::GrantReplay
        );
        let is_consumer = row.grant.consumer == remote_peer;
        let is_provider = row.grant.provider == remote_peer;
        ensure!(is_consumer || is_provider, ShareError::EndpointMismatch);
        match row.state {
            GrantState::Active | GrantState::Restarted => {
                let mut drain = self.read_grant_drain(key.as_str())?;
                if is_consumer {
                    drain.consumer_drained = true;
                }
                if is_provider {
                    drain.provider_drained = true;
                }
                if drain.consumer_drained && drain.provider_drained {
                    row.state = GrantState::Drained;
                    self.activation_deadlines
                        .lock()
                        .map_err(|_| ShareError::StateUnavailable)?
                        .remove(&nonce);
                }
                self.write_grant_and_drain(&key, &row, &drain)
            }
            GrantState::Drained => Ok(()),
            GrantState::Issued | GrantState::Denied | GrantState::Expired => {
                Err(ShareError::GrantReplay.into())
            }
        }
    }

    /// Reads one grant and its two-sided drain state from the same redb read
    /// transaction.  A missing drain row is the conservative pre-migration
    /// value: neither endpoint has acknowledged a drain.
    fn read_grant_with_drain(&self, key: &str) -> Result<Option<(GrantRow, GrantDrainState)>> {
        let read = self.db.begin_read()?;
        let row = {
            let table = match read.open_table(GRANTS) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(error) => return Err(error.into()),
            };
            let Some(value) = table.get(key)? else {
                return Ok(None);
            };
            ensure!(
                value.value().len() <= super::authority::MAX_AUTHORITY_FRAME_BYTES,
                ShareError::StateUnavailable
            );
            postcard::from_bytes::<GrantRow>(value.value())?
        };
        let drain = {
            let table = match read.open_table(GRANT_DRAINS) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => {
                    return Ok(Some((row, GrantDrainState::default())));
                }
                Err(error) => return Err(error.into()),
            };
            let Some(value) = table.get(key)? else {
                return Ok(Some((row, GrantDrainState::default())));
            };
            ensure!(value.value().len() <= 1024, ShareError::StateUnavailable);
            postcard::from_bytes::<GrantDrainState>(value.value())?
        };
        Ok(Some((row, drain)))
    }

    fn activation_receipt_from_row(row: &GrantRow, drain: GrantDrainState) -> ActivationReceipt {
        let state = match row.state {
            GrantState::Issued => ActivationStateView::Issued,
            GrantState::Active => ActivationStateView::Active,
            GrantState::Restarted => ActivationStateView::Restarted,
            GrantState::Drained => ActivationStateView::Drained,
            GrantState::Denied => ActivationStateView::Denied,
            GrantState::Expired => ActivationStateView::Expired,
        };
        ActivationReceipt {
            binding: ActivationBinding::from_grant(&row.grant),
            state,
            activation_id: row.activation_id,
            activation_deadline: row.activation_deadline,
            revoked: row.revoked,
            provider_drained: drain.provider_drained,
            consumer_drained: drain.consumer_drained,
            admission_open: true,
        }
    }

    fn validate_activation_query(
        row: &GrantRow,
        binding: &ActivationBinding,
        activation_id: Option<[u8; 16]>,
        authenticated_peer: EndpointId,
    ) -> Result<()> {
        ensure!(
            ActivationBinding::from_grant(&row.grant) == *binding,
            ShareError::GrantReplay
        );
        ensure!(
            row.grant.consumer == authenticated_peer || row.grant.provider == authenticated_peer,
            ShareError::EndpointMismatch
        );
        if let Some(activation_id) = activation_id {
            ensure!(
                row.activation_id == Some(activation_id),
                ShareError::GrantReplay
            );
        }
        Ok(())
    }

    /// Returns an authenticated owner receipt without changing the live
    /// monotonic lease or treating a wall-clock expiry as a drain proof.
    pub(crate) fn activation_receipt(
        &self,
        query: &ActivationStatusQuery,
        authenticated_peer: EndpointId,
    ) -> Result<ActivationReceipt> {
        let _serial = self
            .serial
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        let catalog = self.read()?;
        ensure!(
            query.binding.owner == catalog.owner,
            ShareError::OwnerMismatch
        );
        let key = grant_key(query.binding.nonce);
        let (row, drain) = self
            .read_grant_with_drain(&key)?
            .ok_or(ShareError::GrantReplay)?;
        ensure!(row.grant.owner == catalog.owner, ShareError::OwnerMismatch);
        Self::validate_activation_query(
            &row,
            &query.binding,
            query.activation_id,
            authenticated_peer,
        )?;
        Ok(Self::activation_receipt_from_row(&row, drain))
    }

    /// Atomically wins the race against an unactivated grant by writing its
    /// existing row as Denied (or Expired when its bounded wall lifetime has
    /// elapsed).  If activation already won, this is an idempotent read of the
    /// Active/Restarted drain blocker and never rewrites it as Denied.
    pub(crate) fn cancel_activation(
        &self,
        cancel: &ActivationCancel,
        authenticated_peer: EndpointId,
    ) -> Result<ActivationReceipt> {
        let _serial = self
            .serial
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        let wall = now();
        self.observe_authority_clock(wall)?;
        let catalog = self.read()?;
        ensure!(
            cancel.binding.owner == catalog.owner,
            ShareError::OwnerMismatch
        );
        let key = grant_key(cancel.binding.nonce);
        let (mut row, drain) = self
            .read_grant_with_drain(&key)?
            .ok_or(ShareError::GrantReplay)?;
        Self::validate_activation_query(
            &row,
            &cancel.binding,
            cancel.activation_id,
            authenticated_peer,
        )?;
        match row.state {
            GrantState::Issued => {
                row.state = if row.grant.expires_at <= wall {
                    GrantState::Expired
                } else {
                    GrantState::Denied
                };
                row.activation_deadline = None;
                self.write_grant_and_drain(&key, &row, &drain)?;
                self.activation_deadlines
                    .lock()
                    .map_err(|_| ShareError::StateUnavailable)?
                    .remove(&cancel.binding.nonce);
            }
            GrantState::Active
            | GrantState::Restarted
            | GrantState::Drained
            | GrantState::Denied
            | GrantState::Expired => {}
        }
        Ok(Self::activation_receipt_from_row(&row, drain))
    }

    #[allow(dead_code)]
    pub(crate) fn activate_grant(
        &self,
        owner_key: &SecretKey,
        request: &super::authority::ActivateGrantRequest,
        remote_peer: EndpointId,
    ) -> Result<super::authority::ActivateGrantReply> {
        self.activate_grant_at(owner_key, request, remote_peer, Instant::now())
    }

    pub(crate) fn activate_grant_at(
        &self,
        owner_key: &SecretKey,
        request: &super::authority::ActivateGrantRequest,
        remote_peer: EndpointId,
        request_started: Instant,
    ) -> Result<super::authority::ActivateGrantReply> {
        let _serial = self
            .serial
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        ensure!(
            request_started.elapsed()
                <= std::time::Duration::from_secs(u64::from(
                    super::authority::MAX_ACTIVATE_TTL_SECONDS
                ),),
            ShareError::GrantExpired
        );
        self.observe_authority_clock(now())?;
        ensure!(
            request.provider == remote_peer,
            ShareError::EndpointMismatch
        );
        let key = grant_key(request.nonce);
        let mut row = self
            .read_grant(key.as_str())?
            .ok_or(ShareError::GrantReplay)?;
        ensure!(
            row.grant.activate_request() == *request,
            ShareError::GrantReplay
        );
        if row.revoked {
            return Err(ShareError::MemberRevoked.into());
        }
        match row.state {
            GrantState::Issued => {
                row.grant
                    .verify_for(owner_key.public(), request.share, now())?;
            }
            GrantState::Active | GrantState::Drained | GrantState::Restarted => {
                return Err(ShareError::GrantReplay.into());
            }
            GrantState::Denied => return Err(ShareError::MemberRevoked.into()),
            GrantState::Expired => return Err(ShareError::GrantExpired.into()),
        }
        let catalog = self.read()?;
        ensure!(
            catalog.owner == owner_key.public(),
            ShareError::OwnerMismatch
        );
        let share_entry = catalog
            .shares
            .get(&request.share)
            .ok_or(ShareError::UnknownShare)?;
        let consumer = share_entry
            .members
            .get(&request.consumer)
            .ok_or(ShareError::NotMember)?;
        ensure!(consumer.revoked_at.is_none(), ShareError::MemberRevoked);
        ensure!(consumer.epoch == request.epoch, ShareError::EpochMismatch);
        if request.provider != catalog.owner {
            let provider = share_entry
                .members
                .get(&request.provider)
                .ok_or(ShareError::NotMember)?;
            ensure!(provider.revoked_at.is_none(), ShareError::MemberRevoked);
            ensure!(
                provider.epoch == request.provider_epoch,
                ShareError::EpochMismatch
            );
            let roster = self
                .read_roster(request.share)?
                .ok_or(ShareError::RosterStale)?;
            roster.verify_for(catalog.owner, request.share, now())?;
            ensure!(
                roster.member_is_fresh(request.provider, now())
                    && roster.member(request.provider).is_some_and(|entry| {
                        entry.member_epoch == provider.epoch && entry.address.id == request.provider
                    }),
                ShareError::RosterStale
            );
        } else {
            ensure!(request.provider_epoch == 0, ShareError::EpochMismatch);
        }
        let activation_id = {
            let bytes = random_nonce();
            let mut id = [0_u8; 16];
            id.copy_from_slice(&bytes[..16]);
            id
        };
        let reply = super::authority::ActivateGrantReply::sign(
            owner_key,
            request.share,
            request.provider,
            request.nonce,
            activation_id,
            true,
            super::authority::MAX_ACTIVATE_TTL_SECONDS,
        )?;
        row.state = GrantState::Active;
        row.activation_id = Some(activation_id);
        let wall_deadline =
            now().saturating_add(u64::from(super::authority::MAX_ACTIVATE_TTL_SECONDS));
        row.activation_deadline = Some(wall_deadline);
        self.activation_deadlines
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?
            .insert(
                request.nonce,
                request_started
                    + std::time::Duration::from_secs(u64::from(
                        super::authority::MAX_ACTIVATE_TTL_SECONDS,
                    )),
            );
        self.write_grant(&key, &row)?;
        Ok(reply)
    }

    fn read_grant(&self, key: &str) -> Result<Option<GrantRow>> {
        let read = self.db.begin_read()?;
        let table = match read.open_table(GRANTS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let Some(value) = table.get(key)? else {
            return Ok(None);
        };
        ensure!(
            value.value().len() <= super::authority::MAX_AUTHORITY_FRAME_BYTES,
            ShareError::StateUnavailable
        );
        Ok(Some(postcard::from_bytes(value.value())?))
    }

    fn read_grant_drain(&self, key: &str) -> Result<GrantDrainState> {
        let read = self.db.begin_read()?;
        let table = match read.open_table(GRANT_DRAINS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(GrantDrainState::default()),
            Err(error) => return Err(error.into()),
        };
        let Some(value) = table.get(key)? else {
            // Rows created before this additive table existed conservatively
            // start with neither endpoint acknowledged.
            return Ok(GrantDrainState::default());
        };
        ensure!(value.value().len() <= 1024, ShareError::StateUnavailable);
        Ok(postcard::from_bytes(value.value())?)
    }

    fn read_grant_rows(&self) -> Result<Vec<(String, GrantRow)>> {
        let read = self.db.begin_read()?;
        let table = match read.open_table(GRANTS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let mut rows = Vec::new();
        for item in table.iter()? {
            let (key, value) = item?;
            ensure!(
                value.value().len() <= super::authority::MAX_AUTHORITY_FRAME_BYTES,
                ShareError::StateUnavailable
            );
            rows.push((key.value().to_owned(), postcard::from_bytes(value.value())?));
        }
        Ok(rows)
    }

    fn write_grant(&self, key: &str, row: &GrantRow) -> Result<()> {
        let drain = self.read_grant_drain(key)?;
        self.write_grant_and_drain(key, row, &drain)
    }

    fn write_grant_and_drain(
        &self,
        key: &str,
        row: &GrantRow,
        drain: &GrantDrainState,
    ) -> Result<()> {
        let bytes = postcard::to_stdvec(row)?;
        let drain_bytes = postcard::to_stdvec(drain)?;
        let tx = self.db.begin_write()?;
        tx.open_table(GRANTS)?.insert(key, bytes.as_slice())?;
        tx.open_table(GRANT_DRAINS)?
            .insert(key, drain_bytes.as_slice())?;
        tx.commit()?;
        Ok(())
    }

    fn read_apply(&self, key: &str) -> Result<Option<ApplyRow>> {
        let read = self.db.begin_read()?;
        let table = match read.open_table(APPLIES) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let Some(value) = table.get(key)? else {
            return Ok(None);
        };
        ensure!(
            value.value().len() <= super::authority::MAX_AUTHORITY_FRAME_BYTES,
            ShareError::StateUnavailable
        );
        Ok(Some(postcard::from_bytes(value.value())?))
    }

    fn read_apply_rows(&self) -> Result<Vec<(String, ApplyRow)>> {
        let read = self.db.begin_read()?;
        let table = match read.open_table(APPLIES) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let mut rows = Vec::new();
        for item in table.iter()? {
            let (key, value) = item?;
            ensure!(
                value.value().len() <= super::authority::MAX_AUTHORITY_FRAME_BYTES,
                ShareError::StateUnavailable
            );
            rows.push((key.value().to_owned(), postcard::from_bytes(value.value())?));
        }
        Ok(rows)
    }

    fn write_apply(&self, key: &str, row: &ApplyRow) -> Result<()> {
        let bytes = postcard::to_stdvec(row)?;
        let tx = self.db.begin_write()?;
        tx.open_table(APPLIES)?.insert(key, bytes.as_slice())?;
        tx.commit()?;
        Ok(())
    }
}

fn roster_key(share: ShareId) -> String {
    hex::encode(share.0)
}

fn heartbeat_key(share: ShareId, member: EndpointId) -> String {
    format!("{}:{}", roster_key(share), hex::encode(member.as_bytes()))
}

fn snapshot_key(snapshot: SnapshotId) -> String {
    hex::encode(snapshot)
}

fn grant_key(nonce: GrantNonce) -> String {
    hex::encode(nonce)
}

#[allow(dead_code)]
fn snapshot_record(
    registry: &Registry,
    snapshot: &SnapshotToken,
    manifest: &ManifestAttestation,
) -> Result<SyncRecord> {
    registry
        .snapshot(snapshot.snapshot)?
        .and_then(|stored| {
            stored
                .records
                .into_iter()
                .find(|record| record.logical_hash() == manifest.record_hash)
        })
        .ok_or_else(|| ShareError::ManifestMismatch.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaweave_reconcile::MerkleTree;
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

    fn authority_fixture() -> (tempfile::TempDir, SecretKey, ShareId, EndpointId, Registry) {
        let temp = tempfile::tempdir().unwrap();
        let owner = SecretKey::generate();
        let peer = SecretKey::generate().public();
        let share = ShareId([0x44; 32]);
        let registry = Registry::open(&temp.path().join("private"), owner.public()).unwrap();
        registry
            .insert_share(
                OwnedShareConfig {
                    share_id: share,
                    owner: owner.public(),
                    name: "Authority".into(),
                    root: temp.path().join("root"),
                    state_root: temp.path().join("state"),
                    replica: ReplicaId(Hash32::digest(b"authority-owner")),
                    min_free_space_bytes: 0,
                },
                BTreeSet::new(),
            )
            .unwrap();
        let ticket = ShareTicket::issue(
            &owner,
            share,
            "Authority".into(),
            Permission::ReadWrite,
            None,
            EndpointAddr::new(owner.public()),
            now(),
        )
        .unwrap();
        registry.issue(&ticket).unwrap();
        registry.enroll(&ticket, peer, None).unwrap();
        (temp, owner, share, peer, registry)
    }

    #[test]
    fn client_intent_replay_is_exact_and_restarts_as_unknown() {
        let (temp, owner, share, peer, registry) = authority_fixture();
        let grant = ShareGrant::sign(
            &owner,
            share,
            peer,
            owner.public(),
            1,
            0,
            [0xc1; 32],
            Hash32::digest(b"intent-manifest"),
            Hash32::digest(b"intent-request"),
            [0xc2; 32],
            now(),
        )
        .unwrap();
        let operation_id = [0xc3; 16];
        let prepared = registry
            .prepare_client_intent(&grant, ClientSide::Consumer, operation_id)
            .unwrap();
        assert!(matches!(prepared.phase, ClientIntentPhase::Prepared));
        let replay = registry
            .prepare_client_intent(&grant, ClientSide::Consumer, operation_id)
            .unwrap();
        assert_eq!(replay, prepared);
        assert_eq!(
            registry
                .prepare_client_intent(&grant, ClientSide::Consumer, [0xc4; 16])
                .unwrap_err()
                .downcast_ref::<ShareError>(),
            Some(&ShareError::Busy)
        );
        registry
            .transition_client_intent(
                &grant,
                ClientSide::Consumer,
                operation_id,
                ClientIntentPhase::AwaitingActivation,
                None,
            )
            .unwrap();
        let before_restart = registry.client_intent(&grant).unwrap().unwrap();
        assert!(matches!(
            before_restart.phase,
            ClientIntentPhase::AwaitingActivation
        ));
        let private = temp.path().join("private");
        drop(registry);

        let reopened = Registry::open(&private, owner.public()).unwrap();
        let after_restart = reopened.client_intent(&grant).unwrap().unwrap();
        assert!(matches!(after_restart.phase, ClientIntentPhase::Unknown));
        assert_eq!(after_restart.binding, before_restart.binding);
        assert_eq!(after_restart.operation_id, operation_id);
        assert_ne!(after_restart.boot_id, before_restart.boot_id);
        assert_eq!(
            reopened
                .transition_client_intent(
                    &grant,
                    ClientSide::Consumer,
                    operation_id,
                    ClientIntentPhase::AwaitingActivation,
                    None,
                )
                .unwrap_err()
                .downcast_ref::<ShareError>(),
            Some(&ShareError::GrantReplay)
        );
        reopened
            .transition_client_intent(
                &grant,
                ClientSide::Consumer,
                operation_id,
                ClientIntentPhase::Cancelled,
                None,
            )
            .unwrap();
    }

    #[test]
    fn one_sided_grant_drain_keeps_client_intent_recoverable() {
        let (_temp, owner, share, peer, registry) = authority_fixture();
        let nonce = [0xd1; 32];
        let activation_id = [0xd2; 16];
        let grant = ShareGrant::sign(
            &owner,
            share,
            peer,
            owner.public(),
            1,
            0,
            [0xd3; 32],
            Hash32::digest(b"one-sided-manifest"),
            Hash32::digest(b"one-sided-request"),
            nonce,
            now(),
        )
        .unwrap();
        registry
            .write_grant(
                &grant_key(nonce),
                &GrantRow {
                    grant: grant.clone(),
                    state: GrantState::Active,
                    activation_id: Some(activation_id),
                    activation_deadline: Some(now().saturating_add(15)),
                    revoked: false,
                },
            )
            .unwrap();
        registry
            .prepare_client_intent(&grant, ClientSide::Consumer, [0xd4; 16])
            .unwrap();
        registry
            .transition_client_intent(
                &grant,
                ClientSide::Consumer,
                [0xd4; 16],
                ClientIntentPhase::AwaitingActivation,
                Some(activation_id),
            )
            .unwrap();
        registry
            .transition_client_intent(
                &grant,
                ClientSide::Consumer,
                [0xd4; 16],
                ClientIntentPhase::Active,
                Some(activation_id),
            )
            .unwrap();
        registry
            .transition_client_intent(
                &grant,
                ClientSide::Consumer,
                [0xd4; 16],
                ClientIntentPhase::Draining,
                Some(activation_id),
            )
            .unwrap();

        // The consumer acknowledgement is authenticated, but the provider
        // acknowledgement is intentionally withheld. The owner grant and
        // local journal must remain active/recoverable and GC must not infer
        // completion from one side.
        registry
            .drain_grant(share, nonce, activation_id, peer)
            .unwrap();
        let receipt = registry
            .activation_receipt(
                &ActivationStatusQuery::for_grant(&grant, Some(activation_id)),
                peer,
            )
            .unwrap();
        assert!(matches!(receipt.state, ActivationStateView::Active));
        assert!(receipt.consumer_drained && !receipt.provider_drained);
        assert!(matches!(
            registry.client_intent(&grant).unwrap().unwrap().phase,
            ClientIntentPhase::Draining
        ));
        registry
            .gc_client_intents(now().saturating_add(CLIENT_INTENT_RETENTION_SECONDS + 1))
            .unwrap();
        assert!(registry.client_intent(&grant).unwrap().is_some());
    }

    #[test]
    fn activation_status_and_cancel_are_exact_and_idempotent() {
        let (_temp, owner, share, peer, registry) = authority_fixture();
        let nonce = [0xa1; 32];
        let grant = ShareGrant::sign(
            &owner,
            share,
            peer,
            owner.public(),
            1,
            0,
            [0xa2; 32],
            Hash32::digest(b"activation-manifest"),
            Hash32::digest(b"activation-request"),
            nonce,
            now(),
        )
        .unwrap();
        registry
            .write_grant(
                &grant_key(nonce),
                &GrantRow {
                    grant: grant.clone(),
                    state: GrantState::Issued,
                    activation_id: None,
                    activation_deadline: None,
                    revoked: false,
                },
            )
            .unwrap();

        let issued = registry
            .activation_receipt(&ActivationStatusQuery::for_grant(&grant, None), peer)
            .unwrap();
        assert!(matches!(issued.state, ActivationStateView::Issued));
        assert_eq!(issued.binding, ActivationBinding::from_grant(&grant));
        assert!(!issued.revoked);
        assert!(!issued.provider_drained && !issued.consumer_drained);

        let cancel = ActivationCancel::for_grant(&grant, None, [0xa3; 16]);
        let denied = registry.cancel_activation(&cancel, peer).unwrap();
        assert!(matches!(denied.state, ActivationStateView::Denied));
        assert!(denied.activation_id.is_none());

        // A retry with a new local operation ID observes the same durable
        // denial and cannot issue or activate another invitation.
        let retry = registry
            .cancel_activation(&ActivationCancel::for_grant(&grant, None, [0xa4; 16]), peer)
            .unwrap();
        assert!(matches!(retry.state, ActivationStateView::Denied));
        assert_eq!(retry.binding, denied.binding);
        let late = registry
            .activate_grant(&owner, &grant.activate_request(), owner.public())
            .unwrap_err();
        assert_eq!(
            late.downcast_ref::<ShareError>(),
            Some(&ShareError::MemberRevoked)
        );

        let mut wrong = ActivationStatusQuery::for_grant(&grant, None);
        wrong.binding.request_hash = Hash32::digest(b"different-request");
        assert_eq!(
            registry
                .activation_receipt(&wrong, peer)
                .unwrap_err()
                .downcast_ref(),
            Some(&ShareError::GrantReplay)
        );
    }

    #[test]
    fn activation_status_does_not_synthesize_expiry_before_durable_cancel() {
        let (_temp, owner, share, peer, registry) = authority_fixture();
        let nonce = [0xa9; 32];
        let mut grant = ShareGrant::sign(
            &owner,
            share,
            peer,
            owner.public(),
            1,
            0,
            [0xaa; 32],
            Hash32::digest(b"expired-manifest"),
            Hash32::digest(b"expired-request"),
            nonce,
            now(),
        )
        .unwrap();
        // This row is trusted durable authority in the registry fixture. Its
        // signed shape is otherwise unchanged; the expired wall value is
        // deliberately used to exercise the cleanup ordering.
        grant.expires_at = now().saturating_sub(1);
        registry
            .write_grant(
                &grant_key(nonce),
                &GrantRow {
                    grant: grant.clone(),
                    state: GrantState::Issued,
                    activation_id: None,
                    activation_deadline: None,
                    revoked: false,
                },
            )
            .unwrap();

        let before_cancel = registry
            .activation_receipt(&ActivationStatusQuery::for_grant(&grant, None), peer)
            .unwrap();
        assert!(matches!(before_cancel.state, ActivationStateView::Issued));
        let expired = registry
            .cancel_activation(&ActivationCancel::for_grant(&grant, None, [0xab; 16]), peer)
            .unwrap();
        assert!(matches!(expired.state, ActivationStateView::Expired));
        let after_cancel = registry
            .activation_receipt(&ActivationStatusQuery::for_grant(&grant, None), peer)
            .unwrap();
        assert!(matches!(after_cancel.state, ActivationStateView::Expired));
    }

    #[test]
    fn remove_share_preserves_nonterminal_activation_for_drain_recovery() {
        let (_temp, owner, share, peer, registry) = authority_fixture();
        let foreign_owner = SecretKey::generate().public();
        registry
            .store_relationship(MemberRelationship {
                membership: Membership {
                    share_id: share,
                    owner: foreign_owner,
                    endpoint: owner.public(),
                    permission: Permission::ReadOnly,
                    replica: ReplicaId(Hash32::digest(b"foreign-replica")),
                    enrolled_at: now(),
                    revoked_at: None,
                    epoch: 1,
                },
                address: EndpointAddr::new(foreign_owner),
            })
            .unwrap();
        let nonce = [0xa5; 32];
        let grant = ShareGrant::sign(
            &owner,
            share,
            peer,
            owner.public(),
            1,
            0,
            [0xa6; 32],
            Hash32::digest(b"remove-manifest"),
            Hash32::digest(b"remove-request"),
            nonce,
            now(),
        )
        .unwrap();
        registry
            .write_grant(
                &grant_key(nonce),
                &GrantRow {
                    grant,
                    state: GrantState::Issued,
                    activation_id: None,
                    activation_deadline: None,
                    revoked: false,
                },
            )
            .unwrap();

        let error = registry.remove_share(share).unwrap_err();
        assert_eq!(
            error.downcast_ref::<ShareError>(),
            Some(&ShareError::RevocationPending)
        );
        assert!(registry.config(share).is_ok());
        assert!(registry.read_grant(&grant_key(nonce)).unwrap().is_some());

        let cancel = ActivationCancel::for_grant(
            &registry
                .read_grant(&grant_key(nonce))
                .unwrap()
                .unwrap()
                .grant,
            None,
            [0xa8; 16],
        );
        registry.cancel_activation(&cancel, peer).unwrap();
        registry.remove_share(share).unwrap();
        assert!(registry.relationship(foreign_owner, share).is_ok());
    }

    #[test]
    fn restart_keeps_active_grant_as_unknown_drain_blocker() {
        let (_temp, owner, share, peer, registry) = authority_fixture();
        let nonce = [0x51; 32];
        let grant = ShareGrant::sign(
            &owner,
            share,
            peer,
            owner.public(),
            1,
            0,
            [0x52; 32],
            Hash32::digest(b"manifest"),
            Hash32::digest(b"request"),
            nonce,
            now(),
        )
        .unwrap();
        registry
            .write_grant(
                &grant_key(nonce),
                &GrantRow {
                    grant,
                    state: GrantState::Active,
                    activation_id: Some([0x53; 16]),
                    activation_deadline: Some(now().saturating_add(15)),
                    revoked: false,
                },
            )
            .unwrap();
        drop(registry);
        let registry = Registry::open(&_temp.path().join("private"), owner.public()).unwrap();
        assert_eq!(
            registry
                .read_grant(&grant_key(nonce))
                .unwrap()
                .unwrap()
                .state,
            GrantState::Restarted
        );
        assert_eq!(registry.active_grant_blockers(share, peer).unwrap(), 1);
        assert!(
            registry
                .activate_grant(
                    &owner,
                    &ShareGrant::sign(
                        &owner,
                        share,
                        peer,
                        owner.public(),
                        1,
                        0,
                        [0x52; 32],
                        Hash32::digest(b"manifest"),
                        Hash32::digest(b"request"),
                        nonce,
                        now(),
                    )
                    .unwrap()
                    .activate_request(),
                    owner.public(),
                )
                .is_err()
        );
    }

    #[test]
    fn durable_revoke_stays_pending_until_exact_grant_drain() {
        let (_temp, owner, share, peer, registry) = authority_fixture();
        let nonce = [0x61; 32];
        let activation_id = [0x62; 16];
        let grant = ShareGrant::sign(
            &owner,
            share,
            peer,
            owner.public(),
            1,
            0,
            [0x63; 32],
            Hash32::digest(b"manifest"),
            Hash32::digest(b"request"),
            nonce,
            now(),
        )
        .unwrap();
        registry
            .write_grant(
                &grant_key(nonce),
                &GrantRow {
                    grant,
                    state: GrantState::Active,
                    activation_id: Some(activation_id),
                    activation_deadline: Some(now().saturating_add(15)),
                    revoked: false,
                },
            )
            .unwrap();
        let revoked = registry.revoke_member_durable(share, peer).unwrap();
        assert!(revoked.revoked_at.is_some());
        assert!(matches!(
            registry.revocation_receipt(share, peer).unwrap(),
            RevocationReceipt::Pending { blockers: 1, .. }
        ));
        registry
            .drain_grant(share, nonce, activation_id, owner.public())
            .unwrap();
        assert!(matches!(
            registry.revocation_receipt(share, peer).unwrap(),
            RevocationReceipt::Pending { blockers: 1, .. }
        ));
        assert_eq!(
            registry
                .read_grant(&grant_key(nonce))
                .unwrap()
                .unwrap()
                .state,
            GrantState::Active
        );
        registry
            .drain_grant(share, nonce, activation_id, peer)
            .unwrap();
        assert!(matches!(
            registry.revocation_receipt(share, peer).unwrap(),
            RevocationReceipt::Complete { .. }
        ));
    }

    #[test]
    fn activation_rejects_after_request_start_monotonic_deadline() {
        let (_temp, owner, share, peer, registry) = authority_fixture();
        let nonce = [0x81; 32];
        let grant = ShareGrant::sign(
            &owner,
            share,
            peer,
            owner.public(),
            1,
            0,
            [0x82; 32],
            Hash32::digest(b"manifest"),
            Hash32::digest(b"request"),
            nonce,
            now(),
        )
        .unwrap();
        registry
            .write_grant(
                &grant_key(nonce),
                &GrantRow {
                    grant: grant.clone(),
                    state: GrantState::Issued,
                    activation_id: None,
                    activation_deadline: None,
                    revoked: false,
                },
            )
            .unwrap();
        let stale_start = Instant::now() - std::time::Duration::from_secs(16);
        let error = registry
            .activate_grant_at(
                &owner,
                &grant.activate_request(),
                owner.public(),
                stale_start,
            )
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<ShareError>(),
            Some(&ShareError::GrantExpired)
        );
        assert_eq!(
            registry
                .read_grant(&grant_key(nonce))
                .unwrap()
                .unwrap()
                .state,
            GrantState::Issued
        );
    }

    #[test]
    fn missing_grant_drain_rows_default_to_two_sided_pending_drain() {
        let (_temp, owner, share, peer, registry) = authority_fixture();
        let nonce = [0x92; 32];
        let grant = ShareGrant::sign(
            &owner,
            share,
            peer,
            owner.public(),
            1,
            0,
            [0x91; 32],
            Hash32::digest(b"manifest"),
            Hash32::digest(b"request"),
            nonce,
            now(),
        )
        .unwrap();
        let activation_id = [0x93; 16];
        registry
            .write_grant(
                &grant_key(nonce),
                &GrantRow {
                    grant,
                    state: GrantState::Active,
                    activation_id: Some(activation_id),
                    activation_deadline: Some(now().saturating_add(15)),
                    revoked: false,
                },
            )
            .unwrap();
        let tx = registry.db.begin_write().unwrap();
        tx.open_table(GRANT_DRAINS)
            .unwrap()
            .remove(grant_key(nonce).as_str())
            .unwrap();
        tx.commit().unwrap();
        assert_eq!(
            registry
                .read_grant_drain(grant_key(nonce).as_str())
                .unwrap(),
            GrantDrainState::default()
        );
        registry
            .drain_grant(share, nonce, activation_id, owner.public())
            .unwrap();
        assert_eq!(
            registry
                .read_grant_drain(grant_key(nonce).as_str())
                .unwrap(),
            GrantDrainState {
                provider_drained: true,
                consumer_drained: false,
            }
        );
    }

    #[test]
    fn authority_clock_quarantine_is_managed_only_and_preserves_catalog() {
        let (temp, owner, share, _peer, registry) = authority_fixture();
        let empty_root = MerkleTree::from_records(Vec::new()).unwrap().root_hash();
        let first_token =
            SnapshotToken::sign(&owner, share, 1, [0x72; 32], empty_root, 0, now()).unwrap();
        registry
            .store_snapshot(&AuthoritativeSnapshot {
                token: first_token,
                records: Vec::new(),
            })
            .unwrap();
        let config_before = registry.config(share).unwrap();
        let future = now().saturating_add(30);
        let anchor = ClockAnchor {
            version: 1,
            last_wall: future,
            boot: [0x73; 16],
            quarantined: false,
        };
        let bytes = postcard::to_stdvec(&anchor).unwrap();
        let tx = registry.db.begin_write().unwrap();
        tx.open_table(CLOCK)
            .unwrap()
            .insert(0, bytes.as_slice())
            .unwrap();
        tx.commit().unwrap();
        let second_token =
            SnapshotToken::sign(&owner, share, 1, [0x74; 32], empty_root, 0, now()).unwrap();
        assert!(matches!(
            registry.store_snapshot(&AuthoritativeSnapshot {
                token: second_token,
                records: Vec::new(),
            }),
            Err(error) if error.downcast_ref::<ShareError>() == Some(&ShareError::ClockRollback)
        ));
        let ticket = ShareTicket::issue(
            &owner,
            share,
            "Authority".into(),
            Permission::ReadOnly,
            None,
            EndpointAddr::new(owner.public()),
            now(),
        )
        .unwrap();
        assert!(matches!(
            registry.issue(&ticket),
            Err(error) if error.downcast_ref::<ShareError>() == Some(&ShareError::ClockRollback)
        ));
        assert_eq!(registry.config(share).unwrap(), config_before);
        drop(registry);
        // Reopening a quarantined managed authority must still leave its
        // legacy catalog readable for manual UI/configuration paths.
        let reopened = Registry::open(&temp.path().join("private"), owner.public()).unwrap();
        assert_eq!(reopened.config(share).unwrap(), config_before);
    }

    #[test]
    fn restart_keeps_started_apply_as_unknown_drain_blocker() {
        let (temp, owner, share, peer, registry) = authority_fixture();
        let permit = ApplyPermit::sign(
            &owner,
            share,
            peer,
            1,
            [0x81; 32],
            Hash32::digest(b"apply-root"),
            now(),
            [0x82; 32],
        )
        .unwrap();
        registry
            .write_apply(
                &grant_key(permit.nonce),
                &ApplyRow {
                    permit,
                    state: ApplyState::Started,
                    operation_id: Some([0x83; 16]),
                    committed: false,
                    revoked: false,
                },
            )
            .unwrap();
        drop(registry);
        let reopened = Registry::open(&temp.path().join("private"), owner.public()).unwrap();
        assert_eq!(
            reopened
                .read_apply(&grant_key([0x82; 32]))
                .unwrap()
                .unwrap()
                .state,
            ApplyState::Restarted
        );
        assert_eq!(reopened.active_apply_blockers(share, peer).unwrap(), 1);
    }

    #[test]
    fn authority_gc_expires_unactivated_rows_then_removes_after_retention() {
        let (_temp, owner, share, peer, registry) = authority_fixture();
        let issued_at = now().saturating_sub(5_000);
        let grant_nonce = [0xa1; 32];
        let grant = ShareGrant::sign(
            &owner,
            share,
            peer,
            owner.public(),
            1,
            0,
            [0xa2; 32],
            Hash32::digest(b"gc-manifest"),
            Hash32::digest(b"gc-request"),
            grant_nonce,
            issued_at,
        )
        .unwrap();
        registry
            .write_grant(
                &grant_key(grant_nonce),
                &GrantRow {
                    grant: grant.clone(),
                    state: GrantState::Issued,
                    activation_id: None,
                    activation_deadline: None,
                    revoked: false,
                },
            )
            .unwrap();

        let apply_nonce = [0xa3; 32];
        let permit = ApplyPermit::sign(
            &owner,
            share,
            peer,
            1,
            [0xa4; 32],
            Hash32::digest(b"gc-root"),
            issued_at,
            apply_nonce,
        )
        .unwrap();
        registry
            .write_apply(
                &grant_key(apply_nonce),
                &ApplyRow {
                    permit: permit.clone(),
                    state: ApplyState::Prepared,
                    operation_id: None,
                    committed: false,
                    revoked: false,
                },
            )
            .unwrap();

        let expired_at = grant.expires_at.max(permit.expires_at);
        registry.gc_authority(expired_at).unwrap();
        assert_eq!(
            registry
                .read_grant(&grant_key(grant_nonce))
                .unwrap()
                .unwrap()
                .state,
            GrantState::Expired
        );
        assert_eq!(
            registry
                .read_apply(&grant_key(apply_nonce))
                .unwrap()
                .unwrap()
                .state,
            ApplyState::Expired
        );

        registry
            .gc_authority(expired_at.saturating_add(AUTHORITY_RETENTION_SECONDS + 1))
            .unwrap();
        assert!(
            registry
                .read_grant(&grant_key(grant_nonce))
                .unwrap()
                .is_none()
        );
        assert!(
            registry
                .read_apply(&grant_key(apply_nonce))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn apply_status_and_prepared_cancel_are_exact_and_replay_safe() {
        let (_temp, owner, share, peer, registry) = authority_fixture();
        let permit = ApplyPermit::sign(
            &owner,
            share,
            peer,
            1,
            [0xb1; 32],
            Hash32::digest(b"status-root"),
            now(),
            [0xb2; 32],
        )
        .unwrap();
        registry
            .write_apply(
                &grant_key(permit.nonce),
                &ApplyRow {
                    permit: permit.clone(),
                    state: ApplyState::Prepared,
                    operation_id: None,
                    committed: false,
                    revoked: false,
                },
            )
            .unwrap();
        let prepared = registry
            .apply_status(&ApplyStatusQuery::for_permit(&permit, None), peer)
            .unwrap();
        assert!(matches!(prepared.state, ApplyStateView::Prepared));
        assert_eq!(prepared.operation_id, None);

        let operation_id = [0xb3; 16];
        // The caller may know its local operation id even when the response
        // was lost before ApplyStart reached the owner.  Prepared is the one
        // state where that query remains exact by permit binding while the
        // durable row correctly reports no operation id yet.
        let prepared_with_local_id = registry
            .apply_status(
                &ApplyStatusQuery::for_permit(&permit, Some(operation_id)),
                peer,
            )
            .unwrap();
        assert_eq!(prepared_with_local_id, prepared);
        let denied = registry
            .cancel_apply(&ApplyCancel::for_permit(&permit, operation_id), peer)
            .unwrap();
        assert!(matches!(denied.state, ApplyStateView::Denied));
        assert_eq!(denied.operation_id, Some(operation_id));
        let replay = registry
            .cancel_apply(&ApplyCancel::for_permit(&permit, operation_id), peer)
            .unwrap();
        assert_eq!(replay, denied);
        let status = registry
            .apply_status(
                &ApplyStatusQuery::for_permit(&permit, Some(operation_id)),
                peer,
            )
            .unwrap();
        assert_eq!(status, denied);
        assert_eq!(
            registry
                .apply_start(
                    owner.public(),
                    share,
                    peer,
                    permit.root_hash,
                    &ApplyStart {
                        operation_id,
                        permit_nonce: permit.nonce,
                    },
                )
                .unwrap_err()
                .downcast_ref::<ShareError>(),
            Some(&ShareError::MemberRevoked)
        );

        let mut wrong = ApplyStatusQuery::for_permit(&permit, Some(operation_id));
        wrong.operation_id = Some([0xb4; 16]);
        assert_eq!(
            registry
                .apply_status(&wrong, peer)
                .unwrap_err()
                .downcast_ref::<ShareError>(),
            Some(&ShareError::GrantReplay)
        );
    }
}
