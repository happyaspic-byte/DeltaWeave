//! Durable peer authorization, one-time pairing tickets, and stable replica identity.

use std::{
    fmt, fs,
    io::Write,
    path::Path,
    str::FromStr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use deltaweave_core::{Hash32, ReplicaId};
use iroh::{EndpointId, SecretKey};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

/// Schema version of the pairing ticket encoding.
pub const PAIRING_SCHEMA_V1: u16 = 1;
/// Prefix of the printable pairing ticket code.
const TICKET_CODE_PREFIX: &str = "dwpair1";
/// Pairing ticket secrets are always 256-bit.
const TICKET_SECRET_LEN: usize = 32;

const PEERS: TableDefinition<&str, &[u8]> = TableDefinition::new("authorized_peers");
const TICKETS: TableDefinition<&str, &[u8]> = TableDefinition::new("pairing_tickets");
const IDENTITY: TableDefinition<&str, &[u8]> = TableDefinition::new("identity_state");
const STABLE_REPLICA_KEY: &str = "stable_replica";

/// Seconds since the Unix epoch.
#[must_use]
pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

/// Durable authorization and pairing state shared by protocol handlers.
pub struct AccessStore {
    database: Database,
}

impl fmt::Debug for AccessStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AccessStore(..)")
    }
}

/// One authorized remote endpoint.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PeerRecord {
    /// Authorized endpoint identifier.
    pub endpoint_id: String,
    /// Unix seconds when the peer was first paired.
    pub paired_at: u64,
}

/// Internal durable ticket state; the secret itself is never stored.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct TicketRecord {
    schema_version: u16,
    created_at: u64,
    expires_at: u64,
    uses_remaining: u32,
}

/// A one-time pairing ticket handed to a new device out of band.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PairingTicket {
    /// Ticket schema version.
    pub schema_version: u16,
    /// Server endpoint that will redeem this ticket.
    pub server_endpoint_id: String,
    /// One direct UDP address of the server.
    pub server_direct_address: String,
    /// 256-bit single-use secret.
    pub secret: [u8; TICKET_SECRET_LEN],
    /// Unix seconds after which redemption is rejected.
    pub expires_at: u64,
}

/// Outcome of a ticket redemption.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedeemOutcome {
    /// The remote peer is now authorized.
    Paired,
    /// The remote peer was already authorized.
    AlreadyPaired,
}

impl AccessStore {
    /// Opens or creates the durable access database.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let database = Database::create(path)
            .with_context(|| format!("failed to open access DB {}", path.display()))?;
        let write = database.begin_write()?;
        write.open_table(PEERS)?;
        write.open_table(TICKETS)?;
        write.open_table(IDENTITY)?;
        write.commit()?;
        Ok(Self { database })
    }

    /// Issues a single-use ticket that expires after `expires_at` Unix seconds.
    pub fn issue_ticket(
        &self,
        server_endpoint_id: &EndpointId,
        server_direct_address: &str,
        expires_at: u64,
    ) -> Result<PairingTicket> {
        let now = unix_now();
        ensure!(
            expires_at > now,
            "pairing ticket expiry must be in the future"
        );
        server_direct_address
            .parse::<std::net::SocketAddr>()
            .context("pairing ticket server address is invalid")?;
        let secret = SecretKey::generate();
        let ticket = PairingTicket {
            schema_version: PAIRING_SCHEMA_V1,
            server_endpoint_id: server_endpoint_id.to_string(),
            server_direct_address: server_direct_address.to_owned(),
            secret: secret.to_bytes(),
            expires_at,
        };
        let record = TicketRecord {
            schema_version: PAIRING_SCHEMA_V1,
            created_at: unix_now(),
            expires_at,
            uses_remaining: 1,
        };
        let encoded = postcard::to_stdvec(&record)?;
        let write = self.database.begin_write()?;
        {
            let mut tickets = write.open_table(TICKETS)?;
            tickets.insert(
                ticket_hash(&ticket.secret).to_hex().as_str(),
                encoded.as_slice(),
            )?;
        }
        write.commit()?;
        Ok(ticket)
    }

    /// Atomically consumes a ticket and authorizes `remote`.
    ///
    /// Exactly one concurrent redemption succeeds; the rest observe the consumed
    /// ticket because the check and the consume share one redb transaction.
    pub fn redeem(
        &self,
        ticket: &PairingTicket,
        expected_server: EndpointId,
        remote: EndpointId,
        now: u64,
    ) -> Result<RedeemOutcome> {
        ensure!(
            ticket.schema_version == PAIRING_SCHEMA_V1,
            "unsupported pairing ticket schema"
        );
        ensure!(
            ticket.server_endpoint_id == expected_server.to_string(),
            "pairing ticket is bound to a different server"
        );
        ensure!(now < ticket.expires_at, "pairing ticket has expired");
        let hash = ticket_hash(&ticket.secret).to_hex();
        let write = self.database.begin_write()?;
        let already = {
            let mut tickets = write.open_table(TICKETS)?;
            let record: TicketRecord = {
                let Some(value) = tickets.get(hash.as_str())? else {
                    bail!("pairing ticket is unknown or already used");
                };
                postcard::from_bytes(value.value()).context("invalid ticket record")?
            };
            ensure!(
                record.schema_version == PAIRING_SCHEMA_V1,
                "unsupported stored ticket schema"
            );
            ensure!(now < record.expires_at, "pairing ticket has expired");
            ensure!(
                record.uses_remaining >= 1,
                "pairing ticket has no uses remaining"
            );
            if record.uses_remaining == 1 {
                tickets.remove(hash.as_str())?;
            } else {
                let updated = TicketRecord {
                    uses_remaining: record.uses_remaining - 1,
                    ..record
                };
                let encoded = postcard::to_stdvec(&updated)?;
                tickets.insert(hash.as_str(), encoded.as_slice())?;
            }
            drop(tickets);

            let mut peers = write.open_table(PEERS)?;
            let key = remote.to_string();
            let already = {
                let existing = peers.get(key.as_str())?;
                existing.is_some()
            };
            if !already {
                let record = PeerRecord {
                    endpoint_id: key.clone(),
                    paired_at: now,
                };
                let encoded = postcard::to_stdvec(&record)?;
                peers.insert(key.as_str(), encoded.as_slice())?;
            }
            already
        };
        write.commit()?;
        if already {
            return Ok(RedeemOutcome::AlreadyPaired);
        }
        Ok(RedeemOutcome::Paired)
    }

    /// Authorizes a peer directly, without a ticket.
    pub fn authorize(&self, peer: EndpointId) -> Result<()> {
        self.insert_peer(peer, unix_now())
    }

    /// Removes a peer. Returns whether a peer was actually revoked.
    pub fn revoke(&self, peer: EndpointId) -> Result<bool> {
        let key = peer.to_string();
        let write = self.database.begin_write()?;
        let removed = {
            let mut peers = write.open_table(PEERS)?;
            peers.remove(key.as_str())?.is_some()
        };
        write.commit()?;
        Ok(removed)
    }

    /// Returns whether the peer is currently authorized.
    pub fn is_authorized(&self, peer: EndpointId) -> Result<bool> {
        let key = peer.to_string();
        let read = self.database.begin_read()?;
        let peers = read.open_table(PEERS)?;
        Ok(peers.get(key.as_str())?.is_some())
    }

    /// Lists every authorized peer in canonical endpoint order.
    pub fn list_peers(&self) -> Result<Vec<PeerRecord>> {
        let read = self.database.begin_read()?;
        let peers = read.open_table(PEERS)?;
        let mut records: Vec<PeerRecord> = Vec::new();
        for value in peers.iter()? {
            let (_, encoded) = value?;
            records.push(postcard::from_bytes(encoded.value()).context("invalid peer record")?);
        }
        records.sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
        Ok(records)
    }

    /// Returns the stable replica identity, creating it once from `identity`.
    ///
    /// The stored value survives transport-key rotation so existing index
    /// databases remain reusable after the endpoint key changes.
    pub fn stable_replica_id(&self, identity: &SecretKey) -> Result<ReplicaId> {
        let read = self.database.begin_read()?;
        let identity_table = read.open_table(IDENTITY)?;
        if let Some(value) = identity_table.get(STABLE_REPLICA_KEY)? {
            let bytes: [u8; 32] = value
                .value()
                .try_into()
                .map_err(|_| anyhow::anyhow!("stable replica record has wrong length"))?;
            return Ok(ReplicaId(Hash32::from_bytes(bytes)));
        }
        drop(identity_table);
        drop(read);
        let derived = Hash32::digest(identity.public().as_bytes());
        let write = self.database.begin_write()?;
        {
            let mut identity_table = write.open_table(IDENTITY)?;
            if let Some(value) = identity_table.get(STABLE_REPLICA_KEY)? {
                let bytes: [u8; 32] = value
                    .value()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("stable replica record has wrong length"))?;
                return Ok(ReplicaId(Hash32::from_bytes(bytes)));
            }
            identity_table.insert(STABLE_REPLICA_KEY, derived.as_bytes().as_slice())?;
        }
        write.commit()?;
        Ok(ReplicaId(derived))
    }

    /// Rotates the transport identity file atomically and reports both IDs.
    ///
    /// The stable replica identity is untouched, so index databases bound to it
    /// keep working. Remote peers must re-pair with the new endpoint ID.
    pub fn rotate_identity(path: &Path) -> Result<IdentityRotation> {
        let previous = crate::read_identity_public(path)?;
        let secret_key = SecretKey::generate();
        let encoded = format!("{}\n", hex::encode(secret_key.to_bytes()));
        let nonce = hex::encode(SecretKey::generate().to_bytes());
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("identity.key");
        let temporary = path.with_file_name(format!(".{file_name}.{nonce}.tmp"));
        let mut options = fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let write_result = (|| -> Result<()> {
            let mut file = options
                .open(&temporary)
                .with_context(|| format!("failed to create {}", temporary.display()))?;
            file.write_all(encoded.as_bytes())?;
            file.sync_all()?;
            Ok(())
        })();
        if let Err(error) = write_result {
            let _ = fs::remove_file(&temporary);
            return Err(error);
        }
        if let Some(parent) = path.parent()
            && let Err(error) = sync_dir(parent)
        {
            let _ = fs::remove_file(&temporary);
            return Err(error);
        }
        if let Err(error) = fs::rename(&temporary, path) {
            let _ = fs::remove_file(&temporary);
            return Err(error).with_context(|| format!("failed to replace {}", path.display()));
        }
        if let Some(parent) = path.parent() {
            let _ = sync_dir(parent);
        }
        Ok(IdentityRotation {
            previous_endpoint_id: previous,
            new_endpoint_id: secret_key.public(),
        })
    }

    fn insert_peer(&self, peer: EndpointId, paired_at: u64) -> Result<()> {
        let key = peer.to_string();
        let record = PeerRecord {
            endpoint_id: key.clone(),
            paired_at,
        };
        let encoded = postcard::to_stdvec(&record)?;
        let write = self.database.begin_write()?;
        {
            let mut peers = write.open_table(PEERS)?;
            if peers.get(key.as_str())?.is_none() {
                peers.insert(key.as_str(), encoded.as_slice())?;
            }
        }
        write.commit()?;
        Ok(())
    }
}

/// Reported endpoint IDs after an identity rotation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IdentityRotation {
    /// Endpoint ID before the rotation.
    pub previous_endpoint_id: EndpointId,
    /// Endpoint ID after the rotation.
    pub new_endpoint_id: EndpointId,
}

/// Domain-separated digest of a ticket secret.
fn ticket_hash(secret: &[u8; TICKET_SECRET_LEN]) -> Hash32 {
    let mut hasher = blake3::Hasher::new_derive_key("deltaweave pairing ticket v1");
    hasher.update(secret);
    Hash32::from_bytes(*hasher.finalize().as_bytes())
}

impl PairingTicket {
    /// Encodes the ticket as a printable single-line code.
    #[must_use]
    pub fn to_code(&self) -> String {
        let payload = postcard::to_stdvec(self).expect("pairing ticket serializes");
        format!("{TICKET_CODE_PREFIX}:{}", hex::encode(payload))
    }

    /// Parses a printable ticket code.
    pub fn from_code(code: &str) -> Result<Self> {
        let rest = code
            .strip_prefix(TICKET_CODE_PREFIX)
            .and_then(|rest| rest.strip_prefix(':'))
            .context("pairing ticket must start with dwpair1:")?;
        ensure!(
            rest.chars().all(|ch| ch.is_ascii_hexdigit()),
            "pairing ticket payload is not hex"
        );
        let bytes = hex::decode(rest).context("pairing ticket payload is not hex")?;
        let ticket: Self =
            postcard::from_bytes(&bytes).context("pairing ticket payload is invalid")?;
        ensure!(
            ticket.schema_version == PAIRING_SCHEMA_V1,
            "unsupported pairing ticket schema"
        );
        EndpointId::from_str(&ticket.server_endpoint_id)
            .map_err(|_| anyhow::anyhow!("pairing ticket has an invalid server endpoint ID"))?;
        ticket
            .server_direct_address
            .parse::<std::net::SocketAddr>()
            .context("pairing ticket has an invalid server address")?;
        Ok(ticket)
    }
}

#[cfg(unix)]
fn sync_dir(path: &Path) -> Result<()> {
    let file = fs::File::open(path)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_dir(_path: &Path) -> Result<()> {
    Ok(())
}

/// Convenience alias used by handlers that share one access store.
pub type SharedAccessStore = Arc<AccessStore>;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn store() -> (TempDir, AccessStore) {
        let temp = TempDir::new().expect("temporary directory can be created");
        let access =
            AccessStore::open(temp.path().join("access.redb")).expect("access store opens");
        (temp, access)
    }

    fn ticket(access: &AccessStore, expires_at: u64) -> (EndpointId, PairingTicket) {
        let server = SecretKey::generate().public();
        let issued = access
            .issue_ticket(&server, "127.0.0.1:11000", expires_at)
            .expect("ticket can be issued");
        (server, issued)
    }

    #[test]
    fn tickets_survive_restart_and_reject_replay() {
        let (temp, access) = store();
        let (server, issued) = ticket(&access, unix_now() + 600);
        let peer = SecretKey::generate().public();
        access
            .redeem(&issued, server, peer, unix_now())
            .expect("fresh ticket redeems");
        drop(access);

        let reopened =
            AccessStore::open(temp.path().join("access.redb")).expect("access store reopens");
        assert!(
            reopened
                .is_authorized(peer)
                .expect("authorization is readable")
        );
        assert!(
            reopened.redeem(&issued, server, peer, unix_now()).is_err(),
            "a consumed ticket cannot be redeemed again"
        );
    }

    #[test]
    fn expired_tickets_are_rejected() {
        let (_temp, access) = store();
        let now = unix_now();
        let (server, issued) = ticket(&access, now + 10);
        assert!(
            access
                .redeem(&issued, server, SecretKey::generate().public(), now + 10)
                .is_err(),
            "expiry boundary rejects redemption"
        );
        assert!(
            access
                .list_peers()
                .expect("peer list is readable")
                .is_empty()
        );
    }

    #[test]
    fn ticket_bound_to_a_different_server_is_not_consumed() {
        let (_temp, access) = store();
        let (server, issued) = ticket(&access, unix_now() + 600);
        let other = SecretKey::generate().public();
        let peer = SecretKey::generate().public();
        assert!(
            access.redeem(&issued, other, peer, unix_now()).is_err(),
            "a ticket cannot authorize a different server"
        );
        assert!(
            !access
                .is_authorized(peer)
                .expect("authorization is readable")
        );
        access
            .redeem(&issued, server, peer, unix_now())
            .expect("unconsumed ticket still redeems on the bound server");
    }

    #[test]
    fn concurrent_single_use_redemption_succeeds_exactly_once() {
        let (_temp, access) = store();
        let (server, issued) = ticket(&access, unix_now() + 600);
        let access = Arc::new(access);
        let peer = SecretKey::generate().public();

        let first = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..4)
                .map(|_| {
                    let access = Arc::clone(&access);
                    let issued = issued.clone();
                    scope.spawn(move || access.redeem(&issued, server, peer, unix_now()).is_ok())
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().expect("worker finishes"))
                .filter(|ok| *ok)
                .count()
        });
        assert_eq!(first, 1, "exactly one concurrent redemption succeeds");
        assert!(
            access.redeem(&issued, server, peer, unix_now()).is_err(),
            "the ticket is consumed after the race"
        );
    }

    #[test]
    fn revocation_takes_effect_immediately() {
        let (_temp, access) = store();
        let peer = SecretKey::generate().public();
        access.authorize(peer).expect("peer can be authorized");
        assert!(
            access
                .is_authorized(peer)
                .expect("authorization is readable")
        );
        assert!(access.revoke(peer).expect("peer can be revoked"));
        assert!(
            !access
                .is_authorized(peer)
                .expect("authorization is readable")
        );
        assert!(!access.revoke(peer).expect("second revoke reports false"));
    }

    #[test]
    fn stable_replica_survives_key_rotation() {
        let (temp, access) = store();
        let first_identity = SecretKey::generate();
        let replica = access
            .stable_replica_id(&first_identity)
            .expect("stable replica is created");
        let second_identity = SecretKey::generate();
        assert_eq!(
            access
                .stable_replica_id(&second_identity)
                .expect("stable replica is reused"),
            replica,
            "the stable replica ignores the transport key"
        );
        drop(access);

        let reopened =
            AccessStore::open(temp.path().join("access.redb")).expect("access store reopens");
        assert_eq!(
            reopened
                .stable_replica_id(&second_identity)
                .expect("stable replica survives restart"),
            replica
        );
    }

    #[test]
    fn identity_rotation_replaces_the_key_atomically() {
        let temp = TempDir::new().expect("temporary directory can be created");
        let path = temp.path().join("identity.key");
        crate::load_or_create_identity(&path).expect("identity can be created");
        let previous = crate::read_identity_public(&path).expect("old key is readable");

        let rotation = AccessStore::rotate_identity(&path).expect("rotation succeeds");
        assert_eq!(rotation.previous_endpoint_id, previous);
        assert_ne!(rotation.new_endpoint_id, previous);
        assert_eq!(
            crate::read_identity_public(&path).expect("new key is readable"),
            rotation.new_endpoint_id
        );
        assert!(
            fs::read_dir(temp.path())
                .expect("rotation directory is readable")
                .filter_map(Result::ok)
                .all(|entry| entry.path() == path),
            "the temporary key file is gone"
        );
    }

    #[test]
    fn ticket_codes_round_trip_and_reject_garbage() {
        let (_temp, access) = store();
        let (_server, issued) = ticket(&access, 4_102_444_800);
        let code = issued.to_code();
        let parsed = PairingTicket::from_code(&code).expect("code round-trips");
        assert_eq!(parsed, issued);
        for garbage in [
            "",
            "dwpair1",
            "dwpair1:only",
            "dwpair1:zzzz",
            "dwpair1:not-hex-payload",
            "dwpair1:00",
            "other1:00",
        ] {
            assert!(PairingTicket::from_code(garbage).is_err(), "{garbage:?}");
        }
    }
}
