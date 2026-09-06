use super::{ShareError, now};
use deltaweave_core::{Hash32, ReplicaId};
use iroh::{EndpointAddr, EndpointId, SecretKey, Signature};
use serde::{Deserialize, Serialize};
use std::fmt;

const PREFIX: &str = "dwshare3:";
const DOMAIN: &[u8] = b"deltaweave/share/invitation/v3\0";
const PROOF_DOMAIN: &[u8] = b"deltaweave/share/legacy-claim/v3\0";
const MAX_TICKET_BYTES: usize = 8192;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct ShareId(pub [u8; 32]);
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct InvitationId(pub [u8; 32]);
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    ReadOnly,
    ReadWrite,
}

/// Verified signature metadata. This does not assert current online issuance validity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TicketPreview {
    pub share_id: ShareId,
    pub owner: EndpointId,
    pub name: String,
    pub permission: Permission,
    pub invitation_id: InvitationId,
    pub expires_at: Option<u64>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct TicketBody {
    pub version: u8,
    pub share_id: ShareId,
    pub owner: EndpointId,
    pub name: String,
    pub permission: Permission,
    pub invitation_id: InvitationId,
    pub bearer: [u8; 32],
    pub issued_at: u64,
    pub expires_at: Option<u64>,
    pub address: EndpointAddr,
}

/// A bearer credential. Encoding is explicit; Debug never prints credential bytes.
#[derive(Clone, Serialize, Deserialize)]
pub struct ShareTicket {
    pub(crate) body: TicketBody,
    signature: Signature,
}
impl fmt::Debug for ShareTicket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShareTicket")
            .field("credential", &"[REDACTED]")
            .finish()
    }
}

pub(crate) fn random_bytes() -> [u8; 32] {
    SecretKey::generate().to_bytes()
}
fn signed_bytes<T: Serialize>(domain: &[u8], value: &T) -> Result<Vec<u8>, ShareError> {
    let mut bytes = domain.to_vec();
    bytes.extend(postcard::to_stdvec(value).map_err(|_| ShareError::InvalidTicket)?);
    Ok(bytes)
}

impl ShareTicket {
    pub(crate) fn issue(
        key: &SecretKey,
        share_id: ShareId,
        name: String,
        permission: Permission,
        expires_at: Option<u64>,
        address: EndpointAddr,
        issued_at: u64,
    ) -> Result<Self, ShareError> {
        let body = TicketBody {
            version: 3,
            share_id,
            owner: key.public(),
            name,
            permission,
            invitation_id: InvitationId(random_bytes()),
            bearer: random_bytes(),
            issued_at,
            expires_at,
            address,
        };
        let signature = key.sign(&signed_bytes(DOMAIN, &body)?);
        let ticket = Self { body, signature };
        ticket.verify_at(issued_at)?;
        Ok(ticket)
    }
    pub fn encode(&self) -> String {
        format!(
            "{PREFIX}{}",
            hex::encode(postcard::to_stdvec(self).expect("ticket serialization"))
        )
    }
    pub fn parse(encoded: &str) -> Result<Self, ShareError> {
        Self::parse_at(encoded, now())
    }
    pub fn parse_at(encoded: &str, now: u64) -> Result<Self, ShareError> {
        if encoded.len() > MAX_TICKET_BYTES * 2 + PREFIX.len() {
            return Err(ShareError::InvalidTicket);
        }
        let raw = encoded
            .strip_prefix(PREFIX)
            .ok_or(ShareError::UnsupportedVersion)?;
        let bytes = hex::decode(raw).map_err(|_| ShareError::InvalidTicket)?;
        let ticket: Self = postcard::from_bytes(&bytes).map_err(|_| ShareError::InvalidTicket)?;
        if postcard::to_stdvec(&ticket).map_err(|_| ShareError::InvalidTicket)? != bytes {
            return Err(ShareError::InvalidTicket);
        }
        ticket.verify_at(now)?;
        Ok(ticket)
    }
    pub(crate) fn verify_at(&self, now: u64) -> Result<(), ShareError> {
        if self.body.version != 3 {
            return Err(ShareError::UnsupportedVersion);
        }
        if self.body.name.is_empty()
            || self.body.name.len() > 255
            || self.body.name.chars().any(char::is_control)
            || self.body.address.id != self.body.owner
            || self.body.address.addrs.len() > 16
            || postcard::to_stdvec(self)
                .map_err(|_| ShareError::InvalidTicket)?
                .len()
                > MAX_TICKET_BYTES
        {
            return Err(ShareError::InvalidTicket);
        }
        self.body
            .owner
            .verify(&signed_bytes(DOMAIN, &self.body)?, &self.signature)
            .map_err(|_| ShareError::InvalidTicket)?;
        if self
            .body
            .expires_at
            .is_some_and(|expiry| expiry <= now || expiry <= self.body.issued_at)
        {
            return Err(ShareError::Expired);
        }
        Ok(())
    }
    pub fn preview(&self) -> TicketPreview {
        TicketPreview {
            share_id: self.body.share_id,
            owner: self.body.owner,
            name: self.body.name.clone(),
            permission: self.body.permission,
            invitation_id: self.body.invitation_id,
            expires_at: self.body.expires_at,
        }
    }
    pub fn address(&self) -> EndpointAddr {
        self.body.address.clone()
    }
    pub(crate) fn issuance_digest(&self) -> Result<Hash32, ShareError> {
        Ok(Hash32::digest(&signed_bytes(DOMAIN, &self.body)?))
    }
}

/// A retained-key continuity claim. The legacy private key never leaves this device.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LegacyProof {
    version: u8,
    invitation: InvitationId,
    share: ShareId,
    owner: EndpointId,
    new_endpoint: EndpointId,
    old_endpoint: EndpointId,
    replica: ReplicaId,
    signature: Signature,
}
impl LegacyProof {
    pub fn create(
        ticket: &ShareTicket,
        old_key: &SecretKey,
        new_endpoint: EndpointId,
        replica: ReplicaId,
    ) -> Result<Self, ShareError> {
        if replica != ReplicaId(Hash32::digest(old_key.public().as_bytes())) {
            return Err(ShareError::ReplicaClaimRejected);
        }
        let mut proof = Self {
            version: 3,
            invitation: ticket.body.invitation_id,
            share: ticket.body.share_id,
            owner: ticket.body.owner,
            new_endpoint,
            old_endpoint: old_key.public(),
            replica,
            signature: old_key.sign(b""),
        };
        proof.signature = old_key.sign(&proof.message()?);
        Ok(proof)
    }
    fn message(&self) -> Result<Vec<u8>, ShareError> {
        signed_bytes(
            PROOF_DOMAIN,
            &(
                self.version,
                self.invitation,
                self.share,
                self.owner,
                self.new_endpoint,
                self.old_endpoint,
                self.replica,
            ),
        )
    }
    pub(crate) fn verify(
        &self,
        ticket: &ShareTicket,
        peer: EndpointId,
    ) -> Result<ReplicaId, ShareError> {
        if self.version != 3
            || self.invitation != ticket.body.invitation_id
            || self.share != ticket.body.share_id
            || self.owner != ticket.body.owner
            || self.new_endpoint != peer
            || self.old_endpoint == peer
            || self.replica != ReplicaId(Hash32::digest(self.old_endpoint.as_bytes()))
        {
            return Err(ShareError::ReplicaClaimRejected);
        }
        self.old_endpoint
            .verify(&self.message()?, &self.signature)
            .map_err(|_| ShareError::ReplicaClaimRejected)?;
        Ok(self.replica)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tickets_verify_canonical_signature_expiry_and_redact_secrets() {
        let key = SecretKey::generate();
        let ticket = ShareTicket::issue(
            &key,
            ShareId([1; 32]),
            "Documents".into(),
            Permission::ReadOnly,
            Some(200),
            EndpointAddr::new(key.public()),
            100,
        )
        .unwrap();
        let encoded = ticket.encode();
        let parsed = ShareTicket::parse_at(&encoded, 150).unwrap();
        assert_eq!(parsed.preview().permission, Permission::ReadOnly);
        assert_eq!(parsed.preview().name, "Documents");
        assert!(!format!("{ticket:?}").contains(&hex::encode(ticket.body.bearer)));
        assert_eq!(
            ShareTicket::parse_at(&encoded, 200).unwrap_err(),
            ShareError::Expired
        );
        let mut corrupt = ticket.clone();
        corrupt.body.permission = Permission::ReadWrite;
        assert_eq!(
            ShareTicket::parse_at(&corrupt.encode(), 150).unwrap_err(),
            ShareError::InvalidTicket
        );
        corrupt = ticket.clone();
        corrupt.body.version = 4;
        assert_eq!(
            ShareTicket::parse_at(&corrupt.encode(), 150).unwrap_err(),
            ShareError::UnsupportedVersion
        );
        assert!(ShareTicket::parse_at(&(encoded + "00"), 150).is_err());
        assert!(ShareTicket::parse_at(&"x".repeat(20000), 150).is_err());
    }
    #[test]
    fn legacy_proof_is_bound_to_every_claim_context_field() {
        let owner = SecretKey::generate();
        let old = SecretKey::generate();
        let peer = SecretKey::generate().public();
        let ticket = ShareTicket::issue(
            &owner,
            ShareId([1; 32]),
            "Files".into(),
            Permission::ReadWrite,
            None,
            EndpointAddr::new(owner.public()),
            now(),
        )
        .unwrap();
        let replica = ReplicaId(Hash32::digest(old.public().as_bytes()));
        let proof = LegacyProof::create(&ticket, &old, peer, replica).unwrap();
        assert_eq!(proof.verify(&ticket, peer).unwrap(), replica);
        let mut attacks = Vec::new();
        let mut p = proof.clone();
        p.version = 2;
        attacks.push(p);
        let mut p = proof.clone();
        p.invitation = InvitationId([9; 32]);
        attacks.push(p);
        let mut p = proof.clone();
        p.share = ShareId([9; 32]);
        attacks.push(p);
        let mut p = proof.clone();
        p.owner = SecretKey::generate().public();
        attacks.push(p);
        let mut p = proof.clone();
        p.new_endpoint = SecretKey::generate().public();
        attacks.push(p);
        let mut p = proof.clone();
        p.old_endpoint = SecretKey::generate().public();
        attacks.push(p);
        let mut p = proof.clone();
        p.replica = ReplicaId(Hash32::digest(b"victim"));
        attacks.push(p);
        let mut p = proof.clone();
        p.signature = SecretKey::generate().sign(b"wrong");
        attacks.push(p);
        for attack in attacks {
            assert_eq!(
                attack.verify(&ticket, peer).unwrap_err(),
                ShareError::ReplicaClaimRejected
            );
        }
    }
}
