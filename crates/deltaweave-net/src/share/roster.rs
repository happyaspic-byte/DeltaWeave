use super::{Permission, ShareError, ShareId};
use anyhow::{Result, ensure};
use iroh::{EndpointAddr, EndpointId, SecretKey, Signature};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Permission epoch copied from the authenticated owner membership.
pub type PermissionEpoch = u64;
/// One-use challenge or grant nonce.
pub type GrantNonce = [u8; 32];
/// Random identity for one signed roster/snapshot generation.
pub type SnapshotId = [u8; 32];

pub(crate) const ROSTER_VERSION: u8 = 1;
pub(crate) const ROSTER_TTL_SECONDS: u64 = 90;
pub const ROSTER_HEARTBEAT_INTERVAL_SECONDS: u64 = 30;
pub(crate) const ROSTER_STALE_AFTER_SECONDS: u64 = 90;
pub(crate) const MAX_ROSTER_ENTRIES: usize = 4096;
pub(crate) const MAX_ROSTER_FRAME_BYTES: usize = 64 * 1024;
pub(crate) const MAX_ENDPOINT_ADDRESSES: usize = 16;
pub(crate) const MAX_CLOCK_SKEW_SECONDS: u64 = 5;

const ROSTER_DOMAIN: &[u8] = b"deltaweave/share-roster/v1\0";
const HEARTBEAT_DOMAIN: &[u8] = b"deltaweave/share-heartbeat/v1\0";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RosterEntry {
    pub owner: EndpointId,
    pub share: ShareId,
    pub member: EndpointId,
    pub address: EndpointAddr,
    pub permission: Permission,
    pub member_epoch: PermissionEpoch,
    pub heartbeat_at: u64,
    pub heartbeat_expires_at: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignedRoster {
    pub owner: EndpointId,
    pub share: ShareId,
    pub generation: SnapshotId,
    pub entries: Vec<RosterEntry>,
    pub issued_at: u64,
    pub expires_at: u64,
    pub signature: Signature,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RosterHeartbeat {
    pub owner: EndpointId,
    pub share: ShareId,
    pub member: EndpointId,
    pub address: EndpointAddr,
    pub challenge: GrantNonce,
    pub sent_at: u64,
    pub signature: Signature,
}

#[derive(Serialize)]
struct RosterSigning<'a> {
    owner: EndpointId,
    share: ShareId,
    generation: SnapshotId,
    entries: &'a [RosterEntry],
    issued_at: u64,
    expires_at: u64,
}

#[derive(Serialize)]
struct HeartbeatSigning<'a> {
    owner: EndpointId,
    share: ShareId,
    member: EndpointId,
    address: &'a EndpointAddr,
    challenge: GrantNonce,
    sent_at: u64,
}

fn signing_bytes<T: Serialize>(domain: &[u8], value: &T) -> Result<Vec<u8>> {
    let mut bytes = domain.to_vec();
    bytes.extend(postcard::to_stdvec(value)?);
    Ok(bytes)
}

fn roster_payload(roster: &SignedRoster) -> RosterSigning<'_> {
    RosterSigning {
        owner: roster.owner,
        share: roster.share,
        generation: roster.generation,
        entries: &roster.entries,
        issued_at: roster.issued_at,
        expires_at: roster.expires_at,
    }
}

fn heartbeat_payload(heartbeat: &RosterHeartbeat) -> HeartbeatSigning<'_> {
    HeartbeatSigning {
        owner: heartbeat.owner,
        share: heartbeat.share,
        member: heartbeat.member,
        address: &heartbeat.address,
        challenge: heartbeat.challenge,
        sent_at: heartbeat.sent_at,
    }
}

impl SignedRoster {
    pub(crate) fn sign(
        key: &SecretKey,
        share: ShareId,
        generation: SnapshotId,
        entries: Vec<RosterEntry>,
        issued_at: u64,
        expires_at: u64,
    ) -> Result<Self> {
        let roster = Self {
            owner: key.public(),
            share,
            generation,
            entries,
            issued_at,
            expires_at,
            signature: key.sign(b""),
        };
        let signature = key.sign(&signing_bytes(ROSTER_DOMAIN, &roster_payload(&roster))?);
        let roster = Self {
            signature,
            ..roster
        };
        roster.validate_shape()?;
        Ok(roster)
    }

    fn validate_shape(&self) -> Result<()> {
        ensure!(
            self.entries.len() <= MAX_ROSTER_ENTRIES,
            ShareError::Protocol
        );
        ensure!(self.expires_at > self.issued_at, ShareError::Protocol);
        ensure!(
            self.expires_at - self.issued_at <= ROSTER_TTL_SECONDS,
            ShareError::Protocol
        );
        ensure!(
            postcard::to_stdvec(self)?.len() <= MAX_ROSTER_FRAME_BYTES,
            ShareError::Protocol
        );
        for entry in &self.entries {
            ensure!(entry.member_epoch > 0, ShareError::EpochMismatch);
            ensure!(
                entry.owner == self.owner
                    && entry.share == self.share
                    && entry.member != self.owner
                    && entry.address.id == entry.member
                    && entry.address.addrs.len() <= MAX_ENDPOINT_ADDRESSES
                    && ((entry.heartbeat_at == 0 && entry.heartbeat_expires_at == 0)
                        || (entry.heartbeat_at > 0
                            && entry.heartbeat_expires_at > entry.heartbeat_at
                            && entry.heartbeat_expires_at
                                <= entry
                                    .heartbeat_at
                                    .saturating_add(ROSTER_STALE_AFTER_SECONDS))),
                ShareError::EndpointMismatch
            );
        }
        let mut members = BTreeSet::new();
        for entry in &self.entries {
            ensure!(members.insert(entry.member), ShareError::Protocol);
        }
        Ok(())
    }

    /// Verifies the owner signature and bounded lifetime without treating a
    /// roster row or address as an enrollment/permission authority.
    pub fn verify_at(&self, now: u64) -> Result<()> {
        self.verify_signature()?;
        ensure!(
            self.issued_at <= now.saturating_add(MAX_CLOCK_SKEW_SECONDS),
            ShareError::ClockRollback
        );
        ensure!(self.expires_at > now, ShareError::RosterStale);
        Ok(())
    }

    /// Verifies a roster against the owner/share binding already authenticated
    /// by the caller. The embedded owner is a signing key, not the caller's
    /// intended share authority.
    pub fn verify_for(&self, owner: EndpointId, share: ShareId, now: u64) -> Result<()> {
        ensure!(self.owner == owner, ShareError::OwnerMismatch);
        ensure!(self.share == share, ShareError::OwnerMismatch);
        self.verify_at(now)
    }

    pub(crate) fn verify_signature(&self) -> Result<()> {
        self.validate_shape()?;
        self.owner
            .verify(
                &signing_bytes(ROSTER_DOMAIN, &roster_payload(self))?,
                &self.signature,
            )
            .map_err(|_| anyhow::Error::new(ShareError::Protocol))?;
        Ok(())
    }

    #[must_use]
    pub fn member(&self, member: EndpointId) -> Option<&RosterEntry> {
        self.entries.iter().find(|entry| entry.member == member)
    }

    #[must_use]
    pub fn member_is_fresh(&self, member: EndpointId, now: u64) -> bool {
        self.member(member)
            .is_some_and(|entry| entry.heartbeat_expires_at > now)
    }

    /// Returns only entries whose owner-received heartbeat is still live.
    /// Stale entries remain signed for observability but are never eligible
    /// for provider selection.
    #[must_use]
    pub fn fresh_members(&self, now: u64) -> Vec<&RosterEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.heartbeat_expires_at > now)
            .collect()
    }
}

impl RosterHeartbeat {
    pub(crate) fn sign(
        key: &SecretKey,
        owner: EndpointId,
        share: ShareId,
        address: EndpointAddr,
        challenge: GrantNonce,
        sent_at: u64,
    ) -> Self {
        let heartbeat = Self {
            owner,
            share,
            member: key.public(),
            address,
            challenge,
            sent_at,
            signature: key.sign(b""),
        };
        let signature = key.sign(
            &signing_bytes(HEARTBEAT_DOMAIN, &heartbeat_payload(&heartbeat))
                .expect("heartbeat payload is serializable"),
        );
        Self {
            signature,
            ..heartbeat
        }
    }

    pub(crate) fn verify_at(&self, now: u64) -> Result<()> {
        ensure!(self.address.id == self.member, ShareError::EndpointMismatch);
        ensure!(
            self.address.addrs.len() <= MAX_ENDPOINT_ADDRESSES,
            ShareError::EndpointMismatch
        );
        ensure!(
            self.sent_at <= now.saturating_add(MAX_CLOCK_SKEW_SECONDS)
                && now <= self.sent_at.saturating_add(ROSTER_STALE_AFTER_SECONDS),
            ShareError::HeartbeatExpired
        );
        ensure!(
            postcard::to_stdvec(self)?.len() <= MAX_ROSTER_FRAME_BYTES,
            ShareError::Protocol
        );
        self.member
            .verify(
                &signing_bytes(HEARTBEAT_DOMAIN, &heartbeat_payload(self))?,
                &self.signature,
            )
            .map_err(|_| anyhow::Error::new(ShareError::Protocol))?;
        Ok(())
    }
}

pub(crate) fn random_nonce() -> GrantNonce {
    super::ticket::random_bytes()
}

pub(crate) fn heartbeat_expiry(sent_at: u64) -> u64 {
    sent_at.saturating_add(ROSTER_STALE_AFTER_SECONDS)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(
        owner: EndpointId,
        share: ShareId,
        member: EndpointId,
        epoch: u64,
        heartbeat_at: u64,
        heartbeat_expires_at: u64,
    ) -> RosterEntry {
        RosterEntry {
            owner,
            share,
            member,
            address: EndpointAddr::new(member),
            permission: Permission::ReadWrite,
            member_epoch: epoch,
            heartbeat_at,
            heartbeat_expires_at,
        }
    }

    #[test]
    fn verify_for_binds_trusted_owner_share_and_filters_stale_members() {
        let owner_key = SecretKey::generate();
        let other_owner = SecretKey::generate();
        let share = ShareId([0x11; 32]);
        let member = SecretKey::generate().public();
        let roster = SignedRoster::sign(
            &owner_key,
            share,
            [1; 32],
            vec![entry(owner_key.public(), share, member, 1, 10, 20)],
            10,
            100,
        )
        .unwrap();

        roster.verify_for(owner_key.public(), share, 50).unwrap();
        assert!(roster.fresh_members(50).is_empty());
        assert!(!roster.member_is_fresh(member, 50));
        assert_eq!(
            roster
                .verify_for(other_owner.public(), share, 50)
                .unwrap_err()
                .downcast_ref::<ShareError>(),
            Some(&ShareError::OwnerMismatch)
        );
        assert_eq!(
            roster
                .verify_for(owner_key.public(), ShareId([0x22; 32]), 50)
                .unwrap_err()
                .downcast_ref::<ShareError>(),
            Some(&ShareError::OwnerMismatch)
        );
    }

    #[test]
    fn roster_shape_rejects_future_clock_zero_epoch_and_duplicate_members() {
        let owner_key = SecretKey::generate();
        let share = ShareId([0x33; 32]);
        let member = SecretKey::generate().public();
        let future = SignedRoster::sign(
            &owner_key,
            share,
            [2; 32],
            vec![entry(owner_key.public(), share, member, 1, 0, 0)],
            1_006,
            1_090,
        )
        .unwrap();
        assert_eq!(
            future
                .verify_at(1_000)
                .unwrap_err()
                .downcast_ref::<ShareError>(),
            Some(&ShareError::ClockRollback)
        );

        assert!(
            SignedRoster::sign(
                &owner_key,
                share,
                [3; 32],
                vec![entry(owner_key.public(), share, member, 0, 0, 0)],
                1_000,
                1_090,
            )
            .unwrap_err()
            .downcast_ref::<ShareError>()
            .is_some_and(|error| *error == ShareError::EpochMismatch)
        );
        assert!(
            SignedRoster::sign(
                &owner_key,
                share,
                [4; 32],
                vec![
                    entry(owner_key.public(), share, member, 1, 0, 0),
                    entry(owner_key.public(), share, member, 1, 0, 0),
                ],
                1_000,
                1_090,
            )
            .unwrap_err()
            .downcast_ref::<ShareError>()
            .is_some_and(|error| *error == ShareError::Protocol)
        );
    }

    #[test]
    fn heartbeat_rejects_wrong_address_identity_stale_and_bad_signature() {
        let owner_key = SecretKey::generate();
        let member_key = SecretKey::generate();
        let share = ShareId([0x44; 32]);
        let challenge = [9; 32];
        let wrong_address = EndpointAddr::new(SecretKey::generate().public());
        let wrong_identity = RosterHeartbeat::sign(
            &member_key,
            owner_key.public(),
            share,
            wrong_address,
            challenge,
            100,
        );
        assert_eq!(
            wrong_identity
                .verify_at(100)
                .unwrap_err()
                .downcast_ref::<ShareError>(),
            Some(&ShareError::EndpointMismatch)
        );

        let stale = RosterHeartbeat::sign(
            &member_key,
            owner_key.public(),
            share,
            EndpointAddr::new(member_key.public()),
            challenge,
            1,
        );
        assert_eq!(
            stale
                .verify_at(100)
                .unwrap_err()
                .downcast_ref::<ShareError>(),
            Some(&ShareError::HeartbeatExpired)
        );

        let mut bad_signature = RosterHeartbeat::sign(
            &member_key,
            owner_key.public(),
            share,
            EndpointAddr::new(member_key.public()),
            challenge,
            100,
        );
        bad_signature.signature = SecretKey::generate().sign(b"not this heartbeat");
        assert_eq!(
            bad_signature
                .verify_at(100)
                .unwrap_err()
                .downcast_ref::<ShareError>(),
            Some(&ShareError::Protocol)
        );
    }
}
