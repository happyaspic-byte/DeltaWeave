use super::{GrantNonce, PermissionEpoch, ShareError, ShareId, SnapshotId};
use anyhow::{Result, ensure};
use deltaweave_core::{FileManifest, Hash32, SyncEntryKind, SyncRecord};
use deltaweave_reconcile::MerkleTree;
use iroh::{EndpointId, SecretKey, Signature};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub(crate) const AUTHORITY_VERSION: u8 = 1;
pub(crate) const MAX_SNAPSHOT_TTL_SECONDS: u64 = 90;
pub(crate) const MAX_MANIFEST_TTL_SECONDS: u64 = 90;
pub(crate) const MAX_GRANT_TTL_SECONDS: u64 = 120;
pub(crate) const MAX_APPLY_TTL_SECONDS: u64 = 10;
pub(crate) const MAX_ACTIVATE_TTL_SECONDS: u16 = 15;
pub(crate) const MAX_REQUEST_HASHES: usize = 64;
pub(crate) const MAX_SNAPSHOT_RECORDS: usize = 1_000_000;
pub(crate) const MAX_AUTHORITY_FRAME_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_CLOCK_SKEW_SECONDS: u64 = 5;

const SNAPSHOT_DOMAIN: &[u8] = b"deltaweave/share-snapshot/v1\0";
const MANIFEST_DOMAIN: &[u8] = b"deltaweave/share-manifest/v1\0";
const GRANT_DOMAIN: &[u8] = b"deltaweave/share-grant/v1\0";
const ACTIVATE_DOMAIN: &[u8] = b"deltaweave/share-activate/v1\0";
const APPLY_DOMAIN: &[u8] = b"deltaweave/share-apply/v1\0";
const REQUEST_DOMAIN: &[u8] = b"deltaweave/share-swarm/request/v1\0";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SnapshotToken {
    pub version: u8,
    pub owner: EndpointId,
    pub share: ShareId,
    pub epoch: PermissionEpoch,
    pub snapshot: SnapshotId,
    pub root_hash: Hash32,
    pub record_count: u32,
    pub issued_at: u64,
    pub expires_at: u64,
    pub signature: Signature,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthoritativeSnapshot {
    pub token: SnapshotToken,
    pub records: Vec<SyncRecord>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ManifestAttestation {
    pub version: u8,
    pub owner: EndpointId,
    pub share: ShareId,
    pub epoch: PermissionEpoch,
    pub snapshot: SnapshotId,
    pub record_hash: Hash32,
    pub manifest: FileManifest,
    pub manifest_hash: Hash32,
    pub issued_at: u64,
    pub expires_at: u64,
    pub signature: Signature,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ShareGrant {
    pub version: u8,
    pub owner: EndpointId,
    pub share: ShareId,
    pub consumer: EndpointId,
    pub provider: EndpointId,
    pub epoch: PermissionEpoch,
    pub provider_epoch: PermissionEpoch,
    pub snapshot: SnapshotId,
    pub manifest: Hash32,
    pub request_hash: Hash32,
    pub issued_at: u64,
    pub expires_at: u64,
    pub nonce: GrantNonce,
    pub signature: Signature,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActivateGrantRequest {
    pub owner: EndpointId,
    pub share: ShareId,
    pub consumer: EndpointId,
    pub provider: EndpointId,
    pub epoch: PermissionEpoch,
    pub provider_epoch: PermissionEpoch,
    pub manifest: Hash32,
    pub request_hash: Hash32,
    pub nonce: GrantNonce,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActivateGrantReply {
    pub share: ShareId,
    pub provider: EndpointId,
    pub nonce: GrantNonce,
    pub activation_id: [u8; 16],
    pub accepted: bool,
    pub max_duration_secs: u16,
    pub signature: Signature,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ApplyPermit {
    pub version: u8,
    pub owner: EndpointId,
    pub share: ShareId,
    pub consumer: EndpointId,
    pub epoch: PermissionEpoch,
    pub snapshot: SnapshotId,
    pub root_hash: Hash32,
    pub issued_at: u64,
    pub expires_at: u64,
    pub nonce: GrantNonce,
    pub signature: Signature,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ApplyStart {
    pub operation_id: [u8; 16],
    pub permit_nonce: GrantNonce,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ApplyDrained {
    pub operation_id: [u8; 16],
    pub permit_nonce: GrantNonce,
    pub committed: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RevocationReceipt {
    Complete {
        member_epoch: PermissionEpoch,
        completed_at: u64,
    },
    Pending {
        member_epoch: PermissionEpoch,
        deadline: u64,
        blockers: u32,
    },
}

#[derive(Serialize)]
struct SnapshotSigning {
    version: u8,
    owner: EndpointId,
    share: ShareId,
    epoch: PermissionEpoch,
    snapshot: SnapshotId,
    root_hash: Hash32,
    record_count: u32,
    issued_at: u64,
    expires_at: u64,
}

#[derive(Serialize)]
struct ManifestSigning<'a> {
    version: u8,
    owner: EndpointId,
    share: ShareId,
    epoch: PermissionEpoch,
    snapshot: SnapshotId,
    record_hash: Hash32,
    manifest: &'a FileManifest,
    manifest_hash: Hash32,
    issued_at: u64,
    expires_at: u64,
}

#[derive(Serialize)]
struct GrantSigning {
    version: u8,
    owner: EndpointId,
    share: ShareId,
    consumer: EndpointId,
    provider: EndpointId,
    epoch: PermissionEpoch,
    provider_epoch: PermissionEpoch,
    snapshot: SnapshotId,
    manifest: Hash32,
    request_hash: Hash32,
    issued_at: u64,
    expires_at: u64,
    nonce: GrantNonce,
}

#[allow(dead_code)]
#[derive(Serialize)]
struct ActivateSigning {
    owner: EndpointId,
    share: ShareId,
    consumer: EndpointId,
    provider: EndpointId,
    epoch: PermissionEpoch,
    provider_epoch: PermissionEpoch,
    manifest: Hash32,
    request_hash: Hash32,
    nonce: GrantNonce,
}

#[derive(Serialize)]
struct ActivateReplySigning {
    share: ShareId,
    provider: EndpointId,
    nonce: GrantNonce,
    activation_id: [u8; 16],
    accepted: bool,
    max_duration_secs: u16,
}

#[derive(Serialize)]
struct ApplySigning {
    version: u8,
    owner: EndpointId,
    share: ShareId,
    consumer: EndpointId,
    epoch: PermissionEpoch,
    snapshot: SnapshotId,
    root_hash: Hash32,
    issued_at: u64,
    expires_at: u64,
    nonce: GrantNonce,
}

#[derive(Serialize)]
struct RequestHash<'a> {
    share: ShareId,
    snapshot: SnapshotId,
    manifest: Hash32,
    hashes: &'a [Hash32],
}

fn signing_bytes<T: Serialize>(domain: &[u8], value: &T) -> Result<Vec<u8>> {
    let mut bytes = domain.to_vec();
    bytes.extend(postcard::to_stdvec(value)?);
    Ok(bytes)
}

fn verify_signature<T: Serialize>(
    owner: EndpointId,
    domain: &[u8],
    payload: &T,
    signature: &Signature,
) -> Result<()> {
    owner
        .verify(&signing_bytes(domain, payload)?, signature)
        .map_err(|_| anyhow::Error::new(ShareError::Protocol))
}

fn max_expiry(issued_at: u64, maximum: u64, expires_at: u64) -> bool {
    expires_at > issued_at && expires_at - issued_at <= maximum
}

fn check_issued_at(issued_at: u64, now: u64) -> Result<()> {
    ensure!(
        issued_at <= now.saturating_add(MAX_CLOCK_SKEW_SECONDS),
        ShareError::ClockRollback
    );
    Ok(())
}

fn snapshot_payload(token: &SnapshotToken) -> SnapshotSigning {
    SnapshotSigning {
        version: token.version,
        owner: token.owner,
        share: token.share,
        epoch: token.epoch,
        snapshot: token.snapshot,
        root_hash: token.root_hash,
        record_count: token.record_count,
        issued_at: token.issued_at,
        expires_at: token.expires_at,
    }
}

fn manifest_payload(attestation: &ManifestAttestation) -> ManifestSigning<'_> {
    ManifestSigning {
        version: attestation.version,
        owner: attestation.owner,
        share: attestation.share,
        epoch: attestation.epoch,
        snapshot: attestation.snapshot,
        record_hash: attestation.record_hash,
        manifest: &attestation.manifest,
        manifest_hash: attestation.manifest_hash,
        issued_at: attestation.issued_at,
        expires_at: attestation.expires_at,
    }
}

fn grant_payload(grant: &ShareGrant) -> GrantSigning {
    GrantSigning {
        version: grant.version,
        owner: grant.owner,
        share: grant.share,
        consumer: grant.consumer,
        provider: grant.provider,
        epoch: grant.epoch,
        provider_epoch: grant.provider_epoch,
        snapshot: grant.snapshot,
        manifest: grant.manifest,
        request_hash: grant.request_hash,
        issued_at: grant.issued_at,
        expires_at: grant.expires_at,
        nonce: grant.nonce,
    }
}

#[allow(dead_code)]
fn activate_payload(request: &ActivateGrantRequest) -> ActivateSigning {
    ActivateSigning {
        owner: request.owner,
        share: request.share,
        consumer: request.consumer,
        provider: request.provider,
        epoch: request.epoch,
        provider_epoch: request.provider_epoch,
        manifest: request.manifest,
        request_hash: request.request_hash,
        nonce: request.nonce,
    }
}

fn activate_reply_payload(reply: &ActivateGrantReply) -> ActivateReplySigning {
    ActivateReplySigning {
        share: reply.share,
        provider: reply.provider,
        nonce: reply.nonce,
        activation_id: reply.activation_id,
        accepted: reply.accepted,
        max_duration_secs: reply.max_duration_secs,
    }
}

fn apply_payload(permit: &ApplyPermit) -> ApplySigning {
    ApplySigning {
        version: permit.version,
        owner: permit.owner,
        share: permit.share,
        consumer: permit.consumer,
        epoch: permit.epoch,
        snapshot: permit.snapshot,
        root_hash: permit.root_hash,
        issued_at: permit.issued_at,
        expires_at: permit.expires_at,
        nonce: permit.nonce,
    }
}

impl SnapshotToken {
    pub(crate) fn sign(
        key: &SecretKey,
        share: ShareId,
        epoch: PermissionEpoch,
        snapshot: SnapshotId,
        root_hash: Hash32,
        record_count: usize,
        issued_at: u64,
    ) -> Result<Self> {
        let record_count = u32::try_from(record_count).map_err(|_| ShareError::Busy)?;
        let token = Self {
            version: AUTHORITY_VERSION,
            owner: key.public(),
            share,
            epoch,
            snapshot,
            root_hash,
            record_count,
            issued_at,
            expires_at: issued_at.saturating_add(MAX_SNAPSHOT_TTL_SECONDS),
            signature: key.sign(b""),
        };
        let signature = key.sign(&signing_bytes(SNAPSHOT_DOMAIN, &snapshot_payload(&token))?);
        let token = Self { signature, ..token };
        token.validate_shape()?;
        Ok(token)
    }

    fn validate_shape(&self) -> Result<()> {
        ensure!(
            self.version == AUTHORITY_VERSION,
            ShareError::UnsupportedVersion
        );
        ensure!(self.epoch > 0, ShareError::EpochMismatch);
        ensure!(
            self.record_count as usize <= MAX_SNAPSHOT_RECORDS,
            ShareError::Busy
        );
        ensure!(
            max_expiry(self.issued_at, MAX_SNAPSHOT_TTL_SECONDS, self.expires_at),
            ShareError::Protocol
        );
        ensure!(
            postcard::to_stdvec(self)?.len() <= MAX_AUTHORITY_FRAME_BYTES,
            ShareError::Protocol
        );
        Ok(())
    }

    pub fn verify_for(&self, owner: EndpointId, share: ShareId, now: u64) -> Result<()> {
        ensure!(self.owner == owner, ShareError::OwnerMismatch);
        ensure!(self.share == share, ShareError::OwnerMismatch);
        self.validate_shape()?;
        verify_signature(
            self.owner,
            SNAPSHOT_DOMAIN,
            &snapshot_payload(self),
            &self.signature,
        )?;
        check_issued_at(self.issued_at, now)?;
        ensure!(self.expires_at > now, ShareError::GrantExpired);
        Ok(())
    }
}

impl AuthoritativeSnapshot {
    pub fn verify_complete(&self, owner: EndpointId, share: ShareId, now: u64) -> Result<()> {
        self.token.verify_for(owner, share, now)?;
        ensure!(
            self.records.len() == self.token.record_count as usize,
            ShareError::ManifestMismatch
        );
        ensure!(self.records.len() <= MAX_SNAPSHOT_RECORDS, ShareError::Busy);
        for window in self.records.windows(2) {
            ensure!(window[0].path < window[1].path, ShareError::InvalidRecord);
        }
        for record in &self.records {
            record.validate()?;
        }
        let tree = MerkleTree::from_records(self.records.clone())?;
        ensure!(
            tree.root_hash() == self.token.root_hash,
            ShareError::ManifestMismatch
        );
        ensure!(
            postcard::to_stdvec(self)?.len() <= MAX_AUTHORITY_FRAME_BYTES,
            ShareError::Protocol
        );
        Ok(())
    }
}

impl ManifestAttestation {
    pub(crate) fn sign(
        key: &SecretKey,
        snapshot: &SnapshotToken,
        record: &SyncRecord,
        manifest: FileManifest,
        issued_at: u64,
    ) -> Result<Self> {
        record.validate()?;
        ensure!(!record.tombstone, ShareError::ManifestMismatch);
        ensure!(
            record.kind == SyncEntryKind::File,
            ShareError::ManifestMismatch
        );
        let content_hash = record.content_hash.ok_or(ShareError::ManifestMismatch)?;
        manifest.validate()?;
        ensure!(
            manifest.file_hash == content_hash,
            ShareError::ManifestMismatch
        );
        ensure!(manifest.size == record.size, ShareError::ManifestMismatch);
        let attestation = Self {
            version: AUTHORITY_VERSION,
            owner: key.public(),
            share: snapshot.share,
            epoch: snapshot.epoch,
            snapshot: snapshot.snapshot,
            record_hash: record.logical_hash(),
            manifest_hash: manifest.manifest_hash(),
            manifest,
            issued_at,
            expires_at: issued_at.saturating_add(MAX_MANIFEST_TTL_SECONDS),
            signature: key.sign(b""),
        };
        let signature = key.sign(&signing_bytes(
            MANIFEST_DOMAIN,
            &manifest_payload(&attestation),
        )?);
        let attestation = Self {
            signature,
            ..attestation
        };
        attestation.validate_shape()?;
        Ok(attestation)
    }

    fn validate_shape(&self) -> Result<()> {
        ensure!(
            self.version == AUTHORITY_VERSION,
            ShareError::UnsupportedVersion
        );
        ensure!(self.epoch > 0, ShareError::EpochMismatch);
        self.manifest.validate()?;
        ensure!(
            self.manifest_hash == self.manifest.manifest_hash(),
            ShareError::ManifestMismatch
        );
        ensure!(
            max_expiry(self.issued_at, MAX_MANIFEST_TTL_SECONDS, self.expires_at),
            ShareError::Protocol
        );
        ensure!(
            postcard::to_stdvec(self)?.len() <= MAX_AUTHORITY_FRAME_BYTES,
            ShareError::Protocol
        );
        Ok(())
    }

    pub fn verify_for(&self, owner: EndpointId, share: ShareId, now: u64) -> Result<()> {
        ensure!(self.owner == owner, ShareError::OwnerMismatch);
        ensure!(self.share == share, ShareError::OwnerMismatch);
        self.validate_shape()?;
        verify_signature(
            self.owner,
            MANIFEST_DOMAIN,
            &manifest_payload(self),
            &self.signature,
        )?;
        check_issued_at(self.issued_at, now)?;
        ensure!(self.expires_at > now, ShareError::GrantExpired);
        Ok(())
    }

    pub fn verify_record(&self, snapshot: &SnapshotToken, record: &SyncRecord) -> Result<()> {
        ensure!(
            self.snapshot == snapshot.snapshot,
            ShareError::ManifestMismatch
        );
        ensure!(self.share == snapshot.share, ShareError::ManifestMismatch);
        ensure!(self.epoch == snapshot.epoch, ShareError::EpochMismatch);
        ensure!(
            self.record_hash == record.logical_hash(),
            ShareError::ManifestMismatch
        );
        let content_hash = record.content_hash.ok_or(ShareError::ManifestMismatch)?;
        ensure!(
            !record.tombstone && record.kind == SyncEntryKind::File,
            ShareError::ManifestMismatch
        );
        ensure!(
            self.manifest.file_hash == content_hash,
            ShareError::ManifestMismatch
        );
        ensure!(
            self.manifest.size == record.size,
            ShareError::ManifestMismatch
        );
        Ok(())
    }
}

impl ShareGrant {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign(
        key: &SecretKey,
        share: ShareId,
        consumer: EndpointId,
        provider: EndpointId,
        epoch: PermissionEpoch,
        provider_epoch: PermissionEpoch,
        snapshot: SnapshotId,
        manifest: Hash32,
        request_hash: Hash32,
        nonce: GrantNonce,
        issued_at: u64,
    ) -> Result<Self> {
        ensure!(consumer != provider, ShareError::EndpointMismatch);
        ensure!(epoch > 0, ShareError::EpochMismatch);
        let grant = Self {
            version: AUTHORITY_VERSION,
            owner: key.public(),
            share,
            consumer,
            provider,
            epoch,
            provider_epoch,
            snapshot,
            manifest,
            request_hash,
            issued_at,
            expires_at: issued_at.saturating_add(MAX_GRANT_TTL_SECONDS),
            nonce,
            signature: key.sign(b""),
        };
        let signature = key.sign(&signing_bytes(GRANT_DOMAIN, &grant_payload(&grant))?);
        let grant = Self { signature, ..grant };
        grant.validate_shape()?;
        Ok(grant)
    }

    fn validate_shape(&self) -> Result<()> {
        ensure!(
            self.version == AUTHORITY_VERSION,
            ShareError::UnsupportedVersion
        );
        ensure!(self.owner != self.consumer, ShareError::EndpointMismatch);
        ensure!(self.consumer != self.provider, ShareError::EndpointMismatch);
        ensure!(self.epoch > 0, ShareError::EpochMismatch);
        ensure!(
            max_expiry(self.issued_at, MAX_GRANT_TTL_SECONDS, self.expires_at),
            ShareError::Protocol
        );
        ensure!(
            postcard::to_stdvec(self)?.len() <= MAX_AUTHORITY_FRAME_BYTES,
            ShareError::Protocol
        );
        Ok(())
    }

    pub fn verify_for(&self, owner: EndpointId, share: ShareId, now: u64) -> Result<()> {
        ensure!(self.owner == owner, ShareError::OwnerMismatch);
        ensure!(self.share == share, ShareError::OwnerMismatch);
        self.validate_shape()?;
        verify_signature(
            self.owner,
            GRANT_DOMAIN,
            &grant_payload(self),
            &self.signature,
        )?;
        check_issued_at(self.issued_at, now)?;
        ensure!(self.expires_at > now, ShareError::GrantExpired);
        Ok(())
    }

    pub fn activate_request(&self) -> ActivateGrantRequest {
        ActivateGrantRequest {
            owner: self.owner,
            share: self.share,
            consumer: self.consumer,
            provider: self.provider,
            epoch: self.epoch,
            provider_epoch: self.provider_epoch,
            manifest: self.manifest,
            request_hash: self.request_hash,
            nonce: self.nonce,
        }
    }
}

impl ActivateGrantRequest {
    pub fn matches(&self, grant: &ShareGrant) -> bool {
        self == &grant.activate_request()
    }

    #[allow(dead_code)]
    pub(crate) fn verify_signature(&self, member: EndpointId, signature: &Signature) -> Result<()> {
        ensure!(self.consumer == member, ShareError::EndpointMismatch);
        verify_signature(member, ACTIVATE_DOMAIN, &activate_payload(self), signature)
    }
}

impl ActivateGrantReply {
    pub(crate) fn sign(
        key: &SecretKey,
        share: ShareId,
        provider: EndpointId,
        nonce: GrantNonce,
        activation_id: [u8; 16],
        accepted: bool,
        max_duration_secs: u16,
    ) -> Result<Self> {
        ensure!(
            max_duration_secs <= MAX_ACTIVATE_TTL_SECONDS,
            ShareError::Protocol
        );
        let reply = Self {
            share,
            provider,
            nonce,
            activation_id,
            accepted,
            max_duration_secs,
            signature: key.sign(b""),
        };
        let signature = key.sign(&signing_bytes(
            ACTIVATE_DOMAIN,
            &activate_reply_payload(&reply),
        )?);
        Ok(Self { signature, ..reply })
    }

    pub fn verify_for(&self, owner: EndpointId, share: ShareId) -> Result<()> {
        ensure!(self.share == share, ShareError::OwnerMismatch);
        ensure!(
            self.max_duration_secs <= MAX_ACTIVATE_TTL_SECONDS,
            ShareError::Protocol
        );
        verify_signature(
            owner,
            ACTIVATE_DOMAIN,
            &activate_reply_payload(self),
            &self.signature,
        )
    }
}

impl ApplyPermit {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn sign(
        key: &SecretKey,
        share: ShareId,
        consumer: EndpointId,
        epoch: PermissionEpoch,
        snapshot: SnapshotId,
        root_hash: Hash32,
        issued_at: u64,
        nonce: GrantNonce,
    ) -> Result<Self> {
        ensure!(epoch > 0, ShareError::EpochMismatch);
        let permit = Self {
            version: AUTHORITY_VERSION,
            owner: key.public(),
            share,
            consumer,
            epoch,
            snapshot,
            root_hash,
            issued_at,
            expires_at: issued_at.saturating_add(MAX_APPLY_TTL_SECONDS),
            nonce,
            signature: key.sign(b""),
        };
        let signature = key.sign(&signing_bytes(APPLY_DOMAIN, &apply_payload(&permit))?);
        let permit = Self {
            signature,
            ..permit
        };
        permit.validate_shape()?;
        Ok(permit)
    }

    fn validate_shape(&self) -> Result<()> {
        ensure!(
            self.version == AUTHORITY_VERSION,
            ShareError::UnsupportedVersion
        );
        ensure!(self.epoch > 0, ShareError::EpochMismatch);
        ensure!(
            max_expiry(self.issued_at, MAX_APPLY_TTL_SECONDS, self.expires_at),
            ShareError::Protocol
        );
        ensure!(
            postcard::to_stdvec(self)?.len() <= MAX_AUTHORITY_FRAME_BYTES,
            ShareError::Protocol
        );
        Ok(())
    }

    pub fn verify_for(
        &self,
        owner: EndpointId,
        share: ShareId,
        consumer: EndpointId,
        now: u64,
    ) -> Result<()> {
        ensure!(self.owner == owner, ShareError::OwnerMismatch);
        ensure!(self.share == share, ShareError::OwnerMismatch);
        ensure!(self.consumer == consumer, ShareError::EndpointMismatch);
        self.validate_shape()?;
        verify_signature(
            self.owner,
            APPLY_DOMAIN,
            &apply_payload(self),
            &self.signature,
        )?;
        check_issued_at(self.issued_at, now)?;
        ensure!(self.expires_at > now, ShareError::GrantExpired);
        Ok(())
    }
}

pub(crate) fn request_hash(
    share: ShareId,
    snapshot: SnapshotId,
    manifest: Hash32,
    hashes: &[Hash32],
) -> Result<Hash32> {
    validate_hash_subset(hashes)?;
    let bytes = signing_bytes(
        REQUEST_DOMAIN,
        &RequestHash {
            share,
            snapshot,
            manifest,
            hashes,
        },
    )?;
    Ok(Hash32::digest(&bytes))
}

pub(crate) fn validate_hash_subset(hashes: &[Hash32]) -> Result<()> {
    ensure!(hashes.len() <= MAX_REQUEST_HASHES, ShareError::Busy);
    let mut unique = BTreeSet::new();
    for hash in hashes {
        ensure!(unique.insert(*hash), ShareError::ManifestMismatch);
    }
    for window in hashes.windows(2) {
        ensure!(window[0] < window[1], ShareError::ManifestMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaweave_core::{
        ChunkDescriptor, ChunkingProfile, MANIFEST_SCHEMA_V1, ReplicaId, SYNC_RECORD_SCHEMA_V1,
        VersionVector, WirePath,
    };

    fn fixture_record(owner: ReplicaId) -> (SyncRecord, FileManifest) {
        let bytes = b"authority fixture";
        let hash = Hash32::digest(bytes);
        let manifest = FileManifest {
            schema_version: MANIFEST_SCHEMA_V1,
            size: bytes.len() as u64,
            file_hash: hash,
            profile: ChunkingProfile::DEFAULT,
            chunks: vec![ChunkDescriptor {
                offset: 0,
                length: bytes.len() as u32,
                hash,
            }],
        };
        let mut version = VersionVector::default();
        version.increment(owner).unwrap();
        let record = SyncRecord {
            schema_version: SYNC_RECORD_SCHEMA_V1,
            path: WirePath::new("authority.txt").unwrap(),
            kind: SyncEntryKind::File,
            size: bytes.len() as u64,
            content_hash: Some(hash),
            readonly: false,
            version,
            tombstone: false,
        };
        (record, manifest)
    }

    #[test]
    fn owner_binding_and_subset_order_are_fail_closed() {
        let owner = SecretKey::generate();
        let other = SecretKey::generate();
        let share = ShareId([1; 32]);
        let token = SnapshotToken::sign(&owner, share, 1, [2; 32], Hash32::digest(b"root"), 0, 100)
            .unwrap();
        assert!(token.verify_for(owner.public(), share, 100).is_ok());
        assert!(token.verify_for(other.public(), share, 100).is_err());
        assert!(
            validate_hash_subset(&[Hash32::from_bytes([2; 32]), Hash32::from_bytes([1; 32])])
                .is_err()
        );
        assert!(
            validate_hash_subset(&[Hash32::from_bytes([1; 32]), Hash32::from_bytes([1; 32])])
                .is_err()
        );
    }

    #[test]
    fn manifest_attestation_binds_exact_record_and_manifest() {
        let owner = SecretKey::generate();
        let share = ShareId([3; 32]);
        let (record, manifest) = fixture_record(ReplicaId(Hash32::digest(b"replica")));
        let token = SnapshotToken::sign(
            &owner,
            share,
            1,
            [4; 32],
            MerkleTree::from_records(vec![record.clone()])
                .unwrap()
                .root_hash(),
            1,
            100,
        )
        .unwrap();
        let attestation =
            ManifestAttestation::sign(&owner, &token, &record, manifest, 100).unwrap();
        attestation.verify_for(owner.public(), share, 100).unwrap();
        attestation.verify_record(&token, &record).unwrap();
    }

    #[test]
    fn manifest_attestation_rejects_record_size_mismatch() {
        let owner = SecretKey::generate();
        let share = ShareId([5; 32]);
        let (record, manifest) = fixture_record(ReplicaId(Hash32::digest(b"size-replica")));
        let token = SnapshotToken::sign(
            &owner,
            share,
            1,
            [6; 32],
            MerkleTree::from_records(vec![record.clone()])
                .unwrap()
                .root_hash(),
            1,
            100,
        )
        .unwrap();
        let mut wrong_size = record;
        wrong_size.size = wrong_size.size.saturating_add(1);
        assert!(ManifestAttestation::sign(&owner, &token, &wrong_size, manifest, 100).is_err());
    }
}
