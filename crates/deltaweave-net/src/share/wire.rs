use super::{
    ActivateGrantReply, ActivateGrantRequest, ActivationCancel, ActivationReceipt,
    ActivationStatusQuery, ApplyDrained, ApplyPermit, ApplyStart, AuthoritativeSnapshot,
    GrantNonce, LegacyProof, ManifestAttestation, Membership, RosterHeartbeat, ShareError,
    ShareGrant, ShareId, ShareTicket, SignedRoster, SnapshotToken, TicketPreview,
};
use crate::{read_frame, write_frame};
use anyhow::{Result, ensure};
use iroh::endpoint::{Connection, RecvStream};
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tokio::io::AsyncReadExt;

#[derive(Serialize, Deserialize)]
pub(crate) struct Hello {
    pub version: u8,
    pub share_id: ShareId,
    pub operation: Operation,
}
#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize)]
pub(crate) enum Operation {
    Validate(ShareTicket),
    Enroll {
        ticket: ShareTicket,
        proof: Option<Box<LegacyProof>>,
    },
    Session,
    /// Queries an existing membership without issuing a ticket or allocating a replica.
    /// Appended after the v3 variants so existing peers retain their wire ordinals.
    Resume,
    /// Requests an owner-signed roster and one member-bound heartbeat challenge.
    Roster,
    /// Submits a member-signed address heartbeat for a previously issued challenge.
    Heartbeat(RosterHeartbeat),
    /// Requests a complete owner-authoritative snapshot token and record set.
    Snapshot,
    /// Requests an owner attestation for one exact file record in a snapshot.
    Manifest {
        snapshot: SnapshotToken,
        record: deltaweave_core::SyncRecord,
    },
    /// Requests one exact provider grant for a sorted hash subset.
    SwarmGrant {
        provider: iroh::EndpointId,
        snapshot: SnapshotToken,
        manifest: ManifestAttestation,
        hashes: Vec<deltaweave_core::Hash32>,
    },
    /// Revalidates the consumer snapshot/root before a local apply.
    Revalidate {
        snapshot: SnapshotToken,
    },
    /// Records the start of a local apply operation against a permit nonce.
    ApplyStart(ApplyStart),
    /// Records completion of a local apply operation against a permit nonce.
    ApplyDrained(ApplyDrained),
    /// Provider asks the owner to activate an already signed grant.
    Activate(ActivateGrantRequest),
    /// One authenticated grant endpoint acknowledges that its side has
    /// drained.  The owner marks the grant terminal only after both the
    /// consumer and provider have sent this acknowledgement.
    GrantDrained {
        nonce: GrantNonce,
        activation_id: [u8; 16],
    },
    /// Queries the owner's durable activation row without extending a lease.
    ActivationStatus(ActivationStatusQuery),
    /// Atomically cancels an Issued activation, or returns the existing
    /// Active/terminal receipt when activation won the race.
    ActivationCancel(ActivationCancel),
}
#[derive(Serialize, Deserialize)]
pub(crate) enum Reply {
    Validated(TicketPreview),
    Enrolled(Membership),
    Accepted,
    Error(ShareError),
    /// Existing authenticated membership returned by the owner.
    Resumed(Membership),
    /// Owner-signed roster plus one-use member heartbeat challenge.
    Roster {
        roster: SignedRoster,
        challenge: GrantNonce,
    },
    /// Owner-signed roster after accepting a member address heartbeat.
    Heartbeat(SignedRoster),
    /// Complete owner-authoritative snapshot and signed token.
    Snapshot(AuthoritativeSnapshot),
    /// Owner-signed manifest attestation.
    Manifest(ManifestAttestation),
    /// Owner-signed one-request provider grant.
    Grant(ShareGrant),
    /// Owner-signed bounded apply permit.
    ApplyPermit(ApplyPermit),
    /// Owner-signed activation response.
    Activate(ActivateGrantReply),
    /// A non-session apply journal transition was durably accepted.
    ApplyAccepted,
    /// A grant endpoint drain acknowledgement was durably recorded.  The
    /// grant may still be Active until its other endpoint acknowledges too.
    GrantDrained,
    /// Owner-authenticated durable activation state for status and cancel.
    ActivationReceipt(ActivationReceipt),
}

/// The grant-gated data stream is deliberately separate from the legacy
/// sync/3 CAS protocol.  The signed grant and manifest are sent on every
/// stream so a provider never authorizes a connection from an address hint or
/// a caller-selected role.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) enum SwarmRequest {
    Grant {
        grant: super::ShareGrant,
        snapshot: super::SnapshotToken,
        record: deltaweave_core::SyncRecord,
        manifest: super::ManifestAttestation,
        hashes: Vec<deltaweave_core::Hash32>,
        operation_id: [u8; 16],
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) enum SwarmResponse {
    Ready(super::ActivationReceipt),
    Chunks {
        present: Vec<deltaweave_core::Hash32>,
        missing: Vec<deltaweave_core::Hash32>,
    },
    ChunkHeader {
        hash: deltaweave_core::Hash32,
        length: u32,
    },
    Finished(super::SwarmTransferReceipt),
    Error(ShareError),
}

pub(crate) async fn read_hello(receive: &mut RecvStream) -> Result<Hello> {
    let length = receive.read_u32().await? as usize;
    ensure!(length <= 16384, ShareError::Protocol);
    let mut bytes = vec![0; length];
    receive.read_exact(&mut bytes).await?;
    let hello: Hello = postcard::from_bytes(&bytes).map_err(|_| ShareError::Protocol)?;
    ensure!(hello.version == 3, ShareError::UnsupportedVersion);
    ensure!(postcard::to_stdvec(&hello)? == bytes, ShareError::Protocol);
    Ok(hello)
}
pub(crate) async fn exchange(connection: &Connection, hello: Hello) -> Result<Reply> {
    let (mut send, mut receive) = connection.open_bi().await?;
    write_frame(&mut send, &hello).await?;
    send.finish()?;
    let reply = read_frame(&mut receive).await?;
    match reply {
        Reply::Error(error) => Err(error.into()),
        other => Ok(other),
    }
}
pub(crate) async fn open_session_until(
    connection: &Connection,
    share_id: ShareId,
    deadline: Instant,
) -> Result<()> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    // `tokio::time::timeout(Duration::ZERO, future)` may poll a ready future
    // once. Do the expiry check before constructing/polling `exchange`, so a
    // caller whose session deadline has elapsed cannot open a stream or send
    // a Session hello as a side effect of a failed admission.
    ensure!(!remaining.is_zero(), ShareError::Offline);
    let reply = tokio::time::timeout(
        remaining,
        exchange(
            connection,
            Hello {
                version: 3,
                share_id,
                operation: Operation::Session,
            },
        ),
    )
    .await
    .map_err(|_| anyhow::Error::new(ShareError::Offline))??;
    ensure!(matches!(reply, Reply::Accepted), ShareError::Protocol);
    Ok(())
}
