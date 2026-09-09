use super::{
    ActivateGrantReply, ActivateGrantRequest, ApplyDrained, ApplyPermit, ApplyStart,
    AuthoritativeSnapshot, GrantNonce, LegacyProof, ManifestAttestation, Membership,
    RosterHeartbeat, ShareError, ShareGrant, ShareId, ShareTicket, SignedRoster, SnapshotToken,
    TicketPreview,
};
use crate::{read_frame, write_frame};
use anyhow::{Result, ensure};
use iroh::endpoint::{Connection, RecvStream};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;

#[derive(Serialize, Deserialize)]
pub(crate) struct Hello {
    pub version: u8,
    pub share_id: ShareId,
    pub operation: Operation,
}
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
pub(crate) async fn open_session(connection: &Connection, share_id: ShareId) -> Result<()> {
    ensure!(
        matches!(
            exchange(
                connection,
                Hello {
                    version: 3,
                    share_id,
                    operation: Operation::Session
                }
            )
            .await?,
            Reply::Accepted
        ),
        ShareError::Protocol
    );
    Ok(())
}
