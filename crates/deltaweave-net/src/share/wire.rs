use super::{
    GrantNonce, LegacyProof, Membership, RosterHeartbeat, ShareError, ShareId, ShareTicket,
    SignedRoster, TicketPreview,
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
