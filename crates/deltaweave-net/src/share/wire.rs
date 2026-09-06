use super::{LegacyProof, Membership, ShareError, ShareId, ShareTicket, TicketPreview};
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
}
#[derive(Serialize, Deserialize)]
pub(crate) enum Reply {
    Validated(TicketPreview),
    Enrolled(Membership),
    Accepted,
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
