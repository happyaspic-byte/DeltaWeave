//! Bounded length-prefixed JSON framing over Tokio I/O.

use anyhow::{Context, Result, ensure};
use serde::{Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Maximum accepted JSON control frame body, excluding the 4-byte length prefix.
pub const MAX_FRAME_BYTES: usize = 64 * 1024;

/// Writes one unsigned-32 big-endian length prefix followed by JSON bytes.
///
/// # Errors
///
/// Returns an error when serialization fails, the payload exceeds
/// [`MAX_FRAME_BYTES`], or the write fails.
pub async fn write_frame<W, T>(writer: &mut W, value: &T) -> Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let body = serde_json::to_vec(value).context("failed to encode IPC frame")?;
    ensure!(
        body.len() <= MAX_FRAME_BYTES,
        "IPC frame of {} bytes exceeds {MAX_FRAME_BYTES}",
        body.len()
    );
    let len = u32::try_from(body.len()).context("IPC frame length overflow")?;
    writer.write_u32(len).await?;
    writer.write_all(&body).await?;
    writer.flush().await?;
    Ok(())
}

/// Reads one bounded JSON frame, rejecting oversized and malformed payloads.
///
/// The length prefix is inspected before the body is allocated, so an
/// advertised size above [`MAX_FRAME_BYTES`] never grows a buffer.
///
/// # Errors
///
/// Returns an error on EOF, oversized length, I/O failure, or JSON parse
/// failure.
pub async fn read_frame<R, T>(reader: &mut R) -> Result<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let len = reader
        .read_u32()
        .await
        .context("failed to read IPC frame length")?;
    let len = usize::try_from(len).context("IPC frame length overflow")?;
    ensure!(
        len <= MAX_FRAME_BYTES,
        "IPC frame of {len} bytes exceeds {MAX_FRAME_BYTES}"
    );
    let mut body = vec![0_u8; len];
    reader
        .read_exact(&mut body)
        .await
        .context("failed to read IPC frame body")?;
    serde_json::from_slice(&body)
        .with_context(|| format!("malformed IPC JSON frame ({} bytes)", body.len()))
}
