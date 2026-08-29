//! Platform helpers kept out of the portable daemon modules.

/// Fills 32 bytes from the operating-system CSPRNG.
///
/// Unix reads `/dev/urandom`. Windows uses `getrandom`, which is already in
/// the workspace lockfile and does not introduce a new network stack.
///
/// # Errors
///
/// Returns an error when the platform CSPRNG is unavailable.
pub fn random_bytes() -> anyhow::Result<[u8; 32]> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|error| anyhow::anyhow!("failed to generate owner token: {error}"))?;
    Ok(bytes)
}
