//! Owner token generation, persistence, and constant-time comparison.

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
};

use anyhow::{Context, Result, bail, ensure};

/// 32-byte owner token authenticating every local IPC request.
#[derive(Clone, Debug, Eq)]
pub struct AuthToken([u8; 32]);

impl PartialEq for AuthToken {
    fn eq(&self, other: &Self) -> bool {
        constant_time_eq(&self.0, &other.0)
    }
}

impl AuthToken {
    /// Constructs a token from its raw bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Generates a fresh 32-byte token from the operating-system CSPRNG.
    ///
    /// # Errors
    ///
    /// Returns an error when the platform cannot provide random bytes.
    pub fn generate() -> Result<Self> {
        Ok(Self(crate::platform::random_bytes()?))
    }

    /// Loads an existing owner-only token file or creates a new one.
    ///
    /// # Errors
    ///
    /// Returns an error when the file is world-readable, malformed, or cannot
    /// be created with owner-only permissions.
    pub fn load_or_create(path: &Path) -> Result<Self> {
        if path.exists() {
            return Self::load(path);
        }
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
            crate::lock::restrict_owner_dir(parent)?;
        }
        let token = Self::generate()?;
        let encoded = format!("{}\n", hex_encode(&token.0));
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(path) {
            Ok(mut file) => {
                file.write_all(encoded.as_bytes())?;
                file.sync_all()?;
                Ok(token)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Self::load(path),
            Err(error) => Err(error)
                .with_context(|| format!("failed to create token file {}", path.display())),
        }
    }

    fn load(path: &Path) -> Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(path)?.permissions().mode();
            ensure!(
                mode & 0o077 == 0,
                "token file {} is accessible by group or other users; run chmod 600",
                path.display()
            );
        }
        let encoded = fs::read_to_string(path)
            .with_context(|| format!("failed to read token file {}", path.display()))?;
        let trimmed = encoded.trim();
        ensure!(
            trimmed.len() == 64,
            "token file {} has invalid length {}",
            path.display(),
            trimmed.len()
        );
        let mut bytes = [0_u8; 32];
        hex_decode(trimmed, &mut bytes)
            .with_context(|| format!("invalid token file {}", path.display()))?;
        Ok(Self(bytes))
    }

    /// Returns the raw token bytes.
    #[must_use]
    pub fn expose(&self) -> [u8; 32] {
        self.0
    }

    /// Constant-time comparison against a presented byte slice.
    #[must_use]
    pub fn verify(&self, presented: &[u8]) -> bool {
        constant_time_eq(&self.0, presented)
    }

    /// Hex encoding used on the JSON IPC envelope.
    #[must_use]
    pub fn to_hex(&self) -> String {
        hex_encode(&self.0)
    }

    /// Parses a hex-encoded token from an IPC envelope.
    ///
    /// # Errors
    ///
    /// Returns an error when the hex string is the wrong length or not hex.
    pub fn from_hex(value: &str) -> Result<Self> {
        ensure!(value.len() == 64, "token hex must be 64 characters");
        let mut bytes = [0_u8; 32];
        hex_decode(value, &mut bytes)?;
        Ok(Self(bytes))
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(value: &str, out: &mut [u8]) -> Result<()> {
    ensure!(value.len() == out.len() * 2, "hex length mismatch");
    for (index, chunk) in value.as_bytes().chunks(2).enumerate() {
        let hi = hex_nibble(chunk[0])?;
        let lo = hex_nibble(chunk[1])?;
        out[index] = (hi << 4) | lo;
    }
    Ok(())
}

fn hex_nibble(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => bail!("invalid hex digit"),
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0_u8;
    for (a, b) in left.iter().zip(right.iter()) {
        diff |= a ^ b;
    }
    diff == 0
}
