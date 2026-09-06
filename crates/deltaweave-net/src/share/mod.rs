//! Owner-mediated, folder-scoped sharing over one persistent device endpoint.
mod registry;
mod runtime;
mod service;
mod ticket;
pub(crate) mod wire;
pub use registry::{Invitation, MemberRelationship, Membership, OwnedShareConfig};
pub(crate) use runtime::Authorization;
pub use runtime::{MutationProvenance, OwnerShare};
pub use service::{ShareService, ShareSession};

pub const ALPN_V3: &[u8] = b"deltaweave/share/3";
pub use ticket::{InvitationId, LegacyProof, Permission, ShareId, ShareTicket, TicketPreview};

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShareError {
    InvalidTicket,
    UnsupportedVersion,
    Expired,
    InvitationRevoked,
    MemberRevoked,
    PermissionDenied,
    UnknownShare,
    NotMember,
    ReplicaClaimRejected,
    InvalidRecord,
    Busy,
    Offline,
    StateUnavailable,
    Protocol,
    OwnerMismatch,
    TransferFailed,
}
impl std::fmt::Display for ShareError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidTicket => "invalid share key",
            Self::UnsupportedVersion => "unsupported share key version",
            Self::Expired => "share key expired",
            Self::InvitationRevoked => "share key revoked",
            Self::MemberRevoked => "membership revoked",
            Self::PermissionDenied => "share permission denied",
            Self::UnknownShare => "share unavailable",
            Self::NotMember => "share enrollment required",
            Self::ReplicaClaimRejected => "legacy identity proof rejected",
            Self::InvalidRecord => "invalid causal record",
            Self::Busy => "share busy; retry",
            Self::Offline => "share owner unavailable",
            Self::StateUnavailable => "private share state unavailable",
            Self::Protocol => "invalid share protocol",
            Self::OwnerMismatch => "share owner mismatch",
            Self::TransferFailed => "share transfer failed",
        })
    }
}
impl ShareError {
    /// Safe boundary for API/UI responses. Never serialize an internal error chain.
    /// After a connection is revoked mid-transfer, reconnect to learn its durable
    /// member status; a generic interrupted transfer is not proof of revocation.
    pub fn classify(error: &anyhow::Error) -> Self {
        error
            .downcast_ref::<Self>()
            .copied()
            .unwrap_or(Self::TransferFailed)
    }
}
impl std::error::Error for ShareError {}
pub(crate) fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
