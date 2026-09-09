//! Owner-mediated, folder-scoped sharing over one persistent device endpoint.
mod authority;
mod registry;
mod roster;
mod runtime;
mod service;
mod ticket;
pub(crate) mod wire;
pub use authority::{
    ActivateGrantReply, ActivateGrantRequest, ActivationBinding, ActivationCancel,
    ActivationReceipt, ActivationStateView, ActivationStatusQuery, ApplyCancel, ApplyDrained,
    ApplyPermit, ApplyReceipt, ApplyStart, ApplyStateView, ApplyStatusQuery, AuthoritativeSnapshot,
    ClientIntentPhase, ClientIntentRow, ClientSide, ManifestAttestation, RevocationReceipt,
    ShareGrant, SnapshotToken, SwarmTransferReceipt,
};
pub use registry::{Invitation, MemberRelationship, Membership, OwnedShareConfig};
pub use roster::{
    GrantNonce, PermissionEpoch, ROSTER_HEARTBEAT_INTERVAL_SECONDS, RosterEntry, RosterHeartbeat,
    SignedRoster, SnapshotId,
};
pub(crate) use runtime::Authorization;
pub use runtime::{MutationProvenance, OwnerShare};
pub use service::{
    ActivationLease, LocalIoDrainProof, ManagedAdmissionLease, ShareService, ShareSession,
    SupplierRegistrationGuard,
};

pub const ALPN_V3: &[u8] = b"deltaweave/share/3";
/// Separate grant-gated data protocol.  D2 registers the endpoint and
/// rejects unauthenticated streams; E supplies the chunk adapter after the
/// authority contract is verified.
pub const ALPN_SWARM_V1: &[u8] = b"deltaweave/share-swarm/1";
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
    HeartbeatExpired,
    HeartbeatReplay,
    EpochMismatch,
    EndpointMismatch,
    ClockRollback,
    RosterStale,
    GrantExpired,
    GrantReplay,
    ManifestMismatch,
    CasUnavailable,
    RevocationPending,
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
            Self::HeartbeatExpired => "share heartbeat expired",
            Self::HeartbeatReplay => "share heartbeat replayed",
            Self::EpochMismatch => "share permission epoch mismatch",
            Self::EndpointMismatch => "share endpoint mismatch",
            Self::ClockRollback => "share clock rollback",
            Self::RosterStale => "share roster is stale",
            Self::GrantExpired => "share grant expired",
            Self::GrantReplay => "share grant replayed",
            Self::ManifestMismatch => "share manifest mismatch",
            Self::CasUnavailable => "share content unavailable",
            Self::RevocationPending => "share revocation is pending",
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
