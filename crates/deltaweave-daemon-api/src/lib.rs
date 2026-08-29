//! Versioned daemon IPC types.

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};

/// Current daemon IPC major version.
pub const PROTOCOL_VERSION_MAJOR: u16 = 1;
/// Current daemon IPC minor version.
pub const PROTOCOL_VERSION_MINOR: u16 = 0;

/// Negotiated IPC protocol version.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProtocolVersion {
    /// Incompatible if different.
    pub major: u16,
    /// Additive within the same major.
    pub minor: u16,
}

impl ProtocolVersion {
    /// Accepts matching majors and returns the overlapping minor.
    pub fn negotiate(self, server_major: u16, server_minor: u16) -> Result<Self, ErrorBody> {
        if self.major != server_major {
            return Err(ErrorBody {
                code: ErrorCode::UpgradeRequired,
                message: "incompatible daemon protocol".into(),
            });
        }
        Ok(Self {
            major: server_major,
            minor: server_minor.min(self.minor),
        })
    }
}

/// One client command.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Request {
    /// Client protocol version.
    pub protocol_version: ProtocolVersion,
    /// Caller-chosen correlation id.
    pub request_id: String,
    /// Tagged command body.
    pub command: Command,
}

/// Transfer direction for a job.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// Both sides may apply changes.
    Bidirectional,
    /// Local changes are pushed; remote changes are not applied locally.
    SendOnly,
    /// Remote changes are applied; local changes are not pushed.
    ReceiveOnly,
}

/// Commands the daemon accepts.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    /// Capability negotiation.
    Hello,
    /// List persisted jobs.
    ListJobs,
    /// Create a folder-to-peer job.
    CreateJob {
        /// User-visible name.
        name: String,
        /// Local folder path.
        local_root: String,
        /// Remote endpoint id as hex.
        peer_endpoint_id: String,
        /// Transfer direction.
        direction: Direction,
        /// The user explicitly accepted the dry-run preview.
        preview_confirmed: bool,
    },
    /// Pause a running job.
    PauseJob {
        /// Job id.
        id: String,
    },
    /// Resume a paused job.
    ResumeJob {
        /// Job id.
        id: String,
    },
    /// Run one sync pass now.
    SyncNow {
        /// Job id.
        id: String,
    },
    /// Cancel the current pass at the next gate.
    CancelJob {
        /// Job id.
        id: String,
    },
    /// Issue a single-use pairing ticket from the live server address.
    IssueTicket {
        /// Lifetime in seconds; default 600.
        ttl_seconds: Option<u64>,
    },
    /// Redeem a printable ticket code using this daemon's identity.
    RedeemTicket {
        /// Printable `dwpair1:` code.
        code: String,
    },
    /// Revoke an authorized peer.
    RevokePeer {
        /// Peer endpoint id as hex.
        endpoint_id: String,
    },
    /// Dry-run a job without applying.
    PreviewJob {
        /// Job id.
        id: String,
    },
    /// List unresolved conflict copies for a job.
    ListConflicts {
        /// Job id.
        id: String,
    },
    /// Resolve one conflict path.
    ResolveConflict {
        /// Job id.
        id: String,
        /// Portable path of the conflicted file.
        path: String,
        /// How to resolve.
        action: ConflictAction,
    },
    /// Stop the daemon after flushing durable state.
    Stop,
}

/// User choice for one conflicted path.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictAction {
    /// Keep this machine's verified content at the canonical path.
    KeepLocal,
    /// Keep the peer's verified content at the canonical path.
    KeepRemote,
    /// Leave the portable conflict copy in place.
    KeepBoth,
}

/// Correlated command reply.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Response {
    /// Matches the request.
    pub request_id: String,
    /// Success payload or domain error.
    pub result: Result<CommandResult, ErrorBody>,
}

/// One job as seen over IPC.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct JobInfo {
    /// Stable job identifier.
    pub id: String,
    /// User-visible name.
    pub name: String,
    /// Local folder path.
    pub local_root: String,
    /// Remote endpoint id as hex.
    pub peer_endpoint_id: String,
    /// Transfer direction.
    pub direction: Direction,
    /// Whether the job runs continuously.
    pub continuous: bool,
    /// Whether the job is paused.
    pub paused: bool,
}

/// Successful command payloads.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CommandResult {
    /// Server accepted Hello.
    Hello {
        /// Negotiated version.
        protocol_version: ProtocolVersion,
        /// Unique daemon instance id.
        instance_id: String,
    },
    /// Current jobs.
    Jobs {
        /// Jobs ordered by id.
        jobs: Vec<JobInfo>,
    },
    /// A job-mutating command succeeded.
    Accepted {
        /// Job id.
        id: String,
    },
    /// A pairing ticket was issued.
    TicketIssued {
        /// Printable `dwpair1:` code. Never appears in events.
        code: String,
        /// Unix-seconds expiry.
        expires_at: u64,
        /// Server endpoint id as hex.
        server_endpoint_id: String,
        /// Truncated endpoint fingerprint for UI confirmation.
        server_fingerprint: String,
    },
    /// A pairing ticket was redeemed.
    TicketRedeemed {
        /// `paired` or `already_paired`.
        outcome: String,
        /// Remote endpoint id as hex.
        peer_endpoint_id: String,
        /// Truncated remote fingerprint for UI confirmation.
        peer_fingerprint: String,
    },
    /// Dry-run counts; no apply.
    Preview {
        /// Local files that would be sent.
        sends: u64,
        /// Remote files that would be received.
        receives: u64,
        /// Paths that would be deleted locally.
        deletes: u64,
        /// Concurrent-edit conflicts.
        conflicts: u64,
    },
    /// Conflict copies for one job.
    Conflicts {
        /// Unresolved conflicts.
        conflicts: Vec<ConflictInfo>,
    },
}

/// One conflict presented to the UI.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConflictInfo {
    /// Canonical portable path.
    pub path: String,
    /// Portable `.conflict-<hash>` copy, if retained.
    pub conflict_path: Option<String>,
    /// Winner content hash hex.
    pub winner_hash: String,
    /// Loser content hash hex.
    pub loser_hash: String,
}

/// Stable error codes.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// Client and server majors differ.
    UpgradeRequired,
    /// Caller is not the owning user.
    Unauthorized,
    /// Command was well-formed JSON but invalid.
    InvalidRequest,
    /// Unexpected daemon failure.
    Internal,
}

/// User-visible error.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ErrorBody {
    /// Stable code.
    pub code: ErrorCode,
    /// Safe message; never includes secrets.
    pub message: String,
}

/// Server-push notification.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Event {
    /// Monotonic event id.
    pub event_id: u64,
    /// Snapshot generation.
    pub state_revision: u64,
    /// Tagged body.
    pub body: EventBody,
}

/// Dashboard severity for one job.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    /// Healthy idle or transferring.
    Normal,
    /// Transient issue; retrying.
    Attention,
    /// User action required.
    ActionRequired,
}

/// Event payloads.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventBody {
    /// Daemon is serving IPC.
    DaemonReady {
        /// Unique daemon instance id.
        instance_id: String,
    },
    /// Coalesced transfer progress.
    JobProgress {
        /// Job id.
        id: String,
        /// Sync phase name.
        phase: String,
        /// Optional path being processed.
        current_path: Option<String>,
        /// Payload bytes pulled so far.
        pulled_bytes: u64,
        /// Payload bytes pushed so far.
        pushed_bytes: u64,
        /// Manifest extents reused so far.
        reused_extents: usize,
        /// Instantaneous throughput estimate.
        bytes_per_second: u64,
        /// Estimated remaining seconds.
        eta_seconds: Option<u64>,
    },
    /// Job status line.
    JobState {
        /// Job id.
        id: String,
        /// Dashboard severity.
        severity: Severity,
        /// User-visible summary; never includes secrets.
        summary: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_round_trips_and_rejects_incompatible_major() {
        let request = Request {
            protocol_version: ProtocolVersion { major: 1, minor: 0 },
            request_id: "r1".into(),
            command: Command::Hello,
        };
        let json = serde_json::to_string(&request).unwrap();
        let back: Request = serde_json::from_str(&json).unwrap();
        assert!(matches!(back.command, Command::Hello));

        let too_new = ProtocolVersion { major: 2, minor: 0 };
        assert_eq!(
            too_new.negotiate(1, 0).unwrap_err().code,
            ErrorCode::UpgradeRequired
        );
    }

    #[test]
    fn ticket_response_omits_secret_bytes() {
        let json = serde_json::to_string(&CommandResult::TicketIssued {
            code: "dwpair1:ab".into(),
            expires_at: 1,
            server_endpoint_id: "aa".repeat(32),
            server_fingerprint: "abcd".into(),
        })
        .unwrap();
        assert!(!json.contains("secret"));
    }
}
