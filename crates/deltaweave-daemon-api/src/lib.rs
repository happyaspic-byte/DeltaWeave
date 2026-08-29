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

/// Commands the daemon accepts.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    /// Capability negotiation.
    Hello,
}

/// Correlated command reply.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Response {
    /// Matches the request.
    pub request_id: String,
    /// Success payload or domain error.
    pub result: Result<CommandResult, ErrorBody>,
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

/// Event payloads.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventBody {
    /// Daemon is serving IPC.
    DaemonReady {
        /// Unique daemon instance id.
        instance_id: String,
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
}
