pub use deltaweave_net::{NetworkMode, share::{InvitationId, Permission, ShareId}};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf};

/// Options that affect only the managed, owner-mediated endpoint.
///
/// Manual folders retain their existing direct-address configuration. The managed
/// endpoint is deliberately configured at runtime so a normal manual-only open does
/// not contact discovery or relay services.
#[derive(Clone, Copy, Debug)]
pub struct ManagerOptions {
    pub managed_network: NetworkMode,
    pub managed_bind: Option<SocketAddr>,
}

impl Default for ManagerOptions {
    fn default() -> Self {
        Self {
            managed_network: NetworkMode::Internet,
            managed_bind: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShareRole {
    Owner,
    Member,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedStatus {
    Waiting,
    Offline,
    InitialSync,
    Complete,
    Conflict,
    Revoked,
    Error,
    Paused,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CreateShareInput {
    pub request_id: String,
    pub name: String,
    pub root: PathBuf,
    pub min_free_space_mib: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PreviewKeyInput {
    pub request_id: String,
    pub encoded_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ValidateKeyInput {
    pub request_id: String,
    pub encoded_key: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IssueKeyInput {
    pub request_id: String,
    pub share: ShareId,
    pub permission: Permission,
    pub expires_at: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct JoinShareInput {
    pub request_id: String,
    pub encoded_key: String,
    pub destination_root: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResumeMembershipInput {
    pub request_id: String,
    pub share: ShareId,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RevokeKeyInput {
    pub request_id: String,
    pub share: ShareId,
    pub invitation: InvitationId,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RotateKeyInput {
    pub request_id: String,
    pub share: ShareId,
    pub invitation: InvitationId,
    pub expires_at: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RevokeMemberInput {
    pub request_id: String,
    pub share: ShareId,
    pub member_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RemoveShareInput {
    pub request_id: String,
    pub share: ShareId,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ShareCommandInput {
    pub request_id: String,
    pub share: ShareId,
    pub command: ShareCommand,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ShareCommand {
    Sync,
    Pause,
    Resume,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FolderInput {
    pub name: String,
    pub root: String,
    pub role: String,
    pub state_path: Option<String>,
    pub identity_path: Option<String>,
    pub device_id: Option<String>,
    pub peer_endpoint_id: Option<String>,
    #[serde(default)]
    pub direct_addresses: Vec<String>,
    #[serde(default)]
    pub allowed_peers: Vec<String>,
    pub bind: Option<String>,
    pub enabled: Option<bool>,
    pub interval_seconds: Option<u64>,
    pub max_connections: Option<usize>,
    pub min_free_space_mib: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FolderView {
    #[serde(flatten)]
    pub input: FolderInput,
    pub id: String,
    pub endpoint_id: String,
    pub addresses: Vec<String>,
    pub status: String,
    pub phase: Option<String>,
    pub current_path: Option<String>,
    pub last_sync_at: Option<u64>,
    pub last_error: Option<String>,
    pub retry_at: Option<u64>,
    pub files_count: u64,
    pub total_bytes: u64,
    pub last_report: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManagedShareView {
    pub share_id: String,
    pub name: String,
    pub role: ShareRole,
    pub permission: Option<Permission>,
    pub root: String,
    pub status: ManagedStatus,
    pub phase: Option<String>,
    pub last_sync_at: Option<u64>,
    pub retry_at: Option<u64>,
    pub files_count: u64,
    pub total_bytes: u64,
    pub transferred_bytes: u64,
    pub speed_bps: u64,
    pub active_peer_count: u32,
    pub connected_devices: Vec<ConnectedDeviceView>,
    pub last_error: Option<ErrorSummary>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConnectedDeviceView {
    pub member_id: String,
    pub permission: Permission,
    pub active_operations: u32,
    pub last_seen_at: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemberView {
    pub member_id: String,
    pub permission: Permission,
    pub revocation_pending: bool,
    pub enrolled_at: u64,
    pub revoked_at: Option<u64>,
    pub active_operations: u32,
    pub last_seen_at: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeySummary {
    pub invitation_id: String,
    pub share_id: String,
    pub permission: Permission,
    pub issued_at: Option<u64>,
    pub expires_at: Option<u64>,
    pub revoked_at: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ErrorSummary {
    pub code: String,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyPreview {
    pub share_id: String,
    pub name: String,
    pub permission: Permission,
    pub invitation_id: String,
    pub expires_at: Option<u64>,
    pub signature_valid: bool,
    pub issuance: KeyIssuance,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyIssuance {
    NotChecked,
    Validated,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IssuedKey {
    pub request_id: String,
    pub share_id: String,
    pub invitation_id: String,
    pub permission: Permission,
    pub expires_at: Option<u64>,
    pub key: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnrollmentState {
    Waiting,
    Enrolled,
    Revoked,
    Error,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinResult {
    pub request_id: String,
    pub share_id: String,
    pub enrollment: EnrollmentState,
    pub status: ManagedStatus,
    pub permission: Option<Permission>,
    pub member_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MutationResult {
    pub request_id: String,
    pub accepted: bool,
    pub completion: MutationCompletion,
    pub retry_at: Option<u64>,
    pub status: ManagedStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationCompletion {
    Pending,
    Complete,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingView {
    pub request_id: String,
    pub share_id: String,
    pub status: ManagedStatus,
    pub created_at: u64,
    pub retry_at: Option<u64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeviceInput {
    pub name: String,
    pub endpoint_id: String,
    pub address: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeviceView {
    #[serde(flatten)]
    pub input: DeviceInput,
    pub id: String,
    pub added_at: u64,
    pub last_seen_at: Option<u64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Settings {
    pub node_name: String,
    pub poll_interval_seconds: u64,
    pub history_limit: usize,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            node_name: "DeltaWeave".into(),
            poll_interval_seconds: 30,
            history_limit: 300,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Activity {
    pub id: String,
    pub folder_id: Option<String>,
    pub kind: String,
    pub title: String,
    pub detail: String,
    pub timestamp: u64,
    pub pushed_bytes: u64,
    pub pulled_bytes: u64,
    pub path: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoryPoint {
    pub timestamp: u64,
    pub folder_id: String,
    pub pushed_bytes: u64,
    pub pulled_bytes: u64,
}
#[derive(Clone, Debug, Serialize)]
pub struct Node {
    pub name: String,
    pub version: String,
    pub platform: String,
    pub started_at: u64,
    pub uptime_seconds: u64,
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct Totals {
    pub folders: usize,
    pub active_folders: usize,
    pub files: u64,
    pub bytes: u64,
    pub pushed_bytes: u64,
    pub pulled_bytes: u64,
    pub conflicts: usize,
}
#[derive(Clone, Debug, Serialize)]
pub struct AppSnapshot {
    pub node: Node,
    pub folders: Vec<FolderView>,
    pub devices: Vec<DeviceView>,
    pub activities: Vec<Activity>,
    pub history: Vec<HistoryPoint>,
    pub totals: Totals,
    pub settings: Settings,
    pub revision: u64,
    #[serde(default)]
    pub shares: Vec<ManagedShareView>,
    #[serde(default)]
    pub pending: Vec<PendingView>,
}

/// One selectable directory in an authenticated server-side browse response.
#[derive(Clone, Debug, Serialize)]
pub struct DirectoryEntry {
    pub name: String,
    pub path: String,
}

/// Canonical directory location and its selectable child directories.
#[derive(Clone, Debug, Serialize)]
pub struct DirectoryListing {
    pub path: String,
    pub parent: Option<String>,
    pub entries: Vec<DirectoryEntry>,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FolderCommand {
    Sync,
    Pause,
    Resume,
}
