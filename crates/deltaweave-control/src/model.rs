use serde::{Deserialize, Serialize};

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
