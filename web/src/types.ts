export interface FolderInput {
  name: string;
  root: string;
  role: "sync" | "receive";
  state_path?: string;
  identity_path?: string;
  device_id?: string;
  peer_endpoint_id?: string;
  direct_addresses?: string[];
  allowed_peers?: string[];
  bind?: string;
  enabled?: boolean;
  interval_seconds?: number;
  max_connections?: number;
  min_free_space_mib?: number;
}
export interface FolderView extends FolderInput {
  id: string;
  endpoint_id: string;
  addresses: string[];
  status:
    | "starting"
    | "idle"
    | "syncing"
    | "pausing"
    | "paused"
    | "listening"
    | "error"
    | "stopped";
  phase: string | null;
  current_path: string | null;
  last_sync_at: number | null;
  last_error: string | null;
  retry_at: number | null;
  files_count: number;
  total_bytes: number;
  last_report: Record<string, unknown> | null;
}
export interface DeviceInput {
  name: string;
  endpoint_id: string;
  address: string;
}
export interface DeviceView extends DeviceInput {
  id: string;
  added_at: number;
  last_seen_at: number | null;
}
export interface Settings {
  node_name: string;
  poll_interval_seconds: number;
  history_limit: number;
}
export interface Activity {
  id: string;
  folder_id: string | null;
  kind: string;
  title: string;
  detail: string;
  timestamp: number;
  pushed_bytes: number;
  pulled_bytes: number;
  path: string | null;
}
export interface HistoryPoint {
  timestamp: number;
  folder_id: string;
  pushed_bytes: number;
  pulled_bytes: number;
}
export interface AppSnapshot {
  node: {
    name: string;
    version: string;
    platform: string;
    started_at: number;
    uptime_seconds: number;
  };
  folders: FolderView[];
  devices: DeviceView[];
  activities: Activity[];
  history: HistoryPoint[];
  totals: {
    folders: number;
    active_folders: number;
    files: number;
    bytes: number;
    pushed_bytes: number;
    pulled_bytes: number;
    conflicts: number;
  };
  settings: Settings;
  revision: number;
}
export interface Session {
  authenticated: boolean;
  csrf_token?: string;
}
export interface Directory {
  path: string;
  parent: string | null;
  entries: { name: string; path: string }[];
}
