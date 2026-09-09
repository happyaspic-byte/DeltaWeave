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
  shares?: ManagedShareView[];
  pending?: PendingView[];
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

/** Managed share values mirror deltaweave-control's public JSON contract. */
export type Permission = "read_only" | "read_write";
export type ShareRole = "owner" | "member";
export type ManagedStatus =
  | "waiting"
  | "offline"
  | "initial_sync"
  | "complete"
  | "conflict"
  | "revoked"
  | "error"
  | "paused";
export type EnrollmentState = "waiting" | "enrolled" | "revoked" | "error";
export type KeyIssuance = "not_checked" | "validated";
export type MutationCompletion = "pending" | "complete";

export interface ErrorSummary {
  code: string;
  message: string;
}
export interface ConnectedDeviceView {
  member_id: string;
  permission: Permission;
  active_operations: number;
  last_seen_at: number | null;
}
export interface ManagedShareView {
  share_id: string;
  name: string;
  role: ShareRole;
  permission: Permission | null;
  root: string;
  status: ManagedStatus;
  phase: string | null;
  last_sync_at: number | null;
  retry_at: number | null;
  files_count: number;
  total_bytes: number;
  transferred_bytes: number;
  speed_bps: number;
  active_peer_count: number;
  connected_devices: ConnectedDeviceView[];
  last_error: ErrorSummary | null;
}
export type ShareView = ManagedShareView;
export interface PendingView {
  request_id: string;
  share_id: string;
  status: ManagedStatus;
  created_at: number;
  retry_at: number | null;
}
export interface KeySummary {
  invitation_id: string;
  share_id: string;
  permission: Permission;
  issued_at: number | null;
  expires_at: number | null;
  revoked_at: number | null;
}
export interface MemberView {
  member_id: string;
  permission: Permission;
  enrolled_at: number;
  revoked_at: number | null;
  active_operations: number;
  last_seen_at: number | null;
  revocation_pending: boolean;
}
export interface KeyPreview {
  share_id: string;
  name: string;
  permission: Permission;
  invitation_id: string;
  expires_at: number | null;
  signature_valid: boolean;
  issuance: KeyIssuance;
}
export interface IssuedKey {
  request_id: string;
  share_id: string;
  invitation_id: string;
  permission: Permission;
  expires_at: number | null;
  /** Display-once bearer. Keep this value in React memory only. */
  key: string;
}
export interface JoinResult {
  request_id: string;
  share_id: string;
  enrollment: EnrollmentState;
  status: ManagedStatus;
  permission: Permission | null;
  member_id: string | null;
}
export interface MutationResult {
  request_id: string;
  accepted: boolean;
  status: ManagedStatus;
  completion: MutationCompletion;
  retry_at: number | null;
}
export interface ShareCommandResult extends ManagedShareView {}

export interface ApiErrorPayload {
  error: string;
  error_code?: string;
  request_id?: string;
}
