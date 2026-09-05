# DeltaWeave local web UI

The user clarified the target as “DeltaWeave 웹 ui” after the audit found only a
CLI in this worktree and a divergent Tauri branch. Implement a real browser UI
on the current 0.4 workspace. Do not import the divergent daemon implementation.

Design Read: DeltaWeave 웹 UI는 PC·NAS 동기화 운영자를 위한 로컬 관리 화면으로,
밝은 배경·짙은 글자·청록색 강조를 사용해 현재 폴더, 연결 대상, 실행 결과를
명확히 보여주는 방향이다.

## Scope and architecture

A new `deltaweave-web` Rust crate/binary embeds native HTML/CSS/JavaScript and
serves an authenticated local HTTP API using axum. The root folder and private
state are selected explicitly at startup. Existing Rust index, network and sync
libraries perform all filesystem and synchronization work. No shell execution,
fake metrics, mock API fallback, new cloud service, or production deployment.

The initial usable flow is: open the process's private browser link, inspect the
chosen folder and device ID, scan local files, enter a peer ID/address, explicitly
confirm bidirectional changes and synchronize once, inspect verified receipts,
or start/stop an allow-listed receiving endpoint. A receiver and local scan/sync
are mutually exclusive because the current libraries own their index handles.
The UI states this and provides the stop action. No silent concurrent DB opens.

The receiver permits only the peer ID supplied by the authenticated user. QUIC
traffic can bind a selected interface; the HTTP management server binds only
loopback. Synchronization preserves the existing version-vector, conflict-copy,
verification, private-trash and allow-list rules. Public data and private state
must not overlap; the identity is private and outside the synchronized root.

## HTTP contract

All `/api/*` routes require `Authorization: Bearer <session token>`. The token is
fresh per process, printed in a URL fragment, removed from browser history on
load and held in sessionStorage for refresh. No token is served by a public API.
Reject unexpected Host and cross-origin Origin headers; do not enable CORS.
Use CSP, no-store, nosniff, frame denial and no-referrer headers. Limit JSON bodies
to 8 KiB. Return JSON errors `{error: string, field?: string}` with HTTP 4xx/5xx.

`GET /api/state` returns the following shape:

```ts
type Phase = 'idle' | 'scanning' | 'synchronizing' | 'starting_receiver' | 'receiving' | 'stopping_receiver';
type Activity = {id:number; kind:'scan'|'sync'|'receiver_start'|'receiver_stop'; status:'running'|'success'|'error'; started_at_ms:number; finished_at_ms:number|null; error:string|null};
type State = {
  version:string; root:string; endpoint_id:string; phase:Phase;
  receiver:null|{endpoint_id:string;direct_addresses:string[];relay_urls:string[]};
  scan:null|{report:object;records:object[];total_records:number};
  sync:null|object;
  activity:Activity[];
};
```

`scan.report` is the actual serialized ScanReport. `scan.records` contains at most
200 actual PathRecords; `total_records` reports the full count. `sync` is the
actual serialized SyncReport from a verified completed round. Unknown/unmeasured
values stay null. Activity retains the latest 20 operations, newest first.

- `POST /api/scan` with `{}` starts an authoritative scan.
- `POST /api/sync` with `{peer_id:string,direct_address:string,confirm:true}` starts
  direct-only bidirectional synchronization. Invalid fields return 422 with the
  corresponding field name. No unconfirmed mutation is accepted.
- `POST /api/receiver/start` with `{peer_id:string}` starts a direct-only endpoint
  that allows that peer. A self peer ID is invalid.
- `POST /api/receiver/stop` with `{}` stops and closes the endpoint.

Accepted operations return HTTP 202 and a State snapshot, with running phase
set before acknowledgement. Conflicting operations return 409. State polling is
available during long work. Failures append an error activity and release busy
state; previously verified scan/sync results are retained and labeled historical.
Operations continue if the browser closes. The executable shuts down gracefully.

## UI contract and design

Native DOM modules, no runtime frontend dependency or build requirement. Korean
copy, one light theme, existing DeltaWeave wordmark, system sans-serif stack with
Korean fallbacks. DESIGN_VARIANCE=2, MOTION_INTENSITY=1, VISUAL_DENSITY=5. Retain
real technical identifiers and reports; decorative metrics/images are excluded.

Layout: a compact branded navigation rail at desktop, a wrapping top navigation
on small screens, a main overview showing current folder and operational state,
an actual result section, peer connection form, and operation history. Use shared
tokens, 4/8px spacing rhythm, restrained 8–12px corners and a single teal accent.
No artificial counts, repeated promotional cards or hero typography.

HTML/JS integration uses these stable IDs:
`auth-panel`, `auth-form`, `token`, `token-error`, `auth-submit`, `app-shell`,
`connection-banner`, `reconnect`, `overview`, `root-path`, `endpoint-id`,
`copy-endpoint`, `phase-label`, `phase-description`, `scan-button`, `receiver-stop`,
`receiver-info`, `receiver-addresses`, `scan-summary`, `scan-empty`, `file-table-body`,
`record-count`, `sync-summary`, `result-details`, `connection`, `peer-form`,
`peer_id`, `peer_id-error`, `direct_address`, `direct_address-error`, `confirm`,
`confirm-error`, `sync-button`, `receiver-start`, `form-error`, `activity`,
`activity-list`, `activity-empty`, `announcement`.

Use actual labels and field error descriptions, text status in addition to color,
visible focus, accessible tables and landmarks, wrap long paths/IDs, at least
44px primary controls, no modal unless needed. Preserve form values during
polling/failures. Auth, loading, empty, working, verified success, error, disabled
and session-rejected states must all be operational. Connection loss retains
visible past data but labels it stale and offers retry. No fake success when an
API is unavailable. A clipboard failure exposes selectable text with a message.

## Acceptance

- Rust tests cover HTTP authorization/origin/host/body limits, path isolation,
  field validation, concurrency, real scan and real two-peer synchronization,
  allowed and denied receivers, failure recovery and shutdown where applicable.
- Browser tests operate real isolated local servers and real temp files. Verify
  auth rejection/recovery, scan empty/changed, validation and preserved input,
  receiver start/stop, successful sync/receipts and unavailable peer recovery.
- Verify 360, 768, 1440 widths, 200% zoom, long Korean/English paths, keyboard
  focus and contrast in the supported light theme. Respect reduced motion.
- Run workspace build, fmt, Clippy and tests; meaningful JS regression tests.
- Save actual screenshots and logs, update audit report with implementation
  evidence. No claims that baseline Tauri screenshots are this new UI's before.

