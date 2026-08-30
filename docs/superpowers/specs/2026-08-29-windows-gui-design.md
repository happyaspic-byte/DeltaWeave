# DeltaWeave Windows GUI Design

## Goal

Ship a light-only Windows application that is easier to operate than Resilio Sync, GoodSync, and Syncthing without moving synchronization work out of the existing Rust core. It supports unattended and manual synchronization, multiple local folders, a different peer per folder, and both client and server roles.

## Scope

The first release targets Windows 10/11 x86-64. Linux and Synology remain headless peers. The application provides:

- a Tauri 2 desktop window and notification-area tray;
- a persistent Rust daemon that owns all synchronization state;
- multiple sync jobs, each binding one local root to one peer and direction;
- bidirectional, send-only, and receive-only modes;
- automatic continuous sync, manual sync-now, per-job pause, and global pause;
- LAN peer discovery, existing `dwpair1` ticket issue/redeem, revocation, and identity display;
- transfer progress, current file, throughput, ETA, retry state, and CDC reuse statistics;
- explicit conflict review and resolution;
- Windows login startup and an installer;
- structured diagnostics export with secret redaction.

Dark mode, mobile applications, cloud accounts, browser administration, on-demand files, and code-signing certificate acquisition are outside the first release.

## Product Principles

1. The first screen combines current transfer performance with blocking conflicts.
2. Every warning states the cause, automatic recovery behavior, and the next available action.
3. IP addresses, endpoint IDs, and UDP ports are hidden behind discovery and tickets; manual entry remains under advanced settings.
4. No destructive synchronization begins before a first-run preview lists expected sends, receives, deletions, and conflicts.
5. Closing the window never stops synchronization. Quit Sync Engine is a separate, explicit action.
6. The GUI never opens a redb database. One daemon process owns every state database and watcher.
7. Credentials, ticket secrets, and private keys never enter telemetry, logs, IPC event payloads, or diagnostic bundles.

## Architecture

### Process Model

The application has three executable surfaces:

- `deltaweave-daemon`: a long-running per-user process that owns iroh endpoints, filesystem watchers, scheduling, redb databases, peer authorization, ticket lifecycle, and synchronization execution;
- `deltaweave-gui`: the Tauri 2 shell and TypeScript UI, which sends commands and renders snapshots/events;
- `deltaweave`: the existing CLI, migrated incrementally to the same daemon API for operations that share persistent state.

Only the daemon may open job indexes, content stores, access stores, or identity state. This removes current cross-process redb lock failures and keeps the in-process materialization lock authoritative. The daemon enforces a single per-user instance. A second invocation connects to the existing instance.

Windows login starts the GUI hidden to the tray. The GUI starts the daemon if it is not already running. Closing the window leaves the tray and daemon running. Opening the window is optional after login. Quit Sync Engine pauses jobs, flushes durable state, shuts down endpoints, then exits both processes.

### Crate Boundaries

- Existing `deltaweave-sync`, `deltaweave-net`, `deltaweave-index`, and `deltaweave-store` remain synchronization authorities.
- A new `deltaweave-daemon-api` crate defines versioned commands, snapshots, events, identifiers, and error codes. It contains no storage or UI implementation.
- A new `deltaweave-daemon` crate owns configuration, job supervisors, progress aggregation, IPC transport, and Windows lifecycle integration.
- A new `deltaweave-gui/src-tauri` crate embeds Tauri commands that connect only to the daemon.
- A new `deltaweave-gui/ui` package contains the light-only frontend.

No data-path logic is duplicated in TypeScript or Tauri commands.

## IPC Contract

IPC is local-only and per-user. On Windows it uses a named pipe with an ACL restricted to the current user. Messages are length-delimited JSON for inspectability during the first release. Every request contains `protocol_version`, `request_id`, and a tagged command. Every response echoes `request_id`; events contain a monotonically increasing `event_id` and `state_revision`.

Connection begins with capability negotiation. Incompatible major versions fail with an Upgrade required error. The GUI reconnects after daemon restart, requests a complete state snapshot, and then resumes events after the latest revision. Events are coalesced to at most 10 updates per second per active job so UI rendering cannot throttle transfer work.

Commands include:

- list/create/update/remove/pause/resume jobs;
- preview and confirm initial synchronization;
- sync now and cancel current pass;
- list/discover/add/revoke peers;
- issue/redeem pairing tickets;
- list and resolve conflicts;
- get/update global resource policies;
- fetch diagnostics summary and create a redacted diagnostic bundle;
- stop daemon.

The daemon emits job state, progress, conflict, peer availability, retry, warning, and lifecycle events. Raw keys and ticket codes appear only in direct command responses that requested them and are never replayable events.

## Configuration and Data Model

Daemon configuration is durable, versioned, and separate from per-root indexes. It uses one redb database under the per-user application data directory. Schema migrations are forward-only, transactional, and backed up before mutation.

A sync job stores:

- stable job ID and display name;
- canonical local root and dedicated state root;
- peer ID and last known endpoint addresses;
- direction: bidirectional, send-only, or receive-only;
- continuous/manual mode and paused state;
- polling, bandwidth, and concurrency policy overrides;
- conflict rule, if the user explicitly chose one;
- creation and last-success metadata.

A root may belong to only one enabled job. State roots cannot overlap synced roots. Identity files remain outside writable sync roots. Existing root-binding checks remain authoritative.

Peer records store endpoint ID, user-assigned name, trust state, credential generation, addresses, last seen time, and revocation state. Secret keys and pairing ticket secrets are stored separately with user-only permissions.

## Pairing and Job Creation

The Add Folder wizard has four stages:

1. choose a local folder;
2. select an automatically discovered LAN peer or paste an existing `dwpair1` ticket;
3. choose bidirectional, send-only, or receive-only mode;
4. run a dry preview and explicitly confirm.

Issued tickets default to 10-minute expiry and one successful redemption. The UI displays both endpoint fingerprints before trust confirmation. Revocation immediately blocks new sessions and marks affected jobs as action required. The first GUI release does not rotate node keys automatically. Credential replacement is revoke-then-reissue, with a new identity generated only by an explicit user action.

LAN discovery advertises only product identity, endpoint ID, protocol compatibility, and a rendezvous hint. It never grants authorization. Authorization still requires an allow-list entry or successful ticket redemption.

## User Experience

### Main Window

The fixed light theme uses Windows-native Segoe UI conventions. The dashboard shows:

- selected job and peer connection state;
- current throughput, current file, progress, ETA, and saved bytes/extents;
- Sync now, Pause, and Open folder actions;
- action-required conflicts alongside transfer status;
- a compact job switcher for multiple roots and peers.

Status has three severities:

- normal: synchronized, scanning, transferring;
- attention: peer offline, backoff, sharing violation, retry queued;
- action required: conflict, insufficient disk, permission failure, incompatible protocol, revoked peer.

### Tray

The tray tooltip gives aggregate status. Its menu offers Open DeltaWeave, Sync all now, Pause all, recent action-required items, and Quit Sync Engine. Tray notifications are reserved for pairing decisions, action-required states, and completion of user-initiated large transfers.

### Conflicts

The daemon preserves both verified contents before presenting a conflict. The UI displays path, peer, size, modification metadata, and hashes. The user chooses Keep this PC, Keep peer, or Keep both. A rule may be applied to future conflicts for one job, but destructive automatic overwrite is never the default.

### Errors

Domain errors have stable codes and safe user messages. Each action-required error includes one or more allowed remediation actions. Detailed technical context remains available in diagnostics, redacted before export. Backoff states show the next retry time and can be retried immediately.

## Performance Policy

Transfer and hashing remain inside Rust core paths. The GUI cannot synchronously enumerate full trees or receive one event per chunk. Progress aggregation samples internal counters and publishes no more than 10 Hz. Long lists use pagination in IPC and virtualization in the UI.

Initial targets on the test Windows PC are:

- daemon idle CPU below 1%;
- GUI idle resident memory at or below 150 MB;
- daemon idle resident memory at or below 100 MB;
- responsive window interaction while four ISO files are added or transferred;
- no measurable sustained-throughput regression greater than 5% versus the same release's headless daemon path.

Users may set a global bandwidth limit, per-job pause, battery policy, and metered-network policy. Resource controls alter scheduling, not correctness or verification.

## Cancellation and Recovery

Pause prevents new sync passes and reaches a safe checkpoint in the active pass. Cancel requests are cooperative and never interrupt an atomic materialization or metadata commit. After cancellation, existing journal and verification rules make the next pass safe to retry.

Daemon crashes, GUI crashes, Windows logoff, network loss, sharing violations, and peer restarts all converge through durable state and bounded retry. On startup, each job reports Recovering until its index, journal, watcher, and peer state are re-established. No success state is shown until local and remote verified roots match.

## Security

Existing deny-by-default peer authorization, path validation, verified CAS, Merkle verification, and root-binding invariants remain mandatory. The GUI does not expose allow-any-authenticated in normal settings. Named-pipe callers are authenticated as the current Windows user. Sensitive files use owner-only ACLs.

The first GUI release does not claim to close existing pre-production gaps such as handle-relative path operations, at-rest encryption, complete metadata synchronization, or safe tombstone GC. It must not describe those gaps as completed in UI or release notes.

## Packaging

The Windows bundle includes GUI, daemon, CLI, runtime assets, and an installer. Installation configures per-user login startup for the daemon and Start Menu entries for the GUI and diagnostics. Uninstall offers to retain or remove configuration and state; synced user files are never removed.

The initial test installer may be unsigned. Production distribution remains gated on a trusted code-signing certificate and signed reproducible artifacts. The existing ZIP remains available for portable diagnostic use but is not the primary GUI installation path.

## Testing and Release Gates

### Automated

- daemon API serialization and major-version compatibility tests;
- per-user single-instance and caller authorization tests;
- job configuration migration and root-overlap tests;
- progress coalescing, pagination, reconnect, pause, and cancellation tests;
- ticket expiry, one-use redemption, revocation, and restart-on-fixed-port tests;
- conflict preservation and all three resolution actions;
- daemon restart, GUI disconnect, watcher overflow, peer loss, disk-full admission, and sharing-violation recovery;
- frontend component and accessibility tests for every state and wizard stage;
- installer smoke test in a clean Windows VM.

### Windows Hardware

The actual Windows PC must synchronize against `172.30.1.22` using the persistent server root. Evidence must cover:

- clean install and first launch;
- login startup and tray-only operation;
- folder and peer setup within 60 seconds;
- bidirectional small-file synchronization;
- four ISO files added while the GUI remains responsive;
- interruption, daemon restart, reconnect, and verified resume;
- GUI conflict review and each resolution choice;
- matching final hashes and verified Merkle roots;
- measured idle resources and throughput comparison against headless mode.

A passing mockup, unit suite, or Linux build is not sufficient to claim the Windows GUI release complete.

## Delivery Sequence

1. Extract a versioned daemon API and add progress/cancellation hooks to the sync engine.
2. Implement single-owner daemon configuration, job supervision, named-pipe IPC, and Windows startup.
3. Move pairing, revocation, and fixed-bind lifecycle behind daemon commands.
4. Build the Tauri shell, tray, light dashboard, job wizard, status, and conflict UI.
5. Add installer and upgrade behavior.
6. Run automated gates, Windows VM tests, and physical Windows-to-22 ISO soak.
