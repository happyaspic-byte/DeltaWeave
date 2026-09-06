# Folder Share Keys Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox syntax for tracking. User authorization permits autonomous implementation and local main integration after verification. Do not pause for routine design approvals.

**Goal:** Deliver folder-scoped RO/RW key enrollment, enforced permissions, automatic durable synchronization, the preserved console, real browser evidence, documentation and verified main integration.

**Architecture:** An owner-mediated v3 share endpoint multiplexes isolated folder runtimes and enforces a durable invitation/membership registry. Members retain distinct local identities and accept snapshots only from their issuer; read-only application preserves local work outside the share. Existing causal merge/CAS/recovery engines and legacy connections remain available under explicit compatible paths.

**Tech Stack:** Rust 1.91, iroh 1.1, Ed25519 from iroh, postcard, redb, Tokio, Axum; existing preserved React/TypeScript/Vite console.

**Spec:** `docs/superpowers/specs/2026-09-06-folder-share-keys-design.md`

## Global Constraints

- Start from main `22664cf559aa08063e0e7756e5babc12ed67b107` in the existing isolated `share-key-sync-update` worktree.
- Preserve all already integrated security, bug, performance, storage, causal conflict and recovery improvements; never replace engine files wholesale from `preserve/main-wip-20260906`.
- Preserve all existing worktrees, preserve branches, live user process and user data.
- Keep device secrets, web administrator credentials, invitation bearers and session credentials separate; never log, snapshot or expose secrets in errors or activities.
- Use one persistent device-wide identity/endpoint for new managed shares. Legacy folders retain their existing identity and data.
- Keep each imported index's DB-bound logical ReplicaId independent of the new transport identity; never reset vectors or weaken normal root/replica binding.
- Only the owner serves managed snapshots. Members only synchronize against the authenticated issuing endpoint. Read/write membership does not grant membership management.
- Check actual durable issuance/membership records, folder scope and current permission on every request and at mutation; revocation affects existing sessions and persists.
- RO synchronization preserves local work outside the shared root and never propagates local modifications or tombstones; OS readonly flags have a different meaning.
- Preserve web authentication, Host, Origin, CSRF and filesystem path protection; preserve existing CLI/API and data formats or provide an explicit migration without deleting folders.
- Keep the preserved console's typography, tokens and common components. Default entry actions are `폴더 공유` and `키로 연결`.
- Test actual independent identities and temporary roots; browser end-to-end must use real backend and actual file transfer, not API mocks alone.
- Run full quality gates and necessary Windows CI; no skipped/deleted tests or weaker authorization to hide a failure. Record untested networks/platforms honestly.
- Integrate locally into main only after required verification. Push only with session authorization and then check remote CI.

---

### Task 1: Preserve the running console on the secure main baseline

**Files:** selectively import `web/`, `crates/deltaweave-control/`, `crates/deltaweave-web/{build.rs,src/assets.rs,src/auth.rs,src/routes.rs,src/routes/tests.rs}` from preserved commit; adapt `crates/deltaweave-web/src/lib.rs`, `crates/deltaweave-cli/{Cargo.toml,src/main.rs}`, workspace manifests/lock, Docker/CI web asset build inputs; add focused observer/inventory/pause/resource hooks in net/sync. Keep legacy web source, tests, executable flags and API. Baseline Windows output failure is diagnosed here; fix the actual formatter defect with a regression.

**Interfaces:** produces `deltaweave_web::run(WebConfig)`, `deltaweave_control::Manager`, preserved React console API, and required `TransferObserver`, `Inventory`, `Server::{pause,resume,inventory}`, `SyncEngine::{inventory,sync_once_observed}` without weakening existing operations. Existing `Config`, `WebApp`, `start_server` and CLI contracts remain supported.

- [x] Capture baseline evidence and read the preserved UI audit. Verify current repo source before choosing each imported hunk.
- [x] Run the existing and imported behavior tests against the baseline/integration and observe any missing API/behavior failures. For Windows path output, add a literal Windows path containing backslashes to the real formatter test and prove the current escaping is wrong.
- [x] Import the preserved console/control and adapt to main. Keep main's private directory creation, namespace validation, async pre-apply rescan, causal preconditions, writer draining and safe errors. Import only needed hooks; do not copy the prior net/sync/index/store implementation.
- [x] Build web assets and verify behavior using the preserved tests plus all legacy web/API/CLI tests. Use real receiver pause/resume tests, inventory and transferred-byte observation tests.

```bash
npm --prefix web ci
npm --prefix web test
npm --prefix web run build
cargo test --locked -p deltaweave-control -p deltaweave-web -p deltaweave --all-targets --all-features
cargo test --locked -p deltaweave-net -p deltaweave-sync --all-targets --all-features
```

- [x] Commit the focused prerequisite, write the report with commands/results, and obtain independent spec/quality review before the next implementation task.

### Task 2: Implement versioned invitation keys and the v3 authorization boundary

**Files:** create focused modules under `crates/deltaweave-net/src/share/` and a public module export; modify transport helpers in `crates/deltaweave-net/src/lib.rs` only as needed to reuse verified transfers. Add a bounded dedicated share-metadata slot and atomic record-adoption variants in `deltaweave-index` so accepted records, causal ceilings and provenance commit together without changing existing bindings/adoption contracts. Add common root admission and focused hooks at legacy `SyncEngine::open`, the control worker lifetime lock, and CLI direct publishing paths where the underlying entry points do not already enforce admission. Add `crates/deltaweave-net/tests/shares.rs` and unit adversarial tests. Add dependencies only when existing maintained iroh/serde/postcard/redb facilities do not suffice.

**Interfaces:** produces `share::Permission::{ReadOnly,ReadWrite}`, a secret-redacting `ShareTicket` encode/parse/verified-preview type, durable owner registry and shared endpoint service, owner share creation/load, key issue/rotate/revoke, membership list/revoke, online validation/enrollment and a member session using the same `fetch_snapshot/pull_record/push_record/apply_metadata` semantics as `SyncSession`. Final Rust signatures are recorded in the task report before Task 3 starts; transport configuration carries owner ID + share ID, never a caller-trusted role. A single endpoint is cloned for member sessions rather than binding its private key again.

- [x] Write behavioral tests for ticket corruption/version/expiry/signature, stored role matching, RO-to-RW escalation and cross-folder access. Assert exact protected filesystem state remains unchanged after attacks.
- [x] Implement the signed versioned envelope with bounded decoding and durable separate invitation/member records. Enrollment uses the authenticated `connection.remote_id()`; online authoritative validation precedes durable membership. Offline preview is clearly distinguishable from accepted enrollment.
- [x] Implement `deltaweave/share/3`, multiplexed owner runtimes and the common transfer session adapter. Test v1/v2 negotiation fails on the share endpoint and all RO write/delete/directory requests are denied. Recheck permissions inside query loops, after chunk reception and before materialization/adoption.
- [x] Assign fresh members a durable owner-generated logical replica distinct from transport identity. For non-empty legacy participant imports, verify a domain/version-separated retained-old-key proof bound to invitation, owner/share, new authenticated endpoint, old endpoint and its derived ReplicaId before permanently reserving the ID for that new endpoint. Reject unproved historical-ID claims and reuse of revoked bindings. Persist known causal IDs/counter ceilings from migrated owner records; bound vectors and reject unknown IDs, forged victim counters and resolver jumps. Only the authenticated writer's own counter may legitimately exceed its trusted ceiling; preserve normal global-counter jumps and one-step conflict resolver increments. Record actual peer/epoch/operation provenance, never label vector IDs as signatures.
- [x] Add tracked connection cancellation and synchronization with in-flight disk operations so completed revocation is effective immediately. Persist key and member revocation separately and test restart. Reject same-identity re-enrollment after removal; test that a new identity with another still-active invitation has explicitly documented behavior.
- [x] Use one per-share mutation gate through final authorization, materialization and adoption. Revocation persists denial, closes tracked connections and drains already-started blocking operations/chunk writers before returning. Test with deterministic barriers, not a sleep-based assumption.
- [x] Add a fixed private host/user root-admission registry independent of configurable state paths, a global check/register lock, per-root lifetime leases, and durable managed ownership sidecars outside public roots. Reject canonical exact/ancestor/descendant overlaps in either start order and through aliases, including legacy push sources inside managed roots. Exercise separate-process check/create races, stale legacy leases, managed crash states and alternate state paths. Preserve local scan/manifest behavior; deliberate trusted OS export remains outside the remote attacker model.
- [x] Reserve device/catalog and per-share private state against every public root in both registration orders and across service instances, including member and legacy state entry points. Treat the fixed admission directory itself as private. Preflight the nearest existing canonical ancestor plus missing suffix under the global lock before mkdir/catalog writes, then create/revalidate/register. Denied attempts must leave protected trees and catalogs unchanged; capacity checks must cover both fresh and retained-proof member IDs before persistence.
- [x] Use iroh N0 discovery/relay for internet mode, retain direct hints for bootstrap and explicit DirectOnly mode. Record verified upstream docs and run actual multi-folder/multi-identity QUIC tests.

```bash
cargo test --locked -p deltaweave-net --all-targets --all-features
cargo clippy --locked -p deltaweave-net --all-targets --all-features -- -D warnings
```

Include focused index transaction/compatibility tests and index static checks for
the atomic share-metadata extension. Existing root/replica metadata is never
exposed through a general arbitrary-key mutation API.

- [x] Commit, report the public API and attack evidence, and obtain independent spec/quality review.

### Task 3: Reuse causal RW sync and implement preserving RO application

**Files:** `crates/deltaweave-sync/src/lib.rs`, new `read_only.rs`/shared sync module, focused `deltaweave-index` authoritative snapshot adoption method if necessary, preserving move/recovery helpers in `deltaweave-store`, focused net/service and necessary direct-CLI constructor/adoption hooks, `crates/deltaweave-sync/tests/shares.rs` and focused store regressions.

**Interfaces:** consumes the authenticated shared-session surface from Task 2. Produces managed RW synchronization with existing `SyncReport` semantics, separate RO synchronization, and a serializable RO report including preserved conflict locations. `LocalIndex` authoritative adoption validates all records and namespace and durably removes local-only versions without changing existing data schemas.

- [ ] With separate owner/RW/RO identities and real endpoints, write tests for RW initial join, two-way add/edit/delete, deterministic conflict preservation and restart.
- [ ] Fix the reproduced preexisting cross-filesystem preservation failure: actual v2 initial transfer succeeds with root and private state on separate filesystems, but replacement and deletion fail at `fs::rename(destination, private_trash)` with EXDEV. Use one Store helper for replacement/deletion/type transitions, atomically preserving the displaced object in a durable private vault on the destination filesystem outside every registered shared namespace. Keep configured private trash when it is safely on the same filesystem; otherwise use a validated root-bound private sibling vault. Fail before mutation when safe placement cannot be established. Do not use copy-then-unlink, which loses writes through an already-open handle. Keep recovery artifacts until explicit purge, expose the actual preserved path, and distinguish disposable incoming staging.
- [ ] Journal unique path-change attempts and exact artifact/staging paths through prepare, capture, install/absence, index adoption and commit. Recover idempotently after each interruption; use atomic no-replace install/restore and revalidate captured data so concurrent local recreation is never clobbered. Cover real two-filesystem replacement/deletion/type transitions, open-handle writes, pre/post-capture races and symlink parent/leaf cases while retaining existing causal/hash/path and mutation-gate checks. Shared RO recovery reuses the same artifact rather than preserving bytes twice.
- [ ] Bind generic RW/legacy attempts to their exact causal record and trusted source context before capture. A materialized but unadopted remote target must not be rescanned as an owner/local edit. Under the existing runtime gate and single index, recheck authorization/epoch, causal preconditions and exact filesystem state before atomic recovery adoption with ceilings/provenance. For a revoked actor, safely roll back an unchanged pending target when the prior artifact/absence is conclusive; otherwise retain both objects and expose recovery-required with safe retry guidance. Test active restart, revoked/epoch mismatch, divergence and exact provenance preservation.
- [ ] Adapt existing RW reconciliation to shared sessions without bypassing content verification, pre-apply rescan or convergence checks. Keep legacy `sync_once` behavior intact.
- [ ] Write failing RO tests: local edit and remote edit preserve exact local bytes; local-only additions never reach owner/RW; local deletions restore from owner; remote deletions preserve modified local bytes outside root; file/directory transitions and restarts retain safe recovery.
- [ ] Implement authoritative RO application: observe local changes, stage and verify owner content, persist local conflict copies/metadata in private state, recheck local preconditions, apply owner actions, adopt complete authoritative records, then verify filesystem/index against owner. Abort/retry on racing local modifications.
- [ ] Persist prepared/preserved/materialized/adopted recovery stages and the last trusted owner checkpoint. Inject interruption at each stage; prove exact local bytes survive and owner rollback/equal-version divergence/missing tombstones are rejected.
- [ ] Test laundering resistance using a malicious RO v2/v3 snapshot endpoint: managed RW refuses it because only the issuer owner is a valid source. Prove unchanged owner and another RW root, not merely a disabled client button.

```bash
cargo test --locked -p deltaweave-index -p deltaweave-sync --all-targets --all-features
cargo clippy --locked -p deltaweave-index -p deltaweave-sync --all-targets --all-features -- -D warnings
```

Include store tests/static checks and an actual two-filesystem transfer/recovery
run for the preservation fix; record the filesystem identities in evidence.
Check the affected net/caller surfaces as well. Keep Store independent of net by
passing a private-recovery reservation callback from admitted callers. Ordinary
Windows replacement/deletion must use a working safe primitive; cross-compilation
does not replace the later required Windows runtime evidence.

- [ ] Commit, record real network evidence, and obtain independent spec/quality review.

### Task 4: Deliver automatic shared-folder workers and the console flow

**Files:** `crates/deltaweave-control/src/{config,model,worker,lib}.rs` and focused share manager module; narrow net share-service existing-membership resume and authenticated-operation observation hooks; `crates/deltaweave-web/src/routes.rs` and route tests; `web/src/{App,components,types,api,styles}` plus focused share components and behavior tests. CLI web entry and docs are updated where needed.

**Interfaces:** consumes Task 2 endpoint/registry and Task 3 sync results. Produces authenticated folder browser, create-share, preview-key, join, key-lifecycle and member-management APIs; persistent worker state survives restart; snapshots redact bearers and expose truthful status and actual peer observations. Existing folder/config formats get additive defaulted fields and in-place conversion.

- [ ] Write Manager/API tests for key preview without enrollment, storage selection and automatic enrollment; distinct local identities; persistence/restart; private-path/root-overlap protection; management forbidden on participant roles; CSRF/Origin/Host/auth checks on all new routes.
- [ ] Integrate one device-wide shared endpoint with separate owner folders/member workers. Poll and retry automatically with bounded backoff. Persist pending enrollment securely for an offline issuer. On revocation stop syncing and surface revoked, never continue on cached permission.
- [ ] Add an authenticated resume-existing-membership operation for a lost enrollment reply followed by invitation expiry/revocation. It queries only the authenticated device's active membership for the expected owner/share, validates returned bindings and persists the relationship; it never enrolls, changes role/replica or revives a revoked member. Test the real response-loss/restart case and terminal revoked/nonmember cases without key re-entry.
- [ ] Implement real server folder browsing and explicit conversion of existing configured folders while preserving root/state/index. Existing manual connections continue under advanced settings. Document which old peer connections need a key during conversion.
- [ ] Exercise owner and participant conversion of non-empty indexes using retained logical ReplicaIds with new transport identities, including the participant's retained-legacy-key proof; then restart and edit again. Fresh joins open a new index only after enrollment returns the owner-assigned logical ID. Make creation/enrollment retries idempotent across marker, registry and control-config writes; persist pending invitation only in private state and discard it after membership succeeds.
- [ ] Implement primary `폴더 공유` and `키로 연결` flows, both permission-key copy controls, permission preview and destination selection, result details, connected devices, key rotation/revocation and member removal. Explain scope of each revocation and rejoin limitations at the relevant action.
- [ ] Show waiting/offline/initial-sync/up-to-date/local-conflict/revoked/invalid-key distinctly using actual runtime evidence. Protect long key entry from layout overflow, keep secrets out of persistent browser storage and debug/activity output, and clear sensitive UI before screenshots.
- [ ] Observe authorized owner operations, including query-only passes, with start/finish and concurrency-safe peer tracking. A membership row or stale inventory is not connection evidence. Managed inventory refresh follows load/completed work or debounced changes; do not duplicate the legacy 250ms full-index polling.
- [ ] Test copy success/failure, labels, keyboard movement/focus restoration, mobile width, contrast and all existing console navigation/settings/activity features.

```bash
npm --prefix web test
npm --prefix web run build
cargo test --locked -p deltaweave-control -p deltaweave-web --all-targets --all-features
```

- [ ] Commit, write API/browser-check instructions, and obtain independent spec/quality review.

### Task 5: Prove real end-to-end behavior and finish documentation

**Files:** real browser/integration harness under `scripts/` or `web/tests/`; existing CI workflow for frontend and shared tests; `README.md`, `docs/{WEB_UI,CLI,PROTOCOL,ARCHITECTURE,THREAT_MODEL}.md`, new `docs/SHARE_KEYS.md` and `docs/SHARE_KEYS_VERIFICATION_2026-09-06.md`.

**Interfaces:** consumes actual compiled servers, shared flows and persisted state. Produces reproducible evidence, isolated running previews, Korean connection/migration instructions and an itemized verification record linked to spec completion requirements.

- [ ] Launch owner and at least two peers in separate temporary directories with distinct persistent identities, dedicated source files and private state. Capture all logs to private files; do not print secrets.
- [ ] In real browsers copy both key types, paste on peers, select destination through folder UI, start synchronization, then assert actual files. Exercise RW bidirectional mutations and RO nonpropagation/conflict preservation through the running workers.
- [ ] Restart processes, interrupt/recover peer connections and keep the owner offline then restore it; prove automatic recovery without key entry. Repeat key/device revocation and inspect files/connection effects, including retained sessions.
- [ ] Add and actually run a focused external N0 relay test using maintained iroh relay-only endpoint construction and selected-path observation around the unchanged production v3 handlers. Use distinct identities and an ID-only key, assert no IP transport/path, and verify real pulled file bytes. A separate opt-in network test is acceptable only when the final gates explicitly execute it and retain its result; never leave it unrun or use a skip to conceal failure. Do not alter host networking or add a user-facing product mode solely for this test.
- [ ] Audit sensitive data absence in logs/activity/errors/screenshots and run responsive/keyboard/copy/contrast checks. Retain only redacted screenshots/evidence.
- [ ] Run the whole documented verification scope below and diagnose/fix baseline or introduced failures with regressions. The current Windows path-output failure must be resolved; Windows execution is a separate required evidence item.

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --all-features
cargo test --locked --workspace --doc --all-features
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --all-features
cargo build --locked --workspace --release --all-features
cargo run --locked -p deltaweave -- self-test
npm --prefix web test
npm --prefix web run build
bash scripts/tests/test-p2p-loopback.sh
./scripts/test-p2p-loopback.sh
git diff --check
```

- [ ] Update usage/security/protocol/migration docs and record each verified platform/network and remaining unverified condition. Preserve earlier accurate historical reports, labeling new results separately.
- [ ] Commit and obtain whole-branch review of all implementation, evidence and requirement coverage. Resolve material issues before integration.

### Task 6: Integrate verified changes and deliver the preview

**Files:** local Git main and integration evidence; no deletion of worktrees/branches.

**Interfaces:** consumes passing checks and independent review. Produces local main with the verified implementation and a Korean final report containing user instructions, key scope/permission model, main files, actual test results, limitations and preview addresses.

- [ ] Recheck current main, worktree dirty states and remote state before integration. Reconcile any new main changes without overwriting unrelated work, then run checks justified by that merge.
- [ ] If Windows CI needs an unpushed branch, present the exact verified commits and remote action for final approval, explaining that the user's original instruction makes remote push conditional on authorization. Do not publish before an answer. Continue all independent local work while awaiting it.
- [ ] Once required verification passes, integrate into local main using a safe fast-forward or reviewed merge. If remote push was authorized, push and inspect live remote Linux/Windows CI handles through completion; fix actual failures and reverify.
- [ ] Verify the preview still serves compiled current code with dedicated test data. Audit every spec requirement against current files, command outputs, actual browser/filesystem evidence and Git/CI state before marking the goal complete.
