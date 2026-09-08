# Web UI validation — 2026-09-05

> Historical record restored from `preserve/main-wip-20260906`. Results below
> describe the September 5 source snapshot and test scope; they are not validation
> of the September 8 integration or its swarm V3 functionality. Referenced local
> evidence paths belong to that historical run and may no longer be available.

The browser console uses the real Rust engine and embedded production assets.
Validation was performed on the Linux development server with two dedicated local
roots, separate identities and private state. Existing NAS/main-pc receivers were
not imported or replaced during this work.

## Automated checks

- Full workspace tests: **139 passed**, including existing CLI/fault-injection suites.
- Final control follow-up: **14 passed** after additional activity regressions.
- HTTP/session tests: **10 passed**, including real TCP and event-stream shutdown.
- Frontend tests: **19 passed**, including deferred manual-sync/pause, rejected
  forms, reconnect/restart handling and clipboard focus restoration.
- Workspace formatting and Clippy with warnings denied: passed.
- Production frontend build: passed; all JavaScript, CSS, fonts and licenses local.

Independent review found and corrected false peer timestamps, receiver shutdown
without draining, copied identity reuse, ephemeral receiver port changes,
missing persisted reports, missing/mislabelled conflict records, discarded file
activity bytes, and disabled pause during a manual sync. Focused regression tests
reproduced these failures before their fixes.

## Real HTTP and browser checks

Authenticated API checks transferred a 4 MiB file and Korean filenames, verified
both directory contents with SHA-256, checked zero-action/zero-payload idle sync,
paused and resumed a worker, pulled receiver-side changes, and observed native
watcher updates. Invalid CSRF, Origin, Host and overlapping paths were rejected;
logout revoked access. Directory browsing and activity export returned real data.

Browser checks exercised login, folder creation/editing/removal, preservation of
local files after configuration removal, settings persistence, public information
copying, mobile navigation, a 390 px viewport without horizontal overflow,
reduced-motion, and offline/reconnection states. Sessions and access keys are not
stored in localStorage or sessionStorage.

Final runtime/screenshot evidence is in `target/web-ui-work/` and
`target/web-preview/` (ignored operational data). The administrator key is excluded
from documentation and verification reports.

## Scope

This run validates the Linux web server. Windows and container build pipelines
now build/embed the same frontend, but new Windows UI execution and Docker image
execution were not performed in this validation run. Each receiver root retains
its own endpoint identity and stable UDP port. Activity history is bounded;
conflict references are retained within that configured history limit.

Final runtime checks: release executable runs from /tmp without Vite, all LAN/Tailscale URLs answer, receiver port/settings/history survive restart and six real alternating-direction transfers converge with zero-payload idle verification. Browser contrast audit reports zero violations on desktop and mobile; directory browsing succeeds. The keyboard focus recovery regression after async directory browsing is fixed and covered by frontend tests; Korean glyph fallback is also bundled.
