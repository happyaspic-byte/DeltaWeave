# DeltaWeave

[![CI](https://github.com/happyaspic-byte/DeltaWeave/actions/workflows/ci.yml/badge.svg)](https://github.com/happyaspic-byte/DeltaWeave/actions/workflows/ci.yml)
[![Security](https://github.com/happyaspic-byte/DeltaWeave/actions/workflows/security.yml/badge.svg)](https://github.com/happyaspic-byte/DeltaWeave/actions/workflows/security.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

![DeltaWeave connects a Windows PC and NAS with verified encrypted chunk transfer](docs/assets/deltaweave-hero.webp)

DeltaWeave is a Rust foundation for authenticated, content-defined P2P file
synchronization. It combines an authoritative local filesystem index, FastCDC
chunking, BLAKE3 integrity, a durable content-addressed store, and iroh's
encrypted QUIC transport.

> **Project status: pre-alpha field preview.** The workspace is v0.4.0. It connects the persistent
> index to deterministic Merkle reconciliation, version-vector conflicts, and
> verified two-way folder synchronization. Windows/Synology hardware soak,
> installers/services, symlink materialization, and on-demand VFS are still
> incomplete. Do not use DeltaWeave as the only copy of important data.

## Actual usage

This GIF reconstructs the actual peer-accept logs and JSON result from the
v0.2.0 Windows release job in a readable terminal. It shows package verification
as **before → during → after → result**.

![DeltaWeave 실제 실행 전, 중, 후, 결과](docs/assets/deltaweave-quickstart.gif)

The [historical usage gallery](docs/USAGE_GALLERY.md) includes full-size frames,
the local-index lifecycle, and the Linux ARM64 package result. These are rendered
v0.2/v0.3 examples, not a fresh v0.4.0 run or evidence of physical DSM hardware
validation. The rendering script embeds the recorded text rather than running the CLI.

The next animation reconstructs a separate v0.3 `sync-once` run: two independent roots
exchange files, preserve simultaneous edits, propagate a deletion, and finish
with a one-node/no-action Merkle fast path.

![DeltaWeave 양방향 동기화 전, 중, 후, 결과](docs/assets/deltaweave-sync-lifecycle.gif)

## What works today

| Capability | Workspace v0.4.0 status |
| --- | --- |
| Streaming FastCDC manifests | Implemented and unit-tested |
| Chunk and whole-file BLAKE3 verification | Implemented and unit-tested |
| Durable chunk CAS + redb metadata | Implemented with restart tests |
| Authenticated iroh/QUIC transfer | Implemented with local P2P integration tests |
| Missing-chunk-only re-transfer | Implemented with insertion/reuse tests |
| Allow-listed peer authorization | Implemented for push and folder read/write access; deny by default |
| Safe replacement and recovery journal | Implemented baseline; old content goes to private trash |
| Persistent local file/directory index | Implemented with restart and operation-storm tests |
| Native watching and adaptive debounce | Implemented with full-rescan and polling fallbacks |
| Rename correlation and deletion tombstones | Implemented using stable OS identity where available |
| Case/Unicode collision detection | Implemented; collisions are reported without overwriting names |
| Locked/mutating file retry queue | Implemented with persistent exponential backoff |
| Distributed Merkle reconciliation | Implemented with partial-subtree queries and final-root verification |
| Bidirectional folder create/update/delete/rename | Implemented; rename is causal delete/create with chunk reuse |
| Version-vector conflict handling | Implemented; deterministic winner and portable conflict copy preserve both file contents |
| Partition/restart convergence | Implemented in three-peer model and two-peer restart integration tests |
| Continuous retrying synchronization | Implemented via `sync`, with bounded exponential backoff |
| Symlink/special-file materialization | Safely rejected; indexed but not followed |
| Windows CFAPI / Linux FUSE on-demand files | Planned |

The scope and acceptance gates for later phases live in [ROADMAP.md](ROADMAP.md).

## Repository layout

| Crate | Responsibility |
| --- | --- |
| `deltaweave-core` | Stable hashes, manifests, portable paths, and version vectors |
| `deltaweave-cdc` | Streaming FastCDC, BLAKE3 manifests, and delta planning |
| `deltaweave-index` | Authoritative scans, watcher hints, collision checks, tombstones, and retries |
| `deltaweave-reconcile` | Canonical Merkle trees, causal merge, conflicts, and apply planning |
| `deltaweave-store` | Verified chunk storage, redb metadata, journaled materialization |
| `deltaweave-net` | iroh identity, authorization, Merkle queries, and causal push/pull protocols |
| `deltaweave-sync` | Retry-safe two-peer orchestration and independent convergence verification |
| `deltaweave` | JSON CLI for identity, indexing, serving, syncing, diagnostics, and self-test |

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) and
[docs/PROTOCOL.md](docs/PROTOCOL.md) for the invariants behind these boundaries.

## Download a test release

The pre-alpha [v0.4.0 release](https://github.com/happyaspic-byte/DeltaWeave/releases/tag/v0.4.0)
provides ready-to-run packages for:

- Windows x86-64
- Synology DSM on x86-64
- Synology DSM on ARM64 (`aarch64`)

Each package includes the executable, license, cross-device test guide, and
local-index test guide. After extracting the correct package, run the isolated
end-to-end check from the extracted package directory (PowerShell uses
`.\deltaweave.exe self-test`):

```bash
./deltaweave self-test
```

It performs encrypted delta transfers and additionally verifies bidirectional
exchange, deterministic conflict copies, deletion propagation, local indexing,
rename correlation, tombstones, and zero-action restart recovery. See
[Windows PC ↔ Synology testing](docs/TESTING_WINDOWS_SYNOLOGY.md) for the full
cross-device procedure and checksum verification.

For a Portainer-managed Synology receiver, an AI operator can follow the
guardrailed [AI Portainer setup runbook](docs/AI_PORTAINER_SETUP.md). The
repository includes a hardened Stack definition and a container workflow that
builds and self-tests images for `linux/amd64` and `linux/arm64`. Check the selected
image separately; CI configuration alone does not prove a deployment works.

## Build and test

Run from a repository checkout. Install Git, rustup, and the native linker/C/C++
build tools for your host: on Linux a compiler toolchain such as `build-essential`,
or on Windows the MSVC C++ Build Tools and Windows SDK. `rust-toolchain.toml`
pins Rust **1.91.0** with rustfmt and Clippy; `Cargo.lock` fixes dependency
resolution. Initial toolchain/dependency downloads require network access.

```bash
rustup show active-toolchain
cargo build --locked --workspace
cargo run --locked -p deltaweave -- --help
cargo test --locked --workspace --all-targets --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

`cargo build` does not add the CLI to `PATH`. Use
`cargo run --locked -p deltaweave -- <command>` during development, or install from
this checkout for the bare `deltaweave` commands below:

```bash
cargo install --locked --path crates/deltaweave-cli
deltaweave --version
```

The install directory is Cargo's configured `bin` directory (normally
`~/.cargo/bin`, or `%USERPROFILE%\.cargo\bin` on Windows); it must be on `PATH`.
Alternatively run `./target/debug/deltaweave` or `.\target\debug\deltaweave.exe`
after a debug build. The source checkout is version `0.4.0`.

CI is configured to repeat the quality gates on Linux and the full test suite on
Windows. Run the release-oriented checks in Bash with
`./scripts/verify-release.sh`; see [CONTRIBUTING.md](CONTRIBUTING.md) for prerequisites,
release builds, rustdoc, and additional loopback checks. The
[v0.3 quality report](docs/QUALITY_REPORT_V0.3.md) is a historical assessment;
[the documentation audit](docs/DOCUMENTATION_AUDIT.md) records this checkout's
actual checks and remaining uncertainty.

## Index or watch a folder

Run one authoritative scan. Keep private state and the node identity outside the
indexed root. The identity file and index database (or its parent directory when
nested under the root) are excluded automatically; other private paths require
explicit `--ignore` options.

```bash
mkdir -p sync-root private
deltaweave scan \
  --root ./sync-root \
  --state ./private/index.redb \
  --identity ./private/node.key
```

For continuous local indexing, native events are debounced and treated as hints.
Periodic complete scans remain authoritative; watcher loss activates a short
polling fallback.

```bash
deltaweave watch \
  --root ./sync-root \
  --state ./private/index.redb \
  --identity ./private/node.key
```

Both commands emit JSON reports containing changes, retries, and cross-platform
name collisions. Add `--include-records` to `scan` when an operator needs the
complete persistent record and retry lists. See
[local-index testing](docs/TESTING_LOCAL_INDEX.md) for safe validation steps.

## Try a direct local transfer

Install the CLI as above and create a persistent identity on each node. For this
two-terminal local example, use the same working directory in both terminals:

```bash
deltaweave init --identity receiver.key
deltaweave init --identity sender.key
```

Start the receiver with the sender's printed endpoint ID. Authorization is
mandatory unless the explicitly unsafe testing flag is supplied. Replace angle-bracket
placeholders with the printed values; do not type the brackets. Keep receiver
root and private state as separate directories on the same filesystem, because
replacement/deletion moves old content into state trash with a filesystem rename.

```bash
deltaweave serve \
  --root ./received \
  --state ./receiver-state \
  --identity receiver.key \
  --allow-peer <SENDER_ENDPOINT_ID> \
  --bind 0.0.0.0:49152 \
  --direct-only
```

`--bind` keeps the receiver UDP port stable across restarts. Open that UDP port in
the host firewall, then copy the receiver's `endpoint_id` and one
`direct_addresses` value from its JSON output. For a same-host test use
`127.0.0.1:49152`; for another host use a reachable LAN address. Prepare an existing
copy of a test file at `./large-file.bin`, then in the second terminal run:

```bash
deltaweave push ./large-file.bin \
  --remote-path archive/large-file.bin \
  --peer <RECEIVER_ENDPOINT_ID> \
  --direct <RECEIVER_IP:PORT> \
  --identity sender.key \
  --direct-only
```

Add `--state ./private/sender-state` to persist manifests for unchanged files
when a stable identity and reliable change-time (Unix inode ctime) are
available; omit it to run without a sender cache. Windows and other platforms
without that change-time token skip the cache and fresh-generate. Direct-push
adoption also requires a reliable change-time before skipping rehash; otherwise
it verifies with `hash_stable_file`.

Files of at least 8 GiB automatically use larger FastCDC chunks when all three
chunk sizes equal the default profile. This reduces manifest size and durable
chunk-store overhead; protocol chunk-count and control-frame limits still apply. Any non-default
`--min-chunk`, `--avg-chunk`, or `--max-chunk` value disables this selection.

Run the command again after editing the source. The receiver requests only
unique chunks not already present and returns a JSON receipt with transferred
bytes and reused extents.

Internet mode uses iroh discovery and encrypted relay fallback. Omit
`--direct-only`, and supply the relay URLs printed by the receiver when needed.

## Synchronize a Windows folder with Synology

Initialize the Windows identity at the path used below, then put its printed
endpoint ID on the Synology `serve --allow-peer` list:

```powershell
.\deltaweave.exe init --identity C:\DeltaWeave-Private\windows.key
```

Keep that receiver running. On Windows, use the receiver values printed by
`serve`; `sync-once` creates the root if it does not yet exist:

```powershell
.\deltaweave.exe sync-once `
  --root C:\DeltaWeave-Sync `
  --state C:\DeltaWeave-Private\state `
  --identity C:\DeltaWeave-Private\windows.key `
  --peer <SYNOLOGY_ENDPOINT_ID> `
  --direct <SYNOLOGY_IP:UDP_PORT> `
  --direct-only
```

`sync-once` returns success only after fresh local and remote snapshots have the
same deterministic Merkle root. Use `sync` with the same arguments for continuous
operation: native local events trigger a pass after the default 750 ms quiet
window, while polling after each successful pass discovers remote-only changes (default
five-second wait, plus scan/transfer time). Watcher startup
failure falls back to polling, and transient failures retry with exponential backoff.
Every pass refuses incomplete scans, retry-queued files, cross-platform name
collisions, stale causal writes, and concurrent records that have not first
gone through deterministic reconciliation.

See the [CLI reference](docs/CLI.md) for every command, default, JSON output shape,
exit behavior, and logging configuration. The wire API is described in
[PROTOCOL.md](docs/PROTOCOL.md); this project has no HTTP API.

## Security model

- iroh authenticates endpoints cryptographically and encrypts transport traffic.
- DeltaWeave independently authorizes the remote endpoint ID.
- Every received chunk and reconstructed file is verified before commit.
- Wire paths reject traversal, absolute paths, Windows device names, and invalid
  deserialized values.
- Existing content is preserved in state trash before replacement.
- The v2 synchronization protocol rejects stale, unmerged-concurrent, and
  equal-clock/different-state writes; exact same-state retries are accepted.
  Legacy `push` is an authorized one-file overwrite without that causal precondition.

This baseline does not yet defend perfectly against a privileged or racing local
attacker and does not materialize links or special files. Read
[docs/THREAT_MODEL.md](docs/THREAT_MODEL.md) before exposing a receiver or using
real data. Report vulnerabilities according to
[SECURITY.md](SECURITY.md).

## License

DeltaWeave is licensed under the [MIT License](LICENSE).
