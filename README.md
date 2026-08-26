# DeltaWeave

[![CI](https://github.com/happyaspic-byte/DeltaWeave/actions/workflows/ci.yml/badge.svg)](https://github.com/happyaspic-byte/DeltaWeave/actions/workflows/ci.yml)
[![Security](https://github.com/happyaspic-byte/DeltaWeave/actions/workflows/security.yml/badge.svg)](https://github.com/happyaspic-byte/DeltaWeave/actions/workflows/security.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

![DeltaWeave connects a Windows PC and NAS with verified encrypted chunk transfer](docs/assets/deltaweave-hero.webp)

DeltaWeave is a Rust foundation for authenticated, content-defined P2P file
synchronization. It combines an authoritative local filesystem index, FastCDC
chunking, BLAKE3 integrity, a durable content-addressed store, and iroh's
encrypted QUIC transport.

> **Project status: pre-alpha.** v0.2 adds a persistent scanner and native
> watcher to the verified one-file delta-transfer vertical slice. The index is
> not yet connected to distributed two-way reconciliation. Do not use
> DeltaWeave as the only copy of important data.

## Actual usage

This GIF reconstructs the actual peer-accept logs and JSON result from the
v0.2.0 Windows release job in a readable terminal. It shows package verification
as **before → during → after → result**.

![DeltaWeave 실제 실행 전, 중, 후, 결과](docs/assets/deltaweave-quickstart.gif)

The [verified usage gallery](docs/USAGE_GALLERY.md) includes full-size frames,
the local-index lifecycle, and the Synology ARM64 result. Only the terminal font
and background are normalized; status values, byte counts, and event counts come
from verified executions.

## What works today

| Capability | v0.2 status |
| --- | --- |
| Streaming FastCDC manifests | Implemented and unit-tested |
| Chunk and whole-file BLAKE3 verification | Implemented and unit-tested |
| Durable chunk CAS + redb metadata | Implemented with restart tests |
| Authenticated iroh/QUIC transfer | Implemented with local P2P integration tests |
| Missing-chunk-only re-transfer | Implemented with insertion/reuse tests |
| Allow-listed peer authorization | Implemented; deny by default |
| Safe replacement and recovery journal | Implemented baseline; old content goes to private trash |
| Persistent local file/directory index | Implemented with restart and operation-storm tests |
| Native watching and adaptive debounce | Implemented with full-rescan and polling fallbacks |
| Rename correlation and deletion tombstones | Implemented using stable OS identity where available |
| Case/Unicode collision detection | Implemented; collisions are reported without overwriting names |
| Locked/mutating file retry queue | Implemented with persistent exponential backoff |
| Distributed Merkle reconciliation | Planned |
| Full two-way conflict handling | Planned |
| Windows CFAPI / Linux FUSE on-demand files | Planned |

The scope and acceptance gates for later phases live in [ROADMAP.md](ROADMAP.md).

## Repository layout

| Crate | Responsibility |
| --- | --- |
| `deltaweave-core` | Stable hashes, manifests, portable paths, and version vectors |
| `deltaweave-cdc` | Streaming FastCDC, BLAKE3 manifests, and delta planning |
| `deltaweave-index` | Authoritative scans, watcher hints, collision checks, tombstones, and retries |
| `deltaweave-store` | Verified chunk storage, redb metadata, journaled materialization |
| `deltaweave-net` | iroh endpoint identity, authorization, and transfer protocol |
| `deltaweave` | JSON CLI for identity, indexing, watching, receive, push, and self-test |

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) and
[docs/PROTOCOL.md](docs/PROTOCOL.md) for the invariants behind these boundaries.

## Download a test release

The pre-alpha [v0.2.1 release](https://github.com/happyaspic-byte/DeltaWeave/releases/tag/v0.2.1)
provides ready-to-run packages for:

- Windows x86-64
- Synology DSM on x86-64
- Synology DSM on ARM64 (`aarch64`)

Each package includes the executable, license, cross-device test guide, and
local-index test guide. After extracting the correct package, run the isolated
end-to-end check:

```bash
deltaweave self-test
```

It performs two encrypted local transfers and verifies chunk integrity,
materialization, delta reuse, local indexing, rename correlation, tombstones,
and index restart recovery. See
[Windows PC ↔ Synology testing](docs/TESTING_WINDOWS_SYNOLOGY.md) for the full
cross-device procedure and checksum verification.

For a Portainer-managed Synology receiver, an AI operator can follow the
guardrailed [AI Portainer setup runbook](docs/AI_PORTAINER_SETUP.md). The
repository includes a hardened Stack definition and CI-tested multi-architecture
container image for `linux/amd64` and `linux/arm64`.

## Build and test

DeltaWeave currently pins Rust 1.91.

```bash
cargo build --workspace
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

CI repeats the quality gates on Linux and runs the full test suite on Windows.

## Index or watch a folder

Run one authoritative scan. Keep private state and the node identity outside the
indexed root when practical; paths beneath the root are excluded automatically.

```bash
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
complete persistent record and retry lists. They do **not** yet send those
changes to another device. See
[local-index testing](docs/TESTING_LOCAL_INDEX.md) for safe validation steps.

## Try a direct local transfer

Build the CLI and create a persistent identity on each node:

```bash
cargo build --release
deltaweave init --identity receiver.key
deltaweave init --identity sender.key
```

Start the receiver with the sender's printed endpoint ID. Authorization is
mandatory unless the explicitly unsafe testing flag is supplied.

```bash
deltaweave serve \
  --root ./received \
  --state ./receiver-state \
  --identity receiver.key \
  --allow-peer <SENDER_ENDPOINT_ID> \
  --direct-only
```

Copy the receiver's `endpoint_id` and one `direct_addresses` value from its JSON
output, then push a file:

```bash
deltaweave push ./large-file.bin \
  --remote-path archive/large-file.bin \
  --peer <RECEIVER_ENDPOINT_ID> \
  --direct <RECEIVER_IP:PORT> \
  --identity sender.key \
  --direct-only
```

Run the command again after editing the source. The receiver requests only
unique chunks not already present and returns a JSON receipt with transferred
bytes and reused extents.

Internet mode uses iroh discovery and encrypted relay fallback. Omit
`--direct-only`, and supply the relay URLs printed by the receiver when needed.

## Security model

- iroh authenticates endpoints cryptographically and encrypts transport traffic.
- DeltaWeave independently authorizes the remote endpoint ID.
- Every received chunk and reconstructed file is verified before commit.
- Wire paths reject traversal, absolute paths, Windows device names, and invalid
  deserialized values.
- Existing content is preserved in state trash before replacement.

This baseline does not yet defend perfectly against a privileged or racing local
attacker. Read [docs/THREAT_MODEL.md](docs/THREAT_MODEL.md) before exposing a
receiver or using real data. Report vulnerabilities according to
[SECURITY.md](SECURITY.md).

## License

DeltaWeave is licensed under the [MIT License](LICENSE).
