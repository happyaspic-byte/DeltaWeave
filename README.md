# DeltaWeave

[![CI](https://github.com/happyaspic-byte/DeltaWeave/actions/workflows/ci.yml/badge.svg)](https://github.com/happyaspic-byte/DeltaWeave/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

DeltaWeave is a Rust foundation for authenticated, content-defined P2P file
synchronization. It combines FastCDC chunking, BLAKE3 integrity, a durable
content-addressed store, and iroh's encrypted QUIC transport.

> **Project status: pre-alpha.** v0.1 is a verified one-file delta-transfer
> vertical slice, not yet a background two-way sync product. Do not use it as
> the only copy of important data.

## What works today

| Capability | v0.1 status |
| --- | --- |
| Streaming FastCDC manifests | Implemented and unit-tested |
| Chunk and whole-file BLAKE3 verification | Implemented and unit-tested |
| Durable chunk CAS + redb metadata | Implemented with restart tests |
| Authenticated iroh/QUIC transfer | Implemented with local P2P integration tests |
| Missing-chunk-only re-transfer | Implemented with insertion/reuse tests |
| Allow-listed peer authorization | Implemented; deny by default |
| Safe replacement and recovery journal | Implemented baseline; old content goes to private trash |
| Continuous filesystem watching | Planned |
| Merkle tree reconciliation and tombstones | Planned |
| Full two-way conflict handling | Planned |
| Windows CFAPI / Linux FUSE on-demand files | Planned |

The scope and acceptance gates for later phases live in [ROADMAP.md](ROADMAP.md).

## Repository layout

| Crate | Responsibility |
| --- | --- |
| `deltaweave-core` | Stable hashes, manifests, portable paths, and version vectors |
| `deltaweave-cdc` | Streaming FastCDC, BLAKE3 manifests, and delta planning |
| `deltaweave-store` | Verified chunk storage, redb metadata, journaled materialization |
| `deltaweave-net` | iroh endpoint identity, authorization, and transfer protocol |
| `deltaweave` | JSON-oriented CLI for identity, manifest, receive, and push |

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) and
[docs/PROTOCOL.md](docs/PROTOCOL.md) for the invariants behind these boundaries.

## Build and test

DeltaWeave currently pins Rust 1.91.

```bash
cargo build --workspace
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

CI repeats the quality gates on Linux and runs the full test suite on Windows.

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
