# DeltaWeave v0.4.0

This pre-alpha release adds a seeded child-process interruption harness and reduces repeated hashing and receive-side storage overhead. The workspace version is 0.4.0. Keep independent backups; DeltaWeave remains unsuitable as the only copy of important data.

## Fault-injection and recovery

- Adds `deltaweave fault-test --seed <SEED> --workspace <EMPTY_DIRECTORY>` and `scripts/fault-test.sh <EMPTY_DIRECTORY>`. The default seed is `424242`; each fault payload defaults to 16 MiB.
- Starts two independent roots named `windows` and `synology`, private states, identities, and real CLI child processes on the current host, using direct loopback transport. These names represent peer roles, not two operating systems.
- Executes a fixed create, modify, delete, and rename sequence. The seed determines identities and file bytes.
- Kills `serve` during one push and `sync-once` during a second push. Both reported barriers are `remote_chunk_persisted_destination_absent`; the implementation polls nonempty files beneath the receiver state while the destination is absent. This observation does not prove a particular chunk has reached durable storage.
- Reuses the original state, compares the peers' final file-path/content-hash maps and Merkle roots, and requires an unchanged retry to perform zero actions. This checks peer agreement, without an independent expected-content oracle for all operations.
- Writes the seed, four ordered mutation records, observed fault points, killed process IDs, peer-log paths, root/state paths, and final result to `report.json`. Setup and fault-payload creation are not entries in the four-operation list.
- Retains evidence in an explicitly supplied workspace after success or failure. Without `--workspace`, the temporary directory is currently removed on exit, including failure; use a new explicit directory for reproducible evidence. A failure before the report can be created cannot produce a complete bundle.
- Adds `serve --bind <IP:PORT>` to select a stable receiver UDP socket address across restarts.

## Transfer and indexing changes

- Adds optional `push --state <DIRECTORY>` sender-manifest caching in `sender-manifests.redb`. Reuse requires matching stable identity, metadata, chunking profile, and a reliable change-time token. Windows and other platforms without that token generate a fresh manifest, as do cache misses or cache-access failures.
- Selects a 512 KiB / 1 MiB / 4 MiB FastCDC profile for files at least 8 GiB when the default chunk profile is requested. Explicit non-default profiles remain unchanged.
- Uses a local materialization observation to adopt a received file into the index without a second whole-file hash when reliable change-time checks succeed. Windows and other platforms without this token retain stable-file hashing and expected-record comparison.
- Passes validated receive payloads through `VerifiedChunk` and a bounded overlapping write pipeline. Queued receive bytes are limited, and in-flight writes are drained before a partial-failure error is returned. Chunk integrity and final file verification remain required.

## Release validation

The configured Linux and Windows CI jobs run workspace tests, including three CLI fault-harness integration tests, plus `self-test`. These tests cover process termination/restart, repeated-seed output, and failure reports with explicit workspaces. Linux additionally runs formatting, Clippy, rustdoc, media checks, and a release build.

After successful push CI on `main`, the release workflow checks for an existing version release, audits dependencies, builds Windows x86-64 and static Linux x86-64/ARM64 packages, and runs each release binary's `self-test`. ARM64 execution uses QEMU in a Linux container. Packaged release binaries are not separately run through `fault-test` by that workflow. Container tests and publication run in a separate workflow.

`scripts/verify-release.sh` also runs the fault integration tests and a standalone seeded scenario locally. Its exit trap removes that scenario's temporary evidence directory even on failure; use the direct command with an explicit workspace when retaining evidence matters. These are configured checks, not a claim that a particular remote workflow or physical Windows/Synology test passed during this documentation update.

See `TESTING.md` in each archive for Windows and Synology setup, recovery commands, evidence paths, and known limits.

## Compatibility

The existing transfer/sync ALPNs and record schema version identifiers remain in place. The optional sender cache introduces separate metadata; it does not replace synchronization state. Keep existing roots, private state, and identities together. No cross-version upgrade run is claimed here. Unsafe root/state overlap and incomplete-scan refusal remain enabled.

## Known limits

- Physical Windows/Synology long-running soak remains a field validation gate.
- Power loss, disk exhaustion, packet-loss injection, long network partitions, pull-side interruption, and DSM package lifecycle are not simulated by this harness. Its transport interruption comes from terminating a process.
- Safe tombstone garbage collection, configurable bandwidth/per-peer quotas, Windows service installation, and DSM SPK packaging remain future work.
