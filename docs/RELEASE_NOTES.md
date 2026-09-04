# DeltaWeave v0.4.0

This pre-alpha release adds a reproducible fault-injection harness for validating Windows/Synology synchronization recovery. Keep independent backups; DeltaWeave remains unsuitable as the only copy of important data.

## Fault-injection and recovery

- Adds `deltaweave fault-test --seed <SEED>` and `scripts/fault-test.sh`.
- Starts independent Windows and Synology-style roots, private states, identities, and real CLI child processes.
- Repeats deterministic create, modify, delete, and rename operations.
- Kills the receiver after a chunk is durable but before destination materialization.
- Kills a `sync-once` process at the corresponding pull-side transfer barrier.
- Reopens the original state, verifies every final path and file byte, compares both Merkle roots, then requires an unchanged retry to perform zero actions.
- Writes the seed, exact operation order, fault points, process IDs, peer logs, root paths, state paths, and final result to `report.json`.
- Preserves reproduction data after intentional or unexpected failure.

## Release validation

The release gate now runs the packaged self-test plus the deterministic fault-injection scenario. Integration tests drive the shipped CLI executable and cover restart recovery, repeated-seed determinism, process termination, and failure evidence.

See `TESTING.md` in each archive for Windows and Synology setup, recovery commands, evidence paths, and known limits.

## Compatibility

The wire protocol and persistent storage schemas remain unchanged from v0.3.0. Existing roots, private state, and identities are reused without migration. Existing unsafe root/state overlap and incomplete-scan refusal remain enabled.

## Known limits

- Physical Windows/Synology long-running soak remains a field validation gate.
- Power loss, disk exhaustion, long network partitions, and DSM package lifecycle are not simulated by this harness.
- Safe tombstone garbage collection, resource limits, Windows service installation, and DSM SPK packaging remain future work.
