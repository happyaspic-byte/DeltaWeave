# CLI reference

This reference describes workspace version 0.4.0. The source of command names,
arguments and defaults is `crates/deltaweave-cli/src/main.rs`; inspect the installed
binary with `deltaweave --version`, `deltaweave --help`, and
`deltaweave <command> --help`. Build/install instructions are in the
[README](../README.md#build-and-test).

## Commands and required inputs

Paths are resolved from the process working directory. Replace `<...>` placeholders
with real values and omit the brackets. Options below belong after the subcommand;
there is no configuration-file option. The global `--output json|text` option
works before or after the subcommand and defaults to `json`. The separate
`deltaweave-web` binary provides the local HTTP UI described in [WEB_UI.md](WEB_UI.md).

| Command | Required inputs | Successful stdout |
| --- | --- | --- |
| `init` | None | `created`, public `endpoint_id`, `identity_file`; opens an existing identity or creates one |
| `manifest <FILE>` | Existing file | Manifest with `schema_version`, `profile`, `size`, `file_hash`, `chunks` |
| `serve` | `--root`; one or more `--allow-peer <ID>` or explicit `--allow-any-authenticated` | `status: ready`, `endpoint_id`, `direct_addresses`, `relay_urls`; then waits for shutdown |
| `push <SOURCE>` | File, `--remote-path`, `--peer` | Receipt with `path`, `file_hash`, `transferred_bytes`, `reused_extents`, `manifest_hash` |
| `scan` | `--root` (existing real directory) | Scan report; `--include-records` instead wraps it as `report`, `records`, `retries` |
| `watch` | `--root` (existing real directory) | Stream of `initial_scan`, scan events and `shutdown` objects |
| `sync-once` | `--root`, `--peer` | `status: pass`, action/byte counts, conflicts and verified Merkle roots |
| `sync` | `--root`, `--peer` | Stream of `sync_started`, `sync`, `sync_error`, optional `local_change`, and `shutdown` objects |
| `self-test` | None | Isolated local QUIC, delta, index and bidirectional/restart verification report with `status: pass` |
| `fault-test` | None; explicit fresh `--workspace` recommended | Process-termination/convergence report; see limitations below |

`serve` exposes authorized folder reads and writes through the v2 protocol as
well as receiving v1 `push`. The allow-list
applies to both protocols, and `--allow-peer` may repeat. The unsafe
`--allow-any-authenticated` flag conflicts with `--allow-peer`.

## Paths, identity and network options

| Option | Commands | Default / condition |
| --- | --- | --- |
| `--identity <FILE>` | All except `manifest`, `self-test`, `fault-test` | `.deltaweave/identity.key`; load or create a private node key |
| `--state <PATH>` | `serve` | `.deltaweave/state`; private CAS/index/journal/trash directory |
| `--state <PATH>` | `push` | No cache by default; optional sender manifest-cache directory |
| `--state <PATH>` | `scan`, `watch` | `.deltaweave/index.redb`; database **file** |
| `--state <PATH>` | `sync-once`, `sync` | `.deltaweave/sync-state`; private CAS/index/journal/trash directory |
| `--bind <IP:PORT>` | `serve` | No fixed address/port; explicit nonzero port remains stable across restarts |
| `--direct <IP:PORT>` | `push`, `sync-once`, `sync` | No explicit addresses; repeatable |
| `--relay <URL>` | `push`, `sync-once`, `sync` | No explicit relay URLs; repeatable |
| `--direct-only` | `serve`, `push`, `sync-once`, `sync` | Off: Internet mode uses iroh discovery/relay services. On: no discovery or relay; clients require at least one `--direct` |
| `--ignore <PATH>` | `scan`, `watch` | None; repeatable, relative to the working directory, not the indexed root |
| `--hash-workers <N>` | `scan`, `watch` | Available CPU parallelism capped at 8 (fallback 1); explicit value must be positive |
| `--include-records` | `scan` | Off |

Keep `serve`/`sync` state outside the synchronized root, and neither directory
inside the other; identity must also be outside the root. Keep root and state
on the **same filesystem**: replacement/deletion renames old content into state
trash and has no cross-device copy fallback. Preserve state and identity across
restarts; the index is bound to one canonical root and replica ID.

`scan`/`watch` automatically ignore the identity file. If their index DB lies
beneath the root, the DB's parent directory is excluded; a DB directly in the
root excludes only that file. They update persistent state, including retries and
tombstones, even though they do not transfer or materialize files.

The optional `push --state` cache is used only with stable identity and reliable
change-time metadata (Unix ctime). Missing metadata, Windows, and cache errors
fall back to generating a fresh manifest. Requested chunk bytes are still verified.
A v1 push can overwrite an authorized destination; use `sync-once` for causal
conflict reconciliation. See [the protocol](PROTOCOL.md) for the wire contract.

## Chunking and scheduling

`manifest`, `push`, `sync-once` and `sync` accept the three chunk sizes in bytes:

| Option | Default | Valid range |
| --- | ---: | ---: |
| `--min-chunk` | 65536 | 64–1048576 |
| `--avg-chunk` | 262144 | 256–4194304 |
| `--max-chunk` | 1048576 | 1024–16777216 |

All must be even with `min < avg < max`. For a file of at least 8 GiB, exactly the
default triple selects 524288 / 1048576 / 4194304 automatically. Explicitly typing
the default values still permits this selection; any non-default value disables it.
For synchronization, these options configure local staging and outgoing files;
they do not change the peer-generated pull manifest, which uses its default profile.

| Option | Commands | Default |
| --- | --- | ---: |
| `--debounce-ms` | `watch`, `sync` | 750 |
| `--max-debounce-ms` | `watch`, `sync` | 5000 |
| `--rescan-seconds` | `watch` | 600 |
| `--poll-fallback-seconds` | `watch` | 5 |
| `--interval-seconds` | `sync` | 5 |
| `--max-backoff-seconds` | `sync` | 300 |

Durations must be positive and maximum debounce must be at least debounce.
`watch` uses native events for incremental scans, periodic full scans, and
full-scan polling after watcher startup failure/loss. `sync` scans during each
pass, waits up to the interval after success, and can wake earlier on debounced
local events. Remote changes are discovered on a later pass; scan/transfer time
and retries mean the interval is **not an end-to-end latency guarantee**. After
failure, retry waits start at one second and double up to the configured cap;
local events do not shorten that failure backoff.

## Output and errors

By default, stdout contains pretty-printed JSON; help/version output is plain text. A long-running
command emits multiple multiline JSON objects, **not JSON Lines or one JSON array**.
Parse a sequence of JSON values if consuming `watch`/`sync`. Diagnostics and tracing
are written to stderr without ANSI escapes. Runtime errors normally appear as
`Error: ...` on stderr in JSON mode; they are not a stable JSON error envelope.
`--output text` selects readable reports and actionable text errors; it leaves
persisted JSON report files unchanged. Use JSON mode for scripts.

| Exit status | Meaning |
| --- | --- |
| `0` | Successful command, help/version, or handled Ctrl+C shutdown |
| `2` | Clap parsing failure, such as a missing required argument or unknown option |
| `1` | Runtime/validation failure returned by the CLI, including forced fault-test failure |

These are normal CLI paths, not a guarantee for OS signals or process crashes.
On Unix, `serve`, `watch` and `sync` also handle SIGTERM. A running `sync` reports
individual failures as `event: sync_error`, `status: retrying` and stays alive.
`scan` may exit 0 while reporting `issues`, `collisions` or queued retries: inspect
those fields rather than relying on the process status. `sync-once` refuses an
incomplete/colliding snapshot and reports success only after fresh local/remote
roots both equal `desired_root`.

Safe argument-error examples (no running peer required):

```bash
deltaweave scan
# exit 2: missing --root
deltaweave serve --root ./received
# exit 1: serve requires at least one --allow-peer, or explicit --allow-any-authenticated
deltaweave push ./sample.bin --remote-path sample.bin --peer invalid --direct-only
# exit 1: --direct-only requires at least one --direct address
```

## Fault injection

`fault-test` accepts `--seed` (u64, default `424242`), `--workspace` (no fixed
path), `--payload-mib` (default `16`) and `--force-failure` (off). Use only a new,
empty disposable workspace: it creates identities, roots, state and logs, writes
test files and kills its own `serve`/`sync-once` child processes. The seed fixes
identities and file bytes; the operation order is fixed in the implementation.
Payload generation has no practical CLI size cap; keep the documented default.

With an explicit workspace, `report.json` and any collected evidence remain on
normal success/failure. Early failures may have only partial evidence. Without
one, the temporary directory is dropped on failure as well as success, despite
the help/error text suggesting failed bundles are preserved. `--force-failure`
finishes the scenario, reports `status: forced_failure`, then exits 1.

The current barrier compares the count of nonempty files under the entire remote
state directory with a CAS-only baseline. Metadata files can satisfy it before a
new chunk arrives. A passing report proves the process-kill/restart scenario and
peer file/hash convergence checks ran; it does not prove interruption at a durable
payload boundary, power-loss recovery, or physical Windows/DSM behavior. The
`windows`/`synology` directory names are peer labels on the current host. See the
[documentation audit](DOCUMENTATION_AUDIT.md) for product follow-up items.

## Configuration and logging

The CLI reads `RUST_LOG` once at startup through tracing's `EnvFilter`. It is
optional; absent/invalid filters fall back to `warn,netwatch=error`. For example,
in Bash, `RUST_LOG=warn,deltaweave_net=info deltaweave self-test` enables network
info logs. In PowerShell, set `$env:RUST_LOG = 'warn,deltaweave_net=info'` before
the command. Command options, not environment variables, select root, state,
identity, peers and scheduling. There is no application `.env` loader.

`DELTAWEAVE_*`, `PUID` and `PGID` in Compose are interpolation inputs, not settings
read by the Rust CLI. Their defaults and required conditions are in the
[Portainer runbook](AI_PORTAINER_SETUP.md). Developer script variables are listed
in [CONTRIBUTING.md](../CONTRIBUTING.md#script-environment-variables). Keys are
created with `init`; never copy their secret contents into commands or examples.
