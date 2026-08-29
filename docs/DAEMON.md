# Daemon and service operations

`deltaweave daemon` runs continuous bidirectional synchronization under a
supervised control plane. The daemon claims a single-instance lock, exposes an
authenticated local IPC endpoint, and reports its lifecycle as JSON.

This document covers operational use: starting the daemon, controlling it with
`ctl`, and installing it as an operating-system service. The synchronization
engine and its invariants are unchanged from `sync`; only supervision,
authentication, and service integration are new.

## Lifecycle states

| State | Meaning |
| --- | --- |
| `starting` | Resources are being claimed; IPC may already be listening |
| `running` | Synchronization loop is eligible to run |
| `paused` | Synchronization is held; status queries still succeed |
| `retrying` | The last pass failed; the loop retries with exponential backoff |
| `stopping` | Shutdown has been requested; the process is draining |
| `stopped` | The process has fully stopped |

The snapshot reported by `ctl status` contains the current state, the remote
endpoint identity, success/failure counters, the last error, the last
successful report, and the next retry time. Snapshots never contain the owner
token or any secret.

## Control plane files

The `--control-state` directory holds three files created with owner-only
permissions:

| File | Purpose |
| --- | --- |
| `daemon.lock` | Single-instance lock recording the owning PID |
| `owner.token` | 32-byte bearer token authenticating IPC requests |
| `control.sock` | IPC endpoint: Unix domain socket, or the loopback address on Windows |

On Unix the control directory and socket are created with mode `0700`/`0600`,
so only the owning user can connect or read the token. On Windows the daemon
avoids named pipes and FFI: it binds loopback TCP on `127.0.0.1:0` and writes
the chosen address to `control.sock`; the token remains the only accepted
credential, and non-loopback connections are refused.

Every IPC request is a length-prefixed JSON frame carrying the hex-encoded
token and one command. Tokens are compared in constant time. Frames larger
than 64 KiB, malformed JSON, and unauthenticated requests are rejected without
revealing internal state.

## Run the daemon

`daemon` accepts the same synchronization arguments as `sync` plus
`--control-state`. Synchronization mode is required; the daemon always
reconciles with a remote peer.

```bash
deltaweave daemon \
  --root ./sync-root \
  --state ./private/sync-state \
  --identity ./private/node.key \
  --peer <PEER_ENDPOINT_ID> \
  --direct <PEER_IP:PORT> \
  --control-state ./private/control
```

The daemon refuses to start when another instance already holds
`daemon.lock`. A lock left behind by a crashed process is detected through the
recorded PID and removed automatically; a live PID is never stolen.

On Windows, a lock file left by a crashed daemon cannot always be verified
without unsafe process handles, so operators may need to delete
`daemon.lock` manually after a crash. The lock is never removed while the
recorded process is assumed live.

## Control a running daemon

`ctl` sends one authenticated command over IPC and prints the daemon snapshot
as JSON.

```bash
deltaweave ctl --control-state ./private/control status
deltaweave ctl --control-state ./private/control pause
deltaweave ctl --control-state ./private/control resume
deltaweave ctl --control-state ./private/control stop
```

`pause` holds synchronization between passes and does not cancel a pass already
in flight. `resume` wakes the loop immediately rather than waiting for the
remaining interval. `stop` requests a graceful shutdown: the current pass
finishes, the state becomes `stopped`, and the process exits cleanly. Signals
(SIGINT/SIGTERM) trigger the same graceful path.

`ctl` must run as the same operating-system user that owns the control
directory; the token file is the credential and is not readable by other users
on Unix. On Windows the same user must read `control.sock` and `owner.token`.

## systemd service

Render a hardened unit from absolute paths. The command refuses relative paths
and empty users; all rendered values are quoted so spaces in paths and user
names cannot inject unit directives.

```bash
deltaweave service systemd-unit \
  --executable /usr/local/bin/deltaweave \
  --user syncuser \
  --root /srv/sync-root \
  --state /var/lib/deltaweave/sync-state \
  --identity /var/lib/deltaweave/identity.key \
  --peer <PEER_ENDPOINT_ID> \
  --control-state /var/lib/deltaweave/control \
  > /etc/systemd/system/deltaweave-sync.service
```

The rendered unit enables `NoNewPrivileges`, `ProtectSystem=strict`,
`ProtectHome=read-only`, and `PrivateTmp`, and grants write access only to the
synchronization root, private state, and control directories. Install it as
root, then enable:

```bash
systemctl daemon-reload
systemctl enable --now deltaweave-sync.service
systemctl status deltaweave-sync.service
```

Caveats:

- `ProtectHome=read-only` blocks synchronization roots beneath `/home`; render
  with a root outside the home directory or drop the directive for that case.
- `ProtectSystem=strict` requires every writable path to appear in
  `ReadWritePaths`; add directories there if you relocate state or control
  files after rendering.
- `Restart=on-failure` restarts after crashes. A crash that leaves a stale
  lock is recovered automatically on Unix; on Windows delete `daemon.lock`
  before restart.
- systemd stops the daemon with SIGTERM, which triggers the same graceful
  shutdown as `ctl stop`. Increase `TimeoutStopSec` if a large pass needs more
  than the default 90 seconds to finish.

## Windows service

DeltaWeave does not ship an unsafe SCM installer or any FFI. The documented
procedure registers the built-in `service run` entry point through `sc.exe`
and lets the Service Control Manager supervise the process.

Build the release binary, then register the service from an elevated prompt:

```
sc.exe create DeltaWeaveSync binPath= "C:\Program Files\DeltaWeave\deltaweave.exe service run --root C:\sync-root --state C:\ProgramData\DeltaWeave\sync-state --identity C:\ProgramData\DeltaWeave\identity.key --peer <PEER_ENDPOINT_ID> --direct <PEER_IP:PORT> --control-state C:\ProgramData\DeltaWeave\control" start= auto
sc.exe description DeltaWeaveSync "DeltaWeave continuous synchronization daemon"
sc.exe start DeltaWeaveSync
```

Caveats:

- `binPath=` and `start=` require the trailing space shown above; `sc.exe`
  rejects them without it.
- Run the service under a dedicated local account whose profile owns the
  control directory, or grant that account access to it. The owner token file
  is the IPC credential and must stay unreadable to other users.
- The Service Control Manager's default shutdown is not a graceful IPC stop.
  For planned maintenance prefer `deltaweave ctl --control-state ... stop`,
  then `sc.exe stop DeltaWeaveSync`, then `sc.exe start DeltaWeaveSync`.
- If a crashed daemon leaves `daemon.lock`, delete it before the next start;
  Windows cannot verify a foreign PID without unsafe process handles.
- Verify recovery after a reboot before relying on `start= auto`.

## Operational checks

- `ctl status` reports `state`, counters, `last_error`, and `next_retry` after
  a failure; use it to distinguish a paused daemon from one that is backing
  off.
- The socket and token files are recreated if missing, but never while another
  instance is live; duplicate instances fail fast at startup.
- A daemon whose remote peer is unreachable alternates between `retrying` and
  `running` as backoff expires; the backoff is capped by
  `--max-backoff-seconds` (default 300).
