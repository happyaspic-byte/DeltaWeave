# Contributing

DeltaWeave welcomes focused issues and pull requests. Because synchronization
bugs can destroy data, changes are held to correctness-first standards.

## Development

Start in the repository root with Git, rustup, and your host's native C/C++
compiler/linker installed (Linux build tools or Windows MSVC Build Tools with a
Windows SDK). This is a Cargo workspace, with no Node/Python dependency for the
Rust build. `rust-toolchain.toml` selects Rust 1.91.0, rustfmt and Clippy;
`Cargo.toml` declares Rust 1.91 and edition 2024. Use the checked-in lockfile.

```bash
rustup show active-toolchain
cargo build --locked --workspace
cargo run --locked -p deltaweave -- --help
cargo run --locked -p deltaweave -- self-test
```

The debug binary is `target/debug/deltaweave` (`deltaweave.exe` on Windows), unless
`CARGO_TARGET_DIR` overrides the target directory. For an optimized binary run
`cargo build --locked --workspace --release --all-features` and use
`target/release/deltaweave`. Building does not install the binary on `PATH`; the
[README](README.md#build-and-test) describes source installation.

Run the quality gates used by `.github/workflows/ci.yml`:

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --all-features
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --all-features
```

The last line uses Bash environment assignment. In PowerShell, set
`$env:RUSTDOCFLAGS = '-D warnings'` before the same `cargo doc` command. These
commands need local filesystem writes; network tests and `self-test` also need
permission to bind local UDP sockets. They do not require real peer credentials.
Rust unit tests live in each crate's `src` module; shipped-CLI process/restart tests
are in `crates/deltaweave-cli/tests/fault_test.rs`. `--all-targets` does not run
rustdoc examples; use `cargo test --locked --workspace --doc --all-features` for
those separately.

On a Bash host, `./scripts/verify-release.sh` runs fmt, Clippy, all-target tests,
warning-denied rustdoc, CLI self-test, an additional serial fault-test gate,
media presence/shell syntax, patch hygiene, and Compose validation. Docker with
the Compose plugin is optional for this script: it prints `SKIP` if unavailable,
so its final `PASS` alone does not prove Compose was checked. It does not build
release archives, run a dependency audit, or validate Markdown links.

For a reproducible fault bundle, use a new empty directory:

```bash
fault_workspace="$(mktemp -d)"
./scripts/fault-test.sh "$fault_workspace"
```

This uses real local child processes and retains the explicit workspace. Without
`--workspace`, the CLI's temporary directory is removed even on failure; the
release verifier also removes its fault workspace on exit. The current barrier
does not prove the kill occurred after a new durable CAS write. See the
[CLI limitations](docs/CLI.md#fault-injection) before interpreting a passing run.

The separate loopback script sends 100 MiB and compares SHA-256. It needs Bash,
`dd` with GNU-style options, `python3`, `sha256sum`, and the standard Unix utilities
used by the script. It builds the debug CLI unless `DELTAWEAVE_BIN` selects an
existing executable. It is not included in `verify-release.sh`:

```bash
./scripts/test-p2p-loopback.sh
bash scripts/tests/test-p2p-loopback.sh
```

The second command tests the wrapper's success/failure paths with a mock CLI; it
does not perform real QUIC transfer. The first uses isolated temporary data and
cleans up its own receiver. With `DELTAWEAVE_WORK_DIR`, use only an empty disposable
directory: the wrapper removes that directory on exit unless
`DELTAWEAVE_KEEP_WORK_DIR=1`, including on failure.

## Script environment variables

All are optional, read when the named script starts; they are not CLI settings.
An unset or empty value uses the fallback below. No secret keys are supplied via
these variables. Runtime `RUST_LOG` and command defaults are in the
[CLI reference](docs/CLI.md#configuration-and-logging); deployment interpolation
variables are in the [Portainer runbook](docs/AI_PORTAINER_SETUP.md).

| Variable | Consumer | Default and purpose |
| --- | --- | --- |
| `DELTAWEAVE_VERIFY_RUST_LOG` | `verify-release.sh` | `warn,netwatch=error`; sets `RUST_LOG` for its self-test step only |
| `DELTAWEAVE_FAULT_SEED` | `fault-test.sh` | `424242`; passed to `--seed` |
| `DELTAWEAVE_FORCE_FAILURE` | `fault-test.sh` | `0`; exactly `1` adds `--force-failure` |
| `DELTAWEAVE_BIN` | `test-p2p-loopback.sh` | No supplied binary; build with Cargo. Set to an executable path to skip the build |
| `DELTAWEAVE_TEST_SIZE_BYTES` | `test-p2p-loopback.sh` | `104857600`; positive integer payload size |
| `DELTAWEAVE_WORK_DIR` | `test-p2p-loopback.sh` | No fixed path; `mktemp -d`. Explicit path must be absent or empty |
| `DELTAWEAVE_KEEP_WORK_DIR` | `test-p2p-loopback.sh` | `0`; exactly `1` retains evidence on success or failure |
| `CARGO_TARGET_DIR` | Cargo; loopback wrapper's binary lookup | Repository `target/` for the documented commands. Relative paths in the wrapper resolve from the repository root |
| `DELTAWEAVE_DOC_FONT` | `render-doc-visuals.sh` | `fc-match` result for `DejaVu Sans Mono`; override with a readable font file |

## Documentation sources

Markdown is edited directly; no site generator or Markdown checker is configured.
Rust API pages are generated from crate comments by `cargo doc` into `target/doc/`
(or the configured Cargo target directory). Edit comments, then regenerate; do
not edit generated HTML. Open `target/doc/deltaweave_core/index.html` for the core API.

`scripts/render-doc-visuals.sh` uses ImageMagick `convert`/`identify`, fontconfig
`fc-match`, and a monospace font. It overwrites the generated terminal frames/GIFs
in `docs/assets/` from embedded historical text; it does **not** run the CLI or
refresh execution evidence. Edit that source and run the script only when changing
those rendered examples. Other static artwork is not generated by that script.
The [usage gallery](docs/USAGE_GALLERY.md) explains the historical scope.

The [documentation audit](docs/DOCUMENTATION_AUDIT.md) records checked commands,
platform limits, and existing discrepancies that require product changes.

## Testing changes

New behavior needs tests at the lowest useful layer. Storage and protocol changes
also need a failure-path or adversarial test. Avoid time-based sleeps in tests;
prefer explicit readiness and bounded timeouts.

## Pull requests

- Keep a pull request to one coherent change.
- Explain the invariant being added or preserved, not only the implementation.
- Update protocol/architecture/threat-model documents when their claims change.
- Call out on-disk or wire compatibility explicitly.
- Never commit node keys, real user data, or generated state directories.

Unsafe Rust is forbidden in the current crates. A future OS integration requiring
FFI must live in a narrowly scoped platform crate with documented safety
invariants and dedicated review.
