# DeltaWeave E-store implementation contract

- Base: `63cb43c4142e0d28931dd5f81decb5506aa5d948` (`integration`)
- Branch: `feat/qsync-e-store-20260909`
- Scope: `crates/deltaweave-store/src/lib.rs`, `crates/deltaweave-store/src/preservation.rs`, and store preservation tests.
- The network, sync, control, CI, and harness layers own authorization, admission, and the
  mutation gate. Store receives an already-reserved private path and does not decide whether a
  remote grant is current.

## Narrow public APIs

```rust
pub fn materialize_private_verified(
    &self,
    manifest: &FileManifest,
    stage_root: &Path,
    stage_path: &WirePath,
) -> Result<PathBuf>;

pub fn rollback_unadopted_path_change(
    &self,
    change: &mut PathChange,
) -> Result<()>;
```

`materialize_private_verified` is for a verified E-consumer staging file. `rollback_unadopted_path_change`
is for a noncausal, not-yet-adopted path attempt whose owner permission has already been checked by
the caller. Neither API changes the existing legacy `materialize`, `resume_path_change`, or causal
recovery behavior.

## Private verified materialization

The caller reserves a unique managed private `stage_root` before calling Store. The root must be an
existing canonical-equivalent real directory. On Windows, ordinary and verbatim (`\\?\`) spellings
of the same no-reparse directory are compared by directory identity. The `WirePath` is a validated
relative path; Store rejects
symlink/reparse ancestors, non-directories, an occupied final leaf, and a path outside that root.
Missing intermediate directories are created one component at a time with private permissions and
revalidated before use.

The method validates the manifest, reads every CAS extent through a no-follow handle, checks the
descriptor length and digest, and recomputes the complete ordered `file_hash`. It repeats the CAS
verification while writing a private temporary file, calls `sync_all`, and installs the leaf with a
same-directory no-replace rename followed by a parent-directory sync. A CAS symlink, missing or
corrupt extent, changed complete hash, length mismatch, reparse component, or concurrent occupant
fails closed and leaves no partially installed leaf. The method does not call
`metadata.put_manifest`, create a `PathChange`, update an index, or touch a public synchronization
root.

On Unix the Store check enforces private mode on the supplied root and managed intermediate
directories. Windows ACL/DACL protection belongs to the host admission caller (`reserve_private`
and its secure-parent preparation); this crate does not add a duplicate ACL framework. The caller
must fail closed if that reservation/ACL check is unavailable.

The common directory walk used by the new CAS and rollback paths rejects both symbolic links and
Windows reparse points/junctions on every existing ancestor. A normal Windows spelling and its
verbatim `\\?\` spelling are accepted only when they resolve to the same directory identity.

## Durable unadopted rollback

The method accepts `Prepared`, `Preserved`, `Materialized`, and a previously persisted
`RollingBack` state. It rejects causal bindings, metadata-only changes, and `Indexed`/`Committed` or
other terminal states. Before moving any filesystem object on a first call, it verifies the state
precondition and writes the journal as `RollingBack`. This write-ahead transition is the durable
crash boundary.

Continuation is deterministic and idempotent:

1. A remaining `staging` object is checked against the bound target and moved with no replacement
   to `rollback_artifact`. A second rollback artifact is an inconsistent journal and fails closed.
2. An installed target is captured with no replacement only when it still matches the bound target.
   The captured observation is rechecked after the move. An existing rollback artifact is checked
   against the target before it is used.
3. The exact expected local object in `artifact` is restored only into an absent destination with
   no-replace semantics. The post-restore observation must equal the original expected observation.
   An expected absence requires both the destination and old artifact to be absent.
4. Only after filesystem syncs and observations succeed is the journal changed to `RolledBack`.
   If that final journal write fails, the caller-held value is returned to `RollingBack` so a later
   retry can converge from the durable state. If the initial write-ahead journal write fails,
   Store restores the caller-held state (`Prepared`, `Preserved`, or `Materialized`) and performs
   no filesystem move, so the same object can retry the write-ahead step.

If a local edit or an occupant makes a capture or restore unsafe, the method returns an error while
leaving `RollingBack` and retaining the old artifact and incoming rollback artifact. A later
authenticated retry may remove the unrelated occupant and resume. No index promotion, manifest
table update, or public metadata update occurs. Store cannot recall bytes already sent to a peer or
control a malicious peer that independently redistributes data; the drain/revoke layer must use
its separate writer and bilateral-ack contract.

## Regression coverage

The focused store suite covers successful CAS-only staging, missing/tampered/length/full-hash
failure, CAS and destination symlinks, broad private permissions, metadata/journal immutability,
ordinary three-state rollback, causal/indexed rejection, and drift preservation. Unit regressions
persist `RollingBack` after each of these checkpoints and reopen the Store before retrying:

- `Prepared`: incoming staging already moved to the rollback artifact.
- `Preserved`: incoming moved and the expected object restored before the final journal write.
- `Materialized`: installed incoming captured and the expected object restored before the final
  journal write.

The occupied-restore regression keeps both artifacts pending, then retries after the occupant is
removed. Tests assert restored bytes, retained incoming bytes, no staging residue, and no artifact
loss. An injected initial journal failure asserts that the caller state and durable row remain at
the original stage before retry. A Windows-only native regression creates junction ancestors in
the CAS and public namespaces and asserts both are rejected without changing the outside target;
that native test is not represented as passing by the Linux run below.

Validation is recorded only when actually run. The implementation validation commands are:

```text
cargo fmt --all -- --check
cargo test --locked -p deltaweave-store --all-targets --all-features --no-fail-fast
cargo clippy --locked -p deltaweave-store --all-targets --all-features -- -D warnings
git diff --check
```

The commands run against the existing shared Cargo target cache; no new production target or
remote state is created. Windows native ACL, network authorization, and the full E-consumer flow
remain caller/integration validation and are not represented as passing here.

Observed validation for this worktree:

- `cargo fmt --all -- --check`: exit 0, captured 2026-09-09T07:16:14Z–2026-09-09T07:16:16Z.
- `cargo test --locked -p deltaweave-store --all-targets --all-features --no-fail-fast`: exit 0,
  captured 2026-09-09T07:16:16Z–2026-09-09T07:16:20Z; 35 unit tests plus 16 preservation
  integration tests passed, with no ignored failures.
- `cargo clippy --locked -p deltaweave-store --all-targets --all-features -- -D warnings`: exit 0,
  captured 2026-09-09T07:16:20Z–2026-09-09T07:16:23Z.
- `CARGO_TARGET_DIR=/home/ubuntu/project/DeltaWeave-qsync-web-20260908/target cargo check
  --locked -p deltaweave-store --tests --all-features --target x86_64-pc-windows-gnu`: exit 0,
  captured 2026-09-09T07:14:03Z–2026-09-09T07:14:18Z using the existing shared target cache.
  This is a Windows-target compile check; the junction regression itself remains unrun on a
  Windows host.
- `git diff --check`: exit 0, captured 2026-09-09T07:16:23Z.
