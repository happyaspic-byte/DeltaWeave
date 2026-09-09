#!/usr/bin/env python3
"""Run the Windows RW provider while a hosted RO job is in flight.

The controller keeps the owner and ticket outside this process.  It supplies
one RW share key and the WinRM/artifact transport values through the workflow's
environment only.  The script writes fixed phase evidence and never records
those values.  A keepalive window is a concurrency aid; it is not a revoke or
share-swarm acknowledgement.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import qsync_three_host_bootstrap as bootstrap  # noqa: E402


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="qSync Windows RW keepalive runner")
    root.add_argument("--source-sha", required=True)
    root.add_argument("--native-manifest", required=True, type=Path)
    root.add_argument("--windows-binary", required=True, type=Path)
    root.add_argument("--expected-file-hash", required=True)
    root.add_argument("--evidence-dir", required=True, type=Path)
    root.add_argument("--keepalive-seconds", type=int, default=300)
    return root


def native_artifact(source_sha: str, manifest_path: Path, binary: Path) -> dict[str, Any]:
    source_sha = bootstrap.require_source_sha(source_sha)
    manifest = bootstrap.load_json_file(bootstrap.validate_abs_path(str(manifest_path), must_exist=True))
    if (
        manifest.get("status") != "pass"
        or manifest.get("source_sha") != source_sha
        or manifest.get("workflow_sha") != source_sha
        or manifest.get("target") != "x86_64-pc-windows-msvc"
    ):
        bootstrap.fail("source_mismatch")
    artifact = bootstrap.json_object(manifest.get("artifact"), "manifest_invalid")
    if (
        artifact.get("source_sha", source_sha) != source_sha
        or artifact.get("workflow_sha", source_sha) != source_sha
    ):
        bootstrap.fail("source_mismatch")
    expected_hash = bootstrap.require_sha256(artifact.get("sha256"), "manifest_invalid")
    expected_size = artifact.get("size_bytes")
    if not isinstance(expected_size, int) or expected_size <= 0:
        bootstrap.fail("manifest_invalid")
    binary = bootstrap.validate_abs_path(str(binary), must_exist=True)
    if bootstrap.file_sha256(binary) != expected_hash or binary.stat().st_size != expected_size:
        bootstrap.fail("binary_hash_mismatch")
    return {"sha256": expected_hash, "size_bytes": expected_size, "target": "windows"}


def keepalive_observed(remote: bootstrap.RemoteRun | None, requested: int) -> bool:
    return bool(
        remote
        and remote.diagnostic_stages.count("keepalive_enter") == 1
        and remote.diagnostic_stages.count("keepalive_done") == 1
        and remote.diagnostic_counts.get("keepalive_enter") == requested
        and remote.diagnostic_counts.get("keepalive_done") == requested
    )


def write_result(
    evidence: bootstrap.SafeEvidence,
    *,
    source_sha: str,
    artifact: dict[str, Any] | None,
    expected_file_hash: str,
    requested: int,
    remote: bootstrap.RemoteRun | None,
    status: str,
    error_class: str,
    run_owned_copy_removed: bool,
) -> None:
    phases = remote.phases if remote is not None else []
    manifest = {
        "record_type": "qsync_f_windows_rw_keepalive",
        "scope": "windows_rw_concurrent_keepalive",
        "status": status,
        "source_sha": source_sha,
        "windows_binary_sha256": artifact.get("sha256") if artifact else None,
        "windows_binary_size_bytes": artifact.get("size_bytes") if artifact else None,
        "keepalive_requested_seconds": requested,
        "keepalive_observed": keepalive_observed(remote, requested),
        "remote_status_code": remote.status_code if remote is not None else None,
        "remote_phase_count": len(phases),
        "remote_contract_valid": bool(
            remote
            and artifact
            and bootstrap.remote_contract_is_complete(
                remote,
                str(artifact["sha256"]),
                int(artifact["size_bytes"]),
                expected_file_hash,
                bootstrap.FIXTURE_A_SIZE_BYTES,
                require_keepalive=True,
            )
        ),
        "remote_command_timed_out": bool(remote and remote.command_timed_out),
        "remote_transport_cleanup": bool(remote and remote.transport_cleanup_completed),
        "remote_transport_error": remote.transport_error_class if remote is not None else error_class,
        "remote_output_bytes": remote.output_bytes if remote is not None else 0,
        "remote_receive_poll_count": remote.receive_poll_count if remote is not None else 0,
        "remote_diagnostic_stages": remote.diagnostic_stages if remote is not None else [],
        "remote_diagnostic_counts": remote.diagnostic_counts if remote is not None else {},
        "remote_diagnostic_elapsed_ms": remote.diagnostic_elapsed_ms if remote is not None else {},
        "remote_phases": phases,
        "error_class": error_class,
        "managed_bilateral_drain_ack": "unverified",
        "state_retained_after_unknown": status != "pass",
        "raw_output_retained": False,
        "run_owned_copy_removed": run_owned_copy_removed,
    }
    evidence.write_bundle("rw-keepalive.json", manifest, "rw-keepalive-events.jsonl", phases)


def run(args: argparse.Namespace) -> int:
    vault = bootstrap.SecretVault()
    evidence: bootstrap.SafeEvidence | None = None
    artifact: dict[str, Any] | None = None
    remote: bootstrap.RemoteRun | None = None
    run_root: Path | None = None
    run_owned_copy_removed = False
    status = "failed"
    error_class = "unexpected"
    result_code = 1
    try:
        source_sha = bootstrap.require_source_sha(args.source_sha)
        expected_file_hash = bootstrap.require_sha256(args.expected_file_hash)
        if not isinstance(args.keepalive_seconds, int) or not 1 <= args.keepalive_seconds <= 900:
            bootstrap.fail("config_invalid")
        evidence = bootstrap.SafeEvidence(args.evidence_dir, vault)
        artifact = native_artifact(source_sha, args.native_manifest, args.windows_binary)
        binary = bootstrap.validate_abs_path(str(args.windows_binary), must_exist=True)
        spec = bootstrap.RoleSpec(
            "rw_provider",
            "winrm",
            binary,
            artifact["sha256"],
            None,
            None,
            "QSYNC_F_WINRM_DESTINATION",
            "127.0.0.1",
            "QSYNC_F_WINRM_HOST",
            "QSYNC_F_WINRM_USERNAME",
            "QSYNC_F_WINRM_PASSWORD",
        )
        run_parent = bootstrap.validate_abs_path(str(args.evidence_dir.parent), must_exist=True, directory=True)
        run_root = Path(tempfile.mkdtemp(prefix="qsync-f-rw-", dir=run_parent))
        run_root.chmod(0o700)
        staged_binary = bootstrap.stage_role_binary(
            spec,
            {
                "target": "windows",
                "sha256": artifact["sha256"],
                "size_bytes": artifact["size_bytes"],
            },
            run_root,
        )
        owner_url_raw = os.environ.get("QSYNC_F_OWNER_API_URL")
        share_key_raw = os.environ.get("QSYNC_F_RW_SHARE_KEY")
        public_host_raw = os.environ.get("QSYNC_F_ARTIFACT_PUBLIC_HOST")
        if not owner_url_raw or not share_key_raw or not public_host_raw:
            bootstrap.fail("external_unavailable", "blocked")
        owner_url = vault.hold(owner_url_raw)
        share_key = vault.hold(share_key_raw)
        public_host = bootstrap.validate_host(public_host_raw)
        destination = bootstrap.fresh_winrm_destination(spec)
        remote = bootstrap.run_winrm_member(
            spec,
            staged_binary,
            artifact["sha256"],
            owner_url,
            share_key,
            destination,
            expected_file_hash,
            public_host,
            artifact["size_bytes"],
            vault,
            keepalive_seconds=args.keepalive_seconds,
        )
        if keepalive_observed(remote, args.keepalive_seconds) and bootstrap.remote_contract_is_complete(
            remote,
            artifact["sha256"],
            artifact["size_bytes"],
            expected_file_hash,
            bootstrap.FIXTURE_A_SIZE_BYTES,
            require_keepalive=True,
        ):
            status = "pass"
            error_class = "none"
        elif remote.command_timed_out or not remote.transport_cleanup_completed or remote.forced_termination:
            status = "pending"
            error_class = remote.transport_error_class or "cleanup_incomplete"
        else:
            status = "failed"
            error_class = remote.transport_error_class or "remote_failure"
        result_code = 0 if status == "pass" else 2 if status == "pending" else 1
    except bootstrap.HarnessError as error:
        status = "blocked" if error.status == "blocked" else "failed"
        error_class = error.error_class
        result_code = 2 if status == "blocked" else 1
    except Exception:
        status = "failed"
        error_class = "unexpected"
        result_code = 1
    finally:
        if run_root is not None:
            can_remove = remote is None or (
                remote.transport_cleanup_completed
                and not remote.command_timed_out
                and not remote.forced_termination
                and remote.graceful_signal
            )
            if can_remove:
                try:
                    shutil.rmtree(run_root)
                    run_owned_copy_removed = not run_root.exists()
                except OSError:
                    run_owned_copy_removed = False
        if evidence is not None:
            try:
                write_result(
                    evidence,
                    source_sha=str(args.source_sha),
                    artifact=artifact,
                    expected_file_hash=expected_file_hash if "expected_file_hash" in locals() else "",
                    requested=int(args.keepalive_seconds),
                    remote=remote,
                    status=status,
                    error_class=error_class,
                    run_owned_copy_removed=run_owned_copy_removed,
                )
            except Exception:
                status = "failed"
                error_class = "unexpected"
                result_code = 1
        vault.clear()
    return result_code


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
