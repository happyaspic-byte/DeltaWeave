#!/usr/bin/env python3
"""Run the hosted-Ubuntu read-only role of the F bootstrap.

Only the one-use share key is an environment input.  The owner endpoint and
administrator credential stay on the owner side; the role driver never places
the share key in argv, child environments, evidence, or logs.
It starts one fresh local member process, exercises the managed API, verifies a
real file hash, reopens the same state, and reports only fixed phase records.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import tempfile
import time
import uuid
from dataclasses import replace
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import qsync_three_host_bootstrap as bootstrap  # noqa: E402


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="qSync hosted Ubuntu read-only role")
    root.add_argument("--source-sha", required=True)
    root.add_argument("--manifest", required=True, type=Path)
    root.add_argument("--binary", required=True, type=Path)
    root.add_argument("--run-parent", required=True, type=Path)
    root.add_argument("--evidence-dir", required=True, type=Path)
    root.add_argument("--share-key-env", required=True)
    root.add_argument("--expected-file-hash", required=True)
    root.add_argument("--expected-file-name", default="fixture-a.bin")
    return root


def safe_env(name: str) -> str:
    return bootstrap.validate_env_name(name)


def load_artifact(source_sha: str, manifest_path: Path, binary: Path) -> dict[str, Any]:
    source_sha = bootstrap.require_source_sha(source_sha)
    manifest = bootstrap.load_json_file(bootstrap.validate_abs_path(str(manifest_path), must_exist=True))
    if manifest.get("status") != "pass" or manifest.get("source_sha") != source_sha or manifest.get("workflow_sha") != source_sha:
        bootstrap.fail("source_mismatch")
    artifact: Any = manifest.get("artifact")
    raw_artifacts = manifest.get("artifacts")
    if isinstance(raw_artifacts, dict):
        artifact = raw_artifacts.get("ro_consumer") or raw_artifacts.get("default")
    else:
        artifact = {
            **bootstrap.json_object(artifact, "manifest_invalid"),
            "source_sha": artifact.get("source_sha", manifest.get("source_sha")),
            "workflow_sha": artifact.get("workflow_sha", manifest.get("workflow_sha")),
            "target": artifact.get("target", manifest.get("target")),
        }
    artifact = bootstrap.json_object(artifact, "manifest_invalid")
    if artifact.get("source_sha") != source_sha or artifact.get("workflow_sha") != source_sha:
        bootstrap.fail("source_mismatch")
    if artifact.get("target") != "linux":
        bootstrap.fail("source_mismatch")
    expected_hash = bootstrap.require_sha256(artifact.get("sha256"), "manifest_invalid")
    expected_size = artifact.get("size_bytes")
    if not isinstance(expected_size, int) or expected_size <= 0:
        bootstrap.fail("manifest_invalid")
    binary = bootstrap.validate_abs_path(str(binary), must_exist=True)
    actual_hash = bootstrap.file_sha256(binary)
    if actual_hash != expected_hash or binary.stat().st_size != expected_size:
        bootstrap.fail("binary_hash_mismatch")
    return {
        "source_sha": source_sha,
        "workflow_sha": source_sha,
        "target": "linux",
        "sha256": expected_hash,
        "size_bytes": expected_size,
    }


def _run(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    vault = bootstrap.SecretVault()
    recorder = bootstrap.PhaseRecorder()
    process: bootstrap.LocalWebProcess | None = None
    run_root: Path | None = None
    evidence: bootstrap.SafeEvidence | None = None
    binary_hashes: dict[str, str] = {}
    file_hash_verified: dict[str, Any] = {}
    result_code = 1
    cleanup = {
        "owned_processes_stopped": False,
        "owned_paths_removed": False,
        "forced_termination_used": False,
        "graceful_drain_proven": "unverified",
        "preexisting_protected_state_unchanged": "unverified",
    }
    try:
        evidence = bootstrap.SafeEvidence(args.evidence_dir, vault)
        bootstrap.require_source_sha(args.source_sha)
        expected_hash = bootstrap.require_sha256(args.expected_file_hash)
        if not args.expected_file_name or not re.fullmatch(r"[A-Za-z0-9._-]{1,128}", args.expected_file_name):
            bootstrap.fail("config_invalid")
        share_key_env = safe_env(args.share_key_env)
        run_parent = bootstrap.validate_abs_path(str(args.run_parent), must_exist=True, directory=True)
        artifact = recorder.run(
            "manifest_verification",
            "controller",
            "ro.provenance.manifest",
            lambda: load_artifact(args.source_sha, args.manifest, args.binary),
        )
        if artifact.status != "pass":
            return 1
        info = artifact.value
        binary = bootstrap.validate_abs_path(str(args.binary), must_exist=True)
        spec = bootstrap.RoleSpec(
            "ro_consumer",
            "local",
            binary,
            info["sha256"],
            None,
            None,
            None,
            "127.0.0.1",
            None,
            None,
            None,
        )
        run_root = Path(tempfile.mkdtemp(prefix="qsync-f-ro-", dir=run_parent))
        run_root.chmod(0o700)
        binary_result = recorder.run(
            "binary_verification",
            "ro_consumer",
            "ro.provenance.binary",
            lambda: bootstrap.stage_role_binary(spec, info, run_root),
        )
        if binary_result.status != "pass":
            return 1
        spec = replace(spec, binary=binary_result.value)
        binary_hashes["ro_consumer"] = info["sha256"]
        self_test = recorder.run(
            "self_test",
            "ro_consumer",
            "ro.host.self_test",
            lambda: bootstrap.run_self_test(spec, run_root / "ro-self-test-profile"),
        )
        if self_test.status != "pass":
            return 1

        share_key_raw = os.environ.get(share_key_env)
        if not share_key_raw:
            bootstrap.fail("external_unavailable", "blocked")
        share_key = vault.hold(share_key_raw)
        process = bootstrap.LocalWebProcess(spec, run_root, "ro_consumer", vault)
        start = recorder.run("member_web_start", "ro_consumer", "ro.web.start", process.start)
        if start.status != "pass":
            return 1
        member_login = recorder.run("member_login", "ro_consumer", "ro.web.login", process.client)
        if member_login.status != "pass":
            return 1
        member_client: bootstrap.ApiClient = member_login.value

        preview = recorder.run(
            "local_preview",
            "ro_consumer",
            "ro.managed.preview",
            lambda: member_client.preview(share_key),
        )
        if preview.status != "pass":
            return 1
        validation = recorder.run(
            "online_validate",
            "ro_consumer",
            "ro.managed.validate",
            lambda: member_client.validate(share_key),
        )
        if validation.status != "pass":
            return 1
        destination = process.root
        joined = recorder.run(
            "member_join",
            "ro_consumer",
            "ro.managed.join",
            lambda: member_client.join(share_key, destination),
        )
        if joined.status != "pass":
            return 1
        share_id = joined.value
        target = destination / args.expected_file_name

        def check_file() -> dict[str, Any]:
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                if target.is_file():
                    actual = bootstrap.file_sha256(target)
                    if actual == expected_hash:
                        return {"sha256": actual, "size_bytes": target.stat().st_size, "observed": True}
                time.sleep(0.5)
            bootstrap.fail("hash_mismatch")

        checked = recorder.run("file_hash", "ro_consumer", "ro.managed.file_hash", check_file)
        if checked.status != "pass":
            return 1
        file_hash_verified["ro_consumer"] = checked.value

        stopped = process.stop()
        cleanup["forced_termination_used"] = process.forced_termination
        # Stopping the OS process is not a managed pause/revoke drain ACK.  Do
        # not start the recovery leg after a forced or otherwise uncertain
        # termination; retain the owned namespace for inspection.
        if not stopped or process.forced_termination:
            recorder.add(
                phase="member_reopen_membership",
                role="ro_consumer",
                command_id="ro.reopen.precondition",
                status="failed",
                started=bootstrap.utc_now(),
                finished=bootstrap.utc_now(),
                elapsed_ms=0,
                error_class="cleanup_incomplete",
            )
            return 1
        restart = recorder.run("member_web_start", "ro_consumer", "ro.web.restart", process.start)
        if restart.status != "pass":
            return 1
        reopened_login = recorder.run("member_login", "ro_consumer", "ro.web.reopen.login", process.client)
        if reopened_login.status != "pass":
            return 1
        reopened_client: bootstrap.ApiClient = reopened_login.value
        membership = reopened_client._call("GET", f"/api/v1/shares/{share_id}")
        reopened_hash = bootstrap.file_sha256(target) if target.is_file() else ""
        reopen_ok = membership[0] == 200 and reopened_hash == expected_hash
        recorder.add(
            phase="member_reopen_membership",
            role="ro_consumer",
            command_id="ro.managed.reopen.membership",
            status="pass" if reopen_ok else "failed",
            started=bootstrap.utc_now(),
            finished=bootstrap.utc_now(),
            elapsed_ms=0,
            error_class="none" if reopen_ok else "hash_mismatch",
        )
        if not reopen_ok:
            return 1
        result_code = 0
        return result_code
    except bootstrap.HarnessError as error:
        result_code = 2 if error.status == "blocked" else 1
        return result_code
    except Exception:
        result_code = 1
        return result_code
    finally:
        if process is not None:
            started_once = process.started_once
            try:
                stopped = process.stop()
                cleanup["owned_processes_stopped"] = stopped
                cleanup["forced_termination_used"] = cleanup["forced_termination_used"] or process.forced_termination
            except Exception:
                cleanup["owned_processes_stopped"] = False
                started_once = True
            cleanup["graceful_drain_proven"] = (
                "verified" if started_once and process.graceful_drain_proven
                else "unverified" if started_once
                else "not_needed"
            )
        else:
            # No child was created, so the run namespace can be removed even
            # when an earlier precondition failed.
            cleanup["owned_processes_stopped"] = True
            cleanup["graceful_drain_proven"] = "not_needed"
        if (
            run_root is not None
            and cleanup["owned_processes_stopped"]
            and cleanup["graceful_drain_proven"] in {"verified", "not_needed"}
            and not cleanup["forced_termination_used"]
        ):
            try:
                shutil.rmtree(run_root)
                cleanup["owned_paths_removed"] = not run_root.exists()
            except OSError:
                cleanup["owned_paths_removed"] = False
        if evidence is not None:
            cleanup_status = "pass" if run_root is None or cleanup["owned_paths_removed"] else "pending"
            recorder.add(
                phase="cleanup",
                role="controller",
                command_id="ro.cleanup.run_owned_only",
                status=cleanup_status,
                started=bootstrap.utc_now(),
                finished=bootstrap.utc_now(),
                elapsed_ms=0,
                error_class="none" if cleanup_status == "pass" else "cleanup_incomplete",
            )
            phases_failed = any(item["status"] == "failed" for item in recorder.phases)
            phases_pending = any(item["status"] == "pending" for item in recorder.phases)
            phases_blocked = any(item["status"] == "blocked" for item in recorder.phases)
            final_status = "failed" if phases_failed else (
                "pending" if phases_pending else ("blocked" if phases_blocked else "pass")
            )
            if result_code == 2 and final_status == "pass":
                final_status = "blocked"
            elif result_code != 0 and final_status == "pass":
                final_status = "failed"
            manifest = {
                "scope": "three_host_transport_smoke_subset",
                "harness_version": "qsync-f-ro-1",
                "status": final_status,
                "source_sha": args.source_sha,
                "binary_sha256": binary_hashes,
                "topology": [{"role": "ro_consumer", "label": "hosted-ubuntu-ro", "runner": "local"}],
                "providers": [
                    {"role": "owner-provider", "epoch": 0, "verified_chunks": 0, "verified_bytes": 0},
                    {"role": "rw-provider", "epoch": None, "verified_chunks": 0, "verified_bytes": 0},
                ],
                "file_hash_verified": file_hash_verified,
                "phases": recorder.phases,
                "cleanup": cleanup,
                "full_f_claim": False,
                "raw_output_retained": False,
            }
            try:
                evidence.write_bundle("f-ro-result.json", manifest, "f-ro-phase-events.jsonl", recorder.phases)
            except Exception:
                vault.clear()
                raise bootstrap.HarnessError("unexpected", "failed", 1)
            if result_code == 0 and final_status != "pass":
                vault.clear()
                raise bootstrap.HarnessError("cleanup_incomplete", "failed", 1)
        vault.clear()


def main(argv: list[str] | None = None) -> int:
    try:
        return _run(argv)
    except bootstrap.HarnessError:
        return 1
    except Exception:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
