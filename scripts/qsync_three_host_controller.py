#!/usr/bin/env python3
"""Local controller for the prepare/execute qSync F handoff.

The controller owns the boundary between GitHub Actions artifacts and the
test-owned local run.  It never puts a share key, credential, bearer, endpoint
URL, or remote command output in a command argument or evidence record.  The
``prepare`` phase builds/verifies immutable role artifacts; ``execute`` only
reuses that prepared run, waits for an already-ready RW provider, dispatches
the hosted RO workflow, and downloads its redacted result.  Product crates are
intentionally outside this module.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import qsync_three_host_bootstrap as bootstrap  # noqa: E402


CONTROLLER_VERSION = "qsync-f-controller-1"
DEFAULT_WORKFLOW = "ci.yml"
PREPARED_ROLE_ARTIFACT_PREFIX = "qsync-f-linux-role-"
NATIVE_ARTIFACT_PREFIX = "qsync-native-windows-"
RO_EVIDENCE_PREFIX = "qsync-f-ro-evidence-"
HOSTED_RO_JOB_NAME = "F hosted Ubuntu read-only consumer"
FIXTURE_SIZE_BYTES = 8 * 1024 * 1024
FIXTURE_CHUNK_BYTES = 64 * 1024
FIXTURE_SEED = b"deltaweave-qsync-f-fixture-v2"
MAX_ARTIFACT_BYTES = 512 * 1024 * 1024
# Preparation may need the full native/Release build window.  Execution only
# waits for the hosted RO leg after a local RW readiness handoff.
MAX_PREPARE_WAIT_SECONDS = 60 * 60
MAX_EXECUTE_WAIT_SECONDS = 15 * 60
# Kept as a compatibility alias for callers that used the first controller
# draft; new code must choose the phase-specific budget above.
MAX_WORKFLOW_WAIT_SECONDS = MAX_EXECUTE_WAIT_SECONDS
MAX_CONTROLLER_RUNS = 100
STATE_FILENAME = "controller-state.json"
OWNED_MARKER = ".qsync-controller-owned"

SOURCE_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
RUN_ID_RE = re.compile(r"^[0-9]{1,20}$")
REPO_RE = re.compile(r"^[A-Za-z0-9_.-]{1,100}/[A-Za-z0-9_.-]{1,100}$")
REF_RE = re.compile(r"^[A-Za-z0-9_./-]{1,200}$")
WORKFLOW_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}(?:\.ya?ml)?$")
INPUT_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
SECRET_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]{0,99}$")


class ControllerError(Exception):
    """Fixed safe error with no user-controlled display text."""

    def __init__(self, error_class: str, status: str = "failed", exit_code: int = 1):
        allowed = {
            "config_invalid",
            "path_invalid",
            "workflow_dispatch_failed",
            "workflow_run_missing",
            "workflow_failed",
            "workflow_timeout",
            "artifact_download_failed",
            "artifact_invalid",
            "source_mismatch",
            "binary_missing",
            "binary_hash_mismatch",
            "external_unavailable",
            "remote_failure",
            "process_start_failed",
            "auth_failed",
            "api_http_error",
            "api_response_invalid",
            "secret_exists",
            "secret_set_failed",
            "secret_delete_failed",
            "rw_not_ready",
            "state_invalid",
            "cleanup_incomplete",
            "fixture_invalid",
            "unexpected",
        }
        self.error_class = error_class if error_class in allowed else "unexpected"
        self.status = status if status in {"failed", "pending", "blocked"} else "failed"
        self.exit_code = int(exit_code)
        super().__init__(self.error_class)


def fail(error_class: str, status: str = "failed", exit_code: int = 1) -> None:
    raise ControllerError(error_class, status, exit_code)


def require_source_sha(value: Any) -> str:
    if not isinstance(value, str) or not SOURCE_SHA_RE.fullmatch(value):
        fail("config_invalid")
    return value


def require_sha256(value: Any, error_class: str = "config_invalid") -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        fail(error_class)
    return value


def require_run_id(value: Any) -> str:
    if isinstance(value, int) and not isinstance(value, bool):
        value = str(value)
    if not isinstance(value, str) or not RUN_ID_RE.fullmatch(value):
        fail("config_invalid")
    return value


def require_positive_size(value: Any, *, exact: int | None = None) -> int:
    if isinstance(value, str) and value.isdecimal():
        value = int(value)
    if type(value) is not int or value <= 0 or value > 128 * 1024 * 1024:
        fail("config_invalid")
    if exact is not None and value != exact:
        fail("fixture_invalid")
    return value


def require_repo(value: Any) -> str:
    if not isinstance(value, str) or not REPO_RE.fullmatch(value):
        fail("config_invalid")
    return value


def require_ref(value: Any) -> str:
    if (
        not isinstance(value, str)
        or not REF_RE.fullmatch(value)
        or value.startswith("-")
        or ".." in value
        or value.endswith("/")
    ):
        fail("config_invalid")
    return value


def require_workflow(value: Any) -> str:
    if not isinstance(value, str) or not WORKFLOW_RE.fullmatch(value):
        fail("config_invalid")
    return value


def require_input_name(value: Any) -> str:
    if not isinstance(value, str) or not INPUT_RE.fullmatch(value):
        fail("config_invalid")
    return value


def require_secret_name(value: Any) -> str:
    if not isinstance(value, str) or not SECRET_NAME_RE.fullmatch(value):
        fail("config_invalid")
    return value


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_utc(value: Any) -> float:
    if not isinstance(value, str):
        fail("workflow_run_missing")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        fail("workflow_run_missing")


def _safe_relative_name(value: Any) -> str:
    if not isinstance(value, str) or not value or value.startswith("/") or "\\" in value:
        fail("artifact_invalid")
    path = Path(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        fail("artifact_invalid")
    return value


def _ensure_private_directory(path: Path, *, create: bool = False) -> Path:
    if not path.is_absolute() or path in {Path("/"), Path("\\")} or path.is_symlink():
        fail("path_invalid")
    try:
        if create:
            path.mkdir(mode=0o700, parents=False, exist_ok=False)
        if not path.is_dir() or path.is_symlink():
            fail("path_invalid")
        path.chmod(0o700)
    except ControllerError:
        raise
    except (OSError, ValueError):
        fail("path_invalid")
    return path


def _within(child: Path, parent: Path) -> bool:
    try:
        child.resolve(strict=False).relative_to(parent.resolve(strict=True))
        return True
    except (OSError, ValueError):
        return False


def fixture_block(index: int, length: int = 32) -> bytes:
    if type(index) is not int or index < 0 or length <= 0 or length > 32:
        fail("fixture_invalid")
    return hashlib.blake2s(FIXTURE_SEED + index.to_bytes(8, "big"), digest_size=32).digest()[:length]


def fixture_chunk(index: int) -> bytes:
    """Return one deterministic, non-repeating 64 KiB fixture chunk."""

    if type(index) is not int or index < 0:
        fail("fixture_invalid")
    blocks_per_chunk = FIXTURE_CHUNK_BYTES // 32
    first_block = index * blocks_per_chunk
    return b"".join(fixture_block(first_block + offset) for offset in range(blocks_per_chunk))


def write_fixture(path: Path, size: int = FIXTURE_SIZE_BYTES) -> str:
    """Write deterministic 64 KiB counter-hash chunks and return their SHA-256."""

    size = require_positive_size(size, exact=FIXTURE_SIZE_BYTES)
    if not path.is_absolute() or path.exists() or path.is_symlink() or not path.parent.is_dir():
        fail("path_invalid")
    digest = hashlib.sha256()
    written = 0
    chunk_index = 0
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as handle:
            while written < size:
                chunk = fixture_chunk(chunk_index)
                chunk_index += 1
                take = min(len(chunk), size - written)
                handle.write(chunk[:take])
                digest.update(chunk[:take])
                written += take
            handle.flush()
            os.fsync(handle.fileno())
    except OSError:
        try:
            path.unlink()
        except OSError:
            pass
        fail("path_invalid")
    return digest.hexdigest()


def _hash_file(path: Path) -> str:
    if not path.is_file() or path.is_symlink():
        fail("binary_missing")
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError:
        fail("binary_missing")
    return digest.hexdigest()


def _reject_symlink_components(root: Path, relative: str) -> Path:
    """Resolve a prepared relative name without following any component."""

    if not root.is_absolute() or root.is_symlink() or not root.is_dir():
        fail("artifact_invalid")
    candidate = root
    for component in Path(relative).parts:
        candidate = candidate / component
        try:
            if candidate.is_symlink():
                fail("artifact_invalid")
        except OSError:
            fail("artifact_invalid")
    return candidate


def prepared_role_binary(state_path: Path, state: Mapping[str, Any], role: str) -> dict[str, Any]:
    """Return a manifest-bound prepared binary for one controller role.

    The state contains only a safe relative artifact name.  Every path
    component is checked before opening it, and the bytes are re-hashed at the
    point of use so an artifact replacement cannot change the role provenance
    between prepare and execute.
    """

    if role not in {"owner", "rw_provider", "ro_consumer"}:
        fail("artifact_invalid")
    source_sha = require_source_sha(state.get("source_sha"))
    artifacts = state.get("artifacts")
    roles = artifacts.get("roles") if isinstance(artifacts, Mapping) else None
    descriptor = roles.get(role) if isinstance(roles, Mapping) else None
    if not isinstance(descriptor, Mapping):
        fail("artifact_invalid")
    if descriptor.get("source_sha") != source_sha or descriptor.get("workflow_sha") != source_sha:
        fail("source_mismatch")
    expected_target = "windows" if role == "rw_provider" else "linux"
    if descriptor.get("target") != expected_target:
        fail("artifact_invalid")
    expected_hash = require_sha256(descriptor.get("sha256"), "artifact_invalid")
    expected_size = require_positive_size(descriptor.get("size_bytes"))
    relative = _safe_relative_name(descriptor.get("binary_name"))
    if not state_path.is_absolute() or state_path.name != STATE_FILENAME or state_path.is_symlink():
        fail("path_invalid")
    candidate = _reject_symlink_components(state_path.parent, relative)
    try:
        if not candidate.is_file() or candidate.is_symlink():
            fail("binary_missing")
        actual_size = candidate.stat().st_size
    except OSError:
        fail("binary_missing")
    if actual_size != expected_size or _hash_file(candidate) != expected_hash:
        fail("binary_hash_mismatch")
    return {
        "path": candidate,
        "source_sha": source_sha,
        "target": expected_target,
        "sha256": expected_hash,
        "size_bytes": expected_size,
        "binary_name": relative,
    }


def _scan_artifact_tree(root: Path) -> None:
    if not root.is_dir() or root.is_symlink():
        fail("artifact_invalid")
    total = 0
    try:
        entries = list(root.rglob("*"))
    except OSError:
        fail("artifact_invalid")
    for entry in entries:
        if entry.is_symlink() or (entry.is_dir() and entry.is_symlink()):
            fail("artifact_invalid")
        try:
            if entry.is_file():
                total += entry.stat().st_size
        except OSError:
            fail("artifact_invalid")
    if total > MAX_ARTIFACT_BYTES:
        fail("artifact_invalid")


def _find_unique(root: Path, name: str) -> Path:
    matches = [item for item in root.rglob(name) if item.is_file() and not item.is_symlink()]
    if len(matches) != 1:
        fail("artifact_invalid")
    return matches[0]


def _load_json(path: Path) -> dict[str, Any]:
    try:
        if not path.is_file() or path.stat().st_size > 2 * 1024 * 1024:
            fail("artifact_invalid")
        value = json.loads(path.read_text(encoding="utf-8"))
    except ControllerError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError):
        fail("artifact_invalid")
    if not isinstance(value, dict):
        fail("artifact_invalid")
    return value


def verify_native_artifact(root: Path, source_sha: str) -> dict[str, Any]:
    source_sha = require_source_sha(source_sha)
    manifest_path = _find_unique(root, "native-verification-manifest.json")
    binary_path = _find_unique(root, "deltaweave.exe")
    manifest = _load_json(manifest_path)
    if (
        manifest.get("status") != "pass"
        or manifest.get("source_sha") != source_sha
        or manifest.get("workflow_sha") != source_sha
        or manifest.get("target") != "x86_64-pc-windows-msvc"
    ):
        fail("source_mismatch")
    artifact = manifest.get("artifact")
    if not isinstance(artifact, dict):
        fail("artifact_invalid")
    expected_hash = require_sha256(artifact.get("sha256"), "artifact_invalid")
    expected_size = require_positive_size(artifact.get("size_bytes"))
    if _hash_file(binary_path) != expected_hash or binary_path.stat().st_size != expected_size:
        fail("binary_hash_mismatch")
    return {
        "source_sha": source_sha,
        "workflow_sha": source_sha,
        "target": "windows",
        "sha256": expected_hash,
        "size_bytes": expected_size,
        "binary_name": _safe_relative_name(binary_path.relative_to(root).as_posix()),
        "manifest_name": _safe_relative_name(manifest_path.relative_to(root).as_posix()),
    }


def verify_role_artifact(root: Path, source_sha: str) -> dict[str, dict[str, Any]]:
    source_sha = require_source_sha(source_sha)
    manifest_path = _find_unique(root, "f-role-manifest.json")
    manifest = _load_json(manifest_path)
    if manifest.get("status") != "pass" or manifest.get("source_sha") != source_sha or manifest.get("workflow_sha") != source_sha:
        fail("source_mismatch")
    raw_artifacts = manifest.get("artifacts")
    if not isinstance(raw_artifacts, dict) or set(raw_artifacts) != {"owner", "rw_provider", "ro_consumer"}:
        fail("artifact_invalid")
    linux_binary = _find_unique(root, "deltaweave")
    expected: dict[str, dict[str, Any]] = {}
    for role, target in (("owner", "linux"), ("rw_provider", "windows"), ("ro_consumer", "linux")):
        descriptor = raw_artifacts.get(role)
        if not isinstance(descriptor, dict):
            fail("artifact_invalid")
        if descriptor.get("source_sha") != source_sha or descriptor.get("workflow_sha") != source_sha:
            fail("source_mismatch")
        if descriptor.get("target") != target:
            fail("artifact_invalid")
        expected[role] = {
            "source_sha": source_sha,
            "workflow_sha": source_sha,
            "target": target,
            "sha256": require_sha256(descriptor.get("sha256"), "artifact_invalid"),
            "size_bytes": require_positive_size(descriptor.get("size_bytes")),
        }
    if expected["owner"]["sha256"] != expected["ro_consumer"]["sha256"]:
        fail("artifact_invalid")
    if _hash_file(linux_binary) != expected["owner"]["sha256"] or linux_binary.stat().st_size != expected["owner"]["size_bytes"]:
        fail("binary_hash_mismatch")
    try:
        linux_binary.chmod(0o700)
    except OSError:
        fail("path_invalid")
    expected["owner"]["binary_name"] = _safe_relative_name(linux_binary.relative_to(root).as_posix())
    expected["ro_consumer"]["binary_name"] = expected["owner"]["binary_name"]
    expected["rw_provider"]["binary_name"] = "native-artifact/deltaweave.exe"
    expected["manifest_name"] = _safe_relative_name(manifest_path.relative_to(root).as_posix())  # type: ignore[index]
    return expected


def verify_ro_evidence(root: Path, state: Mapping[str, Any]) -> dict[str, Any]:
    """Verify only the redacted, role-scoped result downloaded from Actions."""

    _scan_artifact_tree(root)
    result_path = _find_unique(root, "f-ro-result.json")
    result = _load_json(result_path)
    _state_safe(result)
    if (
        result.get("status") != "pass"
        or result.get("source_sha") != state.get("source_sha")
        or result.get("full_f_claim") is not False
        or result.get("expected_file_hash") != state.get("expected_file_hash")
        or result.get("expected_file_size_bytes") != state.get("expected_file_size_bytes")
    ):
        fail("artifact_invalid")
    observed = result.get("file_hash_verified")
    if not isinstance(observed, Mapping):
        fail("artifact_invalid")
    expected_hash = require_sha256(state.get("expected_file_hash"), "artifact_invalid")
    expected_size = require_positive_size(state.get("expected_file_size_bytes"))
    if not any(
        isinstance(value, Mapping)
        and value.get("sha256") == expected_hash
        and value.get("size_bytes") == expected_size
        and value.get("observed") is True
        for value in observed.values()
    ):
        fail("artifact_invalid")
    return {
        "result_name": _safe_relative_name(result_path.relative_to(root).as_posix()),
        "status": "pass",
        "source_sha": state["source_sha"],
        "expected_file_hash": expected_hash,
        "expected_file_size_bytes": expected_size,
    }


class ActionsBackend(Protocol):
    def dispatch(self, workflow: str, ref: str, inputs: Mapping[str, str]) -> None: ...

    def list_runs(self, workflow: str, ref: str) -> list[dict[str, Any]]: ...

    def wait_run(
        self,
        workflow: str,
        ref: str,
        source_sha: str,
        not_before: float,
        *,
        excluded_run_ids: frozenset[str] = frozenset(),
        coordination_run_id: str | None = None,
        timeout_seconds: int = MAX_EXECUTE_WAIT_SECONDS,
    ) -> dict[str, Any]: ...

    def wait_hosted_job(
        self,
        workflow: str,
        ref: str,
        source_sha: str,
        not_before: float,
        *,
        excluded_run_ids: frozenset[str] = frozenset(),
        coordination_run_id: str | None = None,
        timeout_seconds: int = MAX_EXECUTE_WAIT_SECONDS,
    ) -> dict[str, Any]: ...

    def download_artifact(self, run_id: str, name: str, destination: Path) -> Path: ...

    def list_secret_names(self) -> set[str]: ...

    def set_secret(self, name: str, value: str) -> None: ...

    def delete_secret(self, name: str) -> None: ...


def _command_result(argv: Sequence[str], *, input_data: bytes | None = None, timeout: float = 60.0) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            list(argv),
            input=input_data,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        fail("workflow_timeout" if timeout <= MAX_WORKFLOW_WAIT_SECONDS else "unexpected", "pending")


class GhActions:
    """Small safe adapter around ``gh``; subprocess output is never returned."""

    def __init__(self, repo: str, *, command: Callable[..., subprocess.CompletedProcess[bytes]] | None = None):
        self.repo = require_repo(repo)
        self._command = command or _command_result
        if shutil.which("gh") is None and command is None:
            fail("workflow_dispatch_failed", "blocked", 2)

    def _run(self, argv: Sequence[str], *, input_data: bytes | None = None, timeout: float = 60.0) -> subprocess.CompletedProcess[bytes]:
        result = self._command(argv, input_data=input_data, timeout=timeout)
        if not isinstance(result, subprocess.CompletedProcess):
            fail("unexpected")
        return result

    def dispatch(self, workflow: str, ref: str, inputs: Mapping[str, str]) -> None:
        workflow = require_workflow(workflow)
        ref = require_ref(ref)
        argv = ["gh", "workflow", "run", workflow, "--repo", self.repo, "--ref", ref]
        for key, value in sorted(inputs.items()):
            require_input_name(key)
            if not isinstance(value, str) or not value or "\x00" in value or "\n" in value or "\r" in value:
                fail("config_invalid")
            argv.extend(("--field", f"{key}={value}"))
        result = self._run(argv)
        if result.returncode != 0:
            fail("workflow_dispatch_failed", "blocked", 2)

    def list_runs(self, workflow: str, ref: str) -> list[dict[str, Any]]:
        workflow = require_workflow(workflow)
        ref = require_ref(ref)
        result = self._run(
            [
                "gh",
                "run",
                "list",
                "--repo",
                self.repo,
                "--workflow",
                workflow,
                "--branch",
                ref,
                "--limit",
                str(MAX_CONTROLLER_RUNS),
                "--json",
                "databaseId,headSha,status,conclusion,createdAt,event,displayTitle,name",
            ]
        )
        if result.returncode != 0:
            fail("workflow_run_missing", "pending", 2)
        try:
            value = json.loads(result.stdout.decode("utf-8", "strict"))
        except (UnicodeError, json.JSONDecodeError):
            fail("workflow_run_missing", "pending", 2)
        if not isinstance(value, list):
            fail("workflow_run_missing", "pending", 2)
        return [item for item in value if isinstance(item, dict)]

    def wait_run(
        self,
        workflow: str,
        ref: str,
        source_sha: str,
        not_before: float,
        *,
        excluded_run_ids: frozenset[str] = frozenset(),
        coordination_run_id: str | None = None,
        timeout_seconds: int = MAX_EXECUTE_WAIT_SECONDS,
    ) -> dict[str, Any]:
        source_sha = require_source_sha(source_sha)
        if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= MAX_PREPARE_WAIT_SECONDS:
            fail("config_invalid")
        deadline = time.monotonic() + timeout_seconds
        selected_run_id: str | None = None
        while True:
            rows = self.list_runs(workflow, ref)
            if selected_run_id is None:
                try:
                    run = select_dispatched_run(
                        rows,
                        source_sha,
                        not_before,
                        excluded_run_ids=excluded_run_ids,
                        coordination_run_id=coordination_run_id,
                    )
                except ControllerError as error:
                    if error.error_class != "workflow_run_missing":
                        raise
                    run = None
                if run is not None:
                    selected_run_id = require_run_id(run.get("databaseId"))
            else:
                run = next(
                    (
                        {**dict(row), "databaseId": selected_run_id}
                        for row in rows
                        if str(row.get("databaseId")) == selected_run_id
                    ),
                    None,
                )
            if run is not None:
                status = run.get("status")
                if status == "completed":
                    if run.get("conclusion") != "success":
                        fail("workflow_failed")
                    return run
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                fail("workflow_timeout", "pending", 2)
            time.sleep(min(5.0, remaining))

    def wait_hosted_job(
        self,
        workflow: str,
        ref: str,
        source_sha: str,
        not_before: float,
        *,
        excluded_run_ids: frozenset[str] = frozenset(),
        coordination_run_id: str | None = None,
        timeout_seconds: int = MAX_EXECUTE_WAIT_SECONDS,
    ) -> dict[str, Any]:
        """Wait only for the hosted RO job in one already-correlated run.

        The caller run can contain unrelated or skipped jobs.  Once the exact
        dispatch is selected, pin its numeric ID and poll its job list rather
        than waiting for the whole workflow to complete.
        """

        source_sha = require_source_sha(source_sha)
        if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= MAX_PREPARE_WAIT_SECONDS:
            fail("config_invalid")
        deadline = time.monotonic() + timeout_seconds
        selected_run: dict[str, Any] | None = None
        selected_run_id: str | None = None
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                fail("workflow_timeout", "pending", 2)
            if selected_run is None:
                rows = self.list_runs(workflow, ref)
                try:
                    selected_run = select_dispatched_run(
                        rows,
                        source_sha,
                        not_before,
                        excluded_run_ids=excluded_run_ids,
                        coordination_run_id=coordination_run_id,
                    )
                except ControllerError as error:
                    if error.error_class != "workflow_run_missing":
                        raise
                    time.sleep(min(5.0, remaining))
                    continue
                selected_run_id = require_run_id(selected_run.get("databaseId"))
            result = self._run(
                [
                    "gh",
                    "run",
                    "view",
                    selected_run_id,
                    "--repo",
                    self.repo,
                    "--json",
                    "jobs",
                ],
                timeout=min(60.0, max(1.0, remaining)),
            )
            if result.returncode != 0:
                fail("workflow_run_missing", "pending", 2)
            try:
                payload = json.loads(result.stdout.decode("utf-8", "strict"))
            except (UnicodeError, json.JSONDecodeError):
                fail("workflow_run_missing", "pending", 2)
            jobs = payload.get("jobs") if isinstance(payload, dict) else None
            if not isinstance(jobs, list):
                fail("workflow_run_missing", "pending", 2)
            matching = [job for job in jobs if isinstance(job, dict) and job.get("name") == HOSTED_RO_JOB_NAME]
            if len(matching) > 1:
                fail("workflow_run_missing", "pending", 2)
            if matching:
                job = matching[0]
                if job.get("status") == "completed":
                    if job.get("conclusion") != "success":
                        fail("workflow_failed")
                    return {"run": selected_run, "job": job}
            if selected_run.get("status") == "completed" and not matching:
                fail("workflow_run_missing", "pending", 2)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                fail("workflow_timeout", "pending", 2)
            time.sleep(min(5.0, remaining))

    def download_artifact(self, run_id: str, name: str, destination: Path) -> Path:
        run_id = require_run_id(run_id)
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", name):
            fail("config_invalid")
        if destination.exists() or destination.is_symlink():
            fail("path_invalid")
        parent = destination.parent
        _ensure_private_directory(parent)
        try:
            destination.mkdir(mode=0o700, parents=False, exist_ok=False)
        except OSError:
            fail("path_invalid")
        result = self._run(
            [
                "gh",
                "run",
                "download",
                run_id,
                "--repo",
                self.repo,
                "--name",
                name,
                "--dir",
                str(destination),
            ],
            timeout=120,
        )
        if result.returncode != 0:
            try:
                shutil.rmtree(destination)
            except OSError:
                pass
            fail("artifact_download_failed", "pending", 2)
        _scan_artifact_tree(destination)
        return destination

    def list_secret_names(self) -> set[str]:
        result = self._run(["gh", "secret", "list", "--repo", self.repo, "--json", "name"])
        if result.returncode != 0:
            fail("secret_set_failed", "blocked", 2)
        try:
            value = json.loads(result.stdout.decode("utf-8", "strict"))
        except (UnicodeError, json.JSONDecodeError):
            fail("secret_set_failed", "blocked", 2)
        if not isinstance(value, list):
            fail("secret_set_failed", "blocked", 2)
        names: set[str] = set()
        for item in value:
            if isinstance(item, dict) and isinstance(item.get("name"), str) and SECRET_NAME_RE.fullmatch(item["name"]):
                names.add(item["name"])
        return names

    def set_secret(self, name: str, value: str) -> None:
        name = require_secret_name(name)
        if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 256 * 1024:
            fail("config_invalid")
        result = self._run(
            # gh reads the value from stdin when --body is omitted.  Passing
            # --body - stores a literal dash with current gh versions.
            ["gh", "secret", "set", name, "--repo", self.repo],
            input_data=value.encode("utf-8"),
            timeout=60,
        )
        if result.returncode != 0:
            fail("secret_set_failed", "pending", 2)

    def delete_secret(self, name: str) -> None:
        name = require_secret_name(name)
        result = self._run(["gh", "secret", "delete", name, "--repo", self.repo], timeout=60)
        if result.returncode != 0:
            fail("secret_delete_failed", "pending", 2)


def select_dispatched_run(
    rows: Iterable[Mapping[str, Any]],
    source_sha: str,
    not_before: float,
    *,
    excluded_run_ids: frozenset[str] = frozenset(),
    coordination_run_id: str | None = None,
) -> dict[str, Any]:
    source_sha = require_source_sha(source_sha)
    if coordination_run_id is not None:
        coordination_run_id = require_run_id(coordination_run_id)
    candidates: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        if row.get("event") != "workflow_dispatch" or row.get("headSha") != source_sha:
            continue
        try:
            run_id = require_run_id(row.get("databaseId"))
            created = parse_utc(row.get("createdAt"))
        except ControllerError:
            continue
        if run_id in excluded_run_ids:
            continue
        if created + 30 < not_before:
            continue
        if coordination_run_id is not None:
            # CI's run-name includes the controller coordination number.  A
            # source/ref match alone is insufficient because an earlier
            # dispatch may still be completed when execute starts.
            title = " ".join(str(row.get(field) or "") for field in ("displayTitle", "name"))
            marker = rf"(?<![0-9])qsync-{re.escape(coordination_run_id)}(?![0-9])"
            if re.search(marker, str(title)) is None:
                continue
        candidates.append({**dict(row), "databaseId": run_id})
    if not candidates:
        fail("workflow_run_missing", "pending", 2)
    candidates.sort(key=lambda item: (parse_utc(item.get("createdAt")), int(item["databaseId"])))
    return candidates[0]


class RemoteReadinessObserver:
    """Extract a live RW readiness attestation from WinRM stdout chunks.

    Only the fixed FROLE/FTRACE vocabulary is retained.  The complete remote
    transcript remains owned by the worker and is never copied into controller
    state or evidence by this observer.
    """

    _MAX_BUFFER_BYTES = 64 * 1024

    def __init__(self, expected_file_hash: str, expected_file_size: int, keepalive_seconds: int):
        self.expected_file_hash = require_sha256(expected_file_hash)
        self.expected_file_size = require_positive_size(expected_file_size, exact=FIXTURE_SIZE_BYTES)
        if type(keepalive_seconds) is not int or not 1 <= keepalive_seconds <= 900:
            fail("config_invalid")
        self.keepalive_seconds = keepalive_seconds
        self._buffer = b""
        self._lock = threading.Lock()
        self._ready = threading.Event()
        self._ready_at: float | None = None
        self._member_join_ok = False
        self._file_hash_ok = False
        self._file_hash: str | None = None
        self._file_size: int | None = None
        self._keepalive_enter = False
        self._keepalive_count: int | None = None
        self._error_class: str | None = None

    def _maybe_ready(self) -> None:
        if (
            self._member_join_ok
            and self._file_hash_ok
            and self._keepalive_enter
            and self._keepalive_count == self.keepalive_seconds
            and self._error_class is None
            and not self._ready.is_set()
        ):
            self._ready_at = time.monotonic()
            self._ready.set()

    def _consume_line(self, line: bytes) -> None:
        try:
            text = line.strip().decode("ascii", "strict")
        except UnicodeDecodeError:
            return
        phase = bootstrap.REMOTE_LINE_RE.fullmatch(text)
        if phase is not None:
            name, ok, hash_value, size_value, _forced, _signal, error_class = phase.groups()
            if error_class not in (None, "none"):
                # Keep the controller vocabulary fixed even when the remote
                # role reports a known-but-more-specific error.  A failed
                # phase must never be followed by a later line that is
                # mistaken for a live readiness attestation.
                self._error_class = "remote_failure"
            if name == "member_join":
                self._member_join_ok = ok == "true"
            elif name == "file_hash":
                self._file_hash = hash_value
                self._file_size = int(size_value) if size_value is not None else None
                self._file_hash_ok = (
                    ok == "true"
                    and hash_value == self.expected_file_hash
                    and self._file_size == self.expected_file_size
                )
            self._maybe_ready()
            return
        trace = bootstrap.REMOTE_TRACE_RE.fullmatch(text)
        if trace is None or trace.group(1) != "keepalive_enter":
            return
        count = trace.group(2)
        if count is None:
            self._error_class = "remote_failure"
            return
        self._keepalive_count = int(count)
        self._keepalive_enter = self._keepalive_count == self.keepalive_seconds
        self._maybe_ready()

    def feed(self, chunk: bytes) -> None:
        if not isinstance(chunk, bytes) or not chunk:
            return
        with self._lock:
            self._buffer += chunk
            if len(self._buffer) > self._MAX_BUFFER_BYTES:
                # Keep only the incomplete tail; old, non-readiness output is
                # intentionally discarded rather than retained in memory.
                self._buffer = self._buffer[-self._MAX_BUFFER_BYTES :]
                newline = self._buffer.find(b"\n")
                if newline >= 0:
                    self._buffer = self._buffer[newline + 1 :]
            while b"\n" in self._buffer:
                line, self._buffer = self._buffer.split(b"\n", 1)
                self._consume_line(line)

    def wait_ready(self, timeout_seconds: float) -> bool:
        if timeout_seconds < 0:
            return False
        return self._ready.wait(timeout_seconds)

    def is_ready(self) -> bool:
        return self._ready.is_set()

    @property
    def ready_at(self) -> float | None:
        with self._lock:
            return self._ready_at

    @property
    def error_class(self) -> str | None:
        with self._lock:
            return self._error_class

    def attestation(self, source_sha: str, coordination_run_id: str) -> dict[str, Any]:
        source_sha = require_source_sha(source_sha)
        coordination_run_id = require_run_id(coordination_run_id)
        with self._lock:
            if not self._ready.is_set() or self._error_class is not None:
                fail("rw_not_ready", "pending", 2)
            return {
                "status": "ready",
                "role": "rw_provider",
                "source_sha": source_sha,
                "coordination_run_id": coordination_run_id,
                "expected_file_hash": self.expected_file_hash,
                "expected_file_size_bytes": self.expected_file_size,
                "member_join_observed": self._member_join_ok,
                "file_hash_observed": self._file_hash_ok,
                "keepalive_observed": self._keepalive_enter,
                "live_handle_observed": True,
            }


class RwSupervisor:
    """Own one WinRM member call until its terminal result is observed."""

    def __init__(self, runner: Callable[..., bootstrap.RemoteRun], observer: RemoteReadinessObserver):
        self.runner = runner
        self.observer = observer
        self._finished = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._result: bootstrap.RemoteRun | None = None
        self._error_class: str | None = None

    def start(self) -> None:
        with self._lock:
            if self._thread is not None:
                fail("unexpected")
            self._thread = threading.Thread(target=self._run, name="qsync-f-rw-supervisor", daemon=True)
            self._thread.start()

    def _run(self) -> None:
        try:
            result = self.runner(on_stdout=self.observer.feed)
            if not isinstance(result, bootstrap.RemoteRun):
                self._error_class = "remote_failure"
            else:
                self._result = result
        except bootstrap.HarnessError as error:
            self._error_class = error.error_class
        except Exception:
            self._error_class = "remote_failure"
        finally:
            self._finished.set()

    def wait_ready(self, timeout_seconds: float) -> bool:
        return self.observer.wait_ready(timeout_seconds)

    def attestation(self, source_sha: str, coordination_run_id: str) -> dict[str, Any]:
        """Take readiness only while this supervisor still owns a live call."""

        if not self.is_alive():
            fail("rw_not_ready", "pending", 2)
        attestation = self.observer.attestation(source_sha, coordination_run_id)
        if not self.is_alive():
            fail("rw_not_ready", "pending", 2)
        return attestation

    def is_alive(self) -> bool:
        thread = self._thread
        return thread is not None and thread.is_alive()

    def wait_finished(self, timeout_seconds: float) -> bootstrap.RemoteRun | None:
        if timeout_seconds < 0 or not self._finished.wait(timeout_seconds):
            return None
        if self._result is not None:
            return self._result
        return bootstrap.RemoteRun(
            status_code=-1,
            transport_cleanup_completed=False,
            transport_error_class=self._error_class or "remote_failure",
        )

    @property
    def error_class(self) -> str | None:
        return self._error_class


class SecretLease:
    """Create a generated repository secret only after a no-overwrite check."""

    def __init__(self, actions: ActionsBackend, name: str):
        self.actions = actions
        self.name = require_secret_name(name)
        self.created = False
        self.released = False
        self.creation_attempted = False
        self.ownership_unknown = False

    def create(self, value: str) -> None:
        if self.created:
            fail("secret_set_failed")
        if self.name in self.actions.list_secret_names():
            fail("secret_exists", "blocked", 2)
        self.creation_attempted = True
        try:
            self.actions.set_secret(self.name, value)
        except ControllerError:
            # A lost response cannot establish whether GitHub accepted the
            # write.  Preserve the generated name in controller state and
            # require operator reconciliation; never delete an unowned name.
            self.ownership_unknown = True
            raise
        except Exception:
            self.ownership_unknown = True
            fail("secret_set_failed", "pending", 2)
        self.created = True

    def release(self) -> None:
        if self.ownership_unknown and not self.created:
            fail("secret_delete_failed", "pending", 2)
        if not self.created or self.released:
            return
        self.actions.delete_secret(self.name)
        self.released = True


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    if not path.is_absolute() or path.name != STATE_FILENAME:
        fail("path_invalid")
    encoded = (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    if len(encoded) > 2 * 1024 * 1024:
        fail("state_invalid")
    temporary = path.with_name("." + path.name + ".tmp")
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        path.chmod(0o600)
    except (OSError, UnicodeError, TypeError, ValueError):
        try:
            temporary.unlink()
        except OSError:
            pass
        fail("state_invalid")


def _state_safe(value: Any, key: str = "") -> None:
    lowered = key.lower()
    if any(part in lowered for part in ("password", "credential", "bearer", "secret", "token", "share_key")):
        fail("state_invalid")
    if isinstance(value, Mapping):
        for child_key, child_value in value.items():
            _state_safe(child_value, str(child_key))
    elif isinstance(value, list):
        for child in value:
            _state_safe(child, key)
    elif isinstance(value, str) and ("://" in value or "\x00" in value or "\n" in value or "\r" in value):
        fail("state_invalid")


def write_state(path: Path, state: Mapping[str, Any]) -> None:
    _state_safe(state)
    _write_json_atomic(path, state)


def load_state(path: Path) -> dict[str, Any]:
    if not path.is_absolute() or path.name != STATE_FILENAME or not path.is_file() or path.is_symlink():
        fail("state_invalid")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        fail("state_invalid")
    if not isinstance(value, dict):
        fail("state_invalid")
    _state_safe(value)
    if value.get("controller_version") != CONTROLLER_VERSION or value.get("status") not in {"prepared", "pending", "failed", "pass"}:
        fail("state_invalid")
    if value.get("source_sha") != require_source_sha(value.get("source_sha")):
        fail("state_invalid")
    if not path.parent.joinpath(OWNED_MARKER).is_file():
        fail("state_invalid")
    return value


def _load_state_or(path: Path, fallback: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
    """Refresh state and retain whether the durable snapshot was unreadable."""

    try:
        return load_state(path), False
    except ControllerError:
        # The fallback is useful for writing a bounded pending record, but it
        # is never evidence that the original durable state was valid.
        return dict(fallback), True


def cleanup_owned_run(
    run_root: Path,
    *,
    owner_stopped: bool = True,
    remote_stopped: bool,
    secret_released: bool,
) -> dict[str, bool]:
    """Remove only a marked run after every owned external handle is closed."""

    if not run_root.is_absolute() or run_root.is_symlink() or not run_root.is_dir():
        return {"owned_paths_removed": False, "state_retained": True}
    try:
        marker = run_root / OWNED_MARKER
        if not marker.is_file() or marker.is_symlink() or marker.read_text(encoding="ascii") != "owned":
            return {"owned_paths_removed": False, "state_retained": True}
    except (OSError, UnicodeError):
        return {"owned_paths_removed": False, "state_retained": True}
    if not owner_stopped or not remote_stopped or not secret_released:
        return {"owned_paths_removed": False, "state_retained": True}
    try:
        shutil.rmtree(run_root)
    except OSError:
        return {"owned_paths_removed": False, "state_retained": True}
    return {"owned_paths_removed": not run_root.exists(), "state_retained": run_root.exists()}


def _make_coordination_id() -> str:
    return str(secrets.randbelow(10**19 - 1) + 1)


@dataclass(frozen=True)
class PrepareResult:
    state_path: Path
    run_id: str
    source_sha: str
    expected_file_hash: str
    expected_file_size_bytes: int


def _controller_error_from_harness(error: bootstrap.HarnessError) -> ControllerError:
    """Translate a tool result without carrying remote text across layers."""

    allowed = {
        "config_invalid",
        "path_invalid",
        "workflow_dispatch_failed",
        "workflow_run_missing",
        "workflow_failed",
        "workflow_timeout",
        "artifact_download_failed",
        "artifact_invalid",
        "source_mismatch",
        "binary_missing",
        "binary_hash_mismatch",
        "external_unavailable",
        "remote_failure",
        "process_start_failed",
        "auth_failed",
        "api_http_error",
        "api_response_invalid",
        "rw_not_ready",
        "state_invalid",
        "cleanup_incomplete",
        "fixture_invalid",
        "unexpected",
    }
    error_class = error.error_class if error.error_class in allowed else "unexpected"
    status = "pending" if error.status in {"pending", "blocked"} else "failed"
    return ControllerError(error_class, status, error.exit_code)


class LocalController:
    def __init__(self, actions: ActionsBackend, run_parent: Path, *, repo: str, ref: str, workflow: str = DEFAULT_WORKFLOW):
        self.actions = actions
        self.run_parent = _ensure_private_directory(run_parent)
        self.repo = require_repo(repo)
        self.ref = require_ref(ref)
        self.workflow = require_workflow(workflow)

    def _new_run_root(self, state_path: Path | None = None) -> Path:
        try:
            if state_path is not None:
                if (
                    not state_path.is_absolute()
                    or state_path.name != STATE_FILENAME
                    or state_path.exists()
                    or state_path.is_symlink()
                    or state_path.parent.parent != self.run_parent
                    or state_path.parent.exists()
                    or state_path.parent.is_symlink()
                ):
                    fail("path_invalid")
                state_path.parent.mkdir(mode=0o700, parents=False, exist_ok=False)
                root = state_path.parent
            else:
                root = Path(tempfile.mkdtemp(prefix="qsync-f-controller-", dir=self.run_parent))
            root.chmod(0o700)
            (root / OWNED_MARKER).write_text("owned", encoding="ascii")
            (root / OWNED_MARKER).chmod(0o600)
            return root
        except OSError:
            fail("path_invalid")

    def _existing_run_ids(self) -> frozenset[str]:
        """Snapshot current runs so a later poll cannot select an old run."""

        rows = self.actions.list_runs(self.workflow, self.ref)
        values: set[str] = set()
        for row in rows:
            try:
                values.add(require_run_id(row.get("databaseId")))
            except ControllerError:
                continue
        return frozenset(values)

    def _download_and_verify_prepare_artifacts(self, run_id: str, run_root: Path, source_sha: str) -> dict[str, Any]:
        artifacts_root = run_root / "artifacts"
        artifacts_root.mkdir(mode=0o700)
        native_root = self.actions.download_artifact(run_id, NATIVE_ARTIFACT_PREFIX + run_id, artifacts_root / "native-artifact")
        role_root = self.actions.download_artifact(run_id, PREPARED_ROLE_ARTIFACT_PREFIX + run_id, artifacts_root / "role-artifact")
        native = verify_native_artifact(native_root, source_sha)
        roles = verify_role_artifact(role_root, source_sha)
        if roles["rw_provider"]["sha256"] != native["sha256"] or roles["rw_provider"]["size_bytes"] != native["size_bytes"]:
            fail("source_mismatch")
        for role in ("owner", "ro_consumer"):
            roles[role]["binary_name"] = _safe_relative_name(
                (Path("artifacts") / "role-artifact" / roles[role]["binary_name"]).as_posix()
            )
        roles["rw_provider"]["binary_name"] = _safe_relative_name(
            (Path("artifacts") / "native-artifact" / native["binary_name"]).as_posix()
        )
        return {
            "native": {
                **native,
                "binary_name": _safe_relative_name(
                    (Path("artifacts") / "native-artifact" / native["binary_name"]).as_posix()
                ),
            },
            "roles": roles,
        }

    def prepare(
        self,
        source_sha: str,
        expected_file_hash: str,
        expected_file_size: int,
        *,
        state_path: Path | None = None,
        dispatch_utc: float | None = None,
        coordination_run_id: str | None = None,
    ) -> PrepareResult:
        source_sha = require_source_sha(source_sha)
        expected_file_hash = require_sha256(expected_file_hash)
        expected_file_size = require_positive_size(expected_file_size, exact=FIXTURE_SIZE_BYTES)
        if state_path is not None and (
            not state_path.is_absolute()
            or state_path.name != STATE_FILENAME
            or state_path.exists()
            or state_path.is_symlink()
            or state_path.parent.parent != self.run_parent
            or state_path.parent.exists()
            or state_path.parent.is_symlink()
        ):
            fail("path_invalid")
        coordination_run_id = require_run_id(coordination_run_id or _make_coordination_id())
        prior_run_ids = self._existing_run_ids()
        dispatch_started = time.time() if dispatch_utc is None else float(dispatch_utc)
        if dispatch_started < 0:
            fail("config_invalid")
        self.actions.dispatch(
            self.workflow,
            self.ref,
            {
                "qsync_three_host": "true",
                "qsync_three_host_execute": "false",
                "qsync_three_host_expected_file_hash": expected_file_hash,
                "qsync_three_host_expected_file_size": str(expected_file_size),
                "qsync_three_host_coordination_id": coordination_run_id,
            },
        )
        run = self.actions.wait_run(
            self.workflow,
            self.ref,
            source_sha,
            dispatch_started,
            excluded_run_ids=prior_run_ids,
            coordination_run_id=coordination_run_id,
            timeout_seconds=MAX_PREPARE_WAIT_SECONDS,
        )
        run_id = require_run_id(run.get("databaseId"))
        if run.get("headSha") != source_sha or run.get("status") != "completed" or run.get("conclusion") != "success":
            fail("workflow_failed")
        run_root = self._new_run_root(state_path)
        state_path = state_path or run_root / STATE_FILENAME
        if state_path.parent != run_root:
            fail("path_invalid")
        try:
            artifact = self._download_and_verify_prepare_artifacts(run_id, run_root, source_sha)
            state = {
                "controller_version": CONTROLLER_VERSION,
                "status": "prepared",
                "source_sha": source_sha,
                "ref": self.ref,
                "prepare_run_id": run_id,
                "prebuilt_role_run_id": run_id,
                "native_artifact_run_id": run_id,
                "coordination_run_id": coordination_run_id,
                "expected_file_hash": expected_file_hash,
                "expected_file_size_bytes": expected_file_size,
                "artifact_root": "artifacts",
                "artifacts": artifact,
                "prepared_utc": utc_now(),
            }
            write_state(state_path, state)
        except Exception:
            # The marked root is owned by this invocation.  No process or
            # remote secret exists yet, so it is safe to remove failed staging.
            try:
                shutil.rmtree(run_root)
            except OSError:
                pass
            raise
        return PrepareResult(state_path, run_id, source_sha, expected_file_hash, expected_file_size)

    def execute_hosted_ro(
        self,
        state_path: Path,
        share_key: str,
        *,
        rw_ready: Mapping[str, Any] | Callable[[], Mapping[str, Any]],
        secret_name: str | None = None,
        dispatch_utc: float | None = None,
        timeout_seconds: int = MAX_EXECUTE_WAIT_SECONDS,
        require_live_readiness: bool = False,
        finalize_status: bool = True,
    ) -> dict[str, Any]:
        state = load_state(state_path)
        if state.get("ref") != self.ref:
            fail("source_mismatch")
        if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= MAX_PREPARE_WAIT_SECONDS:
            fail("config_invalid")
        vault = bootstrap.SecretVault()
        share_key = vault.hold(share_key)
        ready = rw_ready() if callable(rw_ready) else rw_ready
        if not isinstance(ready, Mapping):
            vault.clear()
            fail("rw_not_ready", "pending", 2)
        if (
            ready.get("status") != "ready"
            or ready.get("role") != "rw_provider"
            or ready.get("source_sha") != state.get("source_sha")
            or ready.get("coordination_run_id") != state.get("coordination_run_id")
            or ready.get("expected_file_hash") != state.get("expected_file_hash")
            or ready.get("expected_file_size_bytes") != state.get("expected_file_size_bytes")
            or ready.get("keepalive_observed") is not True
            or (require_live_readiness and ready.get("live_handle_observed") is not True)
            or (
                require_live_readiness
                and (
                    ready.get("member_join_observed") is not True
                    or ready.get("file_hash_observed") is not True
                )
            )
        ):
            vault.clear()
            fail("rw_not_ready", "pending", 2)
        secret_name = require_secret_name(secret_name or ("QSYNC_F_RO_" + state["coordination_run_id"]))
        lease = SecretLease(self.actions, secret_name)
        dispatch_started = time.time() if dispatch_utc is None else float(dispatch_utc)
        prior_run_ids: frozenset[str] = frozenset()
        ro_run: dict[str, Any] | None = None
        result: dict[str, Any] = {
            "status": "pending",
            "ephemeral_released": False,
            "ro_evidence_downloaded": False,
        }
        state_after = {
            **state,
            "status": "pending",
            "ephemeral_slot": {"name": secret_name, "status": "creating"},
            "ephemeral_released": False,
        }
        try:
            write_state(state_path, state_after)
            prior_run_ids = self._existing_run_ids()
            lease.create(share_key)
            state_after = {
                **state_after,
                "ephemeral_slot": {"name": secret_name, "status": "owned"},
            }
            write_state(state_path, state_after)
            self.actions.dispatch(
                self.workflow,
                self.ref,
                {
                    "qsync_three_host": "true",
                    "qsync_three_host_execute": "true",
                    "qsync_three_host_prebuilt_run_id": require_run_id(
                        state.get("prebuilt_role_run_id", state["prepare_run_id"])
                    ),
                    "qsync_three_host_native_artifact_run_id": require_run_id(state["native_artifact_run_id"]),
                    "qsync_three_host_expected_file_hash": require_sha256(state["expected_file_hash"]),
                    "qsync_three_host_expected_file_size": str(
                        require_positive_size(state["expected_file_size_bytes"], exact=FIXTURE_SIZE_BYTES)
                    ),
                    "qsync_three_host_coordination_id": require_run_id(state["coordination_run_id"]),
                    "qsync_three_host_ro_secret_name": secret_name,
                },
            )
            hosted = self.actions.wait_hosted_job(
                self.workflow,
                self.ref,
                state["source_sha"],
                dispatch_started,
                excluded_run_ids=prior_run_ids,
                coordination_run_id=state["coordination_run_id"],
                timeout_seconds=timeout_seconds,
            )
            if not isinstance(hosted, Mapping):
                fail("workflow_run_missing", "pending", 2)
            ro_run = hosted.get("run")
            hosted_job = hosted.get("job")
            if not isinstance(ro_run, Mapping) or not isinstance(hosted_job, Mapping):
                fail("workflow_run_missing", "pending", 2)
            if (
                hosted_job.get("name") != HOSTED_RO_JOB_NAME
                or hosted_job.get("status") != "completed"
                or hosted_job.get("conclusion") != "success"
            ):
                fail("workflow_failed")
            ro_run_id = require_run_id(ro_run.get("databaseId"))
            # The selected workflow may still be running unrelated jobs.  The
            # hosted consumer job is the completion boundary for this phase;
            # requiring the parent workflow to be completed would reintroduce
            # the long native/test critical path.
            if ro_run.get("headSha") != state["source_sha"] or ro_run.get("event") != "workflow_dispatch":
                fail("workflow_failed")
            evidence_destination = state_path.parent / "ro-evidence"
            self.actions.download_artifact(ro_run_id, RO_EVIDENCE_PREFIX + ro_run_id, evidence_destination)
            evidence = verify_ro_evidence(evidence_destination, state)
            state_after = {
                **state_after,
                # A live controller run keeps the owner/RW handles open until
                # its final contract check.  Keep the durable state pending
                # while execute records a successful hosted RO result.
                "status": "pass" if finalize_status else "pending",
                "ro_status": "pass",
                "ro_run_id": ro_run_id,
                "ro_evidence_root": "ro-evidence",
                "ro_evidence_downloaded": True,
                "ro_evidence_verified": evidence["status"] == "pass",
                "execute_utc": utc_now(),
            }
            result.update({"status": "pass", "ro_run_id": ro_run_id, "ro_evidence_downloaded": True})
            return result
        except ControllerError as error:
            state_after = {
                **state_after,
                "status": "pending" if error.status == "pending" else "failed",
                "execute_error_class": error.error_class,
                "ephemeral_slot": {
                    "name": secret_name,
                    "status": "unknown" if lease.ownership_unknown else ("owned" if lease.created else "unowned"),
                },
            }
            result["status"] = state_after["status"]
            result["error_class"] = error.error_class
            raise
        finally:
            try:
                lease.release()
                result["secret_released"] = True
            except ControllerError:
                result["secret_released"] = False
                # Secret cleanup failure is intentionally not converted to a
                # successful run.  The state root remains for operator action.
                result["status"] = "pending"
                state_after = {
                    **state_after,
                    "status": "pending",
                    "ephemeral_slot": {
                        "name": secret_name,
                        "status": "unknown" if lease.ownership_unknown else "owned",
                    },
                    "execute_error_class": "secret_delete_failed",
                    "ephemeral_released": False,
                }
            else:
                if lease.created:
                    state_after = {
                        **state_after,
                        "ephemeral_slot": {"name": secret_name, "status": "released"},
                        "ephemeral_released": True,
                    }
            write_state(state_path, state_after)
            vault.clear()

    def run(
        self,
        state_path: Path,
        *,
        owner_api_url_env: str = "QSYNC_F_OWNER_API_URL",
        artifact_public_host_env: str = "QSYNC_F_ARTIFACT_PUBLIC_HOST",
        winrm_host_env: str = "QSYNC_F_WINRM_HOST",
        winrm_username_env: str = "QSYNC_F_WINRM_USERNAME",
        winrm_password_env: str = "QSYNC_F_WINRM_PASSWORD",
        winrm_destination_env: str = "QSYNC_F_WINRM_DESTINATION",
        owner_bind_host: str = "127.0.0.1",
        keepalive_seconds: int = 300,
        ready_timeout_seconds: int = 120,
    ) -> dict[str, Any]:
        """Run the local owner/RW handoff and the hosted RO consumer.

        Readiness comes from the live WinRM supervisor callback and its still
        running handle.  A ready JSON file can be useful for the older
        ``execute`` handoff, but it is never used by this method as proof that
        the RW provider is alive.
        """

        state = load_state(state_path)
        if state.get("ref") != self.ref or state.get("status") != "prepared":
            fail("state_invalid")
        source_sha = require_source_sha(state.get("source_sha"))
        expected_file_hash = require_sha256(state.get("expected_file_hash"), "artifact_invalid")
        expected_file_size = require_positive_size(state.get("expected_file_size_bytes"), exact=FIXTURE_SIZE_BYTES)
        coordination_run_id = require_run_id(state.get("coordination_run_id"))
        owner_api_url_env = bootstrap.validate_env_name(owner_api_url_env)
        artifact_public_host_env = bootstrap.validate_env_name(artifact_public_host_env)
        winrm_host_env = bootstrap.validate_env_name(winrm_host_env)
        winrm_username_env = bootstrap.validate_env_name(winrm_username_env)
        winrm_password_env = bootstrap.validate_env_name(winrm_password_env)
        winrm_destination_env = bootstrap.validate_env_name(winrm_destination_env)
        owner_bind_host = bootstrap.validate_host(owner_bind_host)
        if type(keepalive_seconds) is not int or not 1 <= keepalive_seconds <= 900:
            fail("config_invalid")
        if type(ready_timeout_seconds) is not int or not 1 <= ready_timeout_seconds <= 900:
            fail("config_invalid")

        owner_info = prepared_role_binary(state_path, state, "owner")
        rw_info = prepared_role_binary(state_path, state, "rw_provider")
        run_root = state_path.parent / ("runtime-" + coordination_run_id)
        if run_root.exists() or run_root.is_symlink() or not _within(run_root, state_path.parent):
            fail("path_invalid")
        try:
            run_root.mkdir(mode=0o700, parents=False, exist_ok=False)
            run_root.chmod(0o700)
            marker = run_root / OWNED_MARKER
            marker.write_text("owned", encoding="ascii")
            marker.chmod(0o600)
        except OSError:
            fail("path_invalid")

        vault = bootstrap.SecretVault()
        owner_process: bootstrap.LocalWebProcess | None = None
        supervisor: RwSupervisor | None = None
        observer: RemoteReadinessObserver | None = None
        remote_result: bootstrap.RemoteRun | None = None
        owner_stopped = False
        owner_forced = False
        owner_exit_code: int | None = None
        runtime_removed = False
        ro_result: dict[str, Any] | None = None
        run_error: ControllerError | None = None
        state_refresh_failed = False
        state_after: dict[str, Any] = {
            **state,
            "status": "pending",
            "run_status": "starting",
            "owner_stopped": False,
            "owner_forced": False,
            "owner_exit_code": None,
            "rw_ready": False,
            "rw_completed": False,
            "rw_contract_valid": False,
            "ro_status": "unrun",
            "ephemeral_released": False,
            "runtime_removed": False,
            "cleanup_pending": True,
            "run_error_class": "none",
        }
        try:
            write_state(state_path, state_after)
            owner_spec = bootstrap.RoleSpec(
                name="owner",
                runner="local",
                binary=owner_info["path"],
                binary_sha256=owner_info["sha256"],
                api_url_env=None,
                admin_token_env=None,
                destination_env=None,
                bind_host=owner_bind_host,
                winrm_host_env=None,
                winrm_username_env=None,
                winrm_password_env=None,
            )
            staged_owner = bootstrap.stage_role_binary(owner_spec, owner_info, run_root)
            owner_spec = replace(owner_spec, binary=staged_owner)
            rw_spec = bootstrap.RoleSpec(
                name="rw_provider",
                runner="winrm",
                binary=rw_info["path"],
                binary_sha256=rw_info["sha256"],
                api_url_env=None,
                admin_token_env=None,
                destination_env=winrm_destination_env,
                bind_host="127.0.0.1",
                winrm_host_env=winrm_host_env,
                winrm_username_env=winrm_username_env,
                winrm_password_env=winrm_password_env,
            )
            staged_rw = bootstrap.stage_role_binary(rw_spec, rw_info, run_root)
            rw_spec = replace(rw_spec, binary=staged_rw)

            owner_process = bootstrap.LocalWebProcess(owner_spec, run_root, "owner", vault)
            owner_process.start()
            owner_client = owner_process.client()
            fixture_path = owner_process.root / "fixture-a.bin"
            actual_file_hash = write_fixture(fixture_path, expected_file_size)
            if actual_file_hash != expected_file_hash:
                fail("fixture_invalid")
            share_id = owner_client.create_share(owner_process.root)
            rw_key = vault.hold(owner_client.issue_key(share_id, "read_write"))
            ro_key = vault.hold(owner_client.issue_key(share_id, "read_only"))

            owner_url_template = os.environ.get(owner_api_url_env)
            public_host_raw = os.environ.get(artifact_public_host_env)
            if not owner_url_template or not public_host_raw:
                fail("external_unavailable", "blocked", 2)
            public_host = bootstrap.validate_host(public_host_raw)
            if owner_process.base_url is None:
                fail("process_start_failed")
            owner_port = owner_process.base_url.rsplit(":", 1)[-1]
            if not owner_port.isdecimal() or not 1 <= int(owner_port) <= 65535:
                fail("config_invalid")
            owner_url_raw = owner_url_template.replace("{port}", owner_port)
            if "{" in owner_url_raw or "}" in owner_url_raw:
                fail("config_invalid")
            owner_url = vault.hold(owner_url_raw)
            # Constructing this client validates the opaque endpoint format;
            # it does not perform an extra login or retain the endpoint in
            # state/evidence.
            bootstrap.ApiClient(owner_url, vault)
            destination = bootstrap.fresh_winrm_destination(rw_spec)
            observer = RemoteReadinessObserver(expected_file_hash, expected_file_size, keepalive_seconds)

            def run_rw(*, on_stdout: Callable[[bytes], None]) -> bootstrap.RemoteRun:
                return bootstrap.run_winrm_member(
                    rw_spec,
                    staged_rw,
                    rw_info["sha256"],
                    owner_url,
                    rw_key,
                    destination,
                    expected_file_hash,
                    public_host,
                    rw_info["size_bytes"],
                    vault,
                    keepalive_seconds=keepalive_seconds,
                    expected_file_size=expected_file_size,
                    on_stdout=on_stdout,
                )

            supervisor = RwSupervisor(run_rw, observer)
            supervisor.start()
            if not supervisor.wait_ready(ready_timeout_seconds):
                fail("rw_not_ready", "pending", 2)
            if not supervisor.is_alive():
                fail("rw_not_ready", "pending", 2)
            ready = supervisor.attestation(source_sha, coordination_run_id)
            state_after = {
                **state_after,
                "run_status": "rw_ready",
                "rw_ready": True,
            }
            write_state(state_path, state_after)
            ready_at = observer.ready_at
            if ready_at is None:
                fail("rw_not_ready", "pending", 2)
            remaining = keepalive_seconds - max(0.0, time.monotonic() - ready_at)
            if remaining < 1:
                fail("workflow_timeout", "pending", 2)
            ro_result = self.execute_hosted_ro(
                state_path,
                ro_key,
                rw_ready=ready,
                secret_name="QSYNC_F_RO_" + coordination_run_id,
                dispatch_utc=time.time(),
                timeout_seconds=max(1, min(MAX_EXECUTE_WAIT_SECONDS, int(remaining))),
                require_live_readiness=True,
                finalize_status=False,
            )
            state_after, refresh_failed = _load_state_or(state_path, state_after)
            state_refresh_failed = state_refresh_failed or refresh_failed
            state_after = {
                **state_after,
                "run_status": "ro_complete",
                "ro_status": "pass" if ro_result.get("status") == "pass" else "failed",
                "ephemeral_released": ro_result.get("secret_released") is True,
            }
            write_state(state_path, state_after)
        except ControllerError as error:
            run_error = error
        except bootstrap.HarnessError as error:
            run_error = _controller_error_from_harness(error)
        except Exception:
            run_error = ControllerError("unexpected")

        # A terminal remote result is the only point at which it is safe to
        # stop the local owner.  If the WinRM call is still live at the bounded
        # deadline, leave the state/runtime retained for reconciliation.
        if supervisor is not None:
            ready_at = observer.ready_at if observer is not None else None
            if ready_at is not None:
                remaining = max(0.0, keepalive_seconds - (time.monotonic() - ready_at))
                remote_result = supervisor.wait_finished(remaining)
            else:
                remote_result = supervisor.wait_finished(0)
        # If no remote call owns a live handle, stopping the local owner is
        # safe even when setup failed before the supervisor could start.  A
        # still-live supervisor deliberately keeps the owner/runtime retained
        # for reconciliation instead of guessing that the remote child is
        # gone.
        remote_handle_live = supervisor is not None and supervisor.is_alive()
        if remote_result is None and not remote_handle_live and owner_process is not None:
            try:
                owner_stopped = owner_process.stop()
            except Exception:
                owner_stopped = False
            owner_forced = bool(owner_process.forced_termination)
            owner_exit_code = getattr(owner_process, "exit_code", None)
        if remote_result is not None:
            state_after, refresh_failed = _load_state_or(state_path, state_after)
            state_refresh_failed = state_refresh_failed or refresh_failed
            state_after = {**state_after, "rw_completed": True}
            if owner_process is not None:
                try:
                    owner_stopped = owner_process.stop()
                except Exception:
                    owner_stopped = False
                owner_forced = bool(owner_process.forced_termination)
                owner_exit_code = getattr(owner_process, "exit_code", None)
            rw_contract_valid = bootstrap.remote_contract_is_complete(
                remote_result,
                rw_info["sha256"],
                rw_info["size_bytes"],
                expected_file_hash,
                expected_file_size,
                require_keepalive=True,
            )
        else:
            rw_contract_valid = False
        if owner_process is not None:
            owner_forced = owner_forced or bool(owner_process.forced_termination)
            owner_exit_code = getattr(owner_process, "exit_code", None)

        current_state, refresh_failed = _load_state_or(state_path, state_after)
        state_refresh_failed = state_refresh_failed or refresh_failed
        ro_status = str((ro_result or {}).get("status") or current_state.get("ro_status") or "unrun")
        secret_released = bool(
            (ro_result or {}).get("secret_released") or current_state.get("ephemeral_released")
        )
        owner_drain_proven = bool(owner_process is not None and owner_process.graceful_drain_proven)
        fully_clean = bool(
            remote_result is not None
            and rw_contract_valid
            and owner_stopped
            and not owner_forced
            and owner_drain_proven
            and ro_status == "pass"
            and secret_released
            and not state_refresh_failed
        )
        if fully_clean:
            try:
                marker = run_root / OWNED_MARKER
                if (
                    run_root.is_dir()
                    and not run_root.is_symlink()
                    and marker.is_file()
                    and not marker.is_symlink()
                ):
                    shutil.rmtree(run_root)
                    runtime_removed = not run_root.exists()
            except OSError:
                runtime_removed = False
            fully_clean = runtime_removed
        if run_error is None and state_refresh_failed:
            run_error = ControllerError("state_invalid", "pending", 2)
        if run_error is None and not fully_clean:
            run_error = ControllerError("cleanup_incomplete", "pending", 2)
        final_status = "pass" if run_error is None else ("pending" if run_error.status == "pending" else "failed")
        final_error_class = run_error.error_class if run_error is not None else "none"
        final_state = {
            **current_state,
            "status": final_status,
            "run_status": final_status,
            "owner_stopped": owner_stopped,
            "owner_forced": owner_forced,
            "owner_exit_code": owner_exit_code,
            "owner_drain_proven": owner_drain_proven,
            "rw_ready": bool(observer is not None and observer.is_ready()),
            "rw_completed": remote_result is not None,
            "rw_contract_valid": bool(rw_contract_valid),
            "ro_status": ro_status,
            "ephemeral_released": secret_released,
            "runtime_removed": runtime_removed,
            "cleanup_pending": not fully_clean,
            "state_refresh_failed": state_refresh_failed,
            "run_error_class": final_error_class,
            "run_finished_utc": utc_now(),
        }
        try:
            write_state(state_path, final_state)
        except ControllerError:
            if run_error is None:
                run_error = ControllerError("state_invalid")
                final_status = "failed"
        if remote_result is not None or (owner_stopped and not remote_handle_live):
            vault.clear()
        if run_error is not None:
            raise run_error
        return {
            "status": final_status,
            "source_sha": source_sha,
            "coordination_run_id": coordination_run_id,
            "owner_stopped": owner_stopped,
            "rw_ready": bool(observer is not None and observer.is_ready()),
            "rw_completed": remote_result is not None,
            "rw_contract_valid": bool(rw_contract_valid),
            "ro_status": ro_status,
            "secret_released": secret_released,
            "runtime_removed": runtime_removed,
        }

    def cleanup(self, state_path: Path, *, owner_stopped: bool, remote_stopped: bool, secret_released: bool) -> dict[str, bool]:
        state = load_state(state_path)
        result = cleanup_owned_run(
            state_path.parent,
            owner_stopped=owner_stopped,
            remote_stopped=remote_stopped,
            secret_released=secret_released,
        )
        if result["state_retained"]:
            write_state(state_path, {**state, "status": "pending", "cleanup": result, "cleanup_utc": utc_now()})
        return result


def _cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="qSync F local prepare/execute controller")
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare")
    prepare.add_argument("--repo", required=True)
    prepare.add_argument("--ref", required=True)
    prepare.add_argument("--source-sha", required=True)
    prepare.add_argument("--expected-file-hash", required=True)
    prepare.add_argument("--expected-file-size", required=True, type=int)
    prepare.add_argument("--run-parent", required=True, type=Path)
    prepare.add_argument("--state", type=Path)
    prepare.add_argument("--workflow", default=DEFAULT_WORKFLOW)
    execute = sub.add_parser("execute")
    execute.add_argument("--repo", required=True)
    execute.add_argument("--ref", required=True)
    execute.add_argument("--state", required=True, type=Path)
    execute.add_argument("--share-key-env", required=True)
    execute.add_argument("--rw-ready-file", required=True, type=Path)
    execute.add_argument("--secret-name")
    execute.add_argument("--workflow", default=DEFAULT_WORKFLOW)
    run = sub.add_parser("run")
    run.add_argument("--repo", required=True)
    run.add_argument("--ref", required=True)
    run.add_argument("--state", required=True, type=Path)
    run.add_argument("--owner-api-url-env", default="QSYNC_F_OWNER_API_URL")
    run.add_argument("--artifact-public-host-env", default="QSYNC_F_ARTIFACT_PUBLIC_HOST")
    run.add_argument("--winrm-host-env", default="QSYNC_F_WINRM_HOST")
    run.add_argument("--winrm-username-env", default="QSYNC_F_WINRM_USERNAME")
    run.add_argument("--winrm-password-env", default="QSYNC_F_WINRM_PASSWORD")
    run.add_argument("--winrm-destination-env", default="QSYNC_F_WINRM_DESTINATION")
    run.add_argument("--owner-bind-host", default="127.0.0.1")
    run.add_argument("--keepalive-seconds", default=300, type=int)
    run.add_argument("--ready-timeout-seconds", default=120, type=int)
    run.add_argument("--workflow", default=DEFAULT_WORKFLOW)
    cleanup = sub.add_parser("cleanup")
    cleanup.add_argument("--repo", required=True)
    cleanup.add_argument("--ref", required=True)
    cleanup.add_argument("--state", required=True, type=Path)
    cleanup.add_argument("--owner-stopped", action="store_true")
    cleanup.add_argument("--remote-stopped", action="store_true")
    cleanup.add_argument("--secret-released", action="store_true")
    cleanup.add_argument("--workflow", default=DEFAULT_WORKFLOW)
    fixture = sub.add_parser("fixture")
    fixture.add_argument("--output", required=True, type=Path)
    fixture.add_argument("--size", type=int, default=FIXTURE_SIZE_BYTES)
    return parser


def _ready_file(path: Path) -> dict[str, Any] | None:
    if not path.is_absolute() or not path.is_file() or path.is_symlink():
        return None
    try:
        if path.stat().st_size > 16 * 1024:
            return None
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            return None
        _state_safe(value)
    except (ControllerError, OSError, UnicodeError, json.JSONDecodeError):
        return None
    return value


def main(argv: list[str] | None = None) -> int:
    args = _cli().parse_args(argv)
    try:
        if args.command == "fixture":
            digest = write_fixture(args.output, args.size)
            print(f"QSYNC_F_CONTROLLER|phase=fixture|ok=true|size={FIXTURE_SIZE_BYTES}|hash={digest}")
            return 0
        actions = GhActions(args.repo)
        if args.command == "prepare":
            result = LocalController(actions, args.run_parent, repo=args.repo, ref=args.ref, workflow=args.workflow).prepare(
                args.source_sha,
                args.expected_file_hash,
                args.expected_file_size,
                state_path=args.state,
            )
            print(f"QSYNC_F_CONTROLLER|phase=prepare|ok=true|run_id={result.run_id}|size={result.expected_file_size_bytes}")
            return 0
        controller = LocalController(actions, args.state.parent, repo=args.repo, ref=args.ref, workflow=args.workflow)
        if args.command == "execute":
            env_name = args.share_key_env
            if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", env_name):
                fail("config_invalid")
            share_key = os.environ.get(env_name)
            if not share_key:
                fail("config_invalid")
            result = controller.execute_hosted_ro(
                args.state,
                share_key,
                rw_ready=lambda: _ready_file(args.rw_ready_file),
                secret_name=args.secret_name,
            )
            print(f"QSYNC_F_CONTROLLER|phase=execute|ok={str(result['status'] == 'pass').lower()}|status={result['status']}")
            return 0 if result["status"] == "pass" else 2
        if args.command == "run":
            result = controller.run(
                args.state,
                owner_api_url_env=args.owner_api_url_env,
                artifact_public_host_env=args.artifact_public_host_env,
                winrm_host_env=args.winrm_host_env,
                winrm_username_env=args.winrm_username_env,
                winrm_password_env=args.winrm_password_env,
                winrm_destination_env=args.winrm_destination_env,
                owner_bind_host=args.owner_bind_host,
                keepalive_seconds=args.keepalive_seconds,
                ready_timeout_seconds=args.ready_timeout_seconds,
            )
            print(f"QSYNC_F_CONTROLLER|phase=run|ok={str(result['status'] == 'pass').lower()}|status={result['status']}")
            return 0 if result["status"] == "pass" else 2
        result = controller.cleanup(
            args.state,
            owner_stopped=args.owner_stopped,
            remote_stopped=args.remote_stopped,
            secret_released=args.secret_released,
        )
        print(
            "QSYNC_F_CONTROLLER|phase=cleanup|ok="
            + str(result["owned_paths_removed"]).lower()
            + "|removed="
            + str(result["owned_paths_removed"]).lower()
        )
        return 0 if result["owned_paths_removed"] else 2
    except ControllerError as error:
        print(f"QSYNC_F_CONTROLLER|ok=false|error_class={error.error_class}")
        return error.exit_code
    except Exception:
        print("QSYNC_F_CONTROLLER|ok=false|error_class=unexpected")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
