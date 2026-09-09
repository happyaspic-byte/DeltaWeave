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
import time
from dataclasses import dataclass
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
    ) -> dict[str, Any]:
        state = load_state(state_path)
        if state.get("ref") != self.ref:
            fail("source_mismatch")
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
        ):
            vault.clear()
            fail("rw_not_ready", "pending", 2)
        secret_name = require_secret_name(secret_name or ("QSYNC_F_RO_" + state["coordination_run_id"]))
        lease = SecretLease(self.actions, secret_name)
        dispatch_started = time.time() if dispatch_utc is None else float(dispatch_utc)
        prior_run_ids = self._existing_run_ids()
        ro_run: dict[str, Any] | None = None
        result: dict[str, Any] = {
            "status": "pending",
            "secret_released": False,
            "ro_evidence_downloaded": False,
        }
        state_after = {
            **state,
            "status": "pending",
            "ephemeral_slot": {"name": secret_name, "status": "creating"},
        }
        try:
            write_state(state_path, state_after)
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
            ro_run = self.actions.wait_run(
                self.workflow,
                self.ref,
                state["source_sha"],
                dispatch_started,
                excluded_run_ids=prior_run_ids,
                coordination_run_id=state["coordination_run_id"],
                timeout_seconds=MAX_EXECUTE_WAIT_SECONDS,
            )
            ro_run_id = require_run_id(ro_run.get("databaseId"))
            if ro_run.get("headSha") != state["source_sha"] or ro_run.get("status") != "completed" or ro_run.get("conclusion") != "success":
                fail("workflow_failed")
            evidence_destination = state_path.parent / "ro-evidence"
            self.actions.download_artifact(ro_run_id, RO_EVIDENCE_PREFIX + ro_run_id, evidence_destination)
            evidence = verify_ro_evidence(evidence_destination, state)
            state_after = {
                **state_after,
                "status": "pass",
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
                }
            else:
                if lease.created:
                    state_after = {
                        **state_after,
                        "ephemeral_slot": {"name": secret_name, "status": "released"},
                    }
            write_state(state_path, state_after)
            vault.clear()

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
