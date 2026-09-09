#!/usr/bin/env python3
"""qSync three-host bootstrap and verification driver.

This driver is deliberately independent of product crates.  It can execute the
managed web API against locally started role processes, or consume pre-started
role endpoints supplied by an external runner.  It never records keys,
credentials, URLs, paths, endpoint IDs, or command output.  Missing D/E
capabilities are represented as blocked/unrun phases; legacy transport is
never used as a substitute for share-swarm/1.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import http.cookiejar
import http.server
import json
import os
import re
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, NoReturn


SOURCE_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
ENV_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]{0,127}$")
REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
ROLE_NAMES = ("owner", "rw_provider", "ro_consumer")
ROLE_LABELS = {
    "owner": "local-linux-owner",
    "rw_provider": "approved-windows-rw",
    "ro_consumer": "hosted-ubuntu-ro",
}
RUNNERS = {"local", "prestarted", "winrm", "github_hosted"}
SAFE_CHILD_ENV_KEYS = {
    "PATH",
    "LANG",
    "LC_ALL",
    "TZ",
    "SystemRoot",
    "SYSTEMROOT",
    "WINDIR",
    "ComSpec",
    "COMSPEC",
    "PATHEXT",
    "LD_LIBRARY_PATH",
    "DYLD_LIBRARY_PATH",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
}
PHASE_NAMES = {
    "input_validation",
    "manifest_verification",
    "binary_verification",
    "self_test",
    "run_admission",
    "owner_web_start",
    "owner_login",
    "owner_create",
    "owner_issue_keys",
    "member_web_start",
    "member_login",
    "local_preview",
    "online_validate",
    "member_join",
    "file_hash",
    "member_reopen_membership",
    "n0_lookup",
    "member_discovery",
    "relay_session",
    "relay_payload",
    "share_swarm_capability",
    "cleanup",
}
ERROR_CLASSES = {
    "none",
    "config_invalid",
    "manifest_invalid",
    "source_mismatch",
    "binary_missing",
    "binary_hash_mismatch",
    "self_test_failed",
    "self_test_status_missing",
    "process_start_failed",
    "web_start_timeout",
    "auth_failed",
    "api_http_error",
    "api_response_invalid",
    "path_invalid",
    "hash_mismatch",
    "n0_unavailable",
    "relay_unproven",
    "share_swarm_missing",
    "external_unavailable",
    "precondition_missing",
    "remote_failure",
    "timeout",
    "cleanup_incomplete",
    "unexpected",
}
PHASE_STATUSES = {"pass", "failed", "blocked", "unrun", "pending"}


class HarnessError(Exception):
    """An error with a fixed, safe classification and no display message."""

    def __init__(self, error_class: str, status: str = "failed", exit_code: int = 1):
        if error_class not in ERROR_CLASSES:
            error_class = "unexpected"
        if status not in PHASE_STATUSES:
            status = "failed"
        self.error_class = error_class
        self.status = status
        self.exit_code = int(exit_code)
        super().__init__(error_class)


def fail(error_class: str, status: str = "failed", exit_code: int = 1) -> NoReturn:
    raise HarnessError(error_class, status, exit_code)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def json_object(value: Any, error_class: str = "api_response_invalid") -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(error_class)
    return value


def require_string(value: Any, error_class: str = "config_invalid") -> str:
    if not isinstance(value, str) or not value:
        fail(error_class)
    return value


def require_sha256(value: Any, error_class: str = "config_invalid") -> str:
    value = require_string(value, error_class)
    if not SHA256_RE.fullmatch(value):
        fail(error_class)
    return value


def require_source_sha(value: Any) -> str:
    value = require_string(value)
    if not SOURCE_SHA_RE.fullmatch(value):
        fail("config_invalid")
    return value


def validate_env_name(value: Any) -> str:
    value = require_string(value)
    if not ENV_NAME_RE.fullmatch(value):
        fail("config_invalid")
    return value


def validate_host(value: Any) -> str:
    value = require_string(value)
    if (
        len(value) > 253
        or any(character.isspace() for character in value)
        or "/" in value
        or "\\" in value
        or "://" in value
        or "\x00" in value
    ):
        fail("config_invalid")
    if value == "0.0.0.0" or value == "::" or re.fullmatch(r"[A-Za-z0-9_.:-]+", value):
        return value
    fail("config_invalid")


def validate_abs_path(value: Any, *, must_exist: bool = False, directory: bool = False) -> Path:
    value = require_string(value)
    if "\x00" in value or "\n" in value or "\r" in value:
        fail("config_invalid")
    path = Path(value)
    if not path.is_absolute() or str(path) in {"/", "\\"}:
        fail("path_invalid")
    if must_exist:
        try:
            if not path.is_file() and (not directory or not path.is_dir()):
                fail("binary_missing")
        except OSError:
            fail("binary_missing")
    return path


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError:
        fail("binary_missing")
    return digest.hexdigest()


def prepare_child_profile(profile: Path) -> dict[str, str]:
    """Create a private process profile and return its directory environment."""

    if not profile.is_absolute() or profile in {Path("/"), Path("\\")}:
        fail("path_invalid")
    try:
        if profile.exists():
            if profile.is_symlink() or not profile.is_dir():
                fail("path_invalid")
            profile.chmod(0o700)
        else:
            profile.mkdir(mode=0o700, parents=False, exist_ok=False)
            profile.chmod(0o700)
        directories = {
            "home": profile / "home",
            "tmp": profile / "tmp",
            "config": profile / "config",
            "cache": profile / "cache",
            "state": profile / "state",
            "data": profile / "data",
        }
        for directory in directories.values():
            if directory.exists() and (directory.is_symlink() or not directory.is_dir()):
                fail("path_invalid")
            directory.mkdir(mode=0o700, exist_ok=True)
            directory.chmod(0o700)
    except FileExistsError:
        fail("path_invalid")
    except OSError:
        fail("path_invalid")
    return {
        "HOME": str(directories["home"]),
        "USERPROFILE": str(directories["home"]),
        "XDG_CONFIG_HOME": str(directories["config"]),
        "XDG_CACHE_HOME": str(directories["cache"]),
        "XDG_STATE_HOME": str(directories["state"]),
        "XDG_DATA_HOME": str(directories["data"]),
        "TMPDIR": str(directories["tmp"]),
        "TMP": str(directories["tmp"]),
        "TEMP": str(directories["tmp"]),
    }


def clean_child_environment(profile_env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Pass only runtime settings and a fresh private profile to app children."""

    environment = {key: value for key, value in os.environ.items() if key in SAFE_CHILD_ENV_KEYS}
    if profile_env:
        environment.update(profile_env)
    environment["RUST_BACKTRACE"] = "0"
    environment["NO_COLOR"] = "1"
    return environment


class SecretVault:
    """Keep secret values available for protocol calls without exposing them."""

    def __init__(self) -> None:
        self._values: list[bytearray] = []

    def hold(self, value: str | bytes | None) -> str:
        if value is None:
            fail("config_invalid")
        if isinstance(value, bytes):
            raw = bytes(value)
            text = raw.decode("utf-8", "strict")
        else:
            text = value
            raw = text.encode("utf-8")
        if not text or len(raw) > 256 * 1024:
            fail("config_invalid")
        self._values.append(bytearray(raw))
        return text

    def clear(self) -> None:
        for value in self._values:
            value[:] = b"\x00" * len(value)
        self._values.clear()

    def contains(self, value: str) -> bool:
        return any(value and value in held.decode("utf-8", "ignore") for held in self._values)


def reject_secret_config(value: Any, key: str = "") -> None:
    lowered = key.lower()
    if any(word in lowered for word in ("password", "credential", "bearer", "secret", "token")) and not lowered.endswith("_env"):
        if value not in (None, "", False, 0, []):
            fail("config_invalid")
    if isinstance(value, Mapping):
        for child_key, child_value in value.items():
            reject_secret_config(child_value, str(child_key))
    elif isinstance(value, list):
        for child in value:
            reject_secret_config(child, key)
    elif isinstance(value, str) and "://" in value:
        # Endpoint values belong in a named environment variable, never config.
        fail("config_invalid")


def load_json_file(path: Path, max_bytes: int = 2 * 1024 * 1024) -> dict[str, Any]:
    try:
        if path.stat().st_size > max_bytes:
            fail("config_invalid")
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, UnicodeError, json.JSONDecodeError):
        fail("config_invalid")
    return json_object(value, "config_invalid")


@dataclass(frozen=True)
class RoleSpec:
    name: str
    runner: str
    binary: Path | None
    binary_sha256: str
    api_url_env: str | None
    admin_token_env: str | None
    destination_env: str | None
    bind_host: str
    winrm_host_env: str | None
    winrm_username_env: str | None
    winrm_password_env: str | None


@dataclass(frozen=True)
class HarnessConfig:
    source_sha: str
    artifact_manifest: Path
    roles: dict[str, RoleSpec]
    require_share_swarm: bool
    require_relay: bool
    run_parent: Path
    owner_api_url_env: str | None
    artifact_public_host_env: str | None


def load_config(path: Path) -> HarnessConfig:
    raw = load_json_file(path)
    reject_secret_config(raw)
    source_sha = require_source_sha(raw.get("source_sha"))
    manifest = validate_abs_path(raw.get("artifact_manifest"), must_exist=True)
    run_parent = validate_abs_path(raw.get("run_parent"), must_exist=True, directory=True)
    owner_api_url_env = None
    artifact_public_host_env = None
    if raw.get("owner_api_url_env") not in (None, ""):
        owner_api_url_env = validate_env_name(raw.get("owner_api_url_env"))
    if raw.get("artifact_public_host_env") not in (None, ""):
        artifact_public_host_env = validate_env_name(raw.get("artifact_public_host_env"))
    raw_roles = raw.get("roles")
    if not isinstance(raw_roles, dict) or set(raw_roles) != set(ROLE_NAMES):
        fail("config_invalid")
    roles: dict[str, RoleSpec] = {}
    for name in ROLE_NAMES:
        item = raw_roles.get(name)
        if not isinstance(item, dict):
            fail("config_invalid")
        runner = require_string(item.get("runner"))
        if runner not in RUNNERS:
            fail("config_invalid")
        binary_value = item.get("binary")
        binary = None if binary_value in (None, "") else validate_abs_path(binary_value)
        binary_sha = require_sha256(item.get("binary_sha256"))
        api_url_env = None
        admin_token_env = None
        destination_env = None
        bind_host = validate_host(item.get("bind_host", "127.0.0.1"))
        winrm_host_env = None
        winrm_username_env = None
        winrm_password_env = None
        if runner == "winrm":
            destination_env = validate_env_name(item.get("destination_env"))
            winrm_host_env = validate_env_name(item.get("winrm_host_env"))
            winrm_username_env = validate_env_name(item.get("winrm_username_env"))
            winrm_password_env = validate_env_name(item.get("winrm_password_env"))
            if owner_api_url_env is None or artifact_public_host_env is None:
                fail("config_invalid")
        elif runner == "prestarted":
            api_url_env = validate_env_name(item.get("api_url_env"))
            admin_token_env = validate_env_name(item.get("admin_token_env"))
            destination_env = validate_env_name(item.get("destination_env"))
        if runner in {"local", "winrm"} and binary is None:
            fail("config_invalid")
        roles[name] = RoleSpec(
            name,
            runner,
            binary,
            binary_sha,
            api_url_env,
            admin_token_env,
            destination_env,
            bind_host,
            winrm_host_env,
            winrm_username_env,
            winrm_password_env,
        )
    for key in ("require_share_swarm", "require_relay"):
        if not isinstance(raw.get(key), bool):
            fail("config_invalid")
    return HarnessConfig(
        source_sha,
        manifest,
        roles,
        raw["require_share_swarm"],
        raw["require_relay"],
        run_parent,
        owner_api_url_env,
        artifact_public_host_env,
    )


@dataclass
class Outcome:
    status: str
    error_class: str = "none"
    exit_code: int = 0
    value: Any = None


@dataclass
class PhaseRecorder:
    phases: list[dict[str, Any]] = field(default_factory=list)

    def add(
        self,
        *,
        phase: str,
        role: str,
        command_id: str,
        status: str,
        started: str,
        finished: str,
        elapsed_ms: int,
        exit_code: int = 0,
        timeout: bool = False,
        error_class: str = "none",
    ) -> None:
        if phase not in PHASE_NAMES or role not in (*ROLE_NAMES, "controller"):
            raise ValueError("unrecognized phase")
        if status not in PHASE_STATUSES or error_class not in ERROR_CLASSES:
            raise ValueError("unrecognized phase result")
        self.phases.append(
            {
                "phase": phase,
                "role": role,
                "command_id": command_id,
                "status": status,
                "started_utc": started,
                "finished_utc": finished,
                "elapsed_ms": max(0, int(elapsed_ms)),
                "exit_code": int(exit_code),
                "timeout": bool(timeout),
                "error_class": error_class,
            }
        )

    def run(
        self,
        phase: str,
        role: str,
        command_id: str,
        action: Callable[[], Any],
    ) -> Outcome:
        started_at = time.monotonic()
        started = utc_now()
        try:
            value = action()
            outcome = value if isinstance(value, Outcome) else Outcome("pass", value=value)
        except HarnessError as error:
            outcome = Outcome(error.status, error.error_class, error.exit_code)
        except TimeoutError:
            outcome = Outcome("failed", "timeout", 124)
        except Exception:
            outcome = Outcome("failed", "unexpected", 1)
        finished = utc_now()
        self.add(
            phase=phase,
            role=role,
            command_id=command_id,
            status=outcome.status,
            started=started,
            finished=finished,
            elapsed_ms=round((time.monotonic() - started_at) * 1000),
            exit_code=outcome.exit_code,
            timeout=outcome.error_class == "timeout",
            error_class=outcome.error_class,
        )
        return outcome


class SafeEvidence:
    """Write only the fixed evidence vocabulary defined by the F design."""

    _forbidden_key_parts = ("password", "credential", "bearer", "token", "secret", "url", "endpoint", "path")

    def __init__(self, directory: Path, vault: SecretVault):
        self.directory = directory
        self.vault = vault
        try:
            directory.mkdir(mode=0o700, parents=False, exist_ok=False)
        except FileExistsError:
            fail("config_invalid")
        except OSError:
            fail("config_invalid")

    def _check(self, value: Any, key: str = "") -> None:
        lowered = key.lower()
        if any(part in lowered for part in self._forbidden_key_parts):
            allowed = {
                "source_sha",
                "binary_sha256",
                "file_hash_sha256",
                "artifact_sha256",
                "error_class",
                "owned_paths_removed",
            }
            if key not in allowed:
                raise ValueError("forbidden evidence key")
        if isinstance(value, Mapping):
            for child_key, child_value in value.items():
                self._check(child_value, str(child_key))
        elif isinstance(value, list):
            for child in value:
                self._check(child, key)
        elif isinstance(value, str):
            if self.vault.contains(value) or "://" in value or "\\" in value:
                raise ValueError("secret or endpoint in evidence")

    def write_json(self, name: str, value: Mapping[str, Any]) -> None:
        if not re.fullmatch(r"[a-z0-9_-]+\.json", name):
            raise ValueError("unsafe evidence filename")
        self._check(value)
        temporary = self.directory / ("." + name + ".tmp")
        target = self.directory / name
        encoded = (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
        except Exception:
            try:
                temporary.unlink()
            except OSError:
                pass
            raise

    def write_jsonl(self, name: str, values: Iterable[Mapping[str, Any]]) -> None:
        if not re.fullmatch(r"[a-z0-9_-]+\.jsonl", name):
            raise ValueError("unsafe evidence filename")
        target = self.directory / name
        with target.open("x", encoding="utf-8") as handle:
            for value in values:
                self._check(value)
                handle.write(json.dumps(value, ensure_ascii=False, sort_keys=True) + "\n")
        target.chmod(0o600)


def verify_manifest(config: HarnessConfig) -> dict[str, Any]:
    manifest = load_json_file(config.artifact_manifest)
    if manifest.get("status") != "pass":
        fail("manifest_invalid")
    if manifest.get("source_sha") != config.source_sha or manifest.get("workflow_sha") != config.source_sha:
        fail("source_mismatch")
    raw_artifacts = manifest.get("artifacts")
    if raw_artifacts is None:
        # Native artifact manifests from the existing workflow have one
        # descriptor.  A full three-host run should provide role-specific
        # descriptors; this fallback is retained for validate-only callers.
        raw_artifact = json_object(manifest.get("artifact"), "manifest_invalid")
        raw_artifacts = {"default": raw_artifact}
    if not isinstance(raw_artifacts, dict) or not raw_artifacts:
        fail("manifest_invalid")
    artifacts: dict[str, dict[str, Any]] = {}
    for role, raw_artifact in raw_artifacts.items():
        if not isinstance(role, str) or not isinstance(raw_artifact, dict):
            fail("manifest_invalid")
        expected_hash = require_sha256(raw_artifact.get("sha256"), "manifest_invalid")
        expected_size = raw_artifact.get("size_bytes")
        if not isinstance(expected_size, int) or expected_size <= 0:
            fail("manifest_invalid")
        if raw_artifact.get("source_sha", config.source_sha) != config.source_sha:
            fail("source_mismatch")
        if raw_artifact.get("workflow_sha", config.source_sha) != config.source_sha:
            fail("source_mismatch")
        artifacts[role] = {
            "sha256": expected_hash,
            "size_bytes": expected_size,
            "target": raw_artifact.get("target"),
        }
    return {"source_sha": config.source_sha, "artifacts": artifacts}


def verify_role_binary(spec: RoleSpec, artifact: Mapping[str, Any]) -> str:
    assert spec.binary is not None
    expected_target = "windows" if spec.runner == "winrm" else "linux" if spec.runner == "local" else None
    if expected_target is not None and artifact.get("target") not in (None, expected_target):
        fail("source_mismatch")
    if not spec.binary.is_file():
        fail("binary_missing")
    actual = file_sha256(spec.binary)
    if actual != spec.binary_sha256:
        fail("binary_hash_mismatch")
    if spec.binary_sha256 != artifact["sha256"] or spec.binary.stat().st_size != artifact["size_bytes"]:
        fail("source_mismatch")
    return actual


def run_self_test(spec: RoleSpec, profile_root: Path | None = None) -> Outcome:
    if spec.runner != "local":
        return Outcome("blocked", "precondition_missing", 0)
    assert spec.binary is not None
    temporary_profile: Path | None = None
    if profile_root is None:
        temporary_profile = Path.cwd() / (".qsync-f-self-test-" + uuid.uuid4().hex)
        profile_root = temporary_profile
    try:
        profile_env = prepare_child_profile(profile_root)
        completed = subprocess.run(
            [str(spec.binary), "self-test"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=clean_child_environment(profile_env),
            timeout=60,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return Outcome("failed", "timeout", 124)
    except HarnessError as error:
        return Outcome(error.status, error.error_class, error.exit_code)
    except OSError:
        return Outcome("failed", "self_test_failed", 1)
    finally:
        if temporary_profile is not None:
            try:
                shutil.rmtree(temporary_profile)
            except OSError:
                pass
    if len(completed.stdout) > 2 * 1024 * 1024 or len(completed.stderr) > 2 * 1024 * 1024:
        return Outcome("failed", "self_test_failed", int(completed.returncode or 1))
    try:
        status = json.loads(completed.stdout.decode("utf-8", "strict")).get("status")
    except (UnicodeError, json.JSONDecodeError, AttributeError):
        return Outcome("failed", "self_test_status_missing", int(completed.returncode or 1))
    if completed.returncode != 0 or status != "pass":
        return Outcome("failed", "self_test_failed", int(completed.returncode or 1))
    return Outcome("pass")


def free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class _ArtifactRequestHandler(http.server.BaseHTTPRequestHandler):
    server: "ArtifactServer._Server"

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        owner = self.server.owner
        with owner.lock:
            if self.path != owner.route or owner.served:
                self.send_error(404)
                return
            owner.served = True
        try:
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(owner.artifact.stat().st_size))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            with owner.artifact.open("rb") as source:
                shutil.copyfileobj(source, self.wfile, length=1024 * 1024)
        except (BrokenPipeError, ConnectionResetError, OSError):
            owner.transfer_error = True

    def log_message(self, _format: str, *_args: Any) -> None:
        # BaseHTTPRequestHandler otherwise emits request paths and addresses.
        return


class ArtifactServer:
    """One-use binary-only HTTP server for the encrypted WinRM transfer."""

    class _Server(http.server.ThreadingHTTPServer):
        def __init__(self, address: tuple[str, int], owner: "ArtifactServer") -> None:
            super().__init__(address, _ArtifactRequestHandler)
            self.owner = owner

    def __init__(self, artifact: Path, bind_host: str, public_host: str, vault: SecretVault):
        if not artifact.is_file():
            fail("binary_missing")
        self.artifact = artifact
        self.bind_host = validate_host(bind_host)
        self.public_host = validate_host(public_host)
        self.vault = vault
        self.route = "/qsync-f-" + secrets.token_urlsafe(24)
        self.server: ArtifactServer._Server | None = None
        self.thread: threading.Thread | None = None
        self.served = False
        self.transfer_error = False
        self.lock = threading.Lock()

    def start(self) -> None:
        try:
            self.server = self._Server((self.bind_host, 0), self)
            self.server.daemon_threads = True
            self.thread = threading.Thread(target=self.server.serve_forever, name="qsync-f-artifact", daemon=True)
            self.thread.start()
        except OSError:
            fail("external_unavailable", "blocked")

    def url(self) -> str:
        if self.server is None:
            fail("external_unavailable", "blocked")
        port = int(self.server.server_address[1])
        # The URL is held only for the encrypted request; it is never recorded.
        return self.vault.hold(f"http://{self.public_host}:{port}{self.route}")

    def stop(self) -> None:
        server = self.server
        if server is None:
            return
        try:
            server.shutdown()
            server.server_close()
        finally:
            if self.thread is not None:
                self.thread.join(timeout=5)
            self.server = None


REMOTE_LINE_RE = re.compile(
    r"^FROLE\|phase=([a-z0-9_]+)\|ok=(true|false)"
    r"(?:\|hash=([0-9a-f]{64}))?"
    r"(?:\|size=([0-9]+))?"
    r"(?:\|forced=(true|false))?"
    r"(?:\|error_class=([a-z0-9_]+))?$"
)
REMOTE_PHASES = {
    "binary_verification",
    "self_test",
    "member_web_start",
    "member_login",
    "local_preview",
    "online_validate",
    "member_join",
    "file_hash",
    "member_reopen_membership",
    "cleanup",
}

# pywinrm's Session.run_ps builds a `powershell -encodedcommand ...` command
# and sends it through the Windows command shell.  The command shell has an
# approximately 8 KiB command-line limit.  The direct WinRS path below keeps
# the same UTF-16LE/Base64 PowerShell payload but sets WINRS_SKIP_CMD_SHELL so
# cmd.exe is not involved.  The upper bound is a fail-closed guard for the
# WinRS argument itself; it is never reported with payload contents.
WINRM_CMD_SHELL_LIMIT_BYTES = 8191
WINRM_MAX_ENCODED_COMMAND_BYTES = 512 * 1024


@dataclass
class RemoteRun:
    phases: list[dict[str, Any]] = field(default_factory=list)
    file_hash: str | None = None
    file_size: int | None = None
    binary_size: int | None = None
    forced_termination: bool = False
    status_code: int = 1


@dataclass(frozen=True)
class WinRMResult:
    std_out: bytes
    std_err: bytes
    status_code: int


def parse_remote_output(stdout: bytes | str, expected_hash: str, expected_size: int | None = None) -> RemoteRun:
    """Parse only the fixed FROLE vocabulary; discard all other remote output."""

    if isinstance(stdout, bytes):
        text = stdout.decode("utf-8", "replace")
    else:
        text = stdout
    if len(text.encode("utf-8", "replace")) > 512 * 1024:
        fail("api_response_invalid")
    result = RemoteRun()
    for line in text.splitlines():
        match = REMOTE_LINE_RE.fullmatch(line.strip())
        if not match or match.group(1) not in REMOTE_PHASES:
            continue
        phase, ok, hash_value, size_value, forced, error_class = match.groups()
        if error_class not in ERROR_CLASSES:
            error_class = "unexpected"
        if hash_value and phase == "file_hash":
            result.file_hash = hash_value
            if size_value is not None:
                result.file_size = int(size_value)
        if phase == "binary_verification" and size_value is not None:
            result.binary_size = int(size_value)
        if hash_value and phase == "binary_verification" and hash_value != expected_hash:
            ok = "false"
            error_class = "binary_hash_mismatch"
        if phase == "binary_verification" and expected_size is not None and result.binary_size != expected_size:
            ok = "false"
            error_class = "binary_hash_mismatch"
        if forced == "true":
            result.forced_termination = True
        result.phases.append(
            {
                "phase": phase,
                "ok": ok == "true",
                "hash": hash_value,
                "size": int(size_value) if size_value is not None else None,
                "forced": forced == "true",
                "error_class": error_class or ("none" if ok == "true" else "unexpected"),
            }
        )
    return result


def record_remote_output(recorder: PhaseRecorder, role: str, remote: RemoteRun) -> None:
    for index, item in enumerate(remote.phases):
        phase = item["phase"]
        ok = bool(item["ok"])
        error_class = item["error_class"]
        recorder.add(
            phase=phase,
            role=role,
            command_id=f"remote.{role}.{phase}.{index}",
            status="pass" if ok else "failed",
            started=utc_now(),
            finished=utc_now(),
            elapsed_ms=0,
            exit_code=0 if ok else 1,
            error_class=error_class,
        )


def _winrm_wrapper(script: str, config: Mapping[str, str]) -> str:
    """Create a short compressed PowerShell loader; payload values stay in memory."""

    import gzip

    script_b64 = base64.b64encode(gzip.compress(script.encode("utf-8"), compresslevel=9)).decode("ascii")
    config_b64 = base64.b64encode(
        gzip.compress(json.dumps(config, separators=(",", ":")).encode("utf-8"), compresslevel=9)
    ).decode("ascii")
    return (
        "$s=[IO.Compression.GzipStream]::new([IO.MemoryStream]::new([Convert]::FromBase64String('"
        + script_b64
        + "')),[IO.Compression.CompressionMode]::Decompress);$m=[IO.MemoryStream]::new();$s.CopyTo($m);"
        "$s.Dispose();&([scriptblock]::Create([Text.Encoding]::UTF8.GetString($m.ToArray()))) -ConfigB64 '"
        + config_b64
        + "'"
    )


def _winrm_encoded_command(command: str) -> str:
    """Encode a PowerShell command without exposing its payload."""

    if not isinstance(command, str) or not command:
        fail("config_invalid")
    encoded = base64.b64encode(command.encode("utf-16-le")).decode("ascii")
    if len(encoded.encode("ascii")) > WINRM_MAX_ENCODED_COMMAND_BYTES:
        fail("api_response_invalid")
    return encoded


def _winrm_command_lengths(command: str) -> dict[str, int]:
    """Return numeric transport lengths for a non-secret diagnostic/test."""

    encoded = _winrm_encoded_command(command)
    legacy_command = "powershell -encodedcommand " + encoded
    direct_command = (
        "powershell.exe -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -EncodedCommand "
        + encoded
    )
    return {
        "wrapper_bytes": len(command.encode("utf-8")),
        "encoded_command_bytes": len(encoded.encode("ascii")),
        "legacy_run_ps_command_bytes": len(legacy_command.encode("ascii")),
        "direct_skip_cmd_shell_command_bytes": len(direct_command.encode("ascii")),
    }


def _run_winrm_powershell(session: Any, command: str) -> WinRMResult:
    """Run PowerShell through WinRS directly, bypassing cmd.exe."""

    protocol = getattr(session, "protocol", None)
    if protocol is None:
        fail("external_unavailable", "blocked")
    encoded = _winrm_encoded_command(command)
    shell_id: Any = None
    command_id: Any = None
    cleanup_failed = False
    try:
        shell_id = protocol.open_shell()
        command_id = protocol.run_command(
            shell_id,
            "powershell.exe",
            (
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-EncodedCommand",
                encoded,
            ),
            console_mode_stdin=False,
            skip_cmd_shell=True,
        )
        std_out, std_err, status_code = protocol.get_command_output(shell_id, command_id)
    except HarnessError:
        raise
    except Exception:
        fail("external_unavailable", "blocked")
    finally:
        if shell_id is not None and command_id is not None:
            try:
                protocol.cleanup_command(shell_id, command_id)
            except Exception:
                cleanup_failed = True
        if shell_id is not None:
            try:
                protocol.close_shell(shell_id)
            except Exception:
                cleanup_failed = True
    if cleanup_failed:
        fail("external_unavailable", "blocked")
    return WinRMResult(bytes(std_out), bytes(std_err), int(status_code))


def run_winrm_member(
    spec: RoleSpec,
    artifact: Path,
    artifact_hash: str,
    owner_url: str,
    share_key: str,
    destination: str,
    expected_file_hash: str,
    public_host: str,
    artifact_size: int,
    vault: SecretVault,
) -> RemoteRun:
    """Run the approved Windows role over encrypted WinRM without a fake local pass."""

    if not isinstance(artifact_size, int) or artifact_size <= 0:
        fail("manifest_invalid")
    if not all((spec.winrm_host_env, spec.winrm_username_env, spec.winrm_password_env)):
        fail("config_invalid")
    host_raw = os.environ.get(spec.winrm_host_env or "")
    username_raw = os.environ.get(spec.winrm_username_env or "")
    password_raw = os.environ.get(spec.winrm_password_env or "")
    if not host_raw or not username_raw or not password_raw:
        fail("external_unavailable", "blocked")
    host = validate_host(host_raw)
    username = vault.hold(username_raw)
    password = vault.hold(password_raw)
    if not re.fullmatch(r"[A-Za-z]:\\[^\x00\r\n]+", destination):
        fail("path_invalid")
    script_path = Path(__file__).with_name("qsync_three_host_winrm_member.ps1")
    try:
        script = script_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        fail("precondition_missing", "blocked")
    server = ArtifactServer(artifact, "0.0.0.0", public_host, vault)
    try:
        server.start()
        remote_config = {
            "artifact_url": server.url(),
            "artifact_sha256": artifact_hash,
            "artifact_size": str(artifact_size),
            "owner_base_uri": owner_url,
            "share_key": share_key,
            "destination_root": destination,
            "expected_file_hash": expected_file_hash,
            "expected_file_name": "fixture-a.bin",
        }
        wrapper = _winrm_wrapper(script, remote_config)
        try:
            import winrm  # type: ignore[import-not-found]

            session = winrm.Session(
                f"http://{host}:5985/wsman",
                auth=(username, password),
                transport="ntlm",
                message_encryption="always",
                operation_timeout_sec=45,
                read_timeout_sec=60,
            )
            result = _run_winrm_powershell(session, wrapper)
        except ImportError:
            fail("external_unavailable", "blocked")
        except Exception:
            fail("external_unavailable", "blocked")
        remote = parse_remote_output(result.std_out, artifact_hash, artifact_size)
        remote.status_code = int(result.status_code)
        if remote.status_code != 0 and not remote.phases:
            fail("external_unavailable", "blocked")
        if not server.served:
            fail("external_unavailable", "blocked")
        if server.transfer_error:
            fail("external_unavailable", "blocked")
        if remote.file_hash and remote.file_hash != "" and remote.file_hash != "0" * 64:
            return remote
        return remote
    finally:
        server.stop()


class ApiClient:
    def __init__(self, base_url: str, vault: SecretVault):
        if not re.fullmatch(r"https?://[^\s/]+(?::[0-9]{1,5})?", base_url):
            fail("config_invalid")
        self.base_url = base_url.rstrip("/")
        self.vault = vault
        self.cookies = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cookies))
        self.csrf: str | None = None

    def _call(self, method: str, path: str, body: Mapping[str, Any] | None = None, mutation: bool = False) -> tuple[int, Any]:
        if not path.startswith("/") or "?" in path or "#" in path:
            fail("api_response_invalid")
        encoded = None
        headers = {"Accept": "application/json"}
        if body is not None:
            encoded = json.dumps(body, separators=(",", ":")).encode("utf-8")
            if len(encoded) > 64 * 1024:
                fail("api_response_invalid")
            headers["Content-Type"] = "application/json"
        if mutation:
            headers["Origin"] = self.base_url
            if self.csrf:
                headers["x-deltaweave-csrf"] = self.csrf
        request = urllib.request.Request(self.base_url + path, data=encoded, headers=headers, method=method)
        try:
            with self.opener.open(request, timeout=30) as response:
                raw = response.read(1024 * 1024 + 1)
                status = int(response.status)
        except urllib.error.HTTPError as error:
            try:
                error.read(1024 * 1024)
            except OSError:
                pass
            return int(error.code), None
        except (urllib.error.URLError, TimeoutError, OSError):
            fail("external_unavailable", "blocked")
        if len(raw) > 1024 * 1024:
            fail("api_response_invalid")
        if not raw:
            return status, None
        try:
            return status, json.loads(raw.decode("utf-8", "strict"))
        except (UnicodeError, json.JSONDecodeError):
            fail("api_response_invalid")

    def login(self, admin_token: str) -> None:
        token = self.vault.hold(admin_token)
        status, value = self._call("POST", "/api/v1/session", {"token": token}, mutation=True)
        if status != 200:
            fail("auth_failed")
        body = json_object(value)
        csrf = body.get("csrf_token")
        if not isinstance(csrf, str) or not csrf or len(csrf) > 256:
            fail("auth_failed")
        self.csrf = self.vault.hold(csrf)

    def create_share(self, root: Path | str) -> str:
        request_id = "qsync-f-create-" + uuid.uuid4().hex
        if not REQUEST_ID_RE.fullmatch(request_id):
            fail("config_invalid")
        status, value = self._call(
            "POST",
            "/api/v1/shares",
            {"request_id": request_id, "name": "qSync F smoke", "root": str(root), "min_free_space_mib": 0},
            mutation=True,
        )
        if status not in (200, 201):
            fail("api_http_error")
        body = json_object(value)
        share_id = body.get("share_id")
        if not isinstance(share_id, str) or not SHA256_RE.fullmatch(share_id):
            fail("api_response_invalid")
        return share_id

    def issue_key(self, share_id: str, permission: str) -> str:
        request_id = "qsync-f-issue-" + permission + "-" + uuid.uuid4().hex
        status, value = self._call(
            "POST",
            f"/api/v1/shares/{share_id}/keys",
            {"request_id": request_id, "permission": permission, "expires_at": None},
            mutation=True,
        )
        if status != 200:
            fail("api_http_error")
        body = json_object(value)
        key = body.get("key")
        if not isinstance(key, str) or not key:
            fail("api_response_invalid")
        return self.vault.hold(key)

    def preview(self, key: str) -> None:
        status, value = self._call(
            "POST",
            "/api/v1/shares/preview",
            {"request_id": "qsync-f-preview-" + uuid.uuid4().hex, "key": key},
            mutation=True,
        )
        if status != 200 or not isinstance(value, dict) or value.get("signature_valid") is not True:
            fail("api_http_error" if status != 200 else "api_response_invalid")

    def validate(self, key: str) -> None:
        status, value = self._call(
            "POST",
            "/api/v1/shares/validate",
            {"request_id": "qsync-f-validate-" + uuid.uuid4().hex, "key": key},
            mutation=True,
        )
        if status != 200 or not isinstance(value, dict) or value.get("signature_valid") is not True:
            fail("external_unavailable", "blocked" if status == 503 else "failed")

    def join(self, key: str, destination: Path | str) -> str:
        status, value = self._call(
            "POST",
            "/api/v1/shares/join",
            {
                "request_id": "qsync-f-join-" + uuid.uuid4().hex,
                "key": key,
                "destination_root": str(destination),
            },
            mutation=True,
        )
        if status not in (200, 202):
            fail("api_http_error")
        body = json_object(value)
        share_id = body.get("share_id")
        if not isinstance(share_id, str) or not SHA256_RE.fullmatch(share_id):
            fail("api_response_invalid")
        enrollment = body.get("enrollment")
        if enrollment not in ("enrolled", "waiting"):
            fail("api_response_invalid")
        return share_id


class LocalWebProcess:
    def __init__(self, spec: RoleSpec, run_root: Path, role: str, vault: SecretVault):
        self.spec = spec
        self.run_root = run_root
        self.role = role
        self.vault = vault
        self.process: subprocess.Popen[bytes] | None = None
        self.base_url: str | None = None
        self.data_dir = run_root / (role + "-data")
        self.root = run_root / (role + "-files")
        self.profile = run_root / (role + "-profile")
        self.profile_env: dict[str, str] = {}
        self.forced_termination = False
        # A process exit is not a protocol drain acknowledgement.  Keep the
        # distinction explicit so cleanup cannot erase a state directory when
        # the harness only observed the OS process stopping.
        self.started_once = False
        self.graceful_drain_proven = False

    def prepare(self) -> None:
        if self.spec.binary is None:
            fail("binary_missing")
        for directory in (self.data_dir, self.root):
            try:
                directory.mkdir(mode=0o700, exist_ok=True)
                directory.chmod(0o700)
            except OSError:
                fail("path_invalid")
        self.profile_env = prepare_child_profile(self.profile)

    def child_environment(self) -> dict[str, str]:
        if not self.profile_env:
            fail("path_invalid")
        return clean_child_environment(self.profile_env)

    def start(self) -> None:
        if self.spec.binary is None:
            fail("binary_missing")
        self.prepare()
        port = free_tcp_port()
        try:
            self.process = subprocess.Popen(
                [
                    str(self.spec.binary),
                    "web",
                    "--bind",
                    f"{self.spec.bind_host}:{port}",
                    "--data-dir",
                    str(self.data_dir),
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=self.child_environment(),
                close_fds=True,
            )
            self.started_once = True
        except OSError:
            fail("process_start_failed")
        self.base_url = f"http://127.0.0.1:{port}"
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                fail("process_start_failed")
            if (self.data_dir / "admin-token").is_file():
                try:
                    request = urllib.request.Request(self.base_url + "/", headers={"Accept": "text/html"})
                    with urllib.request.urlopen(request, timeout=2):
                        return
                except (urllib.error.URLError, OSError):
                    pass
            time.sleep(0.2)
        fail("web_start_timeout", exit_code=124)

    def client(self) -> ApiClient:
        if self.base_url is None:
            fail("process_start_failed")
        try:
            token = (self.data_dir / "admin-token").read_text(encoding="utf-8").strip()
        except (OSError, UnicodeError):
            fail("auth_failed")
        if not token:
            fail("auth_failed")
        client = ApiClient(self.base_url, self.vault)
        client.login(token)
        return client

    def stop(self) -> bool:
        process = self.process
        if process is None:
            return True
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.forced_termination = True
                process.kill()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    return False
        self.process = None
        return True


def get_external_client(spec: RoleSpec, vault: SecretVault) -> ApiClient:
    if not spec.api_url_env or not spec.admin_token_env:
        fail("config_invalid")
    url = os.environ.get(spec.api_url_env)
    token = os.environ.get(spec.admin_token_env)
    if not url or not token:
        fail("external_unavailable", "blocked")
    client = ApiClient(vault.hold(url), vault)
    client.login(token)
    return client


def make_fixture(owner: LocalWebProcess) -> tuple[Path, dict[str, str]]:
    owner.prepare()
    fixture_a = owner.root / "fixture-a.bin"
    fixture_b = owner.root / "fixture-b.bin"
    fixture_a.write_bytes(bytes((index * 17) % 251 for index in range(256 * 1024)))
    fixture_b.write_bytes(bytes((index * 29 + 7) % 251 for index in range(192 * 1024)))
    return owner.root, {
        "fixture_a": file_sha256(fixture_a),
        "fixture_b": file_sha256(fixture_b),
    }


def role_destination(process: LocalWebProcess | None, spec: RoleSpec, vault: SecretVault) -> Path | str:
    if process is not None:
        return process.root
    if not spec.destination_env:
        fail("config_invalid")
    raw = os.environ.get(spec.destination_env)
    if not raw:
        fail("external_unavailable", "blocked")
    if "\x00" in raw or "\n" in raw or "\r" in raw or "://" in raw or len(raw) > 4096:
        fail("path_invalid")
    # Keep a remote Windows path opaque.  Converting it through Linux Path would
    # silently change drive separators and could send a different destination.
    if spec.runner == "winrm" and not re.fullmatch(r"[A-Za-z]:\\[^\x00\r\n]+", raw):
        fail("path_invalid")
    return raw


def fresh_winrm_destination(spec: RoleSpec) -> str:
    """Create a new opaque Windows destination for this run; never reuse a path."""

    if not spec.destination_env:
        fail("config_invalid")
    configured = os.environ.get(spec.destination_env)
    if not configured or not re.fullmatch(r"[A-Za-z]:\\[^\x00\r\n]*", configured):
        fail("path_invalid")
    drive = configured[:2]
    return drive + r"\DeltaWeave-QSync-F-" + secrets.token_hex(16) + r"\member-files"


def safe_cleanup(processes: list[LocalWebProcess], run_root: Path) -> tuple[bool, bool]:
    all_stopped = True
    graceful_drain_proven = True
    for process in reversed(processes):
        try:
            all_stopped = process.stop() and all_stopped
            graceful_drain_proven = (
                graceful_drain_proven
                and (not process.started_once or process.graceful_drain_proven)
            )
        except Exception:
            all_stopped = False
            graceful_drain_proven = False
    # The harness has no managed pause/revoke drain acknowledgement yet.  A
    # normal OS exit therefore leaves the owned state for later inspection;
    # forced/unknown termination must never be hidden by rmtree.
    if not all_stopped or not graceful_drain_proven or any(
        process.forced_termination for process in processes
    ):
        return all_stopped, False
    try:
        if not run_root.is_dir() or len(run_root.parts) < 3:
            return True, False
        shutil.rmtree(run_root)
    except OSError:
        return True, False
    return True, not run_root.exists()


def run_harness(config: HarnessConfig, evidence_dir: Path, execute_external: bool) -> int:
    vault = SecretVault()
    recorder = PhaseRecorder()
    processes: list[LocalWebProcess] = []
    run_root: Path | None = None
    evidence: SafeEvidence | None = None
    binary_hashes: dict[str, str] = {}
    providers = [
        {"role": "owner-provider", "epoch": 0, "verified_chunks": 0, "verified_bytes": 0},
        {"role": "rw-provider", "epoch": None, "verified_chunks": 0, "verified_bytes": 0},
    ]
    file_hash_verified: dict[str, dict[str, Any]] = {}
    remote_binary: dict[str, dict[str, Any]] = {}
    remote_forced_termination: dict[str, bool] = {}
    status = "failed"
    cleanup_state = {
        "owned_processes_stopped": False,
        "owned_paths_removed": False,
        "preexisting_protected_state_unchanged": "unverified",
        "forced_termination_used": False,
        "graceful_drain_proven": "unverified",
        "remote_forced_termination_used": remote_forced_termination,
    }
    try:
        evidence = SafeEvidence(evidence_dir, vault)
        manifest_info = recorder.run(
            "manifest_verification",
            "controller",
            "provenance.manifest",
            lambda: verify_manifest(config),
        )
        if manifest_info.status != "pass":
            status = manifest_info.status
            return 1
        artifact = manifest_info.value
        run_root = Path(tempfile.mkdtemp(prefix="qsync-f-", dir=config.run_parent))
        run_root.chmod(0o700)
        for role in ROLE_NAMES:
            spec = config.roles[role]
            if spec.runner == "github_hosted":
                # The hosted role verifies its own Linux artifact in its
                # reusable workflow and is not represented by this controller.
                continue
            role_artifact = artifact["artifacts"].get(role) or artifact["artifacts"].get("default")
            if role_artifact is None:
                recorder.add(
                    phase="binary_verification",
                    role=role,
                    command_id=f"provenance.binary.{role}",
                    status="blocked",
                    started=utc_now(),
                    finished=utc_now(),
                    elapsed_ms=0,
                    error_class="manifest_invalid",
                )
                if spec.runner == "local":
                    status = "failed"
                    return 1
                continue
            outcome = recorder.run(
                "binary_verification",
                role,
                f"provenance.binary.{role}",
                lambda spec=spec, role_artifact=role_artifact: verify_role_binary(spec, role_artifact),
            )
            if outcome.status == "pass":
                binary_hashes[role] = outcome.value
            elif spec.runner == "local":
                status = "failed"
                return 1
            if spec.runner == "winrm":
                # The remote worker emits its own native self-test result.
                continue
            self_test = recorder.run(
                "self_test",
                role,
                f"host.self_test.{role}",
                lambda spec=spec: run_self_test(spec, run_root / (spec.name + "-self-test-profile")),
            )
            if self_test.status == "failed":
                status = "failed"
                return 1
        if not execute_external and any(config.roles[role].runner != "local" for role in ROLE_NAMES):
            # The default is safe dry-run/preflight.  No external endpoint or
            # secret is consumed until the caller opts in explicitly.
            recorder.add(
                phase="run_admission",
                role="controller",
                command_id="topology.external.opt_in",
                status="blocked",
                started=utc_now(),
                finished=utc_now(),
                elapsed_ms=0,
                error_class="precondition_missing",
            )
            status = "blocked"
            return 2
        owner_spec = config.roles["owner"]
        if owner_spec.runner == "local":
            owner_process = LocalWebProcess(owner_spec, run_root, "owner", vault)
            processes.append(owner_process)
            recorder.run("owner_web_start", "owner", "web.start.owner", owner_process.start)
            owner_client_outcome = recorder.run("owner_login", "owner", "web.login.owner", owner_process.client)
            if owner_client_outcome.status != "pass":
                status = "failed"
                return 1
            owner_client: ApiClient = owner_client_outcome.value
            owner_root, fixture_hashes = make_fixture(owner_process)
        else:
            if not execute_external:
                fail("precondition_missing", "blocked")
            owner_client = get_external_client(owner_spec, vault)
            owner_root = role_destination(None, owner_spec, vault)
            fixture_hashes = {}
        create_outcome = recorder.run(
            "owner_create",
            "owner",
            "managed.create_share",
            lambda: owner_client.create_share(owner_root),
        )
        if create_outcome.status != "pass":
            status = create_outcome.status
            return 1
        share_id = create_outcome.value
        issued: dict[str, str] = {}
        issue_outcome = recorder.run(
            "owner_issue_keys",
            "owner",
            "managed.issue_keys",
            lambda: {
                "read_only": owner_client.issue_key(share_id, "read_only"),
                "read_write": owner_client.issue_key(share_id, "read_write"),
            },
        )
        if issue_outcome.status != "pass":
            status = issue_outcome.status
            return 1
        issued = issue_outcome.value
        for role, permission in (("rw_provider", "read_write"), ("ro_consumer", "read_only")):
            spec = config.roles[role]
            if spec.runner == "github_hosted":
                # Hosted RO is a separate reusable-workflow role driver.  The
                # controller must not substitute a prestarted client or claim
                # that a local process represents that host.
                recorder.add(
                    phase="member_web_start",
                    role=role,
                    command_id="hosted-ro.external-role-driver",
                    status="blocked",
                    started=utc_now(),
                    finished=utc_now(),
                    elapsed_ms=0,
                    error_class="precondition_missing",
                )
                status = "blocked"
                return 2
            if spec.runner == "winrm":
                destination = fresh_winrm_destination(spec)
                if not isinstance(destination, str) or config.owner_api_url_env is None or config.artifact_public_host_env is None:
                    fail("config_invalid")
                owner_url_raw = os.environ.get(config.owner_api_url_env)
                public_host_raw = os.environ.get(config.artifact_public_host_env)
                if not owner_url_raw or not public_host_raw:
                    fail("external_unavailable", "blocked")
                owner_url = vault.hold(owner_url_raw)
                public_host = validate_host(public_host_raw)
                expected = fixture_hashes.get("fixture_a")
                if not expected:
                    fail("precondition_missing", "blocked")
                if spec.binary is None:
                    fail("binary_missing")
                rw_artifact = artifact["artifacts"].get(role) or artifact["artifacts"].get("default")
                if rw_artifact is None:
                    fail("manifest_invalid")
                remote_outcome = recorder.run(
                    "run_admission",
                    role,
                    f"winrm.run.{role}",
                    lambda: run_winrm_member(
                        spec,
                        spec.binary,
                        rw_artifact["sha256"],
                        owner_url,
                        issued[permission],
                        destination,
                        expected,
                        public_host,
                        rw_artifact["size_bytes"],
                        vault,
                    ),
                )
                if remote_outcome.status != "pass":
                    status = remote_outcome.status
                    return 2 if status == "blocked" else 1
                remote: RemoteRun = remote_outcome.value
                record_remote_output(recorder, role, remote)
                remote_forced_termination[role] = remote.forced_termination
                binary_observed = any(
                    item["phase"] == "binary_verification" and item["ok"] and item["hash"] == rw_artifact["sha256"]
                    for item in remote.phases
                )
                if binary_observed:
                    binary_hashes[role] = rw_artifact["sha256"]
                    remote_binary[role] = {
                        "sha256": rw_artifact["sha256"],
                        "size_bytes": remote.binary_size,
                        "observed": True,
                    }
                if remote.file_hash == expected:
                    file_hash_verified[role] = {
                        "sha256": remote.file_hash,
                        "size_bytes": remote.file_size,
                        "observed": True,
                    }
                if remote.forced_termination:
                    # A forced/unknown remote stop leaves the remote namespace
                    # for inspection; it cannot be reported as a completed
                    # role even when the file hash was correct.
                    status = "pending"
                    return 2
                if remote.status_code != 0 or not remote.phases or any(not item["ok"] for item in remote.phases):
                    cleanup_pending = any(
                        item["phase"] == "cleanup" and item["error_class"] == "cleanup_incomplete"
                        for item in remote.phases
                    )
                    status = "pending" if cleanup_pending else "failed"
                    return 2 if cleanup_pending else 1
                if remote.file_hash != expected or remote.file_size is None or remote.file_size <= 0:
                    status = "failed"
                    return 1
                continue
            process: LocalWebProcess | None = None
            if spec.runner == "local":
                process = LocalWebProcess(spec, run_root, role, vault)
                processes.append(process)
                start_outcome = recorder.run("member_web_start", role, f"web.start.{role}", process.start)
                if start_outcome.status != "pass":
                    status = "failed"
                    return 1
                client_outcome = recorder.run("member_login", role, f"web.login.{role}", process.client)
            else:
                client_outcome = recorder.run(
                    "member_login", role, f"web.login.{role}", lambda spec=spec: get_external_client(spec, vault)
                )
            if client_outcome.status != "pass":
                status = client_outcome.status
                return 1
            member_client: ApiClient = client_outcome.value
            destination = role_destination(process, spec, vault)
            key = issued[permission]
            preview = recorder.run("local_preview", role, f"managed.preview.{role}", lambda key=key: member_client.preview(key))
            if preview.status != "pass":
                status = preview.status
                return 1
            validation = recorder.run(
                "online_validate", role, f"managed.validate.{role}", lambda key=key: member_client.validate(key)
            )
            if validation.status != "pass":
                status = validation.status
                return 1
            join = recorder.run(
                "member_join", role, f"managed.join.{role}", lambda key=key, destination=destination: member_client.join(key, destination)
            )
            if join.status != "pass":
                status = join.status
                return 1
            if process is None or not fixture_hashes:
                recorder.add(
                    phase="file_hash",
                    role=role,
                    command_id=f"managed.file_hash.{role}",
                    status="blocked",
                    started=utc_now(),
                    finished=utc_now(),
                    elapsed_ms=0,
                    error_class="precondition_missing",
                )
                status = "blocked"
                return 2
            expected = fixture_hashes["fixture_a"]
            destination_file = destination / "fixture-a.bin"

            def check_file() -> str:
                deadline = time.monotonic() + 90
                while time.monotonic() < deadline:
                    if destination_file.is_file():
                        actual = file_sha256(destination_file)
                        if actual == expected:
                            return actual
                    time.sleep(0.5)
                fail("hash_mismatch", exit_code=1)

            hash_outcome = recorder.run("file_hash", role, f"managed.file_hash.{role}", check_file)
            if hash_outcome.status != "pass":
                status = hash_outcome.status
                return 1
            file_hash_verified[role] = {
                "sha256": hash_outcome.value,
                "size_bytes": destination_file.stat().st_size,
                "observed": True,
            }
        # These capabilities must be observed by a D/E-specific adapter.  The
        # current binary does not expose such an adapter, so no legacy fallback
        # is attempted and the subset cannot be called full F.
        recorder.add(
            phase="share_swarm_capability",
            role="controller",
            command_id="share-swarm-1.capability",
            status="blocked" if config.require_share_swarm else "unrun",
            started=utc_now(),
            finished=utc_now(),
            elapsed_ms=0,
            error_class="share_swarm_missing" if config.require_share_swarm else "none",
        )
        recorder.add(
            phase="relay_session",
            role="controller",
            command_id="network.relay.session",
            status="blocked" if config.require_relay else "unrun",
            started=utc_now(),
            finished=utc_now(),
            elapsed_ms=0,
            error_class="relay_unproven" if config.require_relay else "none",
        )
        status = "blocked" if config.require_share_swarm or config.require_relay else "unrun"
        return 2 if status in {"blocked", "unrun"} else 0
    finally:
        if run_root is not None:
            stopped, removed = safe_cleanup(processes, run_root)
            cleanup_state["owned_processes_stopped"] = stopped
            cleanup_state["owned_paths_removed"] = removed
            cleanup_state["forced_termination_used"] = any(process.forced_termination for process in processes)
            started_processes = [process for process in processes if process.started_once]
            cleanup_state["graceful_drain_proven"] = (
                "not_needed"
                if not started_processes
                else (
                    "verified"
                    if all(process.graceful_drain_proven for process in started_processes)
                    else "unverified"
                )
            )
        if evidence is not None:
            if run_root is not None and not cleanup_state["owned_paths_removed"]:
                cleanup_status = "pending"
                cleanup_error = "cleanup_incomplete"
            else:
                cleanup_status = "pass"
                cleanup_error = "none"
            cleanup_start = utc_now()
            recorder.add(
                phase="cleanup",
                role="controller",
                command_id="cleanup.run_owned_only",
                status=cleanup_status,
                started=cleanup_start,
                finished=utc_now(),
                elapsed_ms=0,
                error_class=cleanup_error,
            )
            if any(item["status"] == "failed" for item in recorder.phases):
                final_status = "failed"
            elif any(item["status"] == "pending" for item in recorder.phases):
                final_status = "pending"
            elif any(item["status"] == "blocked" for item in recorder.phases):
                final_status = "blocked"
            elif any(item["status"] == "unrun" for item in recorder.phases):
                final_status = "unrun"
            else:
                final_status = "pass"
            manifest = {
                "scope": "three_host_transport_smoke_subset",
                "harness_version": "qsync-f-1",
                "status": final_status,
                "source_sha": config.source_sha,
                "binary_sha256": binary_hashes,
                "remote_binary": remote_binary,
                "topology": [ROLE_LABELS[role] for role in ROLE_NAMES],
                "providers": providers,
                "file_hash_verified": file_hash_verified,
                "phases": recorder.phases,
                "cleanup": cleanup_state,
                "full_f_claim": False,
                "raw_output_retained": False,
            }
            try:
                evidence.write_json("f-run-manifest.json", manifest)
                evidence.write_jsonl("f-phase-events.jsonl", recorder.phases)
            except Exception:
                # Evidence is part of the gate.  A redaction or persistence
                # failure must override an otherwise passing/blocked result.
                vault.clear()
                raise HarnessError("unexpected", "failed", 1)
        vault.clear()


def validate_artifact(source_sha: str, manifest_path: Path, binary: Path, role: str | None = None) -> int:
    source_sha = require_source_sha(source_sha)
    manifest = load_json_file(manifest_path)
    if manifest.get("status") != "pass" or manifest.get("source_sha") != source_sha or manifest.get("workflow_sha") != source_sha:
        print("QSYNC_F_VALIDATE|ok=false|error_class=source_mismatch")
        return 1
    artifact: Any = manifest.get("artifact")
    if role is not None:
        raw_artifacts = manifest.get("artifacts")
        if not isinstance(raw_artifacts, dict):
            print("QSYNC_F_VALIDATE|ok=false|error_class=manifest_invalid")
            return 1
        artifact = raw_artifacts.get(role)
    if not isinstance(artifact, dict) or not SHA256_RE.fullmatch(str(artifact.get("sha256", ""))):
        print("QSYNC_F_VALIDATE|ok=false|error_class=manifest_invalid")
        return 1
    if artifact.get("source_sha", source_sha) != source_sha or artifact.get("workflow_sha", source_sha) != source_sha:
        print("QSYNC_F_VALIDATE|ok=false|error_class=source_mismatch")
        return 1
    if not binary.is_file():
        print("QSYNC_F_VALIDATE|ok=false|error_class=binary_missing")
        return 1
    actual = file_sha256(binary)
    if actual != artifact["sha256"] or binary.stat().st_size != artifact.get("size_bytes"):
        print("QSYNC_F_VALIDATE|ok=false|error_class=binary_hash_mismatch")
        return 1
    print("QSYNC_F_VALIDATE|ok=true|phase=provenance")
    return 0


def validate_config_command(config_path: Path) -> int:
    try:
        config = load_config(config_path)
    except HarnessError as error:
        print(f"QSYNC_F_VALIDATE|ok=false|error_class={error.error_class}")
        return 1
    print(f"QSYNC_F_VALIDATE|ok=true|source_sha={config.source_sha}|roles=3")
    return 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="qSync three-host bootstrap verifier")
    sub = root.add_subparsers(dest="command", required=True)
    artifact = sub.add_parser("validate-artifact")
    artifact.add_argument("--source-sha", required=True)
    artifact.add_argument("--manifest", required=True, type=Path)
    artifact.add_argument("--binary", required=True, type=Path)
    artifact.add_argument("--role")
    config = sub.add_parser("validate-config")
    config.add_argument("--config", required=True, type=Path)
    run = sub.add_parser("run")
    run.add_argument("--config", required=True, type=Path)
    run.add_argument("--evidence-dir", required=True, type=Path)
    run.add_argument("--execute-external", action="store_true")
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "validate-artifact":
            return validate_artifact(args.source_sha, args.manifest, args.binary, args.role)
        if args.command == "validate-config":
            return validate_config_command(args.config)
        config = load_config(args.config)
        return run_harness(config, args.evidence_dir, args.execute_external)
    except HarnessError as error:
        if args.command in {"validate-artifact", "validate-config"}:
            print(f"QSYNC_F_VALIDATE|ok=false|error_class={error.error_class}")
        return 1
    except Exception:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
