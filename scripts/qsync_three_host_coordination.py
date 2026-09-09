#!/usr/bin/env python3
"""Check the safe evidence boundary for concurrent RW and hosted RO jobs.

This is an evidence gate only.  It does not read a key, endpoint, job log, or
remote command output, and it cannot turn a missing managed drain ACK into a
pass.  The RW result's bounded keepalive window is required so the two role
jobs were deliberately scheduled together; the actual provider payload gate
remains an E/share-swarm responsibility.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

SOURCE_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class CoordinationError(Exception):
    def __init__(self, error_class: str):
        self.error_class = error_class
        super().__init__(error_class)


def load_result(path: Path) -> dict[str, Any]:
    if not path.is_absolute() or not path.is_file() or path.stat().st_size > 2 * 1024 * 1024:
        raise CoordinationError("evidence_missing")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise CoordinationError("evidence_invalid") from None
    if not isinstance(value, dict):
        raise CoordinationError("evidence_invalid")
    return value


def require_source(value: Any) -> str:
    if not isinstance(value, str) or not SOURCE_SHA_RE.fullmatch(value):
        raise CoordinationError("source_mismatch")
    return value


def require_hash(value: Any) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise CoordinationError("manifest_invalid")
    return value


def verify(source_sha: str, keepalive_seconds: int, rw: dict[str, Any], ro: dict[str, Any]) -> None:
    if rw.get("status") != "pass" or ro.get("status") != "pass":
        raise CoordinationError("role_not_pass")
    if require_source(rw.get("source_sha")) != source_sha or require_source(ro.get("source_sha")) != source_sha:
        raise CoordinationError("source_mismatch")
    if not isinstance(rw.get("windows_binary_size_bytes"), int) or rw["windows_binary_size_bytes"] <= 0:
        raise CoordinationError("manifest_invalid")
    require_hash(rw.get("windows_binary_sha256"))
    ro_hashes = ro.get("binary_sha256")
    if not isinstance(ro_hashes, dict):
        raise CoordinationError("manifest_invalid")
    require_hash(ro_hashes.get("ro_consumer"))
    ro_file = ro.get("file_hash_verified")
    if not isinstance(ro_file, dict) or not isinstance(ro_file.get("ro_consumer"), dict):
        raise CoordinationError("file_hash_missing")
    ro_file = ro_file["ro_consumer"]
    require_hash(ro_file.get("sha256"))
    if not isinstance(ro_file.get("size_bytes"), int) or ro_file["size_bytes"] <= 0:
        raise CoordinationError("file_hash_missing")
    if rw.get("remote_contract_valid") is not True or ro.get("raw_output_retained") is not False:
        raise CoordinationError("role_contract_invalid")
    if rw.get("keepalive_requested_seconds") != keepalive_seconds or rw.get("keepalive_observed") is not True:
        raise CoordinationError("keepalive_missing")
    if rw.get("managed_bilateral_drain_ack") != "unverified":
        raise CoordinationError("evidence_invalid")
    if rw.get("state_retained_after_unknown") is not False:
        raise CoordinationError("role_contract_invalid")
    if rw.get("run_owned_copy_removed") is not True:
        raise CoordinationError("cleanup_incomplete")
    if ro.get("cleanup", {}).get("owned_paths_removed") is not True:
        raise CoordinationError("cleanup_incomplete")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="verify concurrent qSync role evidence")
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--keepalive-seconds", required=True, type=int)
    parser.add_argument("--rw-evidence", required=True, type=Path)
    parser.add_argument("--ro-evidence", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        source_sha = require_source(args.source_sha)
        if not 1 <= args.keepalive_seconds <= 900:
            raise CoordinationError("config_invalid")
        rw = load_result(args.rw_evidence)
        ro = load_result(args.ro_evidence)
        verify(source_sha, args.keepalive_seconds, rw, ro)
    except CoordinationError as error:
        print(f"QSYNC_F_COORDINATION|ok=false|error_class={error.error_class}")
        return 1
    except Exception:
        print("QSYNC_F_COORDINATION|ok=false|error_class=evidence_invalid")
        return 1
    print("QSYNC_F_COORDINATION|ok=true|roles=2|keepalive=observed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
