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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SOURCE_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
RUN_ID_RE = re.compile(r"^[0-9]{1,20}$")
UTC_TIMESTAMP_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z$"
)


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


def require_run_id(value: Any) -> str:
    if not isinstance(value, str) or not RUN_ID_RE.fullmatch(value):
        raise CoordinationError("run_mismatch")
    return value


def parse_utc_timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or not UTC_TIMESTAMP_RE.fullmatch(value):
        raise CoordinationError("time_window_missing")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise CoordinationError("time_window_missing") from None


def interval_values(start_value: Any, finish_value: Any) -> tuple[datetime, datetime]:
    started = parse_utc_timestamp(start_value)
    finished = parse_utc_timestamp(finish_value)
    if finished <= started:
        raise CoordinationError("time_window_missing")
    return started, finished


def interval(result: dict[str, Any], start_key: str, finish_key: str) -> tuple[datetime, datetime]:
    return interval_values(result.get(start_key), result.get(finish_key))


def intervals_overlap(left: tuple[datetime, datetime], right: tuple[datetime, datetime]) -> bool:
    return max(left[0], right[0]) < min(left[1], right[1])


def validate_keepalive_trace(enter_elapsed_ms: Any, done_elapsed_ms: Any, keepalive_seconds: int) -> None:
    if (
        type(enter_elapsed_ms) is not int
        or enter_elapsed_ms < 0
        or type(done_elapsed_ms) is not int
        or done_elapsed_ms < enter_elapsed_ms
        or done_elapsed_ms - enter_elapsed_ms < keepalive_seconds * 1000
    ):
        raise CoordinationError("keepalive_missing")


def verify(run_id: str, source_sha: str, keepalive_seconds: int, rw: dict[str, Any], ro: dict[str, Any]) -> None:
    if rw.get("status") != "pass" or ro.get("status") != "pass":
        raise CoordinationError("role_not_pass")
    if require_run_id(rw.get("run_id")) != run_id or require_run_id(ro.get("run_id")) != run_id:
        raise CoordinationError("run_mismatch")
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
    ro_file_hash = require_hash(ro_file.get("sha256"))
    if not isinstance(ro_file.get("size_bytes"), int) or ro_file["size_bytes"] <= 0:
        raise CoordinationError("file_hash_missing")
    rw_expected_hash = require_hash(rw.get("expected_file_hash"))
    ro_expected_hash = require_hash(ro.get("expected_file_hash"))
    if rw_expected_hash != ro_expected_hash or ro_file_hash != rw_expected_hash:
        raise CoordinationError("fixture_mismatch")
    rw_expected_size = rw.get("expected_file_size_bytes")
    ro_expected_size = ro.get("expected_file_size_bytes")
    if (
        not isinstance(rw_expected_size, int)
        or rw_expected_size <= 0
        or not isinstance(ro_expected_size, int)
        or ro_expected_size != rw_expected_size
        or ro_file["size_bytes"] != rw_expected_size
    ):
        raise CoordinationError("fixture_mismatch")
    if rw.get("remote_contract_valid") is not True or ro.get("raw_output_retained") is not False:
        raise CoordinationError("role_contract_invalid")
    if rw.get("keepalive_requested_seconds") != keepalive_seconds or rw.get("keepalive_observed") is not True:
        raise CoordinationError("keepalive_missing")
    rw_window = interval(rw, "remote_command_started_utc", "remote_command_finished_utc")
    ro_window = interval(ro, "join_started_utc", "join_finished_utc")
    ro_file_window = interval(ro, "file_hash_started_utc", "file_hash_finished_utc")
    validate_keepalive_trace(
        rw.get("keepalive_trace_enter_elapsed_ms"),
        rw.get("keepalive_trace_done_elapsed_ms"),
        keepalive_seconds,
    )
    # Remote elapsed values prove the requested keepalive duration but use a
    # different clock origin.  Use controller receive timestamps for the
    # cross-role overlap check instead of adding remote elapsed time to the
    # controller's command-start timestamp.
    keepalive_window = interval_values(
        rw.get("keepalive_enter_observed_utc"), rw.get("keepalive_done_observed_utc")
    )
    if keepalive_window[0] < rw_window[0] or keepalive_window[1] > rw_window[1]:
        raise CoordinationError("time_window_missing")
    if not intervals_overlap(keepalive_window, ro_window) or not intervals_overlap(keepalive_window, ro_file_window):
        raise CoordinationError("time_window_missing")
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
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--keepalive-seconds", required=True, type=int)
    parser.add_argument("--rw-evidence", required=True, type=Path)
    parser.add_argument("--ro-evidence", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        run_id = require_run_id(args.run_id)
        source_sha = require_source(args.source_sha)
        if not 1 <= args.keepalive_seconds <= 900:
            raise CoordinationError("config_invalid")
        rw = load_result(args.rw_evidence)
        ro = load_result(args.ro_evidence)
        verify(run_id, source_sha, args.keepalive_seconds, rw, ro)
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
