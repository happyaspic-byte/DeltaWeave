#!/usr/bin/env python3
"""Check the safe evidence boundary for concurrent RW and hosted RO jobs.

This is an evidence gate only.  It does not read a key, endpoint, job log, or
remote command output, and it cannot turn a missing managed drain ACK into a
pass.  The RW result's bounded keepalive window is required so the two role
jobs were deliberately scheduled together; the actual provider payload gate
remains an E/share-swarm responsibility.  When supplied, the optional
provider-payload evidence gate checks that responsibility using only the
redacted, source-tagged counters and event totals emitted by the controller
adapter; it never infers payload work from a roster or a local file hash.
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
PROVIDER_PROTOCOL = "deltaweave/share-swarm/1"
PROVIDER_SOURCE_TAG = "share_swarm_v1"
PROVIDER_ROLES = ("owner-provider", "rw-provider")
PROVIDER_SCHEMA_VERSION = 1
PROVIDER_FIXTURE_SIZE_BYTES = 8 * 1024 * 1024
MAX_PROVIDER_EVENTS = 4096
MAX_COUNTER_VALUE = (1 << 63) - 1


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


def _provider_error(error_class: str) -> CoordinationError:
    return CoordinationError(error_class)


def _provider_object(value: Any, *, required: set[str], allowed: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != allowed or not required.issubset(value):
        raise _provider_error("provider_payload_invalid")
    return value


def _provider_int(value: Any, *, positive: bool = False) -> int:
    if type(value) is not int or value < (1 if positive else 0) or value > MAX_COUNTER_VALUE:
        raise _provider_error("provider_counter_invalid")
    return value


def _provider_hash(value: Any) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise _provider_error("provider_file_mismatch")
    return value


def _provider_timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or not UTC_TIMESTAMP_RE.fullmatch(value):
        raise _provider_error("provider_window_missing")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise _provider_error("provider_window_missing") from None
    if parsed.tzinfo is None:
        raise _provider_error("provider_window_missing")
    return parsed


def _provider_interval(start_value: Any, finish_value: Any) -> tuple[datetime, datetime]:
    started = _provider_timestamp(start_value)
    finished = _provider_timestamp(finish_value)
    if finished <= started:
        raise _provider_error("provider_window_missing")
    return started, finished


def _require_provider_interval_inside(
    inner: tuple[datetime, datetime], outer: tuple[datetime, datetime]
) -> None:
    if inner[0] < outer[0] or inner[1] > outer[1]:
        raise _provider_error("provider_window_missing")


def verify_provider_payload(
    payload: dict[str, Any],
    *,
    run_id: str,
    source_sha: str,
    expected_file_hash: str,
    expected_file_size: int,
    keepalive_window: tuple[datetime, datetime] | None = None,
) -> None:
    """Verify an actual share-swarm provider payload evidence bundle.

    The controller adapter must create this normalized bundle from typed
    share-swarm observations.  A local file hash, member roster, provider
    registration, or an aggregate legacy ``transferred_bytes`` field is not
    accepted as a substitute.  Strict object keys keep secret-bearing or
    endpoint-bearing debug fields out of this evidence boundary.
    """

    if not isinstance(payload, dict):
        raise _provider_error("provider_payload_invalid")
    _provider_object(
        payload,
        required={
            "schema_version",
            "scope",
            "status",
            "full_f_claim",
            "run_id",
            "source_sha",
            "fixture",
            "payload_window",
            "providers",
            "consumer",
        },
        allowed={
            "schema_version",
            "scope",
            "status",
            "full_f_claim",
            "run_id",
            "source_sha",
            "fixture",
            "payload_window",
            "providers",
            "consumer",
        },
    )
    if payload["schema_version"] != PROVIDER_SCHEMA_VERSION or payload["scope"] != "share_swarm_provider_payload_window":
        raise _provider_error("provider_payload_invalid")
    if payload["status"] != "pass" or payload["full_f_claim"] is not False:
        raise _provider_error("provider_payload_invalid")
    try:
        payload_run_id = require_run_id(payload["run_id"])
        payload_source_sha = require_source(payload["source_sha"])
    except CoordinationError:
        raise _provider_error("provider_source_mismatch") from None
    if payload_run_id != run_id or payload_source_sha != source_sha:
        raise _provider_error("provider_source_mismatch")
    expected_file_hash = _provider_hash(expected_file_hash)
    if type(expected_file_size) is not int or expected_file_size != PROVIDER_FIXTURE_SIZE_BYTES:
        raise _provider_error("provider_file_mismatch")

    fixture = _provider_object(
        payload["fixture"],
        required={"sha256", "size_bytes"},
        allowed={"sha256", "size_bytes"},
    )
    if _provider_hash(fixture["sha256"]) != expected_file_hash or fixture["size_bytes"] != expected_file_size:
        raise _provider_error("provider_file_mismatch")
    _provider_int(fixture["size_bytes"], positive=True)

    payload_window_record = _provider_object(
        payload["payload_window"],
        required={"started_utc", "finished_utc", "other_payload_observed"},
        allowed={"started_utc", "finished_utc", "other_payload_observed"},
    )
    payload_window = _provider_interval(
        payload_window_record["started_utc"], payload_window_record["finished_utc"]
    )
    if payload_window_record["other_payload_observed"] is not False:
        raise _provider_error("provider_attribution_invalid")
    if keepalive_window is not None:
        _require_provider_interval_inside(payload_window, keepalive_window)

    providers = payload["providers"]
    if not isinstance(providers, list) or len(providers) != len(PROVIDER_ROLES):
        raise _provider_error("provider_payload_invalid")
    provider_by_role: dict[str, dict[str, Any]] = {}
    provider_event_times: dict[str, list[datetime]] = {}
    for raw_provider in providers:
        provider = _provider_object(
            raw_provider,
            required={
                "role",
                "protocol",
                "source_tag",
                "counter_scope",
                "observed",
                "baseline_observed_utc",
                "end_observed_utc",
                "baseline_transferred_bytes",
                "end_transferred_bytes",
                "baseline_verified_chunks",
                "end_verified_chunks",
                "events",
            },
            allowed={
                "role",
                "protocol",
                "source_tag",
                "counter_scope",
                "observed",
                "baseline_observed_utc",
                "end_observed_utc",
                "baseline_transferred_bytes",
                "end_transferred_bytes",
                "baseline_verified_chunks",
                "end_verified_chunks",
                "events",
            },
        )
        role = provider["role"]
        if role not in PROVIDER_ROLES or role in provider_by_role:
            raise _provider_error("provider_attribution_invalid")
        if (
            provider["protocol"] != PROVIDER_PROTOCOL
            or provider["source_tag"] != PROVIDER_SOURCE_TAG
            or provider["counter_scope"] != "share_swarm_provider"
            or provider["observed"] is not True
        ):
            raise _provider_error("provider_attribution_invalid")
        baseline_window = _provider_interval(
            provider["baseline_observed_utc"], provider["end_observed_utc"]
        )
        _require_provider_interval_inside(baseline_window, payload_window)
        baseline_bytes = _provider_int(provider["baseline_transferred_bytes"])
        end_bytes = _provider_int(provider["end_transferred_bytes"])
        baseline_chunks = _provider_int(provider["baseline_verified_chunks"])
        end_chunks = _provider_int(provider["end_verified_chunks"])
        delta_bytes = end_bytes - baseline_bytes
        delta_chunks = end_chunks - baseline_chunks
        if delta_bytes <= 0 or delta_chunks <= 0 or delta_bytes > expected_file_size:
            raise _provider_error("provider_counter_invalid")
        events = provider["events"]
        if not isinstance(events, list) or not 1 <= len(events) <= MAX_PROVIDER_EVENTS:
            raise _provider_error("provider_payload_invalid")
        event_bytes = 0
        event_chunks = 0
        for raw_event in events:
            event = _provider_object(
                raw_event,
                required={"observed_utc", "protocol", "source_tag", "verified", "bytes", "chunks"},
                allowed={"observed_utc", "protocol", "source_tag", "verified", "bytes", "chunks"},
            )
            observed = _provider_timestamp(event["observed_utc"])
            if (
                event["protocol"] != PROVIDER_PROTOCOL
                or event["source_tag"] != PROVIDER_SOURCE_TAG
                or event["verified"] is not True
            ):
                raise _provider_error("provider_attribution_invalid")
            if (
                observed < baseline_window[0]
                or observed > baseline_window[1]
                or observed < payload_window[0]
                or observed > payload_window[1]
            ):
                raise _provider_error("provider_window_missing")
            event_bytes += _provider_int(event["bytes"], positive=True)
            event_chunks += _provider_int(event["chunks"], positive=True)
        if event_bytes != delta_bytes or event_chunks != delta_chunks:
            raise _provider_error("provider_counter_invalid")
        provider_by_role[role] = provider
        provider_event_times[role] = [
            _provider_timestamp(event["observed_utc"]) for event in events
        ]

    consumer = _provider_object(
        payload["consumer"],
        required={
            "role",
            "protocol",
            "source_tag",
            "join_started_utc",
            "join_finished_utc",
            "file_hash_started_utc",
            "file_hash_finished_utc",
            "received_bytes",
            "reused_bytes",
            "reused_chunks",
            "preexisting_fixture_chunks",
            "file_hash_observed",
            "file_hash",
            "size_bytes",
        },
        allowed={
            "role",
            "protocol",
            "source_tag",
            "join_started_utc",
            "join_finished_utc",
            "file_hash_started_utc",
            "file_hash_finished_utc",
            "received_bytes",
            "reused_bytes",
            "reused_chunks",
            "preexisting_fixture_chunks",
            "file_hash_observed",
            "file_hash",
            "size_bytes",
        },
    )
    if (
        consumer["role"] != "ro-consumer"
        or consumer["protocol"] != PROVIDER_PROTOCOL
        or consumer["source_tag"] != PROVIDER_SOURCE_TAG
        or consumer["file_hash_observed"] is not True
    ):
        raise _provider_error("provider_attribution_invalid")
    join_window = _provider_interval(consumer["join_started_utc"], consumer["join_finished_utc"])
    file_window = _provider_interval(consumer["file_hash_started_utc"], consumer["file_hash_finished_utc"])
    if file_window[0] < join_window[1] or file_window[1] <= file_window[0]:
        raise _provider_error("provider_window_missing")
    _require_provider_interval_inside(join_window, payload_window)
    _require_provider_interval_inside(file_window, payload_window)
    for provider in provider_by_role.values():
        provider_window = _provider_interval(provider["baseline_observed_utc"], provider["end_observed_utc"])
        if provider_window[0] > join_window[0] or provider_window[1] < file_window[1]:
            raise _provider_error("provider_window_missing")
    for event_times in provider_event_times.values():
        if any(observed < join_window[0] or observed > file_window[1] for observed in event_times):
            raise _provider_error("provider_window_missing")
    if (
        _provider_int(consumer["received_bytes"], positive=True) != expected_file_size
        or _provider_int(consumer["reused_bytes"]) != 0
        or _provider_int(consumer["reused_chunks"]) != 0
        or _provider_int(consumer["preexisting_fixture_chunks"]) != 0
        or _provider_hash(consumer["file_hash"]) != expected_file_hash
        or _provider_int(consumer["size_bytes"], positive=True) != expected_file_size
    ):
        raise _provider_error("provider_file_mismatch")
    total_provider_bytes = sum(
        _provider_int(provider["end_transferred_bytes"]) - _provider_int(provider["baseline_transferred_bytes"])
        for provider in provider_by_role.values()
    )
    if total_provider_bytes < expected_file_size:
        raise _provider_error("provider_counter_invalid")


def verify(
    run_id: str,
    source_sha: str,
    keepalive_seconds: int,
    rw: dict[str, Any],
    ro: dict[str, Any],
    provider_payload: dict[str, Any] | None = None,
    require_provider_payload: bool = False,
) -> None:
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
    diagnostic_stages = rw.get("remote_diagnostic_stages")
    diagnostic_counts = rw.get("remote_diagnostic_counts")
    if (
        not isinstance(diagnostic_stages, list)
        or diagnostic_stages.count("keepalive_enter") != 1
        or diagnostic_stages.count("keepalive_done") != 1
        or not isinstance(diagnostic_counts, dict)
        or diagnostic_counts.get("keepalive_enter") != keepalive_seconds
        or diagnostic_counts.get("keepalive_done") != keepalive_seconds
    ):
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
    if require_provider_payload and provider_payload is None:
        raise CoordinationError("provider_payload_missing")
    if provider_payload is not None:
        verify_provider_payload(
            provider_payload,
            run_id=run_id,
            source_sha=source_sha,
            expected_file_hash=rw_expected_hash,
            expected_file_size=rw_expected_size,
            keepalive_window=keepalive_window,
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="verify concurrent qSync role evidence")
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--keepalive-seconds", required=True, type=int)
    parser.add_argument("--rw-evidence", required=True, type=Path)
    parser.add_argument("--ro-evidence", required=True, type=Path)
    parser.add_argument("--provider-evidence", type=Path)
    parser.add_argument("--require-provider-payload", action="store_true")
    args = parser.parse_args(argv)
    try:
        run_id = require_run_id(args.run_id)
        source_sha = require_source(args.source_sha)
        if not 1 <= args.keepalive_seconds <= 900:
            raise CoordinationError("config_invalid")
        rw = load_result(args.rw_evidence)
        ro = load_result(args.ro_evidence)
        if args.require_provider_payload and args.provider_evidence is None:
            raise CoordinationError("provider_payload_missing")
        provider_payload = load_result(args.provider_evidence) if args.provider_evidence is not None else None
        verify(
            run_id,
            source_sha,
            args.keepalive_seconds,
            rw,
            ro,
            provider_payload=provider_payload,
            require_provider_payload=args.require_provider_payload,
        )
    except CoordinationError as error:
        print(f"QSYNC_F_COORDINATION|ok=false|error_class={error.error_class}")
        return 1
    except Exception:
        print("QSYNC_F_COORDINATION|ok=false|error_class=evidence_invalid")
        return 1
    payload_suffix = "|provider_payload=verified" if args.provider_evidence is not None else ""
    print(f"QSYNC_F_COORDINATION|ok=true|roles=2|keepalive=observed{payload_suffix}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
