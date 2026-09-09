from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).parents[2]

import sys

sys.path.insert(0, str(ROOT / "scripts"))
import qsync_three_host_coordination as coordination  # noqa: E402


SOURCE = "a" * 40
RUN_ID = "123456789"
WINDOWS_HASH = "b" * 64
LINUX_HASH = "c" * 64
FILE_HASH = "d" * 64
FIXTURE_SIZE = 256
PAYLOAD_SIZE = 8 * 1024 * 1024


def valid_results() -> tuple[dict[str, object], dict[str, object]]:
    rw = {
        "status": "pass",
        "run_id": RUN_ID,
        "remote_command_started_utc": "2026-09-09T08:00:00Z",
        "remote_command_finished_utc": "2026-09-09T08:10:00Z",
        "source_sha": SOURCE,
        "windows_binary_sha256": WINDOWS_HASH,
        "windows_binary_size_bytes": 100,
        "keepalive_requested_seconds": 300,
        "keepalive_observed": True,
        "keepalive_trace_enter_elapsed_ms": 1000,
        "keepalive_trace_done_elapsed_ms": 301000,
        "keepalive_enter_observed_utc": "2026-09-09T08:00:01.100000Z",
        "keepalive_done_observed_utc": "2026-09-09T08:05:01.100000Z",
        "remote_diagnostic_stages": ["keepalive_enter", "keepalive_done"],
        "remote_diagnostic_counts": {"keepalive_enter": 300, "keepalive_done": 300},
        "expected_file_hash": FILE_HASH,
        "expected_file_size_bytes": FIXTURE_SIZE,
        "remote_contract_valid": True,
        "managed_bilateral_drain_ack": "unverified",
        "state_retained_after_unknown": False,
        "run_owned_copy_removed": True,
    }
    ro = {
        "status": "pass",
        "run_id": RUN_ID,
        "run_started_utc": "2026-09-09T08:02:00Z",
        "run_finished_utc": "2026-09-09T08:05:00Z",
        "join_started_utc": "2026-09-09T08:03:00.100000Z",
        "join_finished_utc": "2026-09-09T08:04:00.100000Z",
        "file_hash_started_utc": "2026-09-09T08:04:00.200000Z",
        "file_hash_finished_utc": "2026-09-09T08:05:00.200000Z",
        "source_sha": SOURCE,
        "binary_sha256": {"ro_consumer": LINUX_HASH},
        "file_hash_verified": {"ro_consumer": {"sha256": FILE_HASH, "size_bytes": FIXTURE_SIZE}},
        "expected_file_hash": FILE_HASH,
        "expected_file_size_bytes": FIXTURE_SIZE,
        "raw_output_retained": False,
        "cleanup": {"owned_paths_removed": True},
    }
    return rw, ro


def valid_provider_payload() -> dict[str, object]:
    def provider(role: str, baseline: int, end: int, chunks: int, observed: str) -> dict[str, object]:
        delta = end - baseline
        return {
            "role": role,
            "protocol": coordination.PROVIDER_PROTOCOL,
            "source_tag": coordination.PROVIDER_SOURCE_TAG,
            "counter_scope": "share_swarm_provider",
            "observed": True,
            "baseline_observed_utc": "2026-09-09T08:02:59.000000Z",
            "end_observed_utc": "2026-09-09T08:05:00.500000Z",
            "baseline_transferred_bytes": baseline,
            "end_transferred_bytes": end,
            "baseline_verified_chunks": 0,
            "end_verified_chunks": chunks,
            "events": [
                {
                    "observed_utc": observed,
                    "protocol": coordination.PROVIDER_PROTOCOL,
                    "source_tag": coordination.PROVIDER_SOURCE_TAG,
                    "verified": True,
                    "bytes": delta,
                    "chunks": chunks,
                }
            ],
        }

    return {
        "schema_version": coordination.PROVIDER_SCHEMA_VERSION,
        "scope": "share_swarm_provider_payload_window",
        "status": "pass",
        "full_f_claim": False,
        "run_id": RUN_ID,
        "source_sha": SOURCE,
        "fixture": {"sha256": FILE_HASH, "size_bytes": PAYLOAD_SIZE},
        "payload_window": {
            "started_utc": "2026-09-09T08:02:58.000000Z",
            "finished_utc": "2026-09-09T08:05:01.000000Z",
            "other_payload_observed": False,
        },
        "providers": [
            provider("owner-provider", 0, 4 * 1024 * 1024, 64, "2026-09-09T08:03:30.000000Z"),
            provider("rw-provider", 8192, 4 * 1024 * 1024 + 8192, 64, "2026-09-09T08:04:30.000000Z"),
        ],
        "consumer": {
            "role": "ro-consumer",
            "protocol": coordination.PROVIDER_PROTOCOL,
            "source_tag": coordination.PROVIDER_SOURCE_TAG,
            "join_started_utc": "2026-09-09T08:03:00.100000Z",
            "join_finished_utc": "2026-09-09T08:04:00.100000Z",
            "file_hash_started_utc": "2026-09-09T08:04:00.200000Z",
            "file_hash_finished_utc": "2026-09-09T08:05:00.200000Z",
            "received_bytes": PAYLOAD_SIZE,
            "reused_bytes": 0,
            "reused_chunks": 0,
            "preexisting_fixture_chunks": 0,
            "file_hash_observed": True,
            "file_hash": FILE_HASH,
            "size_bytes": PAYLOAD_SIZE,
        },
    }


class CoordinationTests(unittest.TestCase):
    def invoke(
        self,
        rw: dict[str, object],
        ro: dict[str, object],
        seconds: int = 300,
        provider: dict[str, object] | None = None,
        require_provider: bool = False,
    ) -> tuple[int, str]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rw_path = root / "rw.json"
            ro_path = root / "ro.json"
            rw_path.write_text(json.dumps(rw), encoding="utf-8")
            ro_path.write_text(json.dumps(ro), encoding="utf-8")
            provider_path = root / "provider.json"
            if provider is not None:
                provider_path.write_text(json.dumps(provider), encoding="utf-8")
            output = io.StringIO()
            arguments = [
                "--source-sha",
                SOURCE,
                "--run-id",
                RUN_ID,
                "--keepalive-seconds",
                str(seconds),
                "--rw-evidence",
                str(rw_path),
                "--ro-evidence",
                str(ro_path),
            ]
            if provider is not None:
                arguments.extend(("--provider-evidence", str(provider_path)))
            if require_provider:
                arguments.append("--require-provider-payload")
            with contextlib.redirect_stdout(output):
                result = coordination.main(arguments)
            return result, output.getvalue()

    def test_accepts_independent_binary_hashes_and_observed_file(self) -> None:
        rw, ro = valid_results()
        result, output = self.invoke(rw, ro)
        self.assertEqual(result, 0)
        self.assertIn("ok=true", output)

    def test_rejects_missing_keepalive_or_role_pass(self) -> None:
        rw, ro = valid_results()
        rw["keepalive_observed"] = False
        result, output = self.invoke(rw, ro)
        self.assertEqual(result, 1)
        self.assertIn("keepalive_missing", output)
        rw, ro = valid_results()
        rw["remote_diagnostic_stages"] = ["keepalive_enter"]
        result, output = self.invoke(rw, ro)
        self.assertEqual(result, 1)
        self.assertIn("keepalive_missing", output)
        rw, ro = valid_results()
        ro["status"] = "pending"
        result, output = self.invoke(rw, ro)
        self.assertEqual(result, 1)
        self.assertIn("role_not_pass", output)

    def test_rejects_different_run_id_or_non_overlapping_join(self) -> None:
        rw, ro = valid_results()
        ro["run_id"] = "987654321"
        result, output = self.invoke(rw, ro)
        self.assertEqual(result, 1)
        self.assertIn("run_mismatch", output)
        rw, ro = valid_results()
        ro["join_started_utc"] = "2026-09-09T08:11:00Z"
        ro["join_finished_utc"] = "2026-09-09T08:12:00Z"
        result, output = self.invoke(rw, ro)
        self.assertEqual(result, 1)
        self.assertIn("time_window_missing", output)
        rw, ro = valid_results()
        rw["keepalive_enter_observed_utc"] = None
        result, output = self.invoke(rw, ro)
        self.assertEqual(result, 1)
        self.assertIn("time_window_missing", output)

    def test_rejects_different_fixture_hash_or_size(self) -> None:
        rw, ro = valid_results()
        ro["expected_file_hash"] = "f" * 64
        result, output = self.invoke(rw, ro)
        self.assertEqual(result, 1)
        self.assertIn("fixture_mismatch", output)

    def test_rejects_role_activity_outside_keepalive_window(self) -> None:
        rw, ro = valid_results()
        ro["join_started_utc"] = "2026-09-09T08:06:00Z"
        ro["join_finished_utc"] = "2026-09-09T08:07:00Z"
        ro["file_hash_started_utc"] = "2026-09-09T08:07:00Z"
        ro["file_hash_finished_utc"] = "2026-09-09T08:08:00Z"
        result, output = self.invoke(rw, ro)
        self.assertEqual(result, 1)
        self.assertIn("time_window_missing", output)
        rw, ro = valid_results()
        ro["expected_file_size_bytes"] = 128
        result, output = self.invoke(rw, ro)
        self.assertEqual(result, 1)
        self.assertIn("fixture_mismatch", output)

    def test_rejects_missing_file_or_unknown_state_cleanup(self) -> None:
        rw, ro = valid_results()
        ro["file_hash_verified"] = {}
        result, output = self.invoke(rw, ro)
        self.assertEqual(result, 1)
        self.assertIn("file_hash_missing", output)
        rw, ro = valid_results()
        rw["state_retained_after_unknown"] = True
        result, output = self.invoke(rw, ro)
        self.assertEqual(result, 1)
        self.assertIn("role_contract_invalid", output)
        rw, ro = valid_results()
        rw["run_owned_copy_removed"] = False
        result, output = self.invoke(rw, ro)
        self.assertEqual(result, 1)
        self.assertIn("cleanup_incomplete", output)

    def test_provider_gate_accepts_two_distinct_verified_deltas(self) -> None:
        rw, ro = valid_results()
        rw["expected_file_size_bytes"] = PAYLOAD_SIZE
        ro["expected_file_size_bytes"] = PAYLOAD_SIZE
        ro["file_hash_verified"] = {
            "ro_consumer": {"sha256": FILE_HASH, "size_bytes": PAYLOAD_SIZE}
        }
        provider = valid_provider_payload()
        result, output = self.invoke(rw, ro, provider=provider, require_provider=True)
        self.assertEqual(result, 0)
        self.assertIn("provider_payload=verified", output)

    def test_provider_gate_requires_explicit_evidence(self) -> None:
        rw, ro = valid_results()
        result, output = self.invoke(rw, ro, require_provider=True)
        self.assertEqual(result, 1)
        self.assertIn("provider_payload_missing", output)

    def test_provider_gate_rejects_zero_or_unattributed_provider_delta(self) -> None:
        rw, ro = valid_results()
        rw["expected_file_size_bytes"] = PAYLOAD_SIZE
        ro["expected_file_size_bytes"] = PAYLOAD_SIZE
        ro["file_hash_verified"] = {
            "ro_consumer": {"sha256": FILE_HASH, "size_bytes": PAYLOAD_SIZE}
        }
        provider = valid_provider_payload()
        provider["providers"][0]["end_transferred_bytes"] = 0
        provider["providers"][0]["events"][0]["bytes"] = 0
        provider["providers"][0]["events"][0]["chunks"] = 1
        result, output = self.invoke(rw, ro, provider=provider, require_provider=True)
        self.assertEqual(result, 1)
        self.assertIn("provider_counter_invalid", output)
        provider = valid_provider_payload()
        provider["providers"][1]["source_tag"] = "legacy_sync"
        result, output = self.invoke(rw, ro, provider=provider, require_provider=True)
        self.assertEqual(result, 1)
        self.assertIn("provider_attribution_invalid", output)

    def test_provider_gate_rejects_counter_event_mismatch_and_cache_reuse(self) -> None:
        rw, ro = valid_results()
        rw["expected_file_size_bytes"] = PAYLOAD_SIZE
        ro["expected_file_size_bytes"] = PAYLOAD_SIZE
        ro["file_hash_verified"] = {
            "ro_consumer": {"sha256": FILE_HASH, "size_bytes": PAYLOAD_SIZE}
        }
        provider = valid_provider_payload()
        provider["providers"][0]["events"][0]["bytes"] += 1
        result, output = self.invoke(rw, ro, provider=provider, require_provider=True)
        self.assertEqual(result, 1)
        self.assertIn("provider_counter_invalid", output)
        provider = valid_provider_payload()
        provider["consumer"]["reused_chunks"] = 1
        result, output = self.invoke(rw, ro, provider=provider, require_provider=True)
        self.assertEqual(result, 1)
        self.assertIn("provider_file_mismatch", output)

    def test_provider_gate_rejects_wrong_source_run_or_window(self) -> None:
        rw, ro = valid_results()
        rw["expected_file_size_bytes"] = PAYLOAD_SIZE
        ro["expected_file_size_bytes"] = PAYLOAD_SIZE
        ro["file_hash_verified"] = {
            "ro_consumer": {"sha256": FILE_HASH, "size_bytes": PAYLOAD_SIZE}
        }
        provider = valid_provider_payload()
        provider["source_sha"] = "e" * 40
        result, output = self.invoke(rw, ro, provider=provider, require_provider=True)
        self.assertEqual(result, 1)
        self.assertIn("provider_source_mismatch", output)
        provider = valid_provider_payload()
        provider["providers"][0]["events"][0]["observed_utc"] = "2026-09-09T08:06:00Z"
        result, output = self.invoke(rw, ro, provider=provider, require_provider=True)
        self.assertEqual(result, 1)
        self.assertIn("provider_window_missing", output)

    def test_provider_gate_rejects_payload_outside_join_window_and_legacy_aggregate(self) -> None:
        rw, ro = valid_results()
        rw["expected_file_size_bytes"] = PAYLOAD_SIZE
        ro["expected_file_size_bytes"] = PAYLOAD_SIZE
        ro["file_hash_verified"] = {
            "ro_consumer": {"sha256": FILE_HASH, "size_bytes": PAYLOAD_SIZE}
        }
        provider = valid_provider_payload()
        provider["providers"][0]["events"][0]["observed_utc"] = "2026-09-09T08:02:59.500000Z"
        result, output = self.invoke(rw, ro, provider=provider, require_provider=True)
        self.assertEqual(result, 1)
        self.assertIn("provider_window_missing", output)
        provider = valid_provider_payload()
        provider["legacy_transferred_bytes"] = PAYLOAD_SIZE
        result, output = self.invoke(rw, ro, provider=provider, require_provider=True)
        self.assertEqual(result, 1)
        self.assertIn("provider_payload_invalid", output)


if __name__ == "__main__":
    unittest.main()
