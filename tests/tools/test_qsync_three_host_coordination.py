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


class CoordinationTests(unittest.TestCase):
    def invoke(self, rw: dict[str, object], ro: dict[str, object], seconds: int = 300) -> tuple[int, str]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rw_path = root / "rw.json"
            ro_path = root / "ro.json"
            rw_path.write_text(json.dumps(rw), encoding="utf-8")
            ro_path.write_text(json.dumps(ro), encoding="utf-8")
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                result = coordination.main(
                    [
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
                )
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


if __name__ == "__main__":
    unittest.main()
