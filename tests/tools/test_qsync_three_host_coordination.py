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
WINDOWS_HASH = "b" * 64
LINUX_HASH = "c" * 64
FILE_HASH = "d" * 64


def valid_results() -> tuple[dict[str, object], dict[str, object]]:
    rw = {
        "status": "pass",
        "source_sha": SOURCE,
        "windows_binary_sha256": WINDOWS_HASH,
        "windows_binary_size_bytes": 100,
        "keepalive_requested_seconds": 300,
        "keepalive_observed": True,
        "remote_contract_valid": True,
        "managed_bilateral_drain_ack": "unverified",
        "state_retained_after_unknown": False,
        "run_owned_copy_removed": True,
    }
    ro = {
        "status": "pass",
        "source_sha": SOURCE,
        "binary_sha256": {"ro_consumer": LINUX_HASH},
        "file_hash_verified": {"ro_consumer": {"sha256": FILE_HASH, "size_bytes": 256}},
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
        ro["status"] = "pending"
        result, output = self.invoke(rw, ro)
        self.assertEqual(result, 1)
        self.assertIn("role_not_pass", output)

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
