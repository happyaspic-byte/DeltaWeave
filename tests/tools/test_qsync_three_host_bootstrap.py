from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import subprocess
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).parents[2] / "scripts" / "qsync_three_host_bootstrap.py"
SPEC = importlib.util.spec_from_file_location("qsync_three_host_bootstrap", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class QsyncBootstrapTests(unittest.TestCase):
    @staticmethod
    def _process_spec() -> object:
        return MODULE.RoleSpec(
            "owner",
            "local",
            None,
            "a" * 64,
            None,
            None,
            None,
            "127.0.0.1",
            None,
            None,
            None,
        )

    @staticmethod
    def _signal_child(exit_code: int, ready: Path) -> list[str]:
        code = (
            "import signal,sys,time\n"
            "from pathlib import Path\n"
            f"def stop(_signum, _frame): sys.exit({exit_code})\n"
            "signal.signal(signal.SIGTERM, stop)\n"
            "sigbreak = getattr(signal, 'SIGBREAK', None)\n"
            "if sigbreak is not None: signal.signal(sigbreak, stop)\n"
            f"Path({json.dumps(str(ready))}).write_text('ready')\n"
            "time.sleep(60)\n"
        )
        return [sys.executable, "-c", code]

    def test_phase_recorder_uses_subsecond_utc_timestamps(self) -> None:
        recorder = MODULE.PhaseRecorder()
        with mock.patch.object(
            MODULE,
            "utc_now_precise",
            side_effect=["2026-09-09T08:00:00.100000Z", "2026-09-09T08:00:00.200000Z"],
        ):
            outcome = recorder.run("member_join", "ro_consumer", "test.join", lambda: None)
        self.assertEqual(outcome.status, "pass")
        self.assertEqual(recorder.phases[0]["started_utc"], "2026-09-09T08:00:00.100000Z")
        self.assertEqual(recorder.phases[0]["finished_utc"], "2026-09-09T08:00:00.200000Z")

    def test_secret_config_is_rejected_but_environment_reference_is_allowed(self) -> None:
        with self.assertRaises(MODULE.HarnessError) as error:
            MODULE.reject_secret_config({"password": "never-in-config"})
        self.assertEqual(error.exception.error_class, "config_invalid")
        MODULE.reject_secret_config({"admin_token_env": "QSYNC_F_ADMIN_TOKEN"})

    def test_source_and_environment_names_are_strict(self) -> None:
        self.assertEqual(MODULE.require_source_sha("a" * 40), "a" * 40)
        with self.assertRaises(MODULE.HarnessError):
            MODULE.require_source_sha("A" * 40)
        self.assertEqual(MODULE.validate_env_name("QSYNC_F_OWNER_API_URL"), "QSYNC_F_OWNER_API_URL")
        with self.assertRaises(MODULE.HarnessError):
            MODULE.validate_env_name("QSYNC_F_OWNER-API_URL")

    def test_config_requires_three_roles_and_no_raw_endpoint(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest = root / "manifest.json"
            manifest.write_text("{}", encoding="utf-8")
            binary = root / "binary"
            binary.write_bytes(b"binary")
            binary_sha = hashlib.sha256(binary.read_bytes()).hexdigest()
            raw = {
                "source_sha": "b" * 40,
                "artifact_manifest": str(manifest),
                "run_parent": str(root),
                "require_share_swarm": True,
                "require_relay": True,
                "roles": {
                    role: {
                        "runner": "local",
                        "binary": str(binary),
                        "binary_sha256": binary_sha,
                    }
                    for role in MODULE.ROLE_NAMES
                },
            }
            config_path = root / "config.json"
            config_path.write_text(json.dumps(raw), encoding="utf-8")
            config = MODULE.load_config(config_path)
            self.assertEqual(config.source_sha, "b" * 40)
            raw["roles"]["owner"]["api_url"] = "https://must-not-be-in-config.example"
            config_path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaises(MODULE.HarnessError):
                MODULE.load_config(config_path)

    def test_phase_errors_are_fixed_classes(self) -> None:
        recorder = MODULE.PhaseRecorder()
        outcome = recorder.run(
            "manifest_verification",
            "controller",
            "test.manifest",
            lambda: MODULE.fail("source_mismatch"),
        )
        self.assertEqual(outcome.status, "failed")
        self.assertEqual(outcome.error_class, "source_mismatch")
        self.assertEqual(recorder.phases[0]["command_id"], "test.manifest")

    def test_evidence_rejects_secret_and_endpoint_values(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            vault = MODULE.SecretVault()
            vault.hold("sensitive-value")
            evidence = MODULE.SafeEvidence(Path(temporary) / "evidence", vault)
            evidence.write_json("safe.json", {"status": "blocked", "source_sha": "c" * 40})
            with self.assertRaises(ValueError):
                evidence.write_json("secret.json", {"status": "failed", "detail": "sensitive-value"})
            with self.assertRaises(ValueError):
                evidence.write_json("url.json", {"status": "failed", "detail": "https://example.invalid"})
            vault.clear()

    def test_child_environment_excludes_qsync_secrets(self) -> None:
        old = os.environ.get("QSYNC_F_TEST_SECRET")
        os.environ["QSYNC_F_TEST_SECRET"] = "sensitive"
        try:
            self.assertNotIn("QSYNC_F_TEST_SECRET", MODULE.clean_child_environment())
        finally:
            if old is None:
                os.environ.pop("QSYNC_F_TEST_SECRET", None)
            else:
                os.environ["QSYNC_F_TEST_SECRET"] = old

    def test_cleanup_retains_started_state_without_drain_ack(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            run_root = Path(temporary) / "run"
            run_root.mkdir(mode=0o700)
            spec = MODULE.RoleSpec(
                "owner",
                "local",
                None,
                "a" * 64,
                None,
                None,
                None,
                "127.0.0.1",
                None,
                None,
                None,
            )
            process = MODULE.LocalWebProcess(spec, run_root, "owner", MODULE.SecretVault())
            process.started_once = True
            self.assertEqual(MODULE.safe_cleanup([process], run_root), (True, False))
            self.assertTrue(run_root.is_dir())
            shutil.rmtree(run_root)

    @unittest.skipUnless(os.name == "posix", "native Windows signal proof uses the dedicated helper")
    def test_stop_proves_signal_and_exit_zero_for_owned_process(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            run_root = Path(temporary) / "run"
            run_root.mkdir(mode=0o700)
            process = MODULE.LocalWebProcess(
                self._process_spec(), run_root, "owner", MODULE.SecretVault()
            )
            ready = run_root / "ready"
            process.process = subprocess.Popen(
                self._signal_child(0, ready),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            process.started_once = True
            try:
                deadline = MODULE.time.monotonic() + 5
                while not ready.exists() and MODULE.time.monotonic() < deadline:
                    self.assertIsNone(process.process.poll())
                    MODULE.time.sleep(0.01)
                self.assertTrue(ready.exists())
                self.assertTrue(process.stop())
                self.assertIsNone(process.process)
                self.assertEqual(process.exit_code, 0)
                self.assertTrue(process.graceful_drain_proven)
                self.assertFalse(process.forced_termination)
            finally:
                if process.process is not None:
                    process.process.kill()
                    process.process.wait(timeout=5)

    @unittest.skipUnless(os.name == "posix", "native Windows signal proof uses the dedicated helper")
    def test_nonzero_owned_process_exit_is_stopped_but_not_graceful(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            run_root = Path(temporary) / "run"
            run_root.mkdir(mode=0o700)
            process = MODULE.LocalWebProcess(
                self._process_spec(), run_root, "owner", MODULE.SecretVault()
            )
            ready = run_root / "ready"
            process.process = subprocess.Popen(
                self._signal_child(7, ready),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            process.started_once = True
            try:
                deadline = MODULE.time.monotonic() + 5
                while not ready.exists() and MODULE.time.monotonic() < deadline:
                    self.assertIsNone(process.process.poll())
                    MODULE.time.sleep(0.01)
                self.assertTrue(ready.exists())
                self.assertTrue(process.stop())
                self.assertIsNone(process.process)
                self.assertEqual(process.exit_code, 7)
                self.assertFalse(process.graceful_drain_proven)
                self.assertFalse(process.forced_termination)
            finally:
                if process.process is not None:
                    process.process.kill()
                    process.process.wait(timeout=5)

    def test_signal_failure_keeps_owned_process_handle_for_reconciliation(self) -> None:
        class RefusedProcess:
            returncode = None

            def poll(self) -> None:
                return None

            def send_signal(self, _signal: object) -> None:
                raise OSError("signal refused")

        with tempfile.TemporaryDirectory() as temporary:
            run_root = Path(temporary) / "run"
            run_root.mkdir(mode=0o700)
            process = MODULE.LocalWebProcess(
                self._process_spec(), run_root, "owner", MODULE.SecretVault()
            )
            owned = RefusedProcess()
            process.process = owned  # type: ignore[assignment]
            process.started_once = True
            self.assertFalse(process.stop())
            self.assertIs(process.process, owned)
            self.assertIsNone(process.exit_code)
            self.assertFalse(process.graceful_drain_proven)
            self.assertFalse(process.forced_termination)

    def test_forced_owned_process_stop_records_non_graceful_exit(self) -> None:
        class SlowProcess:
            returncode = None

            def __init__(self) -> None:
                self.wait_calls = 0
                self.killed = False

            def poll(self) -> int | None:
                return -9 if self.killed else None

            def send_signal(self, _signal: object) -> None:
                return None

            def wait(self, timeout: int) -> int:
                self.wait_calls += 1
                if self.wait_calls == 1:
                    raise subprocess.TimeoutExpired("owned", timeout)
                self.returncode = -9
                return self.returncode

            def kill(self) -> None:
                self.killed = True

        with tempfile.TemporaryDirectory() as temporary:
            run_root = Path(temporary) / "run"
            run_root.mkdir(mode=0o700)
            process = MODULE.LocalWebProcess(
                self._process_spec(), run_root, "owner", MODULE.SecretVault()
            )
            owned = SlowProcess()
            process.process = owned  # type: ignore[assignment]
            process.started_once = True
            self.assertTrue(process.stop())
            self.assertIsNone(process.process)
            self.assertEqual(process.exit_code, -9)
            self.assertFalse(process.graceful_drain_proven)
            self.assertTrue(process.forced_termination)


if __name__ == "__main__":
    unittest.main()
