from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[2] / "scripts" / "qsync_three_host_bootstrap.py"
SPEC = importlib.util.spec_from_file_location("qsync_three_host_bootstrap", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class QsyncBootstrapTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
