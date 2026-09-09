from __future__ import annotations

import base64
import gzip
import hashlib
import importlib.util
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).parents[2]
BOOTSTRAP_PATH = ROOT / "scripts" / "qsync_three_host_bootstrap.py"
SPEC = importlib.util.spec_from_file_location("qsync_three_host_bootstrap_manifest_test", BOOTSTRAP_PATH)
assert SPEC is not None and SPEC.loader is not None
BOOTSTRAP = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = BOOTSTRAP
SPEC.loader.exec_module(BOOTSTRAP)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class QsyncRoleManifestTests(unittest.TestCase):
    def test_platform_hashes_may_differ_when_source_matches(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            windows = root / "deltaweave.exe"
            linux = root / "deltaweave"
            windows.write_bytes(b"windows-platform-binary")
            linux.write_bytes(b"linux-platform-binary")
            source = "a" * 40
            native_manifest = root / "native.json"
            native_manifest.write_text(
                json.dumps(
                    {
                        "status": "pass",
                        "source_sha": source,
                        "workflow_sha": source,
                        "artifact": {"sha256": digest(windows), "size_bytes": windows.stat().st_size},
                    }
                ),
                encoding="utf-8",
            )
            output = root / "role.json"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts/qsync_make_role_manifest.py"),
                    "--source-sha",
                    source,
                    "--windows-manifest",
                    str(native_manifest),
                    "--windows-binary",
                    str(windows),
                    "--linux-binary",
                    str(linux),
                    "--output",
                    str(output),
                ],
                cwd=ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0)
            manifest = json.loads(output.read_text(encoding="utf-8"))
            self.assertNotEqual(manifest["artifacts"]["owner"]["sha256"], manifest["artifacts"]["rw_provider"]["sha256"])
            self.assertEqual(manifest["artifacts"]["owner"]["source_sha"], source)
            self.assertEqual(manifest["artifacts"]["rw_provider"]["source_sha"], source)

    def test_role_manifest_rejects_wrong_native_source(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            windows = root / "deltaweave.exe"
            linux = root / "deltaweave"
            windows.write_bytes(b"windows")
            linux.write_bytes(b"linux")
            native_manifest = root / "native.json"
            native_manifest.write_text(
                json.dumps(
                    {
                        "status": "pass",
                        "source_sha": "b" * 40,
                        "workflow_sha": "b" * 40,
                        "artifact": {"sha256": digest(windows), "size_bytes": windows.stat().st_size},
                    }
                ),
                encoding="utf-8",
            )
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts/qsync_make_role_manifest.py"),
                    "--source-sha",
                    "c" * 40,
                    "--windows-manifest",
                    str(native_manifest),
                    "--windows-binary",
                    str(windows),
                    "--linux-binary",
                    str(linux),
                    "--output",
                    str(root / "role.json"),
                ],
                cwd=ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("source_mismatch", completed.stdout)

    def test_role_binary_rejects_cross_platform_descriptor(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            binary = Path(temporary) / "deltaweave"
            binary.write_bytes(b"linux-platform-binary")
            spec = BOOTSTRAP.RoleSpec(
                "owner",
                "local",
                binary,
                digest(binary),
                None,
                None,
                None,
                "127.0.0.1",
                None,
                None,
                None,
            )
            with self.assertRaises(BOOTSTRAP.HarnessError) as error:
                BOOTSTRAP.verify_role_binary(
                    spec,
                    {"sha256": digest(binary), "size_bytes": binary.stat().st_size, "target": "windows"},
                )
            self.assertEqual(error.exception.error_class, "source_mismatch")

    def test_remote_parser_keeps_hash_size_and_discards_untrusted_lines(self) -> None:
        expected = "d" * 64
        remote = BOOTSTRAP.parse_remote_output(
            "noise with a path\n"
            f"FROLE|phase=binary_verification|ok=true|hash={expected}\n"
            f"FROLE|phase=file_hash|ok=true|hash={expected}|size=262144\n"
            "FROLE|phase=file_hash|ok=true|hash=not-a-hash\n",
            expected,
        )
        self.assertEqual(remote.file_hash, expected)
        self.assertEqual(remote.file_size, 262144)
        self.assertEqual(len(remote.phases), 2)

    def test_child_environment_does_not_inherit_unrelated_credentials(self) -> None:
        old_token = os.environ.get("ADMIN_TOKEN")
        old_winrm = os.environ.get("WINRM_PASSWORD")
        os.environ["ADMIN_TOKEN"] = "secret"
        os.environ["WINRM_PASSWORD"] = "secret"
        try:
            child = BOOTSTRAP.clean_child_environment({"HOME": "/private/home"})
            self.assertNotIn("ADMIN_TOKEN", child)
            self.assertNotIn("WINRM_PASSWORD", child)
            self.assertEqual(child["HOME"], "/private/home")
        finally:
            if old_token is None:
                os.environ.pop("ADMIN_TOKEN", None)
            else:
                os.environ["ADMIN_TOKEN"] = old_token
            if old_winrm is None:
                os.environ.pop("WINRM_PASSWORD", None)
            else:
                os.environ["WINRM_PASSWORD"] = old_winrm

    def test_windows_destination_is_opaque(self) -> None:
        spec = BOOTSTRAP.RoleSpec(
            "rw_provider",
            "winrm",
            None,
            "e" * 64,
            None,
            None,
            "QSYNC_F_WINRM_DESTINATION",
            "127.0.0.1",
            "QSYNC_F_WINRM_HOST",
            "QSYNC_F_WINRM_USERNAME",
            "QSYNC_F_WINRM_PASSWORD",
        )
        old = os.environ.get("QSYNC_F_WINRM_DESTINATION")
        os.environ["QSYNC_F_WINRM_DESTINATION"] = r"C:\DeltaWeave-QSync-F-opaque\member-files"
        try:
            destination = BOOTSTRAP.role_destination(None, spec, BOOTSTRAP.SecretVault())
            self.assertIsInstance(destination, str)
            self.assertEqual(destination, r"C:\DeltaWeave-QSync-F-opaque\member-files")
            fresh = BOOTSTRAP.fresh_winrm_destination(spec)
            self.assertRegex(fresh, r"^C:\\DeltaWeave-QSync-F-[0-9a-f]{32}\\member-files$")
            self.assertNotEqual(fresh, destination)
        finally:
            if old is None:
                os.environ.pop("QSYNC_F_WINRM_DESTINATION", None)
            else:
                os.environ["QSYNC_F_WINRM_DESTINATION"] = old

    def test_windows_driver_redacts_child_streams_and_retains_uncertain_state(self) -> None:
        script = (ROOT / "scripts" / "qsync_three_host_winrm_member.ps1").read_text(encoding="utf-8")
        self.assertNotRegex(script, r"\$home\s*=")
        self.assertIn("$info.RedirectStandardOutput = $true", script)
        self.assertIn("$info.RedirectStandardError = $true", script)
        self.assertIn("if ($stopped) { $script:MemberProcess = $null }", script)
        self.assertIn("$script:GracefulDrainProven", script)
        self.assertIn("$script:GracefulDrainProven -and -not $script:ForcedTermination", script)

    def test_winrm_direct_protocol_bypasses_command_shell_and_closes_handles(self) -> None:
        class FakeProtocol:
            def __init__(self) -> None:
                self.calls: list[tuple[str, object]] = []

            def open_shell(self) -> str:
                self.calls.append(("open_shell", None))
                return "shell"

            def run_command(self, shell_id: str, command: str, arguments: object, **options: object) -> str:
                self.calls.append(("run_command", (shell_id, command, arguments, options)))
                return "command"

            def get_command_output(self, shell_id: str, command_id: str) -> tuple[bytes, bytes, int]:
                self.calls.append(("get_command_output", (shell_id, command_id)))
                return b"FROLE|phase=self_test|ok=true\n", b"", 0

            def cleanup_command(self, shell_id: str, command_id: str) -> None:
                self.calls.append(("cleanup_command", (shell_id, command_id)))

            def close_shell(self, shell_id: str) -> None:
                self.calls.append(("close_shell", shell_id))

        class FakeSession:
            def __init__(self) -> None:
                self.protocol = FakeProtocol()

            def run_ps(self, _command: str) -> None:
                raise AssertionError("Session.run_ps must not be used")

        session = FakeSession()
        result = BOOTSTRAP._run_winrm_powershell(session, "Write-Output FROLE")
        self.assertEqual(result.status_code, 0)
        self.assertEqual(result.std_err, b"")
        run_call = next(call for call in session.protocol.calls if call[0] == "run_command")
        _, details = run_call
        assert isinstance(details, tuple)
        self.assertEqual(details[1], "powershell.exe")
        self.assertIn("-EncodedCommand", details[2])
        self.assertTrue(details[3]["skip_cmd_shell"])
        self.assertFalse(details[3]["console_mode_stdin"])
        self.assertEqual(
            [call[0] for call in session.protocol.calls],
            ["open_shell", "run_command", "get_command_output", "cleanup_command", "close_shell"],
        )

    def test_winrm_command_length_measurement_is_numeric_and_bounded(self) -> None:
        script = (ROOT / "scripts" / "qsync_three_host_winrm_member.ps1").read_text(encoding="utf-8")
        wrapper = BOOTSTRAP._winrm_wrapper(
            script,
            {
                "artifact_url": "https://example.invalid/a",
                "artifact_sha256": "a" * 64,
                "artifact_size": "33190912",
                "owner_base_uri": "https://owner.invalid",
                "share_key": "b" * 256,
                "destination_root": r"C:\DeltaWeave-QSync-F-opaque\member-files",
                "expected_file_hash": "c" * 64,
                "expected_file_name": "fixture-a.bin",
            },
        )
        lengths = BOOTSTRAP._winrm_command_lengths(wrapper)
        self.assertGreater(lengths["wrapper_bytes"], 0)
        self.assertGreater(lengths["encoded_command_bytes"], lengths["wrapper_bytes"])
        self.assertGreater(lengths["legacy_run_ps_command_bytes"], BOOTSTRAP.WINRM_CMD_SHELL_LIMIT_BYTES)
        self.assertGreater(lengths["direct_skip_cmd_shell_command_bytes"], BOOTSTRAP.WINRM_CMD_SHELL_LIMIT_BYTES)
        self.assertLessEqual(lengths["encoded_command_bytes"], BOOTSTRAP.WINRM_MAX_ENCODED_COMMAND_BYTES)
        self.assertLessEqual(
            lengths["direct_skip_cmd_shell_command_bytes"], BOOTSTRAP.WINRM_DIRECT_COMMAND_LIMIT_BYTES
        )

        class NoRunProtocol:
            def open_shell(self) -> None:
                raise AssertionError("oversized command must fail before opening WinRS")

        class NoRunSession:
            protocol = NoRunProtocol()

        with self.assertRaises(BOOTSTRAP.HarnessError) as error:
            BOOTSTRAP._run_winrm_powershell(NoRunSession(), "x" * 20000)
        self.assertEqual(error.exception.error_class, "api_response_invalid")

    def test_winrm_config_payload_matches_gzip_decoder_contract(self) -> None:
        script = (ROOT / "scripts" / "qsync_three_host_winrm_member.ps1").read_text(encoding="utf-8")
        config = {
            "artifact_url": "https://example.invalid/a",
            "artifact_sha256": "a" * 64,
            "artifact_size": "33190912",
            "owner_base_uri": "https://owner.invalid",
            "share_key": "b" * 64,
            "destination_root": r"C:\DeltaWeave-QSync-F-opaque\member-files",
            "expected_file_hash": "c" * 64,
            "expected_file_name": "fixture-a.bin",
        }
        wrapper = BOOTSTRAP._winrm_wrapper(script, config)
        encoded_match = re.search(r"-ConfigB64 '([A-Za-z0-9+/=]+)'$", wrapper)
        self.assertIsNotNone(encoded_match)
        assert encoded_match is not None
        decoded = json.loads(gzip.decompress(base64.b64decode(encoded_match.group(1))).decode("utf-8"))
        self.assertEqual(decoded, config)
        decoder = script[script.index("function Decode-Config") : script.index("function Assert-Config")]
        self.assertIn("GzipStream", decoder)
        self.assertIn("CompressionMode]::Decompress", decoder)


if __name__ == "__main__":
    unittest.main()
