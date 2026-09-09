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
from unittest import mock


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
    def test_config_rejects_raw_share_key(self) -> None:
        with self.assertRaises(BOOTSTRAP.HarnessError) as error:
            BOOTSTRAP.reject_secret_config({"share_key": "raw-secret"})
        self.assertEqual(error.exception.error_class, "config_invalid")

    def test_manifest_requires_role_target_source_and_workflow(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps(
                    {
                        "status": "pass",
                        "source_sha": "a" * 40,
                        "workflow_sha": "a" * 40,
                        "artifacts": {
                            "owner": {"sha256": "b" * 64, "size_bytes": 1, "source_sha": "a" * 40}
                        },
                    }
                ),
                encoding="utf-8",
            )
            config = BOOTSTRAP.HarnessConfig(
                "a" * 40,
                manifest,
                {},
                False,
                False,
                root,
                None,
                None,
            )
            with self.assertRaises(BOOTSTRAP.HarnessError) as error:
                BOOTSTRAP.verify_manifest(config)
            self.assertEqual(error.exception.error_class, "manifest_invalid")

    def test_role_binary_is_staged_exclusively_before_execution(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "deltaweave"
            content = b"immutable staged binary"
            source.write_bytes(content)
            run_root = root / "run"
            run_root.mkdir(mode=0o700)
            source_sha = "c" * 40
            spec = BOOTSTRAP.RoleSpec(
                "owner",
                "local",
                source,
                digest(source),
                None,
                None,
                None,
                "127.0.0.1",
                None,
                None,
                None,
            )
            artifact = {
                "source_sha": source_sha,
                "workflow_sha": source_sha,
                "target": "linux",
                "sha256": digest(source),
                "size_bytes": len(content),
            }
            staged = BOOTSTRAP.stage_role_binary(spec, artifact, run_root)
            source.write_bytes(b"replacement after validation")
            self.assertEqual(staged.read_bytes(), content)
            self.assertEqual(staged.stat().st_mode & 0o777, 0o700)

    def test_rw_runner_stages_private_copy_and_owns_evidence_directory_creation(self) -> None:
        runner = (ROOT / "scripts" / "qsync_three_host_winrm_keepalive.py").read_text(encoding="utf-8")
        workflow = (ROOT / ".github" / "workflows" / "qsync-three-host-bootstrap.yml").read_text(encoding="utf-8")
        self.assertIn("tempfile.mkdtemp(prefix=\"qsync-f-rw-\"", runner)
        self.assertIn("bootstrap.stage_role_binary", runner)
        self.assertIn("run_owned_copy_removed", runner)
        self.assertIn("keepalive_observed(remote, args.keepalive_seconds)", runner)
        self.assertNotRegex(workflow, r"mkdir[^\n]*qsync-f-rw-evidence")
        self.assertNotRegex(workflow, r"mkdir[^\n]*qsync-f-evidence")

    def test_evidence_bundle_removes_partial_publication_on_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "evidence"
            evidence = BOOTSTRAP.SafeEvidence(directory, BOOTSTRAP.SecretVault())
            with mock.patch.object(BOOTSTRAP.os, "replace", side_effect=[None, OSError("publish")]):
                with self.assertRaises(OSError):
                    evidence.write_bundle(
                        "f-run-manifest.json",
                        {"status": "pass"},
                        "f-phase-events.jsonl",
                        [{"phase": "cleanup", "status": "pass"}],
                    )
            self.assertFalse((directory / "f-run-manifest.json").exists())
            self.assertFalse((directory / "f-phase-events.jsonl").exists())

    def test_remote_contract_requires_all_ordered_phases(self) -> None:
        expected_binary = "d" * 64
        expected_file = "e" * 64
        phases = [
            {"phase": phase, "ok": True, "hash": None, "size": None, "forced": False, "signal": None}
            for phase in BOOTSTRAP.REMOTE_REQUIRED_PHASES
        ]
        phases[0].update(hash=expected_binary, size=123)
        phases[7].update(hash=expected_file, size=256)
        phases[8].update(hash=expected_file, size=256)
        phases[-1]["signal"] = "ctrl_c"
        remote = BOOTSTRAP.RemoteRun(
            phases=phases,
            diagnostic_stages=list(BOOTSTRAP.REMOTE_REOPEN_TRACE_STAGES),
            diagnostic_counts={"reopen_checks": 63},
            graceful_signal=True,
            transport_cleanup_completed=True,
            status_code=0,
        )
        self.assertTrue(BOOTSTRAP.remote_contract_is_complete(remote, expected_binary, 123, expected_file, 256))
        self.assertFalse(
            BOOTSTRAP.remote_contract_is_complete(
                remote, expected_binary, 123, expected_file, 256, require_keepalive=True
            )
        )
        remote.diagnostic_stages.extend(BOOTSTRAP.REMOTE_KEEPALIVE_TRACE_STAGES)
        remote.diagnostic_counts.update({"keepalive_enter": 300, "keepalive_done": 300})
        self.assertTrue(
            BOOTSTRAP.remote_contract_is_complete(
                remote, expected_binary, 123, expected_file, 256, require_keepalive=True
            )
        )
        self.assertFalse(BOOTSTRAP.remote_contract_is_complete(remote, expected_binary, 123, expected_binary, 123))
        remote.phases[8]["size"] = 255
        self.assertFalse(BOOTSTRAP.remote_contract_is_complete(remote, expected_binary, 123, expected_file, 256))
        remote.phases[8]["size"] = 256
        remote.diagnostic_counts["reopen_checks"] = 31
        self.assertFalse(BOOTSTRAP.remote_contract_is_complete(remote, expected_binary, 123, expected_file, 256))
        remote.diagnostic_counts["reopen_checks"] = 63
        remote.transport_error_class = "timeout"
        self.assertFalse(BOOTSTRAP.remote_contract_is_complete(remote, expected_binary, 123, expected_file, 256))
        remote.phases.pop(2)
        remote.transport_error_class = None
        self.assertFalse(BOOTSTRAP.remote_contract_is_complete(remote, expected_binary, 123, expected_file, 256))

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
            "FTRACE|stage=preview_enter|elapsed_ms=17\n"
            "FTRACE|stage=not_allowed|elapsed_ms=19\n"
            f"FROLE|phase=binary_verification|ok=true|hash={expected}\n"
            f"FROLE|phase=file_hash|ok=true|hash={expected}|size=262144\n"
            "FROLE|phase=cleanup|ok=true|signal=ctrl_c\n"
            "FROLE|phase=file_hash|ok=true|hash=not-a-hash\n",
            expected,
        )
        self.assertEqual(remote.file_hash, expected)
        self.assertEqual(remote.file_size, 262144)
        self.assertTrue(remote.graceful_signal)
        self.assertEqual(len(remote.phases), 3)
        self.assertEqual(remote.diagnostic_stages, ["preview_enter"])
        self.assertEqual(remote.diagnostic_elapsed_ms, {"preview_enter": 17})

    def test_remote_parser_keeps_bounded_diagnostic_counts(self) -> None:
        remote = BOOTSTRAP.parse_remote_output(
            "FTRACE|stage=console_test_extra_unknown|count=3|elapsed_ms=22\n"
            "FTRACE|stage=console_test_mismatch|count=5|elapsed_ms=23\n",
            "d" * 64,
        )
        self.assertEqual(remote.diagnostic_counts, {"console_test_extra_unknown": 3, "console_test_mismatch": 5})
        self.assertEqual(remote.diagnostic_elapsed_ms, {"console_test_extra_unknown": 22, "console_test_mismatch": 23})

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
        self.assertIn("AllocConsole", script)
        self.assertIn("GetConsoleProcessList", script)
        self.assertIn("InstallOwnerHandler", script)
        self.assertIn("RemoveOwnerHandler", script)
        self.assertNotIn("[QsyncOwnedConsole+HandlerRoutine]", script)
        self.assertIn("GenerateConsoleCtrlEvent", script)
        self.assertIn("Send-OwnedCtrlC", script)
        self.assertIn("[Console]::Out.WriteLine", script)
        self.assertNotIn("CloseMainWindow", script)
        self.assertNotIn("add_OutputDataReceived", script)
        self.assertNotIn("add_ErrorDataReceived", script)
        self.assertNotIn("BeginOutputReadLine", script)
        self.assertNotIn("BeginErrorReadLine", script)
        self.assertIn("CopyToAsync([IO.Stream]::Null)", script)
        self.assertIn("ProcessStreamTasks", script)
        self.assertIn("$Method -eq 'Post' -and $PSBoundParameters.ContainsKey('Body')", script)
        self.assertIn("'keepalive_enter', 'keepalive_done'", script)
        self.assertIn("expected_permission", script)
        self.assertIn("console_test_extra_trusted", script)
        self.assertIn("LastStopErrorClass", script)
        self.assertIn("for ($attempt = 0; $attempt -lt 20; $attempt++)", script)
        self.assertIn("if ($Process.HasExited) { return $false }", script)
        self.assertIn("if (Test-OwnedConsoleProcess $Process)", script)

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

            def send_command_input(self, shell_id: str, command_id: str, stdin_input: bytes, **options: object) -> None:
                self.calls.append(("send_command_input", (shell_id, command_id, stdin_input, options)))

            def get_command_output_raw(self, shell_id: str, command_id: str) -> tuple[bytes, bytes, int, bool]:
                self.calls.append(("get_command_output_raw", (shell_id, command_id)))
                return b"FROLE|phase=self_test|ok=true\n", b"", 0, True

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
        self.assertEqual(details[2], BOOTSTRAP.WINRM_POWER_SHELL_STDIN_ARGUMENTS)
        self.assertTrue(details[3]["skip_cmd_shell"])
        self.assertTrue(details[3]["console_mode_stdin"])
        input_calls = [call for call in session.protocol.calls if call[0] == "send_command_input"]
        self.assertEqual(len(input_calls), 2)
        self.assertEqual(input_calls[-1][1][2], b"")
        self.assertTrue(input_calls[-1][1][3]["end"])
        self.assertEqual(input_calls[0][1][2], b"Write-Output FROLE")
        self.assertFalse(input_calls[0][1][3]["end"])
        self.assertEqual(
            [call[0] for call in session.protocol.calls],
            [
                "open_shell",
                "run_command",
                "send_command_input",
                "send_command_input",
                "get_command_output_raw",
                "cleanup_command",
                "close_shell",
            ],
        )

        large_session = FakeSession()
        BOOTSTRAP._run_winrm_powershell(
            large_session,
            "x" * (BOOTSTRAP.WINRM_STDIN_CHUNK_BYTES * 2 + 3),
        )
        large_inputs = [call for call in large_session.protocol.calls if call[0] == "send_command_input"]
        self.assertEqual(
            [len(call[1][2]) for call in large_inputs],
            [BOOTSTRAP.WINRM_STDIN_CHUNK_BYTES, BOOTSTRAP.WINRM_STDIN_CHUNK_BYTES, 3, 0],
        )
        self.assertTrue(large_inputs[-1][1][3]["end"])

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
                "expected_permission": "read_write",
            },
        )
        lengths = BOOTSTRAP._winrm_command_lengths(wrapper)
        self.assertGreater(lengths["wrapper_bytes"], 0)
        self.assertGreater(lengths["encoded_command_bytes"], lengths["wrapper_bytes"])
        self.assertGreater(lengths["legacy_run_ps_command_bytes"], BOOTSTRAP.WINRM_CMD_SHELL_LIMIT_BYTES)
        self.assertLess(lengths["direct_skip_cmd_shell_command_bytes"], BOOTSTRAP.WINRM_CMD_SHELL_LIMIT_BYTES)
        self.assertGreater(lengths["direct_stdin_payload_bytes"], BOOTSTRAP.WINRM_STDIN_CHUNK_BYTES)
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
            BOOTSTRAP._run_winrm_powershell(
                NoRunSession(), "x" * (BOOTSTRAP.WINRM_MAX_STDIN_PAYLOAD_BYTES + 1)
            )
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
            "expected_permission": "read_write",
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

    def test_winrm_receive_timeout_keeps_partial_phase_and_bounds_cleanup(self) -> None:
        class TimeoutProtocol:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def open_shell(self) -> str:
                self.calls.append("open_shell")
                return "shell"

            def run_command(self, *_args: object, **_kwargs: object) -> str:
                self.calls.append("run_command")
                return "command"

            def send_command_input(self, *_args: object, **_kwargs: object) -> None:
                self.calls.append("send_command_input")

            def get_command_output_raw(self, *_args: object) -> tuple[bytes, bytes, int, bool]:
                self.calls.append("get_command_output_raw")
                return b"FROLE|phase=binary_verification|ok=true\n", b"", -1, False

            def cleanup_command(self, *_args: object) -> None:
                self.calls.append("cleanup_command")

            def close_shell(self, *_args: object) -> None:
                self.calls.append("close_shell")

        class Session:
            def __init__(self) -> None:
                self.protocol = TimeoutProtocol()

        session = Session()
        with mock.patch.object(BOOTSTRAP.time, "monotonic", side_effect=[0] * 6 + [1000] * 10):
            result = BOOTSTRAP._run_winrm_powershell(session, "Write-Output FROLE")
        self.assertTrue(result.command_timed_out)
        self.assertTrue(result.transport_cleanup_completed)
        self.assertEqual(result.status_code, -1)
        self.assertEqual(result.receive_poll_count, 1)
        self.assertEqual(result.error_class, "timeout")
        self.assertGreater(result.output_bytes, 0)
        self.assertIn(b"binary_verification", result.std_out)
        self.assertEqual(
            session.protocol.calls,
            [
                "open_shell",
                "run_command",
                "send_command_input",
                "send_command_input",
                "get_command_output_raw",
                "cleanup_command",
                "close_shell",
            ],
        )

    def test_winrm_setup_failure_is_sanitized_without_dropping_transport_result(self) -> None:
        class SetupFailureProtocol:
            operation_timeout_sec = 45
            read_timeout_sec = 60

            def get_command_output_raw(self, *_args: object) -> tuple[bytes, bytes, int, bool]:
                raise AssertionError("setup failure must stop before receive")

            def open_shell(self) -> str:
                raise OSError("transport detail must not escape")

        protocol = SetupFailureProtocol()
        result = BOOTSTRAP._run_winrm_powershell(type("Session", (), {"protocol": protocol})(), "Write-Output FROLE")
        self.assertEqual(result.status_code, -1)
        self.assertEqual(result.error_class, "external_unavailable")
        self.assertEqual(result.output_bytes, 0)
        self.assertTrue(result.transport_cleanup_completed)
        self.assertEqual(protocol.operation_timeout_sec, 45)
        self.assertEqual(protocol.read_timeout_sec, 60)

    def test_winrm_receive_fault_preserves_partial_output_and_fixed_error(self) -> None:
        class PartialFaultProtocol:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def open_shell(self) -> str:
                self.calls.append("open_shell")
                return "shell"

            def run_command(self, *_args: object, **_kwargs: object) -> str:
                self.calls.append("run_command")
                return "command"

            def send_command_input(self, *_args: object, **_kwargs: object) -> None:
                self.calls.append("send_command_input")

            def get_command_output_raw(self, *_args: object) -> tuple[bytes, bytes, int, bool]:
                self.calls.append("get_command_output_raw")
                if self.calls.count("get_command_output_raw") == 1:
                    return b"FROLE|phase=binary_verification|ok=true\n", b"", 0, False
                raise RuntimeError("untrusted transport detail")

            def cleanup_command(self, *_args: object) -> None:
                self.calls.append("cleanup_command")

            def close_shell(self, *_args: object) -> None:
                self.calls.append("close_shell")

        protocol = PartialFaultProtocol()
        result = BOOTSTRAP._run_winrm_powershell(type("Session", (), {"protocol": protocol})(), "Write-Output FROLE")
        self.assertEqual(result.status_code, -1)
        self.assertEqual(result.error_class, "remote_failure")
        self.assertGreater(result.output_bytes, 0)
        self.assertTrue(result.transport_cleanup_completed)
        parsed = BOOTSTRAP.parse_remote_output(result.std_out, "0" * 64)
        self.assertEqual([phase["phase"] for phase in parsed.phases], ["binary_verification"])

    def test_winrm_cleanup_timeout_is_pending_and_restores_transport_settings(self) -> None:
        class Transport:
            read_timeout_sec = 60

        class CleanupTimeoutProtocol:
            operation_timeout_sec = 45
            read_timeout_sec = 60

            def __init__(self) -> None:
                self.transport = Transport()

            def open_shell(self) -> str:
                return "shell"

            def run_command(self, *_args: object, **_kwargs: object) -> str:
                return "command"

            def send_command_input(self, *_args: object, **_kwargs: object) -> None:
                return None

            def get_command_output_raw(self, *_args: object) -> tuple[bytes, bytes, int, bool]:
                return b"done", b"", 0, True

            def cleanup_command(self, *_args: object) -> None:
                raise TimeoutError("cleanup detail must not escape")

            def close_shell(self, *_args: object) -> None:
                return None

        protocol = CleanupTimeoutProtocol()
        result = BOOTSTRAP._run_winrm_powershell(type("Session", (), {"protocol": protocol})(), "Write-Output done")
        self.assertFalse(result.command_timed_out)
        self.assertFalse(result.transport_cleanup_completed)
        self.assertEqual(result.error_class, "cleanup_incomplete")
        self.assertEqual(protocol.operation_timeout_sec, 45)
        self.assertEqual(protocol.read_timeout_sec, 60)
        self.assertEqual(protocol.transport.read_timeout_sec, 60)

    def test_winrm_completed_output_reports_cleanup_failure(self) -> None:
        class CleanupFailureProtocol:
            def open_shell(self) -> str:
                return "shell"

            def run_command(self, *_args: object, **_kwargs: object) -> str:
                return "command"

            def send_command_input(self, *_args: object, **_kwargs: object) -> None:
                return None

            def get_command_output_raw(self, *_args: object) -> tuple[bytes, bytes, int, bool]:
                return b"done", b"", 0, True

            def cleanup_command(self, *_args: object) -> None:
                raise OSError("cleanup")

            def close_shell(self, *_args: object) -> None:
                return None

        result = BOOTSTRAP._run_winrm_powershell(
            type("Session", (), {"protocol": CleanupFailureProtocol()})(), "Write-Output done"
        )
        self.assertFalse(result.command_timed_out)
        self.assertFalse(result.transport_cleanup_completed)
        self.assertEqual(result.status_code, 0)


if __name__ == "__main__":
    unittest.main()
