from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import sys
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Callable
from unittest import mock


ROOT = Path(__file__).parents[2]
SCRIPT = ROOT / "scripts" / "qsync_three_host_controller.py"
SPEC = importlib.util.spec_from_file_location("qsync_three_host_controller", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
CONTROLLER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = CONTROLLER
SPEC.loader.exec_module(CONTROLLER)


class FakeActions:
    def __init__(self, runs: list[dict[str, object]] | None = None) -> None:
        self.runs = runs or []
        self.dispatches: list[dict[str, object]] = []
        self.downloads: list[tuple[str, str, Path]] = []
        self.secrets: dict[str, str] = {}
        self.ro_evidence: dict[str, object] | None = None
        self.hosted_job_calls: list[tuple[str, str]] = []
        self.on_hosted_job: Callable[[], None] | None = None

    def dispatch(self, workflow: str, ref: str, inputs: dict[str, str]) -> None:
        self.dispatches.append({"workflow": workflow, "ref": ref, "inputs": dict(inputs)})

    def list_runs(self, workflow: str, ref: str) -> list[dict[str, object]]:
        # The dispatch is the boundary that makes the fixture run visible;
        # this models the controller's pre-dispatch ID snapshot.
        return list(self.runs) if self.dispatches else []

    def wait_run(
        self,
        workflow: str,
        ref: str,
        source_sha: str,
        not_before: float,
        *,
        excluded_run_ids: frozenset[str] = frozenset(),
        coordination_run_id: str | None = None,
        timeout_seconds: int = 900,
    ) -> dict[str, object]:
        matching = [
            run
            for run in self.runs
            if run.get("headSha") == source_sha and run.get("event") == "workflow_dispatch"
            and str(run.get("databaseId")) not in excluded_run_ids
        ]
        if coordination_run_id is not None:
            matching = [
                run
                for run in matching
                if coordination_run_id in str(run.get("displayTitle", ""))
            ]
        if not matching:
            raise AssertionError("fake run not found")
        return matching[0]

    def wait_hosted_job(
        self,
        workflow: str,
        ref: str,
        source_sha: str,
        not_before: float,
        *,
        excluded_run_ids: frozenset[str] = frozenset(),
        coordination_run_id: str | None = None,
        timeout_seconds: int = 900,
    ) -> dict[str, object]:
        matching = [
            run
            for run in self.runs
            if run.get("headSha") == source_sha
            and run.get("event") == "workflow_dispatch"
            and str(run.get("databaseId")) not in excluded_run_ids
        ]
        if coordination_run_id is not None:
            matching = [
                run
                for run in matching
                if coordination_run_id in str(run.get("displayTitle", ""))
            ]
        if not matching:
            raise AssertionError("fake hosted run not found")
        run = matching[0]
        self.hosted_job_calls.append((str(run["databaseId"]), CONTROLLER.HOSTED_RO_JOB_NAME))
        if self.on_hosted_job is not None:
            self.on_hosted_job()
        return {
            "run": run,
            "job": {
                "name": CONTROLLER.HOSTED_RO_JOB_NAME,
                "status": "completed",
                "conclusion": "success",
            },
        }

    def download_artifact(self, run_id: str, name: str, destination: Path) -> Path:
        self.downloads.append((run_id, name, destination))
        destination.mkdir(mode=0o700, parents=False)
        if name.startswith(CONTROLLER.RO_EVIDENCE_PREFIX) and self.ro_evidence is not None:
            (destination / "f-ro-result.json").write_text(
                json.dumps(self.ro_evidence), encoding="utf-8"
            )
        return destination

    def list_secret_names(self) -> set[str]:
        return set(self.secrets)

    def set_secret(self, name: str, value: str) -> None:
        if name in self.secrets:
            raise AssertionError("fake secret overwrite")
        self.secrets[name] = value

    def delete_secret(self, name: str) -> None:
        self.secrets.pop(name, None)


class ControllerContractTests(unittest.TestCase):
    def test_prepare_dispatches_prepare_inputs_and_persists_role_provenance(self) -> None:
        source = "a" * 40
        file_hash = "b" * 64
        run = {
            "databaseId": 12345,
            "headSha": source,
            "status": "completed",
            "conclusion": "success",
            "event": "workflow_dispatch",
            "createdAt": "2026-09-09T10:00:01Z",
            "displayTitle": "qsync-777",
        }
        actions = FakeActions([run])
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            controller = CONTROLLER.LocalController(actions, parent, repo="owner/repo", ref="feature")
            with mock.patch.object(controller, "_download_and_verify_prepare_artifacts") as verify:
                verify.return_value = {
                    "owner": {"target": "linux", "sha256": "c" * 64, "size_bytes": 10},
                    "rw_provider": {"target": "windows", "sha256": "d" * 64, "size_bytes": 11},
                    "ro_consumer": {"target": "linux", "sha256": "c" * 64, "size_bytes": 10},
                }
                requested_state = parent / "prepared-run" / CONTROLLER.STATE_FILENAME
                state_path = controller.prepare(
                    source,
                    file_hash,
                    8 * 1024 * 1024,
                    state_path=requested_state,
                    dispatch_utc=0.0,
                    coordination_run_id="777",
                ).state_path
            self.assertEqual(state_path, requested_state)
            self.assertEqual(actions.dispatches[0]["workflow"], "ci.yml")
            self.assertEqual(actions.dispatches[0]["inputs"]["qsync_three_host"], "true")
            self.assertEqual(actions.dispatches[0]["inputs"]["qsync_three_host_execute"], "false")
            self.assertEqual(actions.dispatches[0]["inputs"]["qsync_three_host_expected_file_size"], "8388608")
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(state["prepare_run_id"], "12345")
            self.assertEqual(state["prebuilt_role_run_id"], "12345")
            self.assertEqual(state["expected_file_size_bytes"], 8 * 1024 * 1024)
            self.assertNotIn("https://", state_path.read_text(encoding="utf-8"))
            verify.assert_called_once()

    def test_prepare_rejects_existing_explicit_state_parent_before_dispatch(self) -> None:
        actions = FakeActions()
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            (parent / "existing").mkdir()
            controller = CONTROLLER.LocalController(actions, parent, repo="owner/repo", ref="feature")
            with self.assertRaises(CONTROLLER.ControllerError) as error:
                controller.prepare(
                    "a" * 40,
                    "b" * 64,
                    8 * 1024 * 1024,
                    state_path=parent / "existing" / CONTROLLER.STATE_FILENAME,
                )
            self.assertEqual(error.exception.error_class, "path_invalid")
            self.assertEqual(actions.dispatches, [])

    def test_exact_run_selection_rejects_wrong_source_and_non_dispatch_event(self) -> None:
        source = "e" * 40
        actions = FakeActions(
            [
                {"databaseId": 1, "headSha": source, "status": "completed", "conclusion": "success", "event": "push"},
                {"databaseId": 2, "headSha": "f" * 40, "status": "completed", "conclusion": "success", "event": "workflow_dispatch"},
            ]
        )
        with self.assertRaises(CONTROLLER.ControllerError) as error:
            CONTROLLER.select_dispatched_run(actions.list_runs("ci.yml", "feature"), source, 0.0)
        self.assertEqual(error.exception.error_class, "workflow_run_missing")

    def test_secret_lease_never_overwrites_and_deletes_only_owned_name(self) -> None:
        actions = FakeActions()
        lease = CONTROLLER.SecretLease(actions, "QSYNC_F_RO_UNIQUE_1")
        lease.create("one-use-value")
        self.assertEqual(actions.secrets, {"QSYNC_F_RO_UNIQUE_1": "one-use-value"})
        with self.assertRaises(CONTROLLER.ControllerError) as error:
            CONTROLLER.SecretLease(actions, "QSYNC_F_RO_UNIQUE_1").create("replacement")
        self.assertEqual(error.exception.error_class, "secret_exists")
        lease.release()
        self.assertEqual(actions.secrets, {})

    def test_cleanup_preserves_state_when_remote_or_secret_cleanup_is_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "run"
            root.mkdir(mode=0o700)
            marker = root / ".qsync-controller-owned"
            marker.write_text("owned", encoding="ascii")
            result = CONTROLLER.cleanup_owned_run(root, remote_stopped=False, secret_released=False)
            self.assertEqual(result, {"owned_paths_removed": False, "state_retained": True})
            self.assertTrue(root.exists())

    def test_state_refresh_reports_unreadable_snapshot_without_promoting_fallback(self) -> None:
        fallback = {"status": "pending", "source_sha": "a" * 40}
        with tempfile.TemporaryDirectory() as temporary:
            state_path = Path(temporary) / CONTROLLER.STATE_FILENAME
            with mock.patch.object(
                CONTROLLER,
                "load_state",
                side_effect=CONTROLLER.ControllerError("state_invalid"),
            ):
                snapshot, refresh_failed = CONTROLLER._load_state_or(state_path, fallback)
        self.assertEqual(snapshot, fallback)
        self.assertTrue(refresh_failed)

    def test_ready_file_rejects_url_or_secret_bearing_external_state(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "rw-ready.json"
            path.write_text(json.dumps({"status": "ready", "owner_url": "https://private"}), encoding="utf-8")
            self.assertIsNone(CONTROLLER._ready_file(path))

    def test_fixture_stream_is_8mib_and_nonperiodic(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "fixture.bin"
            digest = CONTROLLER.write_fixture(path)
            self.assertEqual(path.stat().st_size, 8 * 1024 * 1024)
            self.assertEqual(digest, hashlib.sha256(path.read_bytes()).hexdigest())
            self.assertEqual(len(CONTROLLER.fixture_chunk(0)), CONTROLLER.FIXTURE_CHUNK_BYTES)
            self.assertNotEqual(CONTROLLER.fixture_chunk(0), CONTROLLER.fixture_chunk(1))
            self.assertNotEqual(path.read_bytes()[:65536], path.read_bytes()[65536:131072])

    def test_run_selection_excludes_pre_dispatch_run_and_matches_coordination_marker(self) -> None:
        source = "a" * 40
        rows = [
            {
                "databaseId": 100,
                "headSha": source,
                "status": "completed",
                "conclusion": "success",
                "event": "workflow_dispatch",
                "createdAt": "2026-09-09T10:00:01Z",
                "displayTitle": "qsync-777",
            },
            {
                "databaseId": 101,
                "headSha": source,
                "status": "completed",
                "conclusion": "success",
                "event": "workflow_dispatch",
                "createdAt": "2026-09-09T10:00:02Z",
                "displayTitle": "qsync-888",
            },
            {
                "databaseId": 102,
                "headSha": source,
                "status": "completed",
                "conclusion": "success",
                "event": "workflow_dispatch",
                "createdAt": "2026-09-09T10:00:03Z",
                "displayTitle": "CI",
                "name": "qsync-999",
            },
        ]
        selected = CONTROLLER.select_dispatched_run(
            rows,
            source,
            1_000_000_000,
            excluded_run_ids=frozenset({"100"}),
            coordination_run_id="888",
        )
        self.assertEqual(selected["databaseId"], "101")
        selected_by_name = CONTROLLER.select_dispatched_run(
            rows,
            source,
            1_000_000_000,
            excluded_run_ids=frozenset({"100", "101"}),
            coordination_run_id="999",
        )
        self.assertEqual(selected_by_name["databaseId"], "102")
        with self.assertRaises(CONTROLLER.ControllerError) as error:
            CONTROLLER.select_dispatched_run(
                rows,
                source,
                1_000_000_000,
                excluded_run_ids=frozenset({"100", "101", "102"}),
                coordination_run_id="888",
            )
        self.assertEqual(error.exception.error_class, "workflow_run_missing")

    def test_gh_secret_adapter_reads_value_from_stdin_without_unsafe_flags(self) -> None:
        calls: list[tuple[list[str], bytes | None]] = []

        def command(argv: list[str], *, input_data: bytes | None, timeout: float) -> subprocess.CompletedProcess[bytes]:
            calls.append((argv, input_data))
            return subprocess.CompletedProcess(argv, 0, b"", b"")

        actions = CONTROLLER.GhActions("owner/repo", command=command)
        actions.set_secret("QSYNC_F_RO_UNIQUE_2", "one-use-value")
        actions.delete_secret("QSYNC_F_RO_UNIQUE_2")
        set_argv, set_input = calls[0]
        delete_argv, delete_input = calls[1]
        self.assertNotIn("--body", set_argv)
        self.assertEqual(set_input, b"one-use-value")
        self.assertNotIn("--yes", delete_argv)
        self.assertIn("QSYNC_F_RO_UNIQUE_2", delete_argv)
        self.assertIn(delete_input, (None, b"y\n"))

    def test_gh_run_listing_requests_display_title_for_exact_correlation(self) -> None:
        calls: list[list[str]] = []

        def command(argv: list[str], *, input_data: bytes | None, timeout: float) -> subprocess.CompletedProcess[bytes]:
            calls.append(argv)
            return subprocess.CompletedProcess(argv, 0, b"[]", b"")

        CONTROLLER.GhActions("owner/repo", command=command).list_runs("ci.yml", "feature")
        self.assertTrue(any("displayTitle,name" in argument for argument in calls[0]))

    def test_gh_wait_hosted_job_pins_run_and_does_not_wait_whole_workflow(self) -> None:
        source = "a" * 40
        calls: list[list[str]] = []
        run_rows = [
            {
                "databaseId": 777,
                "headSha": source,
                "status": "in_progress",
                "conclusion": None,
                "event": "workflow_dispatch",
                "createdAt": "2026-09-09T10:00:01Z",
                "displayTitle": "DeltaWeave CI qsync-888",
            }
        ]
        job_payloads = iter(
            [
                {"jobs": [{"name": CONTROLLER.HOSTED_RO_JOB_NAME, "status": "in_progress", "conclusion": None}]},
                {"jobs": [{"name": CONTROLLER.HOSTED_RO_JOB_NAME, "status": "completed", "conclusion": "success"}]},
            ]
        )

        def command(argv: list[str], *, input_data: bytes | None, timeout: float) -> subprocess.CompletedProcess[bytes]:
            calls.append(argv)
            if argv[1:3] == ["run", "list"]:
                return subprocess.CompletedProcess(argv, 0, json.dumps(run_rows).encode(), b"")
            if argv[1:3] == ["run", "view"]:
                return subprocess.CompletedProcess(argv, 0, json.dumps(next(job_payloads)).encode(), b"")
            raise AssertionError(f"unexpected gh command shape: {argv[:3]}")

        with mock.patch.object(CONTROLLER.time, "sleep", return_value=None):
            hosted = CONTROLLER.GhActions("owner/repo", command=command).wait_hosted_job(
                "ci.yml",
                "feature",
                source,
                0.0,
                coordination_run_id="888",
                timeout_seconds=2,
            )
        self.assertEqual(hosted["run"]["databaseId"], "777")
        self.assertEqual(hosted["job"]["name"], CONTROLLER.HOSTED_RO_JOB_NAME)
        view_commands = [argv for argv in calls if argv[1:3] == ["run", "view"]]
        self.assertEqual(len(view_commands), 2)
        self.assertIn("jobs", view_commands[0])
        self.assertNotIn("--watch", view_commands[0])
        self.assertEqual(view_commands[0][view_commands[0].index("--json") + 1], "jobs")
        self.assertTrue(all(argv[3] == "777" for argv in view_commands))

    def test_live_readiness_requires_membership_file_hash_and_keepalive_trace(self) -> None:
        expected_hash = "b" * 64
        observer = CONTROLLER.RemoteReadinessObserver(expected_hash, 8 * 1024 * 1024, 300)
        observer.feed(
            b"FROLE|phase=member_join|ok=true\n"
            + f"FROLE|phase=file_hash|ok=true|hash={expected_hash}|size=8388608\n".encode()
        )
        self.assertFalse(observer.wait_ready(0))
        observer.feed(b"FTRACE|stage=keepalive_enter|count=300\n")
        self.assertTrue(observer.wait_ready(0))
        attestation = observer.attestation("a" * 40, "777")
        self.assertEqual(attestation["status"], "ready")
        self.assertTrue(attestation["live_handle_observed"])
        self.assertTrue(attestation["keepalive_observed"])
        self.assertEqual(attestation["expected_file_size_bytes"], 8 * 1024 * 1024)

    def test_live_readiness_rejects_reported_remote_error_even_after_late_success_lines(self) -> None:
        expected_hash = "b" * 64
        observer = CONTROLLER.RemoteReadinessObserver(expected_hash, 8 * 1024 * 1024, 2)
        observer.feed(
            b"FROLE|phase=member_join|ok=false|error_class=auth_failed\n"
            b"FROLE|phase=member_join|ok=true\n"
            + f"FROLE|phase=file_hash|ok=true|hash={expected_hash}|size=8388608\n".encode()
            + b"FTRACE|stage=keepalive_enter|count=2\n"
        )
        self.assertFalse(observer.wait_ready(0))
        self.assertEqual(observer.error_class, "remote_failure")
        with self.assertRaises(CONTROLLER.ControllerError) as error:
            observer.attestation("a" * 40, "777")
        self.assertEqual(error.exception.error_class, "rw_not_ready")

    def test_supervisor_attestation_requires_the_runner_to_still_be_alive(self) -> None:
        expected_hash = "b" * 64
        observer = CONTROLLER.RemoteReadinessObserver(expected_hash, 8 * 1024 * 1024, 1)

        def runner(*, on_stdout: Callable[[bytes], None]) -> CONTROLLER.bootstrap.RemoteRun:
            on_stdout(
                b"FROLE|phase=member_join|ok=true\n"
                + f"FROLE|phase=file_hash|ok=true|hash={expected_hash}|size=8388608\n".encode()
                + b"FTRACE|stage=keepalive_enter|count=1\n"
            )
            return CONTROLLER.bootstrap.RemoteRun(status_code=0)

        supervisor = CONTROLLER.RwSupervisor(runner, observer)
        supervisor.start()
        self.assertTrue(supervisor.wait_ready(1))
        self.assertIsNotNone(supervisor.wait_finished(1))
        with self.assertRaises(CONTROLLER.ControllerError) as error:
            supervisor.attestation("a" * 40, "777")
        self.assertEqual(error.exception.error_class, "rw_not_ready")

    def test_prepared_role_binary_is_bound_to_state_manifest_and_no_symlink(self) -> None:
        source = "a" * 40
        owner_hash = "c" * 64
        with tempfile.TemporaryDirectory() as temporary:
            run_root = Path(temporary) / "run"
            run_root.mkdir(mode=0o700)
            (run_root / CONTROLLER.OWNED_MARKER).write_text("owned", encoding="ascii")
            binary = run_root / "artifacts" / "role-artifact" / "deltaweave"
            binary.parent.mkdir(mode=0o700, parents=True)
            binary.write_bytes(b"owner-binary")
            (run_root / CONTROLLER.STATE_FILENAME).write_text("{}", encoding="utf-8")
            state = {
                "controller_version": CONTROLLER.CONTROLLER_VERSION,
                "status": "prepared",
                "source_sha": source,
                "artifacts": {
                    "roles": {
                        "owner": {
                            "source_sha": source,
                            "workflow_sha": source,
                            "target": "linux",
                            "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                            "size_bytes": binary.stat().st_size,
                            "binary_name": "artifacts/role-artifact/deltaweave",
                        }
                    }
                },
            }
            resolved = CONTROLLER.prepared_role_binary(run_root / CONTROLLER.STATE_FILENAME, state, "owner")
            self.assertEqual(resolved["path"], binary)
            self.assertEqual(resolved["sha256"], hashlib.sha256(binary.read_bytes()).hexdigest())
            self.assertEqual(resolved["size_bytes"], binary.stat().st_size)
            (run_root / "outside").write_bytes(b"outside")
            binary.unlink()
            binary.symlink_to(run_root / "outside")
            with self.assertRaises(CONTROLLER.ControllerError) as error:
                CONTROLLER.prepared_role_binary(run_root / CONTROLLER.STATE_FILENAME, state, "owner")
            self.assertEqual(error.exception.error_class, "artifact_invalid")

    def test_rw_supervisor_keeps_runner_alive_after_readiness(self) -> None:
        expected_hash = "b" * 64
        observer = CONTROLLER.RemoteReadinessObserver(expected_hash, 8 * 1024 * 1024, 2)
        release = threading.Event()
        runner_finished = threading.Event()

        def runner(*, on_stdout: Callable[[bytes], None]) -> CONTROLLER.bootstrap.RemoteRun:
            on_stdout(
                b"FROLE|phase=member_join|ok=true\n"
                + f"FROLE|phase=file_hash|ok=true|hash={expected_hash}|size=8388608\n".encode()
                + b"FTRACE|stage=keepalive_enter|count=2\n"
            )
            release.wait(timeout=2)
            runner_finished.set()
            return CONTROLLER.bootstrap.RemoteRun(status_code=0)

        supervisor = CONTROLLER.RwSupervisor(runner, observer)
        supervisor.start()
        self.assertTrue(supervisor.wait_ready(1))
        self.assertTrue(supervisor.is_alive())
        self.assertFalse(runner_finished.is_set())
        release.set()
        self.assertIsNotNone(supervisor.wait_finished(1))
        self.assertTrue(runner_finished.is_set())

    def test_run_requires_live_rw_readiness_before_hosted_dispatch_and_cleans_owned_runtime(self) -> None:
        source = "a" * 40
        file_hash = hashlib.sha256(
            b"".join(CONTROLLER.fixture_chunk(index) for index in range(128))
        ).hexdigest()
        owner_bytes = b"linux-owner-binary"
        rw_bytes = b"windows-rw-binary"
        owner_hash = hashlib.sha256(owner_bytes).hexdigest()
        rw_hash = hashlib.sha256(rw_bytes).hexdigest()
        remote_phases = [
            {
                "phase": phase,
                "ok": True,
                "hash": (rw_hash if phase == "binary_verification" else file_hash if phase in {"file_hash", "member_reopen_membership"} else None),
                "size": (len(rw_bytes) if phase == "binary_verification" else 8 * 1024 * 1024 if phase in {"file_hash", "member_reopen_membership"} else None),
                "forced": False,
                "signal": "ctrl_c" if phase == "cleanup" else None,
                "error_class": "none",
            }
            for phase in CONTROLLER.bootstrap.REMOTE_REQUIRED_PHASES
        ]
        remote_result = CONTROLLER.bootstrap.RemoteRun(
            phases=remote_phases,
            binary_size=len(rw_bytes),
            file_hash=file_hash,
            file_size=8 * 1024 * 1024,
            graceful_signal=True,
            transport_cleanup_completed=True,
            diagnostic_stages=list(CONTROLLER.bootstrap.REMOTE_REOPEN_TRACE_STAGES)
            + list(CONTROLLER.bootstrap.REMOTE_KEEPALIVE_TRACE_STAGES),
            diagnostic_counts={"reopen_checks": 63},
            status_code=0,
        )

        class FakeOwnerClient:
            def create_share(self, root: Path) -> str:
                self.created_root = root
                return "c" * 64

            def issue_key(self, share_id: str, permission: str) -> str:
                return "rw-key" if permission == "read_write" else "ro-key"

        class FakeOwner:
            instances: list["FakeOwner"] = []

            def __init__(self, spec: object, run_root: Path, role: str, vault: object) -> None:
                self.run_root = run_root
                self.root = run_root / "owner-files"
                self.base_url = None
                self.forced_termination = False
                self.graceful_drain_proven = True
                self.process = object()
                self.stopped = False
                self.client_value = FakeOwnerClient()
                self.__class__.instances.append(self)

            def start(self) -> None:
                self.root.mkdir(mode=0o700)
                self.base_url = "http://127.0.0.1:49152"

            def client(self) -> FakeOwnerClient:
                return self.client_value

            def stop(self) -> bool:
                self.stopped = True
                self.process = None
                return True

        actions = FakeActions(
            [
                {
                    "databaseId": 901,
                    "headSha": source,
                    "status": "in_progress",
                    "conclusion": None,
                    "event": "workflow_dispatch",
                    "createdAt": "2026-09-09T10:00:03Z",
                    "displayTitle": "qsync-777",
                }
            ]
        )
        actions.ro_evidence = {
            "status": "pass",
            "source_sha": source,
            "full_f_claim": False,
            "expected_file_hash": file_hash,
            "expected_file_size_bytes": 8 * 1024 * 1024,
            "file_hash_verified": {
                "ro_consumer": {
                    "sha256": file_hash,
                    "size_bytes": 8 * 1024 * 1024,
                    "observed": True,
                }
            },
        }
        rw_release = threading.Event()
        actions.on_hosted_job = rw_release.set
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            state_root = parent / "prepared"
            state_root.mkdir(mode=0o700)
            (state_root / CONTROLLER.OWNED_MARKER).write_text("owned", encoding="ascii")
            owner_binary = state_root / "artifacts" / "role-artifact" / "deltaweave"
            rw_binary = state_root / "artifacts" / "native-artifact" / "deltaweave.exe"
            owner_binary.parent.mkdir(mode=0o700, parents=True)
            rw_binary.parent.mkdir(mode=0o700, parents=True)
            owner_binary.write_bytes(owner_bytes)
            rw_binary.write_bytes(rw_bytes)
            state_path = state_root / CONTROLLER.STATE_FILENAME
            CONTROLLER.write_state(
                state_path,
                {
                    "controller_version": CONTROLLER.CONTROLLER_VERSION,
                    "status": "prepared",
                    "source_sha": source,
                    "ref": "feature",
                    "prepare_run_id": "901",
                    "prebuilt_role_run_id": "901",
                    "native_artifact_run_id": "901",
                    "coordination_run_id": "777",
                    "expected_file_hash": file_hash,
                    "expected_file_size_bytes": 8 * 1024 * 1024,
                    "artifacts": {
                        "roles": {
                            "owner": {
                                "source_sha": source,
                                "workflow_sha": source,
                                "target": "linux",
                                "sha256": owner_hash,
                                "size_bytes": len(owner_bytes),
                                "binary_name": "artifacts/role-artifact/deltaweave",
                            },
                            "rw_provider": {
                                "source_sha": source,
                                "workflow_sha": source,
                                "target": "windows",
                                "sha256": rw_hash,
                                "size_bytes": len(rw_bytes),
                                "binary_name": "artifacts/native-artifact/deltaweave.exe",
                            },
                        }
                    },
                },
            )
            def fake_run_winrm_member(*_: object, on_stdout: Callable[[bytes], None], **__: object) -> CONTROLLER.bootstrap.RemoteRun:
                on_stdout(
                    b"FROLE|phase=member_join|ok=true\n"
                    + f"FROLE|phase=file_hash|ok=true|hash={file_hash}|size=8388608\n".encode()
                    + b"FTRACE|stage=keepalive_enter|count=5\n"
                )
                rw_release.wait(timeout=2)
                return remote_result

            controller = CONTROLLER.LocalController(actions, parent, repo="owner/repo", ref="feature")
            environment = {
                "QSYNC_F_OWNER_API_URL": "http://127.0.0.1:{port}",
                "QSYNC_F_ARTIFACT_PUBLIC_HOST": "127.0.0.1",
                "QSYNC_F_WINRM_HOST": "127.0.0.1",
                "QSYNC_F_WINRM_USERNAME": "fixture-user",
                "QSYNC_F_WINRM_PASSWORD": "fixture-password",
                "QSYNC_F_WINRM_DESTINATION": r"C:\qsync-test",
            }
            with (
                mock.patch.dict(os.environ, environment, clear=False),
                mock.patch.object(CONTROLLER.bootstrap, "LocalWebProcess", FakeOwner),
                mock.patch.object(CONTROLLER.bootstrap, "run_winrm_member", fake_run_winrm_member),
            ):
                result = controller.run(state_path, keepalive_seconds=5, ready_timeout_seconds=2)
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(result["status"], "pass")
            self.assertTrue(result["rw_contract_valid"])
            self.assertTrue(result["owner_stopped"])
            self.assertTrue(result["runtime_removed"])
            self.assertEqual(state["status"], "pass")
            self.assertTrue(state["rw_ready"])
            self.assertTrue(state["rw_completed"])
            self.assertEqual(state["ro_status"], "pass")
            self.assertEqual(len(actions.dispatches), 1)
            self.assertTrue(FakeOwner.instances[-1].stopped)
            serialized_state = state_path.read_text(encoding="utf-8")
            self.assertNotIn("rw-key", serialized_state)
            self.assertNotIn("ro-key", serialized_state)
            self.assertNotIn("http://", serialized_state)

    def test_run_cli_exposes_only_bounded_role_environment_options(self) -> None:
        parsed = CONTROLLER._cli().parse_args(
            [
                "run",
                "--repo",
                "owner/repo",
                "--ref",
                "feature",
                "--state",
                "/var/tmp/controller-state.json",
                "--keepalive-seconds",
                "30",
                "--ready-timeout-seconds",
                "45",
            ]
        )
        self.assertEqual(parsed.command, "run")
        self.assertEqual(parsed.keepalive_seconds, 30)
        self.assertEqual(parsed.ready_timeout_seconds, 45)
        self.assertEqual(parsed.owner_api_url_env, "QSYNC_F_OWNER_API_URL")

    def test_workflow_declares_prepare_and_execute_artifact_reuse_contract(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "qsync-three-host-bootstrap.yml").read_text(encoding="utf-8")
        self.assertIn("execution_mode", workflow)
        self.assertIn("prebuilt_role_run_id", workflow)
        self.assertIn("expected_file_size", workflow)
        self.assertIn("if: ${{ inputs.execution_mode == 'prepare' }}", workflow)
        self.assertIn("if: ${{ inputs.execution_mode == 'execute' && inputs.execute_ro }}", workflow)
        self.assertIn("qsync-f-linux-role-${{ inputs.prebuilt_role_run_id }}", workflow)
        self.assertIn("--expected-file-size", workflow)
        self.assertNotIn("needs: provenance-and-linux-build", workflow.split("hosted-ro-consumer:", 1)[-1])

    def test_ci_dispatch_passes_controller_fixture_and_splits_execute_job(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        self.assertIn("run-name: DeltaWeave CI qsync-", workflow)
        self.assertIn("qsync_three_host_expected_file_hash", workflow)
        self.assertIn("qsync_three_host_expected_file_size", workflow)
        self.assertIn("qsync_three_host_prebuilt_run_id", workflow)
        self.assertIn("qsync-three-host-bootstrap-execute:", workflow)
        self.assertIn("execution_mode: execute", workflow)
        self.assertIn("expected_file_hash: ${{ inputs.qsync_three_host_expected_file_hash }}", workflow)
        self.assertNotIn("8b666f88f7b033f647f9b5ae66d668b7bb88376630dbecfb0fba757f4f84334c", workflow)
        self.assertIn("inputs.qsync_three_host_execute != true", workflow)
        self.assertIn("qsync-native-artifact:", workflow)

    def test_execute_uses_prepared_run_and_persists_released_ephemeral_slot(self) -> None:
        source = "a" * 40
        file_hash = "b" * 64
        run = {
            "databaseId": 222,
            "headSha": source,
            "status": "completed",
            "conclusion": "success",
            "event": "workflow_dispatch",
            "createdAt": "2026-09-09T10:00:03Z",
            "displayTitle": "qsync-555",
        }
        actions = FakeActions([run])
        actions.ro_evidence = {
            "status": "pass",
            "source_sha": source,
            "full_f_claim": False,
            "expected_file_hash": file_hash,
            "expected_file_size_bytes": 8 * 1024 * 1024,
            "file_hash_verified": {
                "ro_consumer": {
                    "sha256": file_hash,
                    "size_bytes": 8 * 1024 * 1024,
                    "observed": True,
                }
            },
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "run"
            root.mkdir(mode=0o700)
            (root / CONTROLLER.OWNED_MARKER).write_text("owned", encoding="ascii")
            state_path = root / CONTROLLER.STATE_FILENAME
            CONTROLLER.write_state(
                state_path,
                {
                    "controller_version": CONTROLLER.CONTROLLER_VERSION,
                    "status": "prepared",
                    "source_sha": source,
                    "ref": "feature",
                    "prepare_run_id": "111",
                    "prebuilt_role_run_id": "113",
                    "native_artifact_run_id": "112",
                    "coordination_run_id": "555",
                    "expected_file_hash": file_hash,
                    "expected_file_size_bytes": 8 * 1024 * 1024,
                    "artifact_root": "artifacts",
                    "prepared_utc": "2026-09-09T10:00:00Z",
                },
            )
            controller = CONTROLLER.LocalController(actions, Path(temporary), repo="owner/repo", ref="feature")
            result = controller.execute_hosted_ro(
                state_path,
                "share-key-held-only-in-memory",
                rw_ready={
                    "status": "ready",
                    "role": "rw_provider",
                    "source_sha": source,
                    "coordination_run_id": "555",
                    "expected_file_hash": file_hash,
                    "expected_file_size_bytes": 8 * 1024 * 1024,
                    "keepalive_observed": True,
                },
                secret_name="QSYNC_F_RO_555",
                dispatch_utc=0.0,
            )
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(result["status"], "pass")
            self.assertTrue(result["secret_released"])
            self.assertEqual(state["status"], "pass")
            self.assertEqual(state["ephemeral_slot"]["status"], "released")
            self.assertNotIn("share-key-held", state_path.read_text(encoding="utf-8"))
            execute_inputs = actions.dispatches[0]["inputs"]
            self.assertEqual(execute_inputs["qsync_three_host_prebuilt_run_id"], "113")
            self.assertEqual(actions.hosted_job_calls, [("222", CONTROLLER.HOSTED_RO_JOB_NAME)])
        self.assertEqual(execute_inputs["qsync_three_host_native_artifact_run_id"], "112")

    def test_execute_live_mode_rejects_ready_file_without_live_handle_attestation(self) -> None:
        source = "a" * 40
        file_hash = "b" * 64
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "run"
            root.mkdir(mode=0o700)
            (root / CONTROLLER.OWNED_MARKER).write_text("owned", encoding="ascii")
            state_path = root / CONTROLLER.STATE_FILENAME
            CONTROLLER.write_state(
                state_path,
                {
                    "controller_version": CONTROLLER.CONTROLLER_VERSION,
                    "status": "prepared",
                    "source_sha": source,
                    "ref": "feature",
                    "prepare_run_id": "111",
                    "native_artifact_run_id": "112",
                    "coordination_run_id": "555",
                    "expected_file_hash": file_hash,
                    "expected_file_size_bytes": 8 * 1024 * 1024,
                    "artifact_root": "artifacts",
                    "prepared_utc": "2026-09-09T10:00:00Z",
                },
            )
            controller = CONTROLLER.LocalController(FakeActions(), Path(temporary), repo="owner/repo", ref="feature")
            with self.assertRaises(CONTROLLER.ControllerError) as error:
                controller.execute_hosted_ro(
                    state_path,
                    "share-key-held-only-in-memory",
                    rw_ready={
                        "status": "ready",
                        "role": "rw_provider",
                        "source_sha": source,
                        "coordination_run_id": "555",
                        "expected_file_hash": file_hash,
                        "expected_file_size_bytes": 8 * 1024 * 1024,
                        "keepalive_observed": True,
                    },
                    require_live_readiness=True,
                )
            self.assertEqual(error.exception.error_class, "rw_not_ready")

    def test_secret_write_uncertainty_retains_owned_state_for_reconciliation(self) -> None:
        source = "a" * 40
        file_hash = "b" * 64

        class LostSecretActions(FakeActions):
            def set_secret(self, name: str, value: str) -> None:
                raise CONTROLLER.ControllerError("secret_set_failed", "pending", 2)

        actions = LostSecretActions()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "run"
            root.mkdir(mode=0o700)
            (root / CONTROLLER.OWNED_MARKER).write_text("owned", encoding="ascii")
            state_path = root / CONTROLLER.STATE_FILENAME
            CONTROLLER.write_state(
                state_path,
                {
                    "controller_version": CONTROLLER.CONTROLLER_VERSION,
                    "status": "prepared",
                    "source_sha": source,
                    "ref": "feature",
                    "prepare_run_id": "111",
                    "native_artifact_run_id": "112",
                    "coordination_run_id": "555",
                    "expected_file_hash": file_hash,
                    "expected_file_size_bytes": 8 * 1024 * 1024,
                    "artifact_root": "artifacts",
                    "prepared_utc": "2026-09-09T10:00:00Z",
                },
            )
            controller = CONTROLLER.LocalController(actions, Path(temporary), repo="owner/repo", ref="feature")
            with self.assertRaises(CONTROLLER.ControllerError) as error:
                controller.execute_hosted_ro(
                    state_path,
                    "share-key-held-only-in-memory",
                    rw_ready={
                        "status": "ready",
                        "role": "rw_provider",
                        "source_sha": source,
                        "coordination_run_id": "555",
                        "expected_file_hash": file_hash,
                        "expected_file_size_bytes": 8 * 1024 * 1024,
                        "keepalive_observed": True,
                    },
                    secret_name="QSYNC_F_RO_555",
                )
            self.assertEqual(error.exception.error_class, "secret_set_failed")
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(state["status"], "pending")
            self.assertEqual(state["ephemeral_slot"]["status"], "unknown")
            self.assertTrue(root.exists())


if __name__ == "__main__":
    unittest.main()
