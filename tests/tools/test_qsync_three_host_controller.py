from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import sys
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
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
        self.assertEqual(execute_inputs["qsync_three_host_native_artifact_run_id"], "112")

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
