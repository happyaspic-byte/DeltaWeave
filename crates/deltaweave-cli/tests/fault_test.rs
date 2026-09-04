use std::{
    fs,
    process::Command,
    sync::{Mutex, MutexGuard},
};

use serde_json::Value;
use tempfile::TempDir;

static FAULT_TEST_LOCK: Mutex<()> = Mutex::new(());

fn serialized() -> MutexGuard<'static, ()> {
    FAULT_TEST_LOCK.lock().expect("fault-test lock")
}

fn run(workspace: &TempDir, extra: &[&str]) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_deltaweave"));
    command.args([
        "fault-test",
        "--seed",
        "424242",
        "--workspace",
        workspace.path().to_str().expect("UTF-8 temporary path"),
        "--payload-mib",
        "16",
    ]);
    command.args(extra).output().expect("fault-test starts")
}

fn report(workspace: &TempDir) -> Value {
    serde_json::from_slice(&fs::read(workspace.path().join("report.json")).expect("report exists"))
        .expect("report is JSON")
}

#[test]
fn shipped_fault_entrypoint_kills_processes_and_preserves_durable_state() {
    let _guard = serialized();
    let workspace = TempDir::new().expect("workspace can be created");
    let output = run(&workspace, &[]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report = report(&workspace);
    assert_eq!(report["status"], "pass");
    assert_eq!(report["restart_local_actions"], 0);
    assert_eq!(report["restart_remote_actions"], 0);
    assert_eq!(
        report["faults"][0]["barrier"],
        "remote_chunk_persisted_destination_absent"
    );
    assert_eq!(report["faults"][0]["killed_process"], "serve");
    assert_eq!(
        report["faults"][1]["barrier"],
        "remote_chunk_persisted_destination_absent"
    );
    assert_eq!(report["faults"][1]["killed_process"], "sync-once");
    assert!(report["faults"][0]["pid"].as_u64().is_some());
    assert!(report["faults"][1]["pid"].as_u64().is_some());
    assert!(workspace.path().join("states/windows/index.redb").exists());
    assert!(workspace.path().join("states/synology/index.redb").exists());
}

#[test]
fn same_seed_is_deterministic_and_forced_failure_keeps_complete_bundle() {
    let _guard = serialized();
    let first = TempDir::new().expect("first workspace");
    let second = TempDir::new().expect("second workspace");
    assert!(run(&first, &[]).status.success());
    assert!(run(&second, &[]).status.success());
    let first_report = report(&first);
    let second_report = report(&second);
    for key in [
        "seed",
        "operations",
        "final_merkle_root",
        "restart_local_actions",
        "restart_remote_actions",
    ] {
        assert_eq!(first_report[key], second_report[key], "{key}");
    }
    let fault_signature = |report: &Value| {
        report["faults"]
            .as_array()
            .expect("fault list")
            .iter()
            .map(|fault| {
                (
                    fault["barrier"].as_str().expect("barrier").to_owned(),
                    fault["killed_process"]
                        .as_str()
                        .expect("process")
                        .to_owned(),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        fault_signature(&first_report),
        fault_signature(&second_report)
    );

    let failed = TempDir::new().expect("failure workspace");
    let output = run(&failed, &["--force-failure"]);
    assert!(!output.status.success());
    let failed_report = report(&failed);
    assert_eq!(failed_report["status"], "forced_failure");
    assert!(
        failed_report["operations"]
            .as_array()
            .is_some_and(|value| value.len() >= 4)
    );
    for path in [
        "logs/windows.log",
        "logs/synology.log",
        "roots/windows",
        "roots/synology",
        "states/windows",
        "states/synology",
    ] {
        assert!(failed.path().join(path).exists(), "missing {path}");
    }
}

#[test]
fn early_failure_still_writes_reproduction_metadata() {
    let _guard = serialized();
    let workspace = TempDir::new().expect("workspace");
    fs::write(workspace.path().join("roots"), b"blocks directory").expect("fixture written");
    let output = run(&workspace, &[]);
    assert!(!output.status.success());
    let report = report(&workspace);
    assert_eq!(report["status"], "failed");
    assert_eq!(report["seed"], 424242);
    assert!(
        report["error"]
            .as_str()
            .is_some_and(|error| !error.is_empty())
    );
}
