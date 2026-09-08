use std::{fs, process::Command};

use serde_json::Value;
use tempfile::TempDir;

fn run(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_deltaweave"))
        .args(args)
        .output()
        .expect("deltaweave starts")
}

fn json(output: &std::process::Output) -> Value {
    serde_json::from_slice(&output.stdout).expect("stdout is JSON")
}

#[test]
fn doctor_reports_ready_for_separate_writable_paths_and_direct_peer() {
    let workspace = TempDir::new().expect("workspace");
    let root = workspace.path().join("root");
    let state = workspace.path().join("private/state");
    let identity = workspace.path().join("private/node.key");
    fs::create_dir_all(&root).expect("root");
    let initialized = run(&[
        "init",
        "--identity",
        identity.to_str().expect("identity path"),
    ]);
    assert!(initialized.status.success());
    let endpoint_id = json(&initialized)["endpoint_id"]
        .as_str()
        .expect("endpoint ID")
        .to_owned();

    let args = [
        "doctor",
        "--root",
        root.to_str().expect("root path"),
        "--state",
        state.to_str().expect("state path"),
        "--identity",
        identity.to_str().expect("identity path"),
        "--peer",
        &endpoint_id,
        "--direct",
        "192.0.2.10:49152",
        "--direct-only",
    ];
    let output = run(&args);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report = json(&output);
    assert_eq!(report["status"], "pass");
    assert_eq!(report["checks"]["root_writable"]["status"], "pass");
    assert_eq!(report["checks"]["state_writable"]["status"], "pass");
    assert_eq!(report["checks"]["path_separation"]["status"], "pass");
    assert_eq!(report["checks"]["identity"]["status"], "pass");
    assert_eq!(report["checks"]["peer"]["status"], "pass");
    assert_eq!(report["checks"]["direct_addresses"]["status"], "pass");

    let text_args = [vec!["--output", "text"], args.to_vec()].concat();
    let text_output = run(&text_args);
    assert!(text_output.status.success());
    let text = String::from_utf8(text_output.stdout).expect("UTF-8 report");
    assert!(text.contains("DeltaWeave"));
    assert!(text.contains("pass"));
    assert!(!text.trim_start().starts_with('{'));
}

#[test]
fn doctor_returns_actionable_json_for_overlapping_paths_and_missing_direct_input() {
    let workspace = TempDir::new().expect("workspace");
    let root = workspace.path().join("root");
    let state = root.join("state");
    let identity = root.join("node.key");

    let output = run(&[
        "doctor",
        "--root",
        root.to_str().expect("root path"),
        "--state",
        state.to_str().expect("state path"),
        "--identity",
        identity.to_str().expect("identity path"),
        "--direct-only",
    ]);
    assert!(!output.status.success());
    let report = json(&output);
    assert_eq!(report["status"], "fail");
    assert_eq!(report["checks"]["path_separation"]["status"], "fail");
    assert!(
        report["checks"]["path_separation"]["action"]
            .as_str()
            .is_some_and(|value| value.contains("outside"))
    );
    assert_eq!(report["checks"]["identity"]["status"], "fail");
    assert!(report["checks"]["identity"]["action"].as_str().is_some());
    assert_eq!(report["checks"]["direct_addresses"]["status"], "fail");
    assert!(
        report["checks"]["direct_addresses"]["action"]
            .as_str()
            .is_some_and(|value| value.contains("--direct"))
    );
}

#[test]
fn doctor_separates_absent_identity_path_without_creating_or_overwriting_probes() {
    let workspace = TempDir::new().expect("workspace");
    let root = workspace.path().join("root");
    let state = workspace.path().join("private/state");
    let identity = workspace.path().join("identity/node.key");
    fs::create_dir_all(identity.parent().expect("identity parent")).expect("identity parent");
    fs::create_dir_all(&root).expect("root");
    let legacy_probe = root.join(format!(".deltaweave-write-test-{}", std::process::id()));
    fs::write(&legacy_probe, b"operator data").expect("collision fixture");

    let output = run(&[
        "doctor",
        "--root",
        root.to_str().expect("root path"),
        "--state",
        state.to_str().expect("state path"),
        "--identity",
        identity.to_str().expect("identity path"),
    ]);
    assert!(!output.status.success());
    let report = json(&output);
    assert_eq!(report["checks"]["path_separation"]["status"], "pass");
    assert_eq!(report["checks"]["identity"]["status"], "fail");
    assert!(!identity.exists());
    assert_eq!(
        fs::read(legacy_probe).expect("fixture retained"),
        b"operator data"
    );
}

#[test]
fn doctor_and_transfer_commands_reject_unusable_direct_addresses() {
    let commands: &[&[&str]] = &[
        &[
            "doctor",
            "--root",
            ".",
            "--state",
            "target/doctor-state",
            "--identity",
            "target/missing-doctor.key",
            "--direct",
        ],
        &[
            "push",
            "source",
            "--remote-path",
            "file",
            "--peer",
            "bad",
            "--direct",
        ],
        &["sync-once", "--root", ".", "--peer", "bad", "--direct"],
        &["sync", "--root", ".", "--peer", "bad", "--direct"],
        &[
            "sync-once",
            "--root",
            ".",
            "--peer",
            "bad",
            "--swarm-peer",
            "bad",
            "--swarm-direct",
        ],
        &[
            "swarm-fill",
            "--peer",
            "bad",
            "--hash",
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "--direct",
        ],
    ];
    for address in [
        "127.0.0.1:0",
        "0.0.0.0:49152",
        "[::]:49152",
        "224.0.0.1:49152",
        "[ff02::1]:49152",
    ] {
        for command in commands {
            let mut args = command.to_vec();
            args.push(address);
            let output = run(&args);
            assert!(!output.status.success(), "accepted {args:?}");
            assert!(
                String::from_utf8_lossy(&output.stderr).contains("usable unicast"),
                "unexpected rejection for {args:?}: {}",
                String::from_utf8_lossy(&output.stderr),
            );
        }
    }
    let malformed = run(&[
        "push",
        "source",
        "--remote-path",
        "file",
        "--peer",
        "bad",
        "--direct",
        "not-an-address",
    ]);
    assert!(!malformed.status.success());
}

#[test]
fn serve_exposes_bounded_connection_and_disk_admission_options() {
    let output = run(&["serve", "--help"]);
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).expect("UTF-8 help");
    assert!(help.contains("--max-connections"));
    assert!(help.contains("--min-free-space-mib"));
}
