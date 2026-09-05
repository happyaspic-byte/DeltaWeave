//! Exercise presentation through the real CLI, including its machine-output contract.

use std::{fs, process::Command};

fn cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_deltaweave"))
}

#[test]
fn explicit_json_keeps_default_manifest_bytes() {
    let workspace = tempfile::tempdir().unwrap();
    let source = workspace.path().join("sample.txt");
    fs::write(&source, b"same data, same manifest\n").unwrap();
    let default = cli().arg("manifest").arg(&source).output().unwrap();
    let explicit = cli()
        .args(["--output", "json", "manifest"])
        .arg(&source)
        .output()
        .unwrap();
    assert!(default.status.success());
    assert!(explicit.status.success());
    assert_eq!(default.stdout, explicit.stdout);
    serde_json::from_slice::<serde_json::Value>(&default.stdout).unwrap();
}

#[test]
fn text_identity_keeps_full_endpoint_and_redirection_has_no_ansi() {
    let workspace = tempfile::tempdir().unwrap();
    let identity = workspace.path().join("identity.key");
    let original = cli()
        .args(["init", "--identity"])
        .arg(&identity)
        .output()
        .unwrap();
    assert!(original.status.success());
    let original: serde_json::Value = serde_json::from_slice(&original.stdout).unwrap();
    let output = cli()
        .args(["init", "--output", "text", "--identity"])
        .arg(&identity)
        .output()
        .unwrap();
    assert!(output.status.success(), "{:?}", output.stderr);
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains(original["endpoint_id"].as_str().unwrap()));
    assert!(text.contains(identity.to_str().unwrap()));
    assert!(text.contains("existing"));
    assert!(!text.contains('\u{1b}'));
}

#[test]
fn text_scan_reports_empty_state_and_retains_index_changes() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().join("root");
    fs::create_dir(&root).unwrap();
    let scan = || {
        cli()
            .args(["--output", "text", "scan", "--root"])
            .arg(&root)
            .arg("--state")
            .arg(workspace.path().join("index.redb"))
            .arg("--identity")
            .arg(workspace.path().join("identity.key"))
            .output()
            .unwrap()
    };
    let empty = scan();
    assert!(empty.status.success(), "{:?}", empty.stderr);
    assert!(
        String::from_utf8(empty.stdout)
            .unwrap()
            .contains("No changes")
    );
    fs::write(root.join("notes.txt"), b"preserved contents").unwrap();
    let changed = scan();
    assert!(changed.status.success());
    assert!(
        String::from_utf8(changed.stdout)
            .unwrap()
            .contains("notes.txt")
    );
    let unchanged = scan();
    assert!(unchanged.status.success());
    assert!(
        String::from_utf8(unchanged.stdout)
            .unwrap()
            .contains("No changes")
    );
    assert_eq!(
        fs::read(root.join("notes.txt")).unwrap(),
        b"preserved contents"
    );
}

#[test]
fn text_runtime_error_stays_on_stderr_and_fails() {
    let output = cli()
        .args([
            "--output",
            "text",
            "push",
            "missing.txt",
            "--remote-path",
            "notes.txt",
            "--peer",
            "invalid",
            "--direct-only",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let error = String::from_utf8(output.stderr).unwrap();
    assert!(error.contains("--direct-only requires at least one --direct address"));
    assert!(!error.contains('\u{1b}'));
}
