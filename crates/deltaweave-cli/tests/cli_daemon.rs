//! CLI parsing, systemd unit rendering, and Linux subprocess checks.

use std::process::Command;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_deltaweave"))
}

#[test]
fn help_lists_daemon_ctl_and_service() {
    let output = bin()
        .arg("--help")
        .output()
        .expect("deltaweave --help runs");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("daemon"));
    assert!(stdout.contains("ctl"));
    assert!(stdout.contains("service"));
}

#[test]
fn ctl_help_lists_status_pause_resume_stop() {
    let output = bin()
        .args(["ctl", "--help"])
        .output()
        .expect("ctl --help runs");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("status"));
    assert!(stdout.contains("pause"));
    assert!(stdout.contains("resume"));
    assert!(stdout.contains("stop"));
    assert!(stdout.contains("--control-state"));
}

#[test]
fn daemon_help_requires_sync_args_and_control_state() {
    let output = bin()
        .args(["daemon", "--help"])
        .output()
        .expect("daemon --help runs");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--control-state"));
    assert!(stdout.contains("--root"));
    assert!(stdout.contains("--peer"));
}

#[cfg(unix)]
#[test]
fn systemd_unit_subprocess_prints_quoted_unit() {
    let output = bin()
        .args([
            "service",
            "systemd-unit",
            "--executable",
            "/opt/delta weave/bin/deltaweave",
            "--user",
            "syncuser",
            "--control-state",
            "/var/lib/delta weave/control",
            "--root",
            "/srv/data",
            "--state",
            "/var/lib/delta weave/sync",
            "--identity",
            "/var/lib/delta weave/identity.key",
            "--peer",
            "peer-id",
        ])
        .output()
        .expect("service systemd-unit subprocess runs");
    assert!(
        output.status.success(),
        "systemd-unit failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("ExecStart=\"/opt/delta weave/bin/deltaweave\" daemon"));
    assert!(stdout.contains("--control-state \"/var/lib/delta weave/control\""));
    assert!(stdout.contains("NoNewPrivileges=true"));
    assert!(stdout.contains("ProtectSystem=strict"));
    assert!(!stdout.contains("ExecStart=/opt/delta weave/bin/deltaweave daemon"));
}

#[cfg(unix)]
#[test]
fn systemd_unit_rejects_relative_paths() {
    let output = bin()
        .args([
            "service",
            "systemd-unit",
            "--executable",
            "deltaweave",
            "--user",
            "syncuser",
            "--control-state",
            "/var/lib/deltaweave/control",
            "--root",
            "/srv/data",
            "--state",
            "/var/lib/deltaweave/sync",
            "--identity",
            "/var/lib/deltaweave/identity.key",
            "--peer",
            "peer-id",
        ])
        .output()
        .expect("service systemd-unit subprocess runs");
    assert!(!output.status.success());
}

#[cfg(unix)]
#[test]
fn linux_daemon_ctl_lifecycle_pause_resume_and_stop() {
    use std::{
        io::Read,
        os::unix::net::UnixStream,
        thread,
        time::{Duration, Instant},
    };

    let root = tempfile::tempdir().expect("root");
    let state = tempfile::tempdir().expect("state");
    let control = tempfile::tempdir().expect("control");
    let identity_dir = tempfile::tempdir().expect("identity dir");
    let identity = identity_dir.path().join("identity.key");
    let bin = env!("CARGO_BIN_EXE_deltaweave");
    let mut child = Command::new(bin)
        .args([
            "daemon",
            "--root",
            root.path().to_str().unwrap(),
            "--state",
            state.path().to_str().unwrap(),
            "--identity",
            identity.to_str().unwrap(),
            "--peer",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--control-state",
            control.path().to_str().unwrap(),
            "--interval-seconds",
            "60",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn daemon");

    let sock = control.path().join("control.sock");
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if sock.exists() {
            break;
        }
        if let Some(status) = child.try_wait().expect("poll daemon") {
            let mut stderr = String::new();
            if let Some(mut pipe) = child.stderr.take() {
                let _ = pipe.read_to_string(&mut stderr);
            }
            panic!("daemon exited early {status}: {stderr}");
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(sock.exists(), "control.sock was not created");
    assert_eq!(unix_mode(&sock) & 0o777, 0o600);
    assert_eq!(
        unix_mode(&control.path().join("owner.token")) & 0o777,
        0o600
    );

    for command in ["status", "pause", "resume", "stop"] {
        let output = Command::new(bin)
            .args([
                "ctl",
                "--control-state",
                control.path().to_str().unwrap(),
                command,
            ])
            .output()
            .expect("ctl");
        assert!(
            output.status.success(),
            "ctl {command} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("\"ok\": true"), "{stdout}");
        if command == "pause" {
            assert!(stdout.contains("paused"), "{stdout}");
        }
        if command == "stop" {
            assert!(
                stdout.contains("stopping") || stdout.contains("stopped"),
                "{stdout}"
            );
        }
    }

    let _ = UnixStream::connect(&sock);
    let finished = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().expect("wait daemon") {
            assert!(status.success(), "daemon exit {status}");
            break;
        }
        if Instant::now() > finished {
            let _ = child.kill();
            panic!("daemon did not exit after stop");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(unix)]
fn unix_mode(path: &std::path::Path) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .expect("metadata")
        .permissions()
        .mode()
}

#[cfg(unix)]
#[test]
fn systemd_unit_escapes_backslash_and_quote() {
    let output = bin()
        .args([
            "service",
            "systemd-unit",
            "--executable",
            r#"/opt/delta\"weave/bin/deltaweave"#,
            "--user",
            "syncuser",
            "--control-state",
            "/var/lib/deltaweave/control",
            "--root",
            "/srv/data",
            "--state",
            "/var/lib/deltaweave/sync",
            "--identity",
            "/var/lib/deltaweave/identity.key",
            "--peer",
            "peer-id",
        ])
        .output()
        .expect("service systemd-unit subprocess runs");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains(r#"ExecStart="/opt/delta\\\"weave/bin/deltaweave" daemon"#));
}

#[cfg(windows)]
#[test]
fn windows_service_help_documents_run() {
    let output = bin()
        .args(["service", "--help"])
        .output()
        .expect("service --help runs");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("run"));
}
