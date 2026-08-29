use std::{
    fs,
    path::{Path, PathBuf},
};

fn main() {
    ensure_sidecar();
    tauri_build::build();
}

fn ensure_sidecar() {
    let triple = std::env::var("TARGET").unwrap_or_else(|_| "x86_64-unknown-linux-gnu".into());
    let mut dest = PathBuf::from(format!("binaries/deltaweave-daemon-{triple}"));
    if triple.contains("windows") {
        dest.set_extension("exe");
    }
    if dest.exists() {
        if std::env::var("PROFILE").as_deref() == Ok("release") {
            let size = fs::metadata(&dest)
                .map(|meta| meta.len())
                .unwrap_or_default();
            assert!(size > 1024, "invalid release sidecar {}", dest.display());
        }
        return;
    }
    let _ = fs::create_dir_all("binaries");
    for candidate in [
        format!("../../../target/{triple}/release/deltaweave-daemon.exe"),
        format!("../../../target/{triple}/release/deltaweave-daemon"),
        format!("../../../target/{triple}/debug/deltaweave-daemon.exe"),
        format!("../../../target/{triple}/debug/deltaweave-daemon"),
        "../../../target/release/deltaweave-daemon.exe".into(),
        "../../../target/release/deltaweave-daemon".into(),
        "../../../target/debug/deltaweave-daemon.exe".into(),
        "../../../target/debug/deltaweave-daemon".into(),
    ] {
        let src = Path::new(&candidate);
        if src.exists() && fs::copy(src, &dest).is_ok() {
            return;
        }
    }
    if std::env::var("PROFILE").as_deref() == Ok("release") {
        panic!("missing release sidecar {}", dest.display());
    }
    write_stub(&dest);
}

fn write_stub(dest: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::write(dest, b"#!/bin/sh\nexit 0\n");
        if let Ok(meta) = fs::metadata(dest) {
            let mut perms = meta.permissions();
            perms.set_mode(0o755);
            let _ = fs::set_permissions(dest, perms);
        }
    }
    #[cfg(windows)]
    {
        let _ = fs::write(dest, b"");
    }
}
