use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn collect(root: &Path, path: &Path, output: &mut Vec<(String, PathBuf)>) {
    println!("cargo:rerun-if-changed={}", path.display());
    if !path.exists() {
        return;
    }
    for entry in fs::read_dir(path).expect("read browser assets") {
        let path = entry.expect("read browser asset entry").path();
        assert!(
            !fs::symlink_metadata(&path)
                .unwrap()
                .file_type()
                .is_symlink(),
            "browser assets must not contain symlinks"
        );
        if path.is_dir() {
            collect(root, &path, output);
        } else {
            println!("cargo:rerun-if-changed={}", path.display());
            output.push((
                format!(
                    "/{}",
                    path.strip_prefix(root)
                        .unwrap()
                        .to_string_lossy()
                        .replace('\\', "/")
                ),
                path,
            ));
        }
    }
}
fn main() {
    println!("cargo:rerun-if-env-changed=DELTAWEAVE_REQUIRE_WEB_ASSETS");
    let root = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap()).join("../../web/dist");
    let mut assets = Vec::new();
    collect(&root, &root, &mut assets);
    assets.sort();
    let index = fs::read_to_string(root.join("index.html")).ok();
    let mut valid = index.is_some()
        && assets
            .iter()
            .any(|(name, _)| name.starts_with("/assets/") && name.ends_with(".js"));
    if let Some(index) = &index {
        for marker in ["src=\"", "href=\""] {
            for suffix in index.split(marker).skip(1) {
                let reference = suffix.split('"').next().unwrap();
                if reference.starts_with("http:")
                    || reference.starts_with("https:")
                    || reference.starts_with("//")
                {
                    valid = false;
                    continue;
                }
                if reference.starts_with("data:") || reference.starts_with('#') {
                    continue;
                }
                let reference = reference
                    .split(['?', '#'])
                    .next()
                    .unwrap()
                    .trim_start_matches('/');
                valid &= root.join(reference).is_file();
            }
        }
    }
    if env::var("DELTAWEAVE_REQUIRE_WEB_ASSETS").as_deref() == Ok("1") {
        assert!(
            valid,
            "built browser assets missing or index references missing chunks; run npm --prefix web ci && npm --prefix web run build"
        );
    }
    let mut generated =
        format!("pub const AVAILABLE: bool = {valid};\npub static ASSETS: &[(&str, &[u8])] = &[\n");
    for (name, path) in assets {
        generated.push_str(&format!(
            "({name:?}, include_bytes!({:?})),\n",
            path.to_string_lossy()
        ));
    }
    generated.push_str("];\n");
    fs::write(
        PathBuf::from(env::var("OUT_DIR").unwrap()).join("web_assets.rs"),
        generated,
    )
    .unwrap();
}
