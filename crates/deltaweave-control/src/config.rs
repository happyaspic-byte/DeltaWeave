use crate::model::*;
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct Config {
    #[serde(default = "version")]
    pub version: u32,
    pub settings: Settings,
    #[serde(default)]
    pub folders: Vec<FolderView>,
    #[serde(default)]
    pub devices: Vec<DeviceView>,
    #[serde(default)]
    pub activities: Vec<Activity>,
    #[serde(default)]
    pub history: Vec<HistoryPoint>,
}
fn version() -> u32 {
    1
}
pub(crate) fn read(dir: &Path) -> Result<Config> {
    let path = dir.join("config.json");
    if !path.exists() {
        return Ok(Config {
            version: 1,
            ..Config::default()
        });
    }
    let config: Config =
        serde_json::from_slice(&fs::read(path)?).context("invalid management configuration")?;
    ensure!(
        config.version == 1,
        "unsupported management configuration version"
    );
    Ok(config)
}
pub(crate) fn save(dir: &Path, config: &Config) -> Result<()> {
    let mut temp = tempfile::NamedTempFile::new_in(dir)?;
    temp.write_all(&serde_json::to_vec_pretty(config)?)?;
    temp.as_file().sync_all()?;
    temp.persist(dir.join("config.json"))
        .context("replace management configuration")?;
    #[cfg(unix)]
    fs::File::open(dir)?.sync_all()?;
    Ok(())
}
pub(crate) fn candidate(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut resolved = PathBuf::new();
    for part in absolute.components() {
        use std::path::Component;
        match part {
            Component::ParentDir => {
                resolved.pop();
            }
            Component::CurDir => {}
            _ => resolved.push(part),
        }
        if resolved.exists() {
            resolved = fs::canonicalize(&resolved)?;
        }
    }
    Ok(resolved)
}
pub(crate) fn overlaps(a: &Path, b: &Path) -> bool {
    a.starts_with(b) || b.starts_with(a)
}
pub(crate) fn validate_name(name: &str) -> Result<()> {
    ensure!(
        !name.trim().is_empty()
            && name.chars().count() <= 100
            && !name.chars().any(char::is_control),
        "name must contain 1–100 printable characters"
    );
    Ok(())
}
pub(crate) fn normalize(
    mut input: FolderInput,
    id: &str,
    dir: &Path,
    others: &[FolderView],
    poll: u64,
) -> Result<FolderInput> {
    validate_name(&input.name)?;
    input.name = input.name.trim().into();
    ensure!(
        input.role == "sync" || input.role == "receive",
        "role must be sync or receive"
    );
    ensure!(!input.root.trim().is_empty(), "root path is required");
    let root = candidate(Path::new(&input.root))?;
    let state = candidate(
        &input
            .state_path
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| dir.join("folders").join(id).join("state")),
    )?;
    let identity = candidate(
        &input
            .identity_path
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| dir.join("folders").join(id).join("identity.key")),
    )?;
    ensure!(
        !overlaps(&root, &candidate(dir)?),
        "folder root overlaps management data directory"
    );
    let private_folder = dir.join("folders").join(id);
    for path in [&state, &identity] {
        ensure!(
            !overlaps(path, dir) || path.starts_with(&private_folder),
            "private paths inside management data must belong to this folder"
        );
    }
    if input.role == "receive" {
        ensure!(
            !state.join("store/metadata.redb").exists(),
            "state belongs to a sync folder; select receive state or create a new state directory"
        );
    } else {
        ensure!(
            !state.join("metadata.redb").exists(),
            "state belongs to a receive folder; select sync state or create a new state directory"
        );
    }
    ensure!(
        !overlaps(&root, &state) && !overlaps(&root, &identity) && !overlaps(&state, &identity),
        "root, state and identity paths must be separate"
    );
    if root.exists() {
        ensure!(root.is_dir(), "root must be a directory");
    }
    if state.exists() {
        ensure!(state.is_dir(), "state must be a directory");
    }
    if identity.exists() {
        ensure!(identity.is_file(), "identity must be a file");
    }
    for other in others.iter().filter(|folder| folder.id != id) {
        let other_paths = [
            Some(&other.input.root),
            other.input.state_path.as_ref(),
            other.input.identity_path.as_ref(),
        ];
        for path in other_paths.into_iter().flatten() {
            let path = candidate(Path::new(path))?;
            ensure!(
                ![&root, &state, &identity]
                    .into_iter()
                    .any(|own| overlaps(own, &path)),
                "paths overlap managed folder {}",
                other.input.name
            );
        }
    }
    for endpoint in &input.allowed_peers {
        endpoint
            .parse::<iroh::EndpointId>()
            .context("invalid allowed peer endpoint ID")?;
    }
    if let Some(peer) = &input.peer_endpoint_id {
        peer.parse::<iroh::EndpointId>()
            .context("invalid peer endpoint ID")?;
    }
    if input.role == "sync" {
        input
            .peer_endpoint_id
            .as_deref()
            .context("sync folder requires peer_endpoint_id")?
            .parse::<iroh::EndpointId>()
            .context("invalid peer endpoint ID")?;
        ensure!(
            !input.direct_addresses.is_empty(),
            "sync folder requires at least one direct address"
        );
    }
    for address in &input.direct_addresses {
        address
            .parse::<std::net::SocketAddr>()
            .context("direct address must be IP:port")?;
    }
    if let Some(bind) = &input.bind {
        bind.parse::<std::net::SocketAddr>()
            .context("bind must be IP:port")?;
    }
    let interval = input.interval_seconds.unwrap_or(poll);
    ensure!(
        (1..=86400).contains(&interval),
        "interval_seconds must be 1–86400"
    );
    let connections = input.max_connections.unwrap_or(8);
    ensure!(
        (1..=1024).contains(&connections),
        "max_connections must be 1–1024"
    );
    let space = input.min_free_space_mib.unwrap_or(64);
    ensure!(
        space.checked_mul(1024 * 1024).is_some(),
        "min_free_space_mib is too large"
    );
    input.root = root.to_string_lossy().into_owned();
    input.state_path = Some(state.to_string_lossy().into_owned());
    input.identity_path = Some(identity.to_string_lossy().into_owned());
    input.enabled = Some(input.enabled.unwrap_or(true));
    input.interval_seconds = Some(interval);
    input.max_connections = Some(connections);
    input.min_free_space_mib = Some(space);
    Ok(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_root_state_and_symlink_overlap() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("files");
        fs::create_dir(&root).unwrap();
        let input = FolderInput {
            name: "Files".into(),
            root: root.display().to_string(),
            role: "receive".into(),
            state_path: Some(root.join("state").display().to_string()),
            ..Default::default()
        };
        assert!(normalize(input.clone(), "one", &dir.path().join("admin"), &[], 30).is_err());
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&root, dir.path().join("alias")).unwrap();
            let mut input = input;
            input.state_path = Some(dir.path().join("alias/state").display().to_string());
            assert!(normalize(input, "one", &dir.path().join("admin"), &[], 30).is_err());
        }
    }
}
