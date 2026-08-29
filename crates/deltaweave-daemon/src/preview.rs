//! Dry-run merge counts and conflict-copy resolution without applying.

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use deltaweave_core::WirePath;
use deltaweave_daemon_api::{CommandResult, ConflictAction, ConflictInfo};
use deltaweave_reconcile::{ApplyAction, MerkleTree, actions_to_reach, merge_snapshots};

/// Counts sends/receives/deletes/conflicts from two snapshots. Does not apply.
pub fn preview_snapshots(local: &MerkleTree, remote: &MerkleTree) -> Result<CommandResult> {
    let merged = merge_snapshots(local, remote)?;
    let local_actions = actions_to_reach(local, &merged)?;
    let remote_actions = actions_to_reach(remote, &merged)?;
    Ok(CommandResult::Preview {
        sends: count_materialize_files(&remote_actions),
        receives: count_materialize_files(&local_actions),
        deletes: count_deletes(&local_actions),
        conflicts: u64::try_from(merged.conflicts.len()).unwrap_or(u64::MAX),
    })
}

/// Lists portable `.conflict-` copies under `root`.
pub fn list_conflicts(root: &Path) -> Result<CommandResult> {
    let mut conflicts = Vec::new();
    collect_conflicts(root, root, &mut conflicts)?;
    conflicts.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(CommandResult::Conflicts { conflicts })
}

/// Applies a UI conflict action to files already on disk.
pub fn resolve_conflict(root: &Path, path: &str, action: ConflictAction) -> Result<CommandResult> {
    let canonical = join_portable(root, path)?;
    let copy = find_conflict_copy(root, path)?;
    let Some(copy) = copy else {
        bail!("no conflict copy for {path}");
    };
    match action {
        ConflictAction::KeepBoth => {
            let retained = retained_copy_path(&copy)?;
            fs::rename(&copy, &retained)
                .with_context(|| format!("failed to retain {}", copy.display()))?;
        }
        ConflictAction::KeepLocal => {
            fs::remove_file(&copy)
                .with_context(|| format!("failed to remove {}", copy.display()))?;
        }
        ConflictAction::KeepRemote => {
            fs::copy(&copy, &canonical)
                .with_context(|| format!("failed to restore {}", canonical.display()))?;
            fs::remove_file(&copy)
                .with_context(|| format!("failed to remove {}", copy.display()))?;
        }
    }
    Ok(CommandResult::Accepted { id: path.into() })
}

fn count_materialize_files(actions: &[ApplyAction]) -> u64 {
    actions
        .iter()
        .filter(|action| {
            matches!(
                action,
                ApplyAction::Materialize { record } if record.kind == deltaweave_core::SyncEntryKind::File
            )
        })
        .count() as u64
}

fn count_deletes(actions: &[ApplyAction]) -> u64 {
    actions
        .iter()
        .filter(|action| matches!(action, ApplyAction::Delete { .. }))
        .count() as u64
}

fn collect_conflicts(root: &Path, dir: &Path, out: &mut Vec<ConflictInfo>) -> Result<()> {
    let entries = fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_conflicts(root, &path, out)?;
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if conflict_canonical_name(&name).is_some() {
            let portable = portable_from(root, &path)?;
            let canonical = canonical_from_conflict_path(&portable)?;
            out.push(ConflictInfo {
                path: canonical,
                conflict_path: Some(portable),
                winner_hash: String::new(),
                loser_hash: String::new(),
            });
        }
    }
    Ok(())
}

fn find_conflict_copy(root: &Path, canonical: &str) -> Result<Option<PathBuf>> {
    let canonical_path = WirePath::new(canonical).context("invalid canonical conflict path")?;
    let (parent, file_name) = canonical_path
        .as_str()
        .rsplit_once('/')
        .map_or(("", canonical_path.as_str()), |(parent, name)| {
            (parent, name)
        });
    let dir = if parent.is_empty() {
        root.to_path_buf()
    } else {
        join_portable(root, parent)?
    };
    if !dir.is_dir() {
        return Ok(None);
    }
    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if conflict_canonical_name(&name).as_deref() == Some(file_name) {
            return Ok(Some(entry.path()));
        }
    }
    Ok(None)
}

fn conflict_canonical_name(name: &str) -> Option<String> {
    let marker = name.find(".conflict-")?;
    let stem = &name[..marker];
    let suffix = &name[marker + ".conflict-".len()..];
    let extension_index = suffix.find('.');
    let (token, extension) =
        extension_index.map_or((suffix, ""), |index| (&suffix[..index], &suffix[index..]));
    let token = token.split_once('-').map_or(token, |(hash, _)| hash);
    if stem.is_empty()
        || token.len() < 12
        || !token.chars().all(|character| character.is_ascii_hexdigit())
    {
        return None;
    }
    Some(format!("{stem}{extension}"))
}

fn canonical_from_conflict_path(path: &str) -> Result<String> {
    let (parent, name) = path
        .rsplit_once('/')
        .map_or(("", path), |(parent, name)| (parent, name));
    let canonical = conflict_canonical_name(name).context("invalid conflict copy name")?;
    if parent.is_empty() {
        Ok(canonical)
    } else {
        Ok(format!("{parent}/{canonical}"))
    }
}

fn retained_copy_path(copy: &Path) -> Result<PathBuf> {
    let name = copy
        .file_name()
        .and_then(|name| name.to_str())
        .context("conflict copy name is not UTF-8")?;
    let retained = name.replacen(".conflict-", ".kept-", 1);
    Ok(copy.with_file_name(retained))
}

fn join_portable(root: &Path, path: &str) -> Result<PathBuf> {
    let portable = WirePath::new(path).context("invalid portable path")?;
    let mut joined = root.to_path_buf();
    for component in portable.components() {
        joined.push(component);
    }
    Ok(joined)
}

fn portable_from(root: &Path, path: &Path) -> Result<String> {
    let relative = path
        .strip_prefix(root)
        .with_context(|| format!("{} is outside {}", path.display(), root.display()))?;
    Ok(relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_parent_components() {
        let root = tempfile::tempdir().unwrap();
        let outside = root.path().parent().unwrap().join("outside.txt");
        std::fs::write(&outside, b"outside").unwrap();
        let result = resolve_conflict(root.path(), "../outside.txt", ConflictAction::KeepLocal);
        assert!(result.is_err());
        assert_eq!(std::fs::read(outside).unwrap(), b"outside");
    }

    #[test]
    fn keep_local_removes_conflict_copy() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), b"local").unwrap();
        let copy = root.path().join("file.conflict-abcdef123456.txt");
        std::fs::write(&copy, b"remote").unwrap();
        resolve_conflict(root.path(), "file.txt", ConflictAction::KeepLocal).unwrap();
        assert!(!copy.exists());
        assert_eq!(
            std::fs::read(root.path().join("file.txt")).unwrap(),
            b"local"
        );
    }
}
