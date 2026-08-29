//! Dry-run merge counts and conflict-copy resolution without applying.

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
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
    match action {
        ConflictAction::KeepBoth => {}
        ConflictAction::KeepLocal => {}
        ConflictAction::KeepRemote => {
            let copy = find_conflict_copy(root, path)?;
            let Some(copy) = copy else {
                bail!("no conflict copy for {path}");
            };
            fs::copy(&copy, &canonical)
                .with_context(|| format!("failed to restore {}", canonical.display()))?;
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
        if let Some(index) = name.find(".conflict-") {
            let stem = &name[..index];
            let portable = portable_from(root, &path)?;
            let parent = portable.rsplit_once('/').map_or("", |(parent, _)| parent);
            let canonical = if parent.is_empty() {
                stem.to_owned()
            } else {
                format!("{parent}/{stem}")
            };
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
    let parent = Path::new(canonical).parent().unwrap_or(Path::new(""));
    let stem = Path::new(canonical)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(canonical);
    let dir = if parent.as_os_str().is_empty() {
        root.to_path_buf()
    } else {
        join_portable(root, &parent.to_string_lossy())?
    };
    if !dir.is_dir() {
        return Ok(None);
    }
    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(&format!("{stem}.conflict-")) {
            return Ok(Some(entry.path()));
        }
    }
    Ok(None)
}

fn join_portable(root: &Path, path: &str) -> Result<PathBuf> {
    let mut joined = root.to_path_buf();
    if !path.is_empty() {
        for component in path.split('/') {
            joined.push(component);
        }
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
