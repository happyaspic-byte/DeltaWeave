//! Durable job configuration store.

use std::{
    fmt, fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail, ensure};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

const SCHEMA_VERSION: u16 = 1;
const META: TableDefinition<&str, u16> = TableDefinition::new("meta");
const JOBS: TableDefinition<&str, &[u8]> = TableDefinition::new("jobs");
const SCHEMA_KEY: &str = "schema";

/// Transfer direction for one job.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// Both sides may apply changes.
    Bidirectional,
    /// Local changes are pushed; remote changes are not applied locally.
    SendOnly,
    /// Remote changes are applied; local changes are not pushed.
    ReceiveOnly,
}

/// One persisted folder-to-peer job.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct JobConfig {
    /// Stable job identifier.
    pub id: String,
    /// User-visible name.
    pub name: String,
    /// Folder synchronized on this machine.
    pub local_root: PathBuf,
    /// Private index/store/access directory.
    pub state_root: PathBuf,
    /// Remote endpoint id as hex.
    pub peer_endpoint_id: String,
    /// Transfer direction.
    pub direction: Direction,
    /// Whether the job runs continuously.
    pub continuous: bool,
    /// Whether the job is paused.
    pub paused: bool,
}

/// Exclusive redb-backed job list.
pub struct ConfigStore {
    database: Database,
}

impl fmt::Debug for ConfigStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ConfigStore(..)")
    }
}

impl ConfigStore {
    /// Opens or creates the configuration database.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let database = Database::create(path)
            .with_context(|| format!("failed to open config DB {}", path.display()))?;
        let write = database.begin_write()?;
        {
            let mut meta = write.open_table(META)?;
            let schema = meta.get(SCHEMA_KEY)?.map(|existing| existing.value());
            match schema {
                Some(existing) => ensure!(
                    existing == SCHEMA_VERSION,
                    "unsupported config schema {existing}"
                ),
                None => {
                    meta.insert(SCHEMA_KEY, SCHEMA_VERSION)?;
                }
            }
        }
        write.open_table(JOBS)?;
        write.commit()?;
        Ok(Self { database })
    }

    /// Inserts a job after rejecting overlapping roots and duplicate canonical folders.
    pub fn insert_job(&self, job: &JobConfig) -> Result<()> {
        ensure!(!job.id.is_empty(), "job id must not be empty");
        let local_root = canonicalize_existing(&job.local_root, "local root")?;
        let state_root = prepare_state_root(&job.state_root)?;
        ensure!(
            !local_root.starts_with(&state_root) && !state_root.starts_with(&local_root),
            "local root and state root must not overlap"
        );
        let stored = JobConfig {
            local_root,
            state_root,
            ..job.clone()
        };

        let write = self.database.begin_write()?;
        {
            let mut jobs = write.open_table(JOBS)?;
            if jobs.get(stored.id.as_str())?.is_some() {
                bail!("job {} already exists", stored.id);
            }
            for entry in jobs.iter()? {
                let (_, encoded) = entry?;
                let existing: JobConfig =
                    postcard::from_bytes(encoded.value()).context("invalid job record")?;
                if existing.local_root == stored.local_root {
                    bail!("{} already has a job", stored.local_root.display());
                }
                if paths_overlap(&existing.local_root, &stored.local_root)
                    || paths_overlap(&existing.state_root, &stored.state_root)
                    || paths_overlap(&existing.local_root, &stored.state_root)
                    || paths_overlap(&existing.state_root, &stored.local_root)
                {
                    bail!("job roots overlap an existing job");
                }
            }
            let encoded = postcard::to_stdvec(&stored)?;
            jobs.insert(stored.id.as_str(), encoded.as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    /// Returns every job ordered by id.
    pub fn list_jobs(&self) -> Result<Vec<JobConfig>> {
        let read = self.database.begin_read()?;
        let jobs = read.open_table(JOBS)?;
        let mut records: Vec<JobConfig> = Vec::new();
        for entry in jobs.iter()? {
            let (_, encoded) = entry?;
            records.push(postcard::from_bytes(encoded.value()).context("invalid job record")?);
        }
        records.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(records)
    }
}

fn canonicalize_existing(path: &Path, label: &str) -> Result<PathBuf> {
    ensure!(path.exists(), "{label} {} does not exist", path.display());
    fs::canonicalize(path).with_context(|| format!("failed to canonicalize {label}"))
}

fn prepare_state_root(path: &Path) -> Result<PathBuf> {
    fs::create_dir_all(path)
        .with_context(|| format!("failed to create state root {}", path.display()))?;
    fs::canonicalize(path).with_context(|| format!("failed to canonicalize {}", path.display()))
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}
