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
    /// Direct address of the peer, if known.
    #[serde(default)]
    pub peer_address: Option<String>,
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
    data_root: PathBuf,
}

impl fmt::Debug for ConfigStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ConfigStore(..)")
    }
}

impl ConfigStore {
    /// Parent directory of the configuration database.
    #[must_use]
    pub fn data_root(&self) -> &Path {
        &self.data_root
    }

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
        let data_root = path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        Ok(Self {
            database,
            data_root,
        })
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

    /// Creates and persists one GUI job.
    pub fn create_job(
        &self,
        name: String,
        local_root: PathBuf,
        peer_endpoint_id: String,
        peer_address: Option<String>,
        direction: Direction,
    ) -> Result<JobConfig> {
        ensure!(!name.trim().is_empty(), "job name must not be empty");
        ensure!(
            peer_endpoint_id.len() == 64
                && peer_endpoint_id
                    .chars()
                    .all(|character| character.is_ascii_hexdigit()),
            "invalid peer endpoint ID"
        );
        let id = job_id(&local_root, &peer_endpoint_id);
        let job = JobConfig {
            state_root: self.data_root.join("jobs").join(&id),
            id,
            name,
            local_root,
            peer_endpoint_id,
            peer_address,
            direction,
            continuous: true,
            paused: false,
        };
        self.insert_job(&job)?;
        Ok(job)
    }

    /// Updates the durable pause flag for one job.
    pub fn set_paused(&self, id: &str, paused: bool) -> Result<()> {
        let write = self.database.begin_write()?;
        {
            let mut jobs = write.open_table(JOBS)?;
            let encoded = jobs
                .get(id)?
                .map(|value| value.value().to_vec())
                .ok_or_else(|| anyhow::anyhow!("unknown job"))?;
            let mut job: JobConfig =
                postcard::from_bytes(&encoded).context("invalid job record")?;
            job.paused = paused;
            let encoded = postcard::to_stdvec(&job)?;
            jobs.insert(id, encoded.as_slice())?;
        }
        write.commit()?;
        Ok(())
    }
}

fn job_id(local_root: &Path, peer_endpoint_id: &str) -> String {
    let canonical = local_root
        .canonicalize()
        .unwrap_or_else(|_| local_root.to_path_buf());
    let digest = deltaweave_core::Hash32::digest(
        format!("{}:{peer_endpoint_id}", canonical.display()).as_bytes(),
    );
    digest.to_hex()[..16].to_owned()
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
