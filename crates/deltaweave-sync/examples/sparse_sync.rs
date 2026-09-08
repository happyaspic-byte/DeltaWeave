//! Reproducible loopback benchmark for sparse changes in an already synchronized namespace.
//!
//! Run with: cargo run --release -p deltaweave-sync --example sparse_sync -- 1000 7 flat 1
//! Arguments are file count, measured samples, layout (`flat` or `nested`), and warmup pairs.
//! Only `SyncEngine::sync_once` is timed for edit/no-op rows. Fixture creation, verified offline
//! alignment of fixture causal history, and independent final content verification are separate rows.

use std::{
    collections::HashSet,
    env, fs,
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{Context, Result, ensure};
use deltaweave_core::{ChunkingProfile, Hash32, ReplicaId, SyncRecord};
use deltaweave_index::{IndexOptions, LocalIndex, PathRecord};
use deltaweave_net::{NetworkMode, PeerPolicy, ServerConfig, SyncClient, start_server};
use deltaweave_reconcile::MerkleTree;
use deltaweave_sync::{SyncConfig, SyncEngine, SyncReport};
use iroh::SecretKey;
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use tempfile::TempDir;

const FILE_SIZE: usize = 1024;
// This benchmark-only helper depends on deltaweave-index's private RECORDS table name.
// It edits only a closed, newly created TempDir fixture DB after authoritative scans.
// Keep the private schema dependency in one place and fail if the table or keys differ.
const FIXTURE_RECORDS: TableDefinition<&str, &[u8]> = TableDefinition::new("path_records");

#[derive(Clone, Copy)]
enum Layout {
    Flat,
    Nested,
}

impl Layout {
    fn name(self) -> &'static str {
        match self {
            Self::Flat => "flat",
            Self::Nested => "nested",
        }
    }

    fn path(self, index: usize) -> PathBuf {
        let file = format!("f{index:08}.bin");
        match self {
            Self::Flat => PathBuf::from(file),
            Self::Nested => PathBuf::from(format!("group{:04}", index / 128)).join(file),
        }
    }

    fn edit_queries(self) -> usize {
        match self {
            Self::Flat => 2,
            Self::Nested => 3,
        }
    }
}

struct Options {
    count: usize,
    samples: usize,
    layout: Layout,
    warmups: usize,
}

struct ResourceCounters {
    ticks_per_second: Option<f64>,
}

struct ResourceUsage {
    cpu_seconds: Option<f64>,
    rss_kib: Option<u64>,
    high_water_kib: Option<u64>,
}

impl ResourceCounters {
    fn new() -> Self {
        // Resolve Linux's clock tick rate once, before any setup or sync measurement.
        #[cfg(target_os = "linux")]
        let ticks_per_second = std::process::Command::new("getconf")
            .arg("CLK_TCK")
            .output()
            .ok()
            .filter(|output| output.status.success())
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value > 0.0);
        #[cfg(not(target_os = "linux"))]
        let ticks_per_second = None;
        Self { ticks_per_second }
    }

    fn capture(&self) -> ResourceUsage {
        #[cfg(target_os = "linux")]
        {
            let cpu_seconds = self.ticks_per_second.and_then(|ticks_per_second| {
                let stat = fs::read_to_string("/proc/self/stat").ok()?;
                // comm is parenthesized and can itself contain spaces or closing parentheses.
                // The remainder starts with field 3, so utime/stime (14/15) are indices 11/12.
                let (_, remaining) = stat.rsplit_once(") ")?;
                let mut fields = remaining.split_whitespace();
                let user = fields.nth(11)?.parse::<u64>().ok()?;
                let system = fields.next()?.parse::<u64>().ok()?;
                Some(user.checked_add(system)? as f64 / ticks_per_second)
            });
            let status = fs::read_to_string("/proc/self/status").ok();
            let kib = |name: &str| {
                status.as_ref()?.lines().find_map(|line| {
                    line.strip_prefix(name)?
                        .split_whitespace()
                        .next()?
                        .parse::<u64>()
                        .ok()
                })
            };
            ResourceUsage {
                cpu_seconds,
                rss_kib: kib("VmRSS:"),
                // VmHWM is the process lifetime peak, including setup. VmRSS is the current
                // post-sample resident set; neither is presented as a per-sync peak.
                high_water_kib: kib("VmHWM:"),
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = self.ticks_per_second;
            ResourceUsage {
                cpu_seconds: None,
                rss_kib: None,
                high_water_kib: None,
            }
        }
    }
}

fn resource_csv(before: &ResourceUsage, after: &ResourceUsage) -> String {
    let cpu_ms = before
        .cpu_seconds
        .zip(after.cpu_seconds)
        .map(|(before, after)| format!("{:.3}", (after - before) * 1000.0))
        .unwrap_or_default();
    let rss_kib = after
        .rss_kib
        .map(|value| value.to_string())
        .unwrap_or_default();
    let high_water_kib = after
        .high_water_kib
        .map(|value| value.to_string())
        .unwrap_or_default();
    format!("{cpu_ms},{rss_kib},{high_water_kib}")
}

impl Options {
    fn parse() -> Result<Self> {
        let args: Vec<_> = env::args().skip(1).collect();
        ensure!(
            args.len() <= 4,
            "usage: sparse_sync [file-count] [samples] [flat|nested] [warmup-pairs]"
        );
        let count = parse_number(args.first(), 1000)?;
        let samples = parse_number(args.get(1), 7)?;
        let layout = match args.get(2).map(String::as_str).unwrap_or("flat") {
            "flat" => Layout::Flat,
            "nested" => Layout::Nested,
            other => anyhow::bail!("unknown layout {other:?}; expected flat or nested"),
        };
        let warmups = parse_number(args.get(3), 1)?;
        ensure!(count > 0, "file count must be positive");
        ensure!(samples > 0, "sample count must be positive");
        samples
            .checked_add(warmups)
            .context("sample and warmup count overflow")?;
        Ok(Self {
            count,
            samples,
            layout,
            warmups,
        })
    }
}

fn parse_number(value: Option<&String>, default: usize) -> Result<usize> {
    value.map_or(Ok(default), |value| {
        value
            .parse()
            .with_context(|| format!("invalid count {value:?}"))
    })
}

fn replica(key: &SecretKey) -> ReplicaId {
    ReplicaId(Hash32::digest(key.public().as_bytes()))
}

fn bytes(index: usize, revision: usize) -> [u8; FILE_SIZE] {
    let mut output = [0_u8; FILE_SIZE];
    let mut state = (index as u64)
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(revision as u64)
        .wrapping_add(0x6a09_e667_f3bc_c909);
    for byte in &mut output {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state as u8;
    }
    // Include the revision explicitly so every edit has unique content even if a PRNG seed
    // collides with another fixture file's seed.
    output[..8].copy_from_slice(&(index as u64).to_le_bytes());
    output[8..16].copy_from_slice(&(revision as u64).to_le_bytes());
    output
}

fn align_fixture_versions(
    database_path: &Path,
    scanned_local: &[PathRecord],
    remote_records: &[SyncRecord],
    client_replica: ReplicaId,
) -> Result<()> {
    ensure!(
        scanned_local.len() == remote_records.len(),
        "fixture snapshot record counts differ"
    );
    let database = Database::open(database_path)?;
    {
        // A write transaction would create an absent table; explicitly require the scan's
        // existing table before opening the sole fixture mutation transaction.
        let read = database.begin_read()?;
        let table = read
            .open_table(FIXTURE_RECORDS)
            .context("fixture index no longer contains the expected record table")?;
        ensure!(
            table.len()? == scanned_local.len() as u64,
            "fixture record table count differs from the scanned snapshot"
        );
    }
    let write = database.begin_write()?;
    {
        let mut table = write.open_table(FIXTURE_RECORDS)?;
        for (expected_local, remote) in scanned_local.iter().zip(remote_records) {
            ensure!(
                expected_local.to_sync_record().same_state(remote),
                "fixture portable state differs at {}",
                remote.path
            );
            ensure!(
                remote.version.get(client_replica) == 0,
                "fixture remote history unexpectedly contains the client replica"
            );
            let mut record: PathRecord = {
                let encoded = table
                    .get(remote.path.as_str())?
                    .with_context(|| format!("fixture index key is missing: {}", remote.path))?;
                postcard::from_bytes(encoded.value()).context("invalid fixture path record")?
            };
            ensure!(
                record.path == remote.path && record == *expected_local,
                "fixture key or record differs from the authoritative scan at {}",
                remote.path
            );
            // Preserve every other PathRecord field and all other tables: filesystem identity,
            // schema, root/replica binding, generation, retries, and the global client counter.
            // The unchanged local counter already dominates the incoming client component (0).
            record.version = remote.version.clone();
            let encoded = postcard::to_stdvec(&record)?;
            table.insert(remote.path.as_str(), encoded.as_slice())?;
        }
    }
    write.commit()?;
    Ok(())
}

fn seed(
    options: &Options,
    local_root: &Path,
    local_state: &Path,
    remote_root: &Path,
    remote_state: &Path,
    client_key: &SecretKey,
    server_key: &SecretKey,
) -> Result<Hash32> {
    for root in [local_root, remote_root] {
        fs::create_dir_all(root)?;
        for index in 0..options.count {
            let path = root.join(options.layout.path(index));
            fs::create_dir_all(path.parent().context("fixture path has no parent")?)?;
            fs::write(path, bytes(index, 0))?;
        }
    }

    let remote = LocalIndex::open(
        remote_root,
        remote_state.join("index.redb"),
        replica(server_key),
        IndexOptions::default(),
    )?;
    let scan = remote.scan()?;
    ensure!(
        scan.issues.is_empty() && scan.collisions.is_empty() && scan.retries_queued == 0,
        "remote seed scan was incomplete"
    );
    ensure!(
        scan.files_hashed == options.count,
        "unexpected seed file count"
    );
    let records = remote.sync_records()?;
    let local_database = local_state.join("index.redb");
    let local = LocalIndex::open(
        local_root,
        &local_database,
        replica(client_key),
        IndexOptions::default(),
    )?;
    let local_scan = local.scan()?;
    ensure!(
        local_scan.issues.is_empty()
            && local_scan.collisions.is_empty()
            && local_scan.retries_queued == 0
            && local_scan.files_hashed == options.count,
        "local seed scan was incomplete"
    );
    let local_records = local.records()?;
    ensure!(
        local_records.len() == records.len()
            && local_records
                .iter()
                .zip(&records)
                .all(|(local, remote)| local.to_sync_record().same_state(remote)),
        "authoritative fixture scans disagree on portable state"
    );
    drop(local);
    // The temporary fixture uses one offline transaction instead of N public adoptions,
    // avoiding quadratic setup. Production indexing and measured sync use their normal APIs.
    align_fixture_versions(
        &local_database,
        &local_records,
        &records,
        replica(client_key),
    )?;
    let local = LocalIndex::open(
        local_root,
        &local_database,
        replica(client_key),
        IndexOptions::default(),
    )?;
    ensure!(
        local.sync_records()? == records,
        "seed causal histories differ"
    );
    let verified = local.scan()?;
    ensure!(
        verified.issues.is_empty()
            && verified.collisions.is_empty()
            && verified.retries_queued == 0
            && verified.files_hashed == options.count
            && verified.changes.is_empty()
            && local.sync_records()? == records,
        "authoritative rescan changed the aligned fixture state"
    );
    eprintln!("seed: verified {} records on both replicas", records.len());
    Ok(MerkleTree::from_records(records)?.root_hash())
}

fn check_report(report: &SyncReport, options: &Options, edited: bool) -> Result<()> {
    ensure!(report.status == "pass", "sync did not pass");
    ensure!(
        report.desired_root == report.verified_local_root
            && report.desired_root == report.verified_remote_root,
        "final roots differ"
    );
    ensure!(report.conflicts.is_empty(), "unexpected fixture conflict");
    ensure!(report.remote_actions == 0, "unexpected remote action");
    ensure!(report.pushed_bytes == 0, "unexpected pushed payload");
    if edited {
        ensure!(
            report.local_actions == 1,
            "edit did not produce one local action"
        );
        ensure!(
            report.merkle_queries == options.layout.edit_queries(),
            "unexpected sparse-edit query count"
        );
        ensure!(
            report.pulled_bytes == FILE_SIZE as u64,
            "unexpected pulled payload"
        );
        ensure!(
            report.pulled_remote_files == 1,
            "edit did not pull one file"
        );
        ensure!(
            report.local_before_root != report.remote_before_root,
            "edit unexpectedly began with equal roots"
        );
    } else {
        ensure!(report.local_actions == 0, "no-op produced local actions");
        ensure!(report.merkle_queries == 1, "no-op missed root fast path");
        ensure!(report.pulled_bytes == 0, "no-op pulled payload");
        ensure!(report.pulled_remote_files == 0, "no-op pulled a file");
        ensure!(
            report.local_before_root == report.remote_before_root,
            "no-op began with unequal roots"
        );
    }
    Ok(())
}

async fn timed_sync(
    engine: &SyncEngine,
    options: &Options,
    counters: &ResourceCounters,
    phase: &str,
    iteration: usize,
    edited: bool,
) -> Result<()> {
    let before = counters.capture();
    let start = Instant::now();
    let report = engine.sync_once().await?;
    let elapsed = start.elapsed();
    let after = counters.capture();
    check_report(&report, options, edited)?;
    println!(
        "{phase},{},{iteration},{},{},{:.3},{},{},{},{},{},{},{},true,false,{}",
        if edited { "edit" } else { "noop" },
        options.count,
        options.layout.name(),
        elapsed.as_secs_f64() * 1000.0,
        resource_csv(&before, &after),
        report.local_actions,
        report.remote_actions,
        report.merkle_queries,
        report.pulled_bytes,
        report.pushed_bytes,
        report.conflicts.len(),
        report.desired_root,
    );
    Ok(())
}

fn file_count(root: &Path) -> Result<usize> {
    let mut count = 0;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_dir() {
            count += file_count(&entry.path())?;
        } else {
            ensure!(kind.is_file(), "unexpected non-file fixture entry");
            count += 1;
        }
    }
    Ok(count)
}

fn verify_contents(
    options: &Options,
    local_root: &Path,
    remote_root: &Path,
    changed_index: usize,
    revision: usize,
) -> Result<Hash32> {
    ensure!(
        file_count(local_root)? == options.count,
        "local file count differs"
    );
    ensure!(
        file_count(remote_root)? == options.count,
        "remote file count differs"
    );
    // Hash path names and independently read content digests in deterministic index order.
    // The fixed-size digest stream keeps verification memory proportional to file count,
    // without retaining all fixture bytes or consulting either index/CAS.
    let mut local_digests = Vec::new();
    let mut remote_digests = Vec::new();
    for index in 0..options.count {
        let relative = options.layout.path(index);
        let local = fs::read(local_root.join(&relative))?;
        let remote = fs::read(remote_root.join(&relative))?;
        let expected = bytes(index, if index == changed_index { revision } else { 0 });
        ensure!(
            local == expected,
            "local content mismatch at {}",
            relative.display()
        );
        ensure!(
            remote == expected,
            "remote content mismatch at {}",
            relative.display()
        );
        let path = relative.to_string_lossy().replace('\\', "/");
        let path_hash = Hash32::digest(path.as_bytes());
        local_digests.extend_from_slice(path_hash.as_bytes());
        local_digests.extend_from_slice(Hash32::digest(&local).as_bytes());
        remote_digests.extend_from_slice(path_hash.as_bytes());
        remote_digests.extend_from_slice(Hash32::digest(&remote).as_bytes());
    }
    let local_hash = Hash32::digest(&local_digests);
    ensure!(
        local_hash == Hash32::digest(&remote_digests),
        "independent filesystem digests differ"
    );
    Ok(local_hash)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let options = Options::parse()?;
    let counters = ResourceCounters::new();
    let fixture = TempDir::new()?;
    let local_root = fixture.path().join("local-root");
    let local_state = fixture.path().join("local-state");
    let remote_root = fixture.path().join("remote-root");
    let remote_state = fixture.path().join("remote-state");
    // Public, test-only identities keep causal roots reproducible across baseline/after runs.
    // They are only used by this temporary loopback fixture and are never persisted.
    let client_key = SecretKey::from_bytes(&[7; 32]);
    let server_key = SecretKey::from_bytes(&[9; 32]);

    println!(
        "phase,operation,iteration,count,layout,elapsed_ms,cpu_ms,rss_kib,process_hwm_kib,local_actions,remote_actions,merkle_queries,pulled_bytes,pushed_bytes,conflicts,roots_equal,content_verified,digest"
    );
    let before = counters.capture();
    let start = Instant::now();
    let seed_root = seed(
        &options,
        &local_root,
        &local_state,
        &remote_root,
        &remote_state,
        &client_key,
        &server_key,
    )?;
    let elapsed = start.elapsed();
    let after = counters.capture();
    println!(
        "setup,seed,0,{},{},{:.3},{},0,0,0,0,0,0,true,false,{seed_root}",
        options.count,
        options.layout.name(),
        elapsed.as_secs_f64() * 1000.0,
        resource_csv(&before, &after),
    );
    let server = start_server(ServerConfig {
        secret_key: server_key,
        destination_root: remote_root.clone(),
        state_root: remote_state,
        peer_policy: PeerPolicy::AllowListed(HashSet::from([client_key.public()])),
        network_mode: NetworkMode::DirectOnly,
        bind_address: Some("127.0.0.1:0".parse()?),
        max_connections: 64,
        min_free_space_bytes: 0,
    })
    .await?;
    let engine = SyncEngine::open(SyncConfig {
        swarm_sources: Vec::new(),
        root: local_root.clone(),
        state_root: local_state,
        replica: replica(&client_key),
        client: SyncClient {
            secret_key: client_key,
            remote: server.endpoint_addr(),
            network_mode: NetworkMode::DirectOnly,
        },
        profile: ChunkingProfile::DEFAULT,
        ignored_paths: Vec::new(),
    })?;
    timed_sync(&engine, &options, &counters, "initial", 0, false).await?;

    let changed_index = options.count / 2;
    let mut revision = 0;
    for (phase, count) in [("warmup", options.warmups), ("sample", options.samples)] {
        for iteration in 1..=count {
            revision += 1;
            fs::write(
                remote_root.join(options.layout.path(changed_index)),
                bytes(changed_index, revision),
            )?;
            timed_sync(&engine, &options, &counters, phase, iteration, true).await?;
            timed_sync(&engine, &options, &counters, phase, iteration, false).await?;
        }
    }

    let before = counters.capture();
    let start = Instant::now();
    let digest = verify_contents(&options, &local_root, &remote_root, changed_index, revision)?;
    let elapsed = start.elapsed();
    let after = counters.capture();
    println!(
        "validation,filesystem,0,{},{},{:.3},{},0,0,0,0,0,0,true,true,{digest}",
        options.count,
        options.layout.name(),
        elapsed.as_secs_f64() * 1000.0,
        resource_csv(&before, &after),
    );
    drop(engine);
    server.shutdown().await?;
    Ok(())
}
