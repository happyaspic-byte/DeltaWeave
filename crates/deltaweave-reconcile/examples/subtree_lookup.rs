//! Isolates local record lookups for matching children during sparse synchronization.
//!
//! Run with `cargo run --release -p deltaweave-reconcile --example subtree_lookup -- 4000 7`.
//! Arguments are record count and measured sample count; one warmup is always emitted first.

use std::error::Error;
use std::hint::black_box;
use std::time::{Duration, Instant};

use deltaweave_core::{
    Hash32, ReplicaId, SYNC_RECORD_SCHEMA_V1, SyncEntryKind, SyncRecord, VersionVector, WirePath,
};
use deltaweave_reconcile::MerkleTree;

fn measure_lookups(tree: &MerkleTree, unchanged: &[&SyncRecord]) -> Duration {
    let started = Instant::now();
    for expected in unchanged {
        let actual = tree
            .records_under(black_box(expected.path.as_str()))
            .expect("fixture prefix is valid");
        assert_eq!(actual.as_slice(), std::slice::from_ref(*expected));
        drop(black_box(actual));
    }
    started.elapsed()
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let count = args.next().map_or(Ok(4_000_usize), |arg| arg.parse())?;
    let samples = args.next().map_or(Ok(7_usize), |arg| arg.parse())?;
    if args.next().is_some() || count < 2 || samples == 0 {
        return Err("usage: subtree_lookup [record_count >= 2] [sample_count >= 1]".into());
    }

    let replica = ReplicaId(Hash32::digest(b"subtree-lookup-benchmark"));
    let mut version = VersionVector::default();
    version.observe(replica, 1);
    let records: Vec<_> = (0..count)
        .map(|index| {
            let payload = format!("payload-{index:08}");
            SyncRecord {
                schema_version: SYNC_RECORD_SCHEMA_V1,
                path: WirePath::new(format!("file-{index:08}.bin"))
                    .expect("fixture path is portable"),
                kind: SyncEntryKind::File,
                size: payload.len() as u64,
                content_hash: Some(Hash32::digest(payload.as_bytes())),
                readonly: false,
                version: version.clone(),
                tombstone: false,
            }
        })
        .collect();
    let tree = MerkleTree::from_records(records.iter().cloned())?;
    assert_eq!(tree.len(), count);

    // The changed child is fetched remotely; all remaining children use local records_under.
    let changed = count / 2;
    let unchanged: Vec<_> = records
        .iter()
        .enumerate()
        .filter_map(|(index, record)| (index != changed).then_some(record))
        .collect();
    assert_eq!(unchanged.len(), count - 1);

    println!("phase,sample,records,queries,returned_records,elapsed_ms,root_hash");
    let warmup = measure_lookups(&tree, &unchanged);
    println!(
        "warmup,0,{count},{},{},{:.6},{}",
        unchanged.len(),
        unchanged.len(),
        warmup.as_secs_f64() * 1_000.0,
        tree.root_hash()
    );
    for sample in 1..=samples {
        let elapsed = measure_lookups(&tree, &unchanged);
        println!(
            "measured,{sample},{count},{},{},{:.6},{}",
            unchanged.len(),
            unchanged.len(),
            elapsed.as_secs_f64() * 1_000.0,
            tree.root_hash()
        );
    }
    Ok(())
}
