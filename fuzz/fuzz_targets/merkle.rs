#![no_main]

use deltaweave_core::{
    Hash32, ReplicaId, SYNC_RECORD_SCHEMA_V1, SyncEntryKind, SyncRecord, VersionVector, WirePath,
};
use deltaweave_reconcile::{MerkleTree, actions_to_reach, merge_snapshots};
use libfuzzer_sys::fuzz_target;

fn record(replica: ReplicaId, path: &str, counter: u64, bytes: &[u8], tombstone: bool) -> SyncRecord {
    let mut version = VersionVector::default();
    version.observe(replica, counter.saturating_add(1));
    SyncRecord {
        schema_version: SYNC_RECORD_SCHEMA_V1,
        path: WirePath::new(path).expect("generated fuzz path is portable"),
        kind: SyncEntryKind::File,
        size: bytes.len() as u64,
        content_hash: Some(Hash32::digest(bytes)),
        readonly: false,
        version,
        tombstone,
    }
}

fuzz_target!(|data: &[u8]| {
    let split = data.len() / 2;
    let alpha = ReplicaId(Hash32::digest(b"fuzz-alpha"));
    let beta = ReplicaId(Hash32::digest(b"fuzz-beta"));
    let discriminator = data.first().copied().unwrap_or_default();
    let path = format!("bucket-{}/value.bin", discriminator % 16);
    let left = MerkleTree::from_records([record(
        alpha,
        &path,
        u64::from(discriminator),
        &data[..split],
        discriminator & 1 != 0,
    )])
    .expect("left fuzz tree is valid");
    let right = MerkleTree::from_records([record(
        beta,
        &path,
        u64::from(discriminator.rotate_left(1)),
        &data[split..],
        discriminator & 2 != 0,
    )])
    .expect("right fuzz tree is valid");

    let forward = merge_snapshots(&left, &right).expect("forward merge succeeds");
    let reverse = merge_snapshots(&right, &left).expect("reverse merge succeeds");
    assert_eq!(forward, reverse);
    let desired = forward.tree().expect("merged tree is valid");
    assert_eq!(left.different_paths(&right), right.different_paths(&left));
    let _ = actions_to_reach(&left, &forward).expect("left actions are valid");
    let _ = actions_to_reach(&right, &forward).expect("right actions are valid");
    assert_eq!(
        merge_snapshots(&desired, &desired)
            .expect("idempotent merge succeeds")
            .tree()
            .expect("idempotent tree is valid")
            .root_hash(),
        desired.root_hash()
    );
});
