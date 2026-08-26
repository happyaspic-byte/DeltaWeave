//! Deterministic Merkle state comparison and causal replica reconciliation.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use deltaweave_core::{
    CausalRelation, Hash32, ReplicaId, SyncEntryKind, SyncRecord, SyncRecordError, WirePath,
    WirePathError,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Immutable deterministic Merkle tree over portable path records.
#[derive(Clone, Debug)]
pub struct MerkleTree {
    root: MerkleNode,
    records: BTreeMap<WirePath, SyncRecord>,
}

/// Constant-size digest and cardinality for one immediate child subtree.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MerkleChildSummary {
    /// Validated path-component name beneath the queried prefix.
    pub name: String,
    /// Digest of the complete child subtree.
    pub hash: Hash32,
    /// Number of records represented by the child subtree.
    pub record_count: usize,
}

/// Network-friendly description of one Merkle node.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MerkleNodeSummary {
    /// Empty for the root, otherwise a portable path prefix.
    pub prefix: String,
    /// Digest of this complete subtree.
    pub hash: Hash32,
    /// Number of records represented by this subtree.
    pub record_count: usize,
    /// Record at this exact path, if the namespace contains one.
    pub record: Option<SyncRecord>,
    /// Immediate child summaries in portable component order.
    pub children: Vec<MerkleChildSummary>,
}

#[derive(Clone, Debug, Default)]
struct MerkleNode {
    record_hash: Option<Hash32>,
    children: BTreeMap<String, MerkleNode>,
    subtree_hash: Hash32,
    record_count: usize,
}

impl MerkleTree {
    /// Builds a canonical tree independent of record input order.
    pub fn from_records(
        records: impl IntoIterator<Item = SyncRecord>,
    ) -> Result<Self, ReconcileError> {
        let mut map = BTreeMap::new();
        for record in records {
            record.validate()?;
            if map.insert(record.path.clone(), record).is_some() {
                return Err(ReconcileError::DuplicatePath);
            }
        }

        let mut root = MerkleNode::default();
        for record in map.values() {
            root.insert(record);
        }
        root.finalize();
        Ok(Self { root, records: map })
    }

    /// Returns the root digest used as the constant-size synchronization fast path.
    #[must_use]
    pub const fn root_hash(&self) -> Hash32 {
        self.root.subtree_hash
    }

    /// Returns the number of versioned paths represented by the tree.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Returns whether the tree has no path records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Returns one path record.
    #[must_use]
    pub fn get(&self, path: &WirePath) -> Option<&SyncRecord> {
        self.records.get(path)
    }

    /// Iterates over records in canonical portable-path order.
    pub fn records(&self) -> impl Iterator<Item = &SyncRecord> {
        self.records.values()
    }

    /// Returns a network-friendly node summary for `prefix`; an empty prefix selects the root.
    pub fn node_summary(&self, prefix: &str) -> Result<Option<MerkleNodeSummary>, ReconcileError> {
        let components = if prefix.is_empty() {
            Vec::new()
        } else {
            WirePath::new(prefix.to_owned())?
                .components()
                .map(ToOwned::to_owned)
                .collect()
        };
        let mut node = &self.root;
        for component in &components {
            let Some(child) = node.children.get(component) else {
                return Ok(None);
            };
            node = child;
        }
        let record = if prefix.is_empty() {
            None
        } else {
            self.records
                .get(&WirePath::new(prefix.to_owned())?)
                .cloned()
        };
        Ok(Some(MerkleNodeSummary {
            prefix: prefix.to_owned(),
            hash: node.subtree_hash,
            record_count: node.record_count,
            record,
            children: node
                .children
                .iter()
                .map(|(name, child)| MerkleChildSummary {
                    name: name.clone(),
                    hash: child.subtree_hash,
                    record_count: child.record_count,
                })
                .collect(),
        }))
    }

    /// Returns canonical records beneath `prefix`; an empty prefix returns the full snapshot.
    pub fn records_under(&self, prefix: &str) -> Result<Vec<SyncRecord>, ReconcileError> {
        if prefix.is_empty() {
            return Ok(self.records().cloned().collect());
        }
        let prefix = WirePath::new(prefix.to_owned())?;
        let child_prefix = format!("{}/", prefix.as_str());
        Ok(self
            .records
            .iter()
            .filter(|(path, _)| {
                path.as_str() == prefix.as_str() || path.as_str().starts_with(&child_prefix)
            })
            .map(|(_, record)| record.clone())
            .collect())
    }

    /// Descends only mismatched Merkle subtrees and returns their record paths.
    #[must_use]
    pub fn different_paths(&self, other: &Self) -> Vec<WirePath> {
        let mut components = Vec::new();
        let mut paths = Vec::new();
        diff_nodes(&self.root, &other.root, &mut components, &mut paths);
        paths.sort();
        paths.dedup();
        paths
    }
}

impl MerkleNode {
    fn insert(&mut self, record: &SyncRecord) {
        let mut node = self;
        for component in record.path.components() {
            node = node.children.entry(component.to_owned()).or_default();
        }
        node.record_hash = Some(record.logical_hash());
    }

    fn finalize(&mut self) {
        let mut record_count = usize::from(self.record_hash.is_some());
        for child in self.children.values_mut() {
            child.finalize();
            record_count = record_count.saturating_add(child.record_count);
        }

        let mut hasher = blake3::Hasher::new_derive_key("deltaweave merkle path node v1");
        match self.record_hash {
            Some(hash) => {
                hasher.update(&[1]);
                hasher.update(hash.as_bytes());
            }
            None => {
                hasher.update(&[0]);
            }
        }
        for (name, child) in &self.children {
            update_sized(&mut hasher, name.as_bytes());
            hasher.update(child.subtree_hash.as_bytes());
            hasher.update(&(child.record_count as u64).to_le_bytes());
        }
        self.record_count = record_count;
        self.subtree_hash = Hash32::from_bytes(*hasher.finalize().as_bytes());
    }

    fn collect_paths(&self, components: &mut Vec<String>, paths: &mut Vec<WirePath>) {
        if self.record_hash.is_some() && !components.is_empty() {
            paths.push(
                WirePath::new(components.join("/"))
                    .expect("Merkle components originated from validated wire paths"),
            );
        }
        for (name, child) in &self.children {
            components.push(name.clone());
            child.collect_paths(components, paths);
            components.pop();
        }
    }
}

fn diff_nodes(
    left: &MerkleNode,
    right: &MerkleNode,
    components: &mut Vec<String>,
    paths: &mut Vec<WirePath>,
) {
    if left.subtree_hash == right.subtree_hash && left.record_count == right.record_count {
        return;
    }
    if left.record_hash != right.record_hash && !components.is_empty() {
        paths.push(
            WirePath::new(components.join("/"))
                .expect("Merkle components originated from validated wire paths"),
        );
    }

    let names: BTreeSet<_> = left
        .children
        .keys()
        .chain(right.children.keys())
        .cloned()
        .collect();
    for name in names {
        components.push(name.clone());
        match (left.children.get(&name), right.children.get(&name)) {
            (Some(left), Some(right)) => diff_nodes(left, right, components, paths),
            (Some(left), None) => left.collect_paths(components, paths),
            (None, Some(right)) => right.collect_paths(components, paths),
            (None, None) => unreachable!("name came from at least one child map"),
        }
        components.pop();
    }
}

/// Why two path states required a deterministic conflict resolution.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictReason {
    /// Both replicas independently advanced the path.
    ConcurrentEdit,
    /// Equal causal clocks carried different state, indicating a broken writer invariant.
    EqualClockDivergence,
}

/// Audit record for one conflict that preserved a losing live file.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConflictRecord {
    /// Original path whose canonical state was selected.
    pub path: WirePath,
    /// Deterministic portable path used to preserve the losing live file, if any.
    pub conflict_path: Option<WirePath>,
    /// Digest of the selected pre-resolution record.
    pub winner_hash: Hash32,
    /// Digest of the preserved or superseded pre-resolution record.
    pub loser_hash: Hash32,
    /// Causal anomaly class.
    pub reason: ConflictReason,
}

/// Summary of one deterministic two-replica merge.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MergeStats {
    /// Paths equal on both peers before the merge.
    pub equal: usize,
    /// Paths selected from the left snapshot by causality or absence on the right.
    pub selected_left: usize,
    /// Paths selected from the right snapshot by causality or absence on the left.
    pub selected_right: usize,
    /// Concurrent or anomalous paths resolved deterministically.
    pub conflicts: usize,
    /// Live conflict copies retained in the merged namespace.
    pub conflict_copies: usize,
}

/// Canonical namespace produced by merging two snapshots.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MergeResult {
    /// Complete canonical records in portable-path order.
    pub records: Vec<SyncRecord>,
    /// Conflict audit entries in original-path order.
    pub conflicts: Vec<ConflictRecord>,
    /// Merge counters useful for diagnostics and tests.
    pub stats: MergeStats,
}

impl MergeResult {
    /// Builds a Merkle tree for the canonical merged state.
    pub fn tree(&self) -> Result<MerkleTree, ReconcileError> {
        MerkleTree::from_records(self.records.clone())
    }
}

/// Merges two complete causal snapshots into one orientation-independent namespace.
pub fn merge_snapshots(
    left: &MerkleTree,
    right: &MerkleTree,
) -> Result<MergeResult, ReconcileError> {
    if left.root_hash() == right.root_hash() && left.len() == right.len() {
        return Ok(MergeResult {
            records: left.records().cloned().collect(),
            conflicts: Vec::new(),
            stats: MergeStats {
                equal: left.len(),
                selected_left: 0,
                selected_right: 0,
                conflicts: 0,
                conflict_copies: 0,
            },
        });
    }

    let all_paths: BTreeSet<_> = left
        .records
        .keys()
        .chain(right.records.keys())
        .cloned()
        .collect();
    let mut occupied = all_paths.clone();
    let mut merged = BTreeMap::new();
    let mut conflicts = Vec::new();
    let mut stats = MergeStats {
        equal: 0,
        selected_left: 0,
        selected_right: 0,
        conflicts: 0,
        conflict_copies: 0,
    };

    for path in all_paths {
        match (left.records.get(&path), right.records.get(&path)) {
            (Some(left), None) => {
                merged.insert(path, left.clone());
                stats.selected_left += 1;
            }
            (None, Some(right)) => {
                merged.insert(path, right.clone());
                stats.selected_right += 1;
            }
            (Some(left), Some(right)) => match left.version.relation(&right.version) {
                CausalRelation::Before => {
                    merged.insert(path, right.clone());
                    stats.selected_right += 1;
                }
                CausalRelation::After => {
                    merged.insert(path, left.clone());
                    stats.selected_left += 1;
                }
                CausalRelation::Equal if left.same_state(right) => {
                    merged.insert(path, left.clone());
                    stats.equal += 1;
                }
                CausalRelation::Concurrent if left.same_state(right) => {
                    let mut record = left.clone();
                    record.version.merge(&right.version);
                    merged.insert(path, record);
                    stats.equal += 1;
                }
                CausalRelation::Equal => {
                    resolve_conflict(
                        left,
                        right,
                        ConflictReason::EqualClockDivergence,
                        &mut occupied,
                        &mut merged,
                        &mut conflicts,
                        &mut stats,
                    )?;
                }
                CausalRelation::Concurrent => {
                    resolve_conflict(
                        left,
                        right,
                        ConflictReason::ConcurrentEdit,
                        &mut occupied,
                        &mut merged,
                        &mut conflicts,
                        &mut stats,
                    )?;
                }
            },
            (None, None) => unreachable!("path came from at least one snapshot"),
        }
    }

    conflicts.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(MergeResult {
        records: merged.into_values().collect(),
        conflicts,
        stats,
    })
}

#[allow(clippy::too_many_arguments)]
fn resolve_conflict(
    left: &SyncRecord,
    right: &SyncRecord,
    reason: ConflictReason,
    occupied: &mut BTreeSet<WirePath>,
    merged: &mut BTreeMap<WirePath, SyncRecord>,
    conflicts: &mut Vec<ConflictRecord>,
    stats: &mut MergeStats,
) -> Result<(), ReconcileError> {
    let left_hash = left.logical_hash();
    let right_hash = right.logical_hash();
    let left_state_hash = state_hash(left);
    let right_state_hash = state_hash(right);
    // A live directory wins a concurrent namespace-kind conflict. This keeps descendants
    // materializable while the losing live file is retained as a sibling conflict copy. Causal
    // directory-to-file transitions still take the normal Before/After path above.
    let left_directory = !left.tombstone && left.kind == SyncEntryKind::Directory;
    let right_directory = !right.tombstone && right.kind == SyncEntryKind::Directory;
    let (winner, loser, winner_hash, loser_hash) = match (left_directory, right_directory) {
        (true, false) => (left, right, left_hash, right_hash),
        (false, true) => (right, left, right_hash, left_hash),
        _ if left_state_hash >= right_state_hash => (left, right, left_hash, right_hash),
        _ => (right, left, right_hash, left_hash),
    };

    let mut resolved_version = left.version.clone();
    resolved_version.merge(&right.version);
    let resolver = conflict_resolver_replica();
    let counter = resolved_version
        .get(resolver)
        .checked_add(1)
        .ok_or(ReconcileError::CounterOverflow)?;
    resolved_version.observe(resolver, counter);

    let mut canonical = winner.clone();
    canonical.version = resolved_version.clone();
    merged.insert(canonical.path.clone(), canonical);

    let conflict_path = if should_preserve_loser(winner, loser) {
        let path = allocate_conflict_path(loser, occupied)?;
        let mut preserved = loser.clone();
        preserved.path = path.clone();
        preserved.version = resolved_version;
        preserved.tombstone = false;
        merged.insert(path.clone(), preserved);
        occupied.insert(path.clone());
        stats.conflict_copies += 1;
        Some(path)
    } else {
        None
    };

    conflicts.push(ConflictRecord {
        path: left.path.clone(),
        conflict_path,
        winner_hash,
        loser_hash,
        reason,
    });
    stats.conflicts += 1;
    Ok(())
}

fn should_preserve_loser(winner: &SyncRecord, loser: &SyncRecord) -> bool {
    !loser.tombstone
        && loser.kind == SyncEntryKind::File
        && (winner.tombstone || winner.content_hash != loser.content_hash)
}

fn state_hash(record: &SyncRecord) -> Hash32 {
    let mut hasher = blake3::Hasher::new_derive_key("deltaweave sync state winner v1");
    hasher.update(&[match record.kind {
        SyncEntryKind::File => 0,
        SyncEntryKind::Directory => 1,
        SyncEntryKind::Symlink => 2,
        SyncEntryKind::Other => 3,
    }]);
    hasher.update(&record.size.to_le_bytes());
    match record.content_hash {
        Some(hash) => {
            hasher.update(&[1]);
            hasher.update(hash.as_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    }
    hasher.update(&[u8::from(record.readonly), u8::from(record.tombstone)]);
    Hash32::from_bytes(*hasher.finalize().as_bytes())
}

fn conflict_resolver_replica() -> ReplicaId {
    ReplicaId(Hash32::digest(
        b"deltaweave deterministic conflict resolver v1",
    ))
}

fn allocate_conflict_path(
    loser: &SyncRecord,
    occupied: &BTreeSet<WirePath>,
) -> Result<WirePath, ReconcileError> {
    let token = loser.logical_hash().to_hex();
    for length in [12_usize, 16, 24, 32, 48, 64] {
        let path = build_conflict_path(&loser.path, &token[..length], None)?;
        if !occupied.contains(&path) {
            return Ok(path);
        }
    }
    for counter in 1_u32..=10_000 {
        let path = build_conflict_path(&loser.path, &token, Some(counter))?;
        if !occupied.contains(&path) {
            return Ok(path);
        }
    }
    Err(ReconcileError::ConflictPathExhausted)
}

fn build_conflict_path(
    original: &WirePath,
    token: &str,
    counter: Option<u32>,
) -> Result<WirePath, ReconcileError> {
    let (parent, name) = original
        .as_str()
        .rsplit_once('/')
        .map_or((None, original.as_str()), |(parent, name)| {
            (Some(parent), name)
        });
    let (stem, extension) = match name.rfind('.') {
        Some(index) if index > 0 => (&name[..index], &name[index..]),
        _ => (name, ""),
    };
    let serial = counter.map_or_else(String::new, |value| format!("-{value}"));
    let suffix = format!(".conflict-{token}{serial}");
    let parent_bytes = parent.map_or(0, |value| value.len() + 1);
    let path_budget = 4096_usize
        .checked_sub(parent_bytes)
        .ok_or(ReconcileError::ConflictPathTooLong)?;
    let component_budget = path_budget.min(255);
    if suffix.len() >= component_budget {
        return Err(ReconcileError::ConflictPathTooLong);
    }
    let extension = if suffix.len() + extension.len() < component_budget {
        extension
    } else {
        ""
    };
    let stem_budget = component_budget - suffix.len() - extension.len();
    let stem = truncate_utf8(stem, stem_budget);
    let component = format!("{stem}{suffix}{extension}");
    let value = parent.map_or(component.clone(), |parent| format!("{parent}/{component}"));
    WirePath::new(value).map_err(ReconcileError::ConflictPath)
}

fn truncate_utf8(value: &str, maximum_bytes: usize) -> &str {
    if value.len() <= maximum_bytes {
        return value;
    }
    let mut end = maximum_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn update_sized(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

/// Local work needed to reach a canonical merged namespace.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ApplyAction {
    /// Materialize or update a live file or directory, then adopt its exact version vector.
    Materialize { record: SyncRecord },
    /// Remove a live object if present and persist the supplied tombstone.
    Delete { record: SyncRecord },
}

/// Computes idempotent local actions required to match a merge result.
pub fn actions_to_reach(
    current: &MerkleTree,
    desired: &MergeResult,
) -> Result<Vec<ApplyAction>, ReconcileError> {
    let desired_tree = desired.tree()?;
    let mut actions = Vec::new();
    for path in current.different_paths(&desired_tree) {
        let Some(record) = desired_tree.get(&path) else {
            continue;
        };
        if current.get(&path) == Some(record) {
            continue;
        }
        if record.tombstone {
            actions.push(ApplyAction::Delete {
                record: record.clone(),
            });
        } else {
            actions.push(ApplyAction::Materialize {
                record: record.clone(),
            });
        }
    }
    Ok(actions)
}

/// Reconciliation, validation, or deterministic conflict-name failure.
#[derive(Debug, Error)]
pub enum ReconcileError {
    /// A supplied record violated its schema contract.
    #[error("invalid sync record: {0}")]
    InvalidRecord(#[from] SyncRecordError),
    /// A Merkle query prefix violated the portable path contract.
    #[error("invalid Merkle query prefix: {0}")]
    InvalidPrefix(#[from] WirePathError),
    /// A snapshot contained two values for the same portable path.
    #[error("snapshot contains a duplicate path")]
    DuplicatePath,
    /// The deterministic conflict resolver logical counter overflowed.
    #[error("conflict resolver counter overflow")]
    CounterOverflow,
    /// The original path leaves no portable space for a conflict suffix.
    #[error("path is too long to hold a portable conflict name")]
    ConflictPathTooLong,
    /// No unused deterministic conflict name could be allocated.
    #[error("deterministic conflict namespace is exhausted")]
    ConflictPathExhausted,
    /// A generated conflict name violated the portable path contract.
    #[error("invalid conflict path: {0}")]
    ConflictPath(#[source] WirePathError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaweave_core::{SYNC_RECORD_SCHEMA_V1, VersionVector};

    fn replica(name: &[u8]) -> ReplicaId {
        ReplicaId(Hash32::digest(name))
    }

    fn file(path: &str, replica: ReplicaId, counter: u64, bytes: &[u8]) -> SyncRecord {
        let mut version = VersionVector::default();
        version.observe(replica, counter);
        SyncRecord {
            schema_version: SYNC_RECORD_SCHEMA_V1,
            path: WirePath::new(path).expect("fixture path is portable"),
            kind: SyncEntryKind::File,
            size: bytes.len() as u64,
            content_hash: Some(Hash32::digest(bytes)),
            readonly: false,
            version,
            tombstone: false,
        }
    }

    fn directory(path: &str, replica: ReplicaId, counter: u64) -> SyncRecord {
        let mut version = VersionVector::default();
        version.observe(replica, counter);
        SyncRecord {
            schema_version: SYNC_RECORD_SCHEMA_V1,
            path: WirePath::new(path).expect("fixture path is portable"),
            kind: SyncEntryKind::Directory,
            size: 0,
            content_hash: None,
            readonly: false,
            version,
            tombstone: false,
        }
    }

    fn tombstone(mut record: SyncRecord, replica: ReplicaId) -> SyncRecord {
        record
            .version
            .increment(replica)
            .expect("clock can advance");
        record.tombstone = true;
        record
    }

    #[test]
    fn merkle_root_is_input_order_independent() {
        let node = replica(b"node");
        let left = MerkleTree::from_records([
            file("folder/a.txt", node, 1, b"a"),
            file("folder/b.txt", node, 2, b"b"),
        ])
        .expect("tree is valid");
        let right = MerkleTree::from_records([
            file("folder/b.txt", node, 2, b"b"),
            file("folder/a.txt", node, 1, b"a"),
        ])
        .expect("tree is valid");
        assert_eq!(left.root_hash(), right.root_hash());
        assert!(left.different_paths(&right).is_empty());
    }

    #[test]
    fn merkle_diff_descends_to_changed_paths() {
        let node = replica(b"node");
        let left = MerkleTree::from_records([
            file("a/keep.txt", node, 1, b"same"),
            file("a/change.txt", node, 2, b"before"),
            file("b/remove.txt", node, 3, b"remove"),
        ])
        .expect("tree is valid");
        let right = MerkleTree::from_records([
            file("a/keep.txt", node, 1, b"same"),
            file("a/change.txt", node, 3, b"after"),
            file("c/add.txt", node, 4, b"add"),
        ])
        .expect("tree is valid");
        assert_eq!(
            left.different_paths(&right),
            ["a/change.txt", "b/remove.txt", "c/add.txt"]
                .map(|path| WirePath::new(path).expect("path is portable"))
        );
    }

    #[test]
    fn node_summaries_support_changed_subtree_queries() {
        let node = replica(b"node");
        let tree = MerkleTree::from_records([
            file("docs/a.txt", node, 1, b"a"),
            file("docs/nested/b.txt", node, 2, b"b"),
            file("photos/c.jpg", node, 3, b"c"),
        ])
        .expect("tree is valid");
        let root = tree
            .node_summary("")
            .expect("query is valid")
            .expect("root exists");
        assert_eq!(root.hash, tree.root_hash());
        assert_eq!(root.record_count, 3);
        assert_eq!(
            root.children
                .iter()
                .map(|child| child.name.as_str())
                .collect::<Vec<_>>(),
            vec!["docs", "photos"]
        );

        let docs = tree
            .node_summary("docs")
            .expect("query is valid")
            .expect("subtree exists");
        assert_eq!(docs.record_count, 2);
        assert_eq!(tree.records_under("docs").expect("query is valid").len(), 2);
        assert!(
            tree.node_summary("missing")
                .expect("query is valid")
                .is_none()
        );
        assert!(tree.node_summary("../escape").is_err());
    }

    #[test]
    fn large_namespace_diff_stays_path_precise() {
        let node = replica(b"large-node");
        let left_records: Vec<_> = (0_u64..10_000)
            .map(|index| {
                file(
                    &format!("bucket-{:02}/file-{index:05}.bin", index % 100),
                    node,
                    index + 1,
                    &index.to_le_bytes(),
                )
            })
            .collect();
        let mut right_records = left_records.clone();
        let changed = 7_321_usize;
        right_records[changed] = file(
            &format!("bucket-{:02}/file-{changed:05}.bin", changed % 100),
            node,
            20_000,
            b"changed payload",
        );

        let left = MerkleTree::from_records(left_records).expect("large tree is valid");
        let right = MerkleTree::from_records(right_records).expect("changed tree is valid");
        assert_ne!(left.root_hash(), right.root_hash());
        assert_eq!(left.len(), 10_000);
        assert_eq!(
            left.different_paths(&right),
            vec![WirePath::new("bucket-21/file-07321.bin").expect("path is portable")]
        );
        assert_eq!(
            right
                .node_summary("bucket-21")
                .expect("query is valid")
                .expect("bucket exists")
                .record_count,
            100
        );
    }

    #[test]
    fn causal_update_and_delete_win_without_conflict() {
        let alpha = replica(b"alpha");
        let initial = file("report.txt", alpha, 1, b"one");
        let mut updated = file("report.txt", alpha, 2, b"two");
        updated.version = initial.version.clone();
        updated.version.increment(alpha).expect("clock can advance");
        let left = MerkleTree::from_records([initial]).expect("tree is valid");
        let right = MerkleTree::from_records([updated.clone()]).expect("tree is valid");
        let merged = merge_snapshots(&left, &right).expect("merge succeeds");
        assert_eq!(merged.records, vec![updated.clone()]);
        assert!(merged.conflicts.is_empty());

        let deleted = tombstone(updated, alpha);
        let right = MerkleTree::from_records([deleted.clone()]).expect("tree is valid");
        let merged = merge_snapshots(&merged.tree().expect("tree is valid"), &right)
            .expect("delete merge succeeds");
        assert_eq!(merged.records, vec![deleted]);
        assert!(merged.conflicts.is_empty());
    }

    #[test]
    fn concurrent_edits_converge_and_preserve_the_loser() {
        let alpha = replica(b"alpha");
        let beta = replica(b"beta");
        let left = MerkleTree::from_records([file("report.txt", alpha, 2, b"alpha edit")])
            .expect("tree is valid");
        let right = MerkleTree::from_records([file("report.txt", beta, 1, b"beta edit")])
            .expect("tree is valid");

        let forward = merge_snapshots(&left, &right).expect("merge succeeds");
        let reverse = merge_snapshots(&right, &left).expect("reverse merge succeeds");
        assert_eq!(forward, reverse);
        assert_eq!(forward.stats.conflicts, 1);
        assert_eq!(forward.stats.conflict_copies, 1);
        assert_eq!(forward.records.len(), 2);
        let conflict = forward.conflicts.first().expect("conflict is recorded");
        assert!(
            conflict
                .conflict_path
                .as_ref()
                .is_some_and(|path| path.as_str().contains(".conflict-"))
        );

        let canonical = forward.tree().expect("merged tree is valid");
        let repeated = merge_snapshots(&canonical, &canonical).expect("merge is idempotent");
        assert_eq!(repeated.records, forward.records);
        assert!(repeated.conflicts.is_empty());
    }

    #[test]
    fn concurrent_identical_content_only_merges_causal_knowledge() {
        let alpha = replica(b"alpha");
        let beta = replica(b"beta");
        let left =
            MerkleTree::from_records([file("same.bin", alpha, 2, b"same")]).expect("tree is valid");
        let right =
            MerkleTree::from_records([file("same.bin", beta, 3, b"same")]).expect("tree is valid");
        let merged = merge_snapshots(&left, &right).expect("merge succeeds");
        assert!(merged.conflicts.is_empty());
        assert_eq!(merged.records.len(), 1);
        assert_eq!(merged.records[0].version.get(alpha), 2);
        assert_eq!(merged.records[0].version.get(beta), 3);
    }

    #[test]
    fn concurrent_file_vs_tree_preserves_file_and_materializable_subtree() {
        let alpha = replica(b"alpha");
        let beta = replica(b"beta");
        let left = MerkleTree::from_records([file("tree", alpha, 1, b"file bytes")])
            .expect("file snapshot is valid");
        let right = MerkleTree::from_records([
            directory("tree", beta, 1),
            file("tree/child.txt", beta, 2, b"child bytes"),
        ])
        .expect("tree snapshot is valid");

        let forward = merge_snapshots(&left, &right).expect("merge succeeds");
        let reverse = merge_snapshots(&right, &left).expect("reverse merge succeeds");
        assert_eq!(forward.records, reverse.records);
        assert_eq!(forward.conflicts, reverse.conflicts);
        assert_eq!(forward.stats.selected_left, reverse.stats.selected_right);
        assert_eq!(forward.stats.selected_right, reverse.stats.selected_left);
        assert!(forward.records.iter().any(|record| {
            record.path.as_str() == "tree"
                && record.kind == SyncEntryKind::Directory
                && !record.tombstone
        }));
        assert!(forward.records.iter().any(|record| {
            record.path.as_str() == "tree/child.txt"
                && record.content_hash == Some(Hash32::digest(b"child bytes"))
        }));
        let conflict_path = forward.conflicts[0]
            .conflict_path
            .as_ref()
            .expect("file side is retained as a conflict copy");
        assert!(!conflict_path.as_str().starts_with("tree/"));
        assert!(forward.records.iter().any(|record| {
            &record.path == conflict_path
                && record.content_hash == Some(Hash32::digest(b"file bytes"))
        }));
    }

    #[test]
    fn partitioned_three_peer_model_reaches_one_root() {
        let alpha = replica(b"alpha");
        let beta = replica(b"beta");
        let gamma = replica(b"gamma");
        let initial = file("shared.txt", alpha, 1, b"initial");

        let mut alpha_edit = initial.clone();
        alpha_edit
            .version
            .increment(alpha)
            .expect("clock can advance");
        alpha_edit.content_hash = Some(Hash32::digest(b"alpha edit"));
        alpha_edit.size = b"alpha edit".len() as u64;

        let mut beta_edit = initial.clone();
        beta_edit
            .version
            .increment(beta)
            .expect("clock can advance");
        beta_edit.content_hash = Some(Hash32::digest(b"beta edit"));
        beta_edit.size = b"beta edit".len() as u64;

        let gamma_delete = tombstone(initial, gamma);
        let mut peers = [
            MerkleTree::from_records([alpha_edit]).expect("tree is valid"),
            MerkleTree::from_records([beta_edit]).expect("tree is valid"),
            MerkleTree::from_records([gamma_delete]).expect("tree is valid"),
        ];

        for (left, right) in [(0, 1), (1, 2), (0, 2), (0, 1), (1, 2)] {
            let merged = merge_snapshots(&peers[left], &peers[right]).expect("merge succeeds");
            let tree = merged.tree().expect("merged tree is valid");
            peers[left] = tree.clone();
            peers[right] = tree;
        }
        assert_eq!(peers[0].root_hash(), peers[1].root_hash());
        assert_eq!(peers[1].root_hash(), peers[2].root_hash());
    }

    #[test]
    fn conflict_name_stays_portable_for_long_unicode_names() {
        let alpha = replica(b"alpha");
        let beta = replica(b"beta");
        let long_name = format!("{}.txt", "자료".repeat(40));
        let original = WirePath::new(long_name).expect("fixture fits portable limit");
        let left = MerkleTree::from_records([file(original.as_str(), alpha, 1, b"left")])
            .expect("tree is valid");
        let right = MerkleTree::from_records([file(original.as_str(), beta, 1, b"right")])
            .expect("tree is valid");
        let merged = merge_snapshots(&left, &right).expect("merge succeeds");
        let path = merged.conflicts[0]
            .conflict_path
            .as_ref()
            .expect("losing file is preserved");
        assert!(path.as_str().len() <= 255);
        assert!(path.as_str().contains(".conflict-"));
    }

    #[test]
    fn actions_are_idempotent_and_include_tombstones() {
        let alpha = replica(b"alpha");
        let live = file("gone.txt", alpha, 1, b"content");
        let deleted = tombstone(live.clone(), alpha);
        let current = MerkleTree::from_records([live]).expect("tree is valid");
        let desired = MergeResult {
            records: vec![deleted.clone()],
            conflicts: Vec::new(),
            stats: MergeStats {
                equal: 0,
                selected_left: 0,
                selected_right: 1,
                conflicts: 0,
                conflict_copies: 0,
            },
        };
        assert_eq!(
            actions_to_reach(&current, &desired).expect("actions can be planned"),
            vec![ApplyAction::Delete { record: deleted }]
        );
        assert!(
            actions_to_reach(&desired.tree().expect("tree is valid"), &desired)
                .expect("actions can be planned")
                .is_empty()
        );
    }
}
