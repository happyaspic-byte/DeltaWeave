//! Bounded, deterministic multi-peer chunk scheduling.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap};

use deltaweave_core::Hash32;

/// Chunk availability and transfer-health signals for one authorized peer.
#[derive(Clone, Debug)]
pub struct PeerAvailability {
    /// Stable authenticated peer identifier.
    pub id: Hash32,
    /// Exact unique chunk hashes advertised by this peer.
    pub available: BTreeSet<Hash32>,
    /// Smoothed round-trip time in milliseconds.
    pub rtt_ms: u32,
    /// Bytes already queued for this peer.
    pub queued_bytes: u64,
    /// Smoothed verified payload rate.
    pub goodput_bytes_per_second: u64,
    /// Smoothed failure penalty.
    pub failure_penalty: u32,
}

/// Hard bounds applied by one scheduler pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchedulerLimits {
    /// Maximum peers selected as sources.
    pub max_sources: usize,
    /// Maximum chunks assigned to one peer.
    pub max_chunks_per_peer: usize,
    /// Maximum total assignments created in one pass.
    pub max_assignments: usize,
}

impl Default for SchedulerLimits {
    fn default() -> Self {
        Self {
            max_sources: 8,
            max_chunks_per_peer: 8,
            max_assignments: 64,
        }
    }
}

/// One deterministic chunk-to-peer lease decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChunkAssignment {
    /// Missing content hash.
    pub hash: Hash32,
    /// Authorized provider selected for the hash.
    pub peer: Hash32,
}

/// Bounded active/passive membership view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Overlay {
    /// Peers with live connection intent.
    pub active: Vec<Hash32>,
    /// Bounded failover candidates.
    pub passive: Vec<Hash32>,
}

/// Builds a deterministic bounded peer view for a group size.
#[must_use]
pub fn build_overlay(self_id: Hash32, peer_count: usize) -> Overlay {
    let active_limit = match peer_count {
        0 => 0,
        1 => 1,
        2..=10 => 6.min(peer_count),
        11..=100 => 8.min(peer_count),
        _ => 12.min(peer_count),
    };
    let passive_limit = match peer_count {
        0..=1 => 0,
        2..=10 => 12.min(peer_count.saturating_sub(active_limit)),
        11..=100 => 32.min(peer_count.saturating_sub(active_limit)),
        _ => 64.min(peer_count.saturating_sub(active_limit)),
    };
    let mut peers: Vec<_> = (0..peer_count)
        .map(|index| {
            let mut seed = Vec::with_capacity(40);
            seed.extend_from_slice(self_id.as_bytes());
            seed.extend_from_slice(&(index as u64).to_le_bytes());
            Hash32::digest(&seed)
        })
        .collect();
    peers.sort();
    let passive = peers.split_off(active_limit);
    Overlay {
        active: peers,
        passive: passive.into_iter().take(passive_limit).collect(),
    }
}

/// Assigns missing chunks rarest-first, then chooses the lowest-cost provider.
#[must_use]
pub fn schedule_chunks(
    missing: &[Hash32],
    peers: &[PeerAvailability],
    limits: SchedulerLimits,
) -> Vec<ChunkAssignment> {
    let mut provider_counts = HashMap::new();
    for hash in missing {
        provider_counts.insert(
            *hash,
            peers
                .iter()
                .filter(|peer| peer.available.contains(hash))
                .count(),
        );
    }
    let mut ordered = missing.to_vec();
    ordered.sort_by_key(|hash| (provider_counts.get(hash).copied().unwrap_or(0), *hash));

    let mut per_peer = BTreeMap::<Hash32, usize>::new();
    let mut sources = BTreeSet::new();
    let mut assignments = Vec::new();
    for hash in ordered {
        if assignments.len() >= limits.max_assignments {
            break;
        }
        let selected = peers
            .iter()
            .filter(|peer| peer.available.contains(&hash))
            .filter(|peer| {
                per_peer.get(&peer.id).copied().unwrap_or(0) < limits.max_chunks_per_peer
            })
            .filter(|peer| sources.contains(&peer.id) || sources.len() < limits.max_sources)
            .min_by_key(|peer| {
                let goodput = peer.goodput_bytes_per_second.max(1);
                let queue_ms = peer.queued_bytes.saturating_mul(1000) / goodput;
                let assigned = per_peer.get(&peer.id).copied().unwrap_or(0);
                (
                    u64::from(peer.rtt_ms)
                        .saturating_add(queue_ms)
                        .saturating_add(u64::from(peer.failure_penalty) * 500)
                        .saturating_add(assigned as u64),
                    assigned,
                    peer.id,
                )
            });
        if let Some(peer) = selected {
            sources.insert(peer.id);
            *per_peer.entry(peer.id).or_default() += 1;
            assignments.push(ChunkAssignment {
                hash,
                peer: peer.id,
            });
        }
    }
    assignments
}

#[cfg(test)]
mod tests {
    use deltaweave_core::Hash32;

    use super::*;

    fn peer(label: &str, available: &[Hash32], rtt_ms: u32) -> PeerAvailability {
        PeerAvailability {
            id: Hash32::digest(label.as_bytes()),
            available: available.iter().copied().collect(),
            rtt_ms,
            queued_bytes: 0,
            goodput_bytes_per_second: 10 * 1024 * 1024,
            failure_penalty: 0,
        }
    }

    #[test]
    fn scheduler_assigns_rarest_chunks_first() {
        let common = Hash32::digest(b"common");
        let rare = Hash32::digest(b"rare");
        let peers = vec![
            peer("a", &[common, rare], 20),
            peer("b", &[common], 5),
            peer("c", &[common], 10),
        ];

        let assignments = schedule_chunks(&[common, rare], &peers, SchedulerLimits::default());

        assert_eq!(assignments.len(), 2);
        assert_eq!(assignments[0].hash, rare);
        assert_eq!(assignments[0].peer, Hash32::digest(b"a"));
    }

    #[test]
    fn overlay_stays_bounded_for_one_to_one_thousand_peers() {
        for (count, expected_active) in [(1, 1), (10, 6), (100, 8), (1000, 12)] {
            let overlay = build_overlay(Hash32::digest(b"self"), count);
            assert_eq!(overlay.active.len(), expected_active);
            assert!(overlay.active.len() <= 12);
            assert!(overlay.passive.len() <= 64);
            assert!(overlay.active.len() + overlay.passive.len() <= 76);
        }
    }

    #[test]
    fn thousand_peer_simulation_completes_without_full_mesh() {
        let chunks: Vec<_> = (0_u64..512)
            .map(|index| Hash32::digest(&index.to_le_bytes()))
            .collect();
        let overlay = build_overlay(Hash32::digest(b"self"), 1000);
        let peers: Vec<_> = overlay
            .active
            .iter()
            .enumerate()
            .map(|(index, id)| PeerAvailability {
                id: *id,
                available: chunks.iter().copied().collect(),
                rtt_ms: 5 + index as u32,
                queued_bytes: 0,
                goodput_bytes_per_second: 10 * 1024 * 1024,
                failure_penalty: 0,
            })
            .collect();
        let mut pending = chunks.clone();
        let mut passes = 0;

        while !pending.is_empty() {
            let assignments = schedule_chunks(&pending, &peers, SchedulerLimits::default());
            assert!(!assignments.is_empty());
            let completed: BTreeSet<_> = assignments
                .iter()
                .map(|assignment| assignment.hash)
                .collect();
            pending.retain(|hash| !completed.contains(hash));
            passes += 1;
        }

        assert_eq!(passes, 8);
        assert_eq!(overlay.active.len(), 12);
        assert_eq!(overlay.passive.len(), 64);
        assert!(overlay.active.len() + overlay.passive.len() <= 76);
    }

    #[test]
    fn scheduler_balances_a_complete_plan_across_equivalent_providers() {
        let chunks: Vec<_> = (0_u64..100)
            .map(|index| Hash32::digest(&index.to_le_bytes()))
            .collect();
        let peers = vec![peer("a", &chunks, 10), peer("b", &chunks, 10)];

        let assignments = schedule_chunks(
            &chunks,
            &peers,
            SchedulerLimits {
                max_sources: 2,
                max_chunks_per_peer: chunks.len(),
                max_assignments: chunks.len(),
            },
        );

        let first = assignments
            .iter()
            .filter(|assignment| assignment.peer == peers[0].id)
            .count();
        let second = assignments.len() - first;
        assert_eq!(assignments.len(), chunks.len());
        assert!(first.abs_diff(second) <= 1);
    }

    #[test]
    fn scheduler_respects_global_and_per_peer_limits() {
        let chunks: Vec<_> = (0_u64..200)
            .map(|index| Hash32::digest(&index.to_le_bytes()))
            .collect();
        let peers: Vec<_> = (0..100)
            .map(|index| peer(&format!("peer-{index}"), &chunks, 10 + index))
            .collect();
        let limits = SchedulerLimits {
            max_sources: 8,
            max_chunks_per_peer: 8,
            max_assignments: 64,
        };

        let assignments = schedule_chunks(&chunks, &peers, limits);

        assert_eq!(assignments.len(), 64);
        let used: std::collections::BTreeSet<_> = assignments
            .iter()
            .map(|assignment| assignment.peer)
            .collect();
        assert!(used.len() <= 8);
        for peer in used {
            assert!(
                assignments
                    .iter()
                    .filter(|assignment| assignment.peer == peer)
                    .count()
                    <= 8
            );
        }
    }
}
