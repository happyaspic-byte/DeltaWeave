//! Per-peer bandwidth, concurrency, and storage admission.

use std::{collections::HashMap, sync::Mutex, time::Instant};

use anyhow::{Result, ensure};
use iroh::EndpointId;

/// Receive-side resource limits applied after a peer is authenticated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QuotaPolicy {
    /// Sustained receive rate in bytes per second. Zero disables the token bucket.
    pub bytes_per_second: u64,
    /// Extra tokens that may accumulate above the sustained rate.
    pub burst_bytes: u64,
    /// Simultaneous in-flight operations per peer. Zero means unlimited.
    pub max_concurrent_operations_per_peer: u32,
    /// Maximum unique CAS bytes this node will store. Zero means unlimited.
    pub max_storage_bytes: u64,
}

impl QuotaPolicy {
    /// No bandwidth, concurrency, or storage limits.
    pub const UNLIMITED: Self = Self {
        bytes_per_second: 0,
        burst_bytes: 0,
        max_concurrent_operations_per_peer: 0,
        max_storage_bytes: 0,
    };
}

/// Live accounting for one node.
#[derive(Debug)]
pub struct QuotaAccountant {
    policy: QuotaPolicy,
    inner: Mutex<QuotaState>,
}

#[derive(Debug)]
struct QuotaState {
    used_storage: u64,
    reserved_storage: u64,
    next_reservation: u64,
    reservations: HashMap<u64, u64>,
    inflight: HashMap<EndpointId, u32>,
    buckets: HashMap<EndpointId, TokenBucket>,
}

#[derive(Debug)]
struct TokenBucket {
    tokens: u64,
    last_refill: Instant,
}

/// Handle that returns a concurrency permit when dropped.
#[derive(Debug)]
pub struct ConcurrencyPermit {
    accountant: std::sync::Arc<QuotaAccountant>,
    peer: EndpointId,
}

/// Handle that returns unused reserved storage when dropped without commit.
#[derive(Debug)]
pub struct StorageReservation {
    accountant: std::sync::Arc<QuotaAccountant>,
    id: u64,
    remaining: u64,
    committed: bool,
}

impl QuotaAccountant {
    /// Creates an accountant with reconstructed CAS usage.
    #[must_use]
    pub fn new(policy: QuotaPolicy, used_storage: u64) -> Self {
        Self {
            policy,
            inner: Mutex::new(QuotaState {
                used_storage,
                reserved_storage: 0,
                next_reservation: 1,
                reservations: HashMap::new(),
                inflight: HashMap::new(),
                buckets: HashMap::new(),
            }),
        }
    }

    /// Current committed unique CAS bytes.
    pub fn used_storage(&self) -> Result<u64> {
        Ok(self.lock()?.used_storage)
    }

    /// Acquires one in-flight operation for `peer`.
    pub fn acquire_operation(
        self: &std::sync::Arc<Self>,
        peer: EndpointId,
    ) -> Result<ConcurrencyPermit> {
        if self.policy.max_concurrent_operations_per_peer == 0 {
            return Ok(ConcurrencyPermit {
                accountant: std::sync::Arc::clone(self),
                peer,
            });
        }
        let mut state = self.lock()?;
        let current = state.inflight.entry(peer).or_insert(0);
        ensure!(
            *current < self.policy.max_concurrent_operations_per_peer,
            "peer exceeded concurrent operation quota"
        );
        *current += 1;
        Ok(ConcurrencyPermit {
            accountant: std::sync::Arc::clone(self),
            peer,
        })
    }

    /// Reserves unique missing-chunk bytes before payload admission.
    pub fn reserve_storage(
        self: &std::sync::Arc<Self>,
        requested: u64,
    ) -> Result<StorageReservation> {
        if self.policy.max_storage_bytes == 0 || requested == 0 {
            return Ok(StorageReservation {
                accountant: std::sync::Arc::clone(self),
                id: 0,
                remaining: 0,
                committed: true,
            });
        }
        let mut state = self.lock()?;
        let occupied = state
            .used_storage
            .checked_add(state.reserved_storage)
            .ok_or_else(|| anyhow::anyhow!("storage accounting overflow"))?;
        let next = occupied
            .checked_add(requested)
            .ok_or_else(|| anyhow::anyhow!("storage accounting overflow"))?;
        ensure!(
            next <= self.policy.max_storage_bytes,
            "storage quota exceeded"
        );
        let id = state.next_reservation;
        state.next_reservation = state
            .next_reservation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("reservation id overflow"))?;
        state.reserved_storage = state
            .reserved_storage
            .checked_add(requested)
            .ok_or_else(|| anyhow::anyhow!("storage accounting overflow"))?;
        state.reservations.insert(id, requested);
        Ok(StorageReservation {
            accountant: std::sync::Arc::clone(self),
            id,
            remaining: requested,
            committed: false,
        })
    }

    /// Consumes `bytes` from the per-peer token bucket. Returns wait duration if empty.
    ///
    /// Requests larger than the burst are admitted at the bucket's current
    /// capacity, then charged by the wait the remainder needs; the caller
    /// treats the returned duration as the pacing delay for the rest.
    pub fn consume_bandwidth(
        &self,
        peer: EndpointId,
        bytes: u64,
        now: Instant,
    ) -> Result<Option<std::time::Duration>> {
        if self.policy.bytes_per_second == 0 || bytes == 0 {
            return Ok(None);
        }
        let mut state = self.lock()?;
        let rate = self.policy.bytes_per_second;
        let burst = rate.saturating_add(self.policy.burst_bytes);
        let bucket = state.buckets.entry(peer).or_insert(TokenBucket {
            tokens: burst,
            last_refill: now,
        });
        refill(bucket, rate, burst, now);
        let available = bucket.tokens.min(bytes);
        bucket.tokens -= available;
        let deficit = bytes - available;
        if deficit == 0 {
            return Ok(None);
        }
        let service_time = std::time::Duration::from_secs(deficit.div_ceil(rate));
        let deadline = bucket
            .last_refill
            .max(now)
            .checked_add(service_time)
            .ok_or_else(|| anyhow::anyhow!("bandwidth deadline overflow"))?;
        bucket.last_refill = deadline;
        Ok(Some(deadline.saturating_duration_since(now)))
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, QuotaState>> {
        self.inner
            .lock()
            .map_err(|_| anyhow::anyhow!("quota lock is poisoned"))
    }
}

impl Drop for ConcurrencyPermit {
    fn drop(&mut self) {
        if self.accountant.policy.max_concurrent_operations_per_peer == 0 {
            return;
        }
        if let Ok(mut state) = self.accountant.lock()
            && let Some(current) = state.inflight.get_mut(&self.peer)
        {
            *current = current.saturating_sub(1);
            if *current == 0 {
                state.inflight.remove(&self.peer);
            }
        }
    }
}

impl StorageReservation {
    /// Commits newly written unique bytes against this reservation.
    pub fn commit(&mut self, newly_written: u64) -> Result<()> {
        if self.id == 0 {
            return Ok(());
        }
        ensure!(
            newly_written <= self.remaining,
            "committed more storage than reserved"
        );
        let mut state = self.accountant.lock()?;
        let remaining = {
            let reserved = state
                .reservations
                .get_mut(&self.id)
                .ok_or_else(|| anyhow::anyhow!("unknown storage reservation"))?;
            *reserved = reserved
                .checked_sub(newly_written)
                .ok_or_else(|| anyhow::anyhow!("reservation underflow"))?;
            *reserved
        };
        state.reserved_storage = state
            .reserved_storage
            .checked_sub(newly_written)
            .ok_or_else(|| anyhow::anyhow!("reservation underflow"))?;
        state.used_storage = state
            .used_storage
            .checked_add(newly_written)
            .ok_or_else(|| anyhow::anyhow!("storage accounting overflow"))?;
        self.remaining = remaining;
        if remaining == 0 {
            state.reservations.remove(&self.id);
            self.committed = true;
        }
        Ok(())
    }

    /// Releases any unused reservation immediately.
    pub fn release(mut self) {
        self.committed = true;
        self.release_remaining();
    }

    fn release_remaining(&mut self) {
        if self.id == 0 || self.remaining == 0 {
            return;
        }
        if let Ok(mut state) = self.accountant.lock()
            && let Some(reserved) = state.reservations.remove(&self.id)
        {
            state.reserved_storage = state.reserved_storage.saturating_sub(reserved);
        }
        self.remaining = 0;
    }
}

impl Drop for StorageReservation {
    fn drop(&mut self) {
        if !self.committed {
            self.release_remaining();
        }
    }
}

fn refill(bucket: &mut TokenBucket, rate: u64, burst: u64, now: Instant) {
    let elapsed = now.saturating_duration_since(bucket.last_refill);
    let added = elapsed.as_secs().saturating_mul(rate);
    if added > 0 {
        bucket.tokens = bucket.tokens.saturating_add(added).min(burst);
        bucket.last_refill = now;
    }
}

/// Unique payload bytes that would be newly stored for `missing` hashes.
#[must_use]
pub fn unique_missing_bytes(
    manifest: &deltaweave_core::FileManifest,
    missing: &[deltaweave_core::Hash32],
) -> u64 {
    let lengths: HashMap<_, _> = manifest
        .chunks
        .iter()
        .map(|chunk| (chunk.hash, u64::from(chunk.length)))
        .collect();
    missing
        .iter()
        .filter_map(|hash| lengths.get(hash).copied())
        .fold(0_u64, |total, length| total.saturating_add(length))
}

/// Reconstructs unique CAS usage by walking verified chunk files.
pub fn measure_cas_usage(chunks: &deltaweave_store::ChunkStore) -> Result<u64> {
    chunks.usage_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    fn accountant(policy: QuotaPolicy, used: u64) -> Arc<QuotaAccountant> {
        Arc::new(QuotaAccountant::new(policy, used))
    }

    fn peer(_label: u8) -> EndpointId {
        iroh::SecretKey::generate().public()
    }

    #[test]
    fn concurrent_reservations_cannot_exceed_storage_limit() {
        let policy = QuotaPolicy {
            max_storage_bytes: 100,
            ..QuotaPolicy::UNLIMITED
        };
        let accountant = accountant(policy, 40);
        let accepted = thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let accountant = Arc::clone(&accountant);
                    scope.spawn(move || accountant.reserve_storage(30).ok())
                })
                .collect();
            let reservations: Vec<_> = handles
                .into_iter()
                .filter_map(|handle| handle.join().expect("worker finishes"))
                .collect();
            let count = reservations.len();
            drop(reservations);
            count
        });
        assert_eq!(accepted, 2);
        assert_eq!(accountant.used_storage().expect("usage is readable"), 40);
    }

    #[test]
    fn abort_releases_storage_reservation() {
        let policy = QuotaPolicy {
            max_storage_bytes: 50,
            ..QuotaPolicy::UNLIMITED
        };
        let accountant = accountant(policy, 0);
        {
            let reservation = accountant
                .reserve_storage(40)
                .expect("reservation succeeds");
            drop(reservation);
        }
        accountant
            .reserve_storage(50)
            .expect("released reservation returns capacity");
    }

    #[test]
    fn reused_chunks_are_not_charged() {
        let policy = QuotaPolicy {
            max_storage_bytes: 10,
            ..QuotaPolicy::UNLIMITED
        };
        let accountant = accountant(policy, 10);
        accountant
            .reserve_storage(0)
            .expect("zero-byte reuse does not consume quota");
        assert!(accountant.reserve_storage(1).is_err());
    }

    #[test]
    fn peer_concurrency_is_isolated() {
        let policy = QuotaPolicy {
            max_concurrent_operations_per_peer: 1,
            ..QuotaPolicy::UNLIMITED
        };
        let accountant = accountant(policy, 0);
        let left = peer(1);
        let right = peer(2);
        let first = accountant.acquire_operation(left).expect("first permit");
        assert!(accountant.acquire_operation(left).is_err());
        accountant
            .acquire_operation(right)
            .expect("other peer remains unrestricted");
        drop(first);
        accountant
            .acquire_operation(left)
            .expect("permit returns after drop");
    }

    #[test]
    fn token_bucket_uses_injectable_clock() {
        let policy = QuotaPolicy {
            bytes_per_second: 10,
            burst_bytes: 10,
            ..QuotaPolicy::UNLIMITED
        };
        let accountant = accountant(policy, 0);
        let peer = peer(3);
        let start = Instant::now();
        assert_eq!(
            accountant
                .consume_bandwidth(peer, 10, start)
                .expect("consume succeeds"),
            None
        );
        assert_eq!(
            accountant
                .consume_bandwidth(peer, 10, start)
                .expect("burst capacity remains"),
            None
        );
        let wait = accountant
            .consume_bandwidth(peer, 10, start)
            .expect("empty bucket reports wait");
        assert_eq!(wait, Some(Duration::from_secs(1)));
        assert_eq!(
            accountant
                .consume_bandwidth(peer, 10, start)
                .expect("concurrent debt is serialized"),
            Some(Duration::from_secs(2))
        );
        assert_eq!(
            accountant
                .consume_bandwidth(peer, 10, start + Duration::from_secs(1))
                .expect("future debt remains reserved"),
            Some(Duration::from_secs(2))
        );
        assert_eq!(
            accountant
                .consume_bandwidth(peer, 10, start + Duration::from_secs(10))
                .expect("idle capacity refills"),
            None
        );
    }

    #[test]
    fn commit_increases_used_storage_and_partial_abort_returns_remainder() {
        let policy = QuotaPolicy {
            max_storage_bytes: 100,
            ..QuotaPolicy::UNLIMITED
        };
        let accountant = accountant(policy, 0);
        let mut reservation = accountant.reserve_storage(80).expect("reserved");
        reservation.commit(30).expect("partial commit");
        drop(reservation);
        assert_eq!(accountant.used_storage().expect("usage"), 30);
        accountant
            .reserve_storage(70)
            .expect("uncommitted remainder returned");
    }
}
