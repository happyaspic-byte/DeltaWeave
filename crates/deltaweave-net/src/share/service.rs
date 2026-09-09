use super::authority::{
    ActivationCancel, ActivationReceipt, ActivationStateView, ActivationStatusQuery, ApplyCancel,
    ApplyDrained, ApplyPermit, ApplyReceipt, ApplyStart, ApplyStatusQuery, AuthoritativeSnapshot,
    ClientIntentPhase, ClientIntentRow, ClientSide, ManifestAttestation, ShareGrant, SnapshotToken,
    SwarmTransferReceipt, request_hash,
};
use super::roster::random_nonce;
use super::{
    ALPN_SWARM_V1, ALPN_V3, GrantNonce, LegacyProof, MemberRelationship, Membership,
    OwnedShareConfig, RosterHeartbeat, ShareError, ShareId, SharePhase, ShareTicket,
    ShareTransferEvent, ShareTransferObserver, SignedRoster, TicketPreview, TransferDirection,
};
use super::{
    registry::Registry,
    runtime::{Authorization, OwnedRuntime, OwnerShare},
    wire::{self, Hello, Operation, Reply},
};
use crate::{
    NetworkMode, OperationAdmission, SyncClient, SyncHandler, SyncSession, SyncWireRequest,
    SyncWireResponse, TransportObservation, bind_endpoint, endpoint_addr_with_local_fallback,
    load_or_create_identity, prepare_server_roots, read_frame,
    root_admission::{self, RootLease, RootUse},
    write_frame,
};
use anyhow::{Result, ensure};
use deltaweave_core::{ChunkingProfile, Hash32, ReplicaId, SyncRecord};
use deltaweave_index::{IndexOptions, LocalIndex};
use deltaweave_reconcile::MerkleTree;
use deltaweave_store::Store;
use iroh::{
    EndpointAddr, EndpointId, SecretKey,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler, Router},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

const CONTROL_DEADLINE: std::time::Duration = std::time::Duration::from_secs(15);
const CLOSE_CONFIRM_DEADLINE: std::time::Duration = std::time::Duration::from_secs(1);
const MAX_SWARM_OPERATION_KEYS: usize = 4096;
const SHARE_PAYLOAD_TRACE_TARGET: &str = "deltaweave_share_payload";
static SHARE_PAYLOAD_TRACE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn query_event_id(sequence: &AtomicU64, share: ShareId, peer: EndpointId, tag: &[u8]) -> [u8; 16] {
    let mut bytes = b"deltaweave/share-event/query/v1\0".to_vec();
    bytes.extend_from_slice(&share.0);
    bytes.extend_from_slice(peer.as_bytes());
    bytes.extend_from_slice(tag);
    bytes.extend_from_slice(&sequence.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    let digest = Hash32::digest(&bytes);
    let mut operation_id = [0_u8; 16];
    operation_id.copy_from_slice(&digest.as_bytes()[..16]);
    operation_id
}

/// Owns the event lifetime after a control frame has reached the authenticated
/// handler. A transport failure before a reply creates no guard and therefore
/// no active-peer event. If the caller is cancelled after admission but before
/// synchronous reply validation completes, dropping this guard emits Reject so
/// the operation cannot remain active forever.
struct ShareEventGuard {
    events: Arc<ShareEventState>,
    event: ShareTransferEvent,
    finished: bool,
}

impl ShareEventGuard {
    fn admitted(events: &Arc<ShareEventState>, event: ShareTransferEvent) -> Self {
        events.emit(event.clone());
        Self {
            events: events.clone(),
            event,
            finished: false,
        }
    }

    fn finish(mut self, phase: SharePhase, provider_epoch: Option<u64>, grant: Option<GrantNonce>) {
        self.finished = true;
        self.event.phase = phase;
        self.event.provider_epoch = provider_epoch;
        self.event.grant = grant;
        self.events.emit(self.event.clone());
    }
}

impl Drop for ShareEventGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.event.phase = SharePhase::Reject;
            self.event.bytes = 0;
            self.event.provider_epoch = None;
            self.event.grant = None;
            self.events.emit(self.event.clone());
        }
    }
}

/// Shared observer state for one already-bound endpoint. Keeping the callback
/// and operation sequence together lets every session and inbound handler use
/// the same operation-ID namespace without widening constructors.
#[derive(Debug)]
struct ShareEventState {
    observer: Mutex<Option<ShareTransferObserver>>,
    sequence: AtomicU64,
}

/// Emits the optional F collector record for one already-validated payload
/// chunk. Byte-bearing `Swarm` events are produced only after an outbound
/// verified Store read and `send.write_all`, or after an inbound hash check
/// and `put_verified`; this helper deliberately has no path, identity, grant,
/// or share fields. It is debug-level and target-filtered, so normal runs do
/// not pay for a payload trace. A faulty subscriber cannot affect transfer
/// admission, drain, or persistence.
fn trace_verified_payload(event: &ShareTransferEvent) {
    if event.phase != SharePhase::Swarm || event.bytes == 0 {
        return;
    }
    let direction = match event.direction {
        TransferDirection::Inbound => "inbound",
        TransferDirection::Outbound => "outbound",
    };
    let verified_bytes = event.bytes;
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        if !tracing::enabled!(target: SHARE_PAYLOAD_TRACE_TARGET, tracing::Level::DEBUG) {
            return;
        }
        let sequence = SHARE_PAYLOAD_TRACE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            target: SHARE_PAYLOAD_TRACE_TARGET,
            version = 1u8,
            protocol = "deltaweave/share-swarm/1",
            direction = direction,
            verified_bytes,
            verified_chunks = 1u8,
            sequence,
        );
    }));
}

impl ShareEventState {
    fn new() -> Self {
        Self {
            observer: Mutex::new(None),
            sequence: AtomicU64::new(1),
        }
    }

    fn set_observer(&self, observer: Option<ShareTransferObserver>) {
        if let Ok(mut current) = self.observer.lock() {
            *current = observer;
        }
    }

    fn emit(&self, event: ShareTransferEvent) {
        trace_verified_payload(&event);
        let observer = self
            .observer
            .lock()
            .ok()
            .and_then(|observer| observer.clone());
        if let Some(observer) = observer {
            observer.emit(event);
        }
    }

    fn next_query_id(&self, share: ShareId, peer: EndpointId, tag: &[u8]) -> [u8; 16] {
        query_event_id(&self.sequence, share, peer, tag)
    }
}

#[cfg(test)]
mod payload_trace_tests {
    use super::*;

    #[derive(Debug)]
    struct RecordedEvent {
        target: String,
        fields: BTreeMap<String, String>,
    }

    #[derive(Clone)]
    struct RecordingSubscriber {
        events: Arc<Mutex<Vec<RecordedEvent>>>,
        panic_on_event: bool,
    }

    impl RecordingSubscriber {
        fn accepts(metadata: &tracing::Metadata<'_>) -> bool {
            metadata.target() == SHARE_PAYLOAD_TRACE_TARGET
                && *metadata.level() == tracing::Level::DEBUG
        }
    }

    impl tracing::Subscriber for RecordingSubscriber {
        fn register_callsite(
            &self,
            metadata: &'static tracing::Metadata<'static>,
        ) -> tracing::subscriber::Interest {
            if Self::accepts(metadata) {
                tracing::subscriber::Interest::always()
            } else {
                tracing::subscriber::Interest::never()
            }
        }

        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            Self::accepts(metadata)
        }

        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            if self.panic_on_event {
                panic!("test payload trace subscriber failure");
            }
            let mut visitor = FieldValues::default();
            event.record(&mut visitor);
            self.events.lock().unwrap().push(RecordedEvent {
                target: event.metadata().target().to_owned(),
                fields: visitor.0,
            });
        }

        fn enter(&self, _span: &tracing::span::Id) {}

        fn exit(&self, _span: &tracing::span::Id) {}
    }

    #[derive(Default)]
    struct FieldValues(BTreeMap<String, String>);

    impl tracing::field::Visit for FieldValues {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.insert(field.name().to_owned(), format!("{value:?}"));
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.insert(field.name().to_owned(), value.to_owned());
        }
    }

    fn event(phase: SharePhase, direction: TransferDirection, bytes: u64) -> ShareTransferEvent {
        ShareTransferEvent {
            operation_id: [0x11; 16],
            share: ShareId([0x22; 32]),
            peer: SecretKey::from_bytes(&[0x33; 32]).public(),
            phase,
            direction,
            bytes,
            epoch: 4,
            provider_epoch: Some(5),
            grant: Some([0x44; 32]),
        }
    }

    #[test]
    fn payload_trace_is_opt_in_bounded_and_secret_free() {
        let state = ShareEventState::new();
        let no_subscriber =
            tracing::dispatcher::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        tracing::dispatcher::with_default(&no_subscriber, || {
            assert!(!tracing::enabled!(
                target: SHARE_PAYLOAD_TRACE_TARGET,
                tracing::Level::DEBUG
            ));
            state.emit(event(SharePhase::Swarm, TransferDirection::Inbound, 9));
        });

        let events = Arc::new(Mutex::new(Vec::new()));
        let subscriber = RecordingSubscriber {
            events: events.clone(),
            panic_on_event: false,
        };
        let dispatch = tracing::dispatcher::Dispatch::new(subscriber);
        tracing::dispatcher::with_default(&dispatch, || {
            state.emit(event(SharePhase::Swarm, TransferDirection::Inbound, 9));
            state.emit(event(SharePhase::Swarm, TransferDirection::Outbound, 13));
            state.emit(event(SharePhase::Swarm, TransferDirection::Outbound, 0));
            state.emit(event(SharePhase::Drain, TransferDirection::Outbound, 99));
        });

        let events = events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|event| {
            event.target == SHARE_PAYLOAD_TRACE_TARGET
                && event.fields.len() == 6
                && event.fields.contains_key("version")
                && event.fields.contains_key("protocol")
                && event.fields.contains_key("direction")
                && event.fields.contains_key("verified_bytes")
                && event.fields.contains_key("verified_chunks")
                && event.fields.contains_key("sequence")
                && !event.fields.contains_key("operation_id")
                && !event.fields.contains_key("share")
                && !event.fields.contains_key("peer")
                && !event.fields.contains_key("grant")
        }));
        assert_eq!(events[0].fields.get("version").unwrap(), "1");
        assert_eq!(
            events[0].fields.get("protocol").unwrap(),
            "deltaweave/share-swarm/1"
        );
        assert_eq!(events[0].fields.get("direction").unwrap(), "inbound");
        assert_eq!(events[0].fields.get("verified_bytes").unwrap(), "9");
        assert_eq!(events[0].fields.get("verified_chunks").unwrap(), "1");
        assert_eq!(events[1].fields.get("direction").unwrap(), "outbound");
        assert_eq!(events[1].fields.get("verified_bytes").unwrap(), "13");
        assert_ne!(
            events[0].fields.get("sequence"),
            events[1].fields.get("sequence")
        );
    }

    #[test]
    fn payload_trace_subscriber_failure_does_not_escape_event_emit() {
        let state = ShareEventState::new();
        let observer_calls = Arc::new(AtomicUsize::new(0));
        let observer_calls_clone = observer_calls.clone();
        state.set_observer(Some(ShareTransferObserver::new(move |_| {
            observer_calls_clone.fetch_add(1, Ordering::Relaxed);
        })));
        let subscriber = RecordingSubscriber {
            events: Arc::new(Mutex::new(Vec::new())),
            panic_on_event: true,
        };
        let dispatch = tracing::dispatcher::Dispatch::new(subscriber);
        tracing::dispatcher::with_default(&dispatch, || {
            state.emit(event(SharePhase::Swarm, TransferDirection::Outbound, 7));
        });
        assert_eq!(observer_calls.load(Ordering::Relaxed), 1);
    }
}

type SupplierMap = Arc<RwLock<BTreeMap<(EndpointId, ShareId), Arc<SupplierRegistrationGuard>>>>;

/// The service-owned lifecycle coordinator for supplier registrations.  A
/// guard keeps only a weak reference to this object, so a registration cannot
/// keep the endpoint alive after shutdown; while the service is alive,
/// `SupplierRegistrationGuard::drain` is a real admission close and exact map
/// removal rather than a boolean hint.
#[derive(Debug)]
struct SupplierLifecycle {
    suppliers: SupplierMap,
}

type SwarmOperationKey = (GrantNonce, [u8; 16]);

#[derive(Debug, Default)]
struct SwarmOperationState {
    active: BTreeSet<SwarmOperationKey>,
    closed: BTreeSet<SwarmOperationKey>,
    completed: BTreeSet<SwarmOperationKey>,
}

/// A process-local admission token for one exact grant nonce/operation pair.
/// The token is acquired before an outbound task starts IO, and immediately
/// after an inbound task has parsed its grant.  Its drop removes the accepted
/// operation and wakes recovery proofs waiting for the same operation.
#[derive(Debug)]
struct SwarmOperationGuard {
    registry: Weak<SwarmTaskRegistry>,
    key: SwarmOperationKey,
    drained: bool,
}

impl Drop for SwarmOperationGuard {
    fn drop(&mut self) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        if let Ok(mut state) = registry.operations.lock() {
            state.active.remove(&self.key);
            if self.drained && state.completed.len() < MAX_SWARM_OPERATION_KEYS {
                state.completed.insert(self.key);
            }
        }
        registry.operation_notify.notify_waiters();
    }
}

/// Keeps an accepted handler inside the recovery/shutdown ownership boundary
/// until its task has actually returned.  Inbound handlers need this token
/// while they are still waiting to parse the grant, because their exact
/// operation key is not known at `accept` time.
#[derive(Debug)]
struct PendingTaskGuard {
    registry: Weak<SwarmTaskRegistry>,
}

impl Drop for PendingTaskGuard {
    fn drop(&mut self) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        registry.pending_tasks.fetch_sub(1, Ordering::SeqCst);
        registry.operation_notify.notify_waiters();
    }
}

/// Service-owned registry for every accepted share-swarm operation,
/// including outbound member fetches. A caller dropping its future therefore
/// cannot detach a task that still owns a root/store Arc or a blocking CAS
/// writer; shutdown closes admission and awaits the same registry.
#[derive(Debug)]
struct SwarmTaskRegistry {
    closed: AtomicBool,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    operations: Mutex<SwarmOperationState>,
    operation_notify: tokio::sync::Notify,
    pending_tasks: AtomicUsize,
}

impl Default for SwarmTaskRegistry {
    fn default() -> Self {
        Self {
            closed: AtomicBool::new(false),
            tasks: Mutex::new(Vec::new()),
            operations: Mutex::new(SwarmOperationState::default()),
            operation_notify: tokio::sync::Notify::new(),
            pending_tasks: AtomicUsize::new(0),
        }
    }
}

impl SwarmTaskRegistry {
    fn close_admission(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    fn reserve_task(self: &Arc<Self>) -> PendingTaskGuard {
        self.pending_tasks.fetch_add(1, Ordering::SeqCst);
        PendingTaskGuard {
            registry: Arc::downgrade(self),
        }
    }

    /// Reserves one exact operation before its task is allowed to perform
    /// endpoint, root, or CAS work.  Recovery closes the same key under this
    /// mutex, so a retry cannot slip in between an idle observation and the
    /// returned drain proof.  A different operation for the same grant nonce
    /// is rejected as well; the durable registry has one immutable operation
    /// binding per nonce.
    fn begin_operation(self: &Arc<Self>, key: SwarmOperationKey) -> Result<SwarmOperationGuard> {
        ensure!(!self.closed.load(Ordering::SeqCst), ShareError::Busy);
        let mut state = self
            .operations
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        ensure!(!self.closed.load(Ordering::SeqCst), ShareError::Busy);
        ensure!(
            !state.closed.iter().any(|(nonce, _)| nonce == &key.0),
            ShareError::Busy
        );
        ensure!(
            !state.active.iter().any(|(nonce, _)| nonce == &key.0),
            ShareError::Busy
        );
        ensure!(
            !state.completed.iter().any(|(nonce, _)| nonce == &key.0),
            ShareError::Busy
        );
        ensure!(
            state.closed.len() < MAX_SWARM_OPERATION_KEYS || state.closed.contains(&key),
            ShareError::Busy
        );
        state.active.insert(key);
        drop(state);
        Ok(SwarmOperationGuard {
            registry: Arc::downgrade(self),
            key,
            drained: false,
        })
    }

    /// Marks an accepted operation's stream/storage boundary as drained.
    /// This is intentionally separate from dropping the admission guard:
    /// cancellation or a bounded transport timeout may end the handler while
    /// a retained connection still owns bytes, and that must not become local
    /// drain evidence for current-boot recovery.
    fn mark_operation_drained(guard: &mut SwarmOperationGuard, drained: bool) {
        guard.drained = drained;
    }

    /// Closes one operation's admission and waits for the exact accepted
    /// operation task to release its token.  A still-starting inbound handler
    /// has not acquired an operation key yet; once it parses its grant it
    /// observes this closed key in `begin_operation` and is rejected before
    /// any payload or storage IO.  Unkeyed accepted handlers are nevertheless
    /// retained by `pending_tasks` for the endpoint-wide shutdown barrier.
    async fn close_operation_and_drain(&self, key: SwarmOperationKey) -> Result<()> {
        loop {
            let notified = self.operation_notify.notified();
            let wait = {
                let mut state = self
                    .operations
                    .lock()
                    .map_err(|_| ShareError::StateUnavailable)?;
                if state.closed.iter().any(|(nonce, closed_operation)| {
                    nonce == &key.0 && (*nonce, *closed_operation) != key
                }) {
                    return Err(ShareError::GrantReplay.into());
                }
                if state.active.iter().any(|(nonce, active_operation)| {
                    nonce == &key.0 && (*nonce, *active_operation) != key
                }) || state.completed.iter().any(|(nonce, completed_operation)| {
                    nonce == &key.0 && (*nonce, *completed_operation) != key
                }) {
                    return Err(ShareError::GrantReplay.into());
                }
                if !state.closed.contains(&key) {
                    ensure!(
                        state.closed.len() < MAX_SWARM_OPERATION_KEYS,
                        ShareError::Busy
                    );
                    state.closed.insert(key);
                }
                state.active.iter().any(|(nonce, _)| nonce == &key.0)
            };
            if !wait {
                return Ok(());
            }
            notified.await;
        }
    }

    /// Nonblocking form used by bounded startup recovery.  A false result
    /// leaves the operation closed and is retried on a later tick once the
    /// accepted task has released its token.
    fn try_close_operation(&self, key: SwarmOperationKey) -> Result<bool> {
        let mut state = self
            .operations
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        if state
            .closed
            .iter()
            .any(|(nonce, closed_operation)| nonce == &key.0 && (*nonce, *closed_operation) != key)
        {
            return Err(ShareError::GrantReplay.into());
        }
        if state
            .active
            .iter()
            .any(|(nonce, active_operation)| nonce == &key.0 && (*nonce, *active_operation) != key)
            || state.completed.iter().any(|(nonce, completed_operation)| {
                nonce == &key.0 && (*nonce, *completed_operation) != key
            })
        {
            return Err(ShareError::GrantReplay.into());
        }
        if !state.closed.contains(&key) {
            ensure!(
                state.closed.len() < MAX_SWARM_OPERATION_KEYS,
                ShareError::Busy
            );
            state.closed.insert(key);
        }
        Ok(!state.active.iter().any(|(nonce, _)| nonce == &key.0))
    }

    /// Current-process recovery may claim local IO drain only for an exact
    /// operation that was admitted and explicitly marked drained.  A durable
    /// row with no previous boot marker is otherwise indistinguishable from a
    /// fabricated journal entry, so this method fails closed without closing
    /// a new operation's admission.
    fn try_close_completed_operation(&self, key: SwarmOperationKey) -> Result<bool> {
        let mut state = self
            .operations
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        if state
            .closed
            .iter()
            .any(|(nonce, closed_operation)| nonce == &key.0 && (*nonce, *closed_operation) != key)
            || state.active.iter().any(|(nonce, active_operation)| {
                nonce == &key.0 && (*nonce, *active_operation) != key
            })
            || state.completed.iter().any(|(nonce, completed_operation)| {
                nonce == &key.0 && (*nonce, *completed_operation) != key
            })
        {
            return Err(ShareError::GrantReplay.into());
        }
        if !state.completed.contains(&key) {
            return Ok(false);
        }
        ensure!(
            state.closed.contains(&key) || state.closed.len() < MAX_SWARM_OPERATION_KEYS,
            ShareError::Busy
        );
        state.closed.insert(key);
        Ok(!state.active.iter().any(|(nonce, _)| nonce == &key.0))
    }

    fn release_closed_operation(&self, key: SwarmOperationKey) -> Result<()> {
        let mut state = self
            .operations
            .lock()
            .map_err(|_| ShareError::StateUnavailable)?;
        state.closed.remove(&key);
        state.completed.remove(&key);
        Ok(())
    }

    /// Registers a task and releases its start gate while holding the same
    /// mutex used by shutdown.  The task is spawned before this method (the
    /// Tokio API requires that), but it cannot perform endpoint/root/CAS IO
    /// before the caller gives it this gate.  Combining registration and the
    /// signal closes the gap where shutdown could snapshot an accepted task
    /// before it started, or a cancelled caller could release the gate after
    /// the task had escaped the registry.
    fn register_and_start(
        &self,
        task: tokio::task::JoinHandle<()>,
        start: tokio::sync::oneshot::Sender<()>,
    ) -> bool {
        let mut tasks = match self.tasks.lock() {
            Ok(tasks) => tasks,
            Err(_) => {
                task.abort();
                return false;
            }
        };
        if self.closed.load(Ordering::SeqCst) {
            task.abort();
            return false;
        }
        tasks.retain(|task| !task.is_finished());
        tasks.push(task);
        // Sending while the registry lock is held linearizes task start with
        // close_admission/close_and_drain. A full oneshot receiver is only
        // possible if the task already exited before the gate; in that case
        // no endpoint or storage work escaped and retaining the finished
        // handle is harmless.
        let _ = start.send(());
        true
    }

    async fn close_and_drain(&self) -> Result<()> {
        self.close_admission();
        let tasks = std::mem::take(
            &mut *self
                .tasks
                .lock()
                .map_err(|_| anyhow::anyhow!("swarm task registry is poisoned"))?,
        );
        crate::await_swarm_tasks(tasks).await?;
        // A handler can have reserved ownership and be between `spawn` and
        // `register_and_start` when shutdown closes admission.  Its start
        // gate is then aborted and the pending token is dropped only when the
        // task actually returns.  Await that token as well as the registered
        // JoinHandles so no root/store Arc escapes the service shutdown
        // boundary.
        self.wait_for_pending_tasks().await;
        Ok(())
    }

    async fn wait_for_pending_tasks(&self) {
        loop {
            let notified = self.operation_notify.notified();
            if self.pending_tasks.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

/// The provider-side activation lease returned after a signed owner reply.
///
/// `deadline` is derived from the instant immediately before the activation
/// request was sent.  Keeping that local monotonic deadline next to the reply
/// prevents a caller from accidentally starting a fresh lease when a reply is
/// received late.  The type is intentionally local to this process and is not
/// serialized onto the wire.
#[derive(Clone, Debug)]
pub struct ActivationLease {
    pub reply: super::ActivateGrantReply,
    pub deadline: Instant,
}

impl ActivationLease {
    fn from_reply_at(
        reply: super::ActivateGrantReply,
        request_started: Instant,
        now: Instant,
    ) -> Result<Self> {
        ensure!(reply.accepted, ShareError::GrantReplay);
        let duration = Duration::from_secs(u64::from(
            reply
                .max_duration_secs
                .min(super::authority::MAX_ACTIVATE_TTL_SECONDS),
        ));
        let deadline = request_started
            .checked_add(duration)
            .ok_or(ShareError::GrantExpired)?;
        ensure!(now < deadline, ShareError::GrantExpired);
        Ok(Self { reply, deadline })
    }

    /// Returns the remaining monotonic lifetime without exposing wall-clock
    /// expiry or allowing a late response to extend this lease.
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    /// Whether this local activation lease can still admit provider work.
    pub fn is_active(&self) -> bool {
        Instant::now() < self.deadline
    }
}

#[cfg(test)]
mod swarm_task_registry_tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registration_starts_and_shutdown_drains_an_accepted_blocking_task() {
        let registry = Arc::new(SwarmTaskRegistry::default());
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let task = tokio::spawn(async move {
            start_rx.await.expect("registered task start gate");
            let blocking = tokio::task::spawn_blocking(move || {
                let _ = entered_tx.send(());
                release_rx.recv().expect("test releases blocking IO");
            });
            blocking.await.expect("blocking IO task joined");
        });

        assert!(registry.register_and_start(task, start_tx));
        entered_rx.await.expect("accepted task reached blocking IO");

        let mut drain = tokio::spawn({
            let registry = registry.clone();
            async move { registry.close_and_drain().await }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut drain)
                .await
                .is_err(),
            "shutdown must wait for accepted blocking IO"
        );
        release_tx.send(()).expect("release accepted blocking IO");
        drain
            .await
            .expect("shutdown task joined")
            .expect("drain succeeds");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn closed_registry_rejects_and_never_starts_a_task() {
        let registry = SwarmTaskRegistry::default();
        registry.close_admission();
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();
        let started = Arc::new(AtomicBool::new(false));
        let started_task = started.clone();
        let task = tokio::spawn(async move {
            if start_rx.await.is_ok() {
                started_task.store(true, Ordering::SeqCst);
            }
        });

        assert!(!registry.register_and_start(task, start_tx));
        tokio::task::yield_now().await;
        assert!(!started.load(Ordering::SeqCst));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn operation_drain_closes_same_nonce_before_returning_proof() {
        let registry = Arc::new(SwarmTaskRegistry::default());
        let key = ([0x91; 32], [0x92; 16]);
        let guard = registry.begin_operation(key).expect("operation admission");
        let drain = tokio::spawn({
            let registry = registry.clone();
            async move { registry.close_operation_and_drain(key).await }
        });
        tokio::task::yield_now().await;
        assert!(
            !drain.is_finished(),
            "drain must own the accepted operation"
        );
        assert!(registry.begin_operation(key).is_err());
        drop(guard);
        drain
            .await
            .expect("drain task joined")
            .expect("drain completed");
        assert!(
            registry.begin_operation(key).is_err(),
            "a closed operation must not be admitted again"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn operation_drain_waits_for_an_accepted_task_without_lost_wakeup() {
        let registry = Arc::new(SwarmTaskRegistry::default());
        let key = ([0xa1; 32], [0xa2; 16]);
        let operation = registry.begin_operation(key).expect("operation admission");
        let mut drain = Box::pin(registry.close_operation_and_drain(key));

        // Poll the drain once while the accepted task is still owned.  The
        // release below happens at the exact final-count boundary; the
        // implementation must have installed its Notify waiter before the
        // count check or this test would hang forever.
        tokio::task::yield_now().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut drain)
                .await
                .is_err()
        );
        drop(operation);
        drain.await.expect("accepted task drain completes");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_waits_for_reserved_task_before_returning() {
        let registry = Arc::new(SwarmTaskRegistry::default());
        let pending = registry.reserve_task();
        let mut shutdown = Box::pin(registry.close_and_drain());
        tokio::task::yield_now().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut shutdown)
                .await
                .is_err()
        );
        drop(pending);
        shutdown.await.expect("shutdown waits for reserved task");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn operation_proof_does_not_wait_for_an_unkeyed_peer_handler() {
        let registry = Arc::new(SwarmTaskRegistry::default());
        let key = ([0xb1; 32], [0xb2; 16]);
        let operation = registry.begin_operation(key).expect("operation admission");
        let pending = registry.reserve_task();
        drop(operation);

        // An inbound handler can still be before grant parsing. It must be
        // rejected by begin_operation after this exact key is closed, but it
        // must not make a per-operation proof wait for unrelated endpoint
        // activity. Endpoint-wide shutdown continues to await `pending`.
        tokio::time::timeout(
            Duration::from_millis(25),
            registry.close_operation_and_drain(key),
        )
        .await
        .expect("exact operation drain is independent of unrelated handlers")
        .expect("exact operation drain succeeds");
        drop(pending);
    }

    #[test]
    fn operation_lease_rejects_a_different_operation_for_the_same_nonce() {
        let registry = Arc::new(SwarmTaskRegistry::default());
        let nonce = [0x93; 32];
        let first = registry
            .begin_operation((nonce, [0x94; 16]))
            .expect("first operation admission");
        assert!(registry.begin_operation((nonce, [0x95; 16])).is_err());
        drop(first);
    }

    #[test]
    fn current_boot_recovery_requires_an_explicit_drain_marker() {
        let registry = Arc::new(SwarmTaskRegistry::default());
        let key = ([0x96; 32], [0x97; 16]);
        let guard = registry.begin_operation(key).expect("operation admission");
        drop(guard);
        assert!(
            !registry
                .try_close_completed_operation(key)
                .expect("unmarked operation query")
        );

        let mut guard = registry.begin_operation(key).expect("retry admission");
        SwarmTaskRegistry::mark_operation_drained(&mut guard, true);
        drop(guard);
        assert!(
            registry
                .try_close_completed_operation(key)
                .expect("marked operation query")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forced_connection_close_confirms_before_current_boot_drain_marker() {
        let server = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .alpns(vec![b"deltaweave/test-drain".to_vec()])
            .clear_ip_transports()
            .bind_addr(
                "127.0.0.1:0"
                    .parse::<std::net::SocketAddr>()
                    .expect("loopback address"),
            )
            .expect("bind address")
            .bind()
            .await
            .expect("test server endpoint");
        let socket = server
            .bound_sockets()
            .into_iter()
            .find(|socket| socket.is_ipv4())
            .expect("test server IPv4 socket");
        let server_address =
            iroh::EndpointAddr::from_parts(server.id(), [iroh::TransportAddr::Ip(socket)]);
        let server_task = tokio::spawn({
            let server = server.clone();
            async move {
                for _ in 0..2 {
                    let incoming = server.accept().await.expect("incoming test connection");
                    let connection = incoming.await.expect("test handshake");
                    connection.closed().await;
                }
            }
        });
        let client = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .bind()
            .await
            .expect("test client endpoint");
        let connection = client
            .connect(server_address.clone(), b"deltaweave/test-drain")
            .await
            .expect("test connection");

        let registry = Arc::new(SwarmTaskRegistry::default());
        let key = ([0xa6; 32], [0xa7; 16]);
        let unmarked = registry.begin_operation(key).expect("operation admission");
        connection.close(0u8.into(), b"forced close test");
        drop(unmarked);
        assert!(
            !registry
                .try_close_completed_operation(key)
                .expect("unmarked operation query"),
            "calling close alone must not create a current-boot drain marker"
        );

        let connection = client
            .connect(server_address, b"deltaweave/test-drain")
            .await
            .expect("second test connection");
        assert!(
            connection.close_reason().is_none(),
            "the second connection must begin open"
        );
        let mut marked = registry.begin_operation(key).expect("operation retry");
        assert!(
            Handler::wait_closed_bounded_until(&connection, Duration::ZERO).await,
            "timeout path must close and observe the owned connection's terminal state"
        );
        assert!(
            connection.close_reason().is_some(),
            "close confirmation must observe a terminal reason"
        );
        SwarmTaskRegistry::mark_operation_drained(&mut marked, true);
        drop(marked);
        assert!(
            registry
                .try_close_completed_operation(key)
                .expect("confirmed operation query"),
            "only the observed close may create the exact current-boot proof"
        );

        client.close().await;
        server.close().await;
        server_task.await.expect("server close observer");
    }
}

/// One device-wide persistent endpoint. Clone its endpoint for all outbound shares;
/// legacy per-folder identities remain separate and are never rebound here.
#[derive(Debug)]
pub struct ShareService {
    router: Router,
    key: SecretKey,
    registry: Arc<Registry>,
    runtimes: Arc<RwLock<BTreeMap<ShareId, Arc<OwnedRuntime>>>>,
    lifecycle: tokio::sync::Mutex<()>,
    mode: NetworkMode,
    active: Arc<tokio::sync::RwLock<()>>,
    suppliers: SupplierMap,
    supplier_lifecycle: Arc<SupplierLifecycle>,
    swarm_tasks: Arc<SwarmTaskRegistry>,
    share_events: Arc<ShareEventState>,
    /// Rotates bounded recovery batches so a slow first row cannot starve
    /// later endpoint-local intents across managed ticks.
    intent_recovery_cursor: AtomicUsize,
    #[cfg(test)]
    admission_limit: Arc<tokio::sync::Semaphore>,
}

/// Opaque ownership proof for the already-bound device endpoint.  It exposes
/// no bind or legacy protocol registration operation; E may use it only when
/// constructing the separate grant-gated adapter.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct ShareEndpointOwnership {
    endpoint: crate::Endpoint,
    id: EndpointId,
}
impl ShareEndpointOwnership {
    #[allow(dead_code)]
    pub(crate) fn endpoint_id(&self) -> EndpointId {
        self.id
    }
    #[allow(dead_code)]
    pub(crate) fn endpoint(&self) -> &crate::Endpoint {
        &self.endpoint
    }
}

/// Holds the exact admission/index/store Arcs used by a member runtime.  A
/// later swarm handler must retain this guard for every supplier operation and
/// drain it before releasing the engine's root lease.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct SupplierRegistrationGuard {
    owner: EndpointId,
    share: ShareId,
    membership: Membership,
    root_lease: Arc<RootLease>,
    private_root: PathBuf,
    index: Arc<LocalIndex>,
    store: Arc<Store>,
    drained: Arc<std::sync::atomic::AtomicBool>,
    inflight: Arc<AtomicUsize>,
    inflight_notify: Arc<tokio::sync::Notify>,
    generation: Arc<()>,
    lifecycle: Weak<SupplierLifecycle>,
}

/// A typed local proof that an endpoint-local grant operation has no remaining
/// writer/stream work.  The proof binds the exact persisted intent, process
/// generation, and managed public/private admission lease.  It is a local
/// recovery capability only; it does not authorize a new payload operation or
/// extend a remote activation lease.
#[derive(Clone, Debug)]
pub struct LocalIoDrainProof {
    share: ShareId,
    owner: EndpointId,
    side: ClientSide,
    operation_id: [u8; 16],
    boot_id: [u8; 16],
    root_lease: Arc<RootLease>,
    root: PathBuf,
    state_root: PathBuf,
}

/// Exact public/private paths held with a managed admission lease during
/// bounded restart recovery.  Controllers construct this only from paths
/// that were successfully acquired together; the service compares both
/// canonical paths before using the lease as drain evidence.
#[derive(Clone, Debug)]
pub struct ManagedAdmissionLease {
    lease: Arc<RootLease>,
    root: PathBuf,
    state_root: PathBuf,
}

impl ManagedAdmissionLease {
    pub fn new(lease: Arc<RootLease>, root: PathBuf, state_root: PathBuf) -> Result<Self> {
        let canonical_root = fs::canonicalize(&root)?;
        let canonical_state_root = fs::canonicalize(&state_root)?;
        ensure!(
            lease.root() == canonical_root.as_path()
                && lease
                    .private_roots()
                    .iter()
                    .any(|private| private == &canonical_state_root),
            ShareError::StateUnavailable
        );
        Ok(Self {
            lease,
            root: canonical_root,
            state_root: canonical_state_root,
        })
    }

    pub fn lease(&self) -> &Arc<RootLease> {
        &self.lease
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn state_root(&self) -> &Path {
        &self.state_root
    }
}

impl LocalIoDrainProof {
    pub fn share(&self) -> ShareId {
        self.share
    }

    pub fn owner(&self) -> EndpointId {
        self.owner
    }

    pub fn side(&self) -> ClientSide {
        self.side
    }

    pub fn operation_id(&self) -> [u8; 16] {
        self.operation_id
    }

    pub fn boot_id(&self) -> [u8; 16] {
        self.boot_id
    }

    pub fn root_lease(&self) -> &Arc<RootLease> {
        &self.root_lease
    }

    /// The canonical public root bound by the admission lease.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The canonical private state root bound by the admission lease.
    pub fn state_root(&self) -> &Path {
        &self.state_root
    }
}
impl SupplierRegistrationGuard {
    #[allow(dead_code)]
    pub(crate) fn owner(&self) -> EndpointId {
        self.owner
    }
    #[allow(dead_code)]
    pub(crate) fn share(&self) -> ShareId {
        self.share
    }
    #[allow(dead_code)]
    pub(crate) fn membership(&self) -> &Membership {
        &self.membership
    }
    #[allow(dead_code)]
    pub(crate) fn root_lease(&self) -> &Arc<RootLease> {
        &self.root_lease
    }
    #[allow(dead_code)]
    pub(crate) fn private_root(&self) -> &Path {
        &self.private_root
    }
    #[allow(dead_code)]
    pub(crate) fn index(&self) -> &Arc<LocalIndex> {
        &self.index
    }
    #[allow(dead_code)]
    pub(crate) fn store(&self) -> &Arc<Store> {
        &self.store
    }
    /// Closes this supplier admission, waits for already accepted endpoint
    /// operations, and removes exactly this registration generation.  The
    /// operation is idempotent and remains safe if the owning service has
    /// already shut down (in that case the weak lifecycle is gone).
    pub async fn drain(&self) -> Result<()> {
        self.drained.store(true, Ordering::SeqCst);
        self.wait_for_operations().await;
        if let Some(lifecycle) = self.lifecycle.upgrade() {
            lifecycle.unregister(self).await?;
        }
        Ok(())
    }

    fn mark_drained(&self) {
        self.drained.store(true, Ordering::SeqCst);
    }

    fn is_drained(&self) -> bool {
        self.drained.load(Ordering::SeqCst)
    }

    fn begin_operation(&self) -> Result<SupplierOperation> {
        if self.is_drained() {
            return Err(ShareError::Busy.into());
        }
        self.inflight.fetch_add(1, Ordering::AcqRel);
        // Closing can race the increment.  The second check makes the
        // admission boundary linearizable: an operation which observes the
        // close never receives a storage lease, while one which passed both
        // checks is included in drain's counter.
        if self.is_drained() {
            self.release_operation();
            return Err(ShareError::Busy.into());
        }
        Ok(SupplierOperation {
            inflight: self.inflight.clone(),
            notify: self.inflight_notify.clone(),
        })
    }

    fn release_operation(&self) {
        self.inflight.fetch_sub(1, Ordering::AcqRel);
        self.inflight_notify.notify_waiters();
    }

    async fn wait_for_operations(&self) {
        loop {
            // Create the waiter before observing the counter.  A release can
            // otherwise notify between the load and `notified()`, leaving a
            // shutdown task asleep forever at the final operation boundary.
            let notified = self.inflight_notify.notified();
            if self.inflight.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

/// A counted lease for one member-provider operation.  It is deliberately
/// separate from the registration guard so draining can close admission and
/// wait for actual storage users without holding the endpoint-wide control
/// read gate (which would otherwise deadlock owner status/drain requests).
#[derive(Debug)]
struct SupplierOperation {
    inflight: Arc<AtomicUsize>,
    notify: Arc<tokio::sync::Notify>,
}

impl Drop for SupplierOperation {
    fn drop(&mut self) {
        self.inflight.fetch_sub(1, Ordering::AcqRel);
        self.notify.notify_waiters();
    }
}

impl SupplierLifecycle {
    async fn unregister(&self, guard: &SupplierRegistrationGuard) -> Result<()> {
        guard.mark_drained();
        // The per-registration counter waits for every accepted swarm handler
        // that actually borrowed this supplier's storage.  We intentionally
        // do not take the endpoint-wide control gate here: owner status,
        // cancellation, and bilateral drain requests must remain serviceable
        // while a member provider is closing.
        guard.wait_for_operations().await;
        let key = (guard.owner, guard.share);
        let mut suppliers = self.suppliers.write().expect("supplier map");
        if let Some(existing) = suppliers.get(&key)
            && Arc::ptr_eq(&existing.generation, &guard.generation)
        {
            suppliers.remove(&key);
        }
        Ok(())
    }
}
impl ShareService {
    pub async fn open(
        state: impl AsRef<Path>,
        mode: NetworkMode,
        bind: Option<SocketAddr>,
    ) -> Result<Self> {
        let state = root_admission::reserve_private(state)?;
        root_admission::private_directory(&state)?;
        let key = load_or_create_identity(state.join("device.key"))?.secret_key;
        let registry = Arc::new(Registry::open(&state, key.public())?);
        let endpoint = bind_endpoint(
            key.clone(),
            mode,
            Some(vec![ALPN_V3.to_vec(), ALPN_SWARM_V1.to_vec()]),
            bind,
        )
        .await?;
        Ok(Self::from_bound_endpoint(key, registry, mode, endpoint))
    }

    fn from_bound_endpoint(
        key: SecretKey,
        registry: Arc<Registry>,
        mode: NetworkMode,
        endpoint: crate::Endpoint,
    ) -> Self {
        Self::from_bound_endpoint_with_limit(key, registry, mode, endpoint, 64)
    }

    fn from_bound_endpoint_with_limit(
        key: SecretKey,
        registry: Arc<Registry>,
        mode: NetworkMode,
        endpoint: crate::Endpoint,
        max_connections: usize,
    ) -> Self {
        assert!(max_connections > 0, "test endpoint limit must be positive");
        let runtimes = Arc::new(RwLock::new(BTreeMap::new()));
        let active = Arc::new(tokio::sync::RwLock::new(()));
        let limit = Arc::new(tokio::sync::Semaphore::new(max_connections));
        let suppliers = Arc::new(RwLock::new(BTreeMap::new()));
        let supplier_lifecycle = Arc::new(SupplierLifecycle {
            suppliers: suppliers.clone(),
        });
        let swarm_tasks = Arc::new(SwarmTaskRegistry::default());
        let swarm_streams = Arc::new(tokio::sync::Semaphore::new(
            max_connections.saturating_mul(2).max(1),
        ));
        let swarm_inflight = Arc::new(Mutex::new(BTreeSet::new()));
        let share_events = Arc::new(ShareEventState::new());
        #[cfg(test)]
        let admission_limit = limit.clone();
        let endpoint_for_swarm = endpoint.clone();
        let handler = Handler {
            registry: registry.clone(),
            key: key.clone(),
            runtimes: runtimes.clone(),
            limit,
            active: active.clone(),
        };
        let router = Router::builder(endpoint)
            .accept(ALPN_V3, handler)
            .accept(
                ALPN_SWARM_V1,
                SwarmAdmissionHandler {
                    key: key.clone(),
                    registry: registry.clone(),
                    runtimes: runtimes.clone(),
                    suppliers: suppliers.clone(),
                    endpoint: endpoint_for_swarm,
                    mode,
                    active: active.clone(),
                    tasks: swarm_tasks.clone(),
                    streams: swarm_streams.clone(),
                    inflight: swarm_inflight.clone(),
                    share_events: share_events.clone(),
                },
            )
            .spawn();
        Self {
            router,
            key,
            registry,
            runtimes,
            lifecycle: tokio::sync::Mutex::new(()),
            mode,
            active,
            suppliers,
            supplier_lifecycle,
            swarm_tasks,
            share_events,
            intent_recovery_cursor: AtomicUsize::new(0),
            #[cfg(test)]
            admission_limit,
        }
    }
    pub fn endpoint_id(&self) -> EndpointId {
        self.key.public()
    }

    /// Installs the additive share-operation observer on this already-bound
    /// endpoint. Existing sessions and both inbound protocol handlers see the
    /// same callback through the shared service state.
    pub fn set_share_observer(&self, observer: Option<ShareTransferObserver>) {
        self.share_events.set_observer(observer);
    }
    #[cfg(test)]
    fn available_admission_slots(&self) -> usize {
        self.admission_limit.available_permits()
    }
    #[allow(dead_code)]
    pub(crate) fn endpoint_ownership(&self) -> ShareEndpointOwnership {
        ShareEndpointOwnership {
            endpoint: self.router.endpoint().clone(),
            id: self.endpoint_id(),
        }
    }
    pub fn endpoint_addr(&self) -> EndpointAddr {
        endpoint_addr_with_local_fallback(self.router.endpoint())
    }
    /// E's provider handler uses the existing registry and endpoint rather
    /// than opening a second authority/store.  This wrapper keeps the
    /// registry private while exposing the exact grant admission primitive.
    #[allow(dead_code)]
    pub(crate) fn validate_provider_grant(
        &self,
        grant: &ShareGrant,
        remote_consumer: EndpointId,
    ) -> Result<Instant> {
        self.registry
            .validate_provider_grant(grant, self.endpoint_id(), remote_consumer)
    }
    #[allow(dead_code)]
    pub(crate) fn drain_grant(
        &self,
        share: ShareId,
        nonce: GrantNonce,
        activation_id: [u8; 16],
        remote_peer: EndpointId,
    ) -> Result<()> {
        self.registry
            .drain_grant(share, nonce, activation_id, remote_peer)
    }
    pub async fn wait_online(&self, timeout: std::time::Duration) -> bool {
        if self.mode == NetworkMode::DirectOnly {
            return crate::wait_for_direct_address(self.router.endpoint(), timeout)
                .await
                .is_ok();
        }
        tokio::time::timeout(timeout, self.router.endpoint().online())
            .await
            .is_ok()
    }
    pub fn owned_configs(&self) -> Result<Vec<OwnedShareConfig>> {
        self.registry.configs()
    }
    /// For import, supply the exact retained DB-bound logical replica. A wrong
    /// replica fails LocalIndex's unchanged binding check without resetting state.
    pub async fn create_owned_share(
        &self,
        name: String,
        root: PathBuf,
        state_root: PathBuf,
        replica: Option<ReplicaId>,
        min_free_space_bytes: u64,
    ) -> Result<OwnerShare> {
        let _lifecycle = self.lifecycle.lock().await;
        ensure!(
            !name.is_empty() && name.len() <= 255 && !name.chars().any(char::is_control),
            ShareError::InvalidTicket
        );
        let share_id = ShareId(super::ticket::random_bytes());
        let (lease, config) = root_admission::admit_with_private(
            &root,
            RootUse::Managed {
                share: share_id.0,
                owner: *self.endpoint_id().as_bytes(),
            },
            &[state_root],
            |root, private| {
                let state_root = &private[0];
                let retained = LocalIndex::read_bound_replica(root, state_root.join("index.redb"))?;
                if let (Some(requested), Some(retained)) = (replica, retained) {
                    ensure!(requested == retained, ShareError::ReplicaClaimRejected);
                }
                let config = OwnedShareConfig {
                    share_id,
                    owner: self.endpoint_id(),
                    name,
                    root: root.to_path_buf(),
                    state_root: state_root.clone(),
                    replica: retained.or(replica).unwrap_or_else(|| {
                        ReplicaId(Hash32::from_bytes(super::ticket::random_bytes()))
                    }),
                    min_free_space_bytes,
                };
                // Short synchronous intent commit under admission serialization;
                // no runtime lock or disk handler may be acquired here.
                self.registry
                    .insert_share(config.clone(), BTreeSet::new())?;
                Ok(config)
            },
        )?;
        self.load_config_with_lease(config, lease, false).await
    }
    pub async fn load_owned_share(&self, share: ShareId) -> Result<OwnerShare> {
        let _lifecycle = self.lifecycle.lock().await;
        if let Some(runtime) = self.runtimes.read().expect("runtime map").get(&share) {
            return Ok(OwnerShare {
                runtime: runtime.clone(),
                key: self.key.clone(),
            });
        }
        self.load_config(self.registry.config(share)?).await
    }

    /// Loads an owner runtime with admission disabled before it enters the
    /// endpoint map.  Restart recovery uses this for a durable Paused state so
    /// no peer can observe a transient enabled window.
    pub async fn load_owned_share_paused(&self, share: ShareId) -> Result<OwnerShare> {
        let _lifecycle = self.lifecycle.lock().await;
        if let Some(runtime) = self.runtimes.read().expect("runtime map").get(&share) {
            runtime.disable();
            return Ok(OwnerShare {
                runtime: runtime.clone(),
                key: self.key.clone(),
            });
        }
        let config = self.registry.config(share)?;
        let lease = root_admission::acquire_with_private(
            &config.root,
            RootUse::Managed {
                share: share.0,
                owner: *config.owner.as_bytes(),
            },
            std::slice::from_ref(&config.state_root),
        )?;
        self.load_config_with_lease(config, lease, true).await
    }

    /// Pauses and drains a managed owner runtime before deleting only its
    /// transport catalog entry. The manager retains local files and state.
    pub async fn unload_owned_share(&self, share: ShareId) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        let runtime = self
            .runtimes
            .read()
            .expect("runtime map")
            .get(&share)
            .cloned();
        if let Some(runtime) = runtime {
            runtime.pause().await;
        }
        self.registry.remove_share(share)?;
        self.runtimes.write().expect("runtime map").remove(&share);
        Ok(())
    }

    pub fn forget_membership(&self, owner: EndpointId, share: ShareId) -> Result<()> {
        self.registry.forget_relationship(owner, share)
    }

    /// Registers the already-open member storage handles for a future swarm
    /// supplier.  The persisted relationship is authoritative; caller-supplied
    /// permission, epoch, replica, endpoint, or root metadata cannot replace
    /// it, and this function never opens a second root/index/store.
    #[allow(clippy::too_many_arguments)]
    pub fn register_supplier_storage(
        &self,
        owner: EndpointId,
        share: ShareId,
        membership: &Membership,
        root: &Path,
        root_lease: Arc<RootLease>,
        index: Arc<LocalIndex>,
        store: Arc<Store>,
    ) -> Result<SupplierRegistrationGuard> {
        let persisted = self.registry.relationship(owner, share)?;
        ensure!(
            persisted.membership == *membership
                && membership.endpoint == self.endpoint_id()
                && membership.owner == owner
                && membership.share_id == share
                && membership.revoked_at.is_none(),
            ShareError::ReplicaClaimRejected
        );
        let canonical_root = fs::canonicalize(root)?;
        ensure!(
            root_lease.root() == canonical_root.as_path(),
            ShareError::StateUnavailable
        );
        ensure!(
            matches!(
                root_lease.kind(),
                RootUse::Managed {
                    share: admitted_share,
                    owner: admitted_owner
                } if admitted_share == &share.0 && admitted_owner == owner.as_bytes()
            ),
            ShareError::OwnerMismatch
        );
        ensure!(
            fs::canonicalize(index.root())? == canonical_root,
            ShareError::StateUnavailable
        );
        let store_state = fs::canonicalize(store.state_root())?;
        let private_root = root_lease
            .private_roots()
            .iter()
            .filter(|private| store_state.starts_with(private))
            .max_by_key(|private| private.components().count())
            .cloned()
            .ok_or(ShareError::StateUnavailable)?;
        let guard = SupplierRegistrationGuard {
            owner,
            share,
            membership: persisted.membership,
            root_lease,
            private_root,
            index,
            store,
            drained: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            inflight: Arc::new(AtomicUsize::new(0)),
            inflight_notify: Arc::new(tokio::sync::Notify::new()),
            generation: Arc::new(()),
            lifecycle: Arc::downgrade(&self.supplier_lifecycle),
        };
        let key = (owner, share);
        let mut suppliers = self.suppliers.write().expect("supplier map");
        // A drained registration remains in the map until its own drain
        // operation has waited for accepted storage users and removed the
        // exact generation.  Replacing it here would let a new engine acquire
        // the same root/index/store while the old generation still owns an
        // in-flight operation.  Callers must await `SupplierRegistrationGuard
        // ::drain` before registering a replacement.
        ensure!(!suppliers.contains_key(&key), ShareError::Busy);
        suppliers.insert(key, Arc::new(guard.clone()));
        Ok(guard)
    }

    /// Closes one supplier admission, waits for every already-accepted swarm
    /// task to release the shared endpoint gate, then removes only the exact
    /// registration generation supplied by the caller.  The Arc-backed root
    /// lease/index/store are released with the map entry; a newer registration
    /// for the same `(owner, share)` is never removed by an old engine.
    pub async fn unregister_supplier_storage(
        &self,
        guard: &SupplierRegistrationGuard,
    ) -> Result<()> {
        self.supplier_lifecycle.unregister(guard).await
    }
    async fn load_config(&self, config: OwnedShareConfig) -> Result<OwnerShare> {
        let lease = root_admission::acquire_with_private(
            &config.root,
            RootUse::Managed {
                share: config.share_id.0,
                owner: *config.owner.as_bytes(),
            },
            std::slice::from_ref(&config.state_root),
        )?;
        self.load_config_with_lease(config, lease, false).await
    }
    async fn load_config_with_lease(
        &self,
        config: OwnedShareConfig,
        lease: RootLease,
        paused: bool,
    ) -> Result<OwnerShare> {
        let registry = self.registry.clone();
        let ready = registry.is_ready(config.share_id)?;
        let runtime = tokio::task::spawn_blocking(move || -> Result<_> {
            let lease = Arc::new(lease);
            if ready {
                ensure!(
                    config.state_root.join("index.redb").is_file()
                        && config.state_root.join("metadata.redb").is_file(),
                    ShareError::StateUnavailable
                );
            }
            let (root, state_root) = prepare_server_roots(&config.root, &config.state_root)?;
            ensure!(
                root == config.root && state_root == config.state_root,
                ShareError::StateUnavailable
            );
            let index = Arc::new(LocalIndex::open(
                &root,
                state_root.join("index.redb"),
                config.replica,
                IndexOptions::default(),
            )?);
            if ready {
                ensure!(
                    index.share_metadata()?.is_some(),
                    ShareError::StateUnavailable
                );
            }
            let store = Arc::new(Store::open_with_recovery_reserver(&state_root, |path| {
                crate::root_admission::reserve_private(path)
            })?);
            store.recover_path_changes(&root)?;
            let runtime = OwnedRuntime::new(config, registry, index, store, lease)?;
            if paused {
                runtime.disable();
            }
            let report = runtime.index.scan()?;
            crate::ensure_index_report_safe(&report)?;
            runtime.refresh_causal_state()?;
            Ok(Arc::new(runtime))
        })
        .await??;
        self.registry.mark_ready(runtime.config.share_id)?;
        self.runtimes
            .write()
            .expect("runtime map")
            .insert(runtime.config.share_id, runtime.clone());
        Ok(OwnerShare {
            runtime,
            key: self.key.clone(),
        })
    }
    pub fn relationships(&self) -> Result<Vec<MemberRelationship>> {
        self.registry.relationships()
    }
    pub async fn validate_ticket(&self, ticket: &ShareTicket) -> Result<TicketPreview> {
        ticket.verify_at(super::now())?;
        let (connection, _) = self.connect_ticket(ticket).await?;
        let result = self
            .exchange_bounded(
                &connection,
                Hello {
                    version: 3,
                    share_id: ticket.preview().share_id,
                    operation: Operation::Validate(ticket.clone()),
                },
            )
            .await;
        connection.close(0u8.into(), b"validation complete");
        match result? {
            Reply::Validated(preview) => {
                ensure!(preview == ticket.preview(), ShareError::InvalidTicket);
                Ok(preview)
            }
            _ => Err(ShareError::Protocol.into()),
        }
    }
    pub async fn enroll(
        &self,
        ticket: &ShareTicket,
        proof: Option<LegacyProof>,
    ) -> Result<Membership> {
        ticket.verify_at(super::now())?;
        let (connection, connected_address) = self.connect_ticket(ticket).await?;
        let result = self
            .exchange_bounded(
                &connection,
                Hello {
                    version: 3,
                    share_id: ticket.preview().share_id,
                    operation: Operation::Enroll {
                        ticket: ticket.clone(),
                        proof: proof.map(Box::new),
                    },
                },
            )
            .await;
        connection.close(0u8.into(), b"enrollment complete");
        match result? {
            Reply::Enrolled(member) => {
                ensure!(
                    member.owner == ticket.preview().owner
                        && member.share_id == ticket.preview().share_id
                        && member.endpoint == self.endpoint_id(),
                    ShareError::OwnerMismatch
                );
                self.registry.store_relationship(MemberRelationship {
                    membership: member.clone(),
                    address: connected_address,
                })?;
                Ok(member)
            }
            _ => Err(ShareError::Protocol.into()),
        }
    }

    /// Restores a durable membership after a lost enrollment response.
    ///
    /// This path intentionally has no ticket and never allocates a membership or
    /// logical replica. The owner authenticates the caller from the QUIC peer ID and
    /// returns only the currently active binding for the requested share.
    pub async fn resume_membership(
        &self,
        owner: EndpointId,
        share: ShareId,
        address: EndpointAddr,
    ) -> Result<Membership> {
        ensure!(owner != self.endpoint_id(), ShareError::OwnerMismatch);
        ensure!(address.id == owner, ShareError::OwnerMismatch);
        let (connection, connected_address) = self.connect_address(owner, address).await?;
        let result = self
            .exchange_bounded(
                &connection,
                Hello {
                    version: 3,
                    share_id: share,
                    operation: Operation::Resume,
                },
            )
            .await;
        connection.close(0u8.into(), b"membership resume complete");
        match result? {
            Reply::Resumed(member) => {
                ensure!(
                    member.owner == owner
                        && member.share_id == share
                        && member.endpoint == self.endpoint_id(),
                    ShareError::OwnerMismatch
                );
                self.registry.store_relationship(MemberRelationship {
                    membership: member.clone(),
                    address: connected_address,
                })?;
                ensure!(member.revoked_at.is_none(), ShareError::MemberRevoked);
                Ok(member)
            }
            _ => Err(ShareError::Protocol.into()),
        }
    }

    async fn connect_ticket(&self, ticket: &ShareTicket) -> Result<(Connection, EndpointAddr)> {
        ensure!(
            ticket.preview().owner != self.endpoint_id(),
            ShareError::OwnerMismatch
        );
        self.connect_address(ticket.preview().owner, ticket.address())
            .await
    }

    async fn connect_address(
        &self,
        owner: EndpointId,
        address: EndpointAddr,
    ) -> Result<(Connection, EndpointAddr)> {
        let transport = SyncSession {
            client: SyncClient {
                secret_key: self.key.clone(),
                remote: address.clone(),
                network_mode: self.mode,
            },
            endpoint: self.router.endpoint().clone(),
            share: None,
            remote: Arc::new(std::sync::RwLock::new(address)),
            fallback_endpoint: (self.mode == NetworkMode::Internet).then_some(owner),
            observation: Arc::new(std::sync::RwLock::new(None)),
            n0_lookup: Arc::new(std::sync::RwLock::new(None)),
        };
        let connection = transport.connect_control().await?;
        let connected_address = transport.remote_address();
        Ok((connection, connected_address))
    }

    async fn exchange_bounded(&self, connection: &Connection, hello: Hello) -> Result<Reply> {
        tokio::time::timeout(CONTROL_DEADLINE, wire::exchange(connection, hello))
            .await
            .map_err(|_| anyhow::Error::new(ShareError::Offline))?
    }
    /// Opens an issuer-pinned session from persisted enrollment. No caller-supplied
    /// role is accepted, and the existing endpoint is cloned, never rebound.
    pub fn open_session(&self, owner: EndpointId, share: ShareId) -> Result<ShareSession> {
        let relationship = self.registry.relationship(owner, share)?;
        ensure!(
            relationship.membership.owner == owner
                && relationship.address.id == owner
                && owner != self.endpoint_id(),
            ShareError::OwnerMismatch
        );
        Ok(ShareSession::from_relationship(
            self.registry.clone(),
            self.key.clone(),
            self.router.endpoint().clone(),
            self.mode,
            relationship,
            ShareSessionResources {
                active: self.active.clone(),
                tasks: self.swarm_tasks.clone(),
                share_events: self.share_events.clone(),
            },
        ))
    }

    /// Replays all endpoint-local grant intents that belong to this device.
    /// The registry bounds enumeration; every row is handled from its stored
    /// signed binding and operation ID, and a nonterminal row is never
    /// replaced with a newly issued grant. This is intended for manager
    /// restart/pending recovery before a fresh data snapshot is requested.
    /// One bounded control budget covers the whole enumeration. Rows that do
    /// not get a turn because an owner is slow or offline remain `Unknown`
    /// and are retried by a later managed tick instead of serially extending
    /// manager startup by one timeout per share.
    pub async fn recover_client_intents(
        &self,
        local_io_drained: bool,
    ) -> Result<Vec<ClientIntentRow>> {
        self.recover_client_intents_with_budget(local_io_drained, CONTROL_DEADLINE)
            .await
    }

    /// Bounded variant used by a managed tick. The budget is shared by all
    /// rows in this invocation, so an offline owner cannot monopolize the
    /// worker loop with one full timeout per intent. This method has the same
    /// recovery-only semantics as `recover_client_intents`: it never reopens
    /// payload admission. The retained boolean parameter is an older source
    /// compatibility surface; drain completion is now accepted only from
    /// `recover_client_intents_with_budget_and_leases` evidence.
    pub async fn recover_client_intents_with_budget(
        &self,
        _local_io_drained: bool,
        budget: Duration,
    ) -> Result<Vec<ClientIntentRow>> {
        let leases = BTreeMap::new();
        self.recover_client_intents_with_budget_and_leases(budget, &leases)
            .await
    }

    /// Bounded restart recovery with explicit managed admission evidence. A
    /// row may be terminalized only when its previous process generation is
    /// known, the endpoint operation registry is quiescent, and the supplied
    /// lease is the exact Managed(owner, share) lease with a reserved private
    /// root. The legacy boolean argument above is retained for source
    /// compatibility but is deliberately ignored so a caller cannot claim a
    /// drain merely by passing `true`.
    pub async fn recover_client_intents_with_budget_and_leases(
        &self,
        budget: Duration,
        leases: &BTreeMap<ShareId, ManagedAdmissionLease>,
    ) -> Result<Vec<ClientIntentRow>> {
        let handler = SwarmAdmissionHandler {
            key: self.key.clone(),
            registry: self.registry.clone(),
            runtimes: self.runtimes.clone(),
            suppliers: self.suppliers.clone(),
            endpoint: self.router.endpoint().clone(),
            mode: self.mode,
            active: self.active.clone(),
            tasks: self.swarm_tasks.clone(),
            streams: Arc::new(tokio::sync::Semaphore::new(1)),
            inflight: Arc::new(Mutex::new(BTreeSet::new())),
            share_events: self.share_events.clone(),
        };
        let rows = self.registry.client_intents()?;
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        // A shared budget is intentionally paired with a rotating start. If
        // the first sorted row belongs to an offline owner and consumes the
        // tick budget, the next tick must get a chance to process the next
        // row rather than retrying the same owner forever.
        let start = self.intent_recovery_cursor.fetch_add(1, Ordering::AcqRel) % rows.len();
        let recovery_deadline = Instant::now() + budget;
        let mut recovered = Vec::new();
        for offset in 0..rows.len() {
            let row = rows[(start + offset) % rows.len()].clone();
            let local_io_drained = self.local_io_drain_is_proven(&row, leases);
            if row.grant.provider == self.endpoint_id() && row.side == ClientSide::Provider {
                let remaining = recovery_deadline.saturating_duration_since(Instant::now());
                let result = if remaining.is_zero() {
                    None
                } else {
                    Some(
                        tokio::time::timeout(
                            remaining,
                            handler.recover_provider_intent(
                                &row.grant,
                                row.operation_id,
                                local_io_drained,
                            ),
                        )
                        .await,
                    )
                };
                match result {
                    Some(Ok(Ok(row))) => {
                        self.release_operation_if_terminal(&row)?;
                        recovered.push(row)
                    }
                    Some(Ok(Err(_))) | Some(Err(_)) | None => {
                        recovered.push(self.preserve_client_intent_after_error(&row)?)
                    }
                }
            } else if row.grant.consumer == self.endpoint_id()
                && row.side == ClientSide::Consumer
                && row.grant.owner != self.endpoint_id()
            {
                // The durable grant keeps the trusted owner identity even if
                // the enrollment response was lost and a later local cleanup
                // removed the relationship row.  Re-establish the active
                // membership through the owner's authenticated Resume path;
                // the endpoint-only address is intentionally empty so
                // Internet mode can resolve the owner by identity.  Offline
                // remains an error with the exact intent row preserved.
                let session = match self.open_session(row.grant.owner, row.grant.share) {
                    Ok(session) => Some(session),
                    Err(error)
                        if error.downcast_ref::<ShareError>() == Some(&ShareError::NotMember) =>
                    {
                        let remaining = recovery_deadline.saturating_duration_since(Instant::now());
                        let resume = if remaining.is_zero() {
                            None
                        } else {
                            Some(
                                tokio::time::timeout(
                                    remaining,
                                    self.resume_membership(
                                        row.grant.owner,
                                        row.grant.share,
                                        EndpointAddr::new(row.grant.owner),
                                    ),
                                )
                                .await,
                            )
                        };
                        match resume {
                            Some(Ok(Ok(_))) => {
                                self.open_session(row.grant.owner, row.grant.share).ok()
                            }
                            Some(Ok(Err(_))) | Some(Err(_)) | None => None,
                        }
                    }
                    Err(_) => None,
                };
                let Some(session) = session else {
                    recovered.push(self.preserve_client_intent_after_error(&row)?);
                    continue;
                };
                let remaining = recovery_deadline.saturating_duration_since(Instant::now());
                let result = if remaining.is_zero() {
                    None
                } else {
                    Some(
                        tokio::time::timeout(
                            remaining,
                            session.recover_swarm_intent_inner(
                                &row.grant,
                                row.operation_id,
                                local_io_drained,
                            ),
                        )
                        .await,
                    )
                };
                match result {
                    Some(Ok(Ok(row))) => {
                        self.release_operation_if_terminal(&row)?;
                        recovered.push(row)
                    }
                    Some(Ok(Err(_))) | Some(Err(_)) | None => {
                        recovered.push(self.preserve_client_intent_after_error(&row)?)
                    }
                }
            }
        }
        Ok(recovered)
    }

    fn release_operation_if_terminal(&self, row: &ClientIntentRow) -> Result<()> {
        if matches!(
            row.phase,
            ClientIntentPhase::Drained | ClientIntentPhase::Cancelled
        ) {
            self.swarm_tasks
                .release_closed_operation((row.grant.nonce, row.operation_id))?;
        }
        Ok(())
    }

    /// Returns a typed local proof for a caller that has already joined and
    /// waited for its own writer task to finish. The exact persisted row and
    /// managed lease are captured in the proof; the proof itself cannot be
    /// used to issue a new grant or extend an activation.
    pub async fn prove_local_io_drained(
        &self,
        grant: &ShareGrant,
        side: ClientSide,
        operation_id: [u8; 16],
        root_lease: Arc<RootLease>,
        root: impl AsRef<Path>,
        state_root: impl AsRef<Path>,
    ) -> Result<LocalIoDrainProof> {
        let row = self
            .registry
            .client_intent_exact(grant, side, operation_id)?
            .ok_or(ShareError::GrantReplay)?;
        ensure!(row.operation_id == operation_id, ShareError::GrantReplay);
        let root = fs::canonicalize(root.as_ref())?;
        let state_root = fs::canonicalize(state_root.as_ref())?;
        ensure!(
            Self::lease_matches_intent(&root_lease, grant, &root, &state_root),
            ShareError::StateUnavailable
        );
        self.swarm_tasks
            .close_operation_and_drain((grant.nonce, operation_id))
            .await?;
        Ok(LocalIoDrainProof {
            share: grant.share,
            owner: grant.owner,
            side,
            operation_id,
            boot_id: row.boot_id,
            root_lease,
            root,
            state_root,
        })
    }

    fn lease_matches_intent(
        lease: &RootLease,
        grant: &ShareGrant,
        root: &Path,
        state_root: &Path,
    ) -> bool {
        matches!(
            lease.kind(),
            RootUse::Managed { share, owner }
                if share == &grant.share.0 && owner == grant.owner.as_bytes()
        ) && lease.root() == root
            && lease
                .private_roots()
                .iter()
                .any(|private| private == state_root)
    }

    /// A restart row is safe to reconcile only after all accepted swarm tasks
    /// have gone away and the exact Managed root/private lease is held. Owner
    /// runtimes and registered member suppliers provide that lease locally;
    /// consumer engines supply it through the bounded recovery map from the
    /// controller. Missing evidence intentionally keeps the row Unknown.
    fn local_io_drain_is_proven(
        &self,
        row: &ClientIntentRow,
        leases: &BTreeMap<ShareId, ManagedAdmissionLease>,
    ) -> bool {
        let close_operation = || {
            if row.previous_boot_id.is_some() {
                self.swarm_tasks
                    .try_close_operation((row.grant.nonce, row.operation_id))
            } else {
                self.swarm_tasks
                    .try_close_completed_operation((row.grant.nonce, row.operation_id))
            }
        };
        if let Some(admission) = leases.get(&row.grant.share)
            && Self::lease_matches_intent(
                admission.lease(),
                &row.grant,
                admission.root(),
                admission.state_root(),
            )
        {
            // A provider-side lease alone does not prove that its registered
            // Store/index generation has stopped using the root.  The
            // supplier counter is the exact local writer boundary; require
            // it even when the controller also supplied a matching lease.
            // A row carried over from a previous process generation has no
            // live supplier registration by definition.  The exact managed
            // lease plus the old operation-generation boundary is the proof
            // that its former local IO cannot still be running.  Requiring a
            // newly registered supplier here would strand paused/revoked
            // recovery after a clean service reopen.  Same-boot rows still
            // require the live supplier counter below because a marker alone
            // cannot prove that a current writer has stopped.
            if row.previous_boot_id.is_none()
                && row.grant.provider == self.endpoint_id()
                && row.grant.owner != self.endpoint_id()
            {
                return self
                    .suppliers
                    .read()
                    .ok()
                    .and_then(|suppliers| {
                        suppliers.get(&(row.grant.owner, row.grant.share)).cloned()
                    })
                    .is_some_and(|supplier| {
                        supplier.inflight.load(Ordering::Acquire) == 0
                            && ShareService::lease_matches_intent(
                                &supplier.root_lease,
                                &row.grant,
                                admission.root(),
                                &supplier.private_root,
                            )
                            && close_operation().unwrap_or(false)
                    });
            }
            return close_operation().unwrap_or(false);
        }
        if row.grant.provider != self.endpoint_id() {
            return false;
        }
        if row.grant.owner == self.endpoint_id()
            && let Some(runtime) = self
                .runtimes
                .read()
                .ok()
                .and_then(|runtimes| runtimes.get(&row.grant.share).cloned())
        {
            return Self::lease_matches_intent(
                &runtime.lease,
                &row.grant,
                &runtime.config.root,
                &runtime.config.state_root,
            ) && close_operation().unwrap_or(false);
        }
        self.suppliers
            .read()
            .ok()
            .and_then(|suppliers| suppliers.get(&(row.grant.owner, row.grant.share)).cloned())
            .is_some_and(|supplier| {
                Self::lease_matches_intent(
                    &supplier.root_lease,
                    &row.grant,
                    supplier.index.root(),
                    &supplier.private_root,
                ) && supplier.inflight.load(Ordering::Acquire) == 0
                    && close_operation().unwrap_or(false)
            })
    }

    /// Preserves one row when its owner or peer is temporarily unavailable.
    /// Recovery errors are row-scoped: the exact signed grant, operation ID,
    /// and binding remain durable as `Unknown`, while state/database failures
    /// from this transition still propagate to the caller.  A later managed
    /// tick can retry the authenticated status path without inventing a new
    /// grant or blocking unrelated shares during service startup.
    fn preserve_client_intent_after_error(&self, row: &ClientIntentRow) -> Result<ClientIntentRow> {
        let current = self
            .registry
            .client_intent(&row.grant)?
            .ok_or(ShareError::StateUnavailable)?;
        if !matches!(
            current.phase,
            ClientIntentPhase::Unknown | ClientIntentPhase::Drained | ClientIntentPhase::Cancelled
        ) {
            self.registry.transition_client_intent(
                &current.grant,
                current.side,
                current.operation_id,
                ClientIntentPhase::Unknown,
                current.activation_id,
            )?;
        }
        self.registry
            .client_intent(&current.grant)?
            .ok_or(ShareError::StateUnavailable.into())
    }

    /// A managed member engine must retain this lease for its entire lifetime.
    pub fn admit_member_root(
        &self,
        owner: EndpointId,
        share: ShareId,
        root: impl AsRef<Path>,
    ) -> Result<RootLease> {
        self.registry.relationship(owner, share)?;
        root_admission::acquire(
            root,
            RootUse::Managed {
                share: share.0,
                owner: *owner.as_bytes(),
            },
        )
    }
    pub async fn shutdown(self) -> Result<()> {
        // Close new swarm admission before asking the router to stop. Tasks
        // already registered below still own their storage and are drained;
        // a late accept races only with the closed registry and is aborted
        // while waiting for registration, before it can start I/O.
        self.swarm_tasks.close_admission();
        for supplier in self.suppliers.read().expect("supplier map").values() {
            supplier.mark_drained();
        }
        let runtimes: Vec<_> = self
            .runtimes
            .read()
            .expect("runtime map")
            .values()
            .cloned()
            .collect();
        for runtime in runtimes {
            runtime.pause().await;
        }
        self.runtimes.write().expect("runtime map").clear();
        let router_result = self.router.shutdown().await;
        let _drained = self.active.write().await;
        let tasks_result = self.swarm_tasks.close_and_drain().await;
        self.suppliers.write().expect("supplier map").clear();
        router_result?;
        tasks_result?;
        Ok(())
    }
}

#[derive(Debug, Default)]
struct RosterCache {
    roster: Option<SignedRoster>,
    last_heartbeat: Option<Instant>,
    local_address: Option<EndpointAddr>,
}

#[derive(Debug)]
struct ShareSessionState {
    registry: Arc<Registry>,
    membership: Membership,
    active: Arc<tokio::sync::RwLock<()>>,
    tasks: Arc<SwarmTaskRegistry>,
    roster_challenge: Mutex<Option<GrantNonce>>,
    roster: Mutex<RosterCache>,
    roster_gate: tokio::sync::Mutex<()>,
    transport: SyncSession,
    share_events: Arc<ShareEventState>,
}

#[derive(Clone)]
struct ShareSessionResources {
    active: Arc<tokio::sync::RwLock<()>>,
    tasks: Arc<SwarmTaskRegistry>,
    share_events: Arc<ShareEventState>,
}

/// A control connection closes even when its awaiting operation is cancelled.
/// This is required by the managed heartbeat supervisor's shutdown barrier.
struct ControlConnection(Connection);
impl std::ops::Deref for ControlConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl Drop for ControlConnection {
    fn drop(&mut self) {
        self.0.close(0u8.into(), b"share control complete");
    }
}

/// One authenticated managed membership over the device's already-open
/// endpoint. Clones share the exact roster cache, transport address hint, and
/// heartbeat gate; they never create a second endpoint.
#[derive(Clone, Debug)]
pub struct ShareSession {
    state: Arc<ShareSessionState>,
}
impl ShareSession {
    fn from_relationship(
        registry: Arc<Registry>,
        key: SecretKey,
        endpoint: crate::Endpoint,
        mode: NetworkMode,
        relationship: MemberRelationship,
        resources: ShareSessionResources,
    ) -> Self {
        let owner = relationship.membership.owner;
        let share = relationship.membership.share_id;
        let remote = relationship.address;
        Self {
            state: Arc::new(ShareSessionState {
                registry,
                membership: relationship.membership,
                active: resources.active,
                tasks: resources.tasks,
                roster_challenge: Mutex::new(None),
                roster: Mutex::new(RosterCache::default()),
                roster_gate: tokio::sync::Mutex::new(()),
                transport: SyncSession {
                    client: SyncClient {
                        secret_key: key,
                        remote: remote.clone(),
                        network_mode: mode,
                    },
                    endpoint,
                    share: Some(share),
                    remote: Arc::new(std::sync::RwLock::new(remote)),
                    fallback_endpoint: (mode == NetworkMode::Internet).then_some(owner),
                    observation: Arc::new(std::sync::RwLock::new(None)),
                    n0_lookup: Arc::new(std::sync::RwLock::new(None)),
                },
                share_events: resources.share_events,
            }),
        }
    }

    pub fn membership(&self) -> &Membership {
        &self.state.membership
    }

    /// Registers an observer for this share session. The observer is shared
    /// with the service's inbound handlers, so events from the consumer and
    /// provider sides retain one endpoint lifetime and one operation ID space.
    pub fn with_share_observer(self, observer: ShareTransferObserver) -> Self {
        self.set_share_observer(Some(observer));
        self
    }

    /// Replaces the observer for this session without opening another
    /// endpoint or changing the authenticated membership binding.
    pub fn set_share_observer(&self, observer: Option<ShareTransferObserver>) {
        self.state.share_events.set_observer(observer);
    }

    fn emit_share_event(&self, event: ShareTransferEvent) {
        self.state.share_events.emit(event);
    }

    fn next_query_event_id(&self, peer: EndpointId, tag: &[u8]) -> [u8; 16] {
        self.state
            .share_events
            .next_query_id(self.state.membership.share_id, peer, tag)
    }

    /// Proves that this session's exact grant operation has no remaining
    /// endpoint-local IO.  The admission key is closed while the operation
    /// registry is checked, and the caller's managed root/private lease is
    /// compared against the persisted binding before the proof is returned.
    ///
    /// This is a recovery capability only.  It does not open a new stream,
    /// issue a grant, or extend a remote activation lease.  A caller that
    /// drops a public recovery future cannot bypass this barrier because the
    /// accepted operation is owned by the service task registry.
    pub async fn prove_local_io_drained(
        &self,
        grant: &ShareGrant,
        operation_id: [u8; 16],
        root_lease: Arc<RootLease>,
        root: impl AsRef<Path>,
        state_root: impl AsRef<Path>,
    ) -> Result<LocalIoDrainProof> {
        Self::ensure_recovery_binding_for(&self.state.membership, grant)?;
        ensure!(
            grant.consumer == self.state.membership.endpoint,
            ShareError::EndpointMismatch
        );
        let row = self
            .state
            .registry
            .client_intent_exact(grant, ClientSide::Consumer, operation_id)?
            .ok_or(ShareError::GrantReplay)?;
        ensure!(row.operation_id == operation_id, ShareError::GrantReplay);
        let root = fs::canonicalize(root.as_ref())?;
        let state_root = fs::canonicalize(state_root.as_ref())?;
        ensure!(
            ShareService::lease_matches_intent(&root_lease, grant, &root, &state_root),
            ShareError::StateUnavailable
        );
        self.state
            .tasks
            .close_operation_and_drain((grant.nonce, operation_id))
            .await?;
        Ok(LocalIoDrainProof {
            share: grant.share,
            owner: grant.owner,
            side: ClientSide::Consumer,
            operation_id,
            boot_id: row.boot_id,
            root_lease,
            root,
            state_root,
        })
    }

    /// Starts the managed liveness supervisor. The task holds only a weak
    /// reference so dropping an engine without a graceful shutdown cannot keep
    /// a session or endpoint alive. Its 30-second cadence is independent of
    /// the data synchronization gate.
    pub fn start_heartbeat(&self) -> tokio::task::JoinHandle<()> {
        let weak = Arc::downgrade(&self.state);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(
                super::ROSTER_HEARTBEAT_INTERVAL_SECONDS,
            ));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let Some(state) = weak.upgrade() else {
                    break;
                };
                let session = Self { state };
                // Offline is a recoverable liveness result. The next tick
                // retries against the same authenticated owner identity.
                let _ = session.heartbeat_now().await;
            }
        })
    }

    /// Returns the latest secret-free lookup/path observation for the D3
    /// harness. The observation never includes an endpoint address or ID.
    #[must_use]
    pub fn transport_observation(&self) -> Option<TransportObservation> {
        self.state.transport.transport_observation()
    }

    /// Returns the latest secret-free N0 address-lookup result.  A value is
    /// present only after endpoint-ID fallback has observed a matching
    /// pkarr/DNS (or other configured) address item.
    #[must_use]
    pub fn n0_lookup_observation(&self) -> Option<crate::N0LookupObservation> {
        self.state.transport.n0_lookup_observation()
    }

    async fn refresh_roster_inner(&self) -> Result<SignedRoster> {
        let result = self.control_exchange(Operation::Roster).await?;
        let (roster, challenge) = match result {
            Reply::Roster { roster, challenge } => (roster, challenge),
            _ => return Err(ShareError::Protocol.into()),
        };
        roster.verify_for(
            self.state.membership.owner,
            self.state.membership.share_id,
            super::now(),
        )?;
        ensure!(
            roster
                .member(self.state.membership.endpoint)
                .is_some_and(|entry| {
                    entry.permission == self.state.membership.permission
                        && entry.member_epoch == self.state.membership.epoch
                }),
            ShareError::EpochMismatch
        );
        *self
            .state
            .roster_challenge
            .lock()
            .expect("roster challenge mutex") = Some(challenge);
        self.state.roster.lock().expect("roster cache mutex").roster = Some(roster.clone());
        Ok(roster)
    }

    /// Fetches the owner-signed member roster and arms one heartbeat challenge
    /// for the next address update. Roster addresses are transport hints only;
    /// persisted membership remains the authorization authority.
    pub async fn refresh_roster(&self) -> Result<SignedRoster> {
        let _gate = self.state.roster_gate.lock().await;
        self.refresh_roster_inner().await
    }

    /// Refreshes and heartbeats when the 30-second lease or local address hint
    /// requires it. This is called by both the managed supervisor and the
    /// first sync round, while the gate serializes overlapping calls.
    pub async fn ensure_roster_heartbeat(&self) -> Result<SignedRoster> {
        let _gate = self.state.roster_gate.lock().await;
        let local_address =
            crate::endpoint_addr_with_local_fallback(&self.state.transport.endpoint);
        let (due, cached_roster) = {
            let cached = self.state.roster.lock().expect("roster cache mutex");
            let due = cached.roster.is_none()
                || cached.last_heartbeat.is_none_or(|last| {
                    last.elapsed() >= Duration::from_secs(super::ROSTER_HEARTBEAT_INTERVAL_SECONDS)
                })
                || cached.local_address.as_ref() != Some(&local_address);
            (due, cached.roster.clone())
        };
        if !due {
            return cached_roster.ok_or_else(|| anyhow::Error::new(ShareError::Protocol));
        }
        self.refresh_roster_inner().await?;
        let challenge = self
            .roster_challenge()
            .ok_or_else(|| anyhow::Error::new(ShareError::Protocol))?;
        self.heartbeat_inner(challenge).await
    }

    async fn heartbeat_now(&self) -> Result<SignedRoster> {
        let _gate = self.state.roster_gate.lock().await;
        self.refresh_roster_inner().await?;
        let challenge = self
            .roster_challenge()
            .ok_or_else(|| anyhow::Error::new(ShareError::Protocol))?;
        self.heartbeat_inner(challenge).await
    }

    /// Returns the most recently issued challenge for callers that need to
    /// schedule an explicit heartbeat.
    #[must_use]
    pub fn roster_challenge(&self) -> Option<GrantNonce> {
        *self
            .state
            .roster_challenge
            .lock()
            .expect("roster challenge mutex")
    }

    /// Signs the current endpoint address and submits it to the owner. The
    /// owner authenticates both the QUIC peer identity and this signature
    /// before updating the durable roster address.
    async fn heartbeat_inner(&self, challenge: GrantNonce) -> Result<SignedRoster> {
        let address = crate::endpoint_addr_with_local_fallback(&self.state.transport.endpoint);
        let heartbeat = RosterHeartbeat::sign(
            &self.state.transport.client.secret_key,
            self.state.membership.owner,
            self.state.membership.share_id,
            address.clone(),
            challenge,
            super::now(),
        );
        let result = self
            .control_exchange(Operation::Heartbeat(heartbeat))
            .await?;
        let roster = match result {
            Reply::Heartbeat(roster) => roster,
            _ => return Err(ShareError::Protocol.into()),
        };
        roster.verify_for(
            self.state.membership.owner,
            self.state.membership.share_id,
            super::now(),
        )?;
        ensure!(
            roster
                .member(self.state.membership.endpoint)
                .is_some_and(|entry| {
                    entry.permission == self.state.membership.permission
                        && entry.member_epoch == self.state.membership.epoch
                }),
            ShareError::EpochMismatch
        );
        let mut stored = self
            .state
            .roster_challenge
            .lock()
            .expect("roster challenge mutex");
        if stored.as_ref() == Some(&challenge) {
            *stored = None;
        }
        drop(stored);
        let mut cache = self.state.roster.lock().expect("roster cache mutex");
        cache.roster = Some(roster.clone());
        cache.last_heartbeat = Some(Instant::now());
        cache.local_address = Some(address);
        Ok(roster)
    }

    /// Signs and submits one explicit address heartbeat. Managed callers
    /// normally use `ensure_roster_heartbeat`; this method remains available
    /// for authenticated address-change tests and controlled drivers.
    pub async fn heartbeat(&self, challenge: GrantNonce) -> Result<()> {
        let _gate = self.state.roster_gate.lock().await;
        self.heartbeat_inner(challenge).await.map(|_| ())
    }

    async fn control_exchange(&self, operation: Operation) -> Result<Reply> {
        self.control_exchange_until(operation, Instant::now() + CONTROL_DEADLINE)
            .await
    }

    async fn control_exchange_until(
        &self,
        operation: Operation,
        deadline: Instant,
    ) -> Result<Reply> {
        let connection = self.connect_control_until(deadline).await?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(ShareError::Offline.into());
        }
        let result = tokio::time::timeout(
            remaining,
            wire::exchange(
                &connection,
                Hello {
                    version: 3,
                    share_id: self.state.membership.share_id,
                    operation,
                },
            ),
        )
        .await;
        self.state
            .transport
            .refresh_transport_observation(&connection);
        result.map_err(|_| anyhow::Error::new(ShareError::Offline))?
    }

    /// Performs one control exchange while retaining the distinction between
    /// a transport failure and a received authenticated `Reply::Error`.
    /// Query/manifest/grant telemetry starts only after a complete reply frame
    /// has arrived; the returned guard owns the terminal event on validation
    /// failure or caller cancellation.
    async fn control_exchange_observed_until(
        &self,
        operation: Operation,
        deadline: Instant,
        phase: SharePhase,
        peer: EndpointId,
        operation_id: [u8; 16],
    ) -> Result<(Reply, ShareEventGuard)> {
        let connection = self.connect_control_until(deadline).await?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        ensure!(!remaining.is_zero(), ShareError::Offline);
        let result = tokio::time::timeout(
            remaining,
            wire::exchange_raw(
                &connection,
                Hello {
                    version: 3,
                    share_id: self.state.membership.share_id,
                    operation,
                },
            ),
        )
        .await;
        self.state
            .transport
            .refresh_transport_observation(&connection);
        let reply = result.map_err(|_| anyhow::Error::new(ShareError::Offline))??;
        let guard = ShareEventGuard::admitted(
            &self.state.share_events,
            ShareTransferEvent {
                operation_id,
                share: self.state.membership.share_id,
                peer,
                phase,
                direction: TransferDirection::Outbound,
                bytes: 0,
                epoch: self.state.membership.epoch,
                provider_epoch: None,
                grant: None,
            },
        );
        Ok((reply, guard))
    }

    async fn connect_control_until(&self, deadline: Instant) -> Result<ControlConnection> {
        Ok(ControlConnection(
            self.state.transport.connect_control_until(deadline).await?,
        ))
    }

    /// Requests the complete owner-signed snapshot used by managed apply.
    /// `local` is retained for the caller's reconciliation API symmetry; the
    /// owner response is always independently complete and Merkle-verified.
    pub async fn fetch_authoritative_snapshot(
        &self,
        _local: &MerkleTree,
    ) -> Result<AuthoritativeSnapshot> {
        let peer = self.state.membership.owner;
        let operation_id = self.next_query_event_id(peer, b"snapshot");
        let (reply, event) = self
            .control_exchange_observed_until(
                Operation::Snapshot,
                Instant::now() + CONTROL_DEADLINE,
                SharePhase::Query,
                peer,
                operation_id,
            )
            .await?;
        let result: Result<AuthoritativeSnapshot> = (|| {
            let snapshot = match reply {
                Reply::Snapshot(snapshot) => snapshot,
                Reply::Error(error) => return Err(error.into()),
                _ => return Err(ShareError::Protocol.into()),
            };
            snapshot.verify_complete(
                self.state.membership.owner,
                self.state.membership.share_id,
                super::now(),
            )?;
            ensure!(
                snapshot.token.epoch == self.state.membership.epoch,
                ShareError::EpochMismatch
            );
            Ok(snapshot)
        })();
        match result {
            Ok(snapshot) => {
                event.finish(SharePhase::Done, None, None);
                Ok(snapshot)
            }
            Err(error) => {
                event.finish(SharePhase::Reject, None, None);
                Err(error)
            }
        }
    }

    /// Requests an owner attestation for one exact snapshot record.
    pub async fn request_manifest(
        &self,
        snapshot: &SnapshotToken,
        record: &SyncRecord,
    ) -> Result<ManifestAttestation> {
        let peer = self.state.membership.owner;
        let operation_id = self.next_query_event_id(peer, b"manifest");
        ensure!(
            snapshot.epoch == self.state.membership.epoch,
            ShareError::EpochMismatch
        );
        let (reply, event) = self
            .control_exchange_observed_until(
                Operation::Manifest {
                    snapshot: snapshot.clone(),
                    record: record.clone(),
                },
                Instant::now() + CONTROL_DEADLINE,
                SharePhase::Manifest,
                peer,
                operation_id,
            )
            .await?;
        let result: Result<ManifestAttestation> = (|| {
            let attestation = match reply {
                Reply::Manifest(attestation) => attestation,
                Reply::Error(error) => return Err(error.into()),
                _ => return Err(ShareError::Protocol.into()),
            };
            attestation.verify_for(
                self.state.membership.owner,
                self.state.membership.share_id,
                super::now(),
            )?;
            attestation.verify_record(snapshot, record)?;
            Ok(attestation)
        })();
        match result {
            Ok(attestation) => {
                event.finish(SharePhase::Done, None, None);
                Ok(attestation)
            }
            Err(error) => {
                event.finish(SharePhase::Reject, None, None);
                Err(error)
            }
        }
    }

    /// Requests one owner-signed grant for a sorted, duplicate-free subset of
    /// manifest chunks.  A grant is bound to this authenticated consumer and
    /// cannot be reused for a different provider or subset.
    pub async fn request_swarm_grant(
        &self,
        provider: EndpointId,
        snapshot: &SnapshotToken,
        manifest: &ManifestAttestation,
        hashes: &[Hash32],
    ) -> Result<ShareGrant> {
        let operation_id = self.next_query_event_id(provider, b"grant");
        ensure!(
            snapshot.epoch == self.state.membership.epoch,
            ShareError::EpochMismatch
        );
        ensure!(
            snapshot.share == self.state.membership.share_id,
            ShareError::OwnerMismatch
        );
        ensure!(
            manifest.share == snapshot.share,
            ShareError::ManifestMismatch
        );
        ensure!(manifest.epoch == snapshot.epoch, ShareError::EpochMismatch);
        super::authority::validate_hash_subset(hashes)?;
        let (reply, event) = self
            .control_exchange_observed_until(
                Operation::SwarmGrant {
                    provider,
                    snapshot: snapshot.clone(),
                    manifest: manifest.clone(),
                    hashes: hashes.to_vec(),
                },
                Instant::now() + CONTROL_DEADLINE,
                SharePhase::Grant,
                provider,
                operation_id,
            )
            .await?;
        let result: Result<ShareGrant> = (|| {
            let grant = match reply {
                Reply::Grant(grant) => grant,
                Reply::Error(error) => return Err(error.into()),
                _ => return Err(ShareError::Protocol.into()),
            };
            grant.verify_for(
                self.state.membership.owner,
                self.state.membership.share_id,
                super::now(),
            )?;
            ensure!(
                grant.consumer == self.state.membership.endpoint,
                ShareError::EndpointMismatch
            );
            ensure!(grant.provider == provider, ShareError::EndpointMismatch);
            ensure!(
                grant.epoch == self.state.membership.epoch,
                ShareError::EpochMismatch
            );
            ensure!(
                grant.request_hash
                    == request_hash(
                        self.state.membership.share_id,
                        snapshot.snapshot,
                        manifest.manifest_hash,
                        hashes,
                    )?,
                ShareError::ManifestMismatch
            );
            Ok(grant)
        })();
        match result {
            Ok(grant) => {
                event.finish(
                    SharePhase::Done,
                    Some(grant.provider_epoch),
                    Some(grant.nonce),
                );
                Ok(grant)
            }
            Err(error) => {
                event.finish(SharePhase::Reject, None, None);
                Err(error)
            }
        }
    }

    /// Activates one member-provider grant at the owner.  The provider's
    /// monotonic start is captured before the control exchange; the owner
    /// applies its own receive-side deadline and never extends this lease from
    /// a wall-clock expiry.
    pub async fn activate_grant(&self, grant: &ShareGrant) -> Result<ActivationLease> {
        let started = Instant::now();
        self.activate_grant_until(grant, started, started + CONTROL_DEADLINE)
            .await
    }

    async fn activate_grant_until(
        &self,
        grant: &ShareGrant,
        request_started: Instant,
        deadline: Instant,
    ) -> Result<ActivationLease> {
        Self::ensure_activation_binding_for(&self.state.membership, grant)?;
        ensure!(
            grant.provider == self.state.membership.endpoint,
            ShareError::EndpointMismatch
        );
        let result = self
            .control_exchange_until(Operation::Activate(grant.activate_request()), deadline)
            .await;
        if Instant::now() >= deadline {
            return Err(ShareError::GrantExpired.into());
        }
        let result = result?;
        let reply = match result {
            Reply::Activate(reply) => reply,
            _ => return Err(ShareError::Protocol.into()),
        };
        reply.verify_for(self.state.membership.owner, self.state.membership.share_id)?;
        ensure!(
            reply.provider == self.state.membership.endpoint,
            ShareError::EndpointMismatch
        );
        ensure!(reply.nonce == grant.nonce, ShareError::GrantReplay);
        ActivationLease::from_reply_at(reply, request_started, Instant::now())
    }

    fn ensure_recovery_binding_for(membership: &Membership, grant: &ShareGrant) -> Result<()> {
        ensure!(
            grant.owner == membership.owner && grant.share == membership.share_id,
            ShareError::OwnerMismatch
        );
        let endpoint = membership.endpoint;
        ensure!(
            grant.consumer == endpoint || grant.provider == endpoint,
            ShareError::EndpointMismatch
        );
        Ok(())
    }

    fn ensure_activation_binding_for(membership: &Membership, grant: &ShareGrant) -> Result<()> {
        Self::ensure_recovery_binding_for(membership, grant)?;
        let endpoint = membership.endpoint;
        if grant.consumer == endpoint {
            ensure!(grant.epoch == membership.epoch, ShareError::EpochMismatch);
        } else {
            ensure!(
                grant.provider_epoch == membership.epoch,
                ShareError::EpochMismatch
            );
        }
        Ok(())
    }

    /// Queries the owner for the exact durable activation state.  A receipt
    /// never refreshes the local monotonic lease or admits payload work by
    /// itself.
    pub async fn activation_status(
        &self,
        grant: &ShareGrant,
        activation_id: Option<[u8; 16]>,
    ) -> Result<ActivationReceipt> {
        self.activation_status_until(grant, activation_id, Instant::now() + CONTROL_DEADLINE)
            .await
    }

    async fn activation_status_until(
        &self,
        grant: &ShareGrant,
        activation_id: Option<[u8; 16]>,
        deadline: Instant,
    ) -> Result<ActivationReceipt> {
        // Status recovery intentionally accepts the exact old binding after a
        // membership epoch advances.  This is a read/cancel path, not a new
        // data admission; current-epoch checks remain in activation and data
        // permit paths.
        Self::ensure_recovery_binding_for(&self.state.membership, grant)?;
        let result = self
            .control_exchange_until(
                Operation::ActivationStatus(ActivationStatusQuery::for_grant(grant, activation_id)),
                deadline,
            )
            .await?;
        let receipt = match result {
            Reply::ActivationReceipt(receipt) => receipt,
            _ => return Err(ShareError::Protocol.into()),
        };
        receipt.verify_for(grant, activation_id)?;
        Ok(receipt)
    }

    fn ensure_data_admission(
        receipt: &ActivationReceipt,
        grant: &ShareGrant,
        activation_id: [u8; 16],
    ) -> Result<()> {
        receipt.verify_for(grant, Some(activation_id))?;
        ensure!(
            matches!(receipt.state, super::ActivationStateView::Active)
                && receipt.admission_open
                && !receipt.revoked
                && !receipt.consumer_drained,
            ShareError::GrantReplay
        );
        Ok(())
    }

    async fn ensure_data_admission_until(
        &self,
        grant: &ShareGrant,
        activation_id: [u8; 16],
        deadline: Instant,
    ) -> Result<ActivationReceipt> {
        let receipt = self
            .activation_status_until(grant, Some(activation_id), deadline)
            .await?;
        Self::ensure_data_admission(&receipt, grant, activation_id)?;
        Ok(receipt)
    }

    /// Requests an idempotent owner-side cancellation for an exact activation
    /// attempt.  An already Active/Restarted grant returns its drain blocker;
    /// it is never converted to Denied by a late cancellation.
    pub async fn cancel_activation(
        &self,
        grant: &ShareGrant,
        activation_id: Option<[u8; 16]>,
        operation_id: [u8; 16],
    ) -> Result<ActivationReceipt> {
        // Cancellation has the same recovery exception as status: an old
        // activation must remain queryable after revoke/epoch advancement.
        Self::ensure_recovery_binding_for(&self.state.membership, grant)?;
        let result = self
            .control_exchange(Operation::ActivationCancel(ActivationCancel::for_grant(
                grant,
                activation_id,
                operation_id,
            )))
            .await?;
        let receipt = match result {
            Reply::ActivationReceipt(receipt) => receipt,
            _ => return Err(ShareError::Protocol.into()),
        };
        receipt.verify_for(grant, activation_id)?;
        Ok(receipt)
    }

    /// Replays one endpoint-local grant intent after a response loss or
    /// process restart. The stored signed grant and operation ID are the only
    /// inputs that identify the operation; this method never accepts a new
    /// grant, nonce, membership, or replica as a recovery substitute.
    ///
    /// `local_io_drained` is supplied by the managed engine's actual writer
    /// drain barrier. Until it is true, an Active/Restarted/Unknown intent is
    /// kept recoverable and no terminal state is published.
    pub async fn recover_swarm_intent(
        &self,
        grant: &ShareGrant,
        operation_id: [u8; 16],
        _local_io_drained: bool,
    ) -> Result<ClientIntentRow> {
        // Keep the historical signature source-compatible, but do not accept
        // an untyped boolean as proof that a writer drained. Callers needing
        // terminal recovery must use `recover_swarm_intent_with_proof`.
        self.recover_swarm_intent_inner(grant, operation_id, false)
            .await
    }

    async fn recover_swarm_intent_inner(
        &self,
        grant: &ShareGrant,
        operation_id: [u8; 16],
        local_io_drained: bool,
    ) -> Result<ClientIntentRow> {
        Self::ensure_recovery_binding_for(&self.state.membership, grant)?;
        ensure!(
            grant.consumer == self.state.membership.endpoint,
            ShareError::EndpointMismatch
        );
        let mut row = self
            .state
            .registry
            .client_intent_exact(grant, ClientSide::Consumer, operation_id)?
            .ok_or(ShareError::GrantReplay)?;
        if matches!(
            row.phase,
            ClientIntentPhase::Drained | ClientIntentPhase::Cancelled
        ) {
            return Ok(row);
        }

        let deadline = Instant::now() + CONTROL_DEADLINE;
        let mut receipt = match self
            .activation_status_until(grant, row.activation_id, deadline)
            .await
        {
            Ok(receipt) => receipt,
            Err(error) => {
                // A lost owner does not prove NotMember, revoke, or terminal
                // completion. Preserve the exact row for a later retry.
                if !matches!(row.phase, ClientIntentPhase::Unknown) {
                    self.state.registry.transition_client_intent(
                        grant,
                        ClientSide::Consumer,
                        operation_id,
                        ClientIntentPhase::Unknown,
                        row.activation_id,
                    )?;
                }
                return Err(error);
            }
        };

        // A response-loss window may have assigned an activation ID at the
        // owner even though this endpoint never saw Ready. Retain that exact
        // ID; never mint or accept a replacement.
        if row.activation_id.is_none() && receipt.activation_id.is_some() {
            row = self.state.registry.transition_client_intent(
                grant,
                ClientSide::Consumer,
                operation_id,
                row.phase,
                receipt.activation_id,
            )?;
        }

        if matches!(
            receipt.state,
            ActivationStateView::Active | ActivationStateView::Restarted
        ) {
            // Recovery may inspect an old active lease, but it can never
            // reopen payload admission. Quarantine a pre-restart phase before
            // moving it to the local drain barrier.
            if !matches!(
                row.phase,
                ClientIntentPhase::Unknown | ClientIntentPhase::Draining
            ) {
                row = self.state.registry.transition_client_intent(
                    grant,
                    ClientSide::Consumer,
                    operation_id,
                    ClientIntentPhase::Unknown,
                    receipt.activation_id,
                )?;
            }
            if !local_io_drained {
                return Ok(row);
            }
            let activation_id = receipt.activation_id.ok_or(ShareError::GrantReplay)?;
            if !matches!(row.phase, ClientIntentPhase::Draining) {
                row = self.state.registry.transition_client_intent(
                    grant,
                    ClientSide::Consumer,
                    operation_id,
                    ClientIntentPhase::Draining,
                    Some(activation_id),
                )?;
            }
            if !receipt.consumer_drained {
                self.grant_drained(grant, activation_id).await?;
            }
            receipt = self
                .activation_status_until(grant, Some(activation_id), deadline)
                .await?;
        }

        match receipt.state {
            ActivationStateView::Drained => {
                if !local_io_drained {
                    if !matches!(row.phase, ClientIntentPhase::Unknown) {
                        row = self.state.registry.transition_client_intent(
                            grant,
                            ClientSide::Consumer,
                            operation_id,
                            ClientIntentPhase::Unknown,
                            receipt.activation_id,
                        )?;
                    }
                    return Ok(row);
                }
                if matches!(
                    row.phase,
                    ClientIntentPhase::Prepared | ClientIntentPhase::AwaitingActivation
                ) {
                    row = self.state.registry.transition_client_intent(
                        grant,
                        ClientSide::Consumer,
                        operation_id,
                        ClientIntentPhase::Unknown,
                        receipt.activation_id,
                    )?;
                }
                if matches!(row.phase, ClientIntentPhase::Active) {
                    row = self.state.registry.transition_client_intent(
                        grant,
                        ClientSide::Consumer,
                        operation_id,
                        ClientIntentPhase::Draining,
                        receipt.activation_id,
                    )?;
                }
                if !matches!(row.phase, ClientIntentPhase::Drained) {
                    row = self.state.registry.transition_client_intent(
                        grant,
                        ClientSide::Consumer,
                        operation_id,
                        ClientIntentPhase::Drained,
                        receipt.activation_id,
                    )?;
                }
            }
            ActivationStateView::Denied | ActivationStateView::Expired => {
                if !local_io_drained {
                    if !matches!(row.phase, ClientIntentPhase::Unknown) {
                        row = self.state.registry.transition_client_intent(
                            grant,
                            ClientSide::Consumer,
                            operation_id,
                            ClientIntentPhase::Unknown,
                            receipt.activation_id,
                        )?;
                    }
                    return Ok(row);
                }
                if matches!(
                    row.phase,
                    ClientIntentPhase::Active | ClientIntentPhase::Draining
                ) {
                    row = self.state.registry.transition_client_intent(
                        grant,
                        ClientSide::Consumer,
                        operation_id,
                        ClientIntentPhase::Unknown,
                        receipt.activation_id,
                    )?;
                }
                if !matches!(row.phase, ClientIntentPhase::Cancelled) {
                    row = self.state.registry.transition_client_intent(
                        grant,
                        ClientSide::Consumer,
                        operation_id,
                        ClientIntentPhase::Cancelled,
                        receipt.activation_id,
                    )?;
                }
            }
            ActivationStateView::Issued => {
                // Issued means no owner activation won yet. Cancellation is
                // the only terminal transition and is an atomic owner-side
                // race against a late Activate.
                let cancelled = self
                    .cancel_activation(grant, receipt.activation_id, operation_id)
                    .await?;
                if matches!(
                    cancelled.state,
                    ActivationStateView::Denied | ActivationStateView::Expired
                ) && local_io_drained
                {
                    if !matches!(row.phase, ClientIntentPhase::Cancelled) {
                        row = self.state.registry.transition_client_intent(
                            grant,
                            ClientSide::Consumer,
                            operation_id,
                            ClientIntentPhase::Cancelled,
                            cancelled.activation_id,
                        )?;
                    }
                } else if !matches!(row.phase, ClientIntentPhase::Unknown) {
                    row = self.state.registry.transition_client_intent(
                        grant,
                        ClientSide::Consumer,
                        operation_id,
                        ClientIntentPhase::Unknown,
                        cancelled.activation_id,
                    )?;
                }
            }
            // The Active/Restarted cases are handled above. Keeping an
            // explicit arm makes the state machine fail closed if that
            // handling ever returns without a terminal query.
            ActivationStateView::Active | ActivationStateView::Restarted => {}
        }
        Ok(row)
    }

    /// Recovery-only variant that requires a typed local drain proof. The
    /// legacy boolean recovery entry point remains available for callers that
    /// only need to quarantine a row, but it cannot be used to claim that an
    /// old local writer has finished.
    pub async fn recover_swarm_intent_with_proof(
        &self,
        grant: &ShareGrant,
        operation_id: [u8; 16],
        proof: &LocalIoDrainProof,
    ) -> Result<ClientIntentRow> {
        ensure!(proof.share == grant.share, ShareError::GrantReplay);
        ensure!(proof.owner == grant.owner, ShareError::GrantReplay);
        ensure!(proof.side == ClientSide::Consumer, ShareError::GrantReplay);
        ensure!(proof.operation_id == operation_id, ShareError::GrantReplay);
        ensure!(
            ShareService::lease_matches_intent(
                &proof.root_lease,
                grant,
                &proof.root,
                &proof.state_root,
            ),
            ShareError::StateUnavailable
        );
        self.state
            .tasks
            .close_operation_and_drain((grant.nonce, operation_id))
            .await?;
        let row = self
            .state
            .registry
            .client_intent_exact(grant, ClientSide::Consumer, operation_id)?
            .ok_or(ShareError::GrantReplay)?;
        ensure!(row.boot_id == proof.boot_id, ShareError::GrantReplay);
        let recovered = self
            .recover_swarm_intent_inner(grant, operation_id, true)
            .await?;
        if matches!(
            recovered.phase,
            ClientIntentPhase::Drained | ClientIntentPhase::Cancelled
        ) {
            self.state
                .tasks
                .release_closed_operation((grant.nonce, operation_id))?;
        }
        Ok(recovered)
    }

    /// Replays every bounded consumer intent belonging to this authenticated
    /// membership. Rows for other shares/endpoints are ignored; no caller
    /// supplied role or membership is inferred from the journal. Its legacy
    /// boolean parameter is retained but cannot claim local writer drain;
    /// use `recover_swarm_intent_with_proof` for that recovery transition.
    pub async fn recover_swarm_intents(
        &self,
        _local_io_drained: bool,
    ) -> Result<Vec<ClientIntentRow>> {
        let rows = self.state.registry.client_intents()?;
        let mut recovered = Vec::new();
        for row in rows {
            if row.side != ClientSide::Consumer
                || row.grant.owner != self.state.membership.owner
                || row.grant.share != self.state.membership.share_id
                || row.grant.consumer != self.state.membership.endpoint
            {
                continue;
            }
            recovered.push(
                self.recover_swarm_intent_inner(&row.grant, row.operation_id, false)
                    .await?,
            );
        }
        Ok(recovered)
    }

    /// Sends one authenticated endpoint drain acknowledgement for an active
    /// grant.  The owner retains the grant as Active until its other endpoint
    /// sends the same activation-bound acknowledgement.
    pub async fn grant_drained(&self, grant: &ShareGrant, activation_id: [u8; 16]) -> Result<()> {
        ensure!(
            grant.share == self.state.membership.share_id,
            ShareError::OwnerMismatch
        );
        ensure!(
            grant.consumer == self.state.membership.endpoint
                || grant.provider == self.state.membership.endpoint,
            ShareError::EndpointMismatch
        );
        let result = self
            .control_exchange(Operation::GrantDrained {
                nonce: grant.nonce,
                activation_id,
            })
            .await?;
        ensure!(matches!(result, Reply::GrantDrained), ShareError::Protocol);
        Ok(())
    }

    /// Completes the endpoint-local journal only after the owner has returned
    /// the exact terminal grant state.  A successful local drain acknowledgement
    /// by itself is deliberately insufficient: the peer may still have a
    /// writer, reader, or buffered stream in flight.  Transport loss leaves
    /// the row in `Draining` for bounded restart recovery.
    async fn finalize_consumer_intent_after_drain(
        &self,
        grant: &ShareGrant,
        operation_id: [u8; 16],
        activation_id: [u8; 16],
        deadline: Instant,
    ) -> Result<bool> {
        let receipt = match self
            .activation_status_until(grant, Some(activation_id), deadline)
            .await
        {
            Ok(receipt) => receipt,
            Err(error) => {
                // A response lost after the local ACK is a recoverable
                // lifecycle state. The verified bytes are useful, but the
                // caller receives an explicit pending marker and recovery
                // retains the Draining row. Non-transport/state failures are
                // surfaced instead of being presented as success.
                if matches!(
                    error.downcast_ref::<ShareError>(),
                    Some(
                        ShareError::Offline
                            | ShareError::HeartbeatExpired
                            | ShareError::RosterStale
                    )
                ) {
                    return Ok(false);
                }
                return Err(error);
            }
        };
        let phase = match receipt.state {
            ActivationStateView::Drained => Some(ClientIntentPhase::Drained),
            ActivationStateView::Denied | ActivationStateView::Expired => {
                Some(ClientIntentPhase::Cancelled)
            }
            ActivationStateView::Issued
            | ActivationStateView::Active
            | ActivationStateView::Restarted => None,
        };
        if let Some(phase) = phase {
            self.state.registry.transition_client_intent(
                grant,
                ClientSide::Consumer,
                operation_id,
                phase,
                Some(activation_id),
            )?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Revalidates the owner root and obtains a short-lived apply permit.
    pub async fn revalidate_before_apply(&self, snapshot: &SnapshotToken) -> Result<ApplyPermit> {
        ensure!(
            snapshot.epoch == self.state.membership.epoch,
            ShareError::EpochMismatch
        );
        let result = self
            .control_exchange(Operation::Revalidate {
                snapshot: snapshot.clone(),
            })
            .await?;
        let permit = match result {
            Reply::ApplyPermit(permit) => permit,
            _ => return Err(ShareError::Protocol.into()),
        };
        permit.verify_for(
            self.state.membership.owner,
            self.state.membership.share_id,
            self.state.membership.endpoint,
            super::now(),
        )?;
        ensure!(
            permit.epoch == self.state.membership.epoch,
            ShareError::EpochMismatch
        );
        ensure!(
            permit.snapshot == snapshot.snapshot,
            ShareError::ManifestMismatch
        );
        ensure!(
            permit.root_hash == snapshot.root_hash,
            ShareError::ManifestMismatch
        );
        Ok(permit)
    }

    /// Alias retained for the network contract and managed engine adapters.
    pub async fn revalidate(&self, snapshot: &SnapshotToken) -> Result<ApplyPermit> {
        self.revalidate_before_apply(snapshot).await
    }

    /// Records the beginning of a permit-scoped local apply at the owner.
    pub async fn apply_start(&self, permit: &ApplyPermit, operation_id: [u8; 16]) -> Result<()> {
        ensure!(
            permit.consumer == self.state.membership.endpoint,
            ShareError::EndpointMismatch
        );
        let result = self
            .control_exchange(Operation::ApplyStart(ApplyStart {
                operation_id,
                permit_nonce: permit.nonce,
            }))
            .await?;
        ensure!(matches!(result, Reply::ApplyAccepted), ShareError::Protocol);
        Ok(())
    }

    /// Records the durable writer drain acknowledgement for a permit.
    pub async fn apply_drained(
        &self,
        permit: &ApplyPermit,
        operation_id: [u8; 16],
        committed: bool,
    ) -> Result<()> {
        ensure!(
            permit.consumer == self.state.membership.endpoint,
            ShareError::EndpointMismatch
        );
        let result = self
            .control_exchange(Operation::ApplyDrained(ApplyDrained {
                operation_id,
                permit_nonce: permit.nonce,
                committed,
            }))
            .await?;
        ensure!(matches!(result, Reply::ApplyAccepted), ShareError::Protocol);
        Ok(())
    }

    /// Reads the owner's durable apply journal for response-loss recovery.
    /// A status response never renews the permit and does not authorize a new
    /// public write.
    pub async fn apply_status(
        &self,
        permit: &ApplyPermit,
        operation_id: Option<[u8; 16]>,
    ) -> Result<ApplyReceipt> {
        permit.verify_signature_for(
            self.state.membership.owner,
            self.state.membership.share_id,
            self.state.membership.endpoint,
        )?;
        let result = self
            .control_exchange(Operation::ApplyStatus(ApplyStatusQuery::for_permit(
                permit,
                operation_id,
            )))
            .await?;
        let receipt = match result {
            Reply::ApplyReceipt(receipt) => receipt,
            _ => return Err(ShareError::Protocol.into()),
        };
        ensure!(
            receipt.owner == permit.owner
                && receipt.share == permit.share
                && receipt.consumer == permit.consumer
                && receipt.permit_nonce == permit.nonce,
            ShareError::GrantReplay
        );
        if let Some(operation_id) = operation_id {
            // A response can be lost before the owner records ApplyStart.
            // In that Prepared state the durable row intentionally has no
            // operation id yet; the permit binding is the exact recovery key
            // and the caller must use cancel_apply before any new write. Once
            // the row has started, however, an operation id is mandatory and
            // must match byte-for-byte.
            ensure!(
                receipt.operation_id == Some(operation_id)
                    || (matches!(receipt.state, super::ApplyStateView::Prepared)
                        && receipt.operation_id.is_none()),
                ShareError::GrantReplay
            );
        }
        Ok(receipt)
    }

    /// Cancels an exact Prepared apply at the owner. A Started/Restarted row
    /// is returned unchanged so callers can wait for the actual local writer
    /// drain and then resend the same committed ApplyDrained acknowledgement.
    pub async fn cancel_apply(
        &self,
        permit: &ApplyPermit,
        operation_id: [u8; 16],
    ) -> Result<ApplyReceipt> {
        permit.verify_signature_for(
            self.state.membership.owner,
            self.state.membership.share_id,
            self.state.membership.endpoint,
        )?;
        let result = self
            .control_exchange(Operation::ApplyCancel(ApplyCancel::for_permit(
                permit,
                operation_id,
            )))
            .await?;
        let receipt = match result {
            Reply::ApplyReceipt(receipt) => receipt,
            _ => return Err(ShareError::Protocol.into()),
        };
        ensure!(
            receipt.owner == permit.owner
                && receipt.share == permit.share
                && receipt.consumer == permit.consumer
                && receipt.permit_nonce == permit.nonce,
            ShareError::GrantReplay
        );
        Ok(receipt)
    }

    /// Fetches one owner-authorized chunk subset from a member or owner
    /// provider over the separate share-swarm/1 ALPN. The consumer intent is
    /// durable before the provider is contacted; a cancelled caller therefore
    /// cannot make a later retry invent a new activation or payload grant.
    /// Every received chunk is checked against the signed manifest before it
    /// enters the existing CAS.
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_swarm_chunks(
        &self,
        store: Arc<Store>,
        grant: &ShareGrant,
        snapshot: &SnapshotToken,
        record: &SyncRecord,
        manifest: &ManifestAttestation,
        hashes: &[Hash32],
        operation_id: [u8; 16],
    ) -> Result<SwarmTransferReceipt> {
        // The public future is only a result handle. The actual managed
        // operation is owned by the service task registry, so aborting this
        // caller cannot detach a blocking CAS writer or release the endpoint
        // admission/root while the operation is still mutating state.
        let state = self.state.clone();
        let tasks = state.tasks.clone();
        let grant = grant.clone();
        let snapshot = snapshot.clone();
        let record = record.clone();
        let manifest = manifest.clone();
        let hashes = hashes.to_vec();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();
        // Reserve the exact durable operation before spawning.  This closes
        // the only interval in which a recovery proof could otherwise see an
        // idle task registry while this same-nonce request was about to
        // start.  The guard is moved into the manager-owned task and released
        // only after every network/CAS path has returned.
        let operation_guard = tasks.begin_operation((grant.nonce, operation_id))?;
        let pending_task = tasks.reserve_task();
        let task = tokio::spawn(async move {
            // Registration is the ownership hand-off. No endpoint, root, or
            // CAS work begins until the task is in the service registry.
            if start_rx.await.is_err() {
                return;
            }
            let _pending_task = pending_task;
            let _operation_guard = operation_guard;
            let _active = state.active.clone().read_owned().await;
            let session = ShareSession { state };
            let result = session
                .fetch_swarm_chunks_inner(
                    store,
                    &grant,
                    &snapshot,
                    &record,
                    &manifest,
                    &hashes,
                    operation_id,
                )
                .await;
            if result.is_err() {
                let _ = session.state.registry.transition_client_intent(
                    &grant,
                    ClientSide::Consumer,
                    operation_id,
                    ClientIntentPhase::Unknown,
                    None,
                );
            }
            if result.is_err() {
                session.emit_share_event(ShareTransferEvent {
                    operation_id,
                    share: grant.share,
                    peer: grant.provider,
                    phase: SharePhase::Reject,
                    direction: TransferDirection::Inbound,
                    bytes: 0,
                    epoch: grant.epoch,
                    provider_epoch: Some(grant.provider_epoch),
                    grant: Some(grant.nonce),
                });
            }
            let _ = result_tx.send(result);
        });
        if !tasks.register_and_start(task, start_tx) {
            return Err(ShareError::Busy.into());
        }
        result_rx
            .await
            .map_err(|_| anyhow::Error::new(ShareError::StateUnavailable))?
    }

    #[allow(clippy::too_many_arguments)]
    async fn fetch_swarm_chunks_inner(
        &self,
        store: Arc<Store>,
        grant: &ShareGrant,
        snapshot: &SnapshotToken,
        record: &SyncRecord,
        manifest: &ManifestAttestation,
        hashes: &[Hash32],
        operation_id: [u8; 16],
    ) -> Result<SwarmTransferReceipt> {
        let now = super::now();
        ensure!(
            grant.owner == self.state.membership.owner
                && grant.share == self.state.membership.share_id
                && grant.consumer == self.state.membership.endpoint,
            ShareError::OwnerMismatch
        );
        ensure!(
            grant.epoch == self.state.membership.epoch,
            ShareError::EpochMismatch
        );
        ensure!(
            grant.provider != self.state.membership.endpoint,
            ShareError::EndpointMismatch
        );
        grant.verify_for(
            self.state.membership.owner,
            self.state.membership.share_id,
            now,
        )?;
        snapshot.verify_for(grant.owner, grant.share, now)?;
        manifest.verify_for(grant.owner, grant.share, now)?;
        record.validate()?;
        manifest.verify_record(snapshot, record)?;
        ensure!(
            grant.snapshot == snapshot.snapshot,
            ShareError::ManifestMismatch
        );
        ensure!(
            grant.manifest == manifest.manifest_hash,
            ShareError::ManifestMismatch
        );
        ensure!(
            grant.epoch == snapshot.epoch && manifest.epoch == grant.epoch,
            ShareError::EpochMismatch
        );
        ensure!(
            grant.request_hash
                == request_hash(
                    grant.share,
                    snapshot.snapshot,
                    manifest.manifest_hash,
                    hashes
                )?,
            ShareError::GrantReplay
        );
        super::authority::validate_hash_subset(hashes)?;
        for hash in hashes {
            ensure!(
                manifest
                    .manifest
                    .chunks
                    .iter()
                    .any(|chunk| chunk.hash == *hash),
                ShareError::ManifestMismatch
            );
        }

        let intent =
            self.state
                .registry
                .prepare_client_intent(grant, ClientSide::Consumer, operation_id)?;
        ensure!(
            matches!(intent.phase, ClientIntentPhase::Prepared),
            ShareError::GrantReplay
        );
        self.state.registry.transition_client_intent(
            grant,
            ClientSide::Consumer,
            operation_id,
            ClientIntentPhase::AwaitingActivation,
            None,
        )?;

        let provider_address = if grant.provider == self.state.membership.owner {
            self.state.transport.remote_address()
        } else {
            let roster = {
                let cached = self.state.roster.lock().expect("roster cache mutex");
                cached.roster.clone()
            };
            let roster = match roster {
                Some(roster) => roster,
                None => self.ensure_roster_heartbeat().await?,
            };
            roster.verify_for(grant.owner, grant.share, super::now())?;
            let entry = roster.member(grant.provider).ok_or(ShareError::NotMember)?;
            ensure!(
                entry.member_epoch == grant.provider_epoch
                    && roster.member_is_fresh(grant.provider, super::now()),
                ShareError::RosterStale
            );
            entry.address.clone()
        };
        ensure!(
            provider_address.id == grant.provider,
            ShareError::EndpointMismatch
        );
        let provider_transport = SyncSession {
            client: SyncClient {
                secret_key: self.state.transport.client.secret_key.clone(),
                remote: provider_address.clone(),
                network_mode: self.state.transport.client.network_mode,
            },
            endpoint: self.state.transport.endpoint.clone(),
            share: None,
            remote: Arc::new(std::sync::RwLock::new(provider_address)),
            fallback_endpoint: (self.state.transport.client.network_mode == NetworkMode::Internet)
                .then_some(grant.provider),
            observation: Arc::new(std::sync::RwLock::new(None)),
            n0_lookup: Arc::new(std::sync::RwLock::new(None)),
        };
        let deadline = Instant::now() + CONTROL_DEADLINE;
        let connection = provider_transport
            .connect_alpn_until(ALPN_SWARM_V1, deadline)
            .await?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        ensure!(!remaining.is_zero(), ShareError::GrantExpired);
        let (mut send, mut receive) = tokio::time::timeout(remaining, connection.open_bi())
            .await
            .map_err(|_| anyhow::Error::new(ShareError::Offline))??;
        write_swarm_frame(
            &mut send,
            &wire::SwarmRequest::Grant {
                grant: grant.clone(),
                snapshot: snapshot.clone(),
                record: record.clone(),
                manifest: manifest.clone(),
                hashes: hashes.to_vec(),
                operation_id,
            },
            deadline,
        )
        .await?;
        send.finish()?;

        let ready = read_swarm_frame::<wire::SwarmResponse>(&mut receive, deadline).await?;
        let receipt = match ready {
            wire::SwarmResponse::Ready(receipt) => receipt,
            wire::SwarmResponse::Error(error) => return Err(error.into()),
            _ => return Err(ShareError::Protocol.into()),
        };
        let activation_id = receipt.activation_id.ok_or(ShareError::GrantReplay)?;
        // The provider's Ready frame is transported under the provider's
        // identity, but ActivationReceipt is deliberately not a second
        // signature.  Query the trusted owner over the authenticated control
        // session before admitting the provider as Active or writing one byte
        // to the consumer CAS.  Keep this query inside the original swarm
        // deadline so a late receipt cannot mint a fresh lease.
        let owner_receipt = self
            .ensure_data_admission_until(grant, activation_id, deadline)
            .await?;
        ensure!(
            receipt.binding == owner_receipt.binding
                && receipt.activation_id == owner_receipt.activation_id,
            ShareError::GrantReplay
        );
        self.emit_share_event(ShareTransferEvent {
            operation_id,
            share: grant.share,
            peer: grant.provider,
            phase: SharePhase::Swarm,
            direction: TransferDirection::Inbound,
            bytes: 0,
            epoch: grant.epoch,
            provider_epoch: Some(grant.provider_epoch),
            grant: Some(grant.nonce),
        });
        self.state.registry.transition_client_intent(
            grant,
            ClientSide::Consumer,
            operation_id,
            ClientIntentPhase::Active,
            Some(activation_id),
        )?;

        let chunks = read_swarm_frame::<wire::SwarmResponse>(&mut receive, deadline).await?;
        let (present, missing) = match chunks {
            wire::SwarmResponse::Chunks { present, missing } => (present, missing),
            wire::SwarmResponse::Error(error) => return Err(error.into()),
            _ => return Err(ShareError::Protocol.into()),
        };
        self.ensure_data_admission_until(grant, activation_id, deadline)
            .await?;
        let expected: BTreeSet<_> = hashes.iter().copied().collect();
        let mut seen = BTreeSet::new();
        for hash in present.iter().chain(missing.iter()) {
            ensure!(
                seen.insert(*hash) && expected.contains(hash),
                ShareError::Protocol
            );
        }
        ensure!(seen == expected, ShareError::Protocol);
        let mut transferred_bytes = 0_u64;
        for hash in &present {
            // A new chunk operation has its own fresh owner admission check;
            // an earlier Ready/Chunks observation cannot authorize payload
            // after revoke or pause.
            self.ensure_data_admission_until(grant, activation_id, deadline)
                .await?;
            let descriptor = manifest
                .manifest
                .chunks
                .iter()
                .find(|descriptor| descriptor.hash == *hash)
                .ok_or(ShareError::ManifestMismatch)?;
            let header = read_swarm_frame::<wire::SwarmResponse>(&mut receive, deadline).await?;
            let length = match header {
                wire::SwarmResponse::ChunkHeader {
                    hash: header_hash,
                    length,
                } => {
                    ensure!(header_hash == *hash, ShareError::ManifestMismatch);
                    ensure!(length == descriptor.length, ShareError::ManifestMismatch);
                    length as usize
                }
                wire::SwarmResponse::Error(error) => return Err(error.into()),
                _ => return Err(ShareError::Protocol.into()),
            };
            let mut bytes = vec![0_u8; length];
            let remaining = deadline.saturating_duration_since(Instant::now());
            ensure!(!remaining.is_zero(), ShareError::GrantExpired);
            tokio::time::timeout(remaining, receive.read_exact(&mut bytes))
                .await
                .map_err(|_| anyhow::Error::new(ShareError::GrantExpired))??;
            ensure!(
                Hash32::digest(&bytes) == *hash,
                ShareError::ManifestMismatch
            );
            self.ensure_data_admission_until(grant, activation_id, deadline)
                .await?;
            let put_store = store.clone();
            let put_hash = *hash;
            tokio::task::spawn_blocking(move || put_store.chunks().put_verified(put_hash, &bytes))
                .await
                .map_err(|_| anyhow::Error::new(ShareError::CasUnavailable))??;
            transferred_bytes = transferred_bytes.saturating_add(length as u64);
            self.emit_share_event(ShareTransferEvent {
                operation_id,
                share: grant.share,
                peer: grant.provider,
                phase: SharePhase::Swarm,
                direction: TransferDirection::Inbound,
                bytes: length as u64,
                epoch: grant.epoch,
                provider_epoch: Some(grant.provider_epoch),
                grant: Some(grant.nonce),
            });
        }
        let finished = read_swarm_frame::<wire::SwarmResponse>(&mut receive, deadline).await?;
        let mut transfer = match finished {
            wire::SwarmResponse::Finished(transfer) => transfer,
            wire::SwarmResponse::Error(error) => return Err(error.into()),
            _ => return Err(ShareError::Protocol.into()),
        };
        ensure!(
            transfer.share == grant.share
                && transfer.provider == grant.provider
                && usize::from(transfer.transferred_chunks) == present.len()
                && transfer.transferred_bytes == transferred_bytes
                && usize::from(transfer.missing_chunks) == missing.len()
                && transfer.verified == missing.is_empty(),
            ShareError::Protocol
        );
        let remaining = deadline.saturating_duration_since(Instant::now());
        ensure!(!remaining.is_zero(), ShareError::GrantExpired);
        // Consume the provider FIN before acknowledging the owner-side
        // consumer drain.  The provider waits on SendStream::stopped(), so
        // this closes the transport-level half before the bilateral journal
        // can advance.
        tokio::time::timeout(remaining, receive.read_to_end(0))
            .await
            .map_err(|_| anyhow::Error::new(ShareError::GrantExpired))??;
        self.state.registry.transition_client_intent(
            grant,
            ClientSide::Consumer,
            operation_id,
            ClientIntentPhase::Draining,
            Some(activation_id),
        )?;
        self.emit_share_event(ShareTransferEvent {
            operation_id,
            share: grant.share,
            peer: grant.provider,
            phase: SharePhase::Drain,
            direction: TransferDirection::Inbound,
            bytes: 0,
            epoch: grant.epoch,
            provider_epoch: Some(grant.provider_epoch),
            grant: Some(grant.nonce),
        });
        self.grant_drained(grant, activation_id).await?;
        let drain_confirmed = self
            .finalize_consumer_intent_after_drain(grant, operation_id, activation_id, deadline)
            .await?;
        transfer.drain_pending = !drain_confirmed;
        if drain_confirmed {
            self.emit_share_event(ShareTransferEvent {
                operation_id,
                share: grant.share,
                peer: grant.provider,
                phase: SharePhase::Done,
                direction: TransferDirection::Inbound,
                bytes: 0,
                epoch: grant.epoch,
                provider_epoch: Some(grant.provider_epoch),
                grant: Some(grant.nonce),
            });
        }
        connection.close(0u8.into(), b"share swarm receive complete");
        Ok(transfer)
    }

    pub async fn fetch_snapshot(&self, local: &MerkleTree) -> Result<crate::RemoteSnapshot> {
        self.state.transport.fetch_snapshot(local).await
    }
    pub async fn pull_record(
        &self,
        record: SyncRecord,
        store: Arc<Store>,
    ) -> Result<crate::PullReceipt> {
        self.state.transport.pull_record(record, store).await
    }
    pub async fn pull_record_to(
        &self,
        record: SyncRecord,
        store: Arc<Store>,
        destination_root: PathBuf,
    ) -> Result<crate::PullReceipt> {
        self.state
            .transport
            .pull_record_to(record, store, destination_root)
            .await
    }
    pub async fn pull_record_to_with_budget(
        &self,
        record: SyncRecord,
        store: Arc<Store>,
        destination_root: PathBuf,
        min_free_space_bytes: u64,
        pending_destination_bytes: u64,
    ) -> Result<crate::PullReceipt> {
        self.state
            .transport
            .pull_record_to_with_budget(
                record,
                store,
                destination_root,
                min_free_space_bytes,
                pending_destination_bytes,
            )
            .await
    }
    pub async fn push_record(
        &self,
        source: impl AsRef<Path>,
        record: SyncRecord,
        profile: ChunkingProfile,
    ) -> Result<crate::SyncApplyReceipt> {
        self.state
            .transport
            .push_record(source, record, profile)
            .await
    }
    pub async fn apply_metadata(&self, record: SyncRecord) -> Result<crate::SyncApplyReceipt> {
        self.state.transport.apply_metadata(record).await
    }
    /// Releases only this session; the shared device endpoint remains alive.
    pub async fn close(self) {
        // Share sessions borrow the device endpoint owned by ShareService;
        // dropping this state releases only this membership cache.
        drop(self);
    }
}

#[derive(Clone, Debug)]
struct Handler {
    registry: Arc<Registry>,
    key: SecretKey,
    runtimes: Arc<RwLock<BTreeMap<ShareId, Arc<OwnedRuntime>>>>,
    limit: Arc<tokio::sync::Semaphore>,
    active: Arc<tokio::sync::RwLock<()>>,
}

/// One provider-local source retained by the grant handler.  The owner source
/// is the already-open `OwnedRuntime`; a member source is the exact
/// registration guard supplied by its managed engine.  Neither branch opens
/// another endpoint, index, store, or root lease.
#[derive(Clone)]
enum SwarmProviderSource {
    Owner(Arc<OwnedRuntime>),
    Member(Arc<SupplierRegistrationGuard>),
}

impl SwarmProviderSource {
    fn store(&self) -> Arc<Store> {
        match self {
            Self::Owner(runtime) => runtime.store.clone(),
            Self::Member(guard) => guard.store.clone(),
        }
    }

    fn begin_operation(&self) -> Result<Option<SupplierOperation>> {
        match self {
            Self::Owner(_) => Ok(None),
            Self::Member(guard) => guard.begin_operation().map(Some),
        }
    }
}

/// Removes an operation nonce when the stream task finishes.  The durable
/// client-intent row remains the replay authority after a crash; this map is
/// only the process-local same-nonce admission guard.
struct InflightNonce {
    nonces: Arc<Mutex<BTreeSet<GrantNonce>>>,
    nonce: GrantNonce,
}

impl InflightNonce {
    fn acquire(nonces: Arc<Mutex<BTreeSet<GrantNonce>>>, nonce: GrantNonce) -> Result<Self> {
        let mut guard = nonces.lock().map_err(|_| ShareError::StateUnavailable)?;
        ensure!(guard.insert(nonce), ShareError::Busy);
        drop(guard);
        Ok(Self { nonces, nonce })
    }
}

impl Drop for InflightNonce {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.nonces.lock() {
            guard.remove(&self.nonce);
        }
    }
}

#[derive(Clone, Debug)]
struct SwarmAdmissionHandler {
    key: SecretKey,
    registry: Arc<Registry>,
    runtimes: Arc<RwLock<BTreeMap<ShareId, Arc<OwnedRuntime>>>>,
    suppliers: SupplierMap,
    endpoint: crate::Endpoint,
    mode: NetworkMode,
    active: Arc<tokio::sync::RwLock<()>>,
    tasks: Arc<SwarmTaskRegistry>,
    streams: Arc<tokio::sync::Semaphore>,
    inflight: Arc<Mutex<BTreeSet<GrantNonce>>>,
    share_events: Arc<ShareEventState>,
}

impl ProtocolHandler for SwarmAdmissionHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let Ok(stream_permit) = self.streams.clone().try_acquire_owned() else {
            connection.close(0u8.into(), b"share swarm busy");
            return Ok(());
        };
        let active = self.active.clone().read_owned().await;
        let handler = self.clone();
        let tasks = self.tasks.clone();
        let task_connection = connection.clone();
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();
        let pending_task = tasks.reserve_task();
        let task = tokio::spawn(async move {
            // Keep the task before its first connection operation until the
            // service-owned registry has accepted its JoinHandle. If the
            // endpoint is already closing, aborting this waiting task cannot
            // detach a started CAS writer or stream.
            if start_rx.await.is_err() {
                return;
            }
            let _pending_task = pending_task;
            let _stream_permit = stream_permit;
            let _active = active;
            let result = handler.run(task_connection.clone()).await;
            if result.is_err() {
                task_connection.close(0u8.into(), b"share swarm operation failed");
            }
        });
        if !tasks.register_and_start(task, start_tx) {
            connection.close(0u8.into(), b"share swarm unavailable");
        }
        Ok(())
    }
}

impl SwarmAdmissionHandler {
    fn emit_share_event(&self, event: ShareTransferEvent) {
        self.share_events.emit(event);
    }

    async fn run(&self, connection: Connection) -> Result<()> {
        let deadline = Instant::now() + CONTROL_DEADLINE;
        let remaining = deadline.saturating_duration_since(Instant::now());
        ensure!(!remaining.is_zero(), ShareError::Offline);
        let (mut send, mut receive) = tokio::time::timeout(remaining, connection.accept_bi())
            .await
            .map_err(|_| anyhow::Error::new(ShareError::Offline))??;
        let remaining = deadline.saturating_duration_since(Instant::now());
        ensure!(!remaining.is_zero(), ShareError::Offline);
        let request =
            tokio::time::timeout(remaining, read_frame::<wire::SwarmRequest>(&mut receive))
                .await
                .map_err(|_| anyhow::Error::new(ShareError::Offline))??;
        let result = match request {
            wire::SwarmRequest::Grant {
                grant,
                snapshot,
                record,
                manifest,
                hashes,
                operation_id,
            } => {
                self.handle_grant(
                    &connection,
                    &mut send,
                    grant,
                    snapshot,
                    record,
                    manifest,
                    hashes,
                    operation_id,
                )
                .await
            }
        };
        if result.is_err() {
            connection.close(0u8.into(), b"share swarm operation failed");
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_grant(
        &self,
        connection: &Connection,
        send: &mut iroh::endpoint::SendStream,
        grant: ShareGrant,
        snapshot: SnapshotToken,
        record: SyncRecord,
        manifest: ManifestAttestation,
        hashes: Vec<Hash32>,
        operation_id: [u8; 16],
    ) -> Result<()> {
        // Hold the exact operation admission through the final error frame,
        // stream finish, and bounded connection close in this method.  The
        // caller may cancel a recovery future while a provider has already
        // queued bytes; dropping this guard at the inner `?` boundary would
        // let an exact local drain proof race that queued response.
        let mut operation_guard = match self.tasks.begin_operation((grant.nonce, operation_id)) {
            Ok(guard) => guard,
            Err(error) => {
                let _ = write_frame(send, &wire::SwarmResponse::Error(safe_error(&error))).await;
                let _ = send.finish();
                Handler::wait_closed_bounded(connection).await;
                return Err(error);
            }
        };
        let mut supplier_operation = None;
        let mut inflight = None;
        let result = self
            .handle_grant_inner(
                connection,
                send,
                grant.clone(),
                snapshot,
                record,
                manifest,
                hashes,
                operation_id,
                &mut supplier_operation,
                &mut inflight,
            )
            .await;
        if result.is_err() {
            // Once an activation has been admitted, any stream or owner
            // status failure leaves the provider intent recoverable/unknown;
            // it must never look Drained merely because the caller observed
            // an error. Terminal Drained/Cancelled phases reject this update
            // and are intentionally preserved.
            let _ = self.registry.transition_client_intent(
                &grant,
                ClientSide::Provider,
                operation_id,
                ClientIntentPhase::Unknown,
                None,
            );
            let _ = write_frame(
                send,
                &wire::SwarmResponse::Error(safe_error(
                    result.as_ref().expect_err("result is an error"),
                )),
            )
            .await;
        }
        let _ = send.finish();
        // Keep all operation/supplier guards alive until the response frame
        // has been enqueued and the connection has had its bounded close
        // opportunity.  Shutdown/recovery therefore cannot observe a proof
        // while this handler still owns stream bytes or storage admission.
        let transport_drained = Handler::wait_closed_bounded(connection).await;
        if result.is_err() && transport_drained {
            self.emit_share_event(ShareTransferEvent {
                operation_id,
                share: grant.share,
                peer: grant.consumer,
                phase: SharePhase::Reject,
                direction: TransferDirection::Outbound,
                bytes: 0,
                epoch: grant.epoch,
                provider_epoch: Some(grant.provider_epoch),
                grant: Some(grant.nonce),
            });
        }
        let retain_recovery_evidence = self
            .registry
            .client_intent(&grant)
            .ok()
            .flatten()
            .is_some_and(|row| {
                row.side == ClientSide::Provider
                    && row.operation_id == operation_id
                    && matches!(
                        row.phase,
                        ClientIntentPhase::Unknown | ClientIntentPhase::Draining
                    )
            });
        SwarmTaskRegistry::mark_operation_drained(
            &mut operation_guard,
            transport_drained && retain_recovery_evidence,
        );
        drop(inflight);
        drop(supplier_operation);
        drop(operation_guard);
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_grant_inner(
        &self,
        connection: &Connection,
        send: &mut iroh::endpoint::SendStream,
        grant: ShareGrant,
        snapshot: SnapshotToken,
        record: SyncRecord,
        manifest: ManifestAttestation,
        hashes: Vec<Hash32>,
        operation_id: [u8; 16],
        supplier_operation: &mut Option<SupplierOperation>,
        inflight: &mut Option<InflightNonce>,
    ) -> Result<()> {
        let remote_consumer = connection.remote_id();
        let source = self.validate_grant(
            &grant,
            &snapshot,
            &record,
            &manifest,
            &hashes,
            remote_consumer,
        )?;
        // Hold a counted registration lease across validation, inventory,
        // payload reads, stream finish, and the provider drain/status exchange.
        // SupplierRegistrationGuard::drain waits on this exact operation
        // rather than taking the endpoint-wide control gate.
        *supplier_operation = source.begin_operation()?;
        *inflight = Some(InflightNonce::acquire(self.inflight.clone(), grant.nonce)?);
        let intent =
            self.registry
                .prepare_client_intent(&grant, ClientSide::Provider, operation_id)?;
        ensure!(
            matches!(intent.phase, ClientIntentPhase::Prepared),
            ShareError::GrantReplay
        );
        self.registry.transition_client_intent(
            &grant,
            ClientSide::Provider,
            operation_id,
            ClientIntentPhase::AwaitingActivation,
            None,
        )?;

        let (_initial_receipt, activation_id, activation_deadline) =
            match self.activate_provider(&grant, remote_consumer).await {
                Ok(value) => value,
                Err(error) => {
                    let _ = self.registry.transition_client_intent(
                        &grant,
                        ClientSide::Provider,
                        operation_id,
                        ClientIntentPhase::Unknown,
                        None,
                    );
                    return Err(error);
                }
            };
        let receipt = self
            .ensure_provider_admission(&grant, activation_id, &source, activation_deadline)
            .await?;
        self.emit_share_event(ShareTransferEvent {
            operation_id,
            share: grant.share,
            peer: remote_consumer,
            phase: SharePhase::Swarm,
            direction: TransferDirection::Outbound,
            bytes: 0,
            epoch: grant.epoch,
            provider_epoch: Some(grant.provider_epoch),
            grant: Some(grant.nonce),
        });
        self.registry.transition_client_intent(
            &grant,
            ClientSide::Provider,
            operation_id,
            ClientIntentPhase::Active,
            Some(activation_id),
        )?;
        write_swarm_frame(
            send,
            &wire::SwarmResponse::Ready(receipt),
            activation_deadline,
        )
        .await?;

        let store = source.store();
        let mut present = Vec::new();
        let mut missing = Vec::new();
        for hash in hashes.iter().copied() {
            self.ensure_provider_admission(&grant, activation_id, &source, activation_deadline)
                .await?;
            let descriptor = manifest
                .manifest
                .chunks
                .iter()
                .find(|descriptor| descriptor.hash == hash)
                .ok_or(ShareError::ManifestMismatch)?;
            let expected_length = descriptor.length as usize;
            let read_store = store.clone();
            let result = tokio::task::spawn_blocking(move || {
                read_store
                    .chunks()
                    .read_verified_bounded(hash, expected_length)
            })
            .await
            .map_err(|_| anyhow::Error::new(ShareError::CasUnavailable))?;
            match result {
                Ok(bytes) if bytes.len() == expected_length && Hash32::digest(&bytes) == hash => {
                    present.push(hash);
                }
                Ok(_) | Err(_) => missing.push(hash),
            }
        }
        self.ensure_provider_admission(&grant, activation_id, &source, activation_deadline)
            .await?;
        write_swarm_frame(
            send,
            &wire::SwarmResponse::Chunks {
                present: present.clone(),
                missing: missing.clone(),
            },
            activation_deadline,
        )
        .await?;
        let mut transferred_bytes = 0_u64;
        // Inventory and payload transmission are separate passes.  Keeping
        // only the bounded hash lists above prevents a grant of 64 maximum
        // chunks (each up to the core's 16 MiB limit) from accumulating a
        // whole-gigabyte Vec before the first byte is sent.
        for hash in present.iter().copied() {
            self.ensure_provider_admission(&grant, activation_id, &source, activation_deadline)
                .await?;
            let descriptor = manifest
                .manifest
                .chunks
                .iter()
                .find(|descriptor| descriptor.hash == hash)
                .ok_or(ShareError::ManifestMismatch)?;
            let expected_length = descriptor.length as usize;
            let read_store = store.clone();
            let bytes = tokio::task::spawn_blocking(move || {
                read_store
                    .chunks()
                    .read_verified_bounded(hash, expected_length)
            })
            .await
            .map_err(|_| anyhow::Error::new(ShareError::CasUnavailable))??;
            ensure!(
                bytes.len() == expected_length && Hash32::digest(&bytes) == hash,
                ShareError::CasUnavailable
            );
            let length = u32::try_from(bytes.len()).map_err(|_| ShareError::CasUnavailable)?;
            write_swarm_frame(
                send,
                &wire::SwarmResponse::ChunkHeader { hash, length },
                activation_deadline,
            )
            .await?;
            let remaining = activation_deadline.saturating_duration_since(Instant::now());
            ensure!(!remaining.is_zero(), ShareError::GrantExpired);
            tokio::time::timeout(remaining, send.write_all(&bytes))
                .await
                .map_err(|_| anyhow::Error::new(ShareError::GrantExpired))??;
            transferred_bytes = transferred_bytes.saturating_add(bytes.len() as u64);
            self.emit_share_event(ShareTransferEvent {
                operation_id,
                share: grant.share,
                peer: remote_consumer,
                phase: SharePhase::Swarm,
                direction: TransferDirection::Outbound,
                bytes: bytes.len() as u64,
                epoch: grant.epoch,
                provider_epoch: Some(grant.provider_epoch),
                grant: Some(grant.nonce),
            });
        }

        self.registry.transition_client_intent(
            &grant,
            ClientSide::Provider,
            operation_id,
            ClientIntentPhase::Draining,
            Some(activation_id),
        )?;
        self.ensure_provider_admission(&grant, activation_id, &source, activation_deadline)
            .await?;
        write_swarm_frame(
            send,
            &wire::SwarmResponse::Finished(SwarmTransferReceipt {
                share: grant.share,
                provider: grant.provider,
                transferred_chunks: u16::try_from(present.len()).unwrap_or(u16::MAX),
                transferred_bytes,
                missing_chunks: u16::try_from(missing.len()).unwrap_or(u16::MAX),
                verified: missing.is_empty(),
                // The provider cannot claim bilateral completion before the
                // consumer's authenticated GrantDrained arrives. The
                // consumer rewrites its local result after the owner query.
                drain_pending: true,
            }),
            activation_deadline,
        )
        .await?;
        // Finish the response stream, then wait for the peer to acknowledge
        // every buffered byte before recording the provider-side drain. A
        // successful `finish()` call alone is only a local enqueue.
        send.finish()?;
        let remaining = activation_deadline.saturating_duration_since(Instant::now());
        ensure!(!remaining.is_zero(), ShareError::GrantExpired);
        let stopped = tokio::time::timeout(remaining, send.stopped())
            .await
            .map_err(|_| anyhow::Error::new(ShareError::GrantExpired))?
            .map_err(|_| anyhow::Error::new(ShareError::TransferFailed))?;
        ensure!(stopped.is_none(), ShareError::TransferFailed);
        self.emit_share_event(ShareTransferEvent {
            operation_id,
            share: grant.share,
            peer: remote_consumer,
            phase: SharePhase::Drain,
            direction: TransferDirection::Outbound,
            bytes: 0,
            epoch: grant.epoch,
            provider_epoch: Some(grant.provider_epoch),
            grant: Some(grant.nonce),
        });
        self.ack_provider_drain(&grant, activation_id).await?;
        // The local stream is drained, but the provider intent remains
        // recoverable until the owner confirms both endpoint acknowledgements.
        // A lost status response therefore leaves `Draining` durable instead
        // of falsely publishing completion.
        let mut terminal_confirmed = false;
        if let Ok(receipt) = self
            .provider_activation_status(&grant, Some(activation_id), activation_deadline)
            .await
        {
            let phase = match receipt.state {
                ActivationStateView::Drained => Some(ClientIntentPhase::Drained),
                ActivationStateView::Denied | ActivationStateView::Expired => {
                    Some(ClientIntentPhase::Cancelled)
                }
                ActivationStateView::Issued
                | ActivationStateView::Active
                | ActivationStateView::Restarted => None,
            };
            if let Some(phase) = phase {
                self.registry.transition_client_intent(
                    &grant,
                    ClientSide::Provider,
                    operation_id,
                    phase,
                    Some(activation_id),
                )?;
                terminal_confirmed = true;
            }
        }
        if terminal_confirmed {
            self.emit_share_event(ShareTransferEvent {
                operation_id,
                share: grant.share,
                peer: remote_consumer,
                phase: SharePhase::Done,
                direction: TransferDirection::Outbound,
                bytes: 0,
                epoch: grant.epoch,
                provider_epoch: Some(grant.provider_epoch),
                grant: Some(grant.nonce),
            });
        }
        Ok(())
    }

    fn validate_grant(
        &self,
        grant: &ShareGrant,
        snapshot: &SnapshotToken,
        record: &SyncRecord,
        manifest: &ManifestAttestation,
        hashes: &[Hash32],
        remote_consumer: EndpointId,
    ) -> Result<SwarmProviderSource> {
        let now = super::now();
        ensure!(
            grant.provider == self.key.public(),
            ShareError::EndpointMismatch
        );
        ensure!(
            grant.consumer == remote_consumer,
            ShareError::EndpointMismatch
        );
        grant.verify_for(grant.owner, grant.share, now)?;
        snapshot.verify_for(grant.owner, grant.share, now)?;
        manifest.verify_for(grant.owner, grant.share, now)?;
        record.validate()?;
        manifest.verify_record(snapshot, record)?;
        ensure!(
            grant.snapshot == snapshot.snapshot,
            ShareError::ManifestMismatch
        );
        ensure!(grant.epoch == snapshot.epoch, ShareError::EpochMismatch);
        ensure!(
            grant.manifest == manifest.manifest_hash,
            ShareError::ManifestMismatch
        );
        ensure!(manifest.epoch == grant.epoch, ShareError::EpochMismatch);
        ensure!(
            grant.request_hash
                == request_hash(
                    grant.share,
                    snapshot.snapshot,
                    manifest.manifest_hash,
                    hashes
                )?,
            ShareError::GrantReplay
        );
        super::authority::validate_hash_subset(hashes)?;
        for hash in hashes {
            ensure!(
                manifest
                    .manifest
                    .chunks
                    .iter()
                    .any(|chunk| chunk.hash == *hash),
                ShareError::ManifestMismatch
            );
        }

        if grant.owner == self.key.public() {
            ensure!(
                grant.provider == self.key.public(),
                ShareError::EndpointMismatch
            );
            ensure!(grant.provider_epoch == 0, ShareError::EpochMismatch);
            let runtime = self
                .runtimes
                .read()
                .expect("runtime map")
                .get(&grant.share)
                .cloned()
                .ok_or(ShareError::UnknownShare)?;
            ensure!(
                runtime.config.owner == self.key.public(),
                ShareError::OwnerMismatch
            );
            ensure!(
                runtime.config.share_id == grant.share,
                ShareError::UnknownShare
            );
            ensure!(runtime.enabled.load(Ordering::SeqCst), ShareError::Busy);
            let consumer = self
                .registry
                .authorize(grant.share, grant.consumer, false)?;
            ensure!(consumer.epoch == grant.epoch, ShareError::EpochMismatch);
            Ok(SwarmProviderSource::Owner(runtime))
        } else {
            let guard = self
                .suppliers
                .read()
                .expect("supplier map")
                .get(&(grant.owner, grant.share))
                .cloned()
                .ok_or(ShareError::NotMember)?;
            ensure!(!guard.is_drained(), ShareError::Busy);
            ensure!(
                guard.membership.endpoint == self.key.public()
                    && guard.membership.owner == grant.owner
                    && guard.membership.share_id == grant.share
                    && guard.membership.revoked_at.is_none(),
                ShareError::OwnerMismatch
            );
            ensure!(
                guard.membership.epoch == grant.provider_epoch,
                ShareError::EpochMismatch
            );
            let persisted = self.registry.relationship(grant.owner, grant.share)?;
            ensure!(
                persisted.membership == guard.membership,
                ShareError::ReplicaClaimRejected
            );
            Ok(SwarmProviderSource::Member(guard))
        }
    }

    async fn activate_provider(
        &self,
        grant: &ShareGrant,
        remote_consumer: EndpointId,
    ) -> Result<(super::ActivationReceipt, [u8; 16], Instant)> {
        if grant.owner == self.key.public() {
            ensure!(
                grant.provider == self.key.public(),
                ShareError::EndpointMismatch
            );
            let started = Instant::now();
            let reply = self.registry.activate_grant_at(
                &self.key,
                &grant.activate_request(),
                self.key.public(),
                started,
            )?;
            let deadline = started
                + Duration::from_secs(u64::from(
                    reply
                        .max_duration_secs
                        .min(super::authority::MAX_ACTIVATE_TTL_SECONDS),
                ));
            let receipt = self.registry.activation_receipt(
                &ActivationStatusQuery::for_grant(grant, Some(reply.activation_id)),
                self.key.public(),
            )?;
            Ok((receipt, reply.activation_id, deadline))
        } else {
            let relationship = self.registry.relationship(grant.owner, grant.share)?;
            let session = ShareSession::from_relationship(
                self.registry.clone(),
                self.key.clone(),
                self.endpoint.clone(),
                self.mode,
                relationship,
                ShareSessionResources {
                    active: self.active.clone(),
                    tasks: self.tasks.clone(),
                    share_events: self.share_events.clone(),
                },
            );
            let lease = session.activate_grant(grant).await?;
            let activation_id = lease.reply.activation_id;
            let receipt = session
                .activation_status(grant, Some(activation_id))
                .await?;
            ensure!(
                receipt.binding == super::authority::ActivationBinding::from_grant(grant),
                ShareError::GrantReplay
            );
            ensure!(
                receipt.activation_id == Some(activation_id),
                ShareError::GrantReplay
            );
            ensure!(
                remote_consumer == grant.consumer,
                ShareError::EndpointMismatch
            );
            Ok((receipt, activation_id, lease.deadline))
        }
    }

    async fn ensure_provider_admission(
        &self,
        grant: &ShareGrant,
        activation_id: [u8; 16],
        source: &SwarmProviderSource,
        deadline: Instant,
    ) -> Result<ActivationReceipt> {
        match source {
            SwarmProviderSource::Owner(runtime) => {
                ensure!(runtime.enabled.load(Ordering::SeqCst), ShareError::Busy);
            }
            SwarmProviderSource::Member(guard) => {
                ensure!(!guard.is_drained(), ShareError::Busy);
                ensure!(
                    guard.membership.owner == grant.owner
                        && guard.membership.share_id == grant.share
                        && guard.membership.endpoint == self.key.public()
                        && guard.membership.epoch == grant.provider_epoch
                        && guard.membership.revoked_at.is_none(),
                    ShareError::ReplicaClaimRejected
                );
                let persisted = self.registry.relationship(grant.owner, grant.share)?;
                ensure!(
                    persisted.membership == guard.membership,
                    ShareError::ReplicaClaimRejected
                );
            }
        }

        let receipt = if grant.owner == self.key.public() {
            let runtime = self
                .runtimes
                .read()
                .expect("runtime map")
                .get(&grant.share)
                .cloned()
                .ok_or(ShareError::UnknownShare)?;
            let mut receipt = self.registry.activation_receipt(
                &ActivationStatusQuery::for_grant(grant, Some(activation_id)),
                self.key.public(),
            )?;
            receipt.admission_open = runtime.enabled.load(Ordering::SeqCst);
            receipt
        } else {
            let relationship = self.registry.relationship(grant.owner, grant.share)?;
            let session = ShareSession::from_relationship(
                self.registry.clone(),
                self.key.clone(),
                self.endpoint.clone(),
                self.mode,
                relationship,
                ShareSessionResources {
                    active: self.active.clone(),
                    tasks: self.tasks.clone(),
                    share_events: self.share_events.clone(),
                },
            );
            session
                .activation_status_until(grant, Some(activation_id), deadline)
                .await?
        };
        ShareSession::ensure_data_admission(&receipt, grant, activation_id)?;
        ensure!(!receipt.provider_drained, ShareError::GrantReplay);
        Ok(receipt)
    }

    async fn ack_provider_drain(&self, grant: &ShareGrant, activation_id: [u8; 16]) -> Result<()> {
        if grant.owner == self.key.public() {
            ensure!(
                grant.provider == self.key.public(),
                ShareError::EndpointMismatch
            );
            self.registry
                .drain_grant(grant.share, grant.nonce, activation_id, self.key.public())
        } else {
            let relationship = self.registry.relationship(grant.owner, grant.share)?;
            let session = ShareSession::from_relationship(
                self.registry.clone(),
                self.key.clone(),
                self.endpoint.clone(),
                self.mode,
                relationship,
                ShareSessionResources {
                    active: self.active.clone(),
                    tasks: self.tasks.clone(),
                    share_events: self.share_events.clone(),
                },
            );
            session.grant_drained(grant, activation_id).await
        }
    }

    async fn cancel_provider_activation(
        &self,
        grant: &ShareGrant,
        activation_id: Option<[u8; 16]>,
        operation_id: [u8; 16],
    ) -> Result<ActivationReceipt> {
        if grant.owner == self.key.public() {
            return self.registry.cancel_activation(
                &ActivationCancel::for_grant(grant, activation_id, operation_id),
                self.key.public(),
            );
        }
        let relationship = self.registry.relationship(grant.owner, grant.share)?;
        let session = ShareSession::from_relationship(
            self.registry.clone(),
            self.key.clone(),
            self.endpoint.clone(),
            self.mode,
            relationship,
            ShareSessionResources {
                active: self.active.clone(),
                tasks: self.tasks.clone(),
                share_events: self.share_events.clone(),
            },
        );
        session
            .cancel_activation(grant, activation_id, operation_id)
            .await
    }

    /// Recovers one provider-side intent without ever resuming the old
    /// payload stream. It mirrors the consumer recovery rules but sends the
    /// provider half of the bilateral drain acknowledgement.
    async fn recover_provider_intent(
        &self,
        grant: &ShareGrant,
        operation_id: [u8; 16],
        local_io_drained: bool,
    ) -> Result<ClientIntentRow> {
        ensure!(
            grant.provider == self.key.public(),
            ShareError::EndpointMismatch
        );
        let mut row = self
            .registry
            .client_intent_exact(grant, ClientSide::Provider, operation_id)?
            .ok_or(ShareError::GrantReplay)?;
        if matches!(
            row.phase,
            ClientIntentPhase::Drained | ClientIntentPhase::Cancelled
        ) {
            return Ok(row);
        }
        let deadline = Instant::now() + CONTROL_DEADLINE;
        let mut receipt = match self
            .provider_activation_status(grant, row.activation_id, deadline)
            .await
        {
            Ok(receipt) => receipt,
            Err(error) => {
                if !matches!(row.phase, ClientIntentPhase::Unknown) {
                    self.registry.transition_client_intent(
                        grant,
                        ClientSide::Provider,
                        operation_id,
                        ClientIntentPhase::Unknown,
                        row.activation_id,
                    )?;
                }
                return Err(error);
            }
        };

        if row.activation_id.is_none() && receipt.activation_id.is_some() {
            row = self.registry.transition_client_intent(
                grant,
                ClientSide::Provider,
                operation_id,
                row.phase,
                receipt.activation_id,
            )?;
        }

        if matches!(
            receipt.state,
            ActivationStateView::Active | ActivationStateView::Restarted
        ) {
            if !matches!(
                row.phase,
                ClientIntentPhase::Unknown | ClientIntentPhase::Draining
            ) {
                row = self.registry.transition_client_intent(
                    grant,
                    ClientSide::Provider,
                    operation_id,
                    ClientIntentPhase::Unknown,
                    receipt.activation_id,
                )?;
            }
            if !local_io_drained {
                return Ok(row);
            }
            let activation_id = receipt.activation_id.ok_or(ShareError::GrantReplay)?;
            if !matches!(row.phase, ClientIntentPhase::Draining) {
                row = self.registry.transition_client_intent(
                    grant,
                    ClientSide::Provider,
                    operation_id,
                    ClientIntentPhase::Draining,
                    Some(activation_id),
                )?;
            }
            if !receipt.provider_drained {
                self.ack_provider_drain(grant, activation_id).await?;
            }
            receipt = self
                .provider_activation_status(grant, Some(activation_id), deadline)
                .await?;
        }

        match receipt.state {
            ActivationStateView::Drained => {
                if !local_io_drained {
                    if !matches!(row.phase, ClientIntentPhase::Unknown) {
                        row = self.registry.transition_client_intent(
                            grant,
                            ClientSide::Provider,
                            operation_id,
                            ClientIntentPhase::Unknown,
                            receipt.activation_id,
                        )?;
                    }
                    return Ok(row);
                }
                if matches!(
                    row.phase,
                    ClientIntentPhase::Prepared | ClientIntentPhase::AwaitingActivation
                ) {
                    row = self.registry.transition_client_intent(
                        grant,
                        ClientSide::Provider,
                        operation_id,
                        ClientIntentPhase::Unknown,
                        receipt.activation_id,
                    )?;
                }
                if matches!(row.phase, ClientIntentPhase::Active) {
                    row = self.registry.transition_client_intent(
                        grant,
                        ClientSide::Provider,
                        operation_id,
                        ClientIntentPhase::Draining,
                        receipt.activation_id,
                    )?;
                }
                if !matches!(row.phase, ClientIntentPhase::Drained) {
                    row = self.registry.transition_client_intent(
                        grant,
                        ClientSide::Provider,
                        operation_id,
                        ClientIntentPhase::Drained,
                        receipt.activation_id,
                    )?;
                }
            }
            ActivationStateView::Denied | ActivationStateView::Expired => {
                if !local_io_drained {
                    if !matches!(row.phase, ClientIntentPhase::Unknown) {
                        row = self.registry.transition_client_intent(
                            grant,
                            ClientSide::Provider,
                            operation_id,
                            ClientIntentPhase::Unknown,
                            receipt.activation_id,
                        )?;
                    }
                    return Ok(row);
                }
                if matches!(
                    row.phase,
                    ClientIntentPhase::Active | ClientIntentPhase::Draining
                ) {
                    row = self.registry.transition_client_intent(
                        grant,
                        ClientSide::Provider,
                        operation_id,
                        ClientIntentPhase::Unknown,
                        receipt.activation_id,
                    )?;
                }
                if !matches!(row.phase, ClientIntentPhase::Cancelled) {
                    row = self.registry.transition_client_intent(
                        grant,
                        ClientSide::Provider,
                        operation_id,
                        ClientIntentPhase::Cancelled,
                        receipt.activation_id,
                    )?;
                }
            }
            ActivationStateView::Issued => {
                let cancelled = self
                    .cancel_provider_activation(grant, receipt.activation_id, operation_id)
                    .await?;
                if matches!(
                    cancelled.state,
                    ActivationStateView::Denied | ActivationStateView::Expired
                ) && local_io_drained
                {
                    if !matches!(row.phase, ClientIntentPhase::Cancelled) {
                        row = self.registry.transition_client_intent(
                            grant,
                            ClientSide::Provider,
                            operation_id,
                            ClientIntentPhase::Cancelled,
                            cancelled.activation_id,
                        )?;
                    }
                } else if !matches!(row.phase, ClientIntentPhase::Unknown) {
                    row = self.registry.transition_client_intent(
                        grant,
                        ClientSide::Provider,
                        operation_id,
                        ClientIntentPhase::Unknown,
                        cancelled.activation_id,
                    )?;
                }
            }
            ActivationStateView::Active | ActivationStateView::Restarted => {}
        }
        Ok(row)
    }

    async fn provider_activation_status(
        &self,
        grant: &ShareGrant,
        activation_id: Option<[u8; 16]>,
        deadline: Instant,
    ) -> Result<ActivationReceipt> {
        if grant.owner == self.key.public() {
            return self.registry.activation_receipt(
                &ActivationStatusQuery::for_grant(grant, activation_id),
                self.key.public(),
            );
        }
        let relationship = self.registry.relationship(grant.owner, grant.share)?;
        let session = ShareSession::from_relationship(
            self.registry.clone(),
            self.key.clone(),
            self.endpoint.clone(),
            self.mode,
            relationship,
            ShareSessionResources {
                active: self.active.clone(),
                tasks: self.tasks.clone(),
                share_events: self.share_events.clone(),
            },
        );
        session
            .activation_status_until(grant, activation_id, deadline)
            .await
    }
}

async fn write_swarm_frame<T: serde::Serialize>(
    send: &mut iroh::endpoint::SendStream,
    value: &T,
    deadline: Instant,
) -> Result<()> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    ensure!(!remaining.is_zero(), ShareError::GrantExpired);
    tokio::time::timeout(remaining, write_frame(send, value))
        .await
        .map_err(|_| anyhow::Error::new(ShareError::GrantExpired))??;
    Ok(())
}

async fn read_swarm_frame<T: serde::de::DeserializeOwned>(
    receive: &mut iroh::endpoint::RecvStream,
    deadline: Instant,
) -> Result<T> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    ensure!(!remaining.is_zero(), ShareError::GrantExpired);
    tokio::time::timeout(remaining, read_frame(receive))
        .await
        .map_err(|_| anyhow::Error::new(ShareError::GrantExpired))?
}

impl ProtocolHandler for Handler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let Ok(permit) = self.limit.clone().try_acquire_owned() else {
            connection.close(0u8.into(), b"share busy");
            return Ok(());
        };
        let active = self.active.clone().read_owned().await;
        let handler = self.clone();
        // Router cancellation must not drop a future that owns blocking disk work.
        // The spawned handler retains its runtime/lease and drains every join handle.
        let _ = tokio::spawn(async move {
            let _permit = permit;
            let _active = active;
            let outcome = handler.run(connection.clone()).await;
            if outcome.is_err() {
                connection.close(0u8.into(), b"share operation ended");
            }
        })
        .await;
        Ok(())
    }
}
impl Handler {
    async fn run(&self, connection: Connection) -> Result<()> {
        let handshake_deadline = Instant::now() + CONTROL_DEADLINE;
        let remaining = handshake_deadline.saturating_duration_since(Instant::now());
        let (mut send, mut receive) = tokio::time::timeout(remaining, connection.accept_bi())
            .await
            .map_err(|_| anyhow::Error::new(ShareError::Offline))??;
        let remaining = handshake_deadline.saturating_duration_since(Instant::now());
        let hello = tokio::time::timeout(remaining, wire::read_hello(&mut receive))
            .await
            .map_err(|_| anyhow::Error::new(ShareError::Offline))??;
        let runtime = self
            .runtimes
            .read()
            .expect("runtime map")
            .get(&hello.share_id)
            .cloned();
        let Some(runtime) = runtime else {
            write_frame(&mut send, &Reply::Error(ShareError::UnknownShare)).await?;
            send.finish()?;
            Self::wait_closed_bounded(&connection).await;
            return Ok(());
        };
        let peer = connection.remote_id();
        let operation_started = Instant::now();
        let operation = hello.operation;
        let result = match operation {
            authority @ (Operation::Snapshot
            | Operation::Manifest { .. }
            | Operation::SwarmGrant { .. }
            | Operation::Revalidate { .. }
            | Operation::ApplyStart(_)
            | Operation::ApplyDrained(_)
            | Operation::Activate(_)
            | Operation::GrantDrained { .. }
            | Operation::ActivationStatus(_)
            | Operation::ActivationCancel(_)
            | Operation::ApplyStatus(_)
            | Operation::ApplyCancel(_)) => {
                self.run_authority(&runtime, peer, hello.share_id, authority, operation_started)
                    .await
            }
            operation => (|| -> Result<Reply> {
                ensure!(runtime.enabled.load(Ordering::SeqCst), ShareError::Busy);
                match operation {
                    Operation::Validate(ticket) => {
                        ensure!(
                            ticket.preview().share_id == hello.share_id,
                            ShareError::InvalidTicket
                        );
                        self.registry.validate(&ticket)?;
                        Ok(Reply::Validated(ticket.preview()))
                    }
                    Operation::Enroll { ticket, proof } => {
                        ensure!(
                            ticket.preview().share_id == hello.share_id,
                            ShareError::InvalidTicket
                        );
                        Ok(Reply::Enrolled(self.registry.enroll(
                            &ticket,
                            peer,
                            proof.as_deref(),
                        )?))
                    }
                    Operation::Session => {
                        self.registry.authorize(hello.share_id, peer, false)?;
                        Ok(Reply::Accepted)
                    }
                    Operation::Resume => Ok(Reply::Resumed(self.registry.authorize(
                        hello.share_id,
                        peer,
                        false,
                    )?)),
                    Operation::Roster => {
                        let (roster, challenge) = self.registry.issue_roster_challenge(
                            &self.key,
                            hello.share_id,
                            peer,
                        )?;
                        Ok(Reply::Roster { roster, challenge })
                    }
                    Operation::Heartbeat(heartbeat) => {
                        ensure!(heartbeat.share == hello.share_id, ShareError::OwnerMismatch);
                        Ok(Reply::Heartbeat(
                            self.registry
                                .accept_roster_heartbeat(&self.key, &heartbeat, peer)?,
                        ))
                    }
                    _ => Err(ShareError::Protocol.into()),
                }
            })(),
        };
        let reply = result.unwrap_or_else(|error| Reply::Error(safe_error(&error)));
        let session = matches!(reply, Reply::Accepted);
        // Denied and preview-only connections cannot join a drain after revoke
        // took its close snapshot. An accepted session registers before its final
        // authorization check, so every operation is either closed or denied.
        let _tracked = session.then(|| runtime.track(connection.clone()));
        if session {
            ensure!(runtime.enabled.load(Ordering::SeqCst), ShareError::Busy);
            self.registry.authorize(hello.share_id, peer, false)?;
        }
        write_frame(&mut send, &reply).await?;
        send.finish()?;
        if !session {
            Self::wait_closed_bounded(&connection).await;
            return Ok(());
        }
        let member = self.registry.authorize(hello.share_id, peer, false)?;
        let auth = Authorization {
            runtime: runtime.clone(),
            peer,
            epoch: member.epoch,
        };
        let handler = SyncHandler {
            active_handlers: self.active.clone(),
            _root_lease: runtime.lease.clone(),
            share_authorization: Some(auth.clone()),
            admission: Arc::new(OperationAdmission::default()),
            observer: runtime.observer.lock().expect("observer mutex").clone(),
            store: runtime.store.clone(),
            index: runtime.index.clone(),
            destination_root: runtime.config.root.clone(),
            peer_policy: crate::PeerPolicy::AllowListed([peer].into_iter().collect()),
            apply_lock: runtime.gate.clone(),
            connection_limit: self.limit.clone(),
            min_free_space_bytes: runtime.config.min_free_space_bytes,
            state_root: runtime.config.state_root.clone(),
            receive_admission_lock: runtime.receive_gate.clone(),
        };
        let session_admission_deadline = Instant::now() + CONTROL_DEADLINE;
        let remaining = session_admission_deadline.saturating_duration_since(Instant::now());
        let (mut send, mut receive) = tokio::time::timeout(remaining, connection.accept_bi())
            .await
            .map_err(|_| anyhow::Error::new(ShareError::Offline))??;
        let result = async {
            auth.check(false)?;
            let remaining = session_admission_deadline.saturating_duration_since(Instant::now());
            let request =
                tokio::time::timeout(remaining, read_frame::<SyncWireRequest>(&mut receive))
                    .await
                    .map_err(|_| anyhow::Error::new(ShareError::Offline))??;
            match request {
                request @ SyncWireRequest::QueryNode { .. } => {
                    handler
                        .handle_query_session(request, &mut send, &mut receive)
                        .await
                }
                SyncWireRequest::PullRecord { record } => {
                    handler
                        .handle_pull(record, &mut send, &mut receive, peer)
                        .await
                }
                SyncWireRequest::PushRecord { record, manifest } => {
                    handler
                        .handle_push_record(record, manifest, &mut send, &mut receive, peer)
                        .await
                }
                SyncWireRequest::ApplyMetadata { record } => {
                    handler.handle_metadata(record, &mut send).await
                }
                _ => Err(ShareError::Protocol.into()),
            }
        }
        .await;
        if let Err(error) = result {
            let _ = write_frame(&mut send, &SyncWireResponse::ShareError(safe_error(&error))).await;
        }
        let _ = send.finish();
        // All disk/chunk work has completed before the tracked guard can disappear.
        Self::wait_closed_bounded(&connection).await;
        Ok(())
    }

    async fn wait_closed_bounded(connection: &Connection) -> bool {
        Self::wait_closed_bounded_until(connection, CONTROL_DEADLINE).await
    }

    /// Waits for the transport to report its terminal state, including after
    /// a bounded timeout. Calling `Connection::close` only requests an abort;
    /// it is not itself a local drain proof. The second wait observes the
    /// owned connection's close notification before a caller may mark the
    /// exact operation as locally complete.
    async fn wait_closed_bounded_until(connection: &Connection, deadline: Duration) -> bool {
        if tokio::time::timeout(deadline, connection.closed())
            .await
            .is_ok()
        {
            return connection.close_reason().is_some();
        }
        connection.close(0u8.into(), b"share control deadline");
        tokio::time::timeout(CLOSE_CONFIRM_DEADLINE, connection.closed())
            .await
            .is_ok_and(|_| connection.close_reason().is_some())
    }

    /// Handles the owner-authoritative v1 swarm control records.  Each branch
    /// rechecks the authenticated QUIC peer and the live catalog binding; the
    /// serialized wire values are never treated as a caller-selected role.
    async fn run_authority(
        &self,
        runtime: &Arc<OwnedRuntime>,
        peer: EndpointId,
        share: ShareId,
        operation: Operation,
        operation_started: Instant,
    ) -> Result<Reply> {
        // Admission and new writes stop as soon as a runtime is paused or
        // revoked, but an already-started operation must still be able to
        // record its authenticated drain acknowledgement.  Otherwise a
        // normal shutdown would turn a known writer into a permanent Pending
        // blocker merely because the runtime was disabled first.
        if !matches!(
            operation,
            Operation::ApplyDrained(_)
                | Operation::GrantDrained { .. }
                | Operation::ActivationStatus(_)
                | Operation::ActivationCancel(_)
                | Operation::ApplyStatus(_)
                | Operation::ApplyCancel(_)
        ) {
            ensure!(runtime.enabled.load(Ordering::SeqCst), ShareError::Busy);
        }
        ensure!(runtime.config.share_id == share, ShareError::UnknownShare);
        match operation {
            Operation::Snapshot => {
                let member = self.registry.authorize(share, peer, false)?;
                let _gate = runtime.gate.lock().await;
                let report = runtime.index.scan()?;
                crate::ensure_index_report_safe(&report)?;
                runtime.refresh_causal_state()?;
                let records = runtime.index.sync_records()?;
                let tree = MerkleTree::from_records(records.clone())?;
                let token = SnapshotToken::sign(
                    &self.key,
                    share,
                    member.epoch,
                    random_nonce(),
                    tree.root_hash(),
                    records.len(),
                    super::now(),
                )?;
                let snapshot = AuthoritativeSnapshot { token, records };
                snapshot.verify_complete(self.key.public(), share, super::now())?;
                self.registry.store_snapshot(&snapshot)?;
                Ok(Reply::Snapshot(snapshot))
            }
            Operation::Manifest { snapshot, record } => {
                let _member = self.registry.authorize(share, peer, false)?;
                ensure!(snapshot.share == share, ShareError::OwnerMismatch);
                snapshot.verify_for(self.key.public(), share, super::now())?;
                let _gate = runtime.gate.lock().await;
                let stored = self
                    .registry
                    .snapshot(snapshot.snapshot)?
                    .ok_or(ShareError::ManifestMismatch)?;
                ensure!(stored.token == snapshot, ShareError::ManifestMismatch);
                let stored_record = stored
                    .records
                    .iter()
                    .find(|candidate| candidate.logical_hash() == record.logical_hash())
                    .ok_or(ShareError::ManifestMismatch)?;
                ensure!(stored_record == &record, ShareError::ManifestMismatch);
                ensure!(!record.tombstone, ShareError::ManifestMismatch);
                ensure!(
                    record.kind == deltaweave_core::SyncEntryKind::File,
                    ShareError::ManifestMismatch
                );
                // The owner is also a first-class swarm supplier. Build the
                // manifest and ingest every verified missing chunk while the
                // exact owner runtime gate and admission lease are held. A
                // manifest-only response otherwise leaves a cold owner CAS,
                // making a later RO sync depend on an accidental RW warm-up.
                let path = owner_source_path(runtime, share, &record)?;
                let store = runtime.store.clone();
                let admission = crate::DiskAdmission::new(
                    runtime.store.state_root().to_path_buf(),
                    runtime.config.root.clone(),
                    runtime.config.min_free_space_bytes,
                    0,
                );
                let manifest = tokio::task::spawn_blocking(move || {
                    store.ingest_file_with_admission(path, ChunkingProfile::DEFAULT, |bytes| {
                        admission.check_state(bytes)
                    })
                })
                .await??;
                ensure!(
                    manifest.size == record.size
                        && record
                            .content_hash
                            .is_some_and(|content_hash| manifest.file_hash == content_hash),
                    ShareError::ManifestMismatch
                );
                manifest.validate()?;
                let attestation = ManifestAttestation::sign(
                    &self.key,
                    &snapshot,
                    &record,
                    manifest,
                    super::now(),
                )?;
                attestation.verify_for(self.key.public(), share, super::now())?;
                Ok(Reply::Manifest(attestation))
            }
            Operation::SwarmGrant {
                provider,
                snapshot,
                manifest,
                hashes,
            } => {
                ensure!(snapshot.share == share, ShareError::OwnerMismatch);
                ensure!(manifest.share == share, ShareError::OwnerMismatch);
                ensure!(manifest.epoch == snapshot.epoch, ShareError::EpochMismatch);
                ensure!(peer != provider, ShareError::EndpointMismatch);
                if provider == self.key.public() {
                    ensure!(runtime.config.share_id == share, ShareError::UnknownShare);
                    ensure!(runtime.enabled.load(Ordering::SeqCst), ShareError::Busy);
                }
                let grant = self
                    .registry
                    .issue_swarm_grant(&self.key, peer, provider, &snapshot, &manifest, &hashes)?;
                Ok(Reply::Grant(grant))
            }
            Operation::Revalidate { snapshot } => {
                let _member = self.registry.authorize(share, peer, false)?;
                let _gate = runtime.gate.lock().await;
                let report = runtime.index.scan()?;
                crate::ensure_index_report_safe(&report)?;
                runtime.refresh_causal_state()?;
                let records = runtime.index.sync_records()?;
                let root = MerkleTree::from_records(records)?.root_hash();
                let permit = self
                    .registry
                    .issue_apply_permit(&self.key, peer, &snapshot, root)?;
                Ok(Reply::ApplyPermit(permit))
            }
            Operation::ApplyStart(start) => {
                let _member = self.registry.authorize(share, peer, false)?;
                let _gate = runtime.gate.lock().await;
                let records = runtime.index.sync_records()?;
                let root = MerkleTree::from_records(records)?.root_hash();
                self.registry
                    .apply_start(self.key.public(), share, peer, root, &start)?;
                Ok(Reply::ApplyAccepted)
            }
            Operation::ApplyDrained(drained) => {
                self.registry
                    .apply_drained(self.key.public(), share, peer, &drained)?;
                Ok(Reply::ApplyAccepted)
            }
            Operation::GrantDrained {
                nonce,
                activation_id,
            } => {
                self.registry
                    .drain_grant(share, nonce, activation_id, peer)?;
                Ok(Reply::GrantDrained)
            }
            Operation::Activate(request) => {
                ensure!(request.share == share, ShareError::OwnerMismatch);
                let reply = self.registry.activate_grant_at(
                    &self.key,
                    &request,
                    peer,
                    operation_started,
                )?;
                Ok(Reply::Activate(reply))
            }
            Operation::ActivationStatus(query) => {
                ensure!(query.binding.share == share, ShareError::OwnerMismatch);
                let mut receipt = self.registry.activation_receipt(&query, peer)?;
                receipt.admission_open = runtime.enabled.load(Ordering::SeqCst);
                Ok(Reply::ActivationReceipt(receipt))
            }
            Operation::ActivationCancel(cancel) => {
                ensure!(cancel.binding.share == share, ShareError::OwnerMismatch);
                let mut receipt = self.registry.cancel_activation(&cancel, peer)?;
                receipt.admission_open = runtime.enabled.load(Ordering::SeqCst);
                Ok(Reply::ActivationReceipt(receipt))
            }
            Operation::ApplyStatus(query) => {
                ensure!(query.share == share, ShareError::OwnerMismatch);
                let mut receipt = self.registry.apply_status(&query, peer)?;
                receipt.admission_open = runtime.enabled.load(Ordering::SeqCst);
                Ok(Reply::ApplyReceipt(receipt))
            }
            Operation::ApplyCancel(cancel) => {
                ensure!(cancel.share == share, ShareError::OwnerMismatch);
                let mut receipt = self.registry.cancel_apply(&cancel, peer)?;
                receipt.admission_open = runtime.enabled.load(Ordering::SeqCst);
                Ok(Reply::ApplyReceipt(receipt))
            }
            _ => Err(ShareError::Protocol.into()),
        }
    }
}

/// Resolves one owner-authoritative record only through the already-admitted
/// managed runtime. The source must remain beneath the canonical public root,
/// and the runtime's Store/index/private reservation must all refer to the
/// same binding before a manifest request can populate the owner CAS.
fn owner_source_path(
    runtime: &OwnedRuntime,
    share: ShareId,
    record: &SyncRecord,
) -> Result<PathBuf> {
    let root = fs::canonicalize(&runtime.config.root)?;
    let state_root = fs::canonicalize(runtime.store.state_root())?;
    ensure!(
        runtime.lease.root() == root.as_path()
            && fs::canonicalize(runtime.index.root())? == root
            && fs::canonicalize(&runtime.config.state_root)? == state_root
            && runtime
                .lease
                .private_roots()
                .iter()
                .any(|private| private == &state_root),
        ShareError::StateUnavailable
    );
    ensure!(
        matches!(
            runtime.lease.kind(),
            RootUse::Managed {
                share: admitted_share,
                owner: admitted_owner
            } if admitted_share == &share.0 && admitted_owner == runtime.config.owner.as_bytes()
        ),
        ShareError::OwnerMismatch
    );
    let path = runtime.config.root.join(record.path.as_str());
    let canonical_path = fs::canonicalize(path)?;
    ensure!(
        canonical_path.starts_with(&root) && canonical_path.is_file(),
        ShareError::ManifestMismatch
    );
    Ok(canonical_path)
}

fn safe_error(error: &anyhow::Error) -> ShareError {
    ShareError::classify(error)
}

#[cfg(test)]
mod tests {
    use super::super::{ActivationStateView, Permission, now};
    use super::*;
    use futures_lite::StreamExt;
    use iroh::address_lookup::{
        AddressLookup, EndpointData, Error as LookupError, Item, memory::MemoryLookup,
    };
    use iroh::{
        Endpoint, TransportAddr,
        endpoint::{PathEvent, presets},
    };

    #[derive(Debug, Clone)]
    struct HangingAddressLookup;

    impl AddressLookup for HangingAddressLookup {
        fn publish(&self, _data: &EndpointData) {}

        fn resolve(
            &self,
            _endpoint_id: EndpointId,
        ) -> Option<futures_lite::stream::Boxed<Result<Item, LookupError>>> {
            Some(futures_lite::stream::pending().boxed())
        }
    }

    /// Test-only peer which is authenticated by its endpoint identity but
    /// returns a previously captured owner receipt.  The consumer must still
    /// query the owner before admitting the stream or writing a CAS chunk;
    /// this fixture makes that distinction observable without accepting any
    /// payload bytes from an untrusted Ready frame.
    #[derive(Clone, Debug)]
    struct ForgedReadyHandler {
        receipt: ActivationReceipt,
    }

    impl ProtocolHandler for ForgedReadyHandler {
        async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
            let Ok((mut send, mut receive)) = connection.accept_bi().await else {
                return Ok(());
            };
            let result: Result<()> = async {
                let _request = read_frame::<wire::SwarmRequest>(&mut receive).await?;
                write_frame(&mut send, &wire::SwarmResponse::Ready(self.receipt.clone())).await?;
                send.finish()?;
                Handler::wait_closed_bounded(&connection).await;
                Ok(())
            }
            .await;
            if result.is_err() {
                connection.close(0u8.into(), b"test forged ready failed");
            }
            Ok(())
        }
    }

    async fn open_with_bound_test_endpoint(
        state: impl AsRef<Path>,
        mode: NetworkMode,
        endpoint: Endpoint,
    ) -> Result<ShareService> {
        open_with_bound_test_endpoint_with_limit(state, mode, endpoint, 64).await
    }

    async fn open_with_bound_test_endpoint_with_limit(
        state: impl AsRef<Path>,
        mode: NetworkMode,
        endpoint: Endpoint,
        max_connections: usize,
    ) -> Result<ShareService> {
        let state = root_admission::reserve_private(state)?;
        root_admission::private_directory(&state)?;
        let key = load_or_create_identity(state.join("device.key"))?.secret_key;
        ensure!(key.public() == endpoint.id(), ShareError::OwnerMismatch);
        let registry = Arc::new(Registry::open(&state, key.public())?);
        Ok(ShareService::from_bound_endpoint_with_limit(
            key,
            registry,
            mode,
            endpoint,
            max_connections,
        ))
    }

    fn relay_only_address(service: &ShareService) -> EndpointAddr {
        let current = service.router.endpoint().addr();
        let relays: Vec<_> = current
            .relay_urls()
            .cloned()
            .map(TransportAddr::Relay)
            .collect();
        assert!(!relays.is_empty(), "N0 endpoint must advertise a relay");
        EndpointAddr::from_parts(current.id, relays)
    }

    fn direct_only_address(service: &ShareService) -> EndpointAddr {
        let socket = service
            .router
            .endpoint()
            .bound_sockets()
            .into_iter()
            .find(|socket| socket.is_ipv4())
            .expect("endpoint must expose a local direct hint");
        let socket = if socket.ip().is_unspecified() {
            let ip = if socket.is_ipv4() {
                std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
            } else {
                std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)
            };
            std::net::SocketAddr::new(ip, socket.port())
        } else {
            socket
        };
        EndpointAddr::from_parts(service.endpoint_id(), [TransportAddr::Ip(socket)])
    }

    #[derive(Clone, Copy, Debug)]
    struct RelayControlObservation {
        relay_bound_sockets_empty: bool,
        selected_relay: bool,
        selected_ip: bool,
        path_has_relay: bool,
        path_has_ip: bool,
        opened_relay: bool,
        opened_ip: bool,
        selected_event_relay: bool,
        selected_event_ip: bool,
        saw_lagged: bool,
        tx_delta: u64,
        rx_delta: u64,
        path_event_count: usize,
        roster_binding_valid: bool,
    }

    fn print_relay_observation(
        phase: &str,
        started: Instant,
        observation: RelayControlObservation,
    ) {
        println!(
            "{{\"phase\":\"{phase}\",\"elapsed_ms\":{},\"relay_bound_sockets_empty\":{},\"selected_relay\":{},\"selected_ip\":{},\"path_has_relay\":{},\"path_has_ip\":{},\"opened_relay\":{},\"opened_ip\":{},\"selected_event_relay\":{},\"selected_event_ip\":{},\"saw_lagged\":{},\"tx_delta\":{},\"rx_delta\":{},\"path_event_count\":{},\"roster_binding_valid\":{}}}",
            started.elapsed().as_millis(),
            observation.relay_bound_sockets_empty,
            observation.selected_relay,
            observation.selected_ip,
            observation.path_has_relay,
            observation.path_has_ip,
            observation.opened_relay,
            observation.opened_ip,
            observation.selected_event_relay,
            observation.selected_event_ip,
            observation.saw_lagged,
            observation.tx_delta,
            observation.rx_delta,
            observation.path_event_count,
            observation.roster_binding_valid,
        );
    }

    async fn wait_for_actual_n0_address(
        session: &ShareSession,
        owner: EndpointId,
        deadline: Instant,
    ) -> Result<(crate::N0LookupObservation, EndpointAddr)> {
        loop {
            let lookup_deadline = Instant::now() + Duration::from_secs(8);
            match session
                .state
                .transport
                .resolve_n0(owner, lookup_deadline.min(deadline))
                .await
            {
                Ok((observation, address))
                    if observation.matched_endpoint
                        && (observation.pkarr_results > 0 || observation.dns_results > 0) =>
                {
                    return Ok((observation, address));
                }
                Ok(_) | Err(_) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Ok(_) | Err(_) => {
                    return Err(ShareError::Offline.into());
                }
            }
        }
    }

    async fn observed_relay_roster_control(session: &ShareSession) -> RelayControlObservation {
        let connection = match session.state.transport.connect_control().await {
            Ok(connection) => ControlConnection(connection),
            Err(_) => panic!("relay control connection failed"),
        };
        let mut events = connection.path_events();
        let before = connection.stats();
        let reply = match tokio::time::timeout(
            CONTROL_DEADLINE,
            wire::exchange(
                &connection,
                Hello {
                    version: 3,
                    share_id: session.membership().share_id,
                    operation: Operation::Roster,
                },
            ),
        )
        .await
        {
            Ok(Ok(reply)) => reply,
            Ok(Err(_)) => panic!("relay roster control exchange failed"),
            Err(_) => panic!("relay roster control exchange timed out"),
        };
        let roster = match reply {
            Reply::Roster { roster, .. } => roster,
            _ => panic!("relay control returned an unexpected reply"),
        };
        let membership = session.membership();
        assert!(
            roster
                .verify_for(membership.owner, membership.share_id, super::super::now())
                .is_ok(),
            "relay control roster authentication failed"
        );
        let roster_binding_valid = roster.member(membership.endpoint).is_some_and(|entry| {
            entry.permission == membership.permission && entry.member_epoch == membership.epoch
        });
        assert!(
            roster_binding_valid,
            "relay control roster membership binding failed"
        );
        let relay_bound_sockets_empty = session.state.transport.endpoint.bound_sockets().is_empty();
        let paths = connection.paths();
        let selected_relay = paths
            .iter()
            .any(|path| path.is_selected() && path.is_relay());
        let selected_ip = paths.iter().any(|path| path.is_selected() && path.is_ip());
        let path_has_relay = paths.iter().any(|path| path.is_relay());
        let path_has_ip = paths.iter().any(|path| path.is_ip());
        let tx_delta = connection
            .stats()
            .udp_tx
            .bytes
            .saturating_sub(before.udp_tx.bytes);
        let rx_delta = connection
            .stats()
            .udp_rx
            .bytes
            .saturating_sub(before.udp_rx.bytes);
        let mut opened_relay = false;
        let mut opened_ip = false;
        let mut selected_event_relay = false;
        let mut selected_event_ip = false;
        let mut event_count = 0;
        let mut saw_lagged = false;
        let event_deadline = Instant::now() + Duration::from_millis(250);
        while Instant::now() < event_deadline {
            let remaining = event_deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, events.next()).await {
                Ok(Some(PathEvent::Opened { remote_addr, .. })) => {
                    opened_relay |= remote_addr.is_relay();
                    opened_ip |= remote_addr.is_ip();
                    event_count += 1;
                }
                Ok(Some(PathEvent::Selected { remote_addr, .. })) => {
                    selected_event_relay |= remote_addr.is_relay();
                    selected_event_ip |= remote_addr.is_ip();
                    event_count += 1;
                }
                Ok(Some(PathEvent::Lagged { .. })) => saw_lagged = true,
                Ok(Some(_)) => event_count += 1,
                _ => break,
            }
        }
        connection.close(0u8.into(), b"internet experiment complete");
        RelayControlObservation {
            relay_bound_sockets_empty,
            selected_relay,
            selected_ip,
            path_has_relay,
            path_has_ip,
            opened_relay,
            opened_ip,
            selected_event_relay,
            selected_event_ip,
            saw_lagged,
            tx_delta,
            rx_delta,
            path_event_count: event_count,
            roster_binding_valid,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn silent_and_preview_connections_are_bounded_and_admission_recovers() {
        let test_name = "share::service::tests::silent_and_preview_connections_are_bounded_and_admission_recovers";
        if std::env::var("DW_ADMISSION_TIMEOUT_CHILD").ok().as_deref() != Some(test_name) {
            let profile = tempfile::tempdir().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", test_name, "--nocapture"])
                .env("DW_ADMISSION_TIMEOUT_CHILD", test_name)
                .env("HOME", profile.path())
                .env("USERPROFILE", profile.path())
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let owner_state = temp.path().join("owner");
        let prepared = root_admission::reserve_private(&owner_state).unwrap();
        root_admission::private_directory(&prepared).unwrap();
        let identity = load_or_create_identity(prepared.join("device.key")).unwrap();
        let endpoint = Endpoint::builder(presets::Minimal)
            .secret_key(identity.secret_key)
            .alpns(vec![ALPN_V3.to_vec(), ALPN_SWARM_V1.to_vec()])
            .bind()
            .await
            .unwrap();
        let owner = open_with_bound_test_endpoint_with_limit(
            &owner_state,
            NetworkMode::DirectOnly,
            endpoint,
            1,
        )
        .await
        .unwrap();
        let share = owner
            .create_owned_share(
                "bounded admission".into(),
                temp.path().join("owner-root"),
                temp.path().join("owner-state"),
                None,
                0,
            )
            .await
            .unwrap();
        let ticket = share
            .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
            .unwrap();

        let silent_peer = Endpoint::builder(presets::Minimal).bind().await.unwrap();
        let silent_connection = silent_peer
            .connect(owner.endpoint_addr(), ALPN_V3)
            .await
            .unwrap();
        let occupied_deadline = Instant::now() + Duration::from_secs(2);
        while owner.available_admission_slots() != 0 && Instant::now() < occupied_deadline {
            tokio::task::yield_now().await;
        }
        assert_eq!(owner.available_admission_slots(), 0);

        let reclaim_deadline = Instant::now() + CONTROL_DEADLINE + Duration::from_secs(2);
        while owner.available_admission_slots() == 0 && Instant::now() < reclaim_deadline {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(
            owner.available_admission_slots(),
            1,
            "silent peer did not release the bounded admission slot"
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(2), silent_connection.closed())
                .await
                .is_ok(),
            "silent peer connection was not explicitly closed after admission timeout"
        );
        silent_connection.close(0u8.into(), b"silent admission complete");
        silent_peer.close().await;

        let member = ShareService::open(temp.path().join("member"), NetworkMode::DirectOnly, None)
            .await
            .unwrap();
        let enrolled = member.enroll(&ticket, None).await.unwrap();
        assert_eq!(enrolled.permission, Permission::ReadWrite);

        let preview_peer = Endpoint::builder(presets::Minimal).bind().await.unwrap();
        let preview_connection = preview_peer
            .connect(owner.endpoint_addr(), ALPN_V3)
            .await
            .unwrap();
        let preview_reply = wire::exchange(
            &preview_connection,
            Hello {
                version: 3,
                share_id: share.config().share_id,
                operation: Operation::Validate(ticket),
            },
        )
        .await
        .unwrap();
        assert!(matches!(preview_reply, Reply::Validated(_)));
        let preview_deadline = Instant::now() + CONTROL_DEADLINE + Duration::from_secs(2);
        while owner.available_admission_slots() == 0 && Instant::now() < preview_deadline {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(
            owner.available_admission_slots(),
            1,
            "preview connection close wait was not bounded"
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(2), preview_connection.closed())
                .await
                .is_ok(),
            "preview connection was not explicitly closed after admission timeout"
        );
        preview_connection.close(0u8.into(), b"preview admission complete");
        preview_peer.close().await;

        member.shutdown().await.unwrap();
        owner.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn expired_session_admission_does_not_send_hello() {
        let server = Endpoint::builder(presets::Minimal)
            .alpns(vec![ALPN_V3.to_vec()])
            .clear_ip_transports()
            .bind_addr("127.0.0.1:0".parse::<SocketAddr>().unwrap())
            .unwrap()
            .bind()
            .await
            .unwrap();
        let server_socket = server
            .bound_sockets()
            .into_iter()
            .find(|socket| socket.is_ipv4())
            .expect("test server must expose an IPv4 socket");
        let server_address =
            EndpointAddr::from_parts(server.id(), [TransportAddr::Ip(server_socket)]);
        let server_task = tokio::spawn({
            let server = server.clone();
            async move {
                let incoming = server.accept().await.expect("client connection");
                let connection = incoming.await.expect("completed client handshake");
                tokio::time::timeout(Duration::from_secs(1), connection.accept_bi()).await
            }
        });

        let client = Endpoint::builder(presets::Minimal).bind().await.unwrap();
        let connection = client.connect(server_address, ALPN_V3).await.unwrap();
        let result = wire::open_session_until(
            &connection,
            ShareId([0; 32]),
            Instant::now().checked_sub(Duration::from_secs(1)).unwrap(),
        )
        .await;
        assert!(
            result.as_ref().err().is_some_and(
                |error| error.downcast_ref::<ShareError>() == Some(&ShareError::Offline)
            ),
            "an expired session admission must fail before exchange"
        );

        let server_stream_result = server_task.await.unwrap();
        assert!(
            server_stream_result.is_err(),
            "an expired session admission must not open a control stream"
        );
        connection.close(0u8.into(), b"expired admission test complete");
        client.close().await;
        server.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires external N0 lookup and relay services"]
    async fn actual_internet_n0_resume_and_relay_control_experiment() {
        let started = Instant::now();
        let temp = tempfile::tempdir().expect("experiment workspace");
        let owner_state = temp.path().join("owner");
        let owner = match ShareService::open(&owner_state, NetworkMode::Internet, None).await {
            Ok(owner) => owner,
            Err(_) => panic!("owner Internet service failed to start"),
        };
        assert!(
            owner.wait_online(Duration::from_secs(45)).await,
            "owner did not reach an N0 relay"
        );
        let owner_id = owner.endpoint_id();

        let member_state = temp.path().join("relay-member");
        let prepared = match root_admission::reserve_private(&member_state) {
            Ok(prepared) => prepared,
            Err(_) => panic!("member private namespace failed"),
        };
        if root_admission::private_directory(&prepared).is_err() {
            panic!("member private namespace preparation failed");
        }
        let member_key = match load_or_create_identity(prepared.join("device.key")) {
            Ok(identity) => identity.secret_key,
            Err(_) => panic!("member identity failed to load"),
        };
        let relay_endpoint = match Endpoint::builder(presets::N0)
            .secret_key(member_key)
            .alpns(vec![ALPN_V3.to_vec(), ALPN_SWARM_V1.to_vec()])
            .clear_ip_transports()
            .bind()
            .await
        {
            Ok(endpoint) => endpoint,
            Err(_) => panic!("relay-only endpoint failed to bind"),
        };
        let member = match open_with_bound_test_endpoint(
            &member_state,
            NetworkMode::Internet,
            relay_endpoint,
        )
        .await
        {
            Ok(member) => member,
            Err(_) => panic!("relay member service failed to start"),
        };
        assert!(
            member.wait_online(Duration::from_secs(45)).await,
            "relay member did not reach an N0 relay"
        );

        let owner_root = temp.path().join("owner-root");
        std::fs::create_dir_all(&owner_root).expect("owner root");
        std::fs::write(owner_root.join("control.txt"), b"D3 control exchange")
            .expect("owner fixture");
        let owner_share = match owner
            .create_owned_share(
                "Internet experiment".into(),
                owner_root,
                temp.path().join("owner-state"),
                None,
                0,
            )
            .await
        {
            Ok(share) => share,
            Err(_) => panic!("owner share failed to initialize"),
        };
        let share = owner_share.config().share_id;
        let ticket =
            match owner_share.issue_key(Permission::ReadWrite, None, relay_only_address(&owner)) {
                Ok(ticket) => ticket,
                Err(_) => panic!("owner relay ticket failed"),
            };
        let enrolled = match member.enroll(&ticket, None).await {
            Ok(member) => member,
            Err(_) => panic!("relay-only authenticated enrollment failed"),
        };
        let old_direct = direct_only_address(&owner);
        let session = match member.open_session(owner_id, share) {
            Ok(session) => session,
            Err(_) => panic!("member session failed to open"),
        };
        let lookup_deadline = Instant::now() + Duration::from_secs(60);
        let (lookup, _) =
            match wait_for_actual_n0_address(&session, owner_id, lookup_deadline).await {
                Ok(result) => result,
                Err(_) => panic!("actual N0 lookup did not return the owner"),
            };
        assert!(
            lookup.matched_endpoint,
            "N0 lookup endpoint identity mismatch"
        );
        assert!(
            matches!(
                lookup.first_source,
                Some(crate::AddressLookupSource::Pkarr | crate::AddressLookupSource::Dns)
            ),
            "N0 lookup had no pkarr or DNS provenance"
        );
        println!(
            "{{\"phase\":\"n0_lookup_initial\",\"elapsed_ms\":{},\"matched_endpoint\":{},\"pkarr_results\":{},\"dns_results\":{},\"first_source_pkarr\":{},\"first_source_dns\":{}}}",
            started.elapsed().as_millis(),
            lookup.matched_endpoint,
            lookup.pkarr_results,
            lookup.dns_results,
            matches!(lookup.first_source, Some(crate::AddressLookupSource::Pkarr)),
            matches!(lookup.first_source, Some(crate::AddressLookupSource::Dns)),
        );

        let first_relay = observed_relay_roster_control(&session).await;
        assert!(
            first_relay.relay_bound_sockets_empty,
            "relay-only endpoint unexpectedly has bound IP sockets"
        );
        assert!(
            first_relay.selected_relay && first_relay.path_has_relay,
            "relay-only control did not select a relay path"
        );
        assert!(!first_relay.selected_ip && !first_relay.path_has_ip);
        assert!(!first_relay.selected_event_ip && !first_relay.opened_ip);
        assert!(!first_relay.saw_lagged, "relay path event observer lagged");
        assert!(
            first_relay.tx_delta > 0 && first_relay.rx_delta > 0,
            "relay control produced no observed transport bytes"
        );
        print_relay_observation("relay_control_initial", started, first_relay);
        let owner_member_count = match owner_share.members() {
            Ok(members) => members.len(),
            Err(_) => panic!("owner member catalog read failed"),
        };
        session.close().await;
        drop(owner_share);
        if owner.shutdown().await.is_err() {
            panic!("owner offline transition failed");
        }

        let offline = tokio::time::timeout(
            Duration::from_secs(20),
            member.resume_membership(owner_id, share, old_direct.clone()),
        )
        .await;
        assert!(
            matches!(offline, Ok(Err(_))),
            "resume must remain bounded and fail while the owner is offline"
        );

        let owner = match ShareService::open(
            &owner_state,
            NetworkMode::Internet,
            Some("127.0.0.1:0".parse().expect("loopback bind")),
        )
        .await
        {
            Ok(owner) => owner,
            Err(_) => panic!("owner restart failed"),
        };
        let owner_share = match owner.load_owned_share(share).await {
            Ok(share) => share,
            Err(_) => panic!("owner share recovery failed"),
        };
        assert!(
            owner.wait_online(Duration::from_secs(45)).await,
            "restarted owner did not reach an N0 relay"
        );
        let new_direct = direct_only_address(&owner);
        assert!(
            new_direct != old_direct,
            "owner restart did not produce a changed direct address"
        );
        let probe = match member.open_session(owner_id, share) {
            Ok(session) => session,
            Err(_) => panic!("member probe session failed"),
        };
        let (restart_lookup, _) = match wait_for_actual_n0_address(
            &probe,
            owner_id,
            Instant::now() + Duration::from_secs(60),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => panic!("N0 lookup after owner restart failed"),
        };
        assert!(restart_lookup.matched_endpoint);
        assert!(
            restart_lookup.pkarr_results > 0 || restart_lookup.dns_results > 0,
            "restart lookup had no actual N0 provenance"
        );
        println!(
            "{{\"phase\":\"n0_lookup_after_restart\",\"elapsed_ms\":{},\"matched_endpoint\":{},\"pkarr_results\":{},\"dns_results\":{},\"first_source_pkarr\":{},\"first_source_dns\":{}}}",
            started.elapsed().as_millis(),
            restart_lookup.matched_endpoint,
            restart_lookup.pkarr_results,
            restart_lookup.dns_results,
            matches!(
                restart_lookup.first_source,
                Some(crate::AddressLookupSource::Pkarr)
            ),
            matches!(
                restart_lookup.first_source,
                Some(crate::AddressLookupSource::Dns)
            ),
        );
        probe.close().await;

        let resumed = match tokio::time::timeout(
            Duration::from_secs(30),
            member.resume_membership(owner_id, share, old_direct.clone()),
        )
        .await
        {
            Ok(Ok(member)) => member,
            _ => panic!("authenticated resume after owner address change failed"),
        };
        assert!(resumed.endpoint == enrolled.endpoint);
        assert!(resumed.owner == enrolled.owner);
        assert!(resumed.share_id == enrolled.share_id);
        assert!(resumed.permission == enrolled.permission);
        assert!(resumed.replica == enrolled.replica);
        assert!(resumed.enrolled_at == enrolled.enrolled_at);
        assert!(resumed.epoch == enrolled.epoch);
        let relationship = match member.relationships().ok().and_then(|relationships| {
            relationships
                .into_iter()
                .find(|relationship| relationship.membership.share_id == share)
        }) {
            Some(relationship) => relationship,
            None => panic!("resumed relationship was not persisted"),
        };
        assert!(relationship.address.id == owner_id);
        assert!(relationship.address != old_direct);

        let resumed_session = match member.open_session(owner_id, share) {
            Ok(session) => session,
            Err(_) => panic!("resumed member session failed"),
        };
        let roster = match resumed_session.refresh_roster().await {
            Ok(roster) => roster,
            Err(_) => panic!("authenticated roster refresh failed"),
        };
        assert!(
            roster
                .member(resumed.endpoint)
                .is_some_and(|entry| entry.permission == resumed.permission
                    && entry.member_epoch == resumed.epoch),
            "roster did not preserve the resumed membership binding"
        );
        let challenge = match resumed_session.roster_challenge() {
            Some(challenge) => challenge,
            None => panic!("owner did not issue a heartbeat challenge"),
        };
        if resumed_session.heartbeat(challenge).await.is_err() {
            panic!("authenticated heartbeat after resume failed");
        }
        let resumed_relay = observed_relay_roster_control(&resumed_session).await;
        assert!(resumed_relay.relay_bound_sockets_empty);
        assert!(resumed_relay.selected_relay && resumed_relay.path_has_relay);
        assert!(!resumed_relay.selected_ip && !resumed_relay.path_has_ip);
        assert!(!resumed_relay.selected_event_ip && !resumed_relay.opened_ip);
        assert!(!resumed_relay.saw_lagged);
        assert!(resumed_relay.tx_delta > 0 && resumed_relay.rx_delta > 0);
        print_relay_observation("relay_control_after_resume", started, resumed_relay);
        assert!(
            owner_share
                .members()
                .is_ok_and(|members| members.len() == owner_member_count),
            "resume changed the owner membership count"
        );
        println!(
            "{{\"phase\":\"resume_binding\",\"elapsed_ms\":{},\"same_identity\":true,\"address_changed\":true,\"permission_epoch_preserved\":true,\"replica_preserved\":true,\"enrolled_at_preserved\":true,\"owner_member_count_preserved\":true,\"roster_binding_preserved\":{}}}",
            started.elapsed().as_millis(),
            resumed_relay.roster_binding_valid,
        );
        resumed_session.close().await;
        drop(owner_share);
        if owner.shutdown().await.is_err() {
            panic!("restarted owner shutdown failed");
        }
        if member.shutdown().await.is_err() {
            panic!("relay member shutdown failed");
        }
    }

    async fn roster_exchange(
        client: &ShareService,
        owner: &ShareService,
        share: ShareId,
        operation: Operation,
    ) -> Result<Reply> {
        let connection = client
            .router
            .endpoint()
            .connect(owner.endpoint_addr(), ALPN_V3)
            .await?;
        let result = wire::exchange(
            &connection,
            Hello {
                version: 3,
                share_id: share,
                operation,
            },
        )
        .await;
        connection.close(0u8.into(), b"roster test complete");
        result
    }

    #[test]
    fn activation_lease_keeps_request_start_deadline_and_reports_reduced_time() {
        let owner = SecretKey::generate();
        let provider = SecretKey::generate();
        let reply = super::super::authority::ActivateGrantReply::sign(
            &owner,
            ShareId([0x31; 32]),
            provider.public(),
            [0x32; 32],
            [0x33; 16],
            true,
            10,
        )
        .unwrap();
        let request_started = Instant::now();
        let reply_received = request_started + Duration::from_secs(4);
        let lease = ActivationLease::from_reply_at(reply, request_started, reply_received).unwrap();

        assert_eq!(
            lease.deadline,
            request_started + Duration::from_secs(10),
            "the response must not start a fresh activation lease"
        );
        assert_eq!(
            lease.deadline.saturating_duration_since(reply_received),
            Duration::from_secs(6),
            "a delayed response must retain only the remaining request lifetime"
        );
    }

    #[test]
    fn activation_lease_rejects_a_reply_that_arrived_after_request_deadline() {
        let owner = SecretKey::generate();
        let provider = SecretKey::generate();
        let reply = super::super::authority::ActivateGrantReply::sign(
            &owner,
            ShareId([0x41; 32]),
            provider.public(),
            [0x42; 32],
            [0x43; 16],
            true,
            15,
        )
        .unwrap();
        let request_started = Instant::now();
        let error = ActivationLease::from_reply_at(
            reply,
            request_started,
            request_started + Duration::from_secs(16),
        )
        .expect_err("a delayed activation reply must not be revived");

        assert_eq!(
            error.downcast_ref::<ShareError>(),
            Some(&ShareError::GrantExpired)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn activation_deadline_blocks_delayed_lookup_before_owner_activation() {
        let name = "share::service::tests::activation_deadline_blocks_delayed_lookup_before_owner_activation";
        if std::env::var("DW_ACTIVATION_DEADLINE_CHILD")
            .ok()
            .as_deref()
            != Some(name)
        {
            let profile = tempfile::tempdir().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", name, "--nocapture"])
                .env("DW_ACTIVATION_DEADLINE_CHILD", name)
                .env("HOME", profile.path())
                .env("USERPROFILE", profile.path())
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }

        let temp = tempfile::tempdir().unwrap();
        let owner = ShareService::open(
            temp.path().join("owner-service"),
            NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let consumer = ShareService::open(
            temp.path().join("consumer-service"),
            NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let provider = ShareService::open(
            temp.path().join("provider-service"),
            NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        std::fs::create_dir_all(temp.path().join("shared-root")).unwrap();
        std::fs::write(temp.path().join("shared-root/deadline.txt"), b"deadline").unwrap();
        let owner_share = owner
            .create_owned_share(
                "Deadline".into(),
                temp.path().join("shared-root"),
                temp.path().join("shared-state"),
                None,
                0,
            )
            .await
            .unwrap();
        let share = owner_share.config().share_id;
        let consumer_ticket = owner_share
            .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
            .unwrap();
        let provider_ticket = owner_share
            .issue_key(Permission::ReadOnly, None, owner.endpoint_addr())
            .unwrap();
        let consumer_membership = consumer.enroll(&consumer_ticket, None).await.unwrap();
        let provider_membership = provider.enroll(&provider_ticket, None).await.unwrap();
        assert_eq!(provider_membership.permission, Permission::ReadOnly);
        let consumer_session = consumer.open_session(owner.endpoint_id(), share).unwrap();
        let provider_membership_session =
            provider.open_session(owner.endpoint_id(), share).unwrap();
        provider_membership_session.refresh_roster().await.unwrap();
        let provider_challenge = provider_membership_session.roster_challenge().unwrap();
        provider_membership_session
            .heartbeat(provider_challenge)
            .await
            .unwrap();
        let provider_membership_for_session = provider_membership_session.membership().clone();
        let snapshot = consumer_session
            .fetch_authoritative_snapshot(&MerkleTree::from_records(Vec::new()).unwrap())
            .await
            .unwrap();
        assert_eq!(snapshot.token.epoch, consumer_membership.epoch);
        let record = snapshot
            .records
            .first()
            .cloned()
            .unwrap_or_else(|| panic!("deadline fixture requires an authoritative record"));
        let manifest = consumer_session
            .request_manifest(&snapshot.token, &record)
            .await
            .unwrap();
        let hashes: Vec<_> = manifest
            .manifest
            .chunks
            .iter()
            .map(|chunk| chunk.hash)
            .collect();
        let grant = consumer_session
            .request_swarm_grant(provider.endpoint_id(), &snapshot.token, &manifest, &hashes)
            .await
            .unwrap();
        assert_eq!(grant.provider_epoch, provider_membership.epoch);

        // Remove the provider's cached owner path before the delayed lookup
        // probe. Reopening the same persistent identity keeps the membership
        // binding while ensuring the stale address is genuinely exercised.
        provider_membership_session.close().await;
        provider.shutdown().await.unwrap();
        let provider_service_path = temp.path().join("provider-service");
        let provider =
            ShareService::open(provider_service_path.clone(), NetworkMode::DirectOnly, None)
                .await
                .unwrap();

        // The session intentionally has an unusable persisted hint. The test
        // enables the Internet lookup path on this otherwise DirectOnly
        // fixture and installs a resolver that never produces an address.
        // The short caller deadline must expire before any Activate frame is
        // sent, so the owner grant remains Issued rather than Active.
        let stale = EndpointAddr::from_parts(
            owner.endpoint_id(),
            [iroh::TransportAddr::Ip("192.0.2.1:9".parse().unwrap())],
        );
        provider
            .router
            .endpoint()
            .address_lookup()
            .unwrap()
            .add(HangingAddressLookup);
        let provider_session = ShareSession {
            state: Arc::new(ShareSessionState {
                registry: provider.registry.clone(),
                membership: provider_membership_for_session,
                active: provider.active.clone(),
                tasks: provider.swarm_tasks.clone(),
                roster_challenge: Mutex::new(None),
                roster: Mutex::new(RosterCache::default()),
                roster_gate: tokio::sync::Mutex::new(()),
                transport: SyncSession {
                    client: SyncClient {
                        secret_key: provider.key.clone(),
                        remote: stale.clone(),
                        network_mode: NetworkMode::Internet,
                    },
                    endpoint: provider.router.endpoint().clone(),
                    share: Some(share),
                    remote: Arc::new(std::sync::RwLock::new(stale)),
                    fallback_endpoint: Some(owner.endpoint_id()),
                    observation: Arc::new(std::sync::RwLock::new(None)),
                    n0_lookup: Arc::new(std::sync::RwLock::new(None)),
                },
                share_events: Arc::new(ShareEventState::new()),
            }),
        };
        assert_eq!(
            owner
                .registry
                .active_grant_blockers(share, provider.endpoint_id())
                .unwrap(),
            0
        );
        let started = Instant::now();
        let result = provider_session
            .activate_grant_until(&grant, started, started + Duration::from_millis(100))
            .await;
        let error = match result {
            Ok(_) => panic!("a lookup that outlives the caller deadline cannot activate"),
            Err(error) => error,
        };
        assert!(
            matches!(
                error.downcast_ref::<ShareError>(),
                Some(ShareError::Offline | ShareError::GrantExpired)
            ),
            "deadline failure must remain a safe offline/expired result"
        );
        assert_eq!(
            owner
                .registry
                .active_grant_blockers(share, provider.endpoint_id())
                .unwrap(),
            0,
            "no Activate frame may create an owner Active row after lookup expiry"
        );

        consumer_session.close().await;
        provider_session.close().await;
        drop(owner_share);
        provider.shutdown().await.unwrap();
        consumer.shutdown().await.unwrap();
        owner.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn authenticated_roster_heartbeat_updates_address_and_rejects_replay() {
        let name = "share::service::tests::authenticated_roster_heartbeat_updates_address_and_rejects_replay";
        if std::env::var("DW_ROSTER_CHILD").ok().as_deref() != Some(name) {
            let profile = tempfile::tempdir().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", name, "--nocapture"])
                .env("DW_ROSTER_CHILD", name)
                .env("HOME", profile.path())
                .env("USERPROFILE", profile.path())
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }

        let temp = tempfile::tempdir().unwrap();
        let owner = ShareService::open(temp.path().join("owner"), NetworkMode::DirectOnly, None)
            .await
            .unwrap();
        let member = ShareService::open(temp.path().join("member"), NetworkMode::DirectOnly, None)
            .await
            .unwrap();
        let outsider =
            ShareService::open(temp.path().join("outsider"), NetworkMode::DirectOnly, None)
                .await
                .unwrap();
        let owner_share = owner
            .create_owned_share(
                "Files".into(),
                temp.path().join("root"),
                temp.path().join("state"),
                None,
                0,
            )
            .await
            .unwrap();
        let share = owner_share.config().share_id;
        let ticket = owner_share
            .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
            .unwrap();
        let membership = member.enroll(&ticket, None).await.unwrap();
        let session = member.open_session(owner.endpoint_id(), share).unwrap();

        let roster = session.refresh_roster().await.unwrap();
        let observation = session
            .transport_observation()
            .expect("control connection records a transport observation");
        assert_eq!(
            observation.provenance,
            crate::LookupProvenance::PersistedAddress,
            "DirectOnly fixture should use the persisted address hint"
        );
        roster
            .verify_for(owner.endpoint_id(), share, super::super::now())
            .unwrap();
        let entry = roster.member(member.endpoint_id()).unwrap();
        assert_eq!(entry.permission, membership.permission);
        assert_eq!(entry.member_epoch, membership.epoch);
        assert_eq!(entry.heartbeat_at, 0);
        assert_eq!(entry.heartbeat_expires_at, 0);
        assert!(!roster.member_is_fresh(member.endpoint_id(), super::super::now()));
        let first_challenge = session.roster_challenge().unwrap();
        session.heartbeat(first_challenge).await.unwrap();

        let stored = owner.registry.stored_roster(share).unwrap().unwrap();
        stored
            .verify_for(owner.endpoint_id(), share, super::super::now())
            .unwrap();
        let first_entry = stored.member(member.endpoint_id()).unwrap();
        assert_eq!(first_entry.address.id, member.endpoint_id());
        assert!(first_entry.heartbeat_at > 0);
        assert!(first_entry.heartbeat_expires_at > first_entry.heartbeat_at);
        assert!(stored.member_is_fresh(member.endpoint_id(), super::super::now()));

        let _ = session.refresh_roster().await.unwrap();
        let second_challenge = session.roster_challenge().unwrap();
        let receive_floor = super::super::now();
        let changed_address = EndpointAddr::from_parts(
            member.endpoint_id(),
            [iroh::TransportAddr::Ip("127.0.0.1:39999".parse().unwrap())],
        );
        let changed = RosterHeartbeat::sign(
            &member.key,
            owner.endpoint_id(),
            share,
            changed_address.clone(),
            second_challenge,
            receive_floor.saturating_sub(4),
        );
        let reply = roster_exchange(
            &member,
            &owner,
            share,
            Operation::Heartbeat(changed.clone()),
        )
        .await
        .unwrap();
        assert!(matches!(reply, Reply::Heartbeat(_)));
        let changed_stored = owner.registry.stored_roster(share).unwrap().unwrap();
        assert!(
            changed_stored
                .member(member.endpoint_id())
                .unwrap()
                .heartbeat_at
                >= receive_floor,
            "owner receive time must anchor heartbeat liveness"
        );
        assert_eq!(
            changed_stored.member(member.endpoint_id()).unwrap().address,
            changed_address
        );

        let replay =
            match roster_exchange(&member, &owner, share, Operation::Heartbeat(changed)).await {
                Ok(_) => panic!("replayed heartbeat was accepted"),
                Err(error) => error,
            };
        assert_eq!(
            replay.downcast_ref::<ShareError>(),
            Some(&ShareError::HeartbeatReplay)
        );

        let bad_share = ShareId([0x55; 32]);
        let cross_share = RosterHeartbeat::sign(
            &member.key,
            owner.endpoint_id(),
            bad_share,
            member.endpoint_addr(),
            [7; 32],
            super::super::now(),
        );
        let cross_share_error = match roster_exchange(
            &member,
            &owner,
            share,
            Operation::Heartbeat(cross_share),
        )
        .await
        {
            Ok(_) => panic!("cross-share heartbeat was accepted"),
            Err(error) => error,
        };
        assert_eq!(
            cross_share_error.downcast_ref::<ShareError>(),
            Some(&ShareError::OwnerMismatch)
        );

        let outsider_error =
            match roster_exchange(&outsider, &owner, share, Operation::Roster).await {
                Ok(_) => panic!("non-member roster request was accepted"),
                Err(error) => error,
            };
        assert_eq!(
            outsider_error.downcast_ref::<ShareError>(),
            Some(&ShareError::NotMember)
        );

        drop(session);
        drop(owner_share);
        outsider.shutdown().await.unwrap();
        member.shutdown().await.unwrap();
        owner.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn resume_membership_uses_endpoint_id_lookup_after_address_change() {
        let name =
            "share::service::tests::resume_membership_uses_endpoint_id_lookup_after_address_change";
        if std::env::var("DW_RESUME_LOOKUP_CHILD").ok().as_deref() != Some(name) {
            let profile = tempfile::tempdir().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", name, "--nocapture"])
                .env("DW_RESUME_LOOKUP_CHILD", name)
                .env("HOME", profile.path())
                .env("USERPROFILE", profile.path())
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }

        let temp = tempfile::tempdir().unwrap();
        let owner = ShareService::open(
            temp.path().join("owner-service"),
            NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let member_path = temp.path().join("member-service");
        let member = ShareService::open(&member_path, NetworkMode::Internet, None)
            .await
            .unwrap();
        let root = temp.path().join("shared-root");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("resume.txt"), b"resume").unwrap();
        let owner_share = owner
            .create_owned_share(
                "Resume".into(),
                root,
                temp.path().join("shared-state"),
                None,
                0,
            )
            .await
            .unwrap();
        let share = owner_share.config().share_id;
        let ticket = owner_share
            .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
            .unwrap();
        let expected = member.enroll(&ticket, None).await.unwrap();
        let stale = EndpointAddr::from_parts(
            owner.endpoint_id(),
            [iroh::TransportAddr::Ip("192.0.2.1:9".parse().unwrap())],
        );

        // Reopen the same device identity to remove any cached owner path.
        member.shutdown().await.unwrap();
        let member = ShareService::open(&member_path, NetworkMode::Internet, None)
            .await
            .unwrap();
        let lookup = MemoryLookup::with_provenance("pkarr");
        lookup.set_endpoint_info(owner.endpoint_addr());
        member
            .router
            .endpoint()
            .address_lookup()
            .unwrap()
            .add(lookup);

        let resumed = member
            .resume_membership(owner.endpoint_id(), share, stale.clone())
            .await
            .unwrap();
        assert!(resumed.permission == expected.permission);
        assert!(resumed.replica == expected.replica);
        assert!(resumed.epoch == expected.epoch);
        assert!(resumed.enrolled_at == expected.enrolled_at);
        let relationship = member
            .relationships()
            .unwrap()
            .into_iter()
            .find(|relationship| {
                relationship.membership.owner == owner.endpoint_id()
                    && relationship.membership.share_id == share
            })
            .expect("resume must retain the active relationship");
        assert!(relationship.address.id == owner.endpoint_id());
        assert!(relationship.address != stale);
        assert!(
            relationship.address.addrs.iter().next().is_some(),
            "resume must persist a usable discovered address"
        );

        drop(owner_share);
        member.shutdown().await.unwrap();
        owner.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn authority_control_round_trip_binds_snapshot_manifest_grant_and_apply() {
        let name = "share::service::tests::authority_control_round_trip_binds_snapshot_manifest_grant_and_apply";
        if std::env::var("DW_AUTHORITY_CHILD").ok().as_deref() != Some(name) {
            let profile = tempfile::tempdir().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", name, "--nocapture"])
                .env("DW_AUTHORITY_CHILD", name)
                .env("HOME", profile.path())
                .env("USERPROFILE", profile.path())
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let owner = ShareService::open(
            temp.path().join("owner-service"),
            NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let member = ShareService::open(
            temp.path().join("member-service"),
            NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let observed_events = Arc::new(Mutex::new(Vec::<ShareTransferEvent>::new()));
        let observer = ShareTransferObserver::new({
            let observed_events = observed_events.clone();
            move |event| {
                observed_events
                    .lock()
                    .expect("share event mutex")
                    .push(event);
            }
        });
        owner.set_share_observer(Some(observer.clone()));
        member.set_share_observer(Some(observer));
        let root = temp.path().join("shared-root");
        let state = temp.path().join("shared-state");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("authority.txt"), b"authority payload").unwrap();
        let owner_share = owner
            .create_owned_share("Authority".into(), root.clone(), state, None, 0)
            .await
            .unwrap();
        let share = owner_share.config().share_id;
        let ticket = owner_share
            .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
            .unwrap();
        let membership = member.enroll(&ticket, None).await.unwrap();
        let session = member.open_session(owner.endpoint_id(), share).unwrap();

        let empty = MerkleTree::from_records(Vec::new()).unwrap();
        let snapshot = session.fetch_authoritative_snapshot(&empty).await.unwrap();
        assert_eq!(snapshot.token.epoch, membership.epoch);
        let record = snapshot
            .records
            .iter()
            .find(|record| record.path.as_str() == "authority.txt")
            .cloned()
            .unwrap();
        let manifest = session
            .request_manifest(&snapshot.token, &record)
            .await
            .unwrap();
        let hashes: Vec<_> = manifest
            .manifest
            .chunks
            .iter()
            .map(|chunk| chunk.hash)
            .collect();
        // Manifest authority must make the owner a real verified supplier.
        // This assertion deliberately runs before issuing the owner grant, so
        // the provider path cannot be made green by manually warming the CAS.
        for descriptor in &manifest.manifest.chunks {
            assert!(
                owner_share.runtime.store.chunks().contains(descriptor.hash),
                "authoritative manifest must ingest owner chunks"
            );
        }
        let grant = session
            .request_swarm_grant(owner.endpoint_id(), &snapshot.token, &manifest, &hashes)
            .await
            .unwrap();
        assert_eq!(grant.provider, owner.endpoint_id());
        assert_eq!(grant.consumer, member.endpoint_id());
        assert_eq!(grant.provider_epoch, 0);
        let consumer_store = Arc::new(Store::open(temp.path().join("consumer-store")).unwrap());
        let transfer = session
            .fetch_swarm_chunks(
                consumer_store.clone(),
                &grant,
                &snapshot.token,
                &record,
                &manifest,
                &hashes,
                [0x72; 16],
            )
            .await
            .unwrap();
        assert!(transfer.verified);
        assert_eq!(usize::from(transfer.transferred_chunks), hashes.len());
        assert!(transfer.transferred_bytes > 0);
        for hash in hashes.iter().copied() {
            assert!(consumer_store.chunks().contains(hash));
        }
        let events = observed_events.lock().expect("share event mutex").clone();
        assert!(events.iter().any(|event| {
            event.phase == SharePhase::Query
                && event.direction == TransferDirection::Outbound
                && event.peer == owner.endpoint_id()
        }));
        assert!(events.iter().any(|event| {
            event.phase == SharePhase::Manifest
                && event.direction == TransferDirection::Outbound
                && event.peer == owner.endpoint_id()
        }));
        assert!(events.iter().any(|event| {
            event.phase == SharePhase::Grant
                && event.direction == TransferDirection::Outbound
                && event.peer == owner.endpoint_id()
        }));
        let inbound_bytes: u64 = events
            .iter()
            .filter(|event| {
                event.operation_id == [0x72; 16]
                    && event.phase == SharePhase::Swarm
                    && event.direction == TransferDirection::Inbound
            })
            .map(|event| event.bytes)
            .sum();
        assert_eq!(inbound_bytes, transfer.transferred_bytes);
        assert!(events.iter().any(|event| {
            event.operation_id == [0x72; 16]
                && event.phase == SharePhase::Drain
                && event.direction == TransferDirection::Inbound
        }));
        assert!(events.iter().any(|event| {
            event.operation_id == [0x72; 16]
                && event.phase == SharePhase::Done
                && event.direction == TransferDirection::Inbound
                && event.grant == Some(grant.nonce)
        }));
        // A local binding failure happens before an authenticated control
        // exchange. It must not create a query/active-peer event (or a
        // terminal event for an operation that was never admitted).
        let event_count = events.len();
        let mut wrong_epoch = snapshot.token.clone();
        wrong_epoch.epoch = wrong_epoch.epoch.saturating_add(1);
        let error = session
            .request_manifest(&wrong_epoch, &record)
            .await
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<ShareError>(),
            Some(&ShareError::EpochMismatch)
        );
        assert_eq!(
            observed_events.lock().expect("share event mutex").len(),
            event_count,
            "preflight rejection must not inflate authenticated operation telemetry"
        );
        let permit = session.revalidate(&snapshot.token).await.unwrap();
        let operation_id = [0x71; 16];
        let prepared = session
            .apply_status(&permit, Some(operation_id))
            .await
            .unwrap();
        assert!(matches!(
            prepared.state,
            super::super::ApplyStateView::Prepared
        ));
        assert_eq!(prepared.operation_id, None);
        session.apply_start(&permit, operation_id).await.unwrap();
        let started = session
            .apply_status(&permit, Some(operation_id))
            .await
            .unwrap();
        assert!(matches!(
            started.state,
            super::super::ApplyStateView::Started
        ));
        assert_eq!(started.operation_id, Some(operation_id));
        session
            .apply_drained(&permit, operation_id, true)
            .await
            .unwrap();
        let drained = session
            .apply_status(&permit, Some(operation_id))
            .await
            .unwrap();
        assert!(matches!(
            drained.state,
            super::super::ApplyStateView::Drained
        ));
        assert!(drained.committed);
        assert!(matches!(
            owner_share
                .revoke_member_strong(member.endpoint_id())
                .await
                .unwrap(),
            super::super::RevocationReceipt::Complete { .. }
        ));
        let revoked_event_start = observed_events.lock().expect("share event mutex").len();
        let revoked_error = session
            .fetch_authoritative_snapshot(&empty)
            .await
            .unwrap_err();
        assert_eq!(
            revoked_error.downcast_ref::<ShareError>(),
            Some(&ShareError::MemberRevoked)
        );
        let has_revoked_query = {
            let events = observed_events.lock().expect("share event mutex");
            events[revoked_event_start..]
                .iter()
                .any(|event| event.phase == SharePhase::Query)
        };
        let has_revoked_reject = {
            let events = observed_events.lock().expect("share event mutex");
            events[revoked_event_start..]
                .iter()
                .any(|event| event.phase == SharePhase::Reject)
        };
        assert!(has_revoked_query);
        assert!(has_revoked_reject);

        // Once the owner endpoint is closed, cancelling an in-flight control
        // query must not leave an active-peer event behind.  The timeout is
        // only a cancellation boundary; the assertion relies on the event
        // count, rather than on a sleep or a guessed transport error.
        let event_count = observed_events.lock().expect("share event mutex").len();
        owner.shutdown().await.unwrap();
        let cancelled = tokio::time::timeout(
            Duration::from_millis(250),
            session.fetch_authoritative_snapshot(&empty),
        )
        .await;
        assert!(
            cancelled.is_err() || cancelled.expect("timeout result").is_err(),
            "closed owner must not complete an authoritative query"
        );
        assert_eq!(
            observed_events.lock().expect("share event mutex").len(),
            event_count,
            "offline/cancelled query must not inflate authenticated telemetry"
        );
        session.close().await;
        drop(owner_share);
        member.shutdown().await.unwrap();
    }

    #[test]
    fn activation_recovery_binding_survives_permission_epoch_change() {
        let owner = SecretKey::generate();
        let member = SecretKey::generate();
        let share = ShareId([0xb1; 32]);
        let membership = Membership {
            share_id: share,
            owner: owner.public(),
            endpoint: member.public(),
            permission: Permission::ReadWrite,
            replica: ReplicaId(Hash32::digest(b"recovery-replica")),
            enrolled_at: 1,
            revoked_at: Some(2),
            epoch: 2,
        };
        let grant = ShareGrant::sign(
            &owner,
            share,
            member.public(),
            owner.public(),
            1,
            0,
            [0xb2; 32],
            Hash32::digest(b"recovery-manifest"),
            Hash32::digest(b"recovery-request"),
            [0xb3; 32],
            now(),
        )
        .unwrap();

        // A revoked/advanced local membership can still query or cancel the
        // old activation binding for drain recovery, while a new data
        // activation remains rejected by the current epoch gate.
        assert!(ShareSession::ensure_recovery_binding_for(&membership, &grant).is_ok());
        assert_eq!(
            ShareSession::ensure_activation_binding_for(&membership, &grant)
                .unwrap_err()
                .downcast_ref::<ShareError>(),
            Some(&ShareError::EpochMismatch)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn read_only_provider_grant_activation_requires_bilateral_drain() {
        let name =
            "share::service::tests::read_only_provider_grant_activation_requires_bilateral_drain";
        if std::env::var("DW_RO_PROVIDER_CHILD").ok().as_deref() != Some(name) {
            let profile = tempfile::tempdir().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", name, "--nocapture"])
                .env("DW_RO_PROVIDER_CHILD", name)
                .env("HOME", profile.path())
                .env("USERPROFILE", profile.path())
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }

        let temp = tempfile::tempdir().unwrap();
        let owner = ShareService::open(
            temp.path().join("owner-service"),
            NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let consumer = ShareService::open(
            temp.path().join("consumer-service"),
            NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let provider = ShareService::open(
            temp.path().join("provider-service"),
            NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let root = temp.path().join("shared-root");
        let state = temp.path().join("shared-state");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("provider.txt"), b"provider payload").unwrap();
        let owner_share = owner
            .create_owned_share("Provider".into(), root.clone(), state, None, 0)
            .await
            .unwrap();
        let share = owner_share.config().share_id;
        let consumer_ticket = owner_share
            .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
            .unwrap();
        let provider_ticket = owner_share
            .issue_key(Permission::ReadOnly, None, owner.endpoint_addr())
            .unwrap();
        let consumer_membership = consumer.enroll(&consumer_ticket, None).await.unwrap();
        let provider_membership = provider.enroll(&provider_ticket, None).await.unwrap();
        assert_eq!(provider_membership.permission, Permission::ReadOnly);

        // Build the provider's managed storage from the exact admission lease
        // and handles that the supplier registration retains.  This is a
        // member-provider fixture, not a second endpoint or a reopened CAS.
        let provider_root = temp.path().join("provider-root");
        let provider_state =
            root_admission::reserve_private(temp.path().join("provider-state")).unwrap();
        let (provider_root, provider_state) =
            prepare_server_roots(&provider_root, &provider_state).unwrap();
        let provider_lease = Arc::new(
            root_admission::acquire_with_private(
                &provider_root,
                RootUse::Managed {
                    share: share.0,
                    owner: *owner.endpoint_id().as_bytes(),
                },
                std::slice::from_ref(&provider_state),
            )
            .unwrap(),
        );
        let provider_index = Arc::new(
            LocalIndex::open(
                &provider_root,
                provider_state.join("index.redb"),
                provider_membership.replica,
                IndexOptions::default(),
            )
            .unwrap(),
        );
        let provider_store = Arc::new(
            Store::open_with_recovery_reserver(&provider_state, |path| {
                root_admission::reserve_private(path)
            })
            .unwrap(),
        );
        let provider_bytes = std::fs::read(root.join("provider.txt")).unwrap();
        let provider_guard = provider
            .register_supplier_storage(
                owner.endpoint_id(),
                share,
                &provider_membership,
                &provider_root,
                provider_lease,
                provider_index,
                provider_store.clone(),
            )
            .unwrap();
        assert_eq!(
            provider_guard.private_root(),
            fs::canonicalize(&provider_state).unwrap().as_path()
        );

        let consumer_session = consumer.open_session(owner.endpoint_id(), share).unwrap();
        let provider_session = provider.open_session(owner.endpoint_id(), share).unwrap();
        let _ = provider_session.refresh_roster().await.unwrap();
        let provider_challenge = provider_session.roster_challenge().unwrap();
        provider_session
            .heartbeat(provider_challenge)
            .await
            .unwrap();

        let empty = MerkleTree::from_records(Vec::new()).unwrap();
        let snapshot = consumer_session
            .fetch_authoritative_snapshot(&empty)
            .await
            .unwrap();
        assert_eq!(snapshot.token.epoch, consumer_membership.epoch);
        let record = snapshot
            .records
            .iter()
            .find(|record| record.path.as_str() == "provider.txt")
            .cloned()
            .unwrap();
        let manifest = consumer_session
            .request_manifest(&snapshot.token, &record)
            .await
            .unwrap();
        let hashes: Vec<_> = manifest
            .manifest
            .chunks
            .iter()
            .map(|chunk| chunk.hash)
            .collect();
        for descriptor in &manifest.manifest.chunks {
            let begin = usize::try_from(descriptor.offset).unwrap();
            let end = begin + usize::try_from(descriptor.length).unwrap();
            provider_store
                .chunks()
                .put_verified(descriptor.hash, &provider_bytes[begin..end])
                .unwrap();
        }
        let payload_grant = consumer_session
            .request_swarm_grant(provider.endpoint_id(), &snapshot.token, &manifest, &hashes)
            .await
            .unwrap();
        let consumer_store = Arc::new(Store::open(temp.path().join("consumer-store")).unwrap());
        let transfer = consumer_session
            .fetch_swarm_chunks(
                consumer_store.clone(),
                &payload_grant,
                &snapshot.token,
                &record,
                &manifest,
                &hashes,
                [0xd2; 16],
            )
            .await
            .unwrap();
        assert!(transfer.verified);
        assert_eq!(usize::from(transfer.transferred_chunks), hashes.len());
        assert!(transfer.transferred_bytes > 0);
        for hash in hashes.iter().copied() {
            assert!(consumer_store.chunks().contains(hash));
        }

        // A close marker alone is not enough to permit replacement: the
        // original generation still owns the exact storage Arcs until its
        // awaited drain removes the map entry.  This catches a re-register
        // race that could otherwise overlap two suppliers on one root.
        let duplicate = provider.register_supplier_storage(
            owner.endpoint_id(),
            share,
            provider_guard.membership(),
            &provider_root,
            provider_guard.root_lease().clone(),
            provider_guard.index().clone(),
            provider_guard.store().clone(),
        );
        assert_eq!(
            duplicate.unwrap_err().downcast_ref::<ShareError>(),
            Some(&ShareError::Busy)
        );

        let grant = consumer_session
            .request_swarm_grant(provider.endpoint_id(), &snapshot.token, &manifest, &hashes)
            .await
            .unwrap();
        assert_eq!(grant.provider_epoch, provider_membership.epoch);
        let pending_grant = consumer_session
            .request_swarm_grant(provider.endpoint_id(), &snapshot.token, &manifest, &hashes)
            .await
            .unwrap();
        let denied = provider_session
            .cancel_activation(&pending_grant, None, [0xc1; 16])
            .await
            .unwrap();
        assert!(matches!(denied.state, ActivationStateView::Denied));
        let denied_retry = provider_session
            .cancel_activation(&pending_grant, None, [0xc2; 16])
            .await
            .unwrap();
        assert!(matches!(denied_retry.state, ActivationStateView::Denied));
        let activation = provider_session.activate_grant(&grant).await.unwrap();
        let active = provider_session
            .activation_status(&grant, Some(activation.reply.activation_id))
            .await
            .unwrap();
        assert!(matches!(active.state, ActivationStateView::Active));
        assert!(!active.provider_drained && !active.consumer_drained);
        let late_cancel = provider_session
            .cancel_activation(&grant, Some(activation.reply.activation_id), [0xc3; 16])
            .await
            .unwrap();
        assert!(matches!(late_cancel.state, ActivationStateView::Active));
        assert_eq!(
            late_cancel.activation_id,
            Some(activation.reply.activation_id)
        );
        assert!(!late_cancel.provider_drained && !late_cancel.consumer_drained);

        let first_revoke = owner_share
            .revoke_member_strong(provider.endpoint_id())
            .await
            .unwrap();
        assert!(matches!(
            first_revoke,
            super::super::RevocationReceipt::Pending { blockers: 1, .. }
        ));
        // Drain acknowledgements remain admissible after the owner closes new
        // runtime admission during revoke/pause.
        owner_share.pause().await;

        consumer_session
            .grant_drained(&grant, activation.reply.activation_id)
            .await
            .unwrap();
        let one_sided = consumer_session
            .activation_status(&grant, Some(activation.reply.activation_id))
            .await
            .unwrap();
        assert!(matches!(one_sided.state, ActivationStateView::Active));
        assert!(!one_sided.provider_drained && one_sided.consumer_drained);
        assert!(matches!(
            owner
                .registry
                .revocation_receipt(share, provider.endpoint_id())
                .unwrap(),
            super::super::RevocationReceipt::Pending { blockers: 1, .. }
        ));
        provider_session
            .grant_drained(&grant, activation.reply.activation_id)
            .await
            .unwrap();
        let drained = provider_session
            .activation_status(&grant, Some(activation.reply.activation_id))
            .await
            .unwrap();
        assert!(matches!(drained.state, ActivationStateView::Drained));
        assert!(drained.provider_drained && drained.consumer_drained);
        assert!(matches!(
            owner_share
                .revoke_member_strong(provider.endpoint_id())
                .await
                .unwrap(),
            super::super::RevocationReceipt::Complete { .. }
        ));

        provider_guard.drain().await.unwrap();
        assert!(
            !provider
                .suppliers
                .read()
                .expect("supplier map")
                .contains_key(&(owner.endpoint_id(), share))
        );

        // Once the first generation has fully drained and been removed, a
        // replacement may use the same admitted storage handles safely.
        let replacement = provider
            .register_supplier_storage(
                owner.endpoint_id(),
                share,
                &provider_membership,
                &provider_root,
                provider_guard.root_lease().clone(),
                provider_guard.index().clone(),
                provider_guard.store().clone(),
            )
            .unwrap();
        replacement.drain().await.unwrap();

        consumer_session.close().await;
        provider_session.close().await;
        drop(owner_share);
        provider.shutdown().await.unwrap();
        consumer.shutdown().await.unwrap();
        owner.shutdown().await.unwrap();
    }

    async fn stage_unknown_provider_intent_and_recover(
        provider: &ShareService,
        consumer_session: &ShareSession,
        snapshot: &AuthoritativeSnapshot,
        manifest: &ManifestAttestation,
        hashes: &[Hash32],
        storage: (
            &SupplierRegistrationGuard,
            &BTreeMap<ShareId, ManagedAdmissionLease>,
        ),
        operation_id: [u8; 16],
    ) -> Result<ClientIntentRow> {
        let (supplier, leases) = storage;
        let grant = consumer_session
            .request_swarm_grant(provider.endpoint_id(), &snapshot.token, manifest, hashes)
            .await?;
        let row =
            provider
                .registry
                .prepare_client_intent(&grant, ClientSide::Provider, operation_id)?;
        provider.registry.transition_client_intent(
            &grant,
            ClientSide::Provider,
            operation_id,
            ClientIntentPhase::Unknown,
            None,
        )?;

        let key = (grant.nonce, operation_id);
        // A dropped operation without an explicit drain marker must remain
        // unsafe for same-boot recovery, even though no active task remains.
        let unmarked = provider.swarm_tasks.begin_operation(key)?;
        drop(unmarked);
        assert!(
            !provider.local_io_drain_is_proven(&row, leases),
            "an unmarked transport/task exit cannot prove provider drain"
        );

        // A completed marker is still insufficient while the supplier guard
        // owns an accepted storage operation. This models a provider stream
        // whose transport failed while its CAS writer is still running.
        let mut marked = provider.swarm_tasks.begin_operation(key)?;
        let supplier_operation = supplier.begin_operation()?;
        SwarmTaskRegistry::mark_operation_drained(&mut marked, true);
        drop(marked);
        assert!(
            !provider.local_io_drain_is_proven(&row, leases),
            "a completed transport marker cannot bypass an active supplier writer"
        );
        drop(supplier_operation);
        assert!(
            provider.local_io_drain_is_proven(&row, leases),
            "same-boot recovery needs both the marker and an idle exact supplier"
        );

        let recovered = provider
            .recover_client_intents_with_budget_and_leases(Duration::from_secs(5), leases)
            .await?;
        let recovered = recovered
            .into_iter()
            .find(|candidate| candidate.operation_id == operation_id)
            .ok_or(ShareError::StateUnavailable)?;
        assert!(
            matches!(
                recovered.phase,
                ClientIntentPhase::Cancelled | ClientIntentPhase::Drained
            ),
            "an owner-confirmed terminal receipt should close the exact provider intent"
        );
        Ok(recovered)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn same_boot_provider_recovery_checks_direct_and_nested_store_roots() {
        let name = "share::service::tests::same_boot_provider_recovery_checks_direct_and_nested_store_roots";
        if std::env::var("DW_PROVIDER_RECOVERY_ROOTS_CHILD")
            .ok()
            .as_deref()
            != Some(name)
        {
            let profile = tempfile::tempdir().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", name, "--nocapture"])
                .env("DW_PROVIDER_RECOVERY_ROOTS_CHILD", name)
                .env("HOME", profile.path())
                .env("USERPROFILE", profile.path())
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }

        let temp = tempfile::tempdir().unwrap();
        let owner = ShareService::open(
            temp.path().join("owner-service"),
            NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let consumer = ShareService::open(
            temp.path().join("consumer-service"),
            NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let provider_service_path = temp.path().join("provider-service");
        let provider =
            ShareService::open(provider_service_path.clone(), NetworkMode::DirectOnly, None)
                .await
                .unwrap();

        let owner_root = temp.path().join("owner-root");
        std::fs::create_dir_all(&owner_root).unwrap();
        std::fs::write(owner_root.join("recovery.txt"), b"recovery payload").unwrap();
        let owner_share = owner
            .create_owned_share(
                "Provider recovery roots".into(),
                owner_root,
                temp.path().join("owner-state"),
                None,
                0,
            )
            .await
            .unwrap();
        let share = owner_share.config().share_id;
        let consumer_ticket = owner_share
            .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
            .unwrap();
        let provider_ticket = owner_share
            .issue_key(Permission::ReadOnly, None, owner.endpoint_addr())
            .unwrap();
        let _consumer_membership = consumer.enroll(&consumer_ticket, None).await.unwrap();
        let provider_membership = provider.enroll(&provider_ticket, None).await.unwrap();

        let provider_root = temp.path().join("provider-root");
        let provider_state =
            root_admission::reserve_private(temp.path().join("provider-state")).unwrap();
        let (provider_root, provider_state) =
            prepare_server_roots(&provider_root, &provider_state).unwrap();
        let provider_lease = Arc::new(
            root_admission::acquire_with_private(
                &provider_root,
                RootUse::Managed {
                    share: share.0,
                    owner: *owner.endpoint_id().as_bytes(),
                },
                std::slice::from_ref(&provider_state),
            )
            .unwrap(),
        );
        let provider_index = Arc::new(
            LocalIndex::open(
                &provider_root,
                provider_state.join("index.redb"),
                provider_membership.replica,
                IndexOptions::default(),
            )
            .unwrap(),
        );
        let direct_store = Arc::new(
            Store::open_with_recovery_reserver(&provider_state, |path| {
                root_admission::reserve_private(path)
            })
            .unwrap(),
        );
        let direct_guard = provider
            .register_supplier_storage(
                owner.endpoint_id(),
                share,
                &provider_membership,
                &provider_root,
                provider_lease,
                provider_index,
                direct_store.clone(),
            )
            .unwrap();
        let canonical_state = fs::canonicalize(&provider_state).unwrap();
        assert_eq!(direct_guard.private_root(), canonical_state.as_path());

        let consumer_session = consumer.open_session(owner.endpoint_id(), share).unwrap();
        let provider_session = provider.open_session(owner.endpoint_id(), share).unwrap();
        provider_session.refresh_roster().await.unwrap();
        let challenge = provider_session.roster_challenge().unwrap();
        provider_session.heartbeat(challenge).await.unwrap();
        let empty = MerkleTree::from_records(Vec::new()).unwrap();
        let snapshot = consumer_session
            .fetch_authoritative_snapshot(&empty)
            .await
            .unwrap();
        let record = snapshot.records.first().cloned().unwrap();
        let manifest = consumer_session
            .request_manifest(&snapshot.token, &record)
            .await
            .unwrap();
        let hashes: Vec<_> = manifest
            .manifest
            .chunks
            .iter()
            .map(|chunk| chunk.hash)
            .collect();

        let direct_lease = ManagedAdmissionLease::new(
            direct_guard.root_lease().clone(),
            provider_root.clone(),
            provider_state.clone(),
        )
        .unwrap();
        let mut direct_leases = BTreeMap::new();
        direct_leases.insert(share, direct_lease);
        let direct_row = stage_unknown_provider_intent_and_recover(
            &provider,
            &consumer_session,
            &snapshot,
            &manifest,
            &hashes,
            (&direct_guard, &direct_leases),
            [0xe1; 16],
        )
        .await
        .unwrap();
        assert!(matches!(
            direct_row.phase,
            ClientIntentPhase::Cancelled | ClientIntentPhase::Drained
        ));

        // The nested production layout uses the same admitted public root,
        // index, and RootLease while Store::state_root is <private>/store.
        // This must use the same canonical private reservation in recovery;
        // a direct equality check against Store::state_root would reject it.
        direct_guard.drain().await.unwrap();
        let nested_state = provider_state.join("store");
        let nested_store = Arc::new(
            Store::open_with_recovery_reserver(&nested_state, |path| {
                root_admission::reserve_private(path)
            })
            .unwrap(),
        );
        let nested_guard = provider
            .register_supplier_storage(
                owner.endpoint_id(),
                share,
                &provider_membership,
                &provider_root,
                direct_guard.root_lease().clone(),
                direct_guard.index().clone(),
                nested_store,
            )
            .unwrap();
        assert_eq!(
            nested_guard.private_root(),
            canonical_state.as_path(),
            "nested Store placement must retain the outer private reservation"
        );
        assert!(Arc::ptr_eq(
            nested_guard.root_lease(),
            direct_guard.root_lease()
        ));
        assert!(Arc::ptr_eq(nested_guard.index(), direct_guard.index()));
        let nested_lease = ManagedAdmissionLease::new(
            nested_guard.root_lease().clone(),
            provider_root.clone(),
            provider_state.clone(),
        )
        .unwrap();
        let mut nested_leases = BTreeMap::new();
        nested_leases.insert(share, nested_lease);
        let nested_row = stage_unknown_provider_intent_and_recover(
            &provider,
            &consumer_session,
            &snapshot,
            &manifest,
            &hashes,
            (&nested_guard, &nested_leases),
            [0xe2; 16],
        )
        .await
        .unwrap();
        assert!(matches!(
            nested_row.phase,
            ClientIntentPhase::Cancelled | ClientIntentPhase::Drained
        ));

        // Keep one nonterminal provider intent across a real service reopen.
        // The owner then revokes and pauses the share while the provider is
        // offline. Recovery must still use the old exact managed lease and
        // operation-generation evidence; requiring a freshly registered
        // supplier here would strand this paused/revoked row forever.
        let reopen_grant = consumer_session
            .request_swarm_grant(provider.endpoint_id(), &snapshot.token, &manifest, &hashes)
            .await
            .unwrap();
        let reopen_operation = [0xe3; 16];
        provider
            .registry
            .prepare_client_intent(&reopen_grant, ClientSide::Provider, reopen_operation)
            .unwrap();
        provider
            .registry
            .transition_client_intent(
                &reopen_grant,
                ClientSide::Provider,
                reopen_operation,
                ClientIntentPhase::Unknown,
                None,
            )
            .unwrap();
        let before_reopen = provider
            .registry
            .client_intent_exact(&reopen_grant, ClientSide::Provider, reopen_operation)
            .unwrap()
            .unwrap();
        assert!(before_reopen.previous_boot_id.is_none());
        nested_guard.drain().await.unwrap();
        drop(nested_guard);
        drop(direct_guard);
        assert!(matches!(
            owner_share
                .revoke_member_strong(provider.endpoint_id())
                .await
                .unwrap(),
            super::super::RevocationReceipt::Complete { .. }
                | super::super::RevocationReceipt::Pending { .. }
        ));
        owner_share.pause().await;
        provider_session.close().await;
        drop(direct_leases);
        drop(nested_leases);
        provider.shutdown().await.unwrap();

        let reopened =
            ShareService::open(provider_service_path.clone(), NetworkMode::DirectOnly, None)
                .await
                .unwrap();
        let reopened_row = reopened
            .registry
            .client_intent_exact(&reopen_grant, ClientSide::Provider, reopen_operation)
            .unwrap()
            .unwrap();
        assert!(matches!(reopened_row.phase, ClientIntentPhase::Unknown));
        assert_eq!(
            reopened_row.previous_boot_id,
            Some(before_reopen.boot_id),
            "a service reopen must retain the previous generation marker"
        );
        let reopened_lease = Arc::new(
            root_admission::acquire_with_private(
                &provider_root,
                RootUse::Managed {
                    share: share.0,
                    owner: *owner.endpoint_id().as_bytes(),
                },
                std::slice::from_ref(&provider_state),
            )
            .unwrap(),
        );
        let reopened_admission = ManagedAdmissionLease::new(
            reopened_lease,
            provider_root.clone(),
            provider_state.clone(),
        )
        .unwrap();
        let mut reopened_leases = BTreeMap::new();
        reopened_leases.insert(share, reopened_admission);
        assert!(
            reopened.local_io_drain_is_proven(&reopened_row, &reopened_leases),
            "old-boot recovery uses the exact lease even without a supplier registration"
        );
        let recovered = reopened
            .recover_client_intents_with_budget_and_leases(Duration::from_secs(5), &reopened_leases)
            .await
            .unwrap()
            .into_iter()
            .find(|row| row.operation_id == reopen_operation)
            .unwrap();
        assert!(matches!(
            recovered.phase,
            ClientIntentPhase::Cancelled | ClientIntentPhase::Drained
        ));
        assert!(reopened.suppliers.read().unwrap().is_empty());
        reopened.shutdown().await.unwrap();

        consumer_session.close().await;
        drop(owner_share);
        consumer.shutdown().await.unwrap();
        owner.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fabricated_ready_after_owner_revoke_writes_no_consumer_cas() {
        let name =
            "share::service::tests::fabricated_ready_after_owner_revoke_writes_no_consumer_cas";
        if std::env::var("DW_FORGED_READY_CHILD").ok().as_deref() != Some(name) {
            let profile = tempfile::tempdir().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", name, "--nocapture"])
                .env("DW_FORGED_READY_CHILD", name)
                .env("HOME", profile.path())
                .env("USERPROFILE", profile.path())
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }

        let temp = tempfile::tempdir().unwrap();
        let owner = ShareService::open(
            temp.path().join("owner-service"),
            NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let consumer = ShareService::open(
            temp.path().join("consumer-service"),
            NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();
        let provider = ShareService::open(
            temp.path().join("provider-service"),
            NetworkMode::DirectOnly,
            None,
        )
        .await
        .unwrap();

        let root = temp.path().join("shared-root");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("forged.txt"), b"forged-ready payload").unwrap();
        let owner_share = owner
            .create_owned_share(
                "ForgedReady".into(),
                root,
                temp.path().join("shared-state"),
                None,
                0,
            )
            .await
            .unwrap();
        let share = owner_share.config().share_id;
        let consumer_ticket = owner_share
            .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
            .unwrap();
        let provider_ticket = owner_share
            .issue_key(Permission::ReadOnly, None, owner.endpoint_addr())
            .unwrap();
        let _consumer_membership = consumer.enroll(&consumer_ticket, None).await.unwrap();
        let _provider_membership = provider.enroll(&provider_ticket, None).await.unwrap();
        let provider_key = provider.key.clone();
        let provider_id = provider.endpoint_id();

        // First create a valid provider roster entry, then replace its address
        // with the fresh endpoint that will serve the forged response. The
        // consumer caches this signed entry before the owner revoke below;
        // revocation therefore cannot hide the stale-address/authentication
        // distinction this test is exercising.
        let provider_session = provider.open_session(owner.endpoint_id(), share).unwrap();
        provider_session.refresh_roster().await.unwrap();
        let provider_challenge = provider_session.roster_challenge().unwrap();
        provider_session
            .heartbeat(provider_challenge)
            .await
            .unwrap();
        provider_session.close().await;
        provider.shutdown().await.unwrap();
        let forged_endpoint = bind_endpoint(
            provider_key.clone(),
            NetworkMode::DirectOnly,
            Some(vec![ALPN_SWARM_V1.to_vec()]),
            None,
        )
        .await
        .unwrap();
        let forged_address = endpoint_addr_with_local_fallback(&forged_endpoint);
        let (_, forged_challenge) = owner
            .registry
            .issue_roster_challenge(&owner.key, share, provider_id)
            .unwrap();
        let forged_heartbeat = RosterHeartbeat::sign(
            &provider_key,
            owner.endpoint_id(),
            share,
            forged_address,
            forged_challenge,
            now(),
        );
        owner
            .registry
            .accept_roster_heartbeat(&owner.key, &forged_heartbeat, provider_id)
            .unwrap();
        let consumer_session = consumer.open_session(owner.endpoint_id(), share).unwrap();
        consumer_session.refresh_roster().await.unwrap();

        let snapshot = consumer_session
            .fetch_authoritative_snapshot(&MerkleTree::from_records(Vec::new()).unwrap())
            .await
            .unwrap();
        let record = snapshot
            .records
            .iter()
            .find(|record| record.path.as_str() == "forged.txt")
            .cloned()
            .unwrap();
        let manifest = consumer_session
            .request_manifest(&snapshot.token, &record)
            .await
            .unwrap();
        let hashes: Vec<_> = manifest
            .manifest
            .chunks
            .iter()
            .map(|chunk| chunk.hash)
            .collect();
        assert!(!hashes.is_empty());
        let grant = consumer_session
            .request_swarm_grant(provider_id, &snapshot.token, &manifest, &hashes)
            .await
            .unwrap();

        // Make the owner row Active and capture the legitimate receipt before
        // revocation. The forged peer will replay this otherwise-valid receipt
        // after the owner marks the provider revoked.
        let activation = owner
            .registry
            .activate_grant_at(
                &owner.key,
                &grant.activate_request(),
                provider_id,
                Instant::now(),
            )
            .unwrap();
        let captured = owner
            .registry
            .activation_receipt(
                &ActivationStatusQuery::for_grant(&grant, Some(activation.activation_id)),
                provider_id,
            )
            .unwrap();
        assert!(matches!(captured.state, ActivationStateView::Active));
        owner
            .registry
            .revoke_member_durable(share, provider_id)
            .unwrap();

        // Reuse the provider identity on the fresh endpoint, but serve only
        // the stale Ready frame. This exercises authenticated transport
        // identity separately from the owner's durable admission authority.
        let forged_router = Router::builder(forged_endpoint)
            .accept(ALPN_SWARM_V1, ForgedReadyHandler { receipt: captured })
            .spawn();

        let consumer_store = Arc::new(Store::open(temp.path().join("consumer-store")).unwrap());
        let result = consumer_session
            .fetch_swarm_chunks(
                consumer_store.clone(),
                &grant,
                &snapshot.token,
                &record,
                &manifest,
                &hashes,
                [0xf4; 16],
            )
            .await;
        assert!(result.is_err());
        for hash in hashes {
            assert!(
                !consumer_store.chunks().contains(hash),
                "revoked forged Ready must not write consumer CAS"
            );
        }

        forged_router.shutdown().await.unwrap();
        consumer_session.close().await;
        drop(owner_share);
        consumer.shutdown().await.unwrap();
        owner.shutdown().await.unwrap();
    }

    #[test]
    fn denied_session_does_not_join_revocation_drain_after_connections_close() {
        if std::env::var_os("DW_DENIED_DRAIN_CHILD").is_none() {
            let home = tempfile::tempdir().unwrap();
            let result=std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact","share::service::tests::denied_session_does_not_join_revocation_drain_after_connections_close","--nocapture"])
                .env("DW_DENIED_DRAIN_CHILD","1").env("HOME",home.path()).env("USERPROFILE",home.path()).status().unwrap();
            assert!(result.success());
            return;
        }
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let temp = tempfile::tempdir().unwrap();
            let owner =
                ShareService::open(temp.path().join("owner"), NetworkMode::DirectOnly, None)
                    .await
                    .unwrap();
            let share = owner
                .create_owned_share(
                    "Files".into(),
                    temp.path().join("root"),
                    temp.path().join("state"),
                    None,
                    0,
                )
                .await
                .unwrap();
            let ticket = share
                .issue_key(
                    super::super::Permission::ReadOnly,
                    None,
                    owner.endpoint_addr(),
                )
                .unwrap();
            let peer = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
                .bind()
                .await
                .unwrap();
            owner.registry.enroll(&ticket, peer.id(), None).unwrap();
            let gate = share.runtime.gate.lock().await;
            let revoking = share.clone();
            let id = peer.id();
            let mut revoke = tokio::spawn(async move { revoking.revoke_member(id).await });
            while share.members().unwrap()[0].revoked_at.is_none() {
                tokio::task::yield_now().await;
            }
            let connection = peer.connect(owner.endpoint_addr(), ALPN_V3).await.unwrap();
            let result = wire::exchange(
                &connection,
                Hello {
                    version: 3,
                    share_id: share.config().share_id,
                    operation: Operation::Session,
                },
            )
            .await;
            assert_eq!(
                result.err().unwrap().downcast_ref::<ShareError>(),
                Some(&ShareError::MemberRevoked)
            );
            drop(gate);
            let completed =
                tokio::time::timeout(std::time::Duration::from_secs(2), &mut revoke).await;
            connection.close(0u8.into(), b"test complete");
            let finished = completed.is_ok();
            if let Ok(result) = completed {
                result.unwrap().unwrap();
            } else {
                revoke.await.unwrap().unwrap();
            }
            peer.close().await;
            drop(share);
            owner.shutdown().await.unwrap();
            assert!(
                finished,
                "an already-denied idle connection kept revocation waiting"
            );
        });
    }
}

#[cfg(test)]
mod capacity_tests {
    use super::super::{Permission, registry::MAX_REPLICAS};
    use super::*;

    fn catalog_snapshot(registry: &Registry) -> Vec<u8> {
        // Read every persisted share field through redb transactions. A second
        // file handle cannot read a live redb file under Windows byte-range locks.
        let shares: Vec<_> = registry
            .configs()
            .unwrap()
            .into_iter()
            .map(|config| {
                let id = config.share_id;
                (
                    registry.is_ready(id).unwrap(),
                    config,
                    registry.invitations(id).unwrap(),
                    registry.members(id).unwrap(),
                    registry.known(id).unwrap(),
                )
            })
            .collect();
        postcard::to_stdvec(&(shares, registry.relationships().unwrap())).unwrap()
    }

    #[test]
    fn proof_at_replica_capacity_is_atomic_and_existing_writer_survives_restart() {
        let name = "share::service::capacity_tests::proof_at_replica_capacity_is_atomic_and_existing_writer_survives_restart";
        if std::env::var("DW_CAPACITY_CHILD").ok().as_deref() != Some(name) {
            let profile = tempfile::tempdir().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", name, "--nocapture"])
                .env("DW_CAPACITY_CHILD", name)
                .env("HOME", profile.path())
                .env("USERPROFILE", profile.path())
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let temp = tempfile::tempdir().unwrap();
            let device = temp.path().join("device");
            let owner = ShareService::open(&device, NetworkMode::DirectOnly, None)
                .await
                .unwrap();
            let member =
                ShareService::open(temp.path().join("member"), NetworkMode::DirectOnly, None)
                    .await
                    .unwrap();
            let root = temp.path().join("root");
            let share = owner
                .create_owned_share(
                    "Files".into(),
                    root.clone(),
                    temp.path().join("state"),
                    None,
                    0,
                )
                .await
                .unwrap();
            std::fs::write(root.join("file"), b"original").unwrap();
            let ticket = share
                .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
                .unwrap();
            let writer =
                ShareService::open(temp.path().join("writer"), NetworkMode::DirectOnly, None)
                    .await
                    .unwrap();
            let writer_grant = writer.enroll(&ticket, None).await.unwrap();
            let known_key = SecretKey::generate();
            let known_id = ReplicaId(Hash32::digest(known_key.public().as_bytes()));
            let mut known = owner.registry.known(share.config().share_id).unwrap();
            known.insert(known_id);
            for n in 0u64.. {
                if known.len() == MAX_REPLICAS {
                    break;
                }
                known.insert(ReplicaId(Hash32::digest(&n.to_le_bytes())));
            }
            owner
                .registry
                .remember_replicas(share.config().share_id, known.clone())
                .unwrap();
            let unknown_key = SecretKey::generate();
            let unknown_id = ReplicaId(Hash32::digest(unknown_key.public().as_bytes()));
            assert!(!known.contains(&unknown_id));
            let proof =
                LegacyProof::create(&ticket, &unknown_key, member.endpoint_id(), unknown_id)
                    .unwrap();
            let catalog_before = catalog_snapshot(&owner.registry);
            assert!(
                member.enroll(&ticket, Some(proof)).await.is_err(),
                "unknown retained replica exceeded capacity"
            );
            assert!(
                catalog_snapshot(&owner.registry) == catalog_before,
                "denied enrollment wrote catalog"
            );
            assert_eq!(
                owner.registry.known(share.config().share_id).unwrap(),
                known
            );
            assert_eq!(share.members().unwrap(), vec![writer_grant.clone()]);
            let config = share.config().clone();
            drop(share);
            owner.shutdown().await.unwrap();
            let owner = ShareService::open(&device, NetworkMode::DirectOnly, None)
                .await
                .unwrap();
            let share = owner.load_owned_share(config.share_id).await.unwrap();
            assert!(
                catalog_snapshot(&owner.registry) == catalog_before,
                "denied enrollment changed the persisted catalog after restart"
            );
            assert_eq!(owner.registry.known(config.share_id).unwrap(), known);
            assert_eq!(share.members().unwrap(), vec![writer_grant.clone()]);
            let ticket = share
                .issue_key(Permission::ReadWrite, None, owner.endpoint_addr())
                .unwrap();
            let proof =
                LegacyProof::create(&ticket, &known_key, member.endpoint_id(), known_id).unwrap();
            let grant = member.enroll(&ticket, Some(proof)).await.unwrap();
            assert_eq!(grant.replica, known_id);
            assert_eq!(owner.registry.known(config.share_id).unwrap(), known);
            assert_eq!(writer.enroll(&ticket, None).await.unwrap(), writer_grant);
            let incumbent = writer
                .open_session(writer_grant.owner, writer_grant.share_id)
                .unwrap();
            let empty = MerkleTree::from_records(Vec::new()).unwrap();
            let mut incumbent_record = incumbent
                .fetch_snapshot(&empty)
                .await
                .unwrap()
                .records
                .remove(0);
            incumbent_record.path = deltaweave_core::WirePath::new("incumbent-created").unwrap();
            incumbent_record.kind = deltaweave_core::SyncEntryKind::Directory;
            incumbent_record.content_hash = None;
            incumbent_record.size = 0;
            incumbent_record
                .version
                .increment(writer_grant.replica)
                .unwrap();
            incumbent.apply_metadata(incumbent_record).await.unwrap();
            assert!(root.join("incumbent-created").is_dir());
            drop(incumbent);
            let session = member.open_session(grant.owner, grant.share_id).unwrap();
            let empty = MerkleTree::from_records(Vec::new()).unwrap();
            let mut record = session
                .fetch_snapshot(&empty)
                .await
                .unwrap()
                .records
                .remove(0);
            record.path = deltaweave_core::WirePath::new("created").unwrap();
            record.kind = deltaweave_core::SyncEntryKind::Directory;
            record.content_hash = None;
            record.size = 0;
            record.version.increment(grant.replica).unwrap();
            session.apply_metadata(record.clone()).await.unwrap();
            assert_eq!(
                session
                    .fetch_snapshot(&empty)
                    .await
                    .unwrap()
                    .records
                    .into_iter()
                    .find(|item| item.path == record.path)
                    .unwrap(),
                record
            );
            assert_eq!(std::fs::read(root.join("file")).unwrap(), b"original");
            drop(session);
            drop(share);
            writer.shutdown().await.unwrap();
            member.shutdown().await.unwrap();
            owner.shutdown().await.unwrap();
        });
    }
}
