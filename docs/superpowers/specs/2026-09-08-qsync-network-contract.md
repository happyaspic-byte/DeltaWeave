# qsync 네트워크 계약 독립검토 (A단계 최종 handoff)

기준: `/home/ubuntu/project/DeltaWeave`, `HEAD=baed5c0164a2e10fdbe6a1e5a31f7e897c675ced`.
첨부 goal prompt `pasted-text-1.txt` 전체(117행)를 읽었으며, 소스/문서/commit/push는
변경하지 않았다. 이 파일만 검토 산출물로 작성했다.

## B에 즉시 고정할 계약

### 동일 `ShareService` endpoint의 member 저장소 등록

`ManagedSyncEngine`는 현재처럼 caller-supplied role/replica를 신뢰하지 않고 owner
relationship으로 먼저 여는 동작을 유지한다. 이미 B가 정확히 열고 보유하는
`RootLease`, `Arc<LocalIndex>`, `Arc<Store>`를 다시 `MemberStorageLease`로 감싸는
전면 refactor는 필요하지 않다. net API는 이 세 handle을 supplier registry에
제한적으로 연결하는 guard만 반환해야 한다.

```rust
pub struct SupplierRegistrationGuard { /* private: same RootLease/index/store Arcs */ }

impl SupplierRegistrationGuard {
    pub async fn drain(&mut self) -> Result<()>;
}

impl ShareService {
    pub fn register_supplier_storage(
        &self,
        owner: EndpointId,
        share: ShareId,
        membership: &Membership,
        root: &Path,
        root_lease: Arc<RootLease>,
        index: Arc<LocalIndex>,
        store: Arc<Store>,
    ) -> Result<SupplierRegistrationGuard>;

    pub async fn resume_membership(
        &self,
        owner: EndpointId,
        share: ShareId,
        address_hint: Option<EndpointAddr>,
    ) -> Result<Membership>;
}
```

`register_supplier_storage`는 `membership`를 caller가 만든 권위로 취급하지 않고
service registry의 persisted relationship과 다시 비교한다. `owner/share`,
`membership.endpoint == self.endpoint_id()`, non-revoked 상태, replica/epoch, exact
canonical `root`를 대조한다. `root_lease`가 `RootUse::Managed { share, owner }`로
이미 잡혀 있고 `index`/`store`가 그 root의 private state를 가리키는지 확인한 뒤,
같은 `Arc<RootLease>`, `Arc<LocalIndex>`, `Arc<Store>`를 guard가 보유한다. guard는
provider handler가 실행되는 동안 active supplier operation count를 올리고, drop
때 deregister/drain한다. standalone provider가 root/index/store를 다시 열거나
caller가 다른 replica/permission을 주입하는 경로는 없다. 이 API는 root admission을
두 번 하지 않는다.

구체적으로 B는 `ReplicaState._root_lease`를 `Arc<RootLease>`로 공유하거나 동일한
수명 보장용 Arc wrapper를 한 번만 만든다. `LocalIndex::open`/`Store::open`은 현재
`ManagedSyncEngine::open_inner`의 순서를 유지하고, 그 결과를 위 guard에 전달한다.
guard는 worker lifetime 동안 유지하고 shutdown에서는 새 supplier operation을 먼저
막은 뒤 active writer와 guard를 drain하고 마지막에 engine의 root lease를 놓는다.

`resume_membership`은 expected replica/epoch를 클라이언트가 미리 요구하지 않는다.
owner의 별도 `Operation::Resume` handler가 authenticated QUIC `remote_id()`를
현재 active member binding과 비교하고, optional `address_hint`는 transport 주소로만
검증한다. 기존 relationship/index가 있으면 그 persisted replica/permission/epoch와
exact 대조해 반환한다. wire reply는 기존 share/3 control connection이 인증한
`Reply::Resumed(Membership)`이며, 새 SignedMembership wire type을 요구하지 않는다.
member는 owner endpoint identity와 QUIC peer identity를 확인한 뒤 관계를 저장한다.
revoked/nonmember, identity mismatch, 다른 address id는 거부하며 새
replica/role/enrollment를 만들지 않는다. 응답 유실 후 B가 불완전한 expected 값을
발명하지 않아도 되는 hook이다.

### D/E 등록 hook

- D 소유: `share/{registry,runtime,service,wire,mod}.rs`의 owner registry/roster,
  `register_supplier_storage`, owner-authenticated roster heartbeat와
  `ShareSession::{refresh_roster,revalidate}` 및 `resume_membership` primitive.
  B가 owner-side Resume dispatch/worker integration을 즉시 맡는다. `ShareService`는
  device-wide endpoint 하나만 bind하고 `share/3`와 grant-only `share-swarm/1`만
  등록한다.
- E 소유: `share/swarm.rs`(신규) 및 `net/lib.rs`의 얇은 router/transport adapter,
  `sync/transport.rs`의 managed transport 구현. legacy `SwarmHandler`와
  `ALPN_SWARM_V3=deltaweave/sync/3`는 수정/재사용/직접 attach하지 않는다.

필수 E 외부 표면은 `ShareSession::request_swarm_grant(provider, snapshot, manifest,
hashes)`, `ShareSession::fetch_authoritative_snapshot`, `ShareSession::revalidate`,
`ShareSession::open_share_swarm(roster_entry, grant, signed_manifest, hashes)`,
`ShareSwarm::fill_chunks_with_admission`이며, 마지막 단계의 fallback은
`ShareSession`의 owner-authenticated `share/3` `pull_record_to_with_budget`만 허용한다.

이 중간 handoff의 근거는 다음과 같다: `share/service.rs:292-313`은 기존 persisted
relationship으로 동일 endpoint session을 만들지만 저장소 lease를 반환하지 않으며,
`share/service.rs:316-330`은 root lease만 반환한다. `sync/shared.rs:100-148`은
index/store를 별도로 열고 RW permission/epoch를 cache한다. `net/lib.rs:1378-1398`은
managed session의 legacy swarm 호출을 명시적으로 거부하고, `sync/shared.rs:138`은
 managed `swarm_sources`를 항상 비운다.

## 네트워크 계약의 기준선

새 데이터 ALPN은 정확히 `b"deltaweave/share-swarm/1"`로 한다. 하나의
`ShareService` endpoint가 `share/3` control ALPN과 이 ALPN의 grant-gated chunk
handler를 함께 소유한다. endpoint를 두 번째로 만들거나 `start_server_observed`의
legacy endpoint를 재사용하지 않는다. `deltaweave/sync/1`, `/2`, `/3`와 현재
`ALPN_SWARM_V3`는 managed share endpoint에 등록하지 않는다.

`share/3`의 `Hello.version == 3` 및 postcard strict round-trip은 그대로 둔다.
`Operation`/`Reply`에 `Resume`, `Snapshot`, `Manifest`, `Grant`, `Revalidate`,
`Roster`, `Heartbeat`를 추가할 수 있지만 기존 `Validate`, `Enroll`, `Session`의
variant와 wire 구조는 변경하지 않는다. 새 variant를 모르는 peer는 protocol error로
실패하고, 기존 peer의 share/3 session은 계속 동작한다. 이는 version을 올려 legacy
peer를 조용히 다른 의미로 해석하게 만드는 것보다 안전하다. 새 ALPN handler는
`ShareGrant` 없는 연결, ticket bearer, 임의 path/metadata request를 모두 거부한다.
Resume의 wire reply는 기존 `Reply::Resumed(Membership)`를 사용하고, owner와 member가
서로 인증한 share/3 control stream 자체가 peer/owner 증거다. snapshot/grant/permit도
같은 authenticated control stream에서 교환한다. data bytes만 share-swarm/1에 둔다.

권한 세대는 현재 registry가 가진 **member별** `Membership.epoch`를 그대로 사용한다.
`share/registry.rs:31-40,402-415`에서 보듯 revoke 때 해당 member만 증가하므로,
`ShareGrant.epoch`는 consumer/requester의 epoch로 정의하고 반드시 별도
`provider_epoch`도 함께 서명한다. owner는 발급 때 두 endpoint에 대해
`authorize(share, consumer, false)`와 `authorize(share, provider, false)`를 모두
수행한다. 어느 한 쪽의 revoke/permission 변경으로 그 member epoch가 바뀌면
해당 grant가 무효다. requester epoch만 서명하는 축약은 provider revoke를 놓치므로
허용하지 않는다.

기존 `owner_share_catalog_v3` postcard 구조는 바꾸지 않는다. lease/nonce deny,
roster heartbeat/address 및 필요한 monotonic clock anchor는 별도의 bounded
`share-swarm-* -v1` redb tables/records로 저장하고, 각 row에 기존 share/owner key를
명시적으로 둔다. 이미 있는 catalog migration을 새 wire API의 전제조건으로 만들지
않으며, share/3 wire version도 3으로 둔다.

## 최소 타입과 서명 범위

아래는 새 암호를 만들지 않고 현재 `iroh::Signature`, `SecretKey::sign`,
`EndpointId::verify`, `Hash32::digest`, `FileManifest::manifest_hash`를 쓰는
최소 public/crate API다. 모든 signed struct에서 `signature` 필드 자체는 서명
입력에서 제외하고, 고정 domain prefix와 canonical `postcard` bytes를 서명한다.

```rust
pub type PermissionEpoch = u64;
pub type GrantNonce = [u8; 32];
pub type SnapshotId = [u8; 32];

pub struct SnapshotToken {
    pub version: u8,                 // 1
    pub owner: EndpointId,
    pub share: ShareId,
    pub epoch: PermissionEpoch,       // consumer Membership.epoch
    pub snapshot: SnapshotId,
    pub root_hash: Hash32,
    pub record_count: u32,
    pub issued_at: u64,
    pub expires_at: u64,
    pub signature: Signature,
}

pub struct AuthoritativeSnapshot {
    pub token: SnapshotToken,
    pub records: Vec<SyncRecord>,
}

pub struct ManifestAttestation {
    pub version: u8,                 // 1
    pub owner: EndpointId,
    pub share: ShareId,
    pub epoch: PermissionEpoch,       // consumer Membership.epoch
    pub snapshot: SnapshotId,
    pub record_hash: Hash32,         // SyncRecord::logical_hash()
    pub manifest: FileManifest,
    pub manifest_hash: Hash32,       // manifest.manifest_hash()
    pub issued_at: u64,
    pub expires_at: u64,
    pub signature: Signature,
}

pub struct ShareGrant {
    pub version: u8,                 // 1
    pub owner: EndpointId,
    pub share: ShareId,
    pub consumer: EndpointId,
    pub provider: EndpointId,
    pub epoch: PermissionEpoch,       // consumer Membership.epoch
    pub provider_epoch: PermissionEpoch,
    pub snapshot: SnapshotId,
    pub manifest: Hash32,
    pub request_hash: Hash32,
    pub issued_at: u64,
    pub expires_at: u64,
    pub nonce: GrantNonce,
    pub signature: Signature,
}

pub struct ActivateGrantRequest {
    pub owner: EndpointId,
    pub share: ShareId,
    pub consumer: EndpointId,
    pub provider: EndpointId,
    pub epoch: PermissionEpoch,
    pub provider_epoch: PermissionEpoch,
    pub manifest: Hash32,
    pub request_hash: Hash32,
    pub nonce: GrantNonce,
}

pub struct ActivateGrantReply {
    pub share: ShareId,
    pub provider: EndpointId,
    pub nonce: GrantNonce,
    pub activation_id: [u8; 16],
    pub accepted: bool,
    pub max_duration_secs: u16,       // exactly <= 15
    pub signature: Signature,          // owner signs the accepted fields
}

pub struct ApplyPermit {
    pub version: u8,                 // 1
    pub owner: EndpointId,
    pub share: ShareId,
    pub consumer: EndpointId,
    pub epoch: PermissionEpoch,       // consumer Membership.epoch
    pub snapshot: SnapshotId,
    pub root_hash: Hash32,
    pub issued_at: u64,
    pub expires_at: u64,
    pub nonce: GrantNonce,
    pub signature: Signature,
}

pub struct ApplyStart {
    pub operation_id: [u8; 16],
    pub permit_nonce: GrantNonce,
}

pub struct ApplyDrained {
    pub operation_id: [u8; 16],
    pub permit_nonce: GrantNonce,
    pub committed: bool,
}

pub enum RevocationReceipt {
    Complete { member_epoch: u64, completed_at: u64 },
    Pending { member_epoch: u64, deadline: u64, blockers: u32 },
}
```

`ApplyStart`/`ApplyDrained`는 이미 owner가 인증한 share/3 control connection에서
전달하므로 별도 서명 체계를 만들지 않는다. owner는 `remote_id == consumer`를
확인하고 operation id/permit nonce를 lease table에 기록한다. `ApplyDrained`의
`committed`는 파일 반영 완료 여부일 뿐 권한을 새로 부여하지 않는다.

기존 `ShareError`에 필요한 구분은 `GrantExpired`, `GrantReplay`, `EpochMismatch`,
`ManifestMismatch`, `CasUnavailable`, `ClockRollback`, `RevocationPending`,
`EndpointMismatch` 정도로 제한한다. grant/permit 검증 실패를 일반
`TransferFailed`로 뭉개면 B가 재시도 가능한 새 nonce와 revoked/pending을 구분할 수
없다.

`SnapshotToken` signs the complete authoritative Merkle root, record count, owner/share,
epoch, snapshot nonce and bounded lifetime. The receiver validates every record and
rebuilds the same tree; for a response larger than the existing 16 MiB control frame,
pages are still permitted, but every page carries the token snapshot id and the final
root/count check is mandatory. A page is not an independently authoritative snapshot.
The owner creates a fresh random `snapshot` for each snapshot response, so a restarted
owner cannot accidentally make an old root look like a current sequence number.

`ManifestAttestation` is issued only for a live file record in that signed snapshot. It
requires `record.content_hash == manifest.file_hash`, `record_hash` to match the exact
owner record, `manifest_hash == manifest.manifest_hash()`, and
`manifest.validate()`. A tombstone, directory, provider-local record or consumer-local
edit has no attestation. The owner is therefore the only metadata authority.

For a request containing a sorted, strictly unique list `H` of at most 64 chunk hashes,
define the existing-hash-only request digest as:

```text
request_hash = Hash32::digest(
  b"deltaweave/share-swarm/request/v1\0" ||
  postcard({ share, snapshot, manifest, H })
)
```

The owner verifies that every hash in `H` is a descriptor in the attested
`FileManifest`, calls `authorize` for both consumer and provider, and puts the two
membership epochs into `epoch` and `provider_epoch` before signing `ShareGrant`. On the
provider handler the final peer check is exact:
`connection.remote_id() == grant.consumer` (requester) and
`self.endpoint_id() == grant.provider` (local supplier); the consumer connector checks
the converse `connection.remote_id() == grant.provider` and its own local id equals
`grant.consumer`. The provider
recomputes `request_hash` from the bytes actually received and rejects duplicates, an
out-of-manifest hash, a cross-share/snapshot/manifest field, or either endpoint/epoch
mismatch. A grant is one nonce and one exact request; reconnecting for another subset
requires a new owner grant.

The data response is a bounded chunk envelope containing descriptor index/offset/length,
hash and bytes. The provider may read only its verified CAS (`ChunkStore::read_verified`)
for hashes in `H`; it sends no `SyncRecord`, path, tombstone or metadata. The consumer
checks descriptor length and `Hash32::digest(bytes)`, then uses the existing verified
chunk path (`VerifiedChunk`/`put_verified`). Thus successful content is the CAS existence
and content proof. An availability bitset or provider assertion alone is not proof, and
no Merkle-proof implementation is needed. Owner grant issuance confirms only that the
requested hashes belong to the owner snapshot; it must not claim that a partitioned
provider already has them. The successful hash-checked payload is the minimal proof of
that provider's actual CAS contents.

## 시간, replay, lease 및 강한 철회

구현할 유한 상한은 다음으로 고정한다.

```text
MAX_SHARE_SWARM_PROVIDERS = 8
MAX_GRANT_TTL              = 120 seconds (inactive grant display/rejection bound)
MAX_ACTIVATE_TTL           = 15 seconds
MAX_STREAM_TTL             = 15 seconds
MAX_APPLY_TTL              = 10 seconds
MAX_CLOCK_SKEW             = 5 seconds
ROSTER_HEARTBEAT_INTERVAL  = 30 seconds
ROSTER_STALE_AFTER         = 90 seconds
MAX_REQUEST_HASHES         = 64
MAX_STREAMS_PER_PROVIDER   = 2
MAX_INFLIGHT_CHUNKS        = 8
REVOKE_ACTIVE_BOUND        = MAX_ACTIVATE_TTL + MAX_CLOCK_SKEW = 20 seconds
```

Owner grant 발급은 다음 순서다.

1. consumer가 owner에 fresh authorization을 요청하는 순간을 `Instant::now()`로
   잡는다. owner는 현재 consumer `Membership.epoch`, provider `Membership.epoch`,
   current signed snapshot/manifest, authenticated consumer와 fresh roster heartbeat를
   가진 provider를 확인한다.
2. owner의 effective wall clock로 `issued_at`을 기록하고, 정확히
   `expires_at = issued_at + 120` 이하로 lease record를 registry transaction에
   먼저 저장한다. nonce 중복만 durable하게 거부하고, 이전 시도와 같은
   `(consumer, provider, request_hash)`를 새 nonce로 재시도하는 것은 허용한다.
   정상 실패/부분전송/재개가 영구적으로 막히지 않도록 nonce만 one-use다.
3. provider는 share-swarm handshake에서 grant를 받은 직후 owner에 authenticated
   `ActivateGrantRequest`를 보낸다. 이 요청 직전의 provider `Instant::now()`를
   `activation_start`로 잡고, owner도 수신 시 자신의 monotonic lease deadline을
   `receive_start + 15s`로 기록한다. owner는 grant의 두 member epoch, nonce/state,
   request hash와 provider `remote_id`를 다시 확인한 뒤 signed
   `ActivateGrantReply{activation_id,max_duration_secs <= 15}`를 한 번만 돌려준다.
   provider는 응답이 늦어 `activation_start + 15s`를 넘으면 폐기하고, owner도
   activation lease를 그 시각 이후 연장하지 않는다. 응답의 UTC expiry는 표시와
   추가 거부용일 뿐 기기간 clock 동기를 전제하지 않는다.
4. provider의 active stream deadline은 provider process의
   monotonic `activation_start + min(15s, max_duration_secs)`로 고정한다. consumer도
   `open_share_swarm`/grant request를 시작한 자기 `consumer_start`부터 최대 15초로
   별도 제한한다. provider는 각 chunk와 writer enqueue 직전에 검사하고, consumer는
   grant/owner-signed activation reply의 nonce/provider/consumer를 확인한 뒤 자기
   deadline 안에서만 수신한다. 같은 nonce의 새 connection/activation은 state가
   Active/Done이면 거부하며, 새 subset 또는 재연결은 새 nonce로 owner에 다시
   요청한다.

`expires_at`은 wall timestamp를 표시하고 owner가 늦은 grant를 추가 거부하는 데만
쓴다. provider가 자기 UTC와 owner UTC가 같다고 가정해 timer를 계산하지 않는다.
owner와 각 member는 durable `last_seen_wall`을 저장한다. 현재 wall 시간이 이전 값보다
5초 이상 뒤로 가면 `ClockRollback` quarantine으로 전환하고 새 send, CAS write,
materialize, index adoption 및 grant issuance를 모두 거부한다. 정상일 때만
`last_seen_wall = max(old, now)`로 전진시킨다. owner가 grant/ActivateGrant를
받을 때 `expiry <= owner_now + 5`이면 조기 거부하며, provider는 owner의 signed
activation과 자기 monotonic deadline만 신뢰한다. 이 계산으로 늦은 grant 전달이
TTL을 연장하지 않는다.

process restart 때는 기존 active nonce/activation을 `Restarted` deny 상태로 바꾸고
old grant로 새 stream을 열지 않는다. pending RO journal은 남기되
`resume_membership` 성공, 새 snapshot/grant, 새 owner ActivateGrant, 새 monotonic
deadline을 얻기 전에는 transfer/materialize를 재개하지 않는다. 같은 boot에서도
완료/거부 nonce의 재활성화와 active nonce의 다른 connection replay를 거부한다.
이 보수적 one-grant/one-activation 규칙이 nonce 재사용과 restart replay를 단순하게
막는다.

owner `revoke_member_strong(peer)`의 durable 순서는 다음과 같다.

1. 기존 catalog postcard는 유지하되 같은 redb write transaction에서 member revoke와
   해당 `Membership.epoch` 증가를 catalog table에 commit하고, 별도 v1 lease table의
   affected consumer/provider nonce를 즉시 deny한다. 아직 `ActivateGrant`되지 않은
   `Issued` row는 이 시점에 끝나므로 120초 grant TTL을 기다리지 않는다. catalog와
   lease table을 서로 다른 transaction으로 쓰지 않는다. service lifecycle lock도
   함께 잡아 이 durable 경계 뒤에는 어떤 새 grant/activation도 발급하지 않는다.
2. runtime의 known connection을 닫고, grant operation guard와 blocking
   `ChunkWritePipeline`/materialization writer를 stop-and-drain한다. guard는 grant
   검사를 통과한 순간부터 실제 writer future가 끝날 때까지 살아 있어야 한다.
3. 모든 known **active activation lease**가 ack/closed 되고 writer가 0개이면 완료할
   수 있다. partition으로 ack를 못 받으면 최소 `active_deadline + 5s`까지
   `RevocationPending`을 유지한다. `REVOKE_ACTIVE_BOUND=20s`는 active grant
   admission이 끝나는 earliest safety bound이지 revocation 완료를 강제하는
   timeout이 아니다. 그 시각에도 started `ApplyPermit` operation,
   `spawn_blocking` filesystem write/rename, 또는 drain ack가 없는 peer가 있으면
   `RevocationPending { member_epoch, deadline, blockers }`를 계속 반환하며 절대로
   `Complete`로 바꾸지 않는다. clock quarantine도 같은 보수적 경로다. caller는
   Pending을 UI/worker에 그대로 전달하고 blocker가 drain된 뒤 재확인한다.

`ApplyPermit`을 받은 member는 filesystem action 전에 owner에
`ApplyStart { operation_id, permit_nonce }`를 기록하고, 마지막 rename/index commit,
모든 `spawn_blocking` join 및 local gate release 뒤 `ApplyDrained`를 보낸다. owner
lease table은 이 operation을 active로 추적한다. revoke는 permit을 deny하고
`ApplyDrained`를 기다린다. member가 partition 또는 정지 process라 ack를 보낼 수
없으면 lease expiry가 지났어도 owner는 “grant admission 만료”만 말할 수 있고,
remote filesystem write가 끝났다고 추정해 완료를 선언할 수 없다. process restart
후에는 durable pending journal을 복구/검증해 이전 operation의 drain을 명시적으로
기록하기 전까지 새 apply를 차단한다.

철회 완료의 보장 범위는 “commit 이후 새 application send, 새 CAS writer, 새
materialize/index adoption이 0”이다. 이미 QUIC/kernel buffer에 들어간 바이트는
회수할 수 없다. 늦게 도착한 바이트는 stream admission에서 폐기하거나 격리 CAS에
남기되, 완료 이후 새 namespace/file 반영으로 이어지지 않는다. 이미 데이터를
가진 완전 악성 supplier가 별도 경로로 임의 재배포하는 것을 막는다는 주장은 하지
않는다.

owner가 저장하는 최소 lease row는
`{nonce, share, consumer, provider, consumer_epoch, provider_epoch, manifest,
request_hash, issued_at, expires_at, activation_id, activation_deadline, state}`이고,
`active_lease_deadline`은 `Active` row의 activation deadline만 추적한다. `Issued`
row의 120초 expiry는 replay/late activation 거부에 사용하며 revoke drain을
지연시키는 deadline이 아니다. member의 최소 deny cache는
`{owner, share, nonce, consumer_epoch, provider_epoch, expires_at, state,
last_seen_wall, boot_id}`다. 이
상태가 있어야 owner 재시작/partition 중에도 active lease deadline과 늦은 발급 거부
상태를 재구성하고, member 재시작이 old grant를 연장하지 않는다. `Issued` row의
120초 expiry를 모두 기다려 revoke completion을 지연시키는 규칙은 없다.

### B/C의 최소 pending 상태 전달

현재 control에는 `MutationResult`가 없으므로 qsync 공통 API에는 성공과 오류 사이의
불확실한 drain 상태를 새 성공으로 숨기지 않는 DTO만 추가한다.
현재 `worker.rs:19-22`의 command reply가 `Result<()>`로만 되어 있어 이 상태를
그대로 유지하면 Pending을 성공/실패 중 하나로 오인한다. C의 외부 model과 B의
oneshot reply는 아래 두 enum/field를 사용한다.

```rust
#[serde(rename_all = "snake_case")]
pub enum MutationCompletion {
    Pending,
    Complete,
}

pub struct MutationResult {
    pub completion: MutationCompletion,
    pub retry_at: Option<u64>,
}
```

`MemberView`에는 `revocation_pending: bool`을 추가한다. `MutationResult::Pending`은
share 전체를 revoked로 표시하는 상태가 아니며, 영향을 받는 member/operation만
`share8status`의 기존 표면 안에서 `mutationpending.status = "waiting"`으로
표시한다. HTTP/worker에서 Pending을 2xx 성공, `last_sync_at`, `complete`로
표시하지 않는다. 새 권한 grant deny commit과 기존 writer/ApplyPermit drain 완료는
서로 다른 사실이다.

B `FolderCommand::Resume`/revoke와 C mutation endpoint가 같은 request를 재시도하면
cached Pending을 영구 반환하지 말고 durable lease/operation row와 `ApplyDrained`
상태를 다시 조회한다. blocker가 해소되면 같은 request가 `Complete`를 돌려주며,
아직 해소되지 않았을 때만 `Pending`과 다음 조회 시각 `retry_at`을 돌려준다.
`retry_at`이 지나도 completion을 추정하지 않는다. 네트워크 단절/clock quarantine은
같은 Pending이며 상세 anyhow chain 대신 기존 share 상태, retry time 및 내부 blocker
수를 연결한다. 이 DTO가 C 상태 모델과 B worker의 false-complete 및 owner 전체
share-revoked 오인을 막는 최소 변경이다.

## roster, N0 lookup 및 endpoint ownership

member catalog와 별도의 owner-signed roster를 둔다.

```rust
pub struct RosterEntry {
    pub owner: EndpointId,
    pub share: ShareId,
    pub member: EndpointId,
    pub address: EndpointAddr,
    pub permission: Permission,
    pub member_epoch: u64,
    pub heartbeat_at: u64,
    pub heartbeat_expires_at: u64,
}

pub struct SignedRoster {
    pub owner: EndpointId,
    pub share: ShareId,
    pub generation: SnapshotId,       // roster record generation, not member epoch
    pub entries: Vec<RosterEntry>,
    pub issued_at: u64,
    pub expires_at: u64,
    pub signature: Signature,
}

pub struct RosterHeartbeat {
    pub owner: EndpointId,
    pub share: ShareId,
    pub member: EndpointId,
    pub address: EndpointAddr,
    pub challenge: GrantNonce,
    pub sent_at: u64,
    pub signature: Signature,
}
```

owner는 authenticated QUIC `remote_id()`와 member signature를 대조한 heartbeat만
주소 hint로 반영하고 roster를 다시 서명한다. `address.id == member`를 매번 확인하고
90초 지난 heartbeat나 stale roster entry로는 grant를 발급하지 않는다. roster의
주소/online 상태는 membership permission이나 enrollment를 만들지 않는다.

`N0 lookup`은 이 owner-signed `EndpointAddr`로 연결할 transport hint일 뿐이다. N0가
발견한 id/address를 `Registry::enroll`, provider 선택, permission 확인에 직접
사용하지 않는다. consumer의 provider plan은 `SignedRoster` entry를 owner signature
검증한 뒤 최대 8개만 만든다.

동일 endpoint를 강제하기 위해 다음 opaque hook을 둔다.

```rust
pub struct ShareEndpointOwnership { /* private router endpoint + owner id */ }

impl ShareService {
    pub fn endpoint_ownership(&self) -> ShareEndpointOwnership;
    pub fn register_supplier_storage(
        &self, owner: EndpointId, share: ShareId, membership: &Membership,
        root: &Path, root_lease: Arc<RootLease>, index: Arc<LocalIndex>,
        store: Arc<Store>,
    ) -> Result<SupplierRegistrationGuard>;
    pub async fn resume_membership(
        &self, owner: EndpointId, share: ShareId,
        address_hint: Option<EndpointAddr>,
    ) -> Result<Membership>;
}
```

`ShareEndpointOwnership`는 raw endpoint bind나 legacy protocol registration을
외부에 열지 않고, E의 `connect_share_swarm`만 호출할 수 있는 crate-private handle로
둔다. `endpoint_id()`는 handle의 id와 반드시 같고, `ShareSession`과 member CAS
handler는 `self.router.endpoint().clone()`만 사용한다. B가 managed worker를 pause
후 resume할 때는 `Server::resume`만 호출하지 말고 `resume_membership` → 현재
membership/epoch 확인 → 필요한 경우 fresh roster/grant 순서를 지켜야 한다.

## TOCTOU를 막는 gate와 API 흐름

필수 호출 흐름은 다음과 같다.

1. consumer가 owner `Snapshot` control request를 보내 signed token과 complete
   records를 받는다. `root_hash`, count, record validation 및 checkpoint를 확인한
   뒤에만 owner metadata를 기준으로 삼는다.
2. 각 file의 `ManifestAttestation`을 owner에서 받고, provider마다 정확한 hash
   subset에 대해 `request_swarm_grant(provider, snapshot, attestation, hashes)`를
   fresh 호출한다. consumer는 provider가 보낸 record/metadata를 절대 merge하지
   않는다.
3. E가 `open_share_swarm(roster_entry, grant, attestation, hashes)`를 호출하면
   `share-swarm/1` handler가 grant/manifest/request hash/roster endpoint/CAS
   membership을 검증하고 provider가 owner-signed `ActivateGrantReply`를 받아야
   bytes를 열 수 있다. consumer도 reply signature, nonce, provider/consumer를
   검증한다. scheduler는 기존 rarest-first 계산을 사용할 수 있지만
   provider 연결은 이 grant adapter 뒤에만 둔다. 2개 provider에 서로 다른 subset을
   배정하고 source id를 결과에 보존한다.
4. CAS staging은 유효 grant 아래 허용하되 namespace/index를 변경하지 않는다. 새
   `revalidate_before_apply(snapshot_token)`은 owner에 current consumer epoch와
   현재 root를 확인해 `ApplyPermit`(정확히 10초 이하)을 받는다. member는 실제
   filesystem action 전에 이 permit의 `ApplyStart`를 owner lease table에 등록한다.
5. `ManagedSyncEngine`의 share gate를 permit revalidation 직전에 획득하고, permit
   검사/`ApplyStart`부터 `DiskAdmission`, `Store::materialize*`, readonly bit 변경 및
   `LocalIndex::adopt_authoritative_snapshot` redb commit, 모든 blocking writer join과
   `ApplyDrained`까지 같은 gate/drain scope로 묶는다. permit 만료/epoch/root
   mismatch/owner offline이면 materialize와 adoption을 하지 않고 pending journal 및
   verified staging만 보존한다. local path observation mismatch도 같은 실패
   경로다.

owner 쪽에서도 snapshot scan과 root metadata token 발급을 runtime `gate` 아래에서
수행한다. revoke는 같은 gate와 operation guard drain을 기다린다. 이것은 local
lock만으로 remote revoke와 경합을 없앤다고 주장하는 모델이 아니다. 이미
`ApplyStart`된 operation과 writer의 실제 `ApplyDrained`/join을 완료 경계로 추적한다.
partition에서 검사를 못 받은 member가 permit을 이미 얻은 짧은 window는 허용하되,
활성 lease가 끝나도 ack가 없으면 `Pending`을 유지하고 새 반영만 차단한다.

공개/비공개 메서드의 최소 표면은 다음과 같다.

```rust
impl ShareSession {
    pub async fn fetch_authoritative_snapshot(
        &self, local: &MerkleTree,
    ) -> Result<AuthoritativeSnapshot>;
    pub async fn request_manifest(
        &self, snapshot: &SnapshotToken, record: &SyncRecord,
    ) -> Result<ManifestAttestation>;
    pub async fn request_swarm_grant(
        &self, provider: EndpointId, snapshot: &SnapshotToken,
        manifest: &ManifestAttestation, hashes: &[Hash32],
    ) -> Result<ShareGrant>;
    pub async fn revalidate_before_apply(
        &self, snapshot: &SnapshotToken,
    ) -> Result<ApplyPermit>;
    pub async fn refresh_roster(&self) -> Result<SignedRoster>;
    pub async fn heartbeat(&self, challenge: GrantNonce) -> Result<()>;
    pub(crate) fn open_share_swarm(
        &self, entry: &RosterEntry, grant: ShareGrant,
        manifest: ManifestAttestation, hashes: Vec<Hash32>,
    ) -> Result<ShareSwarm>;
}

impl OwnerShare {
    pub async fn revoke_member_strong(
        &self, peer: EndpointId,
    ) -> Result<RevocationReceipt>; // Complete or Pending, never false Complete
}
```

`ShareSwarm::fill_chunks_with_admission`은 `max_sources <= 8`, provider당 stream 2,
inflight 8, request hash 64를 강제하고, source별 `EndpointId`, transferred hashes,
verified bytes 및 grant nonce를 결과에 남긴다. 실패 provider의 subset만 fresh grant로
다른 roster provider에 재배정한다. owner fallback은 `ShareSession`의 authenticated
share/3 `pull_record_to_with_budget`뿐이며, `ShareSession::legacy_session()`을
통해 `sync/3` CAS scheduler로 우회하지 않는다.

이 method가 provider 쪽에서 실행될 때는 `ActivateGrantRequest`를 내부 필수 단계로
수행한다. grant를 받은 것만으로 `send_requested_chunks` 또는 blocking writer를
시작할 수 없다. activation 성공 전/monotonic deadline 후에는 hash availability도
전송하지 않고, 같은 nonce로 재연결하지 않는다.

## observer 계약과 즉시 B hook

기존 `TransferObserver`의 `{phase,path,direction,bytes,peer}`를 깨지 않도록 별도
share event를 additive하게 둔다.

```rust
pub struct ShareTransferEvent {
    pub operation_id: [u8; 16],
    pub share: ShareId,
    pub peer: EndpointId,
    pub phase: SharePhase, // Query, Manifest, Grant, Swarm, Drain, Materialize, Reject, Done
    pub direction: TransferDirection,
    pub bytes: u64,
    pub epoch: PermissionEpoch,              // consumer epoch
    pub provider_epoch: Option<PermissionEpoch>,
    pub grant: Option<GrantNonce>,
}
```

`ShareService::set_share_observer` 또는 `ShareSession::with_share_observer`로 등록하고,
operation guard가 `Started`를 emit한 뒤 모든 writer가 끝난 후에만 `Done`/`Reject`를
emit한다. Query-only, grant 거절, revoke-pending도 event를 남긴다. member inventory,
N0 lookup, roster address가 보였다는 사실은 `peer_seen`/online 전송 event로 세지
않는다. B worker는 이 event를 기존 UI observer로 변환하되 `share`, operation id,
provider id를 버리지 않는다.

즉시 B/D/E 경계는 다음과 같다.

* **B** — `crates/deltaweave-control/src/worker.rs:70-97,334-347`와 owner-side Resume dispatch:
  managed engine startup에서
  기존 `RootLease/Arc<LocalIndex>/Arc<Store>`를 한 번
  `register_supplier_storage`에 연결하고 guard를 worker lifetime에 보관한다.
  owner-side server handler까지 `Operation::Resume`를 처리하며 authenticated
  `remote_id()`를 registry binding과 대조한다. process/worker Resume은
  `resume_membership`과 clock/deny/deadline check를 먼저 통과해야 `sync_once`를
  스케줄한다. paused/revocation-pending 상태에서 새 `cycle`을 시작하지 않는다.
* **D** — `crates/deltaweave-net/src/share/{mod,wire,registry,runtime,service}.rs`:
  existing member epoch plus separate v1 lease/roster tables, resume primitive,
  roster/heartbeat, snapshot/manifest/grant/revalidate control, endpoint ownership,
  observer dispatch 및 strong revoke 상태를 소유한다. B가 호출할 registry/handler
  primitive를 제공하되 Resume worker/handler integration을 D 완료로 미루지 않는다.
  `net/src/lib.rs`에서는 ALPN 상수와 opaque endpoint adapter만 맡긴다.
* **E** — 신규 `crates/deltaweave-net/src/share/swarm.rs`, `net/src/lib.rs`의
  share-swarm thin adapter, `crates/deltaweave-sync/src/transport.rs`의 managed
  transport와 필요한 `shared.rs`/`read_only.rs` stage/apply hook. 기존
  `SwarmHandler`, `start_server_observed`, `ALPN_SWARM_V3`의 allowlist/CAS semantics를
  호출하거나 ShareSession의 `legacy_session()`을 구현하지 않는다.

## 현재 코드의 위험한 갈림길

* `crates/deltaweave-net/src/share/service.rs:47-65`는 endpoint를 `share/3` 하나만
  accept한다. 여기에 `deltaweave/sync/3`를 직접 붙여 swarm을 얻으면 managed와
  legacy CAS 권한 경계가 사라진다. `share-swarm/1`의 grant-only handler를 별도로
  accept해야 한다.
* `share/wire.rs:31-39,51-68`은 version 3 및 postcard strict decode다. 기존 Hello나
  Reply 필드를 바꾸면 compatibility가 깨지므로 additive operation만 사용한다.
* `share/registry.rs:14,57-70,402-417`에는 persistent lease/roster/deny tables가
  없고 revoke는 해당 member epoch만 올린다. `authorize`도
  `share/registry.rs:349-363`의 현재 membership/permission만 검사한다. 이 값을
  새 grant 검증의 유일한 세대로 재사용하면 partition revoke가 조기에 완료된다.
* `share/registry.rs:285-346`은 같은 endpoint가 재-enroll하면 기존 member를
  반환하고 ticket permission을 다시 권위로 삼지 않는다. 이것은 resume에는 유용한
  동작이지만, caller가 role/replica를 넘겨 새 lease를 만들 API로 노출하면 안 된다.
* `share/service.rs:292-330`의 `open_session`은 저장된 owner relationship과 동일
  endpoint를 확인하지만 session membership을 cache하고, `admit_member_root`는
  `RootLease`만 반환한다. index/store와 permission lease가 한 lifecycle로 묶이지
  않는 것이 B의 현재 TOCTOU/재시작 갈림길이다.
* `share/runtime.rs:252-277`은 알려진 connection을 닫고 gate를 기다릴 뿐 direct
  provider lease, writer drain deadline 또는 partition `Pending` 상태를 추적하지
  않는다.
* `share/ticket.rs:71-96,122-149`의 expiry는 `Option<u64>`이고 verify는
  `SystemTime` wall 값 하나만 비교한다. 새 swarm grant에 ticket을 bearer처럼
  재사용하면 unbounded TTL, rollback, late replay가 된다.
* `crates/deltaweave-net/src/lib.rs:59-80`의 `TransferObserver`에는 share,
  operation id, epoch, grant nonce가 없어서 B가 provider transfer와 inventory를
  구분할 수 없다. additive share event가 필요하다.
* `net/src/lib.rs:388-500`은 legacy server에 sync/1, sync/2, sync/3 및
  `SwarmHandler`를 모두 attach하고, `net/src/lib.rs:3244-3448`은 allowlist 뒤에서
  arbitrary hash list를 읽는다. 이것은 legacy CLI용 계약이며 managed ShareService에
  붙일 수 없다.
* `net/src/lib.rs:1378-1398`은 managed share swarm 호출을 명시적으로 실패시키고,
  `sync/src/transport.rs:5-33,67-73`의 `ShareSession`은 legacy session을 반환하지
  않는다. 이 guard를 제거해 `sync/3`를 재사용하는 방식은 요구된 격리를 위반한다.
* `sync/src/shared.rs:100-148,219-243`은 static cached membership과 빈
  `swarm_sources`로 engine을 열고, `sync/src/read_only.rs:140-149,227-317`은
  owner snapshot 뒤에 stage/materialize/adopt를 수행하지만 final owner
  revalidate/ApplyPermit이 없다. 특히 `read_only.rs:294-317`의 materialize와
  redb adoption 사이에 owner revoke가 오면 local gate만으로 remote 권한을 증명할
  수 없다.
* `net/src/lib.rs:685-696`의 `RemoteSnapshot`은 records/root/count만 있고 owner
  signature/token이 없다. `net/src/lib.rs:2227-2348`의 query/pull auth는 현재
  connection membership 확인이지 owner-confirmed snapshot/manifest proof가 아니다.
  `net/src/lib.rs:2351-2504`는 CAS receive 후 final causal adoption gate를 잡으므로,
  새 grant stage는 verified orphan을 허용하되 namespace 반영 전 permit을 다시
  확인해야 한다.

## 수용 테스트 최소 세트

1. **동일 endpoint 저장소 등록** — member가 owner/share relationship의 동일
   `ShareService` endpoint로 이미 열린 root, private state, `RootLease`, `Arc<LocalIndex>`,
   `Arc<Store>`를 supplier guard에 연결한다. root/state overlap, symlink alias, 다른
   share의 state, owner root를 주면 registration이 실패하고 파일과 registry가
   바뀌지 않는다. guard를 drop하기 전에는 두 번째 standalone provider/store open이
   허용되지 않는다.
2. **resume exactness** — enrollment reply를 잃고 process를 재시작한 member가
   `resume_membership(owner, share, address_hint)`로 authenticated remote identity의
   기존 replica/permission/member epoch를 exact 복구한다. revoked member, nonmember,
   identity/address mismatch, caller-supplied role/replica는 거부되고 새
   replica/relationship는 생기지 않는다.
3. **share/3 compatibility와 endpoint 격리** — 기존 postcard strict v3
   Validate/Enroll/Session이 통과하고 malformed/noncanonical Hello는 거부된다.
   managed endpoint의 sync/1, sync/2, sync/3와 grant 없는 share-swarm/1 연결은
   모두 거부되며 legacy server의 manual CAS swarm 테스트는 그대로 통과한다.
4. **snapshot/manifest proof** — owner signed snapshot의 root/count와 records를
   다시 계산하고, tampered signature, cross-share/epoch/snapshot, tombstone 또는
   record/manifest hash mismatch를 거부한다. 16 MiB page를 넘는 snapshot도 같은
   token root로만 통과한다.
5. **request membership/content proof** — duplicate/out-of-manifest hash,
   modified manifest, wrong consumer/provider, stale epoch/expiry/request hash를
   provider가 거부한다. consumer epoch뿐 아니라 provider member epoch를 올린
   revoke도 기존 grant/activation을 거부한다. 정상 chunk는 `VerifiedChunk`
   hash/length 검증 뒤에만 CAS에 들어가며 corrupt/missing source는 `Missing`으로
   끝난다.
6. **실제 2-provider transfer** — owner, consumer, provider A/B를 roster에 두고
   서로 다른 hash subset에 대해 각각 owner grant를 발급한다. 두 provider가 각자
   owner에 fresh `ActivateGrantRequest`를 먼저 보내고, distinct endpoint가 실제
   bytes를 보낸 뒤 result/observer에 두 source id가 남으며 최종 file hash가 맞는다.
   9번째 provider, provider당 세 번째 stream, inflight 9개는 거부한다.
7. **RO 권한과 metadata authority** — RO member의 local add/edit/delete가 owner
   snapshot, manifest, provider roster 또는 tombstone으로 전송되지 않는다. owner의
   signed addition/deletion만 반영되고, provider가 보낸 record/path/metadata는
   protocol error다.
8. **stage/apply TOCTOU** — CAS stage 뒤 owner consumer member epoch/root를 바꾸거나
   `revalidate_before_apply`를 expire시킨다. materialize, readonly update,
   `adopt_authoritative_snapshot`가 모두 실행되지 않고 pending journal/staging이
   보존된다. fresh 10초 permit과 같은 token으로만 atomic apply가 통과한다.
9. **partition revoke/drain** — provider를 owner와 단절한 채 in-flight stream을
   시작한다. owner revoke는 즉시 affected member epoch/lease deny를 commit하고 status는
   `Pending`; known writer/connection 및 `ApplyStart` operation이 모두 drain된 뒤에만
   `Complete`다. 미활성 issued grant의 120초 expiry를 기다리지 않으며, 활성
   lease의 `active_deadline + 5s`는 grant admission 경계일 뿐 timeout 완료가 아니다.
   late buffered bytes는 discard/staging만 되고 complete 이후 새 file/index adoption은
   0이다.
10. **clock/restart/replay** — wall clock을 6초 이상 rollback하면 grant issuance,
    send, materialize가 quarantine되고, provider가 old grant를 owner에 fresh
    `ActivateGrant`하지 않으면 전송하지 않는다. activation 요청의
    `Instant + 15s`가 지나거나 restart 후 active nonce/replayed grant가 오면
    거부된다. old grant의 expiry/nonce가 restart로 연장되지 않으며 owner와 fresh
    resume/grant/activation 뒤에만 진행된다.
11. **roster와 N0 분리** — unsigned address, `address.id != member`, 90초 지난
    heartbeat, N0-only discovered endpoint는 provider로 선택되지 않는다. owner가
    challenge heartbeat를 검증하고 서명한 roster만 주소 변경/새 grant에 반영된다.
12. **observer/fallback** — query/grant/swarm/drain/materialize/reject/done event가
    operation id, share, provider, epoch, bytes와 함께 순서대로 보인다. provider
    실패 시 fresh owner-authenticated share/3 pull만 fallback하고 `sync/3` legacy
    handler가 managed path에 호출되지 않는다.
13. **B/C pending DTO** — 권한 deny commit 직후에는 해당 member만
    `revocation_pending=true`, `MutationResult { completion: pending,
    retry_at: ... }`, `mutationpending.status=waiting`으로 보인다. 같은 request를
    retry해 durable `ApplyDrained`/writer 상태가 끝나면 `completion: complete`가
    반환되고, owner의 다른 share나 unrelated member가 revoked로 표시되지 않는다.

이 계약을 지키면 owner는 최신 namespace metadata와 tombstone의 유일한 권위로
남고, RO member는 owner가 확정한 manifest의 검증 chunk만 공급할 수 있다. 두
provider의 실제 전송, 재시작/replay/clock rollback 방어, partition에서의 논리적
철회 완료 경계가 모두 finite lease와 durable epoch/deny 상태로 관찰 가능해진다.
