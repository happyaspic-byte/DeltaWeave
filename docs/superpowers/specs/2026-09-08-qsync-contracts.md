# qBittorrent 스타일 폴더 공유 동기화 계약

작성일: 2026-09-08
계약 단계: A 조사·설계
기준 커밋: `baed5c0164a2e10fdbe6a1e5a31f7e897c675ced`
기준 브랜치: `origin/main`과 일치하는 `main`
문서 상태: 구현 전 계약. 이 문서만으로 기능 완료를 판정하지 않는다.

이 문서는 DeltaWeave를 사용자가 폴더를 공유하고 RO/RW 키를 다른 기기에
붙여 넣어 동기화하는 제품으로 확장하기 위한 B control, C web의 실행 계약과
D roster/discovery, E share-swarm의 입력 경계를 정한다. 실제 구현·테스트·브라우저·
Windows·인터넷 검증과 main 통합이 끝날 때까지 전체 goal은 완료되지 않는다.

## B/C 최소 DTO·메서드·route 표

아래 표가 구현자가 따라야 할 최초의 공통 계약이다. Rust 내부 메서드는
`deltaweave-control`의 타입을 사용하고 HTTP는 같은 의미를 JSON 문자열·객체로
표현한다. `ShareId`와 `InvitationId`는 HTTP에서 32바이트 값의 소문자 hex
64자 문자열이며, `member_id`는 Manager가 생성하는 불투명 핸들이다. endpoint ID,
IP, UDP 포트, ReplicaId는 기본 UI DTO에 넣지 않는다.

모든 public ID wrapper는 현재 net의 `ShareId([u8; 32])`와
`InvitationId([u8; 32])`를 그대로 사용하고, Rust serde의 배열 표현을 HTTP에
노출하지 않는다. HTTP path/body의 share와 invitation은 정규화된 소문자 hex만
받는다. `member_id`와 `request_id`는 opaque UTF-8 문자열로 각각 128바이트 이하,
빈 문자열·제어 문자·경로 구분자를 거부한다. 서버가 만드는 `member_id`는 같은
active membership 동안 안정적이며 endpoint ID에서 파생한 값을 외부에 노출하지
않는다.

`Permission`과 `ShareRole`은 `read_only`, `read_write`, `owner`, `member`의
snake_case 문자열만 사용한다. `ManagedStatus`, `EnrollmentState`, `KeyIssuance`도
아래 표의 snake_case 값을 사용한다. unknown enum은 API에서 422로 거부하고,
persistent state에서 읽히면 fail closed하여 임의의 `error` 값으로 바꾸거나 저장하지
않는다. 모든 timestamp는 UTC Unix seconds인 JSON number
`u64`다. `expires_at`은 현재 시각보다 뒤여야 하고 발급 시각에서 구현체가 정한 최대
TTL을 넘을 수 없다. `None`은 만료 없음이며 `last_sync_at`, `retry_at`,
`last_seen_at`, `revoked_at`에는 null을 사용한다.

### Manager 공개 표면

| 메서드 | 입력 | 반환 | 소유자·부작용·멱등성 |
| --- | --- | --- | --- |
| `Manager::open_with_options(data_dir: PathBuf, options: ManagerOptions)` | 사설 data dir, 실행 중 주입 옵션 | `Result<Arc<Manager>>` | B. 기존 `open`은 `Default`로 위임한다. managed config/pending가 없으면 ShareService를 열지 않는다. |
| `Manager::create_share(input: CreateShareInput)` | `request_id`, 이름, 현재 기기의 source root, free-space reserve | `Result<ShareView>` | B. owner share와 worker를 만든다. 같은 request hash는 같은 share를 돌려주고 다른 hash는 `IdempotencyConflict`다. 기존 manual folder와 겹치면 거부하고 manual 설정은 건드리지 않는다. |
| `Manager::preview_share_key(input: PreviewKeyInput)` | `request_id`, encoded key | `Result<KeyPreview>` | B. 로컬 서명·버전·구조·만료만 확인한다. enrollment와 issuance DB 검증을 하지 않는다. bearer는 반환하지 않는다. |
| `Manager::validate_share_key(input: ValidateKeyInput)` | `request_id`, encoded key | `Result<KeyPreview>` | B와 net hook. owner에 온라인 접속해 durable issuance와 revoke를 확인한다. offline이면 가입하지 않고 `Offline`이다. |
| `Manager::join_share(input: JoinShareInput)` | `request_id`, encoded key, destination root | `Result<JoinResult>` | B. root/private state를 먼저 예약하고 실제 `enroll`을 수행한다. offline이면 private pending과 `waiting`을 저장한다. 기존 manual folder와 겹치면 거부한다. |
| `Manager::resume_membership(input: ResumeMembershipInput)` | `request_id`, expected share ID | `Result<JoinResult>` | B와 net hook. key 재입력이나 `enroll`을 하지 않고 현재 authenticated endpoint의 active membership만 조회한다. role/ReplicaId를 재발급하지 않는다. |
| `Manager::list_shares()` | 없음 | `Result<Vec<ShareView>>` | B. durable config와 실제 worker 상태를 합친다. membership 등록만으로 online으로 표시하지 않는다. |
| `Manager::list_keys(share: ShareId)` | share ID | `Result<Vec<KeySummary>>` | B. owner만 호출한다. raw key/bearer는 반환하지 않고 invitation digest의 안전한 요약만 반환한다. |
| `Manager::issue_key(input: IssueKeyInput)` | share ID, `request_id`, `Permission`, optional expiry | `Result<IssuedKey>` | B. owner만 호출한다. registry에는 digest만 남기고 raw key는 이 응답에서 한 번만 반환한다. |
| `Manager::revoke_key(input: RevokeKeyInput)` | share ID, invitation ID, `request_id` | `Result<MutationResult>` | B. 신규 enrollment만 막는다. 이미 전달된 파일을 회수한다고 표시하지 않는다. 반복 요청은 같은 결과다. |
| `Manager::rotate_key(input: RotateKeyInput)` | share ID, invitation ID, `request_id`, optional expiry | `Result<IssuedKey>` | B. 기존 invitation을 revoke하고 새 key를 한 번 반환한다. old/new 처리 결과를 idempotency journal에 남긴다. |
| `Manager::list_members(share: ShareId)` | share ID | `Result<Vec<MemberView>>` | B. owner share에서만 허용한다. member는 `OwnerOnly`다. endpoint ID와 주소는 반환하지 않는다. |
| `Manager::revoke_member(input: RevokeMemberInput)` | share ID, opaque `member_id`, `request_id` | `Result<MutationResult>` | B. owner만 호출한다. denial을 먼저 durable하게 기록하고 연결·gate·writer를 drain한 뒤 반환한다. |
| `Manager::remove_share(input: RemoveShareInput)` | share ID, `request_id` | `Result<MutationResult>` | B. owner/member worker와 lease를 정지하고 managed catalog/config만 원자적으로 제거한다. local root, state, index, CAS, identity는 이 goal에서 삭제하거나 초기화하지 않는다. |
| `Manager::share_command(input: ShareCommandInput)` | share ID, `request_id`, `sync`/`pause`/`resume` | `Result<ShareView>` | B. pause는 신규 작업을 막고 진행 중 blocking 작업을 drain한다. resume은 같은 device identity를 재사용한다. |
| `Manager::snapshot()` | 없음 | `AppSnapshot` | B/C. 기존 folders/devices/settings/history를 유지하고 managed shares/pending를 additive field로 넣는다. 비밀은 절대 넣지 않는다. |
| `Manager::shutdown()` | 없음 | `Result<()>` | B. 모든 legacy/managed worker task와 persistence/SSE 참조를 회수하고 ManagedSyncEngine, 단일 ShareService 순서로 종료한다. 반복 호출은 성공이다. |

`Manager::open(data_dir)`의 기존 public signature, legacy `FolderInput`의
`sync`/`receive` enum tag, 기존 `FolderView` 필드, `WebApp` 단일 폴더 API는
계속 지원한다. 새 managed 흐름이 기존 수동 identity, index, CAS, root, allowlist를
초기화하거나 덮어쓰면 계약 위반이다.

### 내부·JSON DTO

다음 타입은 `deltaweave-control/src/model.rs`에 두고 필요한 타입만 `pub`로
노출한다. `Permission`은 `deltaweave_net::share::Permission`의
`read_only`/`read_write` serde tag를 재사용한다.

```rust
pub struct ManagerOptions {
    pub managed_network: NetworkMode,
    pub managed_bind: Option<SocketAddr>,
}
impl Default for ManagerOptions {
    // managed_network = NetworkMode::Internet, managed_bind = None
}

pub enum ShareRole { Owner, Member }
pub enum ManagedStatus {
    Waiting, Offline, InitialSync, Complete,
    Conflict, Revoked, Error, Paused,
}

pub struct CreateShareInput {
    pub request_id: String,
    pub name: String,
    pub root: PathBuf,
    pub min_free_space_mib: Option<u64>,
}
pub struct PreviewKeyInput { pub request_id: String, pub encoded_key: String }
pub struct ValidateKeyInput { pub request_id: String, pub encoded_key: String }
pub struct IssueKeyInput {
    pub request_id: String,
    pub share: ShareId,
    pub permission: Permission,
    pub expires_at: Option<u64>,
}
pub struct JoinShareInput {
    pub request_id: String,
    pub encoded_key: String,
    pub destination_root: PathBuf,
}
pub struct ResumeMembershipInput {
    pub request_id: String,
    pub share: ShareId,
}
pub struct RevokeKeyInput {
    pub request_id: String,
    pub share: ShareId,
    pub invitation: InvitationId,
}
pub struct RotateKeyInput {
    pub request_id: String,
    pub share: ShareId,
    pub invitation: InvitationId,
    pub expires_at: Option<u64>,
}
pub struct RevokeMemberInput {
    pub request_id: String,
    pub share: ShareId,
    pub member_id: String,
}
pub struct RemoveShareInput {
    pub request_id: String,
    pub share: ShareId,
}
pub struct ShareCommandInput {
    pub request_id: String,
    pub share: ShareId,
    pub command: ShareCommand,
}
pub enum ShareCommand { Sync, Pause, Resume }

pub struct ShareView {
    pub share_id: String,
    pub name: String,
    pub role: ShareRole,
    pub permission: Option<Permission>,
    pub root: String,
    pub status: ManagedStatus,
    pub phase: Option<String>,
    pub last_sync_at: Option<u64>,
    pub retry_at: Option<u64>,
    pub files_count: u64,
    pub total_bytes: u64,
    pub transferred_bytes: u64,
    pub speed_bps: u64,
    pub active_peer_count: u32,
    pub connected_devices: Vec<ConnectedDeviceView>,
    pub last_error: Option<ErrorSummary>,
}
pub struct KeySummary {
    pub invitation_id: String,
    pub share_id: String,
    pub permission: Permission,
    pub issued_at: Option<u64>,
    pub expires_at: Option<u64>,
    pub revoked_at: Option<u64>,
}
pub struct ConnectedDeviceView {
    pub member_id: String,
    pub permission: Permission,
    pub active_operations: u32,
    pub last_seen_at: Option<u64>,
}
pub struct MemberView {
    pub member_id: String,
    pub permission: Permission,
    pub enrolled_at: u64,
    pub revoked_at: Option<u64>,
    pub active_operations: u32,
    pub last_seen_at: Option<u64>,
}
pub struct ErrorSummary { pub code: String, pub message: String }
pub struct KeyPreview {
    pub share_id: String,
    pub name: String,
    pub permission: Permission,
    pub invitation_id: String,
    pub expires_at: Option<u64>,
    pub signature_valid: bool,
    pub issuance: KeyIssuance,
}
pub enum KeyIssuance { NotChecked, Validated }
pub struct IssuedKey {
    pub request_id: String,
    pub share_id: String,
    pub invitation_id: String,
    pub permission: Permission,
    pub expires_at: Option<u64>,
    pub key: String, // display-once response field; only the private TTL file may retain it
}
pub struct JoinResult {
    pub request_id: String,
    pub share_id: String,
    pub enrollment: EnrollmentState,
    pub status: ManagedStatus,
    pub permission: Option<Permission>,
    pub member_id: Option<String>,
}
pub enum EnrollmentState { Waiting, Enrolled, Revoked, Error }
pub struct MutationResult {
    pub request_id: String,
    pub accepted: bool,
    pub status: ManagedStatus,
}
pub type ManagedShareView = ShareView;
pub struct PendingView {
    pub request_id: String,
    pub share_id: String,
    pub status: ManagedStatus,
    pub created_at: u64,
    pub retry_at: Option<u64>,
}
pub struct AppSnapshot {
    // 기존 fields는 그대로 유지한다.
    #[serde(default)] pub shares: Vec<ManagedShareView>,
    #[serde(default)] pub pending: Vec<PendingView>,
}
```

`ShareView.root`는 사용자가 자기 장치에서 선택한 저장 위치를 확인하는 데
필요하므로 기존 관리 화면의 로컬 경로 표시와 같이 반환할 수 있다. `state_root`,
`identity_path`, raw ticket, bearer, secret key, endpoint address, endpoint ID,
논리 ReplicaId, 내부 error chain은 일반 snapshot·activity·SSE·browser storage에
넣지 않는다. 관리자 로그인 session/CSRF 값도 share DTO에 섞지 않는다.

### HTTP route 표

모든 새 route는 기존 `protect` middleware 아래에 등록한다. static path를
`/{share_id}`보다 먼저 등록해 `preview`, `validate`, `join`, `resume`가 share ID로
해석되지 않도록 한다.

| HTTP | route | body/query | 성공 응답 |
| --- | --- | --- | --- |
| GET | `/api/v1/state` | 없음 | 기존 `AppSnapshot` + additive `shares`, `pending` |
| GET | `/api/v1/shares` | 없음 | `ShareView[]` |
| POST | `/api/v1/shares` | `{request_id, name, root, min_free_space_mib?}` | `ShareView`, 최초 201 또는 저장된 재생 결과 |
| POST | `/api/v1/shares/preview` | `{request_id, key}` | `KeyPreview` with `issuance: not_checked` |
| POST | `/api/v1/shares/validate` | `{request_id, key}` | `KeyPreview` with `issuance: validated` |
| POST | `/api/v1/shares/join` | `{request_id, key, destination_root}` | enrolled면 200, pending이면 202 `JoinResult` |
| POST | `/api/v1/shares/resume` | `{request_id, share_id}` | active membership 복구 200 |
| GET | `/api/v1/shares/{share_id}` | 없음 | `ShareView` |
| GET | `/api/v1/shares/{share_id}/members` | 없음 | owner만 `MemberView[]` |
| GET | `/api/v1/shares/{share_id}/keys` | 없음 | owner만 `KeySummary[]` |
| POST | `/api/v1/shares/{share_id}/keys` | `{request_id, permission, expires_at?}` | `IssuedKey` |
| POST | `/api/v1/shares/{share_id}/keys/{invitation_id}/rotate` | `{request_id, expires_at?}` | `IssuedKey` |
| POST | `/api/v1/shares/{share_id}/keys/{invitation_id}/revoke` | `{request_id}` | `MutationResult` |
| POST | `/api/v1/shares/{share_id}/members/{member_id}/revoke` | `{request_id}` | `MutationResult` |
| DELETE | `/api/v1/shares/{share_id}` | `{request_id}` | `MutationResult` |
| POST | `/api/v1/shares/{share_id}/pause` | `{request_id}` | `ShareView` |
| POST | `/api/v1/shares/{share_id}/resume` | `{request_id}` | `ShareView` |
| POST | `/api/v1/shares/{share_id}/sync` | `{request_id}` | `ShareView` |
| GET | `/api/v1/browse?path=...` | 기존 query | 기존 authenticated server-folder browse 재사용 |

`preview`는 서명 검증과 key metadata preview만 한다. `validate`는 owner에
온라인으로 연결해 현재 invitation issuance를 확인한다. `join`은 destination
folder selection, path admission, enrollment, worker start를 순서대로 수행한다.
preview 성공을 enrollment 성공, `validate` 성공을 파일 동기화 완료로 표현하지 않는다.

HTTP body의 `key`는 control DTO의 `encoded_key`, `share_id`와
`invitation_id`는 각각 해당 path parameter 또는 DTO의 `ShareId`와
`InvitationId`로 변환한다. `CreateShareInput.root`와
`JoinShareInput.destination_root`는 `/api/v1/browse`가 실행되는 서버 기기의
정규화된 폴더 경로다. 브라우저가 보낸 임의의 local path를 client upload 경로로
해석하지 않는다. owner create의 `min_free_space_mib`는 0 이상 MiB 단위이며 내부
`min_free_space_bytes`로 변환할 때 overflow를 거부한다. 기존 manual folder와
managed root가 겹치면 403 또는 422의 안전한 path 오류로 거부한다.

`GET /keys`가 반환하는 `KeySummary`는 invitation의 발급·만료·폐기 시각과
permission만 담는다. digest 자체, bearer, encoded key는 담지 않는다. `POST /keys`
와 rotate의 raw key는 TLS/QUIC 및 로그인된 HTTPS 응답에서만 한 번 보여 준다.
동일 `request_id` 재시도는 registry에 중복 invitation을 만들지 않는다. raw 응답을
안전하게 재구성할 private 0600 response file이 5분 TTL 안에 남아 있으면 같은 key를
반환하고, 그 파일이 만료·삭제됐으면 `409 key_response_expired`를 반환하며 새 key를
자동 발급하지 않는다. 파일 권한·ACL은 기존 helper를 사용하고, 이미 검증된 AEAD가
있을 때만 재사용한다. 이 파일은 snapshot, activity, log, browser storage와
분리된다. 따라서 response loss가 새 권한이나 새 invitation으로 변하지 않는다.

HTTP 성공·실패 규칙은 다음과 같다.

| 상태 | 의미 |
| --- | --- |
| 200 | authenticated GET, online preview/validate, enrolled mutation, idempotent 재응답 |
| 201 | 새 owner share가 처음 durable하게 만들어짐. 같은 request ID의 재응답은 최초 결과와 같은 body/status를 사용한다. |
| 202 | join이 private pending으로 안전하게 저장됐으나 owner가 아직 offline이다. `Retry-After`를 넣을 수 있다. |
| 400 | malformed JSON, 잘못된 percent/path decode, request body 형식 오류 |
| 401 | 관리자 session 없음 또는 만료 |
| 403 | Host/Origin/CSRF 실패, owner-only 호출, private path, managed 권한 경계 위반 |
| 404 | 알 수 없는 share, invitation, opaque member handle |
| 409 | `idempotency_conflict`, duplicate, busy, 이미 revoke된 target의 충돌하는 mutation |
| 413 | 기존 64 KiB body limit 초과 |
| 422 | 구조화된 JSON field, key encoding, path, expiry, permission 오류 |
| 503 | owner offline를 pending으로 보존할 수 없는 경우, private state unavailable, idempotency journal capacity, shutdown 중 |
| 500 | 분류할 수 없는 내부 오류. raw error chain은 응답하지 않는다. |

새 error body는 기존 `{ "error": "..." }` 소비자를 깨지 않도록 다음 additive
형식을 사용한다.

```json
{
  "error": "안전한 오류 설명",
  "error_code": "invalid_ticket",
  "request_id": "사용자가 보낸 불투명 ID"
}
```

`error`와 `message`에는 키, bearer, private key, endpoint 주소, raw path,
redb 내용, `anyhow` chain을 넣지 않는다. `request_id`는 유효한 경우에만 반사한다.
기존 legacy route는 현재의 JSON error와 status를 유지하되 새 route도 같은
Host/Origin/CSRF/login 경계를 우회하지 않는다.

## 범위와 신뢰 경계

owner가 권한, 최신 snapshot, manifest의 기준이다. 관리형 member는 owner의
authenticated endpoint에만 연결하고 다른 member에게 snapshot이나 record를
제공하지 않는다. 완전 mesh, 공개 DHT, BitTorrent/qBittorrent wire compatibility,
새 public tag, MSI, Windows SCM 설치기는 범위 밖이다. 제품의 사용자 경험이
qBittorrent처럼 단순하다는 뜻은 파일 공유·키·자동 worker 흐름이며 BitTorrent
호환을 뜻하지 않는다.

새 관리형 전송은 `deltaweave/share/3`을 사용한다. 기존 endpoint allowlist
`deltaweave/sync/3` handler를 managed endpoint에 직접 연결하지 않는다. E의
보조 공급자는 별도 `deltaweave/share-swarm/1`로 제한하며, 실패 시 fallback은
owner의 authenticated `deltaweave/share/3` pull뿐이다.

기본 managed network는 iroh N0 discovery/relay를 사용하는 `NetworkMode::Internet`이다.
직접 주소를 입력하는 흐름은 기본 UI에서 제거하고, 재현 가능한 통합 테스트는
`ManagerOptions { managed_network: DirectOnly, managed_bind: ... }`를 주입한다.
이 옵션은 persisted config나 basic UI field가 아니다. 기존 manual folder의
DirectOnly, peer ID, direct address, allowlist 입력과 고급 설정은 그대로 둔다.

키 preview와 join은 다음처럼 분리한다.

1. `ShareTicket::parse`가 bounded canonical envelope, v3, 서명, owner/share/name,
   permission, expiry, address binding을 확인한다.
2. `preview`는 위 결과만 보여준다. online issuance, revoke, enrollment는 하지 않는다.
3. `validate`는 owner endpoint의 durable invitation digest와 permission을 확인한다.
4. `join`은 선택한 이 기기의 destination root/private state를 admission한 뒤 owner
   authenticated connection에서만 `enroll`한다.
5. 응답이 유실되면 같은 request ID 재시도 또는 `resume`으로 현재 endpoint의
   active membership을 조회한다. 새 invitation을 만들거나 role/ReplicaId를
   추측하지 않는다.

## 영속성·config schema·DB migration

### Manager config

현재 control config는 outer `version: 1`, `settings`, `folders`, `devices`,
`activities`, `history`를 사용한다. 새 field는 `#[serde(default)]`인
`managed` 하나를 additive로 추가하며 outer version을 2로 올리지 않는다. 따라서
기존 enum-tagged `FolderInput.role`과 빠진 field의 default를 유지한 old JSON은
그대로 읽힌다.

논리적 disk shape은 다음과 같다. `ticket_file`은 private directory의 상대 이름이며
JSON 안에 ticket/bearer를 넣지 않는다.

```json
{
  "version": 1,
  "settings": { "node_name": "DeltaWeave", "poll_interval_seconds": 30, "history_limit": 300 },
  "folders": [],
  "devices": [],
  "activities": [],
  "history": [],
  "managed": {
    "schema_version": 1,
    "shares": [
      {
        "share_id": "64-hex",
        "role": "owner",
        "permission": null,
        "name": "Documents",
        "root": "/selected/local/root",
        "state_root": "/private/managed/share-state",
        "owner": "private transport binding",
        "member_id": null
      }
    ],
    "pending": [
      {
        "request_id": "opaque-request",
        "share_id": "64-hex",
        "owner": "private transport binding",
        "root": "/selected/local/destination",
        "state_root": "/private/managed/pending-state",
        "ticket_file": "pending/opaque-request.bin",
        "created_at": 0,
        "expires_at": null
      }
    ],
    "intents": [
      {
        "request_id": "opaque-request",
        "operation": "create_share",
        "request_hash": "64-hex",
        "name": "Documents",
        "root": "/selected/local/root",
        "state_root": "/private/managed/share-state",
        "created_at": 0
      }
    ],
    "requests": []
  }
}
```

위 경로와 owner 값은 문서 shape를 설명하기 위한 값이며 실제 증거·snapshot에
그대로 노출하지 않는다. `managed.shares[].state_root`, pending 파일, device key는
`data_dir/managed` 아래 소유자 전용 private namespace에 놓고 root-admission에
영구 예약한다. public root를 그 namespace 아래에 만들 수 없다.

`requests`는 최대 1024개의 `{request_id, operation, request_hash, result_ref,
recorded_at}`만 보관한다. `request_hash`는 canonical JSON과 operation domain으로
계산한 BLAKE3 digest다. active 또는 resolved result reference는 용량이 찼다고
조용히 evict하지 않는다. 만료된 pending/response만 명시적 garbage collection
대상이 되며, stable idempotency reference를 보존할 공간이 없으면
`idempotency_capacity` 오류를 반환한다.

join key raw string은 메모리에서 지우고, owner offline pending을 위해 필요한
signed ticket만 별도 private 0600 파일에 보관한다. 저장 권한·ACL과 삭제에는
기존 private-file/Windows ACL helper를 재사용하고 새로운 암호를 설계하지 않는다.
기존 검증된 AEAD helper가 이미 있는 경우에만 그 helper로 envelope를 보호한다.
완료·취소·만료 후 ticket file을 unlink하고 부모 directory를 sync한다. issue/rotate
raw key response도 별도 private 0600 response file에 최대 5분만 보관하고 같은
helper/TTL 규칙을 사용한다. public config, snapshot, log, activity에는 어느 raw
credential도 쓰지 않는다.

### Migration 규칙

| 저장소 | 현재 schema | A에서 확정한 migration |
| --- | --- | --- |
| `data_dir/config.json` | outer v1 | `managed` missing은 empty default. 기존 v1을 읽고 다음 atomic save 때만 field를 쓴다. unknown outer version은 기존처럼 거부한다. |
| `data_dir/managed/shares.redb` | `owner_share_catalog_v3`, Catalog version 3 | B/C는 table을 직접 조작하지 않고 기존 `ShareService` API를 사용한다. 새 invitation issued-at metadata는 serde default가 있는 additive field로만 저장하고, old row는 `issued_at: null`로 표시한다. catalog mismatch/corruption은 `state_unavailable`로 닫고 reset하지 않는다. |
| member/owner `index.redb` | existing root/replica binding, `share_authorization_metadata_v1` slot | missing metadata는 새 managed enrollment에서만 초기화한다. existing index의 binding, causal vectors, counter ceiling은 유지한다. malformed/rollback checkpoint는 거부한다. |
| managed store | existing path-change journal and CAS | existing `Store` recovery/metadata schema를 재사용한다. RO checkpoint와 causal provenance는 기존 private metadata slot에 version tag를 둔다. |
| pending enrollment | 없음 | `pending/v1/<request_id>.bin` postcard envelope를 atomic temp write, `sync_all`, rename으로 만든다. 기존 private-file/ACL helper를 사용하고 별도 암호를 만들지 않는다. pending TTL은 `min(ticket expiry, 7 days)`이며 expiry 없는 기존 ticket은 호환해 parse하되 pending 파일은 7일 상한을 적용한다. 최대 크기는 32 KiB이고 raw key는 config/로그에 복사하지 않는다. |

redb table을 새로 만들어야 하는 구현이라도 manager config와 registry의 한쪽만
성공한 상태를 성공으로 표시하지 않는다. path admission과 private reservation이
먼저, catalog intent와 config journal이 다음, worker publish가 마지막이다. 중간
종료 후 open은 marker/request record를 조정하고 active relationship을 확인한다.
데이터·root·index를 지우거나 임의로 초기화해 복구하지 않는다.

### create/join crash window

현재 `ShareService::create_owned_share`는 random `ShareId`를 만들어 registry에
intent를 넣은 뒤 runtime을 load하므로, Manager config save 전에 process가 죽으면
catalog에 orphan처럼 보이는 owned config가 남을 수 있다. B는 `create_share` 호출
전에 canonical root, 안정적인 private `state_root`, name, request hash, request ID를
`managed.intents`에 durable하게 기록한다. `state_root`는 retry마다 바뀌지 않는다.
그 후 service를 호출하고 Manager config를 저장한다. open 시 `owned_configs()`와
미해결 create intent를 owner, canonical root, exact state_root, name, request hash로
대조한다. 일치하는 단 하나의 catalog config가 있으면 그 random ShareId와 동일
worker/config 결과를 복구하고 새 share를 발급하지 않는다. 일치하지 않는 catalog
entry는 삭제하지 않고 `state_unavailable`로 fail closed하여 명시적 repair 대상이
된다. 동일 root의 여러 일치 후보도 안전 오류다.

join은 network `enroll` 전에 trusted owner/share, selected destination root,
stable private state_root, request hash/ID, ticket file reference를 pending intent에
먼저 기록한다. process가 enroll 응답 직전에 죽으면 open/resume이 그 intent를 읽어
현재 authenticated endpoint의 active membership만 조회하고 기존 permission,
member binding, logical ReplicaId, epoch을 그대로 저장한다. active membership이
없을 때만 유효한 같은 pending ticket으로 original enroll을 재시도할 수 있으며,
새 invitation/role/ReplicaId를 만들지 않는다. ticket이 만료되었거나 owner가
offline이면 pending TTL 안에서 `waiting`을 유지한다. intent, pending file, catalog,
config가 모두 stable ref를 확인하기 전에는 `complete`나 `enrolled`를 반환하지 않는다.

### 기존 folder 전환

이번 goal의 create/join에는 `existing_folder_id` 필드가 없다. 기존 manual folder와
managed root가 겹치거나 ancestor/descendant이면 `root_admission`이 명시적으로
거부하며, 어느 설정·파일·index·CAS·identity도 자동 전환하거나 삭제하지 않는다.
기존 `FolderInput`의 manual type conversion이 이미 제공하는 원문 보존 범위인 파일,
index, CAS, identity, ReplicaId, allowlist, causal history는 그대로 유지하고 old
manual peer 연결도 계속 동작해야 한다.

legacy folder를 managed owner/member로 전환하는 기능, `LegacyProof`를 사용한
비어 있지 않은 index 이전, 기존 endpoint 교체는 별도 후속 설계로 남긴다. 후속
작업이 시작되더라도 기존 수동 설정과 데이터의 pre/post byte/hash 증거 없이는
전환하지 않는다.

## Manager 수명·worker 계약

`Manager`는 legacy folder slot과 managed share slot을 분리한다. managed slot의
owner는 `OwnerShare`, member는 `ManagedSyncEngine`, pending은 private ticket file과
root lease를 가진 worker다. 모든 managed slot은 하나의 `ShareService` endpoint를
clone해서 사용한다. share마다 endpoint나 device identity를 새로 만들지 않는다.

lazy-init 규칙은 다음과 같다.

- manual-only config를 `Manager::open`하면 기존 legacy worker만 시작하고 Internet
  endpoint 또는 N0 외부 의존성을 열지 않는다.
- managed share 또는 pending record가 있으면 open 중 single ShareService를
  시작하고 각 share worker를 resume한다. transient network failure는 `offline`/
  `waiting`으로 저장하고 manual worker를 멈추지 않는다.
- 처음 `create_share`, `validate`, `join`, `resume`가 호출되면 single service를
  만든다. service state/device key는 `data_dir/managed`에 고정하고 request input으로
  바꾸지 않는다.
- `ManagerOptions.managed_bind`는 test harness가 loopback DirectOnly를 고를 때만
  사용한다. basic UI와 persisted config에는 network mode, IP, port, device ID를
  요구하지 않는다.

worker는 250 ms legacy full-index polling을 복제하지 않는다. managed member는
`ManagedSyncEngine::sync_once`를 bounded exponential backoff로 재시도하고 owner는
local scan/debounced change와 observer event를 사용한다. `waiting`은 enrollment
pending, `offline`은 active membership은 있으나 owner/relay에 현재 연결하지
못하는 상태다.

shutdown 순서는 수명 경계의 일부다.

1. `lifecycle` exclusive lock을 잡고 신규 요청을 막고 `stopped`를 표시한다.
2. legacy `Worker::stop`과 managed pending/owner/member worker의 stop message를
   보내 task handle을 await한다.
3. 각 `ManagedSyncEngine::shutdown`이 pending operation과 `ShareSession`을 닫고
   root lease를 놓는 것을 확인한다. cancellation은 blocking disk 작업 완료의
   증거가 아니다.
4. periodic persistence task, retry timer, observer closure, SSE stream이 Manager
   또는 service를 붙잡지 않는지 join/close로 확인한다.
5. Manager가 유일하게 소유한 `ShareService`를 `shutdown(self)`으로 닫고 router,
   tracked QUIC connection, active handler, swarm task drain 완료를 await한다.
6. 모든 단계가 성공했을 때만 `shutdown`이 반환한다. 오류가 있으면 private state와
   status를 보존하고 error를 반환하며 다음 호출에서 안전하게 재시도한다.

service를 먼저 닫거나 Arc 참조를 남긴 채 `shutdown`이 성공하면 계약 위반이다.
owner revoke도 같은 원칙으로 denial persistence, connection close, per-share gate,
in-flight writer drain을 순서대로 수행한다.

## 상태 전이·오류·멱등성

managed status는 다음 8개만 외부 상태 값으로 사용한다.

| 상태 | 진입 조건 | 허용되는 다음 상태 |
| --- | --- | --- |
| `waiting` | ticket preview 후 owner offline, pending private state 저장 | `initial_sync`, `offline`, `error`, `paused`, `revoked` |
| `offline` | active membership 또는 owner runtime은 있으나 현재 endpoint/relay 연결 실패 | `initial_sync`, `complete`, `conflict`, `error`, `paused`, `revoked` |
| `initial_sync` | 새 enrollment 뒤 첫 authenticated snapshot/apply가 진행 중 | `complete`, `conflict`, `offline`, `error`, `paused`, `revoked` |
| `complete` | fresh scan, verified local root, owner root이 일치 | `initial_sync`, `conflict`, `offline`, `paused`, `revoked`, `error` |
| `conflict` | causal conflict 또는 RO preserved local change가 실제 존재 | `initial_sync`, `complete`, `offline`, `paused`, `revoked`, `error` |
| `revoked` | invitation/member revoke가 durable하게 확인됨 | terminal. 새 key로 별도 request만 가능 |
| `error` | malformed state, admission/recovery failure, non-retryable protocol error | bounded retry 후 `complete`/`offline`/`waiting`, 또는 terminal repair 안내 |
| `paused` | 사용자가 pause 명령을 완료함 | `waiting`/`offline`/`initial_sync`/`complete`/`conflict`, `revoked` |

owner가 offline인 동안 완전 mesh로 동작한다고 표시하지 않는다. partition 중에는
`offline` 또는 `waiting`을 표시하고 revoke 완료를 추측하지 않는다. `revoked`를
확인한 뒤에는 cached permission으로 push, pull, metadata adoption을 계속하지 않는다.

`connected_devices`와 `last_seen_at`은 다음 관측만 사용한다.

- durable membership row, stale inventory, invitation count는 online 증거가 아니다.
- authenticated operation이 시작할 때 peer별 active count를 증가시키고 finish,
  error, connection close에서 감소시킨다.
- query-only Merkle pass도 start/finish observation에 포함한다.
- `active_peer_count`는 그 순간 count이며 등록된 전체 member 수가 아니다.
- observer callback과 tracker는 동시 작업에서 lock-safe하고 peer ID를 UI에 내보내지
  않는다. secret/path redaction은 callback 전에 적용한다.

`request_id`는 create, issue, rotate, join, resume, revoke, command에 필수다.
preview/validate도 같은 DTO를 받아 retry 결과를 추적하지만 키 raw 값은 보관하지
않는다. canonical hash가 같은 재요청은 작업을 다시 수행하지 않고 저장된 public
result를 반환한다. 네트워크 응답이 끊겨도 join의 durable enrollment와 idempotency
record를 우선 확인한다. 동일 ID로 다른 key/destination/share를 보내면 409이며
기존 worker·catalog·파일을 변경하지 않는다.

## 인증·권한·전송 경계

### Key and enrollment

기존 `ShareTicket` contract를 사용한다.

- prefix `dwshare3:`, canonical postcard, bounded parser, v3 only
- owner 공개 key의 서명, share/name/permission/invitation/bearer/expiry/address
  binding
- registry에는 전체 issuance digest, permission, expiry, revoke만 보관
- `Permission`은 signed ticket가 아니라 client field를 authority로 쓰지 않는다.
- `enroll`의 endpoint는 `connection.remote_id()`이며 request body가 지정하는 peer가
  아니다.
- 같은 endpoint의 재-enroll은 같은 active membership만 반환한다. revoked endpoint는
  `member_revoked`다. 새 endpoint와 새 active invitation은 명시된 새 member로
  enroll할 수 있다.
- preview가 보이는 name/permission은 signature metadata일 뿐 online validity가
  아니다.

RO는 owner snapshot을 읽고 확정 청크를 공급할 수 있지만 local edit/delete/tombstone/
metadata를 share record로 승격하지 않는다. `read_only.rs`의 authoritative snapshot
adoption과 private recovery artifact를 재사용하고, local deletion은 owner content를
복구한다. 원격 deletion이나 type transition으로 local modified content를 덮기 전
같은 filesystem의 private vault에 보존하고, root 밖 artifact가 다음 scan으로
재유입되지 않게 한다.

### v3 causal checks

RW mutation은 owner가 현재 permission/epoch를 확인하고, known ReplicaId와 counter
ceiling, exact causal record, record hash/provenance를 함께 검증한다. vector ID를
서명이나 authenticated author라고 부르지 않는다. authenticated writer 자신의
counter만 trusted ceiling을 넘을 수 있고 다른 member counter 위조, unknown ID,
resolver jump, vector overflow를 거부한다. root/index/store materialization 직전,
index adoption 직전에도 권한을 다시 확인한다.

### Revocation and shutdown

member revoke는 새 요청을 막는 durable state를 먼저 commit하고 tracked connection을
닫은 뒤 per-share mutation gate와 already-started blocking writer를 drain한다. 완료
응답 후 새로운 file/index adoption이나 send가 없어야 한다. `pause`도 같은 writer
drain 관찰을 사용하지만 revoke처럼 membership을 철회하지 않는다. key revoke는
초대 신규 사용만 막으며 active member revoke와 별도다.

## D 입력 계약: roster, heartbeat, N0 discovery

D는 B의 `ShareView`·`ManagedStatus`·`ShareService` single endpoint를 소비하고
owner/member의 인증 roster와 주소 변경 관측을 제공한다. 기존 `OwnerShare::members`,
`set_observer`, `inventory`, `pause`, `resume`, `revoke_member` API는 재발명하지
않고 Manager에 연결한다.

response-loss 복구 hook은 B가 필수로 구현하고 service lifecycle과 함께 소유한다.
D는 이 hook을 소비하지, owner-side resume handler를 별도로 소유하지 않는다.
기존 `deltaweave/share/3`의 `Operation`과 `Reply` 뒤에 새 variant를 append하여
기존 variant ordinal과 ALPN을 보존한다. 구버전 peer가 모르는 variant를 받으면
안전한 protocol error로 끝나며 기존 `Validate`/`Enroll`/`Session` 동작은 바뀌지
않는다.

```rust
// B가 소유하는 response-loss recovery hook. D는 관측/주소 갱신만 소비한다.
pub async fn ShareService::resume_membership(
    &self,
    owner: EndpointId,
    share: ShareId,
    address: EndpointAddr,
) -> Result<Membership>;

// wire Operation::Resume / Reply::Resumed(Membership), enum의 마지막 variant로 append
// server authority: connection.remote_id(), expected share, active epoch only
```

`resume_membership`은 `Operation::Enroll`을 호출하지 않는다. B의 owner-side service handler는
현재 connection peer의 active membership을 조회해 같은 permission, member binding,
logical ReplicaId, epoch을 반환한다. owner/share mismatch, nonmember, revoked member,
expired local address, changed replica는 각각 safe error로 거부한다. 새 binding이나
role을 만들어 주는 resume 경로는 없다.

D는 `PeerObservation`을 private runtime에 유지한다. authenticated operation start,
finish, error, address change, heartbeat timestamp를 owner/share/peer에 묶고 UI에는
opaque member handle과 count만 보낸다. heartbeat는 liveness hint이며 membership
권한을 대체하지 않는다. 매 query/session/chunk 단계의 `Authorization::check`와
connection close drain은 유지한다.

N0/relay 규칙은 다음과 같다.

- Internet mode에서 `Endpoint::builder(presets::N0)`와 owner public identity를 쓴다.
- ticket의 initial address는 bootstrap hint일 뿐이며 identity lookup/address update가
  authoritative reconnect 경로다.
- DirectOnly는 test injection과 기존 manual advanced path에서만 사용한다.
- 주소가 변경되면 old IP를 UI에 요구하지 않고 endpoint identity/relay observation으로
  갱신한다.
- 실제 인터넷·NAT·relay 증거와 loopback DirectOnly 증거를 별도 로그로 남긴다.
- signed peer grant의 `expires_at`은 UTC wire timestamp를 유지하지만, provider가
  받은 시점부터 적용하는 monotonic lease/deadline 정책은
  `qsync_protocol_audit`의 보고를 받아 교체할 수 있는 pending contract다. audit
  결론 전에는 UTC 비교만을 liveness 권한으로 사용하지 않고, clock rollback/forward
  시험을 기록한다.

## E 입력 계약: share-swarm/1

E는 D roster와 B lifecycle을 소비해 관리형 보조 청크 전송을 구현한다. `deltaweave-
swarm`의 scheduler와 legacy `sync/3` CAS 검증 함수를 안전한 내부 building block으로
재사용할 수 있지만 legacy handler를 managed endpoint에 직접 붙이지 않는다.

새 ALPN은 정확히 `deltaweave/share-swarm/1`이다. 첫 request 하나에 owner가 서명한
grant와 exact manifest scope를 결합한다.

```rust
pub struct ShareSwarmGrant {
    pub version: u8,
    pub owner: EndpointId,
    pub share: ShareId,
    pub requester: EndpointId,
    pub provider: EndpointId,
    pub permission_epoch: u64,
    pub manifest_hash: Hash32,
    pub requested_hash: Hash32,
    pub expires_at: u64,
    pub nonce: [u8; 32],
    pub signature: Signature,
}
pub struct ShareSwarmRequest {
    pub grant: ShareSwarmGrant,
    pub manifest: FileManifest,
}
```

provider는 owner public key로 grant signature를 검증한다. provider 쪽에서 local
`endpoint.id()`가 `grant.provider`와 같아야 하고, incoming connection의
`connection.remote_id()`는 `grant.requester`와 같아야 한다. grant의
owner/share/requester/provider/epoch/
manifest/requested hash/expiry/nonce를 모두 서명 domain에 넣는다. `requested_hash`는
실제 `manifest.chunks`에 포함되어야 하고 manifest file hash와 record size도 exact
record와 일치해야 한다. 단순 field equality, arbitrary CAS hash lookup, cross-share
grant, stale epoch, expired nonce는 권한 부여 근거가 아니다.

한 managed fill은 최대 8 provider와 connection/queued-byte/disk budget을 갖는다.
위조 payload는 BLAKE3 rehash 즉시 폐기하고 source를 재시도/제외한다. provider
failure는 다른 authorized provider 재시도, pause/resume, owner authenticated
`deltaweave/share/3` pull fallback 순서다. fallback은 legacy allowlist CAS가 아니다.
manifest 밖 hash, owner 외 권위 snapshot, revoked epoch, disk reserve 초과는 파일
materialization 전에 거부한다. pause, root lease, shutdown drain, cancellation,
writer 완료를 모두 관찰한다.

## 저장·복구·호환성 상세

교체·삭제·directory/file type transition은 기존 `Store` path-change helper를
사용한다. private state가 다른 filesystem이면 destination volume의 root-bound
private sibling vault를 선택하고, 안전한 same-volume placement가 없으면 mutation
전에 실패한다. copy-then-unlink로 열린 handle write를 잃지 않는다. 준비·capture·
materialize·index adoption·commit 각 단계와 정확한 artifact/staging path를 journal로
남긴다. preserved artifact는 명시적 purge 전까지 유지하고 incoming staging과
혼동하지 않는다.

RO state는 trusted owner checkpoint와 pending stage를 저장한다. checkpoint가 rollback,
same-version different content, tombstone omission, unknown namespace이면 거부한다.
local precondition이 scan/apply 사이에 바뀌면 `LocalChanged`로 재시도하고 local
bytes는 보존한다. owner record를 적용하기 전에 current membership/epoch, causal
precondition, exact filesystem observation을 다시 검사한다.

기존 `SyncEngine`, `SyncSession`, `Server`, `WebApp`, CLI 수동 경로는 관리형 share와
분리된 상태로 회귀 없이 동작해야 한다. `SyncConfig.swarm_sources`는 legacy
allowlist path에만 적용하고 managed `ShareSession`의 folder-scoped auth를 우회하지
않는다. 기존 `FolderInput`의 `role` 값은 `sync`와 `receive` 그대로이고, 새 managed
`ShareRole`을 그 enum에 끼워 넣지 않는다.

## UI 계약과 비밀 처리

기존 한국어 콘솔의 Manrope, Noto Sans KR, IBM Plex Mono, 숲색 token, button/modal/
navigation/folder browse/activity/settings를 유지한다. `design-taste-frontend`는
이 화면이 dashboard·multi-step product UI임을 명시하므로 랜딩 hero, 사진, decorative
motion 규칙은 적용하지 않는다. 기존 audit의 낮은 motion, 예측 가능한 정보 구조,
접근성 label/focus/contrast를 유지한다.

기본 화면 동작은 다음 두 개다.

- `폴더 공유`: 서버 folder browse로 현재 기기의 source root를 선택하고 share를 만든
  뒤 `읽기 전용 키` 또는 `읽기/쓰기 키`를 각각 issue/copy한다.
- `키로 연결`: 긴 key를 memory-only input에 붙이고 local preview, 별도 online
  validate, 현재 기기의 destination folder browse, actual join을 순서대로 한다.

기본 흐름은 IP, port, device ID, allowlist를 묻지 않는다. 기존 manual 연결은
고급 설정으로 남긴다. folder picker의 `path`는 HTTP server가 실행되는 기기의
폴더이며 client browser의 임의 upload directory가 아니다. private management/data
dir와 기존 managed root는 browse response에서 제외한다.

브라우저는 raw key를 React memory state에서만 보관하고 localStorage, URL, query,
activity, error, analytics, SSE, screenshot, CI artifact에 쓰지 않는다. issue 응답은
copy 성공/실패를 UI에 알려야 하며 modal close, join success, cancel, expiry, logout
시 raw value를 clear한다. owner가 이미 전달한 파일은 key revoke로 회수되지 않는다는
문구를 유지하고, member revoke와 key revoke의 범위를 별도로 설명한다.

status label은 `waiting`, `offline`, `initial_sync`, `complete`, `conflict`,
`revoked`, `error`, `paused`를 실제 runtime state에서 표시한다. membership row나
last inventory만으로 online이라고 표시하지 않는다. 1440px와 390px, 200% zoom,
keyboard Tab/Escape/focus restore, copy failure, long key wrapping, label/aria,
contrast, no horizontal overflow, console/page error를 브라우저에서 확인한다.

## 완료 판정과 요구사항-증거 매핑

이 문서의 상태는 구현 전이다. 각 항목은 실제 명령·exit code·실행 시각·로그 경로와
파일/해시 증거가 있어야 `[x]`가 된다.

| 요구사항 | 필요한 증거 |
| --- | --- |
| owner 폴더 공유, RO/RW key 생성·copy | 3개 분리 identity의 실제 browser 흐름, redacted screenshot, `IssuedKey` 응답·copy 결과 |
| key paste, destination folder 선택, actual join | 3기기 browser/network log, 양쪽 파일 bytes/hash, membership transition |
| B one ShareService per Manager | Manager unit/integration test에서 endpoint ID 1개, manual-only lazy-init에서 Internet bind 없음, restart identity 동일 |
| owner/member/pending worker | private config/pending journal, worker state transition, restart/resume log |
| preview/online validate/actual join 분리 | preview가 enrollment 없음, validate offline/valid 구분, join만 active membership 생성하는 route test |
| request idempotency/response loss | same ID same hash 재실행, same ID different hash 409, lost reply 후 `resume` active lookup |
| role/ReplicaId 재발급 금지 | resume/restart test가 기존 permission/replica/epoch을 그대로 확인, revoked/nonmember 거부 |
| owner/member revoke, key rotation | key revoke와 member revoke 각각 신규/기존 session 효과, in-flight drain timestamp |
| RO semantics | local add/edit/delete 비전파, remote edit/delete 복구와 private preserved bytes, owner/RW unchanged |
| RW add/edit/delete/conflict/restart | 실제 owner/RW 2-way changes, conflict copy/hash, restart 후 action 0 또는 정확한 resume |
| D authenticated roster/heartbeat/address update | member row와 active operation count 구분, query start/finish, N0/relay/address-change log |
| N0 Internet default, DirectOnly test | ManagerOptions test mode evidence와 external internet/relay evidence 분리 |
| E share-swarm/1, grant+manifest scope | 2개 이상 provider payload hash, provider IDs, forged grant/hash/cross-share rejection |
| 8-provider/connection/disk/pause/fallback | limit counters, provider failure retry/resume, owner share/3 fallback, shutdown drain |
| root/private path protection | exact/ancestor/descendant/symlink, both registration orders, denied tree/catalog byte equality |
| existing identity/index/CAS/manual conversion | pre/post file/index/CAS/ReplicaId/allowlist snapshot, old manual flow rerun, no reset/delete |
| config/DB migration | old v1 JSON open, managed field default, catalog/index version checks, malformed state fail closed |
| auth/Host/Origin/CSRF | every new route unauthorized/forbidden cases and existing route regression |
| secret redaction | logs/activity/SSE/errors/screenshots/CI artifact grep and private file permission check |
| statuses | waiting/offline/initial_sync/complete/conflict/revoked/error/paused each actual trigger |
| browser/accessibility | 1440/390, keyboard, copy success/failure, focus, contrast, overflow, console/page error evidence |
| Windows native | actual native exe start/restart/sync evidence. cross-build alone is insufficient. |
| Internet external | separate identity/root, no direct IP input, N0 discovery/relay observation and real file hash |
| quality gates | commands in plan and progress with exact exit codes; no skipped required tests |
| main/CI/push | root-owned integration branch, local main, GitHub Linux/Windows CI URL, post-push `origin/main` SHA |

## 참고한 현재 코드와 선행 문서

조사한 구현 경계는 `crates/deltaweave-control/src/{config,model,lib,worker}.rs`,
`crates/deltaweave-web/src/routes.rs`, `web/src/{App,components,api,types}.tsx/ts`,
`crates/deltaweave-net/src/share/{service,wire,registry,runtime,ticket}.rs`,
`crates/deltaweave-sync/src/{lib,shared,transport,read_only}.rs`다. 현재 control은
config v1과 폴더별 legacy worker를 사용하며 worker는 DirectOnly와 `SyncEngine`을
직접 연결한다. 현재 net은 `ShareService::open`, `OwnerShare`, `ShareSession`,
`deltaweave/share/3`, durable registry와 auth gate를 이미 제공하지만 Manager/web에
연결되지 않았다. 현재 sync는 `ManagedSyncEngine::{open,resume,sync_once,shutdown}`와
RO authoritative path를 제공한다.

선행 판단은 `docs/INTEGRATION_2026-09-08.md`,
`docs/superpowers/specs/2026-09-06-folder-share-keys-design.md`,
`docs/superpowers/plans/2026-09-06-folder-share-keys.md`,
`docs/SHARE_KEYS_BASELINE_2026-09-06.md`, `docs/SHARE_KEYS_VERIFICATION_2026-09-06.md`,
`docs/UI_AUDIT.md`와 `.agents/skills/design-taste-frontend/SKILL.md`에서 가져왔다.
기존 문서의 구현 전/역사적 검증 결과는 새 qsync goal의 완료 증거로 재사용하지
않으며 progress 문서에 별도로 분류한다.
