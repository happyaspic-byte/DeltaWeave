# qBittorrent 스타일 폴더 공유 동기화 실행 계획

작성일: 2026-09-08

기준 커밋: `baed5c0164a2e10fdbe6a1e5a31f7e897c675ced`

기준 상태: `origin/main`이 위 커밋과 일치하는지 확인한 별도 worktree에서
작성한다. 이 계획과 계약 문서는 구현 완료의 증거가 아니다.

이 계획의 결과는 사용자가 한 기기에서 폴더를 공유하고 읽기 전용 또는
읽기/쓰기 키를 복사한 뒤, 다른 두 기기에서 키를 붙여 넣고 서버 기기의 폴더를
선택하여 자동 가입·동기화하는 실제 제품 흐름이다. owner, RW member, RO member
세 기기와 Windows 및 Internet/N0 relay를 실제로 검증할 때까지 완료로 표시하지
않는다. 완전 mesh, 공개 DHT, BitTorrent/qBittorrent wire compatibility, MSI 및
Windows SCM 설치는 이 목표에 포함하지 않는다.

## 작업 경계와 공통 계약

구현 순서는 A 문서 검토, B control, C web, D roster/network, E swarm, 통합·F
검증이다. B와 C가 먼저 사용할 공통 이름과 wire 표현은
[`2026-09-08-qsync-contracts.md`](../specs/2026-09-08-qsync-contracts.md)에
있다. 구현자는 문서의 메서드와 HTTP DTO를 임의로 축약하거나 legacy 타입에
managed role을 끼워 넣지 않는다.

### 먼저 확정한 B/C 표면

`Manager::open(data_dir)`는 기존 signature를 유지하고
`open_with_options(data_dir, ManagerOptions)`를 추가한다.
`ManagerOptions.managed_network` 기본값은 `NetworkMode::Internet`이며
`managed_bind`는 DirectOnly 통합 테스트가 loopback endpoint를 주입할 때만 쓴다.
managed share 또는 pending 설정이 없을 때 `open`은 ShareService를 lazy-init하지
않고 기존 manual worker만 연다.

Manager가 소유할 public operation은 다음과 같다.

- `create_share(CreateShareInput)`: owner share, private state, owner worker를
  원자적인 intent 뒤에 시작한다.
- `preview_share_key(PreviewKeyInput)`: bounded v3 signature/metadata/expiry만
  검사하며 issuance 조회나 enrollment를 하지 않는다.
- `validate_share_key(ValidateKeyInput)`: owner의 durable issuance와 revoke를
  authenticated online connection으로 확인한다.
- `join_share(JoinShareInput)`: server-device destination root를 admission하고
  실제 `enroll`에 성공한 뒤 member worker를 시작한다. owner offline이면 private
  pending과 `waiting`을 남긴다.
- `resume_membership(ResumeMembershipInput)`: key 재입력이나 `enroll` 없이 현재
  authenticated endpoint의 active membership만 조회한다. role, logical
  ReplicaId, epoch을 발급하거나 재할당하지 않는다.
- `list_shares`, `list_keys`, `issue_key`, `rotate_key`, `revoke_key`,
  `list_members`, `revoke_member`, `remove_share`, `share_command`(sync/pause/
  resume), `snapshot`, `shutdown`.

`AppSnapshot`에는 `#[serde(default)] shares: Vec<ManagedShareView>`와 pending
요약을 additive로 추가한다. 기존 `folders`, `devices`, `settings`, `activities`,
`history`, `FolderInput.role`의 `sync`/`receive`, 수동 identity/index/CAS/
allowlist는 유지한다. 외부 ID는 32바이트 ShareId/InvitationId의 소문자 hex
64자이며 permission/role/status는 snake_case, 시각은 UTC Unix seconds `u64`다.
기본 DTO에는 endpoint ID, 주소/포트, bearer/raw key, device private key,
state_root, logical ReplicaId를 넣지 않는다.

웹 static route는 dynamic `/{share_id}`보다 먼저 등록한다.

| HTTP | 경로 | body | 성공 결과 |
| --- | --- | --- | --- |
| GET | `/api/v1/shares` | 없음 | `ShareView[]` |
| POST | `/api/v1/shares` | request_id, name, root, min_free_space_mib? | 새 owner는 201, 재생은 저장된 최초 결과 |
| POST | `/api/v1/shares/preview` | request_id, key | signature metadata, `issuance: not_checked` |
| POST | `/api/v1/shares/validate` | request_id, key | online issuance 확인, `issuance: validated` |
| POST | `/api/v1/shares/join` | request_id, key, destination_root | enrolled 200, private pending 202 |
| POST | `/api/v1/shares/resume` | request_id, share_id | active membership 복구 |
| GET | `/api/v1/shares/{id}` | 없음 | `ShareView` |
| GET | `/api/v1/shares/{id}/keys` | 없음 | owner-only `KeySummary[]` |
| POST | `/api/v1/shares/{id}/keys` | request_id, permission, expires_at? | display-once `IssuedKey` |
| POST | `/api/v1/shares/{id}/keys/{inv}/rotate` | request_id, expires_at? | 기존 폐기 + 새 key |
| POST | `/api/v1/shares/{id}/keys/{inv}/revoke` | request_id | `MutationResult` |
| GET | `/api/v1/shares/{id}/members` | 없음 | owner-only `MemberView[]` |
| POST | `/api/v1/shares/{id}/members/{member}/revoke` | request_id | `MutationResult` |
| DELETE | `/api/v1/shares/{id}` | request_id | managed registration 제거, local data 보존 |
| POST | `/api/v1/shares/{id}/sync` | request_id | `ShareView` |
| POST | `/api/v1/shares/{id}/pause` | request_id | `ShareView` |
| POST | `/api/v1/shares/{id}/resume` | request_id | `ShareView` |

모든 새 route는 현재 `protect`의 login/session, Host, same-origin Origin,
Sec-Fetch-Site, CSRF, 64 KiB body limit과 safe JSON error를 사용한다. 400, 401,
403, 404, 409, 413, 422, 503, 500의 의미와 `{error,error_code,request_id}`
body는 계약 문서에 고정한다. basic UI는 IP, port, device ID, allowlist를 묻지
않으며 기존 `/api/v1/browse`를 server-device folder picker로 재사용한다.

### 공통 불변 조건

- Manager 하나에는 device key와 `ShareService` endpoint 하나만 있다. share마다
  `OwnerShare` 또는 `ManagedSyncEngine` worker만 별도로 둔다.
- managed service는 managed config/pending가 있거나 최초 managed 요청일 때만
  lazy-init한다. manual-only open은 Internet/N0 binding 및 외부 의존성을 만들지
  않는다.
- request_id는 create/join/resume/key/member/command/remove mutation에 필수다.
  canonical request hash가 같으면 같은 durable 결과를 재생하고, 같은 ID의 다른
  body는 409이며 파일·catalog·worker를 변경하지 않는다. raw key는 일반 config,
  log, snapshot, activity, SSE, localStorage에 쓰지 않는다.
- preview, online validate, actual join은 서로 다른 operation과 상태로 남긴다.
  owner가 offline이면 가입을 완료했다고 표시하지 않고 `waiting`으로 pending한다.
- member 목록은 durable registration일 뿐 online 증거가 아니다. active peer count와
  `last_seen_at`은 authenticated operation start/finish/error/close observer에서만
  측정한다.
- revoke는 신규 인증을 막는 durable denial을 먼저 기록한 뒤 connection, per-share
  gate, in-flight writer를 drain하고 응답한다. key revoke는 이미 전달한 파일을
  회수하지 않고, member revoke는 기존 세션과 subsequent adoption까지 차단한다.
- 외부 managed 상태는 정확히 `waiting`, `offline`, `initial_sync`, `complete`,
  `conflict`, `revoked`, `error`, `paused`만 사용한다.
- `remove_share`는 owner/member worker와 lease를 먼저 종료하고 managed registry/
  config만 제거한다. 이 goal에서 local root, state, index, CAS, identity, manual
  settings를 삭제하거나 초기화하지 않는다.

## Task A: 계약 문서와 baseline 고정

**소유 파일:** 이 plan, qsync spec, progress 문서만. 소스 파일은 수정하지 않는다.

**생산 인터페이스:** B/C가 사용할 Manager/API DTO, D/E가 준수할 net boundary,
F가 실행할 acceptance/evidence matrix.

**소비 자료:** 원문 objective, 현재 control/web/net/sync 코드와 root admission,
선행 folder-share 설계/계획, current integration report, CI workflow.

**완료 조건:** 세 문서에 기준 SHA, 실제 worktree/branch, task별 file ownership,
input/output, 정확한 command, completion gate, log path, 모든 명시 요구사항의
evidence row가 있다. historical pass는 qsync 완료로 표시하지 않는다.

**검사:**

```bash
git rev-parse HEAD
git rev-parse origin/main
git status --short
git diff --check
```

**증거 경로:**
`/home/ubuntu/project/DeltaWeave-qsync-evidence/2026-09-08/a-contract/`.

## Task B: control Manager와 managed worker

**소유 파일:**

- `crates/deltaweave-control/src/config.rs`
- `crates/deltaweave-control/src/model.rs`
- `crates/deltaweave-control/src/lib.rs`
- `crates/deltaweave-control/src/worker.rs`
- B가 요구하는 좁은 resume hook에 한해
  `crates/deltaweave-net/src/share/service.rs`와 `wire.rs`

D가 같은 net 파일의 roster/heartbeat 영역을 편집할 때 이 B hook 영역과 충돌하지
않도록 먼저 interface를 고정하고, 필요하면 root가 순서를 조정한다. B는 D/E의
새 swarm protocol을 구현하지 않는다.

**입력:** `ManagerOptions`, `CreateShareInput`, `PreviewKeyInput`,
`ValidateKeyInput`, `JoinShareInput`, `ResumeMembershipInput`, key/member/
command/remove input, 기존 `FolderInput` 및 config v1.

**생산:**

- additive config `managed` with outer version 1 compatibility;
- one lazy `ShareService` per Manager and persistent device identity;
- owner/member/pending worker slots;
- `ManagedShareView`, `PendingView`, exact status and safe errors;
- path admission, durable request journal, private pending ticket file;
- `ManagedSyncEngine` lifecycle and authenticated resume-membership call;
- `AppSnapshot` additive shares/pending consumed by C.

**구현 순서와 조건:**

1. `ManagerOptions::default`를 Internet/None으로 정의하고 기존 `open`은 위 옵션을
   사용한다. managed config/pending 검사 뒤 service를 lazy-init한다. manual-only
   open에서 endpoint bind/N0 task가 생성되지 않는 회귀 테스트를 쓴다.
2. `config.json` outer version 1을 유지하고 `managed`에 `#[serde(default)]`를
   적용한다. old JSON, missing field, enum-tagged manual role을 읽고 저장할 수
   있어야 한다. unknown version과 malformed state는 fail closed하며 reset하지
   않는다.
3. create/join 전에 path와 private sibling state를 `root_admission`에 예약한다.
   기존 manual root와 managed root가 겹치면 명시적으로 거부한다. 자동 legacy-
   managed 전환은 이 task의 범위가 아니며, 기존 root/state/index/CAS/identity/
   ReplicaId/causal history/allowlist와 manual 설정을 그대로 둔다. 두 registration
   순서, exact/ancestor/descendant/symlink와 public-private overlap을 검사한다.
4. `ShareService::create_owned_share`, `load_owned_share`, `ManagedSyncEngine::open`
   을 연결한다. owner는 기존 `OwnerShare::{set_observer,inventory,keys,members,
   pause,resume,revoke_member}`를 사용한다. observer를 중복 구현하지 않는다.
5. preview는 local parse/signature/expiry only, validate는 online durable
   issuance, join은 destination path admission -> pending intent -> authenticated
   enroll -> owner-assigned membership/ReplicaId persistence -> worker start 순서를
   지킨다. owner offline에서는 기존 private-file/Windows ACL helper로 raw ticket을
   별도 0600 파일에 남기고 pending TTL을 `min(ticket expiry, 7 days)`로 적용해
   202/waiting을 반환한다. ticket expiry가 없는 기존 ticket은 parse 호환하되 pending
   보관은 7일로 제한한다.
6. `resume_membership`은 B가 필수로 구현한다. response-loss resume은 expected owner/share와 현재 authenticated endpoint의
   active membership만 확인하고 기존 permission/member/ReplicaId/epoch을 저장한다.
   invitation을 새로 issue/enroll하거나 revoked member를 revive하지 않는다.
7. request journal은 operation domain + canonical JSON BLAKE3 hash와 result_ref를
   저장한다. active/resolved reference를 cap 도달 시 조용히 버리지 않는다. 공간이
   없으면 `idempotency_capacity` 명시 오류를 반환한다. issue/rotate의 raw response는
   기존 private-file/Windows ACL helper가 보호하는 별도 0600 파일에 최대 5분만
   보관하고, 보관이 끝난 동일 요청은 `key_response_expired` 409로 끝내며 새
   invitation을 자동 발급하지 않는다.
8. create intent에는 호출 전에 durable한 request ID/hash, canonical root, stable
   private state_root, name을 남긴다. open은 `owned_configs()`와 정확히 일치하는
   미해결 intent를 찾아 registry가 config save보다 먼저 성공한 crash window를 같은
   random ShareId로 복구한다. unmatched catalog은 삭제하지 않고 fail closed한다.
   join도 enroll 전에 owner/share/root/state_root/request intent를 private persist하고
   reopen 시 resume lookup을 먼저 한다.
9. `pause`/`resume`/`sync`는 per-share gate와 worker state를 사용하고 status를
   실제 report에 맞춘다. revoke/remove/shutdown은 Arc 참조, timers, observer,
   SSE, engine session, root lease, tracked connection을 순서대로 회수한다.

**B 검사와 완료 evidence:**

```bash
DELTAWEAVE_REQUIRE_WEB_ASSETS=1 cargo test --locked -p deltaweave-control --all-targets --all-features
DELTAWEAVE_REQUIRE_WEB_ASSETS=1 cargo test --locked -p deltaweave-net --all-targets --all-features
DELTAWEAVE_REQUIRE_WEB_ASSETS=1 cargo clippy --locked -p deltaweave-control -p deltaweave-net --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

완료 조건은 one endpoint identity test, manual lazy-init test, persistence/restart,
pending/offline/resume response-loss, owner/member role, idempotency conflict,
root-admission unchanged bytes, revoke drain, shutdown rebind, RO/RW engine hookup가
실제 exit 0과 redacted log로 확인되는 것이다. 로그는
`.../b-control/` 아래 `manager-tests.log`, `config-migration.log`,
`shutdown-lifecycle.log`, `redacted-state.json`으로 보관한다.

## Task C: authenticated web API와 React console

**소유 파일:**

- `crates/deltaweave-web/src/routes.rs` 및 해당 route test 파일
- `web/src/App.tsx`, `web/src/components.tsx`, `web/src/api.ts`,
  `web/src/types.ts`, `web/src/styles.css` 및 share-specific component/test 파일

C는 B의 public Manager method를 호출하고 config/net registry를 직접 만지지
않는다. B의 net hook이나 D/E 파일은 편집하지 않는다.

**입력:** B의 DTO와 `AppSnapshot` additive fields, 기존 auth/session/CSRF,
`/api/v1/browse` response.

**생산:** 위 표의 `/api/v1/shares` routes, safe status/error DTO, 기존 한국어
console에 `폴더 공유`와 `키로 연결` 흐름, member/key lifecycle UI.

**구현 조건:**

1. static route를 dynamic route보다 먼저 등록하고 모든 mutation/preview/validate/
   join/resume에 기존 Host, Origin, Sec-Fetch-Site, session, CSRF, 64 KiB limit를
   적용한다. unauthorized, wrong host/origin, missing/invalid CSRF, member owner-only
   access, malformed path/body를 각각 계약 status로 테스트한다.
2. `POST /preview`는 key memory input을 parse metadata만 표시한다. `POST /validate`
   는 online issuance의 유효성을 별도로 표시하며, join button은 destination browse
   뒤에만 enable한다. folder path는 서버 기기 path로 명시한다.
3. `IssueKey`/rotate 결과의 raw key는 React state와 clipboard 작업에만 두며 URL,
   localStorage, query, activity, SSE, error, analytics, test screenshot, CI
   artifact에는 쓰지 않는다. modal close/cancel/success/expiry/logout과 copy
   failure 뒤 clear를 보장한다.
4. snapshot의 `MemberView`와 실제 `connected_devices`를 분리해 registration row를
   online으로 그리지 않는다. status 8개를 runtime 값 그대로 보여주고 key revoke와
   member revoke가 회수하는 범위를 별도 문구로 표시한다.
5. 기존 Manrope/Noto Sans KR/IBM Plex Mono, forest token, navigation/folder browse/
   settings/activity/modal/focus behavior를 보존한다. desktop 1440px, mobile 390px,
   200% zoom, keyboard Tab/Escape/focus restore, long key wrap, labels/aria,
   contrast, no horizontal overflow와 console/page error를 확인한다.

**C 검사와 완료 evidence:**

```bash
npm --prefix web ci
npm --prefix web test
npm --prefix web run build
DELTAWEAVE_REQUIRE_WEB_ASSETS=1 cargo test --locked -p deltaweave-web --all-targets --all-features
```

추가 integration route test는 실제 test-mode Manager와 DirectOnly 통신을 수행해야
하며 mocked HTTP만으로 성공 처리하지 않는다. 로그는
`.../c-web/route-auth.log`, `browser-3-device-redacted.log`, `screenshots/`에
두되 key/개인정보를 제거한다. C 완료는 owner/member/pending 상태를 API와 UI에서
구분하고 기존 manual API/UI 회귀가 모두 exit 0인 경우다.

## Task D: authenticated roster, heartbeat, address update, N0 relay

**소유 영역:** D agent가 root의 지시에 따라 기존 share runtime/registry와 새
roster 모듈을 소유한다. 후보 파일은
`crates/deltaweave-net/src/share/{mod.rs,registry.rs,runtime.rs}`와 필요한
roster/observer 모듈이다. B가 `service.rs`/`wire.rs`에 구현한
`resume_membership` request/reply hook은 D가 소비한다. C는 D 파일을 수정하지
않는다.

**입력:** B가 제공하는 device-wide service, ShareView status, active operation
observer; 기존 `OwnerShare::{members,set_observer,inventory,pause,resume,
revoke_member}`, `ShareTicket`/registry authority.

**생산:** owner authenticated roster, member binding, heartbeat/liveness hint,
identity-based address refresh, N0 Internet default/relay reconnect evidence와
private operation observation.

**완료 조건:** heartbeat가 authority를 대체하지 않고, durable member row/stale
inventory가 online으로 승격되지 않으며, owner/share/peer/epoch에 묶인 start/
finish/error/close observation이 active count를 정확히 drain한다. address change는
old IP 입력 없이 endpoint identity/relay lookup으로 갱신된다. `resume_membership`은
active membership을 같은 permission/member/ReplicaId/epoch으로 돌려주며 new enroll
경로가 아니다. N0 실제 NAT/relay run과 DirectOnly test log를 분리한다. peer grant의
UTC expiry와 monotonic lease 적용은 protocol audit 결론 전 pending이며, audit 보고로
교체될 수 있다.

**검사/로그:**

```bash
DELTAWEAVE_REQUIRE_WEB_ASSETS=1 cargo test --locked -p deltaweave-net --all-targets --all-features
DELTAWEAVE_REQUIRE_WEB_ASSETS=1 cargo clippy --locked -p deltaweave-net --all-targets --all-features -- -D warnings
```

`.../d-roster/heartbeat.log`, `address-refresh.log`, `n0-internet.log`,
`direct-only-test.log`에 peer IDs와 addresses를 redaction한 count/timestamp만
남긴다. protocol audit의 변경 결론이 있으면 root가 이 plan/spec의 pending
contract를 갱신하고 B/C implementation과 충돌 여부를 확인한다.

## Task E: separate share-swarm/1 multi-provider transfer

**소유 영역:** E agent가 `crates/deltaweave-net/src/share_swarm.rs` 또는 기존 net
share와 분리된 동등 모듈, `crates/deltaweave-swarm/src/**`, 그리고 필요한
`crates/deltaweave-sync/src/{lib,shared,transport}.rs`의 swarm adapter만 소유한다.
기존 `deltaweave/share/3` owner session과 legacy `sync/3` handler를 바꾸지 않는다.

**입력:** D의 authenticated roster/epoch/address, B lifecycle/gate/root lease,
owner authoritative manifest와 existing CAS/hash verification.

**생산:** ALPN `deltaweave/share-swarm/1`, owner-signed
`ShareSwarmGrant`(owner/share/requester/provider/permission_epoch/manifest_hash/
requested_hash/expiry/nonce/signature), exact manifest-scoped requests와 bounded
multi-provider scheduler.

**완료 조건:** provider local `endpoint.id()`가 grant의 `provider`이고 incoming
`connection.remote_id()`가 grant의 `requester`인지 먼저 확인한다. owner signature,
share/requester/provider, epoch, expiry/nonce, manifest hash, requested hash, chunk
membership, size/hash도 모두 검증한다. 한 fill의 provider는 최대 8개이며
connection/queued bytes/disk reserve budget을 넘지 않는다. forged grant/hash,
cross-share, stale epoch, manifest 밖 hash, revoked member, disk reserve 초과는
materialization 전에 거부한다. grant의 UTC expiry와 수신 후 monotonic lease 정책은
`qsync_protocol_audit` 보고 전 pending으로 기록하며 그 결론으로 교체한다.
provider failure는 authorized retry, pause/resume 후 owner authenticated
`share/3` pull fallback만 사용하며 legacy allowlist CAS fallback은 금지한다.

**검사/로그:**

```bash
DELTAWEAVE_REQUIRE_WEB_ASSETS=1 cargo test --locked -p deltaweave-swarm -p deltaweave-net -p deltaweave-sync --all-targets --all-features
DELTAWEAVE_REQUIRE_WEB_ASSETS=1 cargo clippy --locked -p deltaweave-swarm -p deltaweave-net -p deltaweave-sync --all-targets --all-features -- -D warnings
```

`.../e-swarm/grant-verification.log`, `provider-limit.log`, `failure-fallback.log`,
`redacted-manifest.json`에 실제 2개 이상 provider, forged/cross-share rejection,
hash 재검증, fallback, pause/shutdown drain을 남긴다. E는 complete mesh나 member-to-
member authoritative snapshot을 만들지 않는다.

## Task F: integration, browser/Windows/Internet validation, CI and main

**소유:** root가 integration worktree, merge/push, final goal, release 판단을
조정한다. validation agent는 검증 worktree에서 test/evidence만 만들며 main이나
다른 agent의 user change를 덮지 않는다.

**검증 장비/경로:**

- integration: `/home/ubuntu/project/DeltaWeave-qbittorrent-20260908`,
  `integration/qbittorrent-sync-20260908`
- contracts: `/home/ubuntu/project/DeltaWeave-qsync-contracts-20260908`,
  `feat/qsync-contracts-20260908`
- B: `/home/ubuntu/project/DeltaWeave-qsync-control-20260908`,
  `feat/qsync-control-20260908`
- C: `/home/ubuntu/project/DeltaWeave-qsync-web-20260908`,
  `feat/qsync-web-20260908`
- validation: `/home/ubuntu/project/DeltaWeave-qsync-verification-20260908`,
  `test/qsync-verification-20260908`

실제 validation은 owner, RO, RW 각각의 지속 identity/root를 사용한다. owner가
share를 만들고 RO/RW key를 각각 issue/copy한 뒤 두 기기에서 preview -> online
validate -> server-device destination browse -> actual join을 한다. 파일 add/edit/
delete, causal conflict, restart, owner offline/waiting, response-loss/resume,
permission downgrade/revoke, key rotate/revoke, local RO add/edit/delete preservation,
remote delete/type transition recovery와 no data reset을 확인한다. 두 member는
owner만 authority인 상태에서 동기화하며 full mesh를 사용하지 않는다.

Windows native executable에서 start, login, browse, join, restart, sync, revoke와
shutdown/rebind를 실행한다. cross-build만으로 Windows 검증을 대체하지 않는다.
Internet mode에서는 별도 identities로 NAT/relay/N0 address change를 실제 관찰하고,
DirectOnly ManagerOptions test evidence와 분리한다. 브라우저는 1440/390/200% zoom,
keyboard/focus/copy failure/contrast/overflow/console error를 redacted screenshot
및 network log로 남긴다.

**필수 품질 명령:** web assets를 먼저 만들고 모든 Rust web-sensitive 명령에
`DELTAWEAVE_REQUIRE_WEB_ASSETS=1`을 사용한다.

```bash
npm --prefix web ci
npm --prefix web test
npm --prefix web run build
export DELTAWEAVE_REQUIRE_WEB_ASSETS=1
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --all-features
cargo test --locked --workspace --doc --all-features
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --all-features
cargo build --locked --workspace --release --all-features
cargo run --locked -p deltaweave -- self-test
bash scripts/verify-release.sh
git diff --check
```

기존 baseline의 새 실패는 반드시 별도 원인/수정/재검증으로 추적한다. 근거는
`/home/ubuntu/project/DeltaWeave-qsync-evidence/2026-09-08/github-failure-
snippets-20260908.txt`이며 다음 세 항목을 숨기거나 단순 재실행으로 덮지 않는다.

| workflow run | 실패 증상 | F 수용 조건 |
| --- | --- | --- |
| `34251266098` Linux | `crates/deltaweave-net/tests/shares.rs:1001`에서 owner shutdown 뒤 같은 주소의 fake owner rebind가 `AddrInUse` | shutdown이 모든 endpoint/router/task/Arc를 await하고 주소 재사용 test가 exit 0 |
| `34251266098` Windows | `crates/deltaweave-control` `manager.rs:209` watcher가 10초 timeout | Windows native watcher의 bounded shutdown/restart가 실제 실행에서 exit 0 |
| `34251265862` Container | Docker UID/GID 65532에서 self-test receiver `PermissionDenied` (home/admission 경로) | non-root image의 private data/root-admission 경로가 writable/secure하고 amd64/arm64 self-test가 pass |
| `34251266012` Security | 성공 | security 성공은 위 실패를 상쇄하지 않으며 final run evidence와 별도 기록 |

Container test/image artifact는 운영 서비스 배포 evidence와 구분한다. Release
workflow는 workspace version `0.4.0`과 이미 published 된 `v0.4.0`을 확인한다.
이번 goal에서 Cargo version을 불필요하게 올리거나 새 tag를 만들지 않는다. final
successful main CI 뒤 `prepare.publish=false`, `v0.4.0` existing, no new release tag를
확인한다.

F 완료 gate는 다음 모두가 같은 integrated SHA에 대해 exit 0이고 실제 evidence
path가 존재할 때뿐이다.

1. B/C API contract and test mode integration, D authenticated N0 roster, E grant/
   manifest provider limit가 통과한다.
2. owner/RO/RW 세 기기와 3개 root의 browser flow가 real data hash로 수렴한다.
3. Windows native와 external Internet/NAT/relay가 각각 실행 증거를 가진다.
4. baseline failures가 원인 수정 후 재검증되어 old failure snippet과 함께 기록된다.
5. full quality commands, security/redaction/path compatibility, CI Linux/Windows/
   container 결과가 모두 기록된다.
6. root가 integration branch를 review하고 local main 및 GitHub `origin/main` SHA,
   final CI URL, release `publish=false`를 기록한 뒤에만 push/final 완료를 보고한다.

통합 evidence 기본 경로는
`/home/ubuntu/project/DeltaWeave-qsync-evidence/2026-09-08/`이고, 각 agent는
하위 `a-contract`, `b-control`, `c-web`, `d-roster`, `e-swarm`, `f-validation`,
`ci`에 command, UTC timestamp, exit code, commit SHA, redacted output을 둔다.

## 요구사항과 증거 매핑

아래 항목은 progress 대장에도 같은 이름으로 유지한다. 증거 파일과 exit code가
실제로 존재하기 전에는 `[ ]`를 `[x]`로 바꾸지 않는다.

| 원문 요구사항 | 구현 owner | 필요한 증거 |
| --- | --- | --- |
| full goal, 3기기, owner/RO/RW | root/F | 세 identity browser/network/hash log |
| B Manager one ShareService, owner/member/pending worker | B | one endpoint/lazy-init/restart/worker log |
| C auth API와 React UI | C | route auth tests, npm test/build, browser evidence |
| preview/validate/actual join | B/C | 세 단계가 membership/status를 달리하는 test |
| folder selection은 서버 기기 경로 | B/C | browse request/selected path 및 private path rejection |
| pending request_id 영속 후 enroll | B | private 0600 ticket file/intent, restart join |
| 응답 유실 resume, role/ReplicaId 재발급 금지 | B/D | lost reply, active lookup, same binding assertion |
| identity/index/CAS/manual/type conversion 보존 | B/F | pre/post hashes, retained ReplicaId/identity, manual rerun |
| Internet N0 기본, DirectOnly test injection | B/D/F | mode-specific logs and no basic address field |
| 실제 인증 roster/heartbeat/address refresh | D | operation observation, relay/address-change log |
| E 별도 swarm/1 grant+manifest/multi-provider | E | signed grant, scope rejection, >=2 provider, max 8 |
| RW add/edit/delete/conflict/restart | E/B/F | files/actions/conflict/resume hash evidence |
| RO one-way authoritative snapshot/preservation | E/B/F | local changes preserved, remote deletion/type recovery |
| waiting/offline/initial_sync/complete/conflict/revoked/error/paused | B/C/F | actual trigger/status snapshots for all 8 |
| revoke/rotation/shutdown drain | B/D/E | denial-before-drain and post-response no write evidence |
| root/private path and filesystem safety | B/F | exact/ancestor/descendant/symlink both order, bytes unchanged |
| auth Host/Origin/CSRF/body limit | C | every new route allow/deny matrix |
| key/PII secrecy | B/C/F | grep/redacted artifacts/private permissions, no localStorage |
| browser responsive/accessibility | C/F | desktop/mobile/zoom/keyboard/copy/axe-style evidence |
| Windows native runtime | F | actual native binary start/restart/sync/revoke |
| external Internet/NAT/relay | D/F | separate external network log and hashes |
| CI/main/release scope | root/F | Linux/Windows/container/Security URLs, main SHA, `publish=false`, no new tag |
| excluded mesh/BitTorrent/MSI-SCM | root review | scope review showing no implementation or claim |

## 실행 기록과 중단 기준

각 task는 시작·종료 commit SHA와 실제 명령 exit code를 progress 문서에 남긴다.
실패 시 원인과 재현 명령을 보존하며, historical pass를 새 결과로 대체하지 않는다.
보안/권한 경계 또는 local data preservation에 의문이 생기면 해당 task를 `blocked`
로 표시하고 root가 계약을 갱신한 뒤 재개한다. 단순히 테스트를 생략하거나 1차
2기기/owner-only로 범위를 축소하여 완료 처리하지 않는다.

최종 commit/push/merge와 goal complete 판정은 root만 수행한다. 이 worktree에서는
세 문서와 문서 검증만 커밋하고 push/merge하지 않는다.
