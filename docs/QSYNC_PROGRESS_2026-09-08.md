# QSYNC 2026-09-08 진행 대장

기준 커밋: `baed5c0164a2e10fdbe6a1e5a31f7e897c675ced`

기준 확인: contracts worktree의 `HEAD`와 `origin/main`이 위 SHA로 일치했다.

작성 agent: `qsync_contracts` / Luna Max

현재 단계: E1 durable activation receipt/status checkpoint. A 계약 문서는 `121d042`로
통합되었고, B 최종 source checkpoint는 `5702ab1`, D2 source checkpoint는
`04818f2b224249b2839b20d0b18f7b6634ae0c0a`이다. D2는 authority와
request-start monotonic activation lease 검증까지 완료했다. D3 checkpoint
`f886ab0`은 heartbeat와 bounded endpoint-ID fallback을 포함하지만 실제
Internet/N0·relay와 E data-plane은 완료로 표시하지 않는다. E1 source checkpoint는
`bfe6f92`이며, durable activation 상태 조회/취소와 양쪽 drain 상태 검증만 포함한다.

## 작업 공간과 agent

| 역할 | agent/worktree | branch | 상태 |
| --- | --- | --- | --- |
| integration/root | `/home/ubuntu/project/DeltaWeave-qbittorrent-20260908` | `integration/qbittorrent-sync-20260908` | root 조정 대기 |
| A contracts | `/home/ubuntu/project/DeltaWeave-qsync-contracts-20260908` | `feat/qsync-contracts-20260908` | `121d042` 완료 |
| A protocol audit | 별도 agent `qsync_protocol_audit` | root 기록 예정 | `8d50a14` network 계약 완료, D/E 입력 |
| baseline validation | `luna_protocol_plan` 재사용 | root 기록 예정 | CI 실패 원인 조사 |
| B control | `/home/ubuntu/project/DeltaWeave-qsync-control-20260908` | `feat/qsync-control-20260908` | `ca63e04` persisted managed-private path recovery; prior final source `5702ab1`, Windows acceptance fixture `39ae489` |
| C web | `/home/ubuntu/project/DeltaWeave-qsync-web-20260908` | `feat/qsync-web-20260908` | 병렬 구현 진행 |
| validation | `/home/ubuntu/project/DeltaWeave-qsync-verification-20260908` | `test/qsync-verification-20260908` | 후속 검증 |
| D network | `/home/ubuntu/project/DeltaWeave-qsync-network-20260908` | `feat/qsync-network-20260908` | `bfe6f92` E1 activation status/cancel checkpoint; D3 Internet/N0·relay 및 E data-plane 검증 대기 |

## A에서 고정한 계약

- Manager 한 개당 ShareService/device identity 하나이며 managed config/pending 또는
  첫 managed 요청에서만 lazy-init한다. 기본 Internet/N0, test DirectOnly 주입이다.
- `ManagerOptions`, `create_share`, `preview_share_key`, `validate_share_key`,
  `join_share`, `resume_membership`, `list_keys`, `issue_key`, `rotate_key`,
  `revoke_key`, `list_members`, `revoke_member`, `remove_share`, `share_command`와
  응답 유실 pending 전용 `RetryPendingJoinInput { request_id, share }` 및
  `retry_pending_join`을 고정했다. 이 endpoint는 기존 join request journal과
  pending share binding을 확인하고 private ticket으로 재시도하며, 이미 enrolled인
  request는 같은 membership 결과를 반환하고 새 key/ReplicaId를 만들지 않는다.
  `AppSnapshot` additive `#[serde(default)] shares`/pending을 고정했다.
- ShareId/InvitationId는 lowercase hex 64자, permission/role/status는 snake_case,
  timestamp는 UTC Unix seconds `u64`이다. unknown API enum은 422, persisted unknown은
  fail closed이다.
- preview, online validate, actual join을 분리하고, owner offline pending의 request
  intent/root/state_root를 먼저 private persist한다. response-loss resume은 active
  membership 조회만 하며 role/ReplicaId/epoch을 재발급하지 않는다.
- 기존 manual root/index/CAS/identity/ReplicaId/allowlist/type-conversion 보존을
  우선하며 automatic legacy-to-managed conversion과 existing_folder_id는 범위 밖이다.
- join ticket은 기존 private-file/Windows ACL helper의 별도 0600 파일과
  `min(ticket expiry, 7 days)` pending TTL을 사용한다. key response file TTL은 5분이다.
  idempotency active/resolved reference는 조용히 evict하지 않고 capacity 오류를 낸다.
- B가 resume wire hook과 create/join crash recovery를 소유한다. D는 roster/heartbeat/
  address observation을 소비한다. E provider는 local endpoint == grant.provider,
  `connection.remote_id()` == grant.requester를 확인한다. grant monotonic lease는
  protocol audit 결론 전 pending이다.

## 확인된 근거

| 확인 | 결과 | 기록 |
| --- | --- | --- |
| baseline SHA | `HEAD == origin/main == baed5c0...` | 이 worktree command output |
| initial worktree status | 문서 생성 전 clean | 작업 시작 기록 |
| current control/web/net/sync boundaries | 읽기 완료 | spec 참고 코드 목록 |
| existing ShareService/OwnerShare/ManagedSyncEngine | API 존재, Manager/web 연결 미완료 | spec current-code section |
| required web asset guard | `DELTAWEAVE_REQUIRE_WEB_ASSETS=1` workflow 확인 | plan Task B/C/F |
| B source checkpoint | `6c864fa` | managed lifecycle crash/recovery plus durable pending-join retry API, provenance lease handoff, duplicate binding, key intent/rotation, per-test profile harness |
| A docs validation | `git diff --cached --check` exit 0; allowed-file list 3개 | 이 commit |
| required cargo check | exit `0`, 10.91s; `deltaweave-net`, `deltaweave-sync`, `deltaweave-control` 검사 | `b-control/check-20260908-current.log` |
| managed owner→RW smoke | `1 passed, 0 failed, 0 ignored`, exit `0`, 2.35s; 실제 파일 수신 확인 | `b-control/managed-smoke-20260908-current.log` |
| control focused lifecycle | isolated process별 5개 `1 passed`, exit `0`; admission fixture 오염을 숨기지 않고 격리 재실행 근거 보존 | `b-control/control-focused-20260908-current.log`, `b-control/control-focused-isolated-retry-20260908-current.log` |
| net resume binding | exact permission/ReplicaId/epoch/revoke binding test `1 passed`, exit `0` | `b-control/net-registry-resume-20260908-current.log` |
| managed acceptance | child profile/TMPDIR harness의 6개 독립 시나리오 `6 passed, 0 failed`, exit `0`, 147.06s; 같은 결과를 기존 isolated run과 중복 집계하지 않음 | `b-control/test-managed-acceptance-harness-20260908T223255Z.log` |
| resume without local relationship | 성공 join 뒤 local `ShareService::forget_membership` 후 재시작, active owner membership의 member 목록/permission/ReplicaId/enrolled_at/epoch 보존 `1 passed`, exit `0`, 40.71s | `b-control/managed-pending-resume-forgot-20260908-current.log` |
| current build/lint/format | `cargo check --locked -p deltaweave-control -p deltaweave-net --all-targets --all-features` exit `0`(7.84s), strict clippy exit `0`(12.38s), fmt/diff check exit `0` | `b-control/cargo-check-pending-api-20260908T230238Z.log`, `b-control/cargo-clippy-pending-api-20260908T230252Z.log` |
| current focused regressions | control managed unit `5 passed`, root admission `16 passed`, smoke `6 passed`, exit `0`; lease provenance/duplicate binding/key orphan/rotation included | `b-control/test-control-unit-managed-20260908T223623Z.log`, `b-control/test-net-root-admission-20260908T223651Z.log`, `b-control/test-managed-smoke-20260908T222615Z.log` |
| public pending retry API | wrong-share conflict, wrong-operation rejection, pending-only owner-offline waiting, owner-return enrollment, same-request member/permission replay, and expired pending terminal mapping `1 passed`, exit `0`, 135.73s | `b-control/test-pending-retry-api-final-20260908T225952Z.log`; started `2026-09-08T22:59:52Z` against the pending API tree and committed as `6c864fa`; this is a durable pending fixture, not packet-loss or HTTP response-loss evidence |
| B cancellation/observer checkpoint | cancelled first shutdown caller, concurrent shutdown waiter, public join/retry caller abort, started writer serialization, pause→resume and revoke→late-error status precedence, observer/failure field preservation `6` focused cases passed; source commits `5331b76`→`5702ab1` | `b-control/aborted_public_join_keeps_pending_save_and_lease-p1p2-current.log`, `b-control/aborted_public_retry_keeps_membership_transition_durable-p1p2-current.log`, `b-control/aborted_save_keeps_writer_until_publication_and_shutdown_is_serial-p1p2-current.log`, `b-control/shutdown-cancellation-final.log`, `b-control/observer-p2-precedence-final.log`, `b-control/observer-p2-publication-final.log` |
| B final compile/lint for checkpoint | `cargo check --locked ... --all-targets --all-features` exit `0`; strict clippy with `-D warnings` exit `0`; fmt and diff check exit `0` | `b-control/check-control-net-cancellation-final-2.log`, `b-control/clippy-control-net-cancellation-final-3.log`; an earlier clippy attempt failed on newly introduced conditional-shape lints and is retained separately, then fixed before this result |
| B final-save observation publication | source `5702ab1`; final fsync callback barrier, memory publication, automatic persistence `config::save`, shutdown/reopen durable Revoked view all passed; no manual follow-up persist | `b-control/final-save-auto-flush-durable-20260909.log`, exit `0`, child+outer each `1 passed`; command used `CARGO_TARGET_DIR=/home/ubuntu/.herdr/worktrees/DeltaWeave/share-key-sync-update/target CARGO_BUILD_JOBS=4 TMPDIR=... cargo test --locked -p deltaweave-control --lib managed_error_tests::final_save_preserves_callback_before_memory_publication_and_next_flush -- --exact --nocapture` (started `2026-09-09T01:37:17Z`, ended `2026-09-09T01:37:47Z`) |
| B Windows acceptance fixture follow-up | source `39ae489`; pause-completion `last_sync_at` baseline, actual ticket expiry bounded wait, and reopen-after-expiry assertion fixed; lifecycle and pending-expiry tests each exit `0` (outer/child each `1 passed`) | `b-control/acceptance-pause-final-20260909T025639Z.log`, `b-control/acceptance-expiry-final2-20260909T025704Z.log`; no production clock or ACL relaxation |
| B persisted managed-private path recovery | source `ca63e04`; persisted normal and verbatim Windows parent forms are checked without following original components, compared by canonical trusted parent, and returned with a strict generated leaf; missing expired leaves remain valid, while malformed/external/symlink paths fail closed; GC uses the same canonical key and Windows leaf-case folding with live duplicate pending records taking precedence | `2026-09-09/b-control/path-recovery-01-fmt-diff.log`, `path-recovery-02-generated-path.log` (Linux 2 passed), `path-recovery-02-gc.log` (Linux 1 passed), `path-recovery-03-pending-resume.log` (outer/child 1 passed), `path-recovery-04-check.log`, `path-recovery-05-clippy.log`, `path-recovery-06-windows-check.log` (cfg compile only; native Windows cases pending CI) |
| D2 source checkpoint | source `63226b9d7dbd0e227e27c9dbb9748a5fa479f91a`; authority signed snapshot/manifest/grant/permit, exact record size/root/epoch/provider checks, bounded registry GC, clock quarantine/restart blockers, revoke and bilateral drain, ALPN `share-swarm/1` grant-only rejection handler | `d-network/d2-final-build-20260909T025247Z.log`: control+net check exit `0`, strict net clippy exit `0`; `d-network/d2-focused-final-20260909T025317Z.log`: authority `3`, roster `3`, registry `10`, service `4` tests passed, all exits `0` |
| D2 activation lease checkpoint | source `04818f2b224249b2839b20d0b18f7b6634ae0c0a`; `ShareSession::activate_grant` now returns local `ActivationLease { reply, deadline }`, with `deadline = request_started + min(reply.max_duration_secs, 15s)` and no response-time extension; wire reply remains unchanged | `d-network/d2-activation-lease-20260909T031405Z.log`: the two activation lease tests passed, exit `0`; delayed-reply test proves exactly 6 seconds remain from a fixed request/reply interval, and late-reply test rejects the expired lease |
| D2 quality gates | source `04818f2b224249b2839b20d0b18f7b6634ae0c0a`; format, diff, control+net all-target check, and strict clippy all passed | `d-network/d2-activation-quality-20260909T031532Z.log`: start `2026-09-09T03:15:32Z`, end `2026-09-09T03:15:58Z`, each step exit `0`; command records `CARGO_TARGET_DIR` and `CARGO_BUILD_JOBS=4` |
| D2 isolated full-net rerun | source `04818f2b224249b2839b20d0b18f7b6634ae0c0a`; fresh workspace-private `HOME`/`USERPROFILE`/`TMPDIR` with preserved `/home/ubuntu/.rustup` and `/home/ubuntu/.cargo`; previous 31 ENOENT did not recur | `f-validation/net-full-isolated-20260909T031429Z.log`: start `2026-09-09T03:14:29Z`, end `2026-09-09T03:14:45Z`, exit `0`; unit `104`, admission `2`, share `13`, doctest `0`, all failed `0` |
| D2 full-net baseline observation | source `b571d30` plus uncommitted D2 tree at test start; `64 passed, 31 failed`, exit `101`; all 31 failures were existing ENOENT child/profile or host admission fixture failures, while D2 modules passed | `d-network/net-all-tests-20260909T024459Z.log`; retained as baseline evidence and not reclassified as D2 logic failures |
| npm/Windows/Internet/full CI | 아직 실행하지 않음 | 후속 evidence 필요 |

## baseline 실패와 남은 문제

baseline push CI evidence는
`/home/ubuntu/project/DeltaWeave-qsync-evidence/2026-09-08/github-failure-snippets-20260908.txt`에
보존한다. run `34251266098` Linux는 owner shutdown 뒤 같은 주소 rebind의
`AddrInUse`, Windows는 control watcher 10초 timeout, run `34251265862` Container는
UID/GID 65532 self-test receiver `PermissionDenied`였다. Security run
`34251266012` 성공은 이 세 실패를 상쇄하지 않는다. F에서 원인 수정과 재검증을
별도 log로 남긴다.

추가 acceptance 결과: 동시 create/issue/join/command의 같은 request ID 동일 결과와 다른
body 충돌, owner/RW/RO 가입 후 restart의 permission/ReplicaId/endpoint/enrolled_at/epoch
보존, ticket 만료·철회 뒤 active membership resume 및 owner offline pending, RW 양방향
실제 파일 전송, RO local 변경 private 보존·복구와 `Conflict`, `Revoked`, observer 0,
pause/remove/shutdown drain을 `e8122ac`의 네 테스트에서 확인했다. response-loss durable
pending 변형은 local relationship을 별도로 잊은 뒤 owner active membership만으로 복구하고
owner member 목록 및 binding 필드를 보존하는 exact test로 추가 확인했다. 이 결과를 실제
패킷 손실이나 인증 HTTP 응답 유실로 해석하지 않는다. 테스트들은 B evidence
아래 전용 `0700` TMPDIR에서 실행했으며 raw ticket은 테스트 종료 시 workspace 정리로
남기지 않는다.

묶음 acceptance를 처음 같은 프로세스에서 실행했을 때 첫 테스트 뒤 `TempDir`가 삭제한
managed ownership marker를 다음 테스트의 영속 `root_admission` catalog가 읽어 `ENOENT`가
났다(`managed-acceptance-all-retry-20260908-current.log`). fixture가 workspace를 프로세스
끝까지 보존하도록 고친 뒤 전용 TMPDIR 묶음에서 4/4가 통과했다. 별도 `/tmp` 재실행은
tmpfs `usrquota`의 `EDQUOT`로 중단되었으며 제품 오류로 분류하지 않는다(`managed-acceptance-all-retry2-20260908-current.log`);
이후 evidence 디스크의 전용 TMPDIR로 분리했다.

미실행 acceptance: B/C 전체 API 통합과 실제 인증 web 통신, 3기기 owner/RO/RW browser
flow, D N0/relay/address update, E swarm grant/manifest/max-8/fallback, root safety의
전체 경로, Windows native 제품 검증, external Internet, full CI 및 main push.

## D2 network authority checkpoint (`63226b9`)

- `ShareService`가 기존 device endpoint에서 `share/3` control과 정확한
  `share-swarm/1` ALPN을 함께 소유한다. D2의 swarm handler는 grant 없는 데이터 연결을
  명시적으로 거부하며 실제 CAS/chunk 응답은 E가 연결할 때까지 성공으로 주장하지 않는다.
- `SnapshotToken`/`AuthoritativeSnapshot`은 owner/share/consumer epoch, 정렬된 전체
  record, Merkle root/count/frame bound를 검증한다. `ManifestAttestation`은 exact
  record hash, content hash, descriptor hash와 `record.size`를 모두 대조한다.
- `Registry`의 별도 v1 authority tables는 provider/consumer epoch와 fresh signed
  roster를 확인하고, owner provider는 `provider_epoch=0`과 현재 enabled runtime을
  요구한다. RO member provider도 허용한다. row/byte cap, finite GC, managed clock
  high-water/quarantine, restart의 Active/Started `Restarted` blocker를 유지한다.
- revoke는 catalog와 grant/apply deny를 같은 transaction에 기록한다. Issued/Prepared는
  즉시 deny하고 Active/Started는 blocker로 남긴다. grant는 provider와 consumer 양쪽의
  exact activation drain ACK가 있어야 Drained/Complete가 되며 한쪽 ACK, partition,
  restart, TTL만으로 Complete로 승격하지 않는다.
- `ShareSession`에는 bounded snapshot/manifest/grant/activate/revalidate/apply/drain
  methods가 연결되고, `activate_grant`는 wire reply와 request 시작 기준의 local
  monotonic `ActivationLease`를 함께 반환하여 늦은 응답이 새 15초를 시작하지 못하게
  한다. provider 검증 primitive는 local endpoint가 signed provider, remote peer가
  signed consumer인지 확인한다. `Registry::remove_share`는 authority rows를 share
  단위로 정리하며 기존 root/index/CAS 파일은 보존한다.

D2에서 실제 확인한 것은 위 source-level authority/control 경계와 DirectOnly isolated
  tests, request-start activation lease 및 isolated full-net 회귀다. 아직 D 전체 완료가
  아닌 남은 항목은 managed worker의 30초 heartbeat와 90초
  stale refresh, owner 주소 변경/실제 offline 복귀, Internet/N0 relay payload 증거,
  signed roster pagination, E의 실제 `share-swarm/1` verified-CAS multi-provider data
  handler/stream limits, Windows 3-host 및 F full CI 검증이다. DirectOnly stale-address
  refresh와 live provider discovery는 D3 의존성으로 유지한다.

## e8122ac finding 매핑

| finding/게이트 | 현재 상태 | 근거와 남은 검증 |
| --- | --- | --- |
| durable key intent, exact replay, RW/RO rotate permission | fixed + tested | `test-managed-smoke-20260908T222615Z.log`, `test-managed-acceptance-harness-20260908T223255Z.log`; file/registry crash fault injection의 모든 경계는 후속 보강 |
| mixed clock/manual 보존, startup 전체 cleanup | fixed + tested | `test-control-unit-managed-20260908T223623Z.log`, smoke의 mixed reopen; 실제 fault-injected post-worker-start failure는 후속 |
| pending GC mutation serialization, private ancestor/filename guard | fixed + tested | `test-control-unit-managed-20260908T223623Z.log`; live-ticket write/commit barrier의 독립 fault fixture는 후속 |
| per-share failure isolation | fixed in tick path | bad pending이 healthy share를 막지 않도록 per-share 오류를 기록하고 계속한다; 두 share fault fixture는 후속 |
| remove tombstone replay와 ghost-worker 경합 | fixed + tested for replay; lock protected | `test-managed-smoke-20260908T222615Z.log`; deterministic restore/remove barrier와 observer/lease absence 증거는 후속 |
| duplicate pending/active binding 및 immutable root/state | fixed + tested | `test-duplicate-binding-20260908T222550Z.log`, `test-managed-acceptance-harness-20260908T223255Z.log` |
| same-Arc lease handoff 및 provenance(role/share/private root) | fixed; metadata tested | `test-admission-provenance-20260908T222455Z.log`, `test-net-root-admission-20260908T223651Z.log`; forced engine-open failure에서 동일 lease 유지 회귀는 후속 |
| pending-only join retry API 및 response-loss journal mapping | fixed + tested | `test-pending-retry-api-final-20260908T225952Z.log`; background completion race는 preflight 없이 helper 후 journal/record를 재조회해 enrolled 결과를 우선하며 C의 authenticated HTTP adapter는 후속 |
| public cancellation / shutdown ownership | fixed + focused tested | manager-owned shutdown completion은 첫 caller 취소 뒤에도 drain/final persist/ownership release를 끝내며 후속 caller가 같은 결과를 기다린다. public join/retry와 writer barrier도 caller abort 뒤 계속된다. final source `5702ab1`; 실제 test/evidence는 위 B cancellation checkpoint 행 참조 |
| managed observer direct publication / lifecycle precedence | fixed + focused tested | snapshot save 중 observer/failure callback 필드를 merge하고, target pause/resume/revoke 상태를 stale callback이 덮지 않는다. 관측 세대 재저장은 bounded/coalesced (`2`회 후 final save)이며 final fsync 직후 callback은 memory에 보존한 뒤 기존 자동 persistence가 disk에 flush한다. source `5702ab1`; `observer-p2-*`, `final-save-auto-flush-durable-20260909.log` |
| per-member revoke pending/drain and observer zeroing | implemented; immediate owner drain tested | acceptance에서 `Revoked`/observer 0 확인; delayed writer ACK의 `Pending → Complete` 실제 증거는 D/E transport hook 후속 |
| D/E network ownership | pending dependency | provider/requester remote-id, monotonic lease, cancellation/admission-close, roster/heartbeat/address refresh는 B가 구현하지 않으며 protocol audit/root가 선행 확정 |

B checkpoint 뒤 남은 구현·검증: C 인증 경계와 실제 API 통합, pending malformed/lease
fail-closed와 create-intent orphan recovery의 추가 fault tests, paused/revoked 복구,
member별 delayed revocation-pending, snapshot managed totals, Windows 제품 runtime/ACL 및
npm asset guard 검증이다. C 소유 `private.rs` Windows native/DACL 증거와 Docker HOME/UID
검증은 B가 수정하지 않고 C가 담당한다. baseline Linux UDP rebind/Windows watcher/container
실패는 원인 수정 전까지 실패로 유지한다. 이번 checkpoint의 Windows native pre-engine
회귀 실패는 C private namespace 준비의 runner `PermissionDenied` 범주로 별도 보존하며,
ACL 완화나 테스트 skip으로 닫지 않았다.

release 범위: workspace `0.4.0`을 올리지 않고 기존 published `v0.4.0`을 재사용한다.
최종 successful main CI 뒤 release prepare의 `publish=false`, existing tag/no new tag를
root가 확인한다. Container image artifact는 운영 배포 증거와 구분한다.

증거 기본 경로는 `/home/ubuntu/project/DeltaWeave-qsync-evidence/2026-09-08/`이며
`a-contract`, `b-control`, `c-web`, `d-roster`, `e-swarm`, `f-validation`, `ci`
하위에 UTC 시각, command, exit code, SHA, redacted output을 기록한다. 실제 확인 전
요구사항을 완료로 바꾸지 않는다.

## 다음 작업과 인계

1. root가 `f68b2ea`, `e8122ac`, `6c864fa`와 이 진행 대장 commit을 B/C 통합 기준으로 전달한다. source 변경은
   현재 worktree에 보존하며 push/merge하지 않는다.
2. B는 위 남은 수명주기·멱등성·복구 항목과 focused tests를 이어서 실행한다. C는 이
   문서의 route/DTO/auth 계약과 Windows private namespace를 소비한다.
3. protocol audit 결론이 D/E lease 또는 grant contract를 바꾸면 root가 spec/plan과
   B/C 영향 범위를 갱신한 뒤 후속 implementation을 재개한다.
4. final integration, 3기기/Windows/Internet/full CI 검증, release 판단과 goal complete
   판정은 root가 담당한다.

## D3 network lifecycle and Internet control checkpoint (`d8225bf`)

- `SyncSession`의 endpoint-ID fallback은 하나의 monotonic caller deadline을
  connect, bounded N0 lookup, 재연결에 전달한다. fallback이 있는 경우 stale
  persisted address에 전체 시간을 소비하지 않도록 primary dial에 일부 예산만
  배정하며, lookup 결과는 endpoint ID와 실제 주소가 일치하는 첫 usable item에서
  멈춘다. 남은 시간이 없으면 owner에 `Activate`를 전송하지 않는다.
- `ShareService::resume_membership`은 Internet 모드에서 저장된 주소가 stale해도
  동일 device endpoint의 endpoint-ID lookup을 시도하고, 인증된 연결의 실제 주소
  hint를 relationship에 저장한다. peer identity, owner/share, permission, epoch은
  계속 authenticated response와 기존 binding으로 검증한다.
- managed `ShareSession`은 기존 ShareService endpoint와 transport를 공유한다.
  별도 endpoint/index/store를 열지 않으며, data synchronization gate와 독립된
  heartbeat supervisor가 30초 cadence로 roster challenge/heartbeat를 시도한다.
  heartbeat/control 연결은 취소 시 Drop-close되고 engine shutdown에서 task를
  abort·await한 뒤 session을 해제한다. heartbeat 실패도 managed observer의
  terminal `error`로 전달된다.
- share protocol Handler의 첫 stream/Hello와 accepted Session의 첫 sync request는
  하나의 15초 admission deadline 안에서 읽고, preview/unknown-share와 완료된
  session의 peer close 대기도 같은 bounded 정책으로 회수한다. `SyncSession`의
  connect와 share Session handshake도 하나의 caller deadline을 공유한다.
- `TransportObservation`과 `N0LookupObservation`은 주소, endpoint ID, key를
  포함하지 않는 관측값이다. 아래 Internet 실험의 byte/path 값은 인증된 roster
  control exchange의 관측이며 `share-swarm/1` payload 증거로 사용하지 않는다.

| D3 검사 | 결과 | 증거 |
| --- | --- | --- |
| net 전체 isolated suite | `121 passed, 0 failed`, exit `0`; outer `106` unit + `2` admission + `13` shares, child 출력 중복 제외 | `2026-09-09/d3/d3-full-net-isolated-rerun-20260909T051428Z.log`; 05:14:28Z–05:14:55Z, workspace-private HOME/USERPROFILE/TMPDIR, preserved rustup/cargo homes |
| sync heartbeat/lease focused | `2 passed, 0 failed`, exit `0`; outer `2` | `2026-09-09/d3/d3-sync-focused-20260909T051524Z.log`; 05:15:25Z–05:15:51Z |
| activation deadline and resume fallback | 각 outer `1 passed`, exit `0` | `d3-activation-deadline-rerun2-20260909T050211Z.log`, `d3-resume-lookup-rerun-20260909T050550Z.log`; resume의 `MemoryLookup`는 합성 lookup fixture이며 실제 pkarr/dns/N0 증거가 아님 |
| managed heartbeat observer error | outer `1 passed`, exit `0` | `d3-heartbeat-observer-error-20260909T050732Z.log`; owner offline 시 sync 경로에서 observer `error` 확인 |
| share focused tests | `27 passed, 0 failed`, exit `0` | `d3-share-focused-rerun-20260909T050953Z.log` 및 outer-count correction log |
| format/diff | `git diff --check`, `cargo fmt --all -- --check` exit `0` | `d3-quality-20260909T051622Z.log`; 05:16:22Z–05:16:24Z |
| compile/lint | locked all-target check와 strict `clippy -D warnings` exit `0` | `d3-check-all-targets-20260909T051022Z.log`, `d3-clippy-strict-rerun-20260909T051134Z.log`; check는 `52c29fe` 기반의 이전 D3 tree이고, 이후 lint 수정 뒤 최종 strict clippy와 후속 검사가 최종 source를 검증했다 |
| delayed endpoint-ID fallback | `1 passed, 0 failed`, exit `0`; outer `1` | `d3-delayed-fallback-ipv4-rerun2-20260909T053627Z.log`; stale direct hint가 IPv4 test socket을 잘못 재사용하지 않고 남은 deadline으로 delayed lookup/reconnect를 완료 |
| actual Internet/N0/relay harness compile | test binary compile exit `0` | `d3-actual-experiment-compile-observation-20260909T054125Z.log`; workspace-private HOME/USERPROFILE/TMPDIR 및 보존된 rustup/cargo home |
| actual Internet/N0/relay first observation | owner service open 단계에서 즉시 실패, outer `0`, exit `101` | `d3-actual-internet-n0-relay-observed-20260909T054203Z.log`; 상세 원인은 이 실행에서 미확인으로 남기고 이후 새 격리 profile 재실행 결과와 분리 기록 |
| actual Internet/N0/relay observed rerun | `1 passed, 0 failed`, exit `0`; outer `1`, 26.90s | `d3-actual-internet-n0-relay-observed-rerun-20260909T054241Z.log`; 실제 DNS provenance, relay-only socket/path, authenticated roster/permission/epoch, positive tx/rx bytes, owner restart/address change, offline/back resume와 binding 보존을 secret-free JSON으로 기록 |
| final net lint/check after harness | net strict clippy와 control/net all-targets/all-features check exit `0` | `d3-net-clippy-observation-final-20260909T054328Z.log`, `d3-control-net-check-observation-final-20260909T054350Z.log`; 최종 source checkpoint `1d5be66` 직전 실행 |
| final format/diff after harness | `git diff --check`, `cargo fmt --all -- --check` exit `0` | `d3-quality-observation-final-20260909T054409Z.log`; source checkpoint `1d5be66` |
| bounded share admission regression | child 포함 inner/outer `1 passed`, exit `0`, 30.57s; outer count는 `1` | `d3-admission-timeout-regression-rerun-20260909T055518Z.log`; workspace-private TMPDIR와 child HOME/USERPROFILE에서 silent Hello/preview close timeout, slot recovery, 정상 enrollment 확인 |
| final Internet/N0/relay rerun after admission fix | `1 passed, 0 failed`, exit `0`; outer `1`, 26.58s | `d3-actual-internet-n0-relay-final-d8225bf-20260909T055805Z.log`; start `05:58:05Z`, end `05:58:32Z`, source `d8225bf9f912b81a977a257f1bace9f610b368e5`; wrapper exit marker `0`, phase JSON은 실제 DNS1/relay selected/IP 없음/tx-rx 양수/동일 binding 보존을 기록 |
| final admission lint/check | strict `clippy -D warnings` 및 locked control/net all-targets/all-features check exit `0` | `d3-admission-timeout-clippy-final-20260909T055617Z.log`, `d3-admission-timeout-check-final-20260909T055633Z.log`; source `d8225bf` 직전 |

isolated resume의 `MemoryLookup` 검사는 합성 endpoint-ID lookup fixture이고 실제
pkarr/dns/N0 증거가 아니다. source checkpoint `1d5be66`의 별도 ignored 실험은 같은
Linux host에서 분리한 owner/member identity로 실제 DNS provenance와 relay-only
control path를 확인했고, owner 재시작·주소변경·offline/back resume의 인증 binding을
보존했다. JSON 관측의 tx/rx byte delta는 share-swarm payload 증거가 아니며, 이
실험은 E의 verified-CAS 다중 provider payload 또는 세 기기 검증을 주장하지 않는다.
durable late activation receipt/ACK, provider/consumer 양쪽 drain, Windows 3-host 및
F release 검증은 여전히 E/F 순차 범위다.

## D3 bounded admission follow-up (`e437b85`)

- `wait_closed_bounded`는 bounded close-wait가 만료되면 retained `Connection` clone에
  의존하지 않고 명시적으로 connection close를 전송한다. silent/preview admission
  회귀는 peer close 관측과 slot 회수를 함께 확인한다.
- `open_session_until`은 deadline이 이미 만료된 경우 `exchange`를 poll하기 전에
  `Offline`을 반환한다. 따라서 zero-duration timeout이 Session BI stream이나 Hello를
  생성하는 부작용을 낼 수 없다. 이 변경은 wire ordinal/정상 deadline 경로를 바꾸지
  않는다.

| 검사 | 결과 | 증거 |
| --- | --- | --- |
| expired session no-Hello | outer `1 passed`, exit `0`, 1.05s | `2026-09-09/d3/availability/expired-session-no-hello-20260909.log`; 06:14:59Z–06:15:18Z, pre-commit HEAD `87d6687` plus the source diff committed as `e437b85` |
| silent/preview timeout close | outer `1 passed`, exit `0`, 30.53s; nested child는 중복 집계하지 않음 | `2026-09-09/d3/availability/admission-close-regression-20260909.log`; 06:15:32Z–06:16:03Z |
| net all-target check | exit `0` | `2026-09-09/d3/availability/net-check-availability-20260909.log`; source 변경 적용 후 check |
| net strict clippy (`-D warnings`) | exit `0` | `2026-09-09/d3/availability/net-clippy-availability-20260909.log`; source 변경 적용 후 clippy |

CI dispatch `34317854588`는 integration ref `63cb43c4142e0d28931dd5f81decb5506aa5d948`
에서 실행됐으며, 이 후속 로컬 commit은 해당 실행에 포함되지 않는다. 실제 integration
CI 결과와 이 후속 source 검사는 별도 근거로 집계한다.

## E1 durable activation receipt checkpoint (`bfe6f92`)

- `ActivationStatus`와 `ActivationCancel`은 기존 `Operation`/`Reply` 뒤에 append되어
  postcard wire ordinal을 보존한다. `ActivationBinding`은 owner/share/consumer/provider,
  양쪽 epoch, manifest/request hash와 nonce를 고정하며 raw key나 endpoint secret을
  담지 않는다. `ActivationReceipt::verify_for`는 이 binding과 요청한 activation ID를
  exact 비교한다.
- owner registry의 status 조회는 `GrantRow`와 양쪽 `GrantDrainState`를 하나의 redb
  read snapshot에서 읽고 lease를 갱신하지 않는다. 저장 상태가 `Issued`인 동안 wall
  clock만으로 `Expired`를 합성하지 않으며, `ActivationCancel`이 같은 transaction에서
  `Issued → Denied` 또는 만료된 `Issued → Expired`를 기록한다. `Active`/`Restarted`는
  늦은 취소가 지우지 않고 provider/consumer 양쪽 drain ACK가 모두 있을 때만 `Drained`가
  된다. 새 grant/data admission에는 현재 epoch 검사가 남고, response-loss recovery의
  status/cancel만 기존 exact binding을 보존한 채 membership epoch 변경을 허용한다.
- `ShareService::unload_owned_share`는 runtime을 먼저 pause하고 registry 삭제가
  성공한 뒤에만 runtime map에서 제거한다. nonterminal activation/apply row가 있으면
  remove를 `RevocationPending`으로 보류하며, 같은 ShareId를 쓰는 foreign-owner
  relationship은 삭제하지 않는다.

| E1 검사 | 결과 | 증거 |
| --- | --- | --- |
| registry authority status/cancel/remove | `13 passed, 0 failed`, exit `0`, outer `13` | `2026-09-09/e1/registry-e1-final2-20260909T064242Z.log`, 06:42:42Z–06:43:00Z |
| bilateral drain + idempotent cancel service regression | `1 passed, 0 failed`, exit `0`, logical outer `1`; nested child 출력 미합산 | `2026-09-09/e1/service-bilateral-drain-e1-final2.log`, 06:48:27Z 완료 |
| activation recovery binding after epoch change | outer `1 passed`, exit `0` | `2026-09-09/e1/service-recovery-binding-final-20260909T064351Z.log` |
| locked net check | `cargo check --locked -p deltaweave-net --all-targets --all-features`, exit `0` | `2026-09-09/e1/net-check-e1-final2.log`, 06:48:52Z 완료; `CARGO_TARGET_DIR` 공유 캐시와 jobs `4` |
| strict net clippy | `cargo clippy --locked -p deltaweave-net --all-targets --all-features -- -D warnings`, exit `0` | `2026-09-09/e1/net-clippy-e1-final2.log`, 06:49:14Z 완료 |
| format | `cargo fmt --all -- --check`, exit `0` | `2026-09-09/e1/fmt-e1-final.log`, 06:47:55Z |

E1은 provider/consumer durable intent와 late/lost Activate receipt의 양단 query/ACK
복구, paused runtime의 별도 admission-open 확인, 실제 `share-swarm/1` verified-CAS
다중 provider payload를 구현하지 않는다. 이 항목들은 E2/E3의 후속 소비 계약이며,
timeout/TTL만으로 Active/Restarted를 Complete로 만들지 않는 조건을 유지한다.

CI fixture `af5b21a23ce418811abae0c30da9c0ca96179da6`는 실제 owner-signed roster와
별도 roster/heartbeat 응답을 제공하도록 수정했고 기존 rollback/divergence,
tombstone, 파일 보존 assertion을 유지했다. `ci-malicious-owner-fixture-final2-20260909T064051Z.log`
의 두 cargo `test ... ok` 줄은 parent와 isolated child가 같은 logical outer test를
출력한 것이므로 `logical_outer_count=1`로 정정했다. 그 실행은 E1 net dirty tree에서
수행되어 `af5b21a` exact-tree 검증으로 집계하지 않으며, fixture commit과 원격
integration CI는 별도 근거다. 새 dispatch `34320666264`는 integration ref
`af5b21a23ce418811abae0c30da9c0ca96179da6`에서 실행됐고, 이 기록 시점에는 결과를
완료로 표시하지 않는다.
