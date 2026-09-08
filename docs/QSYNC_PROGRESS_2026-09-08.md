# QSYNC 2026-09-08 진행 대장

기준 커밋: `baed5c0164a2e10fdbe6a1e5a31f7e897c675ced`

기준 확인: contracts worktree의 `HEAD`와 `origin/main`이 위 SHA로 일치했다.

작성 agent: `qsync_contracts` / Luna Max

현재 단계: A 계약·계획 문서 작성 중. 제품 소스 변경 없음.

## 작업 공간과 agent

| 역할 | agent/worktree | branch | 상태 |
| --- | --- | --- | --- |
| integration/root | `/home/ubuntu/project/DeltaWeave-qbittorrent-20260908` | `integration/qbittorrent-sync-20260908` | root 조정 대기 |
| A contracts | `/home/ubuntu/project/DeltaWeave-qsync-contracts-20260908` | `feat/qsync-contracts-20260908` | 문서 작성 |
| A protocol audit | 별도 agent `qsync_protocol_audit` | root 기록 예정 | D/E 입력 대기 |
| baseline validation | `luna_protocol_plan` 재사용 | root 기록 예정 | CI 실패 원인 조사 |
| B control | `/home/ubuntu/project/DeltaWeave-qsync-control-20260908` | `feat/qsync-control-20260908` | 계약 후 착수 |
| C web | `/home/ubuntu/project/DeltaWeave-qsync-web-20260908` | `feat/qsync-web-20260908` | 계약 후 착수 |
| validation | `/home/ubuntu/project/DeltaWeave-qsync-verification-20260908` | `test/qsync-verification-20260908` | 후속 검증 |

## A에서 고정한 계약

- Manager 한 개당 ShareService/device identity 하나이며 managed config/pending 또는
  첫 managed 요청에서만 lazy-init한다. 기본 Internet/N0, test DirectOnly 주입이다.
- `ManagerOptions`, `create_share`, `preview_share_key`, `validate_share_key`,
  `join_share`, `resume_membership`, `list_keys`, `issue_key`, `rotate_key`,
  `revoke_key`, `list_members`, `revoke_member`, `remove_share`, `share_command`와
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
| product source edits | 없음 | allowed files만 untracked |
| A docs validation | `git diff --cached --check` exit 0; allowed-file list 3개 | 이 commit |
| Rust/npm/Windows/Internet tests | 실행하지 않음 | 구현 후 evidence 필요 |

## baseline 실패와 남은 문제

baseline push CI evidence는
`/home/ubuntu/project/DeltaWeave-qsync-evidence/2026-09-08/github-failure-snippets-20260908.txt`에
보존한다. run `34251266098` Linux는 owner shutdown 뒤 같은 주소 rebind의
`AddrInUse`, Windows는 control watcher 10초 timeout, run `34251265862` Container는
UID/GID 65532 self-test receiver `PermissionDenied`였다. Security run
`34251266012` 성공은 이 세 실패를 상쇄하지 않는다. F에서 원인 수정과 재검증을
별도 log로 남긴다.

미실행 acceptance: B/C 구현과 실제 통신, 3기기 owner/RO/RW browser flow, restart/
response-loss/resume, revoke/rotation, RO preservation, D N0/relay/address update,
E swarm grant/manifest/max-8/fallback, root safety, Windows native, external Internet,
full CI 및 main push.

release 범위: workspace `0.4.0`을 올리지 않고 기존 published `v0.4.0`을 재사용한다.
최종 successful main CI 뒤 release prepare의 `publish=false`, existing tag/no new tag를
root가 확인한다. Container image artifact는 운영 배포 증거와 구분한다.

증거 기본 경로는 `/home/ubuntu/project/DeltaWeave-qsync-evidence/2026-09-08/`이며
`a-contract`, `b-control`, `c-web`, `d-roster`, `e-swarm`, `f-validation`, `ci`
하위에 UTC 시각, command, exit code, SHA, redacted output을 기록한다. 실제 확인 전
요구사항을 완료로 바꾸지 않는다.

## 다음 작업과 인계

1. 이 worktree에서는 세 문서만 `git diff --check`와 allowed-file 검사 후 commit한다.
2. root가 이 commit SHA를 B/C worktree에 전달하고, B는 stable create intent,
   `owned_configs()` orphan recovery, private pending intent, mandatory resume hook부터
   구현한다. C는 이 문서의 route/DTO/auth 계약을 소비한다.
3. protocol audit 결론이 D/E lease 또는 grant contract를 바꾸면 root가 spec/plan과
   B/C 영향 범위를 갱신한 뒤 후속 implementation을 재개한다.
4. 이 agent는 push/merge/main 변경을 하지 않는다. final integration, verification,
   release 판단과 goal complete 판정은 root가 담당한다.
