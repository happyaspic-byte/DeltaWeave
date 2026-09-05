# 6개 Goal 통합 기록

기준: main 75ffab74ee3de4dd1d071c13ab04ae26fb4e6e5b
통합 브랜치: integration/six-goals-20260905
원본 worktree의 브랜치, HEAD, 인덱스와 미커밋 파일은 변경하지 않는다.

## 원본 현황

| 작업 | 시작 상태 | 보존 커밋 |
| --- | --- | --- |
| bug-fix-update | 미커밋 변경, 기준 대비 추가 커밋 0 | ed1ac1ec83555c5d99b42748292539075e696584 |
| security-update | 미커밋 변경, 기준 대비 추가 커밋 0 | 1898d5b2f9b6b747d97748d09cbd8c06f6d17b4c |
| performance-update | 미커밋 변경, 기준 대비 추가 커밋 0 | 9269dd4c3fc7dc3eac10540dcd711e178105fae7 |
| test-hardening | 미커밋 변경, 기준 대비 추가 커밋 0 | 3e40c607c6175cbadfa0a4c80edfbde78e300363 |
| ui-update | 미커밋 변경, 기준 대비 추가 커밋 0 | aaba96515bc28d475f8835d9fb53296eca6874aa |
| docs-sync-update | 미커밋 변경, 기준 대비 추가 커밋 0 | 0c077d1a73478d043cc744cd02da569cee4690a3 |

## 통합 순서와 의존성

1. bug-fix-update: 비동기 로컬 스냅샷 재검사와 6개 회귀.
2. security-update: 중복 스냅샷 검사는 비동기 버전으로 통일하고 경로·권한·wire 방어 병합.
3. performance-update: Merkle prefix 조회 최적화와 측정 자료.
4. test-hardening: 보안 변경 위에서 저장소·causal 네트워크 회귀 확인.
5. ui-update: CLI 출력과 웹 API를 강화된 엔진 위에 연결.
6. docs-sync-update: 최종 구현에 맞춰 문서 충돌과 오래된 설명 정리.

## 경계

현재 main 작업 폴더에는 별도의 미커밋 웹 UI·엔진 변경이 있다. 이번 6개 작업과 다른 결과이므로 통합 입력으로 자동 포함하지 않았다. 기존 luvus 및 claude worktree도 보존한다.
각 작업 보고서의 과거 테스트 결과는 원본 작업의 증거이며 통합 결과의 통과 증거로 대체하지 않는다.

## 병합 결과와 해결한 충돌

| 순서 | 병합 커밋 | 확인한 변경 |
| --- | --- | --- |
| 1 | 4967c9f | 계획 후 로컬 변경을 비동기 재스캔으로 차단; 원격 작업만 있는 경우도 검사 |
| 2 | 0d4be16 | 파일·DB 권한, 복구 이력, 키 생성 순서, Merkle 입력 검증, 이름 충돌과 오류 노출 방어 |
| 3 | efa39b1 | Merkle prefix 조회 범위 탐색 최적화와 원본 성능 측정 자료 |
| 4 | 22c3b83 | 파일 무결성·저널 재개·causal 네트워크 회귀 보강 |
| 5 | 6c64b4d | 기본 JSON을 유지하는 CLI text 출력 및 로컬 웹 관리 UI |
| 6 | d9d280f | 최종 엔진·CLI·웹 API에 맞춘 사용자 및 내부 문서 |

- sync 충돌: 중복 사전 검사를 비동기·무조건 검사로 통일했다. 보안의 새 로컬
  변경 보호 의도와 bug-fix의 원격 전용 계획 보호를 함께 유지하고 두 작업의
  추가 테스트를 모두 보존했다.
- store 충돌: 같은 위치에 추가된 서로 다른 회귀 테스트를 모두 유지했다.
- CLI 자동 병합 후 E0061: UI가 `serve(args, output)`으로 바꾼 함수에 보안 테스트가
  이전 호출 형식을 사용했다. JSON Output 인자만 추가했으며 키 부재 단언은 유지했다.
- 문서 충돌: 아키텍처의 적용 순서·전체 사전 스캔·경로 충돌 검사, 프로토콜의
  부분 적용 한계·정적 오류 응답, 위협 모델의 v1/v2 차이를 함께 반영했다.
- Git 충돌 없이 남은 문서 불일치도 정리했다. 9개 crate, CLI 출력 모드,
  별도 HTTP 관리 API와 인증 경계를 명시했다. 과거 감사 기록은 당시 결과로 표시했다.

## 단계별 실제 검증

로그 디렉터리: `/home/ubuntu/project/DeltaWeave-integration-evidence/2026-09-05/`.
명령은 통합 worktree에서 실행했다. 각 단계 결과를 확인한 다음 병합 커밋을 남겼다.

| 단계·명령 | 결과 | 로그 |
| --- | --- | --- |
| 기준 `cargo test --locked --workspace --all-targets --all-features` | 103 통과 | baseline-tests.log |
| bug `cargo test --locked -p deltaweave-sync --lib` | 7 통과 | 01-bug-tests.log |
| security `cargo test --locked --workspace --lib --all-features` | 116 통과 | 02-security-tests.log |
| security `cargo test --locked -p deltaweave --bin deltaweave identity` | 4 통과 | 02-security-cli-tests.log |
| performance `cargo test --locked -p deltaweave-core -p deltaweave-reconcile -p deltaweave-sync --all-targets --all-features` | 37 통과 | 03-performance-tests.log |
| hardening `cargo test --locked -p deltaweave-net -p deltaweave-store --lib --all-features` | 49 통과 | 04-hardening-tests.log |
| UI `cargo test --locked --workspace --lib --bins --test output --test api --all-features` | 처음 E0061; 호출 수정 후 168 통과 | 05-ui-tests.log, 05-ui-tests-fixed.log |
| `cargo build --locked --workspace --all-features` | 종료 0 | build.log |
| `node --test crates/deltaweave-web/ui/model.test.js` | 8 통과 | 05-ui-model-tests.log |
| `node --check crates/deltaweave-web/ui/app.js` | 종료 0 | 직접 명령 결과 |
| 실제 browser.py, agent-browser/Chromium | 종료 0, 양쪽 파일 SHA-256 일치 | browser.log, browser/browser-results.json |
| `DELTAWEAVE_BIN=<통합 debug binary> bash scripts/test-p2p-loopback.sh` | 104857600 bytes 전송, SHA-256 일치, 종료 0 | p2p-100mib.log |
| `cargo audit --no-fetch --deny warnings --ignore RUSTSEC-2024-0436` | 종료 0, 캐시 advisory DB의 436개 의존성 검사 | audit.log |
| README와 docs 최상위 Markdown의 로컬 파일 링크 | 110개 대상 존재 | 직접 Python 검사 결과 |

브라우저에서 인증 오류와 복구, 실제 빈 폴더·한영 파일 검사, 피어 거부,
수신 중지·재시작, 실제 양방향 동기화, 오래된 상태 응답 방지, 서버 종료 후
오류 상태, 키보드 포커스와 reduced motion을 확인했다. 360/768/1440px 캡처와
넘침 검사도 통과했다. 360px·1440px 결과 이미지를 직접 열어 확인했다.
스크린샷은 위 로그 디렉터리의 `browser/web-verified-*-light.png`에 있다.

독립 읽기 전용 코드 검토에서 추가 critical/important 통합 문제는 발견되지 않았다.
5개 코드 작업에서 새로 추가한 Rust 함수·테스트가 모두 유지된 것을 비교했다.
검토자는 별도 빌드·테스트를 실행하지 않았으며 문서 검토는 통합 담당자가 수행했다.

## 보존과 남은 범위

6개 원본 worktree의 HEAD, porcelain 상태, 별도 인덱스로 계산한 전체 파일 tree가
스냅샷과 일치했다. 원본 브랜치에는 추가 커밋을 만들지 않았다.
main은 75ffab7이며 배포·push·기존 worktree 삭제는 하지 않았다.

- Linux x86_64 및 로컬 Chromium·loopback 검증이다. 실제 Windows·macOS·NAS,
  원격 LAN/WAN·방화벽·릴레이와 운영 배포는 검증하지 않았다.
- 성능 원시 자료는 performance-update 단독 변경 당시의 실측이다. 통합된
  보안·버그 재스캔 비용을 포함한 같은 개선율은 재측정하지 않았으므로 보장하지 않는다.
- `paste` 유지보수 경고 RUSTSEC-2024-0436은 기존 예외를 유지했다. 감사는
  로컬 advisory DB 기준이며 이 통합 단계에서 DB를 새로 fetch하지 않았다.
- 기존의 OS 경로 경쟁·전원 차단 내구성·상태 암호화·키 수명주기 한계는
  위협 모델과 각 원본 감사 기록을 참조한다. 전체 보안 인증을 뜻하지 않는다.
- 현재 main 폴더의 미커밋 변경은 이번 입력에 포함되지 않았다. 추후 main에
  반영하려면 해당 변경과의 관계를 별도로 검토해야 한다.

## 최종 저장소 검증

`bash scripts/verify-release.sh`는 종료 코드 0과
`DeltaWeave release verification: PASS`로 9단계를 모두 마쳤다. Compose skip은 없었다.

- rustfmt와 Clippy(`-D warnings`) 통과.
- 전체 Rust 테스트 171개 통과, 실패·무시 0.
- 경고를 오류로 처리한 rustdoc 생성 통과.
- CLI 자체 검사: 양방향 동기화·삭제 true, 충돌 사본 1개 보존, 재시작 추가 작업 0.
- 별도 serial 장애 복구 테스트 3개 통과.
- 실행 파일의 seed 424242 / 16 MiB fault-test 통과; serve·sync-once 종료 및 복구,
  양쪽 restart action 0, error null 확인.
- 문서 미디어·셸 구문·diff 위생·Portainer Compose 설정 검사 통과.

검토 준비를 마쳤다. 통합 코드의 신규 실패나 미해결 병합 충돌은 없다.
통합 worktree: `/home/ubuntu/project/DeltaWeave-integration-20260905`
통합 브랜치: `integration/six-goals-20260905`

검토 명령:

```bash
cd /home/ubuntu/project/DeltaWeave-integration-20260905
git log --oneline --first-parent 75ffab7..HEAD
git diff --stat 75ffab7..HEAD
git diff 75ffab7..HEAD
```

main 반영과 배포는 수행하지 않았으며 이 보고서는 운영 승인 요청이 아니다.
