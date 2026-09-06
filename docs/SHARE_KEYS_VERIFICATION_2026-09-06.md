# 공유 키 구현 검증 기록

현재 **진행 중인 검증 기록**이다. 아래 선행 단계의 통과는 공유 키 기능 전체의
완료나 main 반영을 뜻하지 않는다. 최종 기능·플랫폼 검증 후 이 문서를 갱신한다.

## 선행 콘솔 통합

기준 상태는 [변경 전 조사](SHARE_KEYS_BASELINE_2026-09-06.md)에 기록했다.
`0cc209c`에서 개선 콘솔을 선별 통합했고, 독립 리뷰가 발견한 저장 공간 제한과
중복 집계 문제를 `56933b8`에서 수정했다. 수정 재검토에서 해당 세 항목이 해결됐고
새로운 중요 결함이 없음을 확인했다.

| 대상 커밋·범위 | 실제 실행 | 결과 |
| --- | --- | --- |
| `0cc209c` 전체 Rust | workspace, all targets/features | 205개 통과 |
| `0cc209c` 웹 UI | Vitest + production build | 21개 통과, 빌드 통과 |
| `0cc209c` 정적 검사 | workspace clippy `-D warnings`, fmt | 통과 |
| `0cc209c` 기존 실제 브라우저 | 별도 기기의 실제 파일 동기화 | 통과 |
| `56933b8` 수정 영향 범위 | store/net/sync/control, all targets/features | 85개 통과 |
| `56933b8` Windows 대상 | `cargo check --target x86_64-pc-windows-gnu`의 net/sync/control | 교차 컴파일 통과; Windows 실행 아님 |
| `56933b8` 수정 정적 검사 | 해당 crate의 clippy `-D warnings`, fmt, diff check | 통과 |

실제 QUIC 시험에서 충족할 수 없는 여유 공간 설정이 파일/CAS 수신 전에 거부되는지
확인했다. 같은 파일시스템에서는 CAS와 예정된 최종 파일 생성 공간을 합산하고,
최종 파일 생성 직전에 다시 검사한다. 서로 다른 파일시스템의 예산 계산은 결정적
입력으로 검증했다. 실제 두 파일시스템을 이용한 전송과 동시 writer의 공간 예약
보장은 이 선행 증거에 포함하지 않는다.

독립 Chromium 검사에서는 전용 빈 상태로 로그인·로그아웃, 탐색, 1440/360 폭에서
가로 넘침과 브라우저 오류가 없음을 확인했다. 별도 axe 4.12.1 검사는 로그인과
인증 후 개요에서 자동 판정 위반 0개였다. 배경 겹침 때문에 자동 판정하지 못한
문구는 별도로 확인했다. 이는 공유 키 대화상자와 모든 상태의 접근성 검증을
대신하지 않는다.

## 실제 N0 검색 시험

iroh 1.1.0의 [N0 preset](https://docs.rs/iroh/1.1.0/iroh/endpoint/presets/struct.N0.html)은
주소 게시·검색과 Number 0 릴레이를 구성한다. [Endpoint 문서](https://docs.rs/iroh/1.1.0/iroh/endpoint/struct.Endpoint.html)는
주소 힌트 없이 endpoint ID로 연결할 때 구성한 검색 서비스를 사용한다고 설명한다.

이를 기준 커밋 `22664cf`의 실제 CLI로 시험했다. 별도 identity와 임시 root를 가진
두 기기에서 클라이언트에 직접 주소나 relay URL을 전달하지 않았다. 최초 파일
동기화는 첫 시도 2.09초에 성공했다. 공유 기기를 같은 identity, 새 UDP 포트로
재시작하고 파일을 수정했으며, 새 클라이언트 프로세스도 주소 힌트 없이 첫 시도
1.82초에 변경된 바이트를 받았다. 두 실행 모두 실제 N0 relay 주소를 보고했다.

범위는 **같은 Linux 호스트에서 실제 N0 서비스를 이용한 시험**이다. 실제 payload가
릴레이를 통과했는지, 서로 다른 외부 NAT나 인터넷 단절에서 동작하는지는 이 결과로
판단하지 않는다.

이후 `427320c`의 v3 공개 API를 별도 임시 Rust 프로그램에서 실제로 시험했다.
서명된 RO 키와 지속된 가입 관계 모두 직접 주소·relay 힌트가 0개였다. 서로 다른
identity에서 온라인 검증은 첫 시도 620ms, 가입은 24ms에 성공했고, 실제 131,071
바이트를 받아 디스크에 생성한 뒤 원본과 비교했다.

키를 버리고 양쪽 서비스를 모두 종료한 후 같은 사설 상태로 다시 열었다. 공유 기기
identity는 유지하면서 UDP 포트는 36144에서 46235로 바꿨다. 참여자는 새 endpoint
인스턴스에서 지속된 관계만 사용했고, 키 검증·가입을 다시 수행하지 않았다.
주소 힌트 없는 첫 스냅샷 요청이 470ms에 성공했으며 갱신된 147,457바이트의 실제
수신·파일 생성·내용 비교도 통과했다. 두 공유 기기 실행 모두 N0 온라인 상태와
relay 주소를 확인했다.

이 역시 같은 Linux 호스트의 실제 외부 N0 검색 시험이다. 선택된 QUIC 전송 경로는
공개 API에서 노출하지 않아 payload의 릴레이 통과는 확인하지 못했다. 서로 다른
NAT, 강제 릴레이, 인터넷 단절, 자동 worker의 오프라인 복구나 Windows 실행을
검증한 것으로 해석하지 않는다. 재현 프로그램과 결과는
`/tmp/deltaweave-v3-n0-probe-1UBD8u/`에 있으며 모든 시험 서비스는 종료했다.

## v3 권한 계층 구현 검사

`427320c`의 독립 리뷰에서 확인된 세 결함을 `b6b5263`에서 수정했다. 사설 상태와
public root의 전역 겹침, 거부 전에 디렉터리를 만드는 순서, 이전 증명의 ID 수
제한을 보완했으며, 독립 재검토에서 모두 해결됐고 새 중요 결함이 없음을 확인했다.
아래 결과는 해당 단계의 실제 명령 로그에서
확인했으며, 아직 전체 공유 키 기능이나 UI의 완료를 뜻하지 않는다.

| 범위 | 실제 실행 | 결과 |
| --- | --- | --- |
| net | 모든 target/feature의 단위·실제 QUIC·별도 프로세스 시험 | 59개 통과 |
| index/sync/control | 모든 target/feature | 63개 통과 |
| 정적 검사 | 해당 네 crate의 clippy `-D warnings`, fmt, diff check | 통과 |
| Windows 대상 | 해당 네 crate의 GNU 대상 `cargo check` | 교차 컴파일 통과; Windows 실행 아님 |
| `b6b5263` 수정 영향 범위 | net/sync/control, 모든 target/feature | 95개 통과 |
| `b6b5263` 정적·Windows 대상 | 해당 세 crate의 clippy/fmt 및 GNU 대상 `cargo check` | 통과; Windows 실행 아님 |

실제 서로 다른 identity의 QUIC 시험은 RO 파일·삭제·디렉터리 요청 거부 후 원본
바이트와 root hash, 다른 공유 접근 거부, v1/v2 ALPN 거부, 정상 RW counter 증가와
다른 기기의 counter 위조 거부를 확인했다. 실행 중인 namespace/CAS/content 작업을
결정적 barrier에서 멈추고, 권한 해제가 해당 작업을 기다린 뒤 반환하는지도 검사했다.
해제의 drain 부분만 제거한 별도 결함 주입에서는 세 시험 모두 예상한 조기 반환
오류로 실패했으며 원래 코드를 복원한 최종 실행은 통과했다.

별도 프로세스 종료 시험은 인덱스 record와 share metadata가 redb commit 전후에
함께 유지되는지 확인했다. 기존 v2 이력과 수신만 했던 참여자의 이전, 활성 공유의
인덱스가 사라졌을 때 초기화 거부, 중단된 생성의 재시도, 레거시 작업이 끝나기 전
root 재사용 거부도 검사했다. 증거는 `/tmp/deltaweave-task2-verification-_3wy9uzb/`에
있다. 테스트마다 별도 사용자 profile을 사용했으며 실제 사용자 registry는 초기화하지 않았다.

출처 기록은 인증된 바로 앞 RW 기기의 최근 작업 512개를 보관한다. 지속 권한과
신뢰한 counter 상한은 별도로 유지하며 매 변경 때 검사한다. 무제한 감사 이력이나
vector 작성자에 대한 암호학적 증명은 제공하지 않는다.

`b6b5263`의 수정 시험은 비공개 경로·public root의 양방향 등록 순서와 별도 프로세스
경합, 거부 전후 디렉터리·catalog 바이트 불변, 제한에 걸린 이전 가입 거부 후 기존
writer의 실제 변경 채택을 확인했다. RO 구성원이 활성 RW 키로 다시 가입해도 권한이
RO로 유지되는 시험도 통과했다. 별도 로그는 `/tmp/deltaweave-task2-fix1-r0exr828/`에 있다.

## 기존 데이터 이전 시험 준비

기준 버전으로 identity와 인덱스를 만들고 세 차례 스캔하여 수정·삭제 이력을 남겼다.
두 번째 identity를 가진 참여 기기와 실제 v2 동기화를 두 차례 수행했다. 양쪽에는
동일한 파일 3개, 디렉터리 1개, 삭제 기록 1개와 두 기기의 논리 ID가 존재한다.
각 인덱스를 기존 콘솔 설정에 일시 정지한 수동 폴더로 가져왔으며, 파일과 기존
identity를 유지했다. 실제 공유 키 전환은 아직 이 준비 단계의 증거가 아니다.

## 기존 파일시스템 간 복구 결함

기준 `22664cf`의 실제 v2 CLI로 root를 `/tmp` 파일시스템, 사설 상태를
`/dev/shm` 파일시스템에 두고 시험했다. 최초 전송은 성공했지만 원격 편집 적용은
기존 파일을 사설 trash로 `rename`하는 단계에서 `Invalid cross-device link`
(EXDEV)로 실패했다. 기존 파일의 원본 81,920바이트가 정확히 남았음을 확인했다.
첫 시험은 편집 실패에서 종료했으며, 별도 후속 시험에서 원격 삭제도 시도했다.
삭제 역시 같은 파일시스템 간 trash 이동에서 EXDEV로 실패했고 원본은 유지됐다.

이는 선행 통합 이전에도 존재하는 결함이다. 공유 키의 저장 위치 선택과 RO 복구에도
영향을 주므로 동기화·저장 복구 구현 단계에서 해결하고 실제 두 파일시스템으로
다시 검증한다. 증거는 `/tmp/deltaweave-crossfs-probe-zstw7got/result.json`과
`/dev/shm/deltaweave-crossfs-state-gnobqc4z/`에 있다. 해당 시험 프로세스는 종료했다.

## 동기화·복구 단계의 중간 검증

아래는 구현 중인 동기화 단계에서 실제 실행한 중간 결과다. 아직 이 단계의 최종
커밋과 독립 리뷰가 끝나지 않았으며, 이후 수정한 전체 코드를 검증한 결과로 보지 않는다.

| 시험 | 확인한 동작 | 증거 |
| --- | --- | --- |
| 서로 다른 owner/RW/RO endpoint | RW 전송, RO 수신·로컬 수정 보존·로컬 추가 비전파·로컬 삭제 복원·원격 삭제 보존 및 서비스/엔진 재시작 | `/tmp/dw-task3-shared-green.log` |
| 실제 두 파일시스템의 RO 중단 | prepared/path-prepared/preserved/materialized/adopted 각 단계에서 프로세스 종료 후 원래 사본을 같은 경로에 하나만 유지하며 복구 | `/tmp/dw-task3-restarts-green.log` |
| 실제 v1/v2 전송 | root 장치 36과 state 장치 26에서 최초 전송·교체·삭제 및 보존 journal 4건의 commit 통과 | `/tmp/dw-task3-physical-legacy.log` |
| 악성 RO 출처 대체 | 유효한 형식의 변조된 v2/v3 스냅샷을 제공해도 managed RW가 거부하고 owner/다른 RW 파일은 그대로 유지 | `/tmp/dw-task3-laundering.log` |

추가로 실제 owner 프로세스를 파일 생성 후 인덱스 채택 전에 종료하여, 재시작 시
상대의 변경을 소유자의 새 변경으로 잘못 기록하는 결함을 재현했다. 실패 증거는
`/tmp/dw-task3-causal-recovery-red.log`다. 정확한 인과 기록과 인증된 출처를 보존하는
복구, 권한 해제된 적용의 안전한 되돌리기 및 경합 시 데이터 보존을 수정·검증 중이다.
이 실패가 해결되고 영향 범위 검사와 리뷰를 통과하기 전에는 해당 단계를 완료로
판정하지 않는다.

## 증거 위치와 남은 필수 검증

작업 환경의 임시 증거 위치:

- 선행 기존 브라우저: `/tmp/deltaweave-share-task1-legacy-browser/`
- 독립 콘솔 캡처: `/tmp/deltaweave-console-smoke-{login,authenticated,mobile}.png`
- 독립 접근성: `/tmp/deltaweave-task1-a11y-*.json`
- 실제 N0 시험: `/tmp/deltaweave-n0-probe-ct7r4nxj/`
- 실제 v3 N0 시험: `/tmp/deltaweave-v3-n0-probe-1UBD8u/`
- 이전 시험용 기존 데이터: `/tmp/deltaweave-migration-fixture-4e59eewe/`

전용 테스트 identity/관리자 값은 사설 디렉터리에만 두며 이 문서나 캡처에 넣지 않는다.
원래 사용자 콘솔과 파일은 별도로 유지한다.

아직 필요한 항목은 실제 RW/RO 동기화와
충돌·중단 복구, 실제 브라우저 복사→붙여넣기→폴더 선택→파일 전송, 기존 데이터의
공유 키 전환, 새 흐름의 접근성과 비밀 노출 검사, 최종 전체 품질 검사 및 Windows
실행이다. [설계의 완료 증거 행렬](superpowers/specs/2026-09-06-folder-share-keys-design.md)을
기준으로 최종 결과를 대조한다. 원격 push나 main 통합은 아직 수행하지 않았다.
