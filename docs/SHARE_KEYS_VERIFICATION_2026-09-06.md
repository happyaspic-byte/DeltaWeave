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
판단하지 않는다. v3 공유 키 흐름의 네트워크 시험도 별도로 필요하다.

## 기존 데이터 이전 시험 준비

기준 버전으로 identity와 인덱스를 만들고 세 차례 스캔하여 수정·삭제 이력을 남겼다.
두 번째 identity를 가진 참여 기기와 실제 v2 동기화를 두 차례 수행했다. 양쪽에는
동일한 파일 3개, 디렉터리 1개, 삭제 기록 1개와 두 기기의 논리 ID가 존재한다.
각 인덱스를 기존 콘솔 설정에 일시 정지한 수동 폴더로 가져왔으며, 파일과 기존
identity를 유지했다. 실제 공유 키 전환은 아직 이 준비 단계의 증거가 아니다.

## 증거 위치와 남은 필수 검증

작업 환경의 임시 증거 위치:

- 선행 기존 브라우저: `/tmp/deltaweave-share-task1-legacy-browser/`
- 독립 콘솔 캡처: `/tmp/deltaweave-console-smoke-{login,authenticated,mobile}.png`
- 독립 접근성: `/tmp/deltaweave-task1-a11y-*.json`
- 실제 N0 시험: `/tmp/deltaweave-n0-probe-ct7r4nxj/`
- 이전 시험용 기존 데이터: `/tmp/deltaweave-migration-fixture-4e59eewe/`

전용 테스트 identity/관리자 값은 사설 디렉터리에만 두며 이 문서나 캡처에 넣지 않는다.
원래 사용자 콘솔과 파일은 별도로 유지한다.

아직 필요한 항목은 v3 키·권한·격리·실시간 폐기 공격 시험, 실제 RW/RO 동기화와
충돌·중단 복구, 실제 브라우저 복사→붙여넣기→폴더 선택→파일 전송, 기존 데이터의
공유 키 전환, 새 흐름의 접근성과 비밀 노출 검사, 최종 전체 품질 검사 및 Windows
실행이다. [설계의 완료 증거 행렬](superpowers/specs/2026-09-06-folder-share-keys-design.md)을
기준으로 최종 결과를 대조한다. 원격 push나 main 통합은 아직 수행하지 않았다.
