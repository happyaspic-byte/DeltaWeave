# Windows 10 메인 PC ↔ Synology NAS 검증 — 2026-09-05

> 역사 기록: `preserve/main-wip-20260906`에서 복원했다. 아래 결과는 9월 5일의
> 소스 스냅샷과 당시 검증 범위에 한정되며, 9월 8일 통합본이나 swarm V3 기능의
> 검증 결과가 아니다. 로컬 증거 경로는 당시 실행 환경을 가리키며 현재 남아 있지
> 않을 수 있다.

사용자가 제공한 메인 PC 콘솔 출력에서 일회성 동기화, 무변경 재실행,
상시 동기화의 양방향 파일 전파가 성공했다. NAS에서도 메인 PC가 보낸 파일의
실제 내용과 SHA-256을 별도로 확인했다. 메인 PC의 파일시스템을 독립적으로
열람하거나 전체 파일의 SHA-256을 별도 대조한 결과는 아니다.

이번 검증은 [앞선 Windows Server·NAS·Ubuntu 4대 검증](FIELD_TEST_2026-09-05.md)에
사용한 NAS 수신기에 Windows 10 메인 PC를 추가한 후 진행했다.
앞선 네 장비의 검증 결과를 메인 PC에서 모두 재현한 것으로 해석하지 않는다.

| 항목 | 환경·증거 |
| --- | --- |
| 메인 PC | 사용자 제공 콘솔: Windows 10, 빌드 `19045.6456` |
| 전달 패키지 | `DeltaWeave-main-pc-20260905`, `0.4.0 + local field-tested changes` |
| Windows 빌드 | `x86_64-pc-windows-gnu` release |
| 소스 HEAD | `75ffab74ee3de4dd1d071c13ab04ae26fb4e6e5b` 및 당시 로컬 수정 사항 |
| 소스 스냅샷 SHA-256 | `4527deb081a8092a1ad217301db7d232a7275812e61bc331668a74201f6e2cc9` |
| 전달 실행 파일 SHA-256 | `bce7624891c5992599b68c7ed30f2fe26484d87b3b7171917fcfe0f21d358536` |
| 전달 ZIP SHA-256 | `09c54f3982244db750a36718ccb37852fb08d6fe37c6772007ab8572fd26a71d` |
| 메인 PC 공개 endpoint ID | `4810630020917196fb9cd41b1aca73e23b1bdacb375ca119b68a023408acd830` |
| NAS 공개 endpoint ID | `ebae0d2acac157a85047758ede71fc01aa59d5f1e32b8f372d372aff00ae64bf` |
| NAS 등록 기록 | 기존 허용 피어 유지, 허용 피어 총 4개, `doctor: pass`, 수신기 실행 확인 |

전달 전 실행 스크립트의 자체 검사·공백 경로·반복 초기화 시 identity 보존은
Windows Server에서 확인한 기록이다. 메인 PC에서는 사용자가 준비 스크립트의
종료 코드 0을 확인했고, NAS 로그에도 해당 공개 endpoint ID의 접속 수락이 기록됐다.

| 메인 PC에서 수행한 검증 | 결과와 근거 |
| --- | --- |
| 일회성 동기화 | 사용자 출력 `pass`, 종료 코드 0, 세 root 일치. NAS 별도 검사에서 빈 `main-pc-test.txt` 추가와 총 68개 파일 확인 |
| 일회성 무변경 재실행 | 사용자 출력 `pass`, 종료 코드 0, Merkle 조회 1개, 양쪽 작업·payload 전송 0, `conflicts: []` |
| 상시 동기화 시작 | `04-sync-continuous.cmd`의 사용자 출력에 `sync_started`, `local_change_detection: native_watcher`, `remote_poll_seconds: 5`, `watcher_error: null` |
| NAS → 메인 PC | 사용자 출력에서 64바이트 시험 파일 `nas-to-main-pc-20260905.txt` 수신 성공: `local_actions: 1`, `remote_actions: 0`, `pulled_bytes: 64`, `pushed_bytes: 0` |
| 메인 PC → NAS | 사용자의 `main-pc-test.txt` 수정 후 성공: `local_actions: 0`, `remote_actions: 1`, `staged_local_files: 1`, `pushed_bytes: 22`. `local_change` 이벤트 관측 |
| 후속 무변경 주기 | 사용자 출력에서 성공, 양쪽 작업·payload 전송 0, 수정 후 root 유지, `conflicts: []` |

각 성공 결과에서 `desired_root`, `verified_local_root`, `verified_remote_root`가
모두 일치했다. 아래 값은 사용자 제공 CLI 결과의 root이며 개별 파일 SHA-256과 구분한다.

| 시점 | 일치한 root |
| --- | --- |
| 일회성 동기화 및 무변경 재실행 | `d8f8075b418cc684ff07161f0a72d1d5d1a08e8c3f2cef781c3db9116fc12784` |
| NAS 시험 파일 수신 후 | `cd2d46774b2049e6a71e534242c8c35b90d79e7545057e6afad60ecf42e17bbd` |
| 메인 PC 파일 수정 전파 및 후속 무변경 주기 | `9458a4c42d3c061c0cebde57848a835234e4056703ff73ec1cd7bcf984b7bdc9` |

**상시 동기화 후 NAS 파일 내용의 별도 검사도 통과했다.**
2026-09-05 12:56:17 UTC에 실행 중인 NAS 수신기의 테스트 파일 두 개를 읽었다.
`main-pc-test.txt`는 정확히 22바이트였고, 내용은 `main-pc auto sync test`였다.
읽어 온 내용으로 다시 계산한 SHA-256도 NAS의 `sha256sum` 결과와 일치했다.

| NAS에서 별도로 확인한 파일 | SHA-256 |
| --- | --- |
| `main-pc-test.txt` | `c66082138c37a5f171838524b745ae73451c68b53cb824306097d1caff1a0547` |
| `nas-to-main-pc-20260905.txt` | `a219ff2fd962f6a8bdce46394445bef1d2e95dde7116ecf3cee0c74856bc8348` |

전송량은 CLI의 파일 payload 카운터이며 전체 네트워크 사용량이나 처리량 측정값이 아니다.
GUI, 공식 MSVC 패키지, 장시간 연속 운전은 이번 메인 PC 검증에서 시험하지 않았다.

기존 증거는 Git 추적 대상이 아닌 로컬 `target/main-pc-delivery-20260905/`의
`delivery.json`, 패키지 내부 `BUILD-INFO.json`, `nas-main-pc-registration.json`,
`main-pc-first-sync-observation.json`, `main-pc-sync-pass.json`,
`main-pc-no-change-pass.json`, `main-pc-auto-sync-fixture.json`,
`main-pc-auto-sync-pass.json`에 있다.
일부 JSON의 `pending`은 작성 당시 상태다. 상시 동기화 결과는 이후 사용자가 제공한
Windows 콘솔 출력을 근거로 이 문서에 기록했다.
