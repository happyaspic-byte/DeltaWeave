# Windows Server 실제 검증 — 2026-09-06

## 대상과 범위

사용자가 지정한 `172.30.1.15`의 Windows Server 2022 Standard Evaluation
(10.0.20348, x64, NTFS)에 Administrator로 접속해 실행했다.
WinRM NTLM 메시지 암호화를 사용했으며 SSH/Rust/Node/MSVC를 서버에 설치하지 않았다.
방화벽 설정과 기존 업무 파일을 변경하지 않았다.

공유 키 작업의 커밋 `9cb38cb67e2211b628b48be9f056600bf5768a67`에서 별도 worktree와
`test/windows-server-20260906` 브랜치를 만들었다. 원래 작업자들의 worktree는 보존했다.
이 검증은 해당 시점의 코드에 대한 것으로, 이후 다른 작업자가 변경한 공유 키 브랜치의
최신 상태까지 검증한 것으로 해석하면 안 된다.

실제 테스트 파일과 실행 파일은 `C:\DeltaWeave-Test-20260906`에 두었다.
이 디렉터리 ACL은 Administrators와 SYSTEM으로 제한했다.
프로그램의 설계에 따라 사용자 공용 경로 등록부는
`%USERPROFILE%\.deltaweave\root-admission`에 생성된다.

## 발견한 문제와 수정

1. Windows의 canonical 경로(`\\?\C:\...`)를 구성하는 도중 드라이브 접두사만
   `symlink_metadata`로 조회하여 `Incorrect function (OS 1)`이 발생했다.
   저장소 보존 경로 검증, 네트워크 경로 등록, 웹의 경로 해석에서 접두사 이후
   루트 구분자가 붙을 때까지 조회를 미루도록 수정했다. 관리 설정 경로 해석도
   동일한 규칙을 적용했다. 링크·경로 중첩 검증은 유지했다.
2. 보존한 파일을 읽기 전용 핸들로 `sync_all()`하여 Windows에서 `Access denied (OS 5)`가
   발생했다. 일반 파일은 쓰기 가능한 비추적 핸들로 flush한다. 원래부터 읽기 전용인
   파일은 속성을 바꾸지 않는다. 다른 위치의 하드링크 속성까지 바꾸는 일을 피한다.
   복구 재개도 보존 파일의 flush가 성공해야 다음 단계로 넘어가도록 수정했다.
3. Windows 읽기 전용 속성을 변경할 핸들에 `FILE_WRITE_ATTRIBUTES` 권한이 없었다.
   실제 속성 변경이 필요할 때만 해당 권한으로 파일을 다시 열고 링크 검증을 반복한다.
   속성이 이미 맞으면 기존처럼 읽기 핸들에서 반환한다.

실패를 먼저 확인한 증거:

- 수정 전 최신 Windows 실행 파일의 `self-test`: 종료 코드 1.
- 보존 경로 테스트: OS 1, 접두사만 수정했을 때 파일 flush/속성 변경의 OS 5 재현.
- flush 복구 회귀 테스트: 잠금이 유지되는 동안에도 복구가 진행되어 실패.
- 관리 폴더 등록 테스트: `manager.rs`의 최초 폴더 추가에서 OS 1, 0 passed / 1 failed.
- 실제 소스에서 추출한 세 경로 함수의 Windows 실행: 수정 전 1 passed / 2 failed,
  수정 후 3 passed / 0 failed. 별도로 각 해당 crate에 회귀 테스트를 추가했다.

## 빌드

웹 의존성 설치와 production build 후 `DELTAWEAVE_REQUIRE_WEB_ASSETS=1`로
React 웹 자산을 실행 파일에 포함했다. Rust 1.91.0, Windows GNU x64 대상이다.
Windows에서 추가 MinGW 런타임 DLL 없이 실행됐다.

제품 코드 기준 diff SHA256:
`aa0e9d3d78ccf83f6e8634c2e43c5ffeba4b52b40f50d489bbf6a9fcd12b324f`

| 산출물 | SHA256 |
|---|---|
| Windows ZIP `deltaweave-fixed-v2-windows-x64.zip` | `265fd2a392441a7bd52496a6ac3eb6588a93ab718d949fac7d0c900c8b542607` |
| Windows `deltaweave.exe` | `16c9c406b6cb54759a1695fbeb1530554600c49b64f19074b9fa1cf55ec6c79b` |
| Linux 실행 파일 | `8aec8815965107709a0556de40bc360f2e460b71cb604bb3c5fcaec60abd0926` |

## 실제 확인 결과

공식 v0.4.0 prerelease Windows MSVC ZIP은 체크섬을 확인하고 기준 비교용으로 실행했다.
공식 버전의 자체 검사는 통과했다. 위 수정본은 최신 작업 코드를 GNU로 빌드한 별도 산출물이다.

수정본 `deltaweave.exe self-test`는 Windows에서 종료 코드 0, 오류 출력 없이 통과했다.
4 MiB 최초 전송, 수정 후 257,800바이트 전송, 16 extent 재사용,
양방향 전송·충돌 사본 보존·삭제 전파·재시작 후 0 action을 확인했다.

전송 바이트 수는 CAS로 주고받은 파일 콘텐츠 payload 카운터다. 프로토콜과 암호화
오버헤드를 포함한 전체 네트워크 트래픽 측정치는 아니다.

Windows 서버에서 실행 중인 웹 콘솔을 실제 브라우저로 조작하여 다음을 확인했다.

| 실제 사용자 흐름 | 결과 |
|---|---|
| 로그인, 폴더 추가, 설정 저장 | 성공; 한글/공백 경로와 기존 identity 파일 사용 |
| Linux ↔ Windows 최초 동기화 | 파일 4개, Windows 수신 33,554,537B / 송신 63B; 양쪽 SHA256 전부 일치 |
| 32MiB 파일 중간 4KiB 수정 | 콘텐츠 payload 152,646B만 재수신; 양쪽 SHA256 일치 |
| 두 장치에서 같은 파일을 동시에 수정 | 원본과 `공동 작업.conflict-6e7cb8995638.txt`로 두 내용 보존; 5개 파일의 해시 일치 |
| Windows에서 파일 삭제 | Linux에도 삭제 전파; 남은 4개 파일 해시 일치 |
| Windows 웹 프로세스 종료·재시작 | 폴더/설정/identity/일시정지 상태 유지 |
| 재시작 후 동기화 재개 | local_actions=0, remote_actions=0; 파일 해시와 과거 충돌 기록 유지 |
| 폴더 상세의 보존된 충돌 사본 | 재시작 후에도 실제 파일 경로 표시 |
| 로그아웃/다시 로그인 | 로그아웃 후 state API 401, 재로그인 성공 |
| 데스크톱/모바일 표시 | 실제 Windows 서버에 접속; 모바일 390px에서 가로 넘침 없음 |
| Windows 자체 Edge | 설치된 Edge 152를 실제 실행해 로그인 화면 렌더링 확인 |
| 브라우저 실행 오류 | 최종 browser errors 목록 비어 있음 |
| 웹 자동 테스트 | Vitest 21 passed / 0 failed |

전체 조작은 Linux Chromium에서 실제 Windows HTTP 서버에 접속해 실행했고,
Windows의 Edge는 로그인 화면 렌더링까지만 별도 확인했다. 응답이나 파일을 모킹하지 않았다.

파일 삭제가 진행 중인 주기와 겹쳤을 때 안전 검사에서
`local state changed before applying reconciliation; retry with a fresh snapshot`이
한 차례 기록됐으며 다음 자동 주기에서 정상 수렴했다. 최종 폴더 `last_error`는 null이다.
과거 충돌은 활동 기록과 폴더 상세에서 확인한다. 개요의 conflicts 합계는 최신 완료
주기에 발생한 충돌 수이므로 유휴 주기 이후 0이 될 수 있다.

Linux 관련 5개 crate(store/net/sync/control/web)의 최종 회귀 검사는
14개 실행 단위에서 174 passed / 0 failed였다. 예제·빈 테스트 실행 단위의 0개는
테스트 수에 합산하지 않았다. `cargo fmt --all -- --check`, `git diff --check`도 통과했다.

Windows 서버의 최종 결과는 **20개 실행 파일, 249 passed / 0 failed / 0 ignored**였다.
모든 실행 파일의 종료 코드가 0이고 타임아웃도 없었다. 하위 프로세스가 출력한
중첩 요약은 중복 합산하지 않고 각 실행 파일의 최종 요약만 집계했다.
CLI 출력 4개와 강제 종료·재시작·재현 묶음 검사 3개도 실제 수정본 exe를 실행해 통과했다.

최종 Clippy `--all-targets --all-features -- -D warnings`도 종료 코드 0으로 통과했다.

| 검증 대상 | 통과 | 실패 |
|---|---:|---:|
| Windows 전체 실행 검사(20개 실행 파일) | 249 | 0 |
| Linux 관련 5개 crate 회귀(14개 실행 단위) | 174 | 0 |
| React 웹 Vitest | 21 | 0 |

Windows 원본 로그는 `windows-final-suite-logs`, 집계는 `windows-final-test-summary.json`에 있다.
Linux 집계는 `linux-final-regression-summary.json`, 최종 정적 검사 증거는
`final-check-provenance.json`에 있다.

열린 redb 파일을 별도 핸들로 읽던 두 테스트는 Windows OS33 잠금 오류가 발생했다.
해당 테스트를 제외하지 않고, 전체 catalog의 논리 상태와 재시작 후 영속 상태,
닫힌 DB의 모든 테이블 키·값을 비교하도록 바꿨다. 일반 파일의 전체 바이트 비교는 유지했다.

최종 테스트 소스 diff SHA256:
`b92d075dfd6be4424edd7e6610ca72bed09ffb3f24ada411d196d62ae734beeb`
추가된 두 파일의 변경은 테스트 코드뿐이므로 위 v2 제품 실행 파일의 구현과 동일하다.
런타임 검사 산출물을 만든 뒤 Clippy의 `items_after_test_module` 지적을 해결하기 위해
`operations.rs`의 `path_tests` 모듈을 내용 변경 없이 파일 끝으로 옮겼다.
이 배치 정리 후 최종 소스 diff SHA256은
`38e7837afc2379d4696a6ddd421179f55cd1c905894babb85a4ad3c4c685f1b3`이며,
런타임 산출물의 기존 provenance와 구분해 최종 Clippy로 확인했다.

Cargo로 만든 Windows 실행 파일 18개 외에 CLI integration 2개를 추가로 실행했다.
이 두 원본 테스트의 `env!(CARGO_BIN_EXE_deltaweave)`는 교차 빌드 시 Linux 절대경로가
들어가므로, 원본 파일을 수정하지 않고 동일 Rust 1.91 및 Cargo가 선택한 의존성으로
직접 `rustc --test`를 실행했다. 컴파일 프로세스의 해당 환경 값만 실제 Windows 실행 파일
경로로 지정했다. 이는 Windows MSVC/native Cargo 빌드의 성공을 주장하는 근거는 아니다.
정확한 명령·원본 SHA·의존성 SHA는 `windows-cli-relocated-tests-provenance.json`에 있다.


## 확인 범위의 한계

이 실행 결과는 Windows Server 2022의 관리자 계정과 NTFS, 동일 LAN 환경에 대한 것이다.
최신 코드의 MSVC 빌드/Windows CI, 일반 사용자 계정, Windows 10/11,
NAS·SMB 공유 폴더, 인터넷 NAT/릴레이, 장시간 soak는 이번 확인 범위에 포함하지 않았다.

Windows의 기존 읽기 전용 원본과 디렉터리 변경에는 명시적인 flush 보장이 없다.
파일 잠금과 프로세스 재시작 시험의 성공은 정전·커널 장애 시 내구성을 입증하지 않는다.

공유 키의 백엔드와 별개로 이 빌드의 웹 화면에는 아직 RO/RW 키만 붙여 넣어
자동 연결하는 흐름이 없다. 실제 웹 검증은 공개 장치 ID와 주소를 입력하는 기존 연결 방식이다.

## 증거 보관

Linux 증거 디렉터리:
`/home/ubuntu/project/DeltaWeave-windows-evidence/2026-09-06`

Windows 로그 디렉터리:
`C:\DeltaWeave-Test-20260906\logs`

관리자 비밀번호와 웹 접근 키 값은 이 보고서 및 Git에 포함하지 않는다.

## 확인용 서버 사용

웹 콘솔: http://172.30.1.15:8391

접근 키 파일: `C:\DeltaWeave-Test-20260906\web-private\admin-token`

실행 파일: `C:\DeltaWeave-Test-20260906\current\deltaweave.exe`

웹은 확인용으로 실행 상태를 유지하고 테스트 동기화 폴더는 일시정지했다.
임시 Linux 수신 프로세스는 종료했다. 따라서 이 테스트 연결을 다시 재개하려면
상대 테스트 프로세스를 별도로 실행하거나 실제 사용할 장치로 연결 설정을 바꿔야 한다.
서비스 등록이나 부팅 시 자동 실행은 설정하지 않았다. 서버를 재부팅한 뒤에는 다음 명령으로
웹을 실행할 수 있다(이미 실행 중이면 중복 실행하지 않는다).

```powershell
& 'C:\DeltaWeave-Test-20260906\current\deltaweave.exe' web --bind 172.30.1.15:8391 --data-dir 'C:\DeltaWeave-Test-20260906\web-private'
```

## 반영 상태

수정과 이 보고서는 `test/windows-server-20260906` 브랜치에 보존한다.
main 병합·원격 push·기존 worktree 삭제는 이번 Windows 검증 작업에서 수행하지 않았다.
공유 키 기능 작업자의 원래 브랜치와 작업 디렉터리는 수정하지 않았다.

테스트용 브라우저와 Linux 수신 프로세스는 종료했다. Windows의 확인용 웹과 테스트 파일,
로그는 남겼다. 임시 파일 전달 서버를 종료하고 로컬 임시 접속 비밀번호 파일 및
웹 접근 키 사본을 제거했다. Windows 웹의 정상 로그인에 필요한 원본 키 파일은 유지했다.
