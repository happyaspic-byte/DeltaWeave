# 최신 작업 통합 — 2026-09-08

## 대상과 보존

사용자 요청에 따라 최신 완료 작업을 검증한 뒤 GitHub `main`에 반영한다.
기준 `main`은 `22664cf559aa08063e0e7756e5babc12ed67b107`이다.
별도 `integration/latest-20260908` 브랜치와 worktree에서 통합했다.

- `test/windows-server-20260906`의 `91d0c95`: 공유 키 `9cb38cb`까지의 전체 이력과 Windows 수정.
- `origin/feat/swarm-v3-foundation`의 `4cf4cf3`: 다중 공급자 CAS 전송, 스케줄러, 프로토콜 경계 검사.
- `preserve/main-wip-20260906`의 `329ab2e`에서 누락된 완료 항목만 선별 복원: doctor, CLI 연결·용량 제한, 주소 검사, Windows 실행 스크립트, 웹 자산 검증, 당시 검증 보고서.

기존 6개 worktree의 소스는 각각 이전 통합 스냅샷과 파일 내용·모드가 일치했다.
그 스냅샷은 모두 기존 main에 포함되어 있어 중복 적용하지 않았다.
기존 worktree, 브랜치, stash, 미커밋 파일은 삭제하거나 초기화하지 않았다.
등록만 남은 과거 worktree도 정리하지 않았다.

## 충돌 해결과 보완

Swarm 브랜치는 더 오래된 공통 조상에서 갈라졌으므로 최신 파일을 통째로
대체하지 않고 3-way 병합으로 기능을 조합했다.

- 최신 causal journal, 변경 직전 재검사, 충돌 사본, 삭제·재시작 복구와 Windows 경로·flush 처리를 유지했다.
- 공유 키 `deltaweave/share/3`과 기존 endpoint allowlist 기반 Swarm `deltaweave/sync/3`을 분리했다. 관리형 공유 세션에는 legacy Swarm CAS 경로를 연결하지 않는다.
- Swarm에도 연결 한도, pause, root lease, 종료 시 작업 대기, CAS·목적지 용량 예산을 적용했다. 로컬 저장 실패를 일반 전송 fallback으로 우회하지 않는다.
- primary peer는 Swarm 보조 공급자 목록에서 제외한다. 연결 한도가 1이어도 다음 manifest 요청과 fallback이 primary의 연결 슬롯을 사용할 수 있다.
- 원격 snapshot과 record adoption에서 로컬 replica counter 위조를 거부한다. 로컬 직접 쓰기와 신뢰된 RO 체크포인트 복구는 필요한 기존 동작을 유지한다.
- `swarm-fill`은 입력을 검증하고 state·identity를 예약한 뒤 키와 CAS를 만든다. identity는 정확한 파일 경로만 예약하며 부모 디렉터리 전체나 형제 공유 폴더를 차단하지 않는다.
- Windows 릴리스 ZIP에 `start-web.cmd`와 웹 사용 설명서를 포함하도록 패키징 경로를 연결했다.

## 검증

제품 코드 커밋은 `b531d339119d86bf81487bc7ad366430ade942d5`다.
후속 `9d294ea63db6506c1934a20309330a481201d5ca`는 Windows CI에서 확인된
테스트 경합만 수정하며 제품 실행 코드는 동일하다.
중첩 실행되는 자식 테스트 요약을 다시 합산하지 않고, Cargo 실행 단위마다
마지막 결과만 집계했다. 병합 전 기준 검사는 Rust 277개, 웹 21개가 통과했다.

| 검증 | 결과 |
|---|---|
| Rust workspace, all targets/features | 25개 실행 단위, 332 passed / 0 failed / 0 ignored |
| React Vitest | 21 passed / 0 failed |
| React production build | 성공, 자산 140개 생성 |
| rustfmt, Clippy `-D warnings`, rustdoc `-D warnings` | 모두 성공 |
| Linux workspace release build | 성공 |
| Windows GNU workspace/all-targets/all-features check | 성공 |
| Windows GNU CLI release build | 성공, 별도 GCC/pthread 런타임 DLL import 없음 |
| Linux debug/release CLI self-test | 양방향 전송·충돌 사본·삭제·재시작 검증 성공 |
| 실제 Chromium 웹 흐름 | 정상/잘못된 키 로그인, 로그아웃, 설정 저장·reload 성공 |
| 데스크톱 1440px / 모바일 390px | 로그인·개요·설정 6개 조합에서 가로 넘침·console/page error 없음 |
| 문서 미디어·README 로컬 링크·loopback 셸 회귀 | 모두 성공 |
| RustSec dependency audit | 439개 의존성 검사 성공; 기존 `RUSTSEC-2024-0436` 예외 유지 |
| Windows CI 경합 수정 후 로컬 control 검사 | 16 passed / 0 failed, Clippy 성공 |

GitHub [Security 실행](https://github.com/happyaspic-byte/DeltaWeave/actions/runs/34248603482)은
제품 코드 커밋 `b531d33`에서 성공했다. 이후 변경은 테스트와 이 검증 문서뿐이다.

[최종 GitHub CI](https://github.com/happyaspic-byte/DeltaWeave/actions/runs/34250085482)는
`9d294ea63db6506c1934a20309330a481201d5ca`에서 Linux·Windows 모두 성공했다.
Linux는 웹 검사·빌드, formatting, Clippy, 전체 Rust 테스트, CLI self-test,
rustdoc, 문서 미디어, 릴리스 빌드를 통과했다. Linux 전체 Rust 검사는
25개 실행 단위에서 **332 passed / 0 failed / 0 ignored**였다. Windows 네이티브 실행은
25개 Cargo 실행 단위에서 **304 passed / 0 failed / 0 ignored**였으며,
웹 검사·빌드와 CLI self-test도 성공했다. Windows 자체 검사는 양방향 전송,
삭제, 충돌 사본 1개, 재시작 후 action 0과 인덱스 복구를 확인했다.
운영체제별 조건부 테스트 때문에 Linux와 Windows의 테스트 수가 다르다.
이후 커밋은 이 보고서만 추가하므로 최종 main의 제품·테스트 코드는 CI 검증본과 동일하다.

최종 로컬 Rust 검사는 `RUST_TEST_THREADS=4`로 실행했다.
`cargo test --locked --workspace --all-targets --all-features --no-fail-fast`를 사용했으며,
Windows 교차 검사와 빌드는 `x86_64-pc-windows-gnu` 대상이다.
웹 자산은 `DELTAWEAVE_REQUIRE_WEB_ASSETS=1`로 포함하고, Linux/Windows 실행 파일 안에
140개 자산의 원본 바이트가 모두 존재함을 확인했다.

릴리스 자체 검사는 첫 콘텐츠 4,194,304B를 전송하고 변경 후 257,800B만 전송했다.
충돌 사본 1개를 보존하고 삭제 전파를 확인했으며, 재시작 후 action은 0이었다.
이 수치는 콘텐츠 payload이며 전체 암호화·프로토콜 트래픽을 뜻하지 않는다.

| 로컬 빌드 산출물 | SHA-256 |
|---|---|
| Linux `deltaweave` | `c4a7198c9bf55cf29dda85d5a167449d9d949b8d998d195156b35692bba906a2` |
| Windows GNU `deltaweave.exe` | `af3ecb25d7a9da9e305ebaab32974ce7414eb4d507a9a35ee2169142cf38f1fb` |

빌드·테스트·브라우저 증거는 로컬 `DeltaWeave-integration-evidence/2026-09-08`에 보관한다.
임시 브라우저 서버와 브라우저를 종료했고 해당 테스트의 접근 키·설정 파일은 삭제했다.

## 중간 실패와 해석

- 병합 후 누락된 생성자 필드와 잠금 파일 구성을 컴파일 오류를 통해 찾아 수정했다.
- 관리형 endpoint 회귀의 임시 폴더는 서비스가 전용 하위 폴더를 만들도록 바꾸어 실제 비공개 경로 조건을 충족했다.
- 저장 공간 검사에서 순간적인 여유 공간을 실패 기준으로 사용하면 다른 테스트의 파일 정리로 결과가 바뀌었다. 불가능한 reserve를 사용해 writer의 영속화 직전 검사 자체를 검증한다.
- 종료 대기 검사가 admission write lock을 직접 점유해 검사 대상 요청을 거부하는 경쟁을 만들었다. 등록된 실행 작업을 관찰하도록 수정했다.
- 최초 전체 실행에서 기존 managed root 재획득 검사가 한 번 실패했다. 당시 `is_ok()`가 실제 오류를 숨겼다. 이후 일반 실행 80회와 exec 지연 추적 1회에서는 재현되지 않았으며, 추적 실행의 최대 FD는 207개였다. 원인을 확정하지 않았고, 다시 발생하면 실제 오류를 남기도록 진단을 개선했다. 이 항목을 제품 결함 수정 완료로 해석하지 않는다.
- [첫 Windows GitHub CI](https://github.com/happyaspic-byte/DeltaWeave/actions/runs/34248602376)에서는 watcher의 자동 동기화와 테스트의 수동 동기화 요청이 겹쳐 정상적인 busy 응답으로 검사가 실패했다. 성공을 기대하는 수동 요청 3곳만 해당 busy 응답을 30초 이내에 재시도하도록 수정했다. 다른 오류는 즉시 실패하고, 실제 명령 완료·파일 내용·idle 전송량·pause/resume·자동 watcher·재시작 검증은 모두 유지한다. 제품의 동시 실행 차단 동작은 변경하지 않았다.

## 기능과 검증 범위

이 통합은 완료된 변경을 main에 모으는 작업이다. RO/RW 키 붙여넣기 웹 화면과
관리형 공유의 자동 worker는 아직 완료되지 않았다. Swarm은 CLI에서 명시적으로
지정한 허용된 공급자를 사용하는 기능이며, 공개 DHT나 qBittorrent 호환 기능을 뜻하지 않는다.

2026-09-05와 2026-09-06의 실기기·웹 보고서는 당시 코드의 역사적 증거다.
이번 통합본의 Windows Server 실기기 재검증이나 인터넷 NAT·릴레이 검증으로
확대 해석하지 않는다. 이번 작업은 main 반영·push까지이며 새 릴리스 태그는 만들지 않는다.
