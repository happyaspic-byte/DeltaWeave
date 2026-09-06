# 공유 키 작업의 기준 상태

2026-09-06 조사 기록이다. 이 문서는 변경 전 증거이며 공유 키 기능의 완료 보고가 아니다.

## 저장소와 실행 콘솔

- 작업 시작 커밋: `22664cf559aa08063e0e7756e5babc12ed67b107`.
- 로컬·원격 main과 새 `share-key-sync-update` worktree가 같은 커밋이었다.
- 새 worktree의 시작 상태는 깨끗했다. DeltaWeave에 적용되는 별도 `AGENTS.md`는 없었다.
- `preserve/main-wip-20260906`은 `329ab2e79dd91963e142711426ac074a001557af`로 실제 존재했고, `/home/ubuntu/project/DeltaWeave-main-wip-20260906`에 보존되어 있었다.
- main과 보존 브랜치의 공통 조상은 `75ffab74ee3de4dd1d071c13ab04ae26fb4e6e5b`다. 직접 비교는 121개 파일에 걸친 서로 다른 구현이었다.
- 8390번에서 실행 중인 콘솔은 보존된 React/Vite 구현이었다. main의 `crates/deltaweave-web/ui/` 단일 폴더 UI와 다르다.
- 실제 브라우저에서 로그인 화면과 인증 후 개요·폴더·장치·상세 화면을 확인했다. 폴더/장치/작업 설정은 변경하지 않았고 프로세스를 재시작하지 않았다.

보존할 UI는 Manrope Variable, Noto Sans KR Variable, IBM Plex Mono와 숲색·돌색
토큰, 공통 버튼·대화상자·폴더 선택기, 상단 탐색, 실제 전송량·활동·설정이다.
설계 강도는 기존 브랜드 보존(4), 낮은 모션(2), 업무 정보 밀도 유지(5)로 읽었다.
적용한 `design-taste-frontend`는 관리 화면을 주 용도에서 제외하므로 랜딩 페이지용
히어로·사진·애니메이션 규칙을 업무 화면에 강제하지 않는다.

개선본의 관리 계층과 웹 세션/CSRF/Host/Origin 보호는 선별 통합할 가치가 있다.
다만 엔진 파일 전체를 가져오면 main에서 통합된 사설 디렉터리 권한, symlink,
스캔 재확인, 인과관계, 오류 비공개화, 비동기 writer 종료 보장이 소실될 수 있다.
따라서 observer, inventory, pause/resume, 자원 제한 같은 필요한 기능만 main 위에
추가한다. 기존 CLI, `WebApp`, 단일 폴더 바이너리와 회귀 테스트도 보존한다.

## 변경 전 로컬 검사

검사는 코드 변경 영향을 받지 않는 깨끗한
`/home/ubuntu/project/DeltaWeave-integration-20260905` 체크아웃에서 실행했다.
그 HEAD가 위 기준 커밋과 같은지 검사 전 확인했다.

| 검사 | 결과 |
| --- | --- |
| `cargo fmt --all -- --check` | 통과 |
| `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` | 통과 |
| `cargo test --locked --workspace --all-targets --all-features` | 171개 통과, 실패 0 |
| 기존 웹 UI Node 테스트 | 8개 통과 |
| CLI `self-test` | 통과 |
| 실제 Chromium `crates/deltaweave-web/tests/browser.py` | 두 기기 파일 전송, 해시·인증·오류 복구·연결 단절·키보드·반응형 검사 통과 |

브라우저 검사는 각 기기의 파일 3개, 송신 59바이트·수신 40바이트, 동일한 검증된
Merkle root를 확인했다. 화면 폭 360/768/1440에서 캡처 6개를 남겼다. 이 검사는
기존 수동 연결 동작의 기준이며 새로운 공유 키 UX의 증거는 아니다.
로컬 로그와 비밀이 없는 캡처는 `/tmp/deltaweave-share-baseline/`에 보관했다.
개인 로그인 토큰, 장치 비밀, 사용자 파일 내용은 저장소에 복사하지 않았다.

## 변경 전 원격 CI

같은 기준 커밋의 [CI 실행 34015088967](https://github.com/happyaspic-byte/DeltaWeave/actions/runs/34015088967)에서 Linux 품질 단계는 모두 통과했다.
Windows의 Cargo 테스트가 실패했고 이후 Windows self-test는 실행되지 않았다.
[Security](https://github.com/happyaspic-byte/DeltaWeave/actions/runs/34015089003)와
[Container](https://github.com/happyaspic-byte/DeltaWeave/actions/runs/34015089011)는 통과했다.
[Release 실행](https://github.com/happyaspic-byte/DeltaWeave/actions/runs/34015444809)은
CI 실패에 따라 건너뛰었다.

Windows 실패는 `crates/deltaweave-cli/tests/output.rs`의
`text_identity_keeps_full_endpoint_and_redirection_has_no_ansi`였다. 실제 identity
경로가 텍스트 출력에 그대로 포함되기를 기대하지만 `output::escape`가 모든
backslash를 두 번 출력하여 `C:\Users`가 `C:\\Users`로 렌더링된다. Linux의 `/`
경로에는 나타나지 않는 기존 결함이다. Windows 형태의 입력으로 재현했고,
일반 backslash를 유지하면서 ESC/C0/C1/bidi 제어문자 방어는 그대로 두는 수정과
회귀 검사를 선행 통합 범위에 포함했다. 수정 후의 Windows 실행은 별도 증거가 필요하다.

## 네트워크·권한 조사

기존 v1/v2의 `PeerPolicy`는 정적인 연결 단위 허용 목록이며 모든 요청에 대한
변경 가능한 폴더 권한이 아니다. v1은 받은 파일을 수신자 로컬 이벤트로 채택한다.
v2 `SyncRecord`와 version vector는 서명되지 않았고 `logical_hash`는 내용 해시다.
따라서 이를 공유 권한이나 작성자 서명으로 해석할 수 없다. 기존 `SyncEngine`도
무조건 양방향 병합하므로 RO는 별도의 적용 경로가 필요하다.

선택한 구조의 신뢰 경계와 필수 공격 검사는
[공유 키 설계](superpowers/specs/2026-09-06-folder-share-keys-design.md)에 있다.
iroh의 기존 인증·서명·검색 기능을 확인한 주요 자료는
[iroh transport 문서](https://docs.rs/iroh/1.1.0/iroh/),
[SecretKey](https://docs.rs/iroh/1.1.0/iroh/struct.SecretKey.html),
[EndpointAddr](https://docs.rs/iroh/1.1.0/iroh/struct.EndpointAddr.html)이다.
실제 사용 중인 Cargo 소스의 Ed25519 sign/verify와 N0 preset도 함께 확인했다.
외부 네트워크 자동 검색·릴레이의 실제 실행 결과는 아직 이 기준 검사에 포함되지 않았다.
