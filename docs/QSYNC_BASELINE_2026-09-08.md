# qSync 기준선 검증 — 2026-09-08

이 문서는 `test/qsync-verification-20260908`에서 수행한 기준선 검증의 보존 기록이다. 검증 worktree의 기준 HEAD는 `baed5c0164a2e10fdbe6a1e5a31f7e897c675ced`이며, 같은 시각 `origin/main`도 이 SHA를 가리켰다. 제품 소스, 운영 서비스, 사용자 데이터, identity, index, CAS에는 변경을 가하지 않았다.

## 보존 기준

2026-09-08T18:25:18Z에 등록된 worktree 32개를 확인했다. 세 개의 prunable 등록(`/home/ubuntu/project/DeltaWeave-integration-20260905`, `...-main-wip-20260906`, `...-windows-validation-20260906`)은 실제 경로가 없어도 prune하거나 삭제하지 않았다. `main`의 기존 dirty 상태는 untracked `.agents/`, `.claude/`, `goal-prompts/`, `skills-lock.json` 목록과 SHA-256으로 보존했다. nested `.claude/worktrees/*`는 재귀 중복 해시를 피하기 위해 경로만 기록했다. 검증 worktree는 검사 전후 clean이었다.

상세 등록·HEAD·branch·dirty 목록·해시는 [worktree-baseline.txt](/home/ubuntu/project/DeltaWeave-qsync-evidence/2026-09-08/worktree-baseline.txt), 원본 목록은 [worktrees-raw.txt](/home/ubuntu/project/DeltaWeave-qsync-evidence/2026-09-08/worktrees-raw.txt)에 있다.

## 원격 기준선과 실패 분류

GitHub의 현재 HEAD에 대한 신규 확인만 사용했다. 이전 `9d294ea` CI 수치는 참고값으로 재집계하지 않았다.

- CI run `34251266098`은 실패했다. Linux quality gate의 `authenticated_owner_rollback_divergence_and_missing_tombstones_are_rejected`가 `crates/deltaweave-sync/tests/shares.rs:1001`에서 `Failed to bind sockets` / `Address already in use (os error 98)`로 중단됐다. 확정된 원인은 endpoint bind 실패이며, 포트 재사용·동시성의 세부 원인은 재현 전 가설로 남긴다.
- Windows run `34251266098`의 Windows tests job은 `real_pair_sync_idle_pause_and_reopen`에서 `crates/deltaweave-control/tests/manager.rs:209`의 10초 watcher 대기가 `Elapsed(())`로 끝나 실패했다. 확정된 원인은 watcher 관찰 timeout이며, Windows 파일 이벤트의 세부 원인은 별도 native 재현이 필요하다.
- Container run `34251265862`의 linux/amd64와 linux/arm64 self-test가 `self-test receiver failed to start` / `Permission denied (os error 13)`로 실패했다. 이미지 build 자체보다 receiver 실행 권한 단계가 실패한 것이다.
- Security run `34251266012`와 RustSec dependency audit은 성공했다. Release workflow는 해당 HEAD에서 skip됐다. code scanning은 분석 없음(404), Dependabot alerts는 저장소에서 비활성화(403), secret scanning open 결과는 0건으로 확인됐다.

run·job URL, HEAD 대조, 원문 오류 발췌는 [github-status-20260908.txt](/home/ubuntu/project/DeltaWeave-qsync-evidence/2026-09-08/github-status-20260908.txt)와 [github-failure-snippets-20260908.txt](/home/ubuntu/project/DeltaWeave-qsync-evidence/2026-09-08/github-failure-snippets-20260908.txt)에 있다. 위 실패는 이 기준선에서 발견한 기존 CI 상태이며, 이번 검증에서 소스 수정으로 우회하지 않았다.

## 이 worktree에서 실행한 검사

모든 명령은 별도 로그로 보존했다. 각 명령의 정확한 문자열·exit code·완료 시각은 [verification-ledger.json](/home/ubuntu/project/DeltaWeave-qsync-evidence/2026-09-08/verification-ledger.json)에 있다.

| 명령 | 결과 |
| --- | --- |
| `npm --prefix web ci` | exit 0; 166 packages 추가, 167개 audit, 취약점 0건. deprecated `whatwg-encoding` 및 esbuild install-script 승인 경고만 있었다. [로그](/home/ubuntu/project/DeltaWeave-qsync-evidence/2026-09-08/npm-ci.log) |
| `npm --prefix web test` | exit 0; 4 files, 21 tests 통과. [로그](/home/ubuntu/project/DeltaWeave-qsync-evidence/2026-09-08/npm-test.log) |
| `npm --prefix web run build` | exit 0; TypeScript와 Vite build 성공, 4,573 modules transformed. [로그](/home/ubuntu/project/DeltaWeave-qsync-evidence/2026-09-08/npm-build.log) |
| `DELTAWEAVE_REQUIRE_WEB_ASSETS=1 CARGO_BUILD_JOBS=4 RUST_TEST_THREADS=4 CARGO_TARGET_DIR=/home/ubuntu/.herdr/worktrees/DeltaWeave/share-key-sync-update/target cargo test --locked -p deltaweave-control -p deltaweave-web --all-targets --all-features` | exit 0; control 16개(단위 3, manager 13), web 27개(lib 12, API 15; main 0), 총 43개 통과. [로그](/home/ubuntu/project/DeltaWeave-qsync-evidence/2026-09-08/cargo-focused.log) |

## 접근 환경과 증거 경계

검사 호스트는 `roobicom-server-01` Linux 한 대다. GNU cross compiler와 `x86_64-pc-windows-gnu` target은 설치되어 있으나 `cargo-xwin`, PowerShell, Chrome/Chromium/Firefox는 로컬에 없다. 교차 빌드는 native Windows 실행의 증거가 아니다. GitHub Windows runner와 승인된 격리 서버 `172.30.1.15` 경로는 후속 검증 대상으로 확인했지만 이번에 원격 접속·credential 사용·서비스 변경은 하지 않았다. 브라우저·N0/relay도 실행하지 않았다. 로컬 테스트가 내부적으로 여러 논리 peer를 만들더라도 세 물리 기기 검증으로 세지 않는다.

## F 단계의 후속 명령과 완료 조건

다음 명령은 이 기준선에서 실행하지 않았고, F 담당이 실패 원인 수정 후 새 로그로 실행해야 한다.

```text
cargo fmt --all -- --check
DELTAWEAVE_REQUIRE_WEB_ASSETS=1 cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
DELTAWEAVE_REQUIRE_WEB_ASSETS=1 cargo test --locked --workspace --all-targets --all-features --no-fail-fast
DELTAWEAVE_REQUIRE_WEB_ASSETS=1 RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --all-features --no-deps
DELTAWEAVE_REQUIRE_WEB_ASSETS=1 cargo build --locked --release --workspace --all-features
DELTAWEAVE_REQUIRE_WEB_ASSETS=1 cargo run --locked --all-features -p deltaweave -- self-test
npm --prefix web test -- --run
git diff --check
```

F 완료에는 위 회귀 검사와 보안 자산 검사를 포함해 owner/RW/RO의 **분리된 세 기기** 실제 흐름, 검증된 공급자 2개 이상과 manifest 포함·payload hash 거부, provider failure/resume, revoke·lease 만료·partition 경계, 1440px/390px 브라우저 접근성, native Windows exe 재시작 복구, 실제 N0/relay 전송, 기존 수동 연결 보존을 각각 원문 로그로 증명해야 한다. local three-process/three-identity 실험은 준비 단계 증거일 뿐 물리 분리 증거가 아니다. 권한 철회는 분할 중 즉시 중지를 주장하지 않고, 새 grant 거부와 peer stream 취소 확인 또는 제한된 lease 만료·실제 전송 종료까지를 완료 경계로 기록한다. owner offline 독립 mesh는 1차 범위에 포함하지 않는다.

현재 문서는 기준선과 집중 검사의 기록만 완료한다. 전체 F 증거, Windows native, 외부 네트워크, CI green 및 push 후 `origin/main` 일치는 아직 완료 조건을 충족하지 않는다.
