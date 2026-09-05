# 문서 동기화 검증 기록

> 이 문서는 개별 worktree에서 수행한 당시의 검증 기록입니다. 6개 작업을 합친
> 현재 구현과 재검증 결과는 [통합 보고서](INTEGRATION_2026-09-05.md)를 기준으로 합니다.

확인일: 2026-09-05 (UTC)

기준: `docs-sync-update`, `75ffab74ee3de4dd1d071c13ab04ae26fb4e6e5b`, workspace `0.4.0`

## 범위와 기준 출처

시작 시 작업 트리는 깨끗했고 적용되는 `AGENTS.md`는 없었다. 제품 코드, 의존성,
설정, workflow, 테스트, 이미지 파일은 변경하지 않았다. 배포·게시·운영 데이터 변경도
수행하지 않았다. 문서 구조를 유지하며 다음 독자별 절차와 직접 참조를 점검했다.

| 독자/문서 | 확인할 계약 | 기준 출처 |
| --- | --- | --- |
| 처음 설치하는 사용자: [README](../README.md), [장비 간 안내](TESTING_WINDOWS_SYNOLOGY.md) | 소스 설치, 실행 파일 위치, 릴리스 파일, root/state 준비 | `Cargo.toml`, `rust-toolchain.toml`, `.github/workflows/release.yml`, CLI 도움말 |
| 기여자: [CONTRIBUTING](../CONTRIBUTING.md) | 개발·빌드·테스트·문서 생성, script 환경변수 | `.github/workflows/ci.yml`, `scripts/verify-release.sh`, `scripts/test-p2p-loopback.sh`, `scripts/fault-test.sh`, `scripts/render-doc-visuals.sh` |
| CLI 사용자: [CLI 참조](CLI.md), [인덱스 안내](TESTING_LOCAL_INDEX.md) | 필수 인자, 기본값, 출력, 종료 코드, watcher/scan 의미 | `crates/deltaweave-cli/src/main.rs`, `deltaweave-index`, 실제 CLI 실행 |
| NAS 운영자: [Portainer 실행서](AI_PORTAINER_SETUP.md) | Compose 치환, UID/GID, 데이터 보존, 포트, 조건부 인증 | 두 `deploy/portainer/*.yml`, `Dockerfile`, container workflow |
| 프로토콜/라이브러리 사용자: [PROTOCOL](PROTOCOL.md), [ARCHITECTURE](ARCHITECTURE.md), [THREAT_MODEL](THREAT_MODEL.md) | 두 QUIC ALPN, 메시지/인증/제한, causal apply와 복구 범위 | `deltaweave-core`, `deltaweave-net`, `deltaweave-store`, `deltaweave-sync`, 관련 단위·통합 테스트 |
| 기존 설명 참조: [RECONCILE_ANALYSIS](RECONCILE_ANALYSIS.md), [ROADMAP](../ROADMAP.md), [CHANGELOG](../CHANGELOG.md), [RELEASE_NOTES](RELEASE_NOTES.md) | 구현과 계획, 중복 record 처리, 장애 주입 한계 | reconcile/net/CLI 구현과 release workflow |
| 과거 실행 기록: [USAGE_GALLERY](USAGE_GALLERY.md), [QUALITY_REPORT_V0.3](QUALITY_REPORT_V0.3.md) | 역사적 예시와 현재 실행 증거 구분 | 고정 문자열을 렌더링하는 원본 script, 보고서에 기재된 버전·날짜 |

HTTP API나 별도 제품 설정 파일은 없다. Rust API 페이지는 crate 주석에서 rustdoc으로
생성된다. Markdown site/lint 플랫폼은 없으며 번역 문서 쌍도 없다. 영어 README와
한국어 사용 안내에 반복된 버전·명령·설정·검증 의미를 함께 맞췄다. 원본 렌더러가
보관한 과거 출력은 최신 출력으로 바꾸지 않았으므로 이미지 재생성은 필요하지 않았다.

## 해결한 불일치

| 영향 | 기존 설명의 문제 | 반영한 내용 |
| --- | --- | --- |
| 설치 실패 | `cargo build` 다음 bare `deltaweave` 명령, 설치/PATH 전제 누락 | checkout 기준 locked build/run/install, Rust 1.91.0, 네이티브 도구, 실행 파일 경로 안내 |
| 잘못된 버전 선택 | README·일부 실행서가 v0.3을 현재 버전으로 표기 | workspace/확인한 릴리스 v0.4.0과 과거 v0.2/v0.3 자료 구분 |
| 권한·경로 오해 | allow-list가 push 전용처럼 보임, state 파일/디렉터리 혼동 | 폴더 전체 읽기/쓰기 권한, root/identity 분리, 같은 파일시스템의 trash 이동 조건 |
| 잘못된 자동화 판정 | scan을 읽기 전용으로 표현, 모든 성공을 exit 0만으로 판단 | DB 갱신, issues/collisions 확인, multiline JSON stream과 runtime/parse 오류 코드 |
| 재시작·설정 오류 | Compose 필수값/기본값 혼동, UDP 포트와 5초 완료 보장 | 필수 치환 3개, 설정 적용 시점, 기본 동적 포트, pass 시간·backoff를 포함한 polling 의미 |
| 인터페이스 드리프트 | v1만 설명, v2/pull에도 동일 제한·causal 보장 주장 | 실제 ALPN/메시지/응답 필드, v1 overwrite와 v2 causal 조건, push/pull 제한 차이 |
| 복구 과장 | 단일 atomic apply/자동 journal 복구처럼 해석 가능 | 별도 rename/DB commit, startup replay 부재, 반복 시도와 외부 쓰기 경합 한계 |
| 검증 과장 | 정확한 durable payload 중단·자동 실패 bundle 보존·물리 DSM 성공 주장 | 실제 프로세스 종료, barrier 결함, explicit workspace 보존, CI/QEMU와 하드웨어 구분 |
| 배포 문서 링크 | 아카이브에서 `TESTING_WINDOWS_SYNOLOGY.md`는 `TESTING.md`로 이름 변경됨 | 원격 원문 링크와 아카이브 파일명 안내, package 문서의 상대 링크 별도 확인 |

## 실행 환경과 확인 결과

호스트는 Linux x86_64, kernel `7.0.0-31-generic`이다. 실제 도구 출력:

- rustc `1.91.0 (f8297e351 2025-10-28)`
- cargo `1.91.0 (ea2d97820 2025-10-10)`
- active toolchain `1.91.0-x86_64-unknown-linux-gnu`
- Bash `5.3.9`, Python `3.14.4`, Docker Compose `v5.5.0`

| 실제 명령/검사 | 결과와 범위 |
| --- | --- |
| `./scripts/verify-release.sh` | 9단계 모두 통과, 최종 `DeltaWeave release verification: PASS`; Compose skip 없음. 추가 serial fault 테스트 3개와 독립 seed 424242 시나리오도 통과 |
| `cargo build --locked --workspace` | 통과; debug binary 생성 |
| `cargo fmt --all -- --check` | 통과 |
| `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` | 통과 |
| `cargo test --locked --workspace --all-targets --all-features` | 9개 suite, 103 passed / 0 failed / 0 ignored; shipped CLI fault 통합 테스트 3개 포함 |
| `cargo test --locked --workspace --doc --all-features` | 통과; 현재 7개 library crate에 실행할 doctest 0개이므로 예제 검증 커버리지를 뜻하지 않음 |
| `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --all-features` | 통과; crate 원본에서 HTML 생성 |
| `cargo run --locked -p deltaweave -- self-test` | `status: pass`; delta/index/양방향·conflict·delete·restart 검사 |
| `cargo build --locked --workspace --release --all-features` | exit 0, optimized release binary 생성 |
| `cargo install --locked --path crates/deltaweave-cli --root <새 임시 경로>` 및 설치 binary `--version` | exit 0, `deltaweave 0.4.0`; 사용자 Cargo 설치 경로를 덮어쓰지 않음. 임시 bin이 PATH에 없다는 Cargo 안내는 절대 경로 실행으로 확인 |
| CLI `--help`, `--version`, 10개 하위 명령 `--help` | `deltaweave 0.4.0`, 전체 도움말 정상 종료, 문서 옵션/기본값 대조 |
| 임시 폴더의 `init`, `manifest`, `scan`, `watch` | identity 재사용, 기본 profile, rename/delete, collision report, native event, SIGTERM 정상 종료 확인 |
| 문서의 잘못된 인자 예시 3개 | `scan` 필수 인자 누락 exit 2, allow-list 없는 `serve` exit 1, direct 주소 없는 `push --direct-only` exit 1 |
| 두 Compose 파일 `config --quiet` | 통과; 가짜 공개 endpoint ID·임시 데이터 경로 사용. 필수값 각각 누락 시 exit 1도 6회 확인 |
| `bash scripts/tests/test-p2p-loopback.sh`; `DELTAWEAVE_BIN=<debug binary 절대 경로> ./scripts/test-p2p-loopback.sh` | 모두 exit 0; 기본 104857600 bytes 실제 전송, 양쪽 SHA-256 일치 |
| 임시 두 peer의 continuous `sync` | 기본 5초 polling, native watcher; 로컬 이벤트 0.911초, 원격 전용 변경 5.224초 후 반영. 파일 bytes/BLAKE3/세 Merkle root 일치, 양쪽 SIGTERM exit 0. 이 시간은 단일 실행 관측값 |
| Markdown AST·상대 경로·제목 앵커·코드 fence·`git diff --check` | Markdown 16개, 상대 링크/asset 95개, 앵커 8개, 닫힌 fence 63개 통과; 최종 수정 후 재검사 완료 |
| release workflow대로 임시 package 문서 레이아웃 구성 | Markdown 5개, 상대 링크/asset 34개 모두 존재. 바이너리 archive 생성 시험은 아님 |
| 변경한 외부 링크 3개 HTTP GET | 모두 HTTP 200; 릴리스 페이지, 장비 안내 원문, GitHub GHCR 인증 문서 |

CLI 예제는 새 임시 디렉터리에서 실행했다. `self-test`와 테스트 suite도 자체 임시
root/state를 사용한다. 설치는 `--root`로 별도 임시 prefix를 지정했다. 별도 수동 CLI
검사는 저장소에 테스트나 의존성을 추가하지 않고 임시 Python harness로 실행했다.

실행 로그는 이 작업 환경의 `/tmp/deltaweave-docs-verify-release.log`,
`/tmp/deltaweave-docs-cli-check.log`, `/tmp/deltaweave-docs-release-build.log`,
`/tmp/deltaweave-docs-install.log`에 있다. 임시 로그는 영구 배포 산출물이 아니며,
재현 명령과 검증 범위는 위 표와 contributor 안내를 기준으로 한다.

## 외부 참조 확인

읽기 전용 GitHub API 및 `git ls-remote`로 확인했다. 이는 릴리스 존재 확인이며
해당 archive의 바이너리를 실행했다는 뜻은 아니다.

- `refs/heads/main`, `refs/tags/v0.4.0` → 기준 commit `75ffab74ee3de4dd1d071c13ab04ae26fb4e6e5b`.
- `refs/tags/v0.3.0` → `477eb26538f455d3936c6d8f07a5e771f90ebf74`.
- [v0.4.0 릴리스](https://github.com/happyaspic-byte/DeltaWeave/releases/tag/v0.4.0):
  2026-09-04 06:43:44 UTC 게시, prerelease, Windows x86_64 zip,
  Synology x86_64/aarch64 tar.gz, `SHA256SUMS.txt` 네 asset 존재.
- GHCR `main`과 `sha-75ffab74ee3de4dd1d071c13ab04ae26fb4e6e5b`의 manifest:
  `sha256:5533f0b11dc550c73ab65e06c026db5fbcecc4f364d994dc8cca5509deaf6194`,
  runnable `linux/amd64`·`linux/arm64` 포함. 익명 읽기만 수행했고 pull/run하지 않았다.

최종 독립 검토에서 추가로 수정할 중대한 문서 불일치는 발견되지 않았다. 변경 파일은
기존 Markdown 14개와 새 CLI 참조·검증 기록 2개뿐이다. 선택한 문서 범위와 가능한
로컬 검증은 완료했고, 아래의 제품 후속 작업과 대상 환경 검증은 별도로 남긴다.

## 남은 제품 문제와 검증하지 않은 범위

아래는 문서 불일치와 분리한 구현/설정의 후속 사항이다. 이번 문서 작업은 이를
고치거나 테스트의 판정을 변경하지 않았다.

| 항목 | 코드 근거와 필요한 후속 조치 |
| --- | --- |
| 장애 시점 판정 | CLI `wait_active_transfer`는 state 전체 파일 수를 CAS-only baseline과 비교한다. metadata/index만으로 성립할 수 있으므로 CAS 경로 기준과 독립적인 durable barrier 검증 필요 |
| 실패 bundle 수명 | CLI `fault_test`의 `TempDir`은 성공/실패 모두 drop된다. help/오류 메시지의 보존 약속과 충돌한다. explicit workspace가 없는 실패의 보존 정책·구현·도움말을 함께 수정할 필요가 있다 |
| 하네스 증거 범위 | receiver 준비는 ready 로그가 아닌 약 2초 생존 확인; 일부 오류에서 child cleanup 보장 없음. 최종 peers 파일/hash 비교는 모든 동작의 독립 expected oracle이 아니다. 이에 대한 추가 제품 테스트 필요 |
| 컨테이너 버전 label | `Dockerfile`의 `ARG DELTAWEAVE_VERSION=0.3.0`. 현재 binary 버전 판정은 `--version`으로 하며 build/publication label의 일관성 수정 필요 |
| QUIC 입력 hardening | push에서 마지막 requested payload 뒤 EOF/trailing data 검사 없음, v2 pull에 push 전용 file-size/chunk-count 제한 없음. 정책 확정과 구현/적대 입력 테스트 필요 |
| 복구·외부 변경 | `Store::materialize`/`remove_path`의 rename, journal, index가 별도이고 startup replay 없음. local apply에는 receiver와 같은 매 action causal 재검사가 없다. 전원 차단·경합 경계 검증과 제품 설계 필요 |

Windows/MSVC, 물리 Synology/DSM, ARM64/QEMU, macOS, 실제 장비 사이의 방화벽과
연결, Internet discovery/relay, 컨테이너 실행과 Portainer 배포, 장기 soak, 8 GiB 이상
실파일, 릴리스 archive의 체크섬 비교·실행, 버전 간 migration은 이번 Linux 실행으로
검증하지 않았다. 플랫폼/장비가 필요한 항목은 해당 안내에 따라 별도 검증한다.
NAS 디렉터리 준비, 방화벽 변경, image pull/run, stack 생성, rollback/백업 복원 등
운영 예제는 구문·코드·설정 전제만 대조했고 자동 실행하지 않았다.

과거 이미지의 원 실행 로그와 v0.3 품질 점수는 이번에 재확인한 결과가 아니다.
RustSec audit과 원격 CI의 전체 통과 여부도 실행/판정하지 않았다. 자료를 업데이트했다는
이유로 제품의 운영 지원·장비 인증·release 승인을 주장하지 않는다.
