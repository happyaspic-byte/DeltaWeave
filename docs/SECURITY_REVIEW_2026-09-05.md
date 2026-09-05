# 보안 검토 및 회귀 검증 — 2026-09-05

> 이 문서는 개별 worktree에서 수행한 당시의 검증 기록입니다. 6개 작업을 합친
> 현재 구현과 재검증 결과는 [통합 보고서](INTEGRATION_2026-09-05.md)를 기준으로 합니다.

## 범위와 판정 원칙

기준 커밋은 `75ffab74ee3de4dd1d071c13ab04ae26fb4e6e5b`이며, 결과는
`security-update` 작업 트리의 변경을 대상으로 한다. 이어서 작업할 때 발견한
미커밋 수정을 보존하고 실제 코드·설정·검증 결과로 다시 평가했다. 별도
`AGENTS.md`는 발견하지 못했으며 `CONTRIBUTING.md`, `SECURITY.md`,
`docs/THREAT_MODEL.md`, 프로토콜·아키텍처 문서, CI와 릴리스 검사를 읽었다.
심각도는 공격 전제·노출·데이터 영향에 대한 정성 평가이며 CVSS 점수나 CVE를
부여하지 않았다. 프로젝트 전체가 안전하다는 보증은 아니다.

Rust 1.91 / Cargo 8개 crate, iroh 1.1.0의 인증된 QUIC, postcard 프레임,
redb 인덱스·메타데이터, BLAKE3 CAS를 조사했다. 우선순위는 원본·복구 데이터
보존, 동기화 루트 밖 파일 접근, 개인 상태의 기밀성, 원격 입력의 검증이었다.
검증은 임시 디렉터리와 합성 파일·생성한 테스트 키·로컬 피어만 사용했다.
운영 데이터·자격 증명·서비스·배포·외부 대상 스캔은 변경 범위에 포함하지 않았다.

| 보호 대상 / 경계 | 실제 진입점과 권한 |
| --- | --- |
| 파일 내용·이름·readonly·복구 이력 | v1 Push, v2 PushRecord/ApplyMetadata, 로컬 sync apply → Store |
| 신원 비밀 키 | CLI init/serve/sync/push → 키 파일; iroh가 소유 증명 |
| 인덱스·CAS·저널·송신 캐시 | CLI의 로컬 경로 및 파일시스템 → redb/일반 파일 |
| 원격 스냅샷·자원 | 선택한 피어의 Merkle 요약·manifest·chunk → 검증 → staging/apply |
| 배포 권한 | GitHub Actions의 검사·빌드와 publish job 사이 토큰 권한 |

서버는 두 ALPN 모두 `connection.remote_id()`를 허용 목록과 대조한 뒤 스트림을
받는다. 로그인 계정·브라우저 세션·테넌트별 객체 권한 모델은 없다. 허용 피어는
설정된 루트 전체를 읽고 쓸 수 있는 주체다. 이 권한과 인과적 버전 검사는 서로
별개이며, 다른 계정의 ID를 바꿔 권한을 얻는 HTTP IDOR 모델은 적용되지 않는다.

`--direct-only`는 릴레이·검색을 끄며 localhost 제한이 아니다. Compose는
host networking을 쓰고 `--bind`를 지정하지 않는다. iroh의 기본 IPv4/IPv6
와일드카드 바인딩 때문에 LAN 및 방화벽/NAT 설정에 따라 인터넷에서 도달할 수
있다. 실제 운영 방화벽/NAT는 검사하지 않았다.
[iroh 바인드 문서](https://docs.rs/iroh/1.1.0/iroh/endpoint/struct.Builder.html#method.bind_addr)

웹 UI·HTTP 애플리케이션·쿠키·브라우저 인증이 없으므로 XSS, CSRF, CORS,
쿠키 속성, HTTP 보안 헤더 검사는 비적용이다. 임의 SQL/셸 실행 서버도 없으며
redb의 타입 기반 API와 로컬 진단용 프로세스 실행을 확인했다. 경로·업로드
검사는 실제 P2P 파일 수신 경계에 적용했다. UI 변경이 불필요하여 taste-skill을
적용하지 않았으며, 화면·브랜드·키보드 동작은 변경하지 않았다.

## 확인된 결함과 수정

아래 재현은 모두 합성 데이터다. 상세 검증 결과는 뒤의 명령 표에 기록한다.

| ID / 심각도 | 위치·공격 전제·수정 전 영향 | 재현과 핵심 수정 | 상태 / 잔여 위험 |
| --- | --- | --- | --- |
| S1 중간 — 하드 링크를 통한 루트 밖 권한 변경 | `deltaweave-store`: 루트 안 파일이 밖의 파일과 같은 inode이고, 허용 피어가 같은 내용과 다른 readonly 값을 보냄. 기존 idempotent 경로가 공유 inode에 chmod 수행 | 밖의 합성 파일 mode 0600, 안의 hardlink에 readonly 적용 시 밖의 mode도 바뀜. 링크 수가 1인 파일만 재사용하고, 그 외에는 검증된 새 inode로 교체. readonly 적용 직전에도 링크 수 확인 | 수정. Unix 재현; Windows 링크 수 분기는 실제 실행 미검증. 로컬 경로 교체 경쟁은 잔존 |
| S2 중간 — 재시작 후 복구 이력 덮어쓰기 | `deltaweave-store::trash_path`: 프로세스별 카운터가 초기화되어 동일 경로·operation hash가 같은 백업 이름을 재사용 | 두 별도 프로세스에서 `first content`, `later content`를 삭제하면 첫 백업 소실. 백업 디렉터리를 create-dir로 원자적 예약하고 충돌 시 다음 이름 선택 | 수정. 두 버전 보존 검증. 디스크 할당량·GC는 별도 잔여 범위 |
| S3 낮음 — 끊어진 심볼릭 링크 복구 손실 | `Store::materialize`: `Path::exists()`가 dangling symlink를 없음으로 취급하여 교체 시 trash에 보존하지 않음 | `missing-target` 링크를 일반 파일로 교체하는 합성 재현. `symlink_metadata`로 존재 판정하고 NotFound 이외 오류를 전파 | 수정. 링크 자체와 새 파일 내용 모두 검증. 링크 materialization 기능을 추가한 것은 아님 |
| S4 중간 — 새 개인 상태의 로컬 노출 | Store·서버·SyncEngine·SenderManifestCache의 신규 디렉터리가 umask 022에서 0755. 다른 로컬 계정이 접근 가능한 부모 아래라면 동기화 데이터·백업·경로·manifest 노출 | 제어한 umask 하위 프로세스 및 생성 테스트. Unix DirBuilder mode 0700을 신규 상태·하위 디렉터리·송신 캐시에 적용 | 수정. 기존 디렉터리의 운영자 지정 mode는 바꾸지 않음; 기존 설치의 권한 확인 및 Windows ACL 검토 필요 |
| S5 낮음 — 인덱스 DB 심볼릭 링크 검사 우회 | `LocalIndex::open`: DB 경로를 canonicalize한 후 symlink 여부 검사. 로컬 DB 경로를 준비할 수 있는 주체가 밖의 쓰기 가능한 파일을 가리킴; 분리된 private state 배포에서 원격 도달성은 없음 | 바깥의 빈 파일로 향하는 `index.redb`가 redb로 초기화됨. 원래 입력 경로의 symlink를 해석 전에 거부 | 수정. 바깥 파일이 비어 있는 그대로인지 검증. 부모 경로의 동시 교체는 별도 위험 |
| S6 중간 — 거부된 설정이 공유 루트에 비밀 키 생성 | CLI `serve`/`open_sync_engine`: 키 생성 후 루트 안 키를 거부. 잘못 지정된 경로에 남은 키가 이후 다른 동기화로 노출될 수 있음 | `received/private/receiver.key` 설정 거부 뒤 키 파일이 존재. 기존/신규 키 위치를 먼저 검증하도록 순서 수정 | 수정. serve·sync 거부 후 키 부재 및 정상 외부 신규 경로 검증. 실제 유출 키는 발견하지 못함 |
| S7 중간 — Merkle 요약 검증 전 작업 증폭 | `SyncClient::fetch_snapshot_connected`: 선택한 악성 인증 피어가 중복·비직접 자식, 잘못된 개수/record prefix, 부모와 다른 자식 commitment를 제공 | 스크립트형 로컬 QUIC 피어에서 불필요한 후속 query/Finish 관찰. 즉시 자식·정렬·중복·개수·record prefix·부모 commitment 검증, 대기 queue까지 총 query 예산 적용 | 수정. 유효 스냅샷·한 노드 fast path 유지. 전역 세션/동시 연결/디스크 quota는 별개 |
| S8 중간 — 양측 병합 뒤 새 이름 충돌 | `validate_materializable_namespace`: 각 피어는 개별적으로 정상이나 병합하면 `README.txt`/`readme.txt`, Unicode 동등 이름 또는 부모 `Docs`/`docs` 충돌 | 검증 전에는 양측 파일 변경 후 최종 scan에서 실패. 병합한 live 경로와 모든 부모의 기존 collision_key를 staging 전에 검증 | 수정. 단위 검사와 두 피어의 원본 내용·항목 수 보존 검사. tombstone 별칭은 아래 미검증 후보와 구분 |
| S9 중간 — 오래된 로컬 스냅샷으로 새 작업 삭제 | `SyncEngine::apply_local`: 원격 staging 중 사용자가 수정한 파일을 이전 snapshot의 삭제 계획으로 trash 이동 후 tombstone 채택 | snapshot 뒤 `new local work after the snapshot` 작성, remote deletion 적용 시 원래 경로 소실. apply 직전 안전한 전체 scan 및 root/count 일치 검사 | 수정. 원래 경로·내용 보존과 정상 충돌/삭제/재시작 동기화 검증. scan 이후의 동시 쓰기 및 OS snapshot 부재는 잔존 |
| S10 낮음 — 모순된 매니페스트를 정상 재사용으로 커밋 | `Store::materialize`: 허용 피어가 기존 전체 파일 해시와 다른 내용의 청크·크기를 조합. 실제 파일이 존재하면 전체 해시만 비교해 메타데이터와 성공 receipt 생성 | 동일 크기·다른 크기의 합성 payload 모두 수정 전 수락. `file_matches_manifest`가 크기·각 chunk digest·EOF·전체 hash를 한 번의 읽기로 검증 | 수정. 원본 내용·기존 metadata 보존, 거짓 Committed 부재, 정상 대체 청크 경계의 already_current 재사용 통과. 원본 덮어쓰기/권한 상승으로 확대 해석하지 않음 |
| S11 낮음 — 오류 응답의 내부 경로 노출 | v1/v2 ProtocolHandler가 허용 피어에게 storage error.to_string을 전달하고 그대로 로그/AcceptError에 복사 | 합성 `receiver-private-sentinel` 아래 비어 있지 않은 디렉터리 변경을 시도하면 두 프로토콜의 오류에 절대 경로 포함. `public_error_message`의 한정된 정적 복구 안내를 응답·로그·router 오류에 공통 적용 | 수정. 내부 경로 부재와 원본 보존, 수정한 요청의 정상 v1/v2 동작 검증. CLI의 운영자용 로컬 경로 진단은 별도 |

S4에는 독립 인덱싱 및 기존 공개 부모 아래 **새 DB 파일** 생성도 포함한다.
인덱스·스토어 metadata·송신 캐시는 동일한 read/write/create/truncate(false)
OpenOptions에 Unix mode 0600을 적용한 File을 redb `create_file`에 전달한다.
기존 파일 mode, DB 잠금·재시작·잘못된 기존 파일 보존 계약도 검사한다.
S11의 wire 응답은 실제 QUIC 회귀로 검사했다. 로그·router 오류에도 같은 정적
매핑이 적용된다는 판정은 코드 검토 근거이며 로그 수집기의 전체 동작 검증은 아니다.

## 의존성·CI와 기각한 후보

RustSec 조회 시점은 **2026-09-05T20:50:05Z**, cargo-audit는 **0.22.2**,
DB 커밋은 `5a0ebedfe8bdd2e295b171f4162f8c977bcad9a5`였다. DB 갱신 시각은
`2026-09-02T11:13:32+02:00`, 공지는 1,239개였다. 잠금 파일의 실제 버전을
검사했고 설치/해결 그래프는 `cargo tree --locked`로 확인했다.

| 후보 | 공식 근거·영향/수정 버전 | 결론 |
| --- | --- | --- |
| atomic-polyfill 1.0.3 / RUSTSEC-2023-0089 | 유지보수 중단; 수정 버전 없음. postcard 기본 기능 → heapless 0.7.17 경로 | 정보성 유지보수 위험. 사용하지 않는 postcard 기본 기능을 해제하여 제거. 원격 취약점으로 단정하지 않음 |
| paste 1.0.15 / RUSTSEC-2024-0436 | 유지보수 중단; 수정 버전 없음. netlink-packet-core → netwatch/netdev → iroh 빌드 매크로 경로 | 정보성 잔여 위험. 호환 가능한 좁은 제거가 없어 기존 단일 예외 유지; upstream 전환 추적 |
| 검사·빌드 job의 쓰기 토큰 | container의 전역 packages:write 및 release의 전역 contents:write; 빌드 코드 실행 권한이 전제 | 낮은 위험의 최소 권한 강화. publish job에만 쓰기 권한 부여. 실제 악성 job 실행·원격 익명 악용은 주장하지 않음 |
| pull의 명시적 chunk-count 한도 누락 | Hash32가 64자 hex 문자열로 직렬화됨. 최소 250,001개 1-byte chunk 응답 실측 17,233,793 bytes, 프레임 한도 16,777,216 bytes 초과 | 현재 직렬화/프레임에서 재현 불가로 기각. helper 검사 누락만으로 원격 취약점을 주장하지 않고 코드 변경하지 않음 |
| 인증/객체 접근 우회 | 두 ALPN 모두 인증된 EndpointId allow-list를 스트림 전 검사. causal push는 stale/concurrent/equal-clock divergent 상태 거부 | 조사한 경로에서 신규 결함 미발견; 기존 허용/거부 테스트로 확인. 허용 피어의 루트 전체 권한은 설계 |
| 경로 traversal·부모 symlink·내용 위조 | WirePath 생성/역직렬화, manifest 구조, chunk와 완성 파일 hash, 부모 symlink 검사 | 조사한 비경쟁 경로에서 신규 우회 미발견. 관련 정상·악성 입력 검사를 실행 |
| 추적 파일의 실제 자격 증명 | 키 파일 이름·private-key block·고신뢰 GitHub/AWS 토큰 패턴을 값 출력 없이 검사 | 후보 0개. 저장소 전체 역사·외부 secrets 서비스·저엔트로피 비밀은 검사하지 않음. 회전 대상이 확인된 실제 키 없음 |

공식 공지: [atomic-polyfill](https://rustsec.org/advisories/RUSTSEC-2023-0089.html),
[paste](https://rustsec.org/advisories/RUSTSEC-2024-0436.html).
두 공지는 유지보수 경고이며 CVE나 알려진 원격 취약점 공지가 아니다.
[GitHub job 권한 문서](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#permissions)

원본 Cargo.lock은 **433 packages / 취약점 0 / 유지보수 경고 2**, 수정 후는
**428 packages / 취약점 0 / 유지보수 경고 1**이다. `postcard` 버전 1.1.3은
그대로이며 `default-features = false, features = ["use-std"]`로 바꿨다.
atomic-polyfill 1.0.3, heapless 0.7.17, hash32 0.2.1, byteorder 1.5.0,
spin 0.9.9가 제거됐고 추가·업그레이드한 패키지는 없다. 보안/릴리스 workflow에서
제거된 atomic-polyfill 예외도 삭제했다. 다른 warning은 계속 실패 처리한다.

## 실제 검증

최종 작업 트리에서 아래 검증을 완료했다. 수정 전 재현은 HEAD의 생산 코드에
현재 회귀 테스트를 적용한 임시 복사본을 사용했다. 공유 Cargo target이 다른
복사본의 바이너리를 재사용할 수 있음을 확인하여 수정 전/후 target을 분리했다.
공유 target에서 재컴파일 없이 나온 결과는 수정 전 실패 증거로 채택하지 않았다.

수정 전 재현 결과:

| 임시 HEAD 생산 코드 + 회귀 테스트 명령 | 관찰 |
| --- | --- |
| `cargo test --locked -p deltaweave-store -p deltaweave-index --all-targets --all-features --no-fail-fast` | store 4 + index 1 실패, 정상 45 통과: S1~S5의 기존 회귀 |
| `CARGO_TARGET_DIR=/tmp/deltaweave-security-baseline-target cargo test --locked -p deltaweave --bin deltaweave identity` | 3 실패 / 1 통과: serve·sync 키 잔존, 신규 키 위치 사전 검사 |
| 같은 target의 `cargo test --locked -p deltaweave-sync --lib` | 4 실패 / 1 통과: 새 상태 mode, 병합 충돌, 양측 항목 수 변경, 새 로컬 작업 소실 |
| 같은 target의 `cargo test --locked -p deltaweave-net snapshot_rejects_ -- --nocapture` | 2 실패: malformed 요약 후 query 2회(기대 1), commitment 변경 후 요청 3회(기대 2) |
| 같은 target의 `cargo test --locked -p deltaweave-net creates_private -- --nocapture` | 3 실패: 서버·송신 상태 루트 0755, 송신 DB 0644 |
| 수정 전 `cargo test --locked -p deltaweave-store idempotent_materialization_` | 악성 매니페스트 1 실패, 정상 대체 청크 경계 1 통과 |
| 수정 전 `cargo test --locked -p deltaweave-net filesystem_errors_hide_receiver_paths_and_allow_recovery -- --nocapture` | v1/v2 내부 경로 노출 assertion 실패; 원본 보존·수정 요청 성공은 확인 |
| 수정 전 store/index `database_` 필터 테스트 | 신규 DB 0600 검사 각각 실패; 잘못된 기존 DB를 보존하는 정상 경계 각각 통과 |

처음 전체 검증 중 추가 조사용 pull-count 테스트 두 개가 잘못된 프레임 크기
가정으로 실패했다. 위 직렬화 실측으로 후보가 기각되어 해당 임시 조사 코드는
제거했다. 기존 회귀 검사의 기대값을 약화한 변경은 없으며 최종 결과는 안정된
작업 트리에서 재실행한 전체 검사로 판정한다.

| 명령 | 결과 |
| --- | --- |
| `cargo fmt --all -- --check` | 종료 0 |
| `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` | 종료 0 |
| `cargo test --locked --workspace --all-targets --all-features` | 종료 0; 총 127 통과, 실패/무시 0 (CLI 14 + fault 3 + cdc 7 + core 11 + index 31 + net 20 + reconcile 11 + store 25 + sync 5) |
| `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --all-features` | 종료 0 |
| `cargo run --locked -p deltaweave -- self-test` | 종료 0; status pass, 양방향 동기화·삭제 true, 충돌 복사본 1개 보존, restart actions 0 |
| `cargo build --locked --workspace --release --all-features` | 종료 0; 최종 수정 후 workspace 전체 최적화 빌드 완료 |
| `RUST_LOG=warn,netwatch=error target/release/deltaweave self-test` | 종료 0; 최적화 바이너리에서도 pass, 양방향·삭제 true, 충돌 복사본 1, restart actions 0 |
| `cargo test --locked -p deltaweave --test fault_test -- --test-threads=1` | 종료 0; 독립 장애 복구·동일 seed 재현·실패 증거 보존 3개 통과 |
| `cargo run --locked -p deltaweave -- fault-test --seed 424242 --payload-mib 16 --workspace "$fault_workspace"` | 종료 0; 임시 workspace 사용, status pass, 실제 serve 및 sync-once 종료·복구, 양쪽 restart actions 0, error null |
| `bash scripts/verify-release.sh` | 종료 0; 최종 9단계 모두 통과, Compose 포함 skip 없음 |
| `git diff --check` | 종료 0 |
| `cargo audit --json` | 종료 0; 428 dependencies, 취약점 0, paste 유지보수 경고 1 |
| `git show HEAD:Cargo.lock > /tmp/deltaweave-security-original-Cargo.lock` 후 `cargo audit --no-fetch --file /tmp/deltaweave-security-original-Cargo.lock --json` | 종료 0; 433 dependencies, 취약점 0, 유지보수 경고 2 |
| `cargo audit --no-fetch --deny warnings` | 종료 1; 잔존 paste 경고 때문에 실패하는 것을 확인 |
| `cargo audit --no-fetch --deny warnings --ignore RUSTSEC-2024-0436` | 종료 0; 문서화한 기존 예외 하나만 허용 |
| `cargo tree --locked -i paste` / `cargo tree --locked -e features -p postcard` | 종료 0; 전이 경로와 제거한 기본 기능 확인 |
| 양쪽 Compose 파일에 `docker compose -f … config --quiet` | 합성 allow-peer, PUID/PGID=65532, /tmp 데이터 경로로 모두 종료 0; 두 alias 파일 동일 |
| 4개 workflow PyYAML 파싱 | 성공; GitHub 원격 job 실행 및 actionlint는 미실행 |
| `bash scripts/tests/test-p2p-loopback.sh` | 종료 0; 스크립트의 정상 전송·실패 시 정리 계약 통과(가짜 CLI 사용, 실제 네트워크 증거와 구분) |
| `DELTAWEAVE_BIN=$PWD/target/debug/deltaweave bash scripts/test-p2p-loopback.sh` | 종료 0; 실제 인증 QUIC loopback 104,857,600 bytes 전송, 양측 SHA-256 일치 |
| `cargo check --locked --workspace --all-targets --all-features --target x86_64-pc-windows-gnu` | 종료 0; 설치된 MinGW 도구로 Windows 코드·테스트 대상 타입 검사, Windows 실행 검증은 아님 |

검증 로그는 이번 실행 환경의 `/tmp/deltaweave-security-final-verification.log`,
`/tmp/deltaweave-security-final-release-build.log`,
`/tmp/deltaweave-security-windows-check.log`,
`/tmp/deltaweave-security-real-loopback.log`에 남겼다. 임시 baseline 빌드 캐시는
검증 종료 후 정리했다. 로그 경로는 배포 산출물이 아니며 위 명령·회귀 테스트로
검증을 다시 실행할 수 있다.

## 호환성과 잔여 위험

- ALPN, postcard 메시지 필드, manifest/record 해시와 redb 포맷을 유지한다.
  기존 정상 파일 전송·누락 chunk 재사용·충돌 복사본·삭제·재시작 흐름을 검사한다.
- Windows 코드·테스트 대상은 교차 `cargo check`에 통과했다. Windows/Synology
  실기기, Windows ACL/하드 링크의 실제 동작, 인터넷 릴레이·NAT와 장기간 soak는
  이 Linux 로컬 검증에서 실행하지 않았다.
- **의심·미재현:** 대소문자를 구분하지 않는 파일시스템에서 live `foo.txt`와
  tombstone `FOO.txt`가 병합될 때 exact-path 인덱스 검사와 OS alias 삭제가
  어긋날 가능성이 있다. Windows/macOS에서 원래 경로·내용·trash 변화를 검사해야
  한다. tombstone을 일괄 충돌로 거부하면 정상 case-only rename도 막히므로
  이번 live 이름 충돌 수정으로 해결했다고 주장하지 않는다.
- 디렉터리 핸들에 상대적인 openat2/Windows API 사용이 없어 악성 로컬 프로세스의
  경로 교체 경쟁은 해결하지 않았다. OS·관리자·개인 상태의 쓰기 권한은 신뢰한다.
- 허용 피어도 디스크를 채우거나 많은 세션을 열 수 있다. 프레임·chunk pipeline·
  Merkle 작업 한도는 전역 디스크 quota, 피어별 동시 연결/시간 한도를 대체하지
  않는다. 이 기능들은 기존 위협 모델의 별도 production gate다. 전체 자원 정책·
  장치 멤버십·핸들 기반 파일 접근은 호환성과 운영 정책을 함께 설계해야 하므로
  이번 재현 가능한 경계 결함의 최소 수정 범위에서 제외했다.
- 기존 Unix state/cache 디렉터리가 다른 사용자에게 열려 있다면 운영자가 소유자와
  공유 의도를 확인한 뒤 디렉터리 0700 및 파일 접근 권한을 검토해야 한다.
  이번 변경은 기존 mode/ACL을 임의 변경하지 않는다. 이미 유출된 키가 별도로
  확인되면 해당 EndpointId를 상대 허용 목록에서 제거하고 새 키 생성·공개 ID 교환·
  재등록 계획을 준비해야 한다. 실제 운영 키 회전이나 서비스 재시작은 실행하지 않았다.
- 보안 정책의 권고대로 전용 비특권 계정·명시적 allow-list·독립 백업을 유지한다.
  at-rest 암호화, 장치 폐기/멤버십, tombstone GC, 세션 제한은 별도 후속 범위다.
