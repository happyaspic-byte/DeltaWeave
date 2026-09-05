# 데이터 보존 경계 테스트 보강

## 범위와 근거

기준 커밋은 `75ffab7`이며 시작 시 작업 트리는 깨끗했다. Rust 1.91.0,
Cargo workspace, 기존 `#[test]` / Tokio 테스트와 `tempfile`을 사용한다.
저장소별 `AGENTS.md`는 없었으며 `CONTRIBUTING.md`, CI, 프로토콜과
아키텍처 문서의 계약을 기준으로 삼았다.

기존 테스트는 인증되지 않은 피어 거부, 정상·증분 전송, 충돌 사본,
삭제 전파, 동기화 재시작을 다룬다. 이번에는 데이터 유실에 직결되지만
기존 검증이 빠진 다음 경계에 한정한다.

| 보호할 흐름 | 기존 빈틈 | 이번 검증 수준과 완료 조건 |
| --- | --- | --- |
| 검증에 실패한 파일 교체 | 전체 파일 해시가 틀린 매니페스트의 실패 후 상태를 검증하지 않음 | 실제 CAS·파일시스템·redb를 사용해 원본과 기존 메타데이터 보존, 임시 파일 정리, 미완료 저널 확인 |
| 설치 후 메타데이터 기록 중단의 재시도 | 기존 재시작 테스트는 매니페스트 저장만 검증하며 저널 상태를 읽지 않음 | 설치된 파일과 `Prepared` 체크포인트를 구성하고 재개방 후 청크 없이 재시도하여 메타데이터와 `Committed` 복구 및 재개방 후 영속성 확인 |
| 파일 교체 이력 복구 | 휴지통 최상위 항목 개수만 확인 | 보존된 파일의 실제 바이트가 교체 전 원본과 같은지 확인 |
| 업로드 도중 로컬 편집 | 업로드 후 인덱스를 새로 스캔하는 인과 검증을 직접 보호하지 않음 | 실제 로컬 QUIC의 `NeedChunks` 응답 후 로컬 파일을 편집하고 업로드를 마쳐도 편집 내용이 보존되고 요청이 거부되는지 확인 |
| 원격 삭제와 중복 재시도 | stale 파일 쓰기와 디렉터리 삭제는 다루지만 concurrent 삭제와 파일 tombstone 재시도는 빠짐 | stale·equal·concurrent 삭제 거부 후 내용·인과 상태·복구 이력 보존, 지배하는 삭제 성공과 동일 요청의 멱등성 확인 |

기대 결과의 근거는 [아키텍처의 커밋·복구 계약](ARCHITECTURE.md),
[v2의 인과 쓰기와 멱등 재시도 계약](PROTOCOL.md),
[위협 모델의 로컬 편집·교체 이력 보호](THREAT_MODEL.md)다.
최근 전송 파이프라인과 검증 후 인덱스 채택 경로가 변경된 이력
(`951e368`, `cabb2db`, `7131206`)도 확인했다. 이 이력이 실제 결함을
발생시켰다고 가정하지 않는다.

## 검증 방법

테스트는 각자의 임시 디렉터리와 로컬 테스트 피어를 사용한다. 외부 서비스와
운영 계정·데이터를 사용하지 않는다. 비동기 순서는 `NeedChunks`라는 관찰
가능한 응답으로 제어하고 제한된 타임아웃을 둔다. 시간 경과나 파일 타임스탬프
해상도에 의존하지 않도록 서로 다른 길이의 내용을 사용한다.

핵심 테스트마다 해당 보호 조건을 임시로 제거하거나 잘못된 부수 효과를
주입해 예상 단언에서 실패하는지 확인한다. 각 변경은 원복한 뒤 다시 실행한다.
네트워크 신규 테스트는 정상 코드에서 3회 연속 실행한다. 이는 이 로컬 환경의
제한된 반복 검증이며 모든 운영체제에서의 안정성을 보증하지 않는다.

기존 CI의 workspace 테스트 명령이 새 테스트를 포함하므로 CI 변경이나
새 의존성은 필요하지 않다. 로컬 재현에는 고정 Rust 도구 체인, 다운로드된
Cargo 의존성, 임시 디렉터리 생성 권한과 로컬 UDP 통신이 필요하다.

## 실행 결과

### 변경 파일과 테스트 매핑

- `crates/deltaweave-store/src/lib.rs`
  - `whole_file_hash_mismatch_preserves_destination_and_metadata`: 잘못된 전체
    해시의 교체를 거부하고 재개방 후에도 원본·기존 메타데이터·미완료 저널을 보존한다.
  - `retry_after_install_recovers_prepared_operation_without_cached_chunks`:
    설치 후 중단 체크포인트에서 다운로드나 재교체 없이 영속 메타데이터를 복구한다.
  - `replacement_preserves_old_content`: 기존 테스트를 강화하여 휴지통에서
    실제 원본 바이트를 읽는다.
- `crates/deltaweave-net/src/lib.rs`
  - `causal_push_rechecks_local_edits_after_chunk_negotiation`: 청크 협상 이후
    로컬 편집이 발생한 업로드를 거부하고 편집 내용과 새 인과 기록을 보존한다.
  - `causal_tombstones_preserve_conflicts_and_allow_idempotent_delete`: 세 종류의
    잘못된 삭제를 거부하고, 해결된 삭제와 동일 요청의 재시도를 검증한다.

### 기준 상태

- `cargo test --workspace --all-targets`: **103 passed, 0 failed, 0 ignored**.
  최초 컴파일로 만든 수정 전 바이너리가 실행되었으며, 저장소 17개와
  네트워크 14개를 포함한다. 프로세스 종료를 수행하는 기존 CLI 통합 테스트
  3개도 통과했다(이 환경에서 약 214초).
- `cargo fmt --all -- --check`: 통과.
- 관련 테스트의 수정 전 실행도 각각 저장소 **17/17**, 네트워크 **14/14**
  통과했다. 기존 실패는 관찰되지 않았다.

### 최종 검증과 임시 결함

저장소의 정상 코드에서 `cargo test --locked -p deltaweave-store --lib`는
**19 passed, 0 failed**였다. 다음 임시 결함은 각 대상 테스트를
`cargo test --locked -p deltaweave-store --lib tests::<아래 테스트 이름> -- --exact`
형태로 실행하여 확인했다. 모두 컴파일에 성공한 뒤 명시한 단언에서 실패했다.
각 임시 변경은 실험 직후 복원했고, 최종 원복 상태에서 저장소 테스트 19개가
다시 통과했다.

| 임시 결함 | 대상 테스트 | 실제 탐지 결과 |
| --- | --- | --- |
| `Store::materialize`의 전체 파일 해시 불일치 거부 제거 | `whole_file_hash_mismatch_preserves_destination_and_metadata` | 잘못된 매니페스트가 성공 결과를 반환하여 `expect_err` 실패 |
| 이미 설치된 파일 처리 분기의 `put_operation` 생략 | `retry_after_install_recovers_prepared_operation_without_cached_chunks` | 재개방한 저널이 `Prepared`여서 기대한 `Committed`와 불일치 |
| 기존 파일을 휴지통으로 옮긴 직후 백업 내용을 빈 바이트로 변경 | `replacement_preserves_old_content` | 읽은 백업 `[]`와 기대한 `old content` 불일치 |

네트워크의 최초 정상 실행 `cargo test --locked -p deltaweave-net --lib`는
**16 passed, 0 failed**였다. 다음 두 결함에서는 각각 새 테스트가 실패했다.
같은 결함을 유지한 채 `cargo test --locked -p deltaweave-net --lib -- --skip tests::causal_`
로 실행한 기존 네트워크 테스트 **14개는 각각 모두 통과**했다.

| 임시 결함 | 대상 테스트 | 실제 탐지 결과 |
| --- | --- | --- |
| `ensure_causally_applicable`의 새 스캔 및 스캔 건전성 검사 제거 | `causal_push_rechecks_local_edits_after_chunk_negotiation` | 로컬 편집 이후에도 15바이트 업로드가 `Applied`로 승인되어 거부 단언 실패 |
| `CausalRelation::Concurrent` 분기를 `Ok(())`로 변경 | `causal_tombstones_preserve_conflicts_and_allow_idempotent_delete` | concurrent 삭제가 성공 receipt를 반환하여 거부 단언 실패 |

결함 검증 명령은 다음과 같다. 각 명령은 해당하는 임시 결함이 있을 때만
실패해야 하며 정상 소스에서는 통과해야 한다.

```bash
cargo test --locked -p deltaweave-net --lib tests::causal_push_rechecks_local_edits_after_chunk_negotiation -- --exact
cargo test --locked -p deltaweave-net --lib tests::causal_tombstones_preserve_conflicts_and_allow_idempotent_delete -- --exact
```

두 네트워크 결함을 원복한 후 개별 크레이트 테스트 **16개가 모두 통과**했다.
다음 명령으로 비동기 신규 테스트 두 개를 정상 코드에서 **3회 연속 실행**했고,
각 회차는 **2 passed, 0 failed**였다(0.85초, 0.76초, 0.76초).
각 테스트는 2개 Tokio 워커, 고정 테스트 키, 임시 파일과 임의 할당된 루프백
UDP 포트, 30초 완료 타임아웃을 사용한다. 재시도나 skip으로 실패를 숨기지 않았다.

```bash
for run in 1 2 3; do
  cargo test --locked -p deltaweave-net --lib tests::causal_ || exit 1
done
```

두 Rust 파일의 테스트 모듈 앞 제품 소스를 `git show HEAD:<경로>`와 비교하여
임시 결함이 남지 않았음을 확인했다. 제품 동작 수정이나 의존성·CI 변경은 없다.
최초 최종 포맷 검사에서 신규 코드 한 곳의 줄바꿈 차이를 발견해 수정했고,
`cargo fmt --all -- --check`를 다시 실행하여 통과했다.

### 최종 품질 검사

모든 명령은 이 작업 트리의 Linux 환경, Rust 1.91.0에서 실행했고 종료 코드가
0이었다. 기존 CI의 테스트 명령이 신규 테스트를 자동으로 포함한다.

| 실행 명령 | 실제 결과 |
| --- | --- |
| `cargo test --locked --workspace --all-targets --all-features` | **107 passed, 0 failed, 0 ignored**; 저장소 19개, 네트워크 16개, 기존 CLI 장애 복구 3개 및 동기화 통합 테스트 포함 |
| `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` | 통과; 테스트 코드 포함 타입 검사와 Clippy 경고 없음 |
| `cargo fmt --all -- --check` | 신규 테스트의 줄바꿈 수정 후 통과 |
| `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --all-features` | 통과; 문서 경고 없음 |
| `cargo build --locked --workspace --all-features` | 통과; 개발 프로필 빌드 |
| `cargo run --locked -p deltaweave -- self-test` | `status: pass`; 양방향 동기화·삭제 검증 true, 충돌 보존 1건, 재시작 작업 0건, 임시 데이터 정리 |
| `git diff --check` | 통과 |

최종 전체 테스트의 CLI 장애 복구 테스트는 약 453초가 걸렸다. 다른 빌드와
검사를 병행하는 로컬 실행 조건이었으며, 기준 실행과의 시간 차이를 성능 회귀로
단정하지 않는다. 타임아웃 증가나 테스트 생략은 하지 않았다.

수정 전 테스트 실패와 최종 미해결 실패는 없었다. 추가한 테스트 4개와 강화한
기존 테스트 1개의 결함 탐지력 확인을 마쳤으며, 임시 결함으로 유도한 다섯 실패는
정상 코드의 회귀 실패와 구분한다. 이 범위에서는 제품 버그가 발견되지 않았다.
CI 설정은 읽어 확인했지만 원격 실행·결과 조회는 하지 않았다.

## 범위 밖과 한계

- stale exact-pull 요청 거부는 별도 보강 후보로 기록한다. 후속 작업에서는
  내용은 같지만 인과 버전이 바뀐 파일의 이전 기록으로 pull을 요청해 거부를
  검증할 수 있다. 이번에는 데이터를 직접 변경하는 수신 측 교체·삭제를 우선했다.
- 인덱스의 불완전 스캔·경로 충돌과 전체 동기화의 최종 루트 검증은 이번
  보강 범위가 아니다. 기존 테스트 실행 결과만 기록하며 이 경계의 결함 탐지력을
  새로 확인했다고 주장하지 않는다.
- 복구 체크포인트를 명시적으로 구성하는 테스트는 실제 OS 장애, 전원 차단,
  디스크 `fsync` 내구성 또는 모든 프로세스 종료 지점을 증명하지 않는다.
- Windows·Synology 실기기와 원격 GitHub CI는 실행하거나 결과를 조회하지 않았다.
  Windows 검증에는 기존 CI의 `test-windows` 작업이 필요하며, 실제 Synology
  파일시스템·하드웨어 검증에는 격리된 데이터와 해당 장비가 필요하다.
