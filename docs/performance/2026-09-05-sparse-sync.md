# 2026-09-05: 변경 파일이 적은 동기화의 Merkle subtree 조회

4,000개의 작은 파일 중 하나를 수정하는 로컬 동기화에서 전체 `SyncEngine::sync_once` 경과 시간 중앙값이 **1,606.886 ms에서 1,373.449 ms로 233.437 ms(14.53%) 감소**했다. 반복 전체 레코드 순회를 정렬 범위 조회로 바꾼 결과다. 같은 조건의 전후 각 7회 시간 범위가 겹치지 않고 무변경 대조군은 거의 같아, 이번에 정한 병목의 개선을 확인했다. 전체 107개 테스트와 아래 품질 검사를 통과했다. 작은 데이터·하위 폴더의 작은 차이와 메모리 감소는 확실한 개선으로 주장하지 않는다.

## 코드 근거와 변경 범위

기준 production commit은 `75ffab74ee3de4dd1d071c13ab04ae26fb4e6e5b`다. 이 기준에 같은 benchmark harness를 추가한 실행 파일과 최적화 후 실행 파일을 비교한다.

호출 경로는 다음과 같다.

1. [`SyncEngine::sync_once`](../../crates/deltaweave-sync/src/lib.rs)는 로컬 전체 scan으로 snapshot을 만든 뒤 동기화를 시작한다.
2. [`SyncClient::fetch_snapshot_connected`](../../crates/deltaweave-net/src/lib.rs)는 remote의 Merkle node를 조회하고, hash와 record count가 같은 child의 레코드를 로컬 tree에서 재사용한다.
3. 이때 [`MerkleTree::records_under`](../../crates/deltaweave-reconcile/src/lib.rs)를 각 일치 child에 호출한다. 기준 구현은 비어 있지 않은 prefix마다 `self.records.iter().filter(...)`로 전체 `BTreeMap`을 순회한다.
4. 복원된 remote snapshot은 record count와 재구성한 Merkle root로 다시 검증된다. 이후 merge, 필요한 파일의 staging 및 적용, 양쪽의 독립적인 최종 scan과 root 검증이 이어진다.

전체 레코드 수를 `N`, 한 번 반환하는 subtree의 레코드 수를 `K`라고 하자. 기준 `records_under`의 조회 비용은 subtree가 작아도 `O(N)`이다. root 바로 아래에 파일 `N`개가 있고 하나만 달라진 경우, 약 `N-1`개의 일치 child가 각각 전체 map을 순회하므로 레코드 재사용 부분에 `O(N²)` 비교가 발생한다. 네트워크 node query가 root와 변경 파일의 2회로 줄어드는 것과 로컬 조회 비용은 별개다.

변경한 구현은 검증된 exact prefix의 레코드를 한 번 조회하고, `prefix + "/"`부터 시작하는 별도 ordered range에서 descendant만 가져온다. `WirePath`에 `Borrow<str>`를 구현하면, 저장되는 경로의 validation을 유지하면서 문자열을 map 조회 경계로 사용할 수 있다. 기존 `WirePath`의 정렬과 해시는 내부 `String`을 따르므로 borrowed `str`의 정렬·해시와 일치한다. 이 방식의 한 subtree 조회 비용은 `O(log N + K)`이며, flat namespace에서 일치 leaf들을 재사용하는 비용은 `O(N log N)` 수준이 된다. tree 생성, 복원 snapshot 생성과 검증 등의 비용은 여전히 남는다.

exact prefix와 descendant range를 분리해야 한다. 예를 들어 `docs`, `docs-old`, `docs.txt`, `docs/child`가 함께 있으면, exact `docs`에서 시작해 처음 subtree 바깥 레코드를 만났을 때 중단하는 구현은 `docs-old` 또는 `docs.txt` 때문에 실제 descendant를 놓칠 수 있다. 별도로 `docs/`에서 range를 시작하면 이 문제를 피할 수 있다. 조회 경계 문자열 자체는 저장할 `WirePath`가 아니므로, trailing slash가 붙은 임시 경계로 새 경로를 생성하거나 validation을 완화할 필요가 없다.

코드와 fixture 준비 방식은 독립 검토를 통과했다. 변경 없는 root 전체 조회는 원래부터 레코드를 한 번 복제하므로, 이 최적화의 직접 대상이 아니다. 무변경 측정은 회귀를 확인하는 대조군으로 사용한다.

## 측정하는 동작

[`sparse_sync` example](../../crates/deltaweave-sync/examples/sparse_sync.rs)은 실제 `SyncEngine::sync_once` 전체 호출을 측정한다. CLI 프로세스 시작이나 CLI 인자 처리는 측정하지 않는다. server와 client는 같은 프로세스에서 실행하며 Tokio worker thread는 4개다.

fixture는 새로운 임시 디렉터리 안의 두 root와 분리된 두 private state 디렉터리로 구성된다. 기존 사용자 파일이나 인덱스를 사용하지 않는다. 두 replica에는 서로 다른 고정 test-only identity를 사용하며, server는 `127.0.0.1:0`, `DirectOnly`, client key에 대한 `AllowListed` 정책으로 실행한다. 해당 test key는 파일에 저장하지 않는다.

기본 `/tmp`가 tmpfs임을 확인했으므로, 최종 전후 비교는 양쪽 실행 모두 `TMPDIR="$PWD/target/performance/fixtures"`를 지정한다. 이 경로는 ext4 filesystem의 `/dev/mapper/ubuntu--vg-ubuntu--lv`에 있으며, 각 실행은 그 아래에 새 임시 fixture를 만든다. 기본 `/tmp`를 사용했던 결과는 탐색용 baseline으로만 보관하고 ext4 최종 비교와 섞지 않는다.

모든 파일의 크기는 1 KiB이며, 파일 번호와 revision으로부터 결정적으로 내용을 생성한다. layout별 경로는 다음과 같다.

| layout | 경로 구조 | 측정 의도 |
| --- | --- | --- |
| `flat` | root 바로 아래 `f00000000.bin` 형태의 파일들 | 일치 leaf마다 전체 map을 읽는 비용이 크게 나타나는 경우 |
| `nested` | `group0000/f00000000.bin` 형태, group당 최대 128개 파일 | subtree가 묶여 있을 때의 개선 범위와 대조 |

최종 전체 sync 비교는 `flat` 1,000파일, `flat` 4,000파일, `nested` 4,000파일의 세 설정을 사용한다. 각 설정의 before와 after를 연속 실행하여 비교 시점의 차이를 줄인다.

remote와 local을 각각 `LocalIndex::scan`으로 완전히 scan하여 실제 파일을 hash하고, 양쪽의 모든 portable record가 `SyncRecord::same_state` 기준으로 같은지 비교한다. 이 비교는 version vector를 제외한 경로, 종류, 크기, 내용 hash, readonly와 tombstone을 확인한다. 각 replica의 identity는 서로 다르게 유지된다.

같은 과거 동기화 상태를 구성하기 위해 local index를 닫고, fixture 전용 helper가 하나의 offline redb transaction에서 local `PathRecord.version`만 remote의 canonical version으로 맞춘다. helper는 `deltaweave-index`의 private record table 이름에 의존하며, 그 이름은 example의 `FIXTURE_RECORDS` 상수 한 곳에만 둔다. 기존 table이 없거나, record 수·key·직전에 scan한 record와 DB 내용이 다르면 실패한다. 나머지 `PathRecord` 필드와 metadata, root·replica binding, global local counter, schema, retry table은 수정하지 않는다. remote 초기 version에는 client replica의 counter가 없음을 검사하므로, 유지하는 local global counter는 incoming client counter 0 이상이다.

이 offline helper는 새 `TempDir` fixture를 준비할 때만 호출되며 production API에 추가되지 않는다. DB를 닫은 뒤 같은 local root와 replica로 정상 `LocalIndex`를 다시 열어 전체 snapshot이 remote와 정확히 같은지 확인한다. 이어서 authoritative local scan을 한 번 더 실행해 모든 파일이 hash되고, issue·collision·retry·변경이 없으며, version을 포함한 전체 record가 remote와 여전히 같은지 검사한다. 측정되는 sync는 정상 production 경로를 그대로 사용한다.

이 seed 과정은 초기 `N`개 파일 전송을 피하기 위한 준비 단계다. 공개 API `adopt_verified_record`를 record마다 호출하면 인덱스 전체를 반복해서 읽고 쓰는 `O(N²)` setup 비용이 발생하므로, 비교에 사용하는 harness는 두 scan과 단일 fixture transaction으로 구성한다. setup 시간은 CSV의 `setup,seed` 행에 별도로 기록하며, sync 성능 통계에 포함하지 않는다. `redb`와 `postcard`는 이 example의 fixture 준비를 위해 sync crate의 dev-dependency에만 명시한다. 두 package는 기존 workspace와 lockfile에 이미 포함되어 있으며, 새로운 package나 production dependency를 추가하지 않는다.

최초 per-record adoption harness로 실행한 pilot과 중단된 4,000파일 실행은 변경된 harness의 최종 전후 비교에 섞지 않는다. 최종 비교용 baseline은 production 변경 전 구현에 수정된 동일 harness를 적용하여 다시 build한다.

실행 순서는 다음과 같다.

1. seed 후 초기 무변경 sync를 한 번 실행한다.
2. warmup으로 remote의 한 파일을 변경한 sync와 곧바로 이어지는 무변경 sync를 한 쌍 실행한다.
3. 측정 sample마다 같은 remote 파일을 새 revision의 1 KiB 내용으로 변경한 뒤 edit sync를 실행한다. 이어서 noop sync를 실행한다. 기본 비교에서는 7쌍을 사용한다.
4. 모든 sample이 끝나면 두 root의 실제 파일 수와 모든 bytes를 기대값과 대조한다. 인덱스나 CAS에 의존하지 않고 각 root를 다시 읽어 경로·파일 내용의 digest를 계산하고 비교한다. 이 검증 시간도 sync 시간과 분리한다.

각 revision은 다른 bytes를 사용하므로 과거에 사용한 변경 내용을 번갈아 쓰면서 CAS 재사용량이 달라지는 것을 피한다. edit sync는 local action 1회, remote action 0회, pull payload 1,024 bytes, push payload 0 bytes, conflict 0개를 검사한다. Merkle query는 `flat`에서 2회, `nested`에서 3회여야 한다. noop sync는 action과 payload가 모두 0이고 query는 1회여야 한다. 매 호출의 desired root, verified local root, verified remote root가 모두 같은지도 검사한다. 최종 전후 실행에서 이 조건을 모두 통과했다.

경과 시간의 시작과 끝은 `SyncEngine::sync_once` 호출 바로 앞뒤에 있다. 여기에는 로컬과 remote의 전체 scan, Merkle 복원과 검증, merge, staging, 해당 파일의 전송과 CAS 검증, 적용, 최종 수렴 검증, 해당 sync의 client endpoint 생성·정리가 포함된다. fixture 변경, 자원 측정용 `/proc` 읽기, CSV 출력과 반환값 assertion은 경과 시간에서 제외한다.

## 자원 지표와 해석의 한계

CSV의 `sample` 행을 layout, file count, operation별로 묶고 경과 시간과 CPU 시간의 중앙값·최솟값·최댓값을 비교한다. 초기 실행, warmup, seed, 최종 filesystem 검증 행을 sample 통계에 합치지 않는다.

- `cpu_ms`는 Linux `/proc/self/stat`의 `utime + stime` 차이다. `getconf CLK_TCK`는 측정 전에 한 번 조회하며, 이 환경에서 확인한 값은 100이므로 CPU 시간 해상도는 10 ms다. client와 server를 포함한 전체 프로세스 CPU 시간이고 clock tick 단위로 양자화된다. CPU 측정 구간에는 작은 `/proc` 읽기 비용이 포함되며, 매우 짧은 sample의 CPU 차이는 정밀하게 구분할 수 없다.
- `rss_kib`는 해당 sample 직후 `/proc/self/status`의 `VmRSS`다. sync 실행 중의 최대 RSS를 의미하지 않는다.
- `process_hwm_kib`는 같은 파일의 `VmHWM`으로, seed를 포함한 프로세스 전체 수명의 최대 RSS다. setup의 메모리 사용이 높으면 이후 sample의 고유 peak를 이 지표에서 분리할 수 없다.
- Linux 측정 파일 또는 clock 정보가 없으면 자원 지표는 비어 있을 수 있다. wall time 및 동작 검증과 구분해서 해석한다.
- fixture 생성, 양쪽 seed scan, offline version 정렬 후 local 재검증 scan과 warmup을 거친 상태에서 측정한다. OS page cache를 비우지 않으며 인덱스와 파일은 이미 읽힌 상태다. 이는 cache가 준비된 반복 sync 측정이고, 실제 저장장치에서 처음 읽는 cold-cache 성능을 주장하지 않는다.
- 최종 fixture의 ext4 지정은 tmpfs 탐색 결과와 저장 위치를 구분하기 위한 조건이다. ext4에서도 OS·저장장치 cache는 비우지 않으며, 실제 디스크의 cold I/O 성능을 측정했다는 뜻은 아니다.
- 이 fixture는 작은 파일 다수 중 한 파일의 변경을 다룬다. 대용량 단일 파일의 delta transfer, 최초 전체 복제, 광역망 지연, NAS 저장장치 성능을 대표하지 않는다.

## 기록된 실행 환경

아래 값은 로컬 `target/performance/environment.json`의 `2026-09-05T20:45:33Z` 기록이다.

| 항목 | 값 |
| --- | --- |
| OS | Linux 7.0.0-31-generic, x86_64, glibc 2.43 |
| 가상 환경 | KVM shared host |
| 노출된 CPU 모델 | Intel Xeon E312xx (Sandy Bridge) |
| 논리 CPU 수 | 20 |
| benchmark CPU affinity | CPU 12–15 |
| rustc | 1.91.0 (`f8297e351`, 2025-10-28) |
| Cargo | 1.91.0 (`ea2d97820`, 2025-10-10) |
| release profile | `codegen-units = 1`, `lto = "thin"`, `strip = "symbols"` |
| 기록 시 메모리 | `MemTotal = 51114364 kB`, `MemAvailable = 46343464 kB` |
| 기록 시 load average | 26.7890625 / 17.99462890625 / 7.99951171875 |
| `perf_event_paranoid` | 4 |

추가 확인에서 `findmnt`로 기본 `/tmp`가 tmpfs이고 `target/performance/fixtures`가 ext4의 `/dev/mapper/ubuntu--vg-ubuntu--lv`에 있음을 확인했다. `getconf CLK_TCK`는 100이다. 최종 sync 비교에는 ext4 경로를 명시적으로 사용한다.

다른 worktree에서 독립적인 build 부하가 관측됐다. 해당 작업을 중단하지 않으며 shared host의 외부 부하까지 통제할 수 없다. 자체 build와 test가 모두 종료된 뒤 benchmark를 실행하고, baseline과 변경 후 실행 모두 동일하게 CPU 12–15에 고정한다. affinity만으로 동일 CPU의 다른 작업이나 shared host 자원 경합이 제거되지는 않는다. 오래 전에 측정한 탐색용 baseline과 변경 후 결과만 비교하지 않고, 보관한 baseline 실행 파일과 변경 후 실행 파일을 각 설정에서 before, after 순으로 연속 실행한다.

## 재현 명령

production 변경 전 source에 최종 benchmark example을 둔 상태에서 다음 명령으로 baseline 실행 파일을 만든다. `target/performance`는 실행 결과를 저장하는 로컬 build 디렉터리다. 이미 보관한 baseline 실행 파일을 변경 후 binary로 덮어쓰지 않는다.

```bash
cargo build --release --locked --workspace --examples
mkdir -p target/performance/fixtures
cp target/release/examples/sparse_sync target/performance/sparse-sync-before
cp target/release/examples/subtree_lookup target/performance/subtree-lookup-before
```

production 변경 후 같은 toolchain, release profile과 harness로 build하고 실행 파일을 보관한다.

```bash
cargo build --release --locked --workspace --examples
cp target/release/examples/sparse_sync target/performance/sparse-sync-after
cp target/release/examples/subtree_lookup target/performance/subtree-lookup-after
```

자체 build와 필요한 test가 모두 종료된 뒤, 세 설정 각각에 대해 before와 after를 연속 실행한다. `TMPDIR`를 모든 sync 실행에 동일하게 지정하며, 파일마다 새로운 ext4 fixture를 사용하는 독립 실행 결과가 저장된다.

```bash
mkdir -p target/performance/fixtures
TMPDIR="$PWD/target/performance/fixtures" taskset -c 12-15 target/performance/sparse-sync-before 1000 7 flat 1 > target/performance/sync-before-flat-1000.csv
TMPDIR="$PWD/target/performance/fixtures" taskset -c 12-15 target/performance/sparse-sync-after 1000 7 flat 1 > target/performance/sync-after-flat-1000.csv
TMPDIR="$PWD/target/performance/fixtures" taskset -c 12-15 target/performance/sparse-sync-before 4000 7 flat 1 > target/performance/sync-before-flat-4000.csv
TMPDIR="$PWD/target/performance/fixtures" taskset -c 12-15 target/performance/sparse-sync-after 4000 7 flat 1 > target/performance/sync-after-flat-4000.csv
TMPDIR="$PWD/target/performance/fixtures" taskset -c 12-15 target/performance/sparse-sync-before 4000 7 nested 1 > target/performance/sync-before-nested-4000.csv
TMPDIR="$PWD/target/performance/fixtures" taskset -c 12-15 target/performance/sparse-sync-after 4000 7 nested 1 > target/performance/sync-after-nested-4000.csv
python3 scripts/summarize-performance.py --compare target/performance/sync-before-flat-1000.csv target/performance/sync-after-flat-1000.csv
python3 scripts/summarize-performance.py --compare target/performance/sync-before-flat-4000.csv target/performance/sync-after-flat-4000.csv
python3 scripts/summarize-performance.py --compare target/performance/sync-before-nested-4000.csv target/performance/sync-after-nested-4000.csv
```

호출 비용의 원인을 분리하는 [`subtree_lookup` example](../../crates/deltaweave-reconcile/examples/subtree_lookup.rs)은 메모리에 구성한 flat Merkle tree에서 변경 경로 하나를 제외한 모든 leaf의 `records_under` 호출과 반환값 assertion을 측정한다. 전체 sync의 대체 지표로 사용하지 않으며, 일치 child 재사용 부분의 변화만 확인한다. 각 크기에서 warmup 1회와 sample 7회를 사용한다.

```bash
taskset -c 12-15 target/performance/subtree-lookup-before 1000 7 > target/performance/lookup-before-1000.csv
taskset -c 12-15 target/performance/subtree-lookup-after 1000 7 > target/performance/lookup-after-1000.csv
taskset -c 12-15 target/performance/subtree-lookup-before 4000 7 > target/performance/lookup-before-4000.csv
taskset -c 12-15 target/performance/subtree-lookup-after 4000 7 > target/performance/lookup-after-4000.csv
python3 scripts/summarize-performance.py --compare target/performance/lookup-before-1000.csv target/performance/lookup-after-1000.csv
python3 scripts/summarize-performance.py --compare target/performance/lookup-before-4000.csv target/performance/lookup-after-4000.csv
```

[`summarize-performance.py`](../../scripts/summarize-performance.py)는 Python 표준 라이브러리만 사용한다. `--compare BEFORE AFTER`는 시간·메모리 자원 지표를 제외한 모든 CSV 출력의 일치를 검사하며, 여기에는 sample 식별자, action/query/payload, Merkle root와 마지막 독립 filesystem digest가 포함된다. sync CSV에는 완료된 최종 filesystem 검증 행과 측정 행의 수렴·conflict 조건도 요구한다. 비교 조건이 맞으면 지표별 중앙값과 최솟값·최댓값, 중앙값 차이와 변화율을 출력하고, 조건이 다르면 오류로 종료한다. 각 실행의 exit status도 확인한다. 고정 test identity와 결정적 fixture는 같은 설정의 baseline과 변경 후 root를 직접 비교할 수 있게 한다.

## 보존한 동작과 검사

변경 범위는 ordered map에서 subtree record를 찾는 방법이다. 아래 조건을 구현 검토, 경로 회귀 검사와 전체 동기화 실행으로 확인했다.

- 빈 prefix는 전체 snapshot을 canonical 순서로 반환한다.
- 비어 있지 않은 prefix는 기존 `WirePath::new` 검증을 통과해야 한다. traversal, 절대 경로, trailing slash 등 기존 거부 조건을 유지한다.
- exact path와 모든 descendant를 포함하고, 비슷한 이름의 sibling은 포함하지 않는다. exact record가 없는 implicit directory도 처리한다.
- Unicode, 최대 허용 길이의 경로, tombstone, 비어 있는 tree와 존재하지 않는 prefix를 처리한다.
- 반환 record의 bytes, version vector, tombstone 상태와 canonical 순서는 기존 선형 filter 결과와 같다.
- `Borrow<str>`는 조회를 위한 borrowed view이며 record 생성·역직렬화 validation, wire schema, 저장 schema 또는 hashing 규칙을 바꾸지 않는다.
- remote snapshot의 record count와 재구성 root 검증, 파일·청크 hash 확인, conflict 보존, causal precondition과 최종 양쪽 scan을 유지한다.

실행한 품질 검사와 결과는 다음과 같다. 전체 로그는 `target/performance/`에 있으며, [검증 요약](data/2026-09-05-sparse-sync/verification.txt)과 [CLI 자체 검사 JSON](data/2026-09-05-sparse-sync/self-test.json)을 함께 보관했다.

| 실제 실행 명령 | 결과 |
| --- | --- |
| `cargo build --release --locked --workspace --all-features --bins --examples` | 통과, 최적화된 전체 workspace 및 benchmark build |
| `cargo test --locked --workspace --all-targets --all-features` | 107 passed, 0 failed, 0 ignored |
| `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` | 통과 |
| `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --all-features` | 통과 |
| `cargo fmt --all -- --check` | 통과 |
| `target/release/deltaweave self-test` | `status: pass`, 양방향·충돌 내용 보존·삭제·재시작 0 action 확인 |
| `git diff --check` | 통과 |

변경 전에는 `cargo test --release --locked -p deltaweave-core -p deltaweave-reconcile --lib`로 새 경로 경계 검사를 포함한 11+15개 검사가 기존 동작을 통과하는지 확인했다. 시간 임계값을 unit test에 넣는 대신 동일 harness의 변경 전·후 실측으로 성능 문제를 검증했다. 최종 전체 검사에는 비허용 peer 거부, 경로 validation, chunk/file 무결성, 충돌·삭제·재시작 수렴 및 실제 프로세스 중단을 동반한 fault 검사 3개가 포함된다. 새롭게 발생한 실패나 경고는 관측되지 않았다. Windows 전용 검사는 Linux에서 실행되지 않는다.

## 최종 측정값

아래 전후 값은 **ext4 최종 실행만** 사용한다. 시간은 ms, 표기는 **중앙값 [최솟값, 최댓값]**, 변화율은 `(후/전-1)×100`이다. 각 행은 버전별 warmup 1회 후 **7개 표본**이다. 적은 표본으로 p95/p99를 추정하지 않았다. [`measurement-environment.jsonl`](data/2026-09-05-sparse-sync/measurement-environment.jsonl)에 따르면 설정별 before→after를 2026-09-05 21:18–21:20 UTC에 연속 실행했고, 당시 1분 load average는 약 5.4–6.2였다. 자체 build/test 부하는 없었다.

### 전체 동기화 경과 시간

| 시나리오 | 변경 전 (ms) | 변경 후 (ms) | 차이 (ms) | 변화율 | n/버전 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 평면 1,000개, 1개 변경 | 393.309 [371.562, 440.062] | 383.522 [365.974, 407.650] | -9.787 | -2.49% | 7 |
| 평면 1,000개, 무변경 | 285.416 [264.441, 303.971] | 283.738 [274.981, 309.353] | -1.678 | -0.59% | 7 |
| 평면 4,000개, 1개 변경 | 1,606.886 [1,560.616, 1,721.091] | 1,373.449 [1,309.944, 1,393.486] | -233.437 | -14.53% | 7 |
| 평면 4,000개, 무변경 | 1,053.021 [1,000.703, 1,121.655] | 1,045.265 [1,035.793, 1,091.814] | -7.756 | -0.74% | 7 |
| 하위 폴더 4,000개, 1개 변경 | 1,433.528 [1,401.343, 1,509.899] | 1,399.943 [1,385.109, 1,456.515] | -33.585 | -2.34% | 7 |
| 하위 폴더 4,000개, 무변경 | 1,118.977 [1,099.847, 1,144.416] | 1,117.151 [1,099.243, 1,146.759] | -1.826 | -0.16% | 7 |

### client와 server를 합한 CPU 시간

| 시나리오 | 변경 전 (ms) | 변경 후 (ms) | 차이 (ms) | 변화율 | n/버전 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 평면 1,000개, 1개 변경 | 870.000 [790.000, 930.000] | 870.000 [820.000, 910.000] | +0.000 | +0.00% | 7 |
| 평면 1,000개, 무변경 | 670.000 [610.000, 740.000] | 680.000 [660.000, 750.000] | +10.000 | +1.49% | 7 |
| 평면 4,000개, 1개 변경 | 3,420.000 [3,250.000, 3,700.000] | 3,240.000 [3,040.000, 3,390.000] | -180.000 | -5.26% | 7 |
| 평면 4,000개, 무변경 | 2,500.000 [2,350.000, 2,630.000] | 2,500.000 [2,460.000, 2,630.000] | +0.000 | +0.00% | 7 |
| 하위 폴더 4,000개, 1개 변경 | 3,260.000 [3,110.000, 3,400.000] | 3,180.000 [3,150.000, 3,230.000] | -80.000 | -2.45% | 7 |
| 하위 폴더 4,000개, 무변경 | 2,600.000 [2,530.000, 2,680.000] | 2,560.000 [2,490.000, 2,650.000] | -40.000 | -1.54% | 7 |

평면 4,000개 변경 시 전후 경과 시간 범위가 분리되었고, 무변경 중앙값 차이는 −0.74%였다. 따라서 주 시나리오의 전체 처리 시간 개선을 확인한다. 같은 시나리오의 CPU 중앙값은 5.26% 감소했지만 범위가 겹치며 10 ms 단위 측정이므로 보장된 개선율로 해석하지 않는다. 평면 1,000개와 하위 폴더 대조군의 작은 차이 역시 변동 범위 안에 있다.

### 메모리

`rss_kib`의 모든 표본은 원시 CSV에 있다. 아래는 준비·초기·warmup·측정·마지막 내용 검사에서 읽은 **프로세스 VmHWM의 최댓값**이다. 조건·버전별 프로세스는 1개이고 sync 호출은 17회다. 시간 표의 독립 표본 7개와 같은 의미의 메모리 표본 수가 아니다.

| 시나리오 | 전 (KiB) | 후 (KiB) | 차이 (KiB) | 변화율 |
| --- | ---: | ---: | ---: | ---: |
| 평면 1,000개 | 50,516 | 51,076 | +560 | +1.11% |
| 평면 4,000개 | 102,368 | 103,040 | +672 | +0.66% |
| 하위 폴더 4,000개 | 99,796 | 97,400 | -2,396 | -2.40% |

평면 4,000개의 sample 직후 RSS 중앙값은 100,880→98,784 KiB였지만 기록된 최대값은 672 KiB 증가했다. 메모리 감소는 확인되지 않았다. 변화가 작은 단일 프로세스 비교이며, 새 캐시·동시성·지속 보관 자료구조를 추가하지 않았다. 장시간 운용의 메모리 상한이나 누수 부재를 이 측정만으로 보장하지 않는다.

### 원인 확인용 subtree 조회

이 보조 측정은 `N-1`개의 일치 leaf를 조회하는 반복만 다룬다. 실제 반환 record 동등성 assertion, 복제·해제도 포함한다. 네트워크나 전체 동기화 개선율로 확대하지 않는다.

| 레코드/조회 수 | 전 (ms) | 후 (ms) | 차이 (ms) | 변화율 | n/버전 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1,000/999 | 15.807 [15.431, 16.987] | 0.978 [0.969, 1.018] | -14.829 | -93.81% | 7 |
| 4,000/3,999 | 306.507 [300.879, 318.171] | 4.201 [4.141, 4.589] | -302.307 | -98.63% | 7 |

4,000개에서 약 302 ms의 반복 조회 비용이 사라진 결과는 전체 sync의 약 233 ms 단축과 방향이 일치한다. 서로 다른 실행 구간의 수치이므로 정확한 비용 분해로 보지는 않는다.

### 초기 실행과 정확성

CSV의 `initial`은 fixture를 검증한 뒤 첫 무변경 sync다. OS cache가 이미 준비되어 있으므로 cold-disk 실행이 아니다. 전후 각 1회 관측값이며, warm sample 통계에서 제외했다.

| 시나리오 | 첫 호출 전 (ms) | 첫 호출 후 (ms) | n/버전 |
| --- | ---: | ---: | ---: |
| 평면 1,000개 | 277.931 | 282.332 | 1 |
| 평면 4,000개 | 1,149.763 | 1,096.379 | 1 |
| 하위 폴더 4,000개 | 1,254.099 | 1,327.486 | 1 |

측정 sync는 전후 각각 42회, 합계 **84회에서 오류 0회**다. 초기·warmup을 포함한 총 **102회**도 모두 통과했다. 같은 설정의 전후 CSV에서 시간·메모리 외 모든 필드가 일치했다. 변경은 local action 1, remote action 0, payload 1,024 bytes, Merkle query 2회(flat)/3회(nested)를 유지했다. 무변경은 action/payload 0, query 1회다. 각 프로세스의 마지막 독립적인 전체 filesystem 검증 6회도 통과했고 전후 digest가 같다.

## 기록과 재현할 source

[원시 결과 디렉터리](data/2026-09-05-sparse-sync/)에는 최종 CSV 10개, sync 비교 결과 3개, 환경 기록, source/실행 파일 SHA256 및 검증 요약이 있다. 대표 전후 원본은 [before](data/2026-09-05-sparse-sync/sync-before-flat-4000.csv)와 [after](data/2026-09-05-sparse-sync/sync-after-flat-4000.csv)다. 모든 결과는 실측이며 tmpfs 탐색 결과와 중단한 최초 pilot을 최종 통계에서 제외했다.

기준 executable의 core/reconcile source는 `git show 75ffab7:<path>`와 SHA256이 정확히 같다. workspace `Cargo.toml`·`Cargo.lock`과 두 example의 SHA256도 전후 동일하다. 실제 작업에서는 해당 두 source만 원본으로 복원하여 baseline을 build·보관한 뒤 최적화 source를 복원했다. 현재 최적화 source를 그대로 build한 파일을 baseline으로 이름만 바꾸어 비교해서는 안 된다.

새로 재현할 때는 별도의 detached worktree를 `75ffab7`에 만들고, 현재 `Cargo.lock`, `crates/deltaweave-sync/Cargo.toml` 및 두 example 파일만 복사하여 앞의 baseline build 명령을 실행한다. core/reconcile은 그 worktree의 원본을 유지한다. 최적화 binary는 현재 worktree에서 build한다. 이는 새 재현 방법이며, 이번 실제 수행 기록은 보관된 source SHA256·명령·CSV다. 어떤 경우에도 사용자의 기존 작업을 덮어쓰지 않는다.

## 한계와 남은 비용

- full authoritative scan, 매 파일의 해시 읽기 버퍼 할당, snapshot 전체 구성·검증 및 action별 수신 측 재스캔은 남아 있다. 이 비용을 추가로 최적화하지 않았다.
- 공유 KVM host의 외부 부하는 완전히 통제할 수 없다. 같은 CPU affinity와 가까운 시간의 전후 실행·무변경 대조군으로 영향을 점검했다. 중앙값과 범위를 제시했으며 통계적 신뢰구간이나 개선율 보장은 제공하지 않는다.
- `perf stat -e task-clock -- true`는 `perf_event_paranoid=4` 때문에 실패했다. 권한을 바꾸지 않았으며, sampled CPU profile 대신 코드의 실제 순회 구조와 보조/전체 실행 측정을 근거로 사용했다.
- Windows·실물 Synology/NAS·WAN·cold-disk·최초 전체 복제·대용량 단일 파일·장시간 soak는 실행하지 않았다. 해당 OS/장비와 통제된 cache·장기 부하 환경에서 후속 검증이 필요하다.
- CLI 인자 처리·JSON 출력과 프로세스 시작 시간은 이번 전체 엔진 측정 범위에 포함되지 않는다. product 기능, wire/on-disk schema, 권한, 무결성 검사, CLI 출력·접근성 동작은 변경하지 않았다.

정한 주 시나리오의 측정 개선과 관련 정확성 검증을 완료했으므로 이번 작업은 여기서 마친다. 추가 최적화나 운영 배포는 수행하지 않았다.
