# `deltaweave-reconcile` 동시성 충돌 로직 및 Merkle 트리 분석

## 1. 목적과 범위

이 문서는 `crates/deltaweave-reconcile/src/lib.rs`의 실제 구현을 기준으로 다음을 분석한다.

- `SyncRecord`와 버전 벡터를 이용한 경로별 인과관계 판정
- 동시 편집과 비정상적인 동일 시계 충돌의 결정적 해소
- 충돌 파일 보존 경로와 병합 결과의 수렴 성질
- portable path component trie 형태의 Merkle 트리 구성
- 변경된 subtree만 조회하는 네트워크 snapshot 복원
- 원하는 canonical namespace까지의 idempotent action 계산
- 현재 구현이 보장하는 범위와 운영상 한계

여기서 **canonical state**는 두 snapshot의 입력 순서와 무관하게 계산되는 하나의 목표 namespace를 뜻한다. `deltaweave-reconcile`은 상태 비교·병합·action 계획을 담당하고, 실제 scan·전송·filesystem 반영·최종 검증은 각각 `deltaweave-index`, `deltaweave-net`, `deltaweave-sync`가 담당한다.

## 2. 핵심 데이터 모델

### 2.1 `SyncRecord`: 경로 하나의 논리 상태

각 동기화 경로는 `SyncRecord` 하나로 표현된다.

| 필드 | 의미 | 병합/해시에서의 역할 |
| --- | --- | --- |
| `schema_version` | record 형식 버전 | 입력 validation에 사용 |
| `path` | `/`를 구분자로 쓰는 portable 상대 경로 | 병합 key이며 `logical_hash`에 포함 |
| `kind` | file, directory, symlink, other | 동일 상태 판정과 승자 선택에 포함 |
| `size` | live file의 논리 크기 | 동일 상태 판정과 승자 선택에 포함 |
| `content_hash` | live file 전체의 BLAKE3 | content identity 및 충돌 판정에 포함 |
| `readonly` | materialize할 read-only 상태 | 동일 상태 판정과 승자 선택에 포함 |
| `version` | replica별 logical counter | 인과관계와 `logical_hash`에 포함 |
| `tombstone` | durable deletion marker | 삭제 전파 및 충돌 판정에 포함 |

`same_state`는 `path`, `kind`, `size`, `content_hash`, `readonly`, `tombstone`을 비교하고 version vector는 무시한다. 반대로 `logical_hash`는 경로, 상태, version vector 전체를 domain-separated BLAKE3로 해시한다. 따라서 내용과 metadata가 같아도 causal knowledge가 다르면 Merkle leaf의 identity는 다르다.

Tombstone은 snapshot에서 제거되지 않는 versioned record다. 이 때문에 삭제도 Merkle 비교와 인과 병합에 참여하며, 이전 live record보다 causal하게 뒤인 tombstone은 정상 업데이트와 같은 방식으로 승리한다.

### 2.2 버전 벡터의 부분 순서

`VersionVector`는 `ReplicaId -> u64`의 정렬 map이다. 누락된 replica counter는 0으로 취급한다. 두 벡터 `L`, `R`에 대해 모든 replica의 counter를 비교해 관계를 계산한다.

- 모든 counter가 같음: `Equal`
- `L`이 하나 이상 작고 큰 counter는 없음: `Before`
- `L`이 하나 이상 크고 작은 counter는 없음: `After`
- 작은 counter와 큰 counter가 모두 존재: `Concurrent`

예를 들어 공통 상태 `{A:1}`에서 A와 B가 독립적으로 편집하면 각각 `{A:2}`와 `{A:1,B:1}`이 된다. 어느 쪽도 다른 쪽을 포함하지 않으므로 `Concurrent`다. wall clock이나 파일 수정 시각은 이 판단에 쓰지 않는다.

## 3. `merge_snapshots` 처리 흐름

### 3.1 snapshot 단위 fast path와 경로 순회

두 Merkle root와 record 수가 모두 같으면 왼쪽 records를 그대로 반환하고 모든 경로를 `equal`로 집계한다. root만 비교하지 않고 cardinality도 함께 비교한다.

서로 다르면 양쪽 `BTreeMap` key의 합집합을 `BTreeSet`으로 만들고 portable path 순서로 각 경로를 처리한다. 한쪽에만 있는 record는 그쪽 상태를 선택한다. 양쪽에 모두 있으면 version vector 관계와 `same_state` 결과로 다음과 같이 분기한다.

| 인과관계 | 논리 상태 | 결과 | 충돌 기록 |
| --- | --- | --- | --- |
| `Before` | 무관 | 오른쪽 최신 record 선택 | 없음 |
| `After` | 무관 | 왼쪽 최신 record 선택 | 없음 |
| `Equal` | 같음 | 왼쪽 record 선택 | 없음 |
| `Concurrent` | 같음 | 상태는 하나만 유지하고 두 vector의 replica별 최댓값 병합 | 없음 |
| `Equal` | 다름 | `EqualClockDivergence`로 결정적 충돌 해소 | 있음 |
| `Concurrent` | 다름 | `ConcurrentEdit`로 결정적 충돌 해소 | 있음 |

`EqualClockDivergence`는 정상 writer라면 나오지 않아야 한다. 동일한 causal version이 서로 다른 상태에 재사용되었다는 뜻이므로, 오류로 즉시 중단하는 대신 모든 peer가 같은 결과를 만들 수 있도록 충돌로 기록하고 deterministic resolution을 수행한다. 네트워크 apply 경계에서는 이런 원격 입력이 직접 적용되지 않도록 별도로 거부한다.

### 3.2 동시인데 상태가 같은 경우

두 peer가 동일한 bytes와 metadata를 갖지만 관측한 causal history가 다를 수 있다. 이 경우 conflict copy는 만들지 않는다. 왼쪽 record를 복제하고 vector의 각 counter를 최댓값으로 병합한다.

이 과정은 내용 중복을 만들지 않으면서도 두 peer의 관측 정보를 잃지 않는다. 병합된 vector는 두 입력 vector를 모두 causal하게 포함하므로 이후 동일 입력을 재병합해도 충돌하지 않는다.

### 3.3 서로 다른 상태의 결정적 승자 선택

`resolve_conflict`는 우선 두 record의 `logical_hash`와 causal history를 제외한 `state_hash`를 계산한다. `state_hash`의 입력은 `kind`, `size`, `content_hash` 존재 여부와 값, `readonly`, `tombstone`이며 경로와 version vector는 포함하지 않는다.

승자 규칙은 다음 순서다.

1. 한쪽만 live directory이면 live directory가 승리한다.
2. 그 외에는 `state_hash`가 사전식으로 더 크거나 같은 상태가 승리한다.

첫 번째 예외는 같은 경로의 file 대 directory 충돌에서 subtree를 materialize할 수 있게 한다. directory가 canonical path를 차지하고 losing file은 그 directory 아래가 아닌 동일 parent의 sibling conflict copy가 된다. 단, causal한 directory-to-file 또는 file-to-directory 전환은 이미 `Before`/`After` 분기에서 최신 상태가 이기므로 이 예외의 대상이 아니다.

일반 승자 선택에는 left/right 방향, replica 이름, wall clock이 들어가지 않는다. 따라서 두 입력을 뒤집어도 canonical winner와 conflict metadata가 같다. BLAKE3 충돌을 현실적으로 무시한다는 가정 아래 `state_hash`의 ordering이 안정적인 tie-break가 된다.

### 3.4 해소된 version vector

충돌 후에는 승자 record의 기존 version을 그대로 쓰지 않는다.

1. 양쪽 version vector를 replica별 최댓값으로 병합한다.
2. 상수 문자열 `deltaweave deterministic conflict resolver v1`에서 파생한 가상 resolver `ReplicaId`의 counter를 1 증가시킨다.
3. 이 vector를 canonical winner와 conflict copy에 동일하게 설정한다.

결과 vector는 양쪽 입력보다 causal하게 뒤에 있다. 따라서 receiver가 적용할 때 incoming record가 기존 상태를 `Before` 관계로 지배하고, 같은 충돌이 다음 동기화에서 다시 검출되는 것을 방지한다. resolver counter가 `u64::MAX`라 증가할 수 없으면 `CounterOverflow`로 병합이 실패한다.

가상 resolver ID는 모든 설치에서 동일하다. 이는 conflict resolution event를 결정적으로 표현하기 위한 logical identity이며 실제 peer identity가 아니다.

### 3.5 losing file 보존 조건

loser는 다음 조건을 모두 만족할 때만 별도 경로에 보존된다.

- tombstone이 아닌 live record
- kind가 regular file
- winner가 tombstone이거나 winner와 `content_hash`가 다름

따라서 losing directory, symlink, special object, tombstone은 conflict copy로 materialize하지 않는다. 서로 다른 metadata만 가진 동일 content file도 별도 bytes copy를 만들지 않는다. 충돌 자체는 `ConflictRecord`에 남지만 `conflict_path`는 `None`일 수 있다.

### 3.6 conflict 경로 생성

예를 들어 `docs/report.txt`의 losing record는 다음 형태로 보존된다.

```text
docs/report.conflict-<token>.txt
```

`token`은 loser의 `logical_hash`를 16진수로 표현한 값이다. 먼저 12, 16, 24, 32, 48, 64자의 prefix를 차례로 시도하고, 모두 점유되어 있으면 전체 token 뒤에 `-1`부터 `-10000`까지 붙인다. 이 순서도 결정적이다.

경로 생성 시 다음 portability 조건을 지킨다.

- 원래 parent를 유지한다.
- 마지막 `.`이 첫 문자가 아닐 때 확장자를 suffix 뒤에 유지한다.
- 전체 경로는 최대 4096 UTF-8 bytes, component는 최대 255 UTF-8 bytes다.
- 길면 stem을 UTF-8 문자 경계에서 자른다.
- 확장자까지 넣을 공간이 없으면 확장자를 생략한다.
- 생성 후 `WirePath` validation을 다시 수행한다.

충돌 경로의 점유 여부는 양쪽 snapshot에 있던 모든 원래 경로와 이번 병합에서 이미 할당한 conflict 경로를 기준으로 확인한다. 가능한 이름을 모두 소진하면 `ConflictPathExhausted`, suffix 자체를 넣을 공간이 없으면 `ConflictPathTooLong`을 반환한다.

### 3.7 `ConflictRecord`와 통계

충돌마다 다음 audit 정보가 original path 순서로 반환된다.

- canonical path
- 생성된 conflict path 또는 `None`
- 선택 전 winner/loser record의 `logical_hash`
- `ConcurrentEdit` 또는 `EqualClockDivergence`

`MergeStats`는 `equal`, `selected_left`, `selected_right`, `conflicts`, `conflict_copies`를 센다. `selected_left`와 `selected_right`는 입력 방향에 따른 진단 값이라 입력을 뒤집으면 서로 바뀔 수 있지만, `records`와 `conflicts`는 방향 독립적이다.

## 4. 충돌 시나리오별 결과

| 시나리오 | canonical path | conflict copy | 핵심 이유 |
| --- | --- | --- | --- |
| causal update 대 이전 file | 최신 update | 없음 | version vector가 지배 |
| causal tombstone 대 이전 live file | tombstone | 없음 | 삭제가 causal하게 최신 |
| concurrent 동일 file state | 동일 file + 병합된 vector | 없음 | 상태가 같아 지식만 병합 |
| concurrent 서로 다른 file bytes | 큰 `state_hash`의 file | losing file 보존 | 양쪽 bytes 보존과 결정적 수렴 |
| concurrent edit 대 delete | 결정적 winner | live file이 loser일 때만 보존 | 삭제와 편집 모두 상태로 비교 |
| concurrent live directory 대 file | directory | file을 sibling으로 보존 | descendants materialization 보장 |
| equal vector, divergent state | 결정적 winner | 조건부 보존 | writer invariant 위반을 수렴 가능한 형태로 기록 |
| 한쪽 snapshot에만 존재 | 존재하는 record | 없음 | 부재 자체는 tombstone이 아니며 충돌로 보지 않음 |

마지막 행은 중요하다. durable deletion은 반드시 tombstone으로 표현되어야 한다. 단순히 snapshot에서 record가 사라지면 병합은 다른 쪽 record를 선택하므로 삭제가 전파되지 않는다.

## 5. Merkle 트리 구조

### 5.1 canonical path trie

`MerkleTree`는 두 표현을 함께 가진다.

- `BTreeMap<WirePath, SyncRecord>`: 경로순 record 조회와 snapshot 열거
- `MerkleNode` root: path component별 trie와 subtree digest

예를 들어 `docs/a.txt`, `docs/nested/b.txt`, `photos/c.jpg`는 다음처럼 배치된다.

```text
(root)
├── docs
│   ├── a.txt              [record_hash]
│   └── nested
│       └── b.txt          [record_hash]
└── photos
    └── c.jpg              [record_hash]
```

record는 leaf에만 제한되지 않는다. `tree` record와 `tree/child.txt` record가 동시에 있으면 `tree` node는 자신의 `record_hash`와 `child.txt` subtree를 모두 가진다. 입력 record는 먼저 validation되고, 같은 `WirePath`가 두 번 나오면 `DuplicatePath`로 거부된다.

`BTreeMap`을 사용하므로 record 입력 순서와 상관없이 node와 child 순서가 고정된다. 이 canonical ordering이 같은 snapshot에서 같은 root를 만드는 기반이다.

### 5.2 node hash 계산

각 node는 children을 먼저 재귀적으로 finalize한 뒤 다음 값을 BLAKE3 derive-key context `deltaweave merkle path node v1`로 해시한다.

```text
node_hash = BLAKE3_DERIVE_KEY(
  record_present_tag ||
  record_logical_hash? ||
  for child in child_name_order {
    u64_le(name_byte_length) ||
    name_utf8 ||
    child_hash ||
    u64_le(child_record_count)
  }
)
```

- record가 있으면 tag `1`과 `SyncRecord::logical_hash`, 없으면 tag `0`을 넣는다.
- child name은 UTF-8 byte length를 먼저 넣어 경계를 명확히 한다.
- child hash뿐 아니라 child cardinality도 넣는다.
- `record_count`는 현재 node의 record 존재 여부와 모든 child count의 합이며 내부 계산은 saturating addition이다.

이 방식으로 파일 내용, metadata, tombstone, version vector 또는 path hierarchy의 변경이 ancestor를 따라 root까지 전파된다. 빈 tree도 같은 domain에서 계산된 안정적인 root hash를 가진다.

### 5.3 로컬 `different_paths`

두 tree의 node hash와 `record_count`가 모두 같으면 해당 subtree 탐색을 즉시 중단한다. 다르면 현재 node의 `record_hash`를 비교하고, 양쪽 child name 합집합을 정렬 순회한다.

- 양쪽에 child가 있으면 재귀 비교한다.
- 한쪽에만 child가 있으면 그 subtree의 모든 record path를 수집한다.
- 마지막에 path를 정렬하고 중복 제거한다.

따라서 10,000개 경로 중 하나만 바뀐 테스트에서도 결과는 그 한 경로다. 다만 이 함수의 출력 비용은 실제 차이 수와 한쪽에만 존재하는 subtree의 크기에 비례한다.

## 6. 네트워크 부분 subtree snapshot

`deltaweave-net`의 `fetch_snapshot`은 local tree를 기준으로 remote의 전체 logical snapshot을 복원하되, 일치하는 subtree의 record는 local copy를 재사용한다.

1. 빈 prefix로 remote root node를 질의한다.
2. remote는 session 시작 시 한 번 authoritative scan을 수행하고 immutable `MerkleTree`를 만든다.
3. 받은 node의 hash와 count가 local node와 같으면 `records_under(prefix)`로 local records를 복원 결과에 넣고 더 내려가지 않는다.
4. node가 다르면 그 exact prefix의 optional record를 받고, immediate children 각각을 local summary와 비교한다.
5. 같은 child는 local records를 재사용하고 다른 child prefix만 breadth-first queue에 넣는다.
6. 질의 종료 후 record 수를 remote root count와 비교한다.
7. 복원한 records로 Merkle tree를 다시 만들고 root hash를 remote가 처음 보낸 값과 비교한다.

변경이 없으면 root 한 번, 즉 `merkle_queries = 1`로 끝난다. 변경이 국소적이면 root부터 변경 경로까지의 node와 갈라지는 mismatched subtree만 원격 질의한다. 하지만 protocol response의 각 node summary에는 모든 immediate child summary가 들어가므로, 전송량은 단순히 tree depth만의 함수가 아니라 방문한 node의 fan-out에도 영향을 받는다.

### 6.1 검증과 방어 조건

부분 snapshot이 local record를 재사용해도 마지막에 전체 tree를 재구축하므로, 악의적이거나 일관되지 않은 summary 조합은 root mismatch로 검출된다. 추가 방어는 다음과 같다.

- remote snapshot 최대 1,000,000 records
- client node query 최대 1,000,000회
- query prefix의 `WirePath` validation
- 응답 prefix와 요청 prefix 일치 확인
- 중복 path 삽입 거부
- control frame 크기 제한과 `postcard` decoding validation
- remote scan의 collision, read issue, queued retry가 있으면 snapshot session 거부

Hash 비교는 cryptographic commitment이지만 remote filesystem의 진실성을 외부에서 증명하는 합의 protocol은 아니다. authenticated peer가 session 시작 시 만든 자기 snapshot이 내부적으로 일관된지를 검증하는 구조다.

## 7. action 계획과 end-to-end 수렴

`actions_to_reach(current, desired)`는 `different_paths`로 후보를 좁힌 뒤 desired tree에 존재하는 record만 action으로 만든다.

- desired record가 tombstone이면 `Delete`
- live record이면 `Materialize`
- current에 exact record가 이미 있으면 생략
- desired tree 자체를 current로 다시 넣으면 action은 0개

모든 merged record는 canonical desired tree에 남는다. 삭제도 tombstone record가 desired에 있으므로 `Delete` action이 생성된다. desired에 없는 경로에 대해서는 action을 만들지 않는데, 정상 merge는 양쪽 path 합집합을 보존하므로 물리 삭제 의미는 tombstone으로 전달된다.

`deltaweave-sync`는 이 계획을 다음 순서로 운영한다.

1. local authoritative scan과 remote 부분 snapshot을 확보한다.
2. `merge_snapshots`으로 desired tree를 계산한다.
3. 양쪽 action에 필요한 모든 live file content를 local content-addressed store에 먼저 stage한다.
4. tombstone은 깊은 경로부터, directory는 얕은 경로부터, file은 path 순서로 적용한다.
5. remote receiver는 fresh scan 후 incoming vector가 current를 causal하게 지배하거나 exact idempotent retry인 경우만 적용한다. stale, concurrent, equal-clock/different-state 입력은 거부한다.
6. local을 다시 scan하고 remote snapshot을 다시 가져온다.
7. 양쪽의 root hash와 record count가 desired tree와 같을 때만 성공한다.

선행 content staging은 canonical path overwrite나 file/directory type 전환 전에 losing bytes를 확보하기 위한 장치다. 최종 root 재검증은 action receipt만으로 성공을 선언하지 않게 한다. 적용 도중 다른 변경이 발생하면 causal precondition 또는 마지막 root 비교가 실패하며, 상위 continuous sync loop가 새 snapshot으로 다시 reconcile할 수 있다.

## 8. 결정성·수렴성·멱등성 평가

### 8.1 구현이 제공하는 성질

- **입력 순서 독립 Merkle root:** record와 child가 `BTreeMap` 순서로 처리된다.
- **병합 방향 독립 canonical records:** causal relation과 `state_hash` ordering이 left/right 이름에 의존하지 않는다.
- **충돌 후 causal dominance:** merged vector에 양쪽 knowledge와 resolver event가 들어간다.
- **losing file 보존:** 서로 다른 bytes의 live losing file은 portable conflict path로 유지된다.
- **idempotent 재실행:** canonical tree를 자기 자신과 병합하면 새 충돌이 없고 action도 없다.
- **삭제의 분산 비교 참여:** tombstone이 record와 Merkle leaf로 유지된다.
- **부분 비교 후 전체 commitment 검증:** remote reconstruction 결과를 root와 count로 다시 검증한다.

테스트는 입력 순서 독립 root, 변경 경로 정밀 diff, 10,000경로 중 단일 변경, causal update/delete, concurrent identical state, divergent file conflict, file/directory 충돌, 긴 Unicode conflict name, action idempotence, 세 peer partition 모델의 최종 root 일치를 다룬다.

### 8.2 성질의 전제와 한계

- **BLAKE3 충돌 저항성 전제:** root와 winner tie-break의 유일성은 hash collision이 실질적으로 발생하지 않는다는 전제에 의존한다.
- **현재 orchestrator는 2-peer:** merge model의 세 peer 테스트는 있지만 production membership과 multi-peer protocol은 없다.
- **tombstone garbage collection 부재:** tombstone acknowledgment와 안전한 제거 정책이 없어 metadata가 계속 남을 수 있다.
- **snapshot isolation은 session 단위:** remote query session은 immutable tree를 사용하지만 이후 pull/apply 사이 실제 파일 변경은 별도 exact-record 검사와 causal precondition으로 실패 처리한다. 전체 pass가 하나의 분산 transaction은 아니다.
- **부재는 삭제가 아님:** 삭제 history가 tombstone으로 유지되지 않으면 남아 있는 record가 복원된다.
- **일부 object kind는 보존 copy 대상이 아님:** losing regular file만 conflict copy가 가능하고 symlink/special object materialization도 상위 계층에서 허용되지 않는다.
- **metadata 범위가 제한적:** directory mtime과 directory readonly는 portable state에서 정규화되며 regular-file readonly만 유지된다.
- **충돌 이름 namespace가 유한:** hash prefix와 10,000개 serial을 모두 점유하면 병합이 실패한다.
- **긴 conflict 경로 생성도 실패할 수 있음:** byte budget에 맞춰 줄인 stem이 Windows 예약 이름이 되면 `WirePath::new`가 `ConflictPath` 오류를 반환한다. 예를 들어 충분히 긴 parent 아래 `config.txt`의 stem이 `con`으로 줄어드는 경우다. 이는 silent overwrite가 아니라 해당 merge round의 명시적 실패다.
- **정확한 Byzantine proof는 아님:** 인증된 peer가 제공한 snapshot의 hash 일관성을 확인하지만, 외부 신뢰 기준으로 remote storage를 증명하지 않는다.

## 9. 복잡도와 성능 특성

`N`을 전체 record 수, `C`를 path component 총수, `D`를 실제로 다른 record 수라고 하자.

| 작업 | 시간 | 메모리/전송 특성 |
| --- | --- | --- |
| `MerkleTree::from_records` | map 구성 `O(N log N)` + trie 구성/finalize `O(C)` | record map과 trie를 함께 보유 |
| `merge_snapshots` | path 합집합 구성과 map 작업으로 대략 `O(N log N)` | complete canonical snapshot 생성 |
| `different_paths` | 동일 subtree는 `O(1)` prune, 최악 `O(C)` | 출력은 changed/one-sided paths에 비례 |
| remote snapshot | 일치 root는 1 query, 최악 node 수에 비례 | 방문 node의 immediate-child summaries + 복원 records |
| `actions_to_reach` | diff traversal + changed path lookup | action 수는 desired와 다른 경로 수에 비례 |
| remote apply | incoming action마다 receiver index 전체 scan | action 수가 많으면 반복 scan이 지배적 비용이 될 수 있음 |

현재 구현은 snapshot과 merge를 memory에 완전히 materialize한다. Merkle protocol은 네트워크에서 불필요한 remote record 전송을 줄이지만, 양쪽 모두 local complete index와 tree를 구성하는 비용까지 없애지는 않는다.

## 10. 코드 탐색 지점

| 관심사 | 구현 위치 |
| --- | --- |
| Merkle build, node summary, subtree records, local diff | `crates/deltaweave-reconcile/src/lib.rs`의 `MerkleTree`, `MerkleNode`, `diff_nodes` |
| merge 분기와 conflict resolution | 같은 파일의 `merge_snapshots`, `resolve_conflict` |
| conflict 경로 | 같은 파일의 `allocate_conflict_path`, `build_conflict_path` |
| idempotent action 계획 | 같은 파일의 `actions_to_reach` |
| version vector와 record hash | `crates/deltaweave-core/src/lib.rs`의 `VersionVector`, `SyncRecord` |
| partial Merkle query와 reconstruction 검증 | `crates/deltaweave-net/src/lib.rs`의 `fetch_snapshot_connected` |
| snapshot server isolation | 같은 파일의 `handle_query_session` |
| incoming causal precondition | 같은 파일의 `ensure_causally_applicable` |
| stage, apply ordering, 최종 root 검증 | `crates/deltaweave-sync/src/lib.rs`의 `sync_with_session`, `apply_local`, `apply_remote` |

## 11. 코드 대조 결론

현재 `deltaweave-core`, `deltaweave-reconcile`, `deltaweave-net`, `deltaweave-sync` 구현과 절별 설명을 대조한 결과, 주요 동작 설명은 코드와 일치한다. 다만 8.2와 9절의 conflict path 생성 실패 가능성과 action별 receiver 재스캔 비용은 안전성·성능 해석 시 함께 고려해야 한다.

`deltaweave-reconcile`의 핵심은 **version vector로 최신 상태와 동시 상태를 구분하고, divergent concurrency는 hash 기반의 방향 독립 규칙으로 한 번만 해소하며, losing file bytes를 결정적 sibling path에 보존하는 것**이다. 해소 결과에는 양쪽 causal knowledge와 가상 resolver event가 들어가므로 다음 pass에서 다시 충돌하지 않는다.

Merkle 계층은 각 portable path component를 trie node로 만들고 complete logical record hash, 정렬된 child name/hash/count를 재귀적으로 묶는다. 이 구조는 동일 subtree를 한 hash 비교로 건너뛰게 하며, 네트워크에서는 mismatched prefix만 질의한 뒤 복원한 complete snapshot의 root와 cardinality를 다시 검증한다.

결과적으로 현재 구현은 2-peer 환경에서 결정적 conflict handling, 효율적인 변경 탐색, idempotent action planning, 적용 후 독립적 수렴 검증을 하나의 흐름으로 제공한다. production multi-peer membership과 tombstone garbage collection은 이 계층 밖의 후속 과제로 남아 있다.
