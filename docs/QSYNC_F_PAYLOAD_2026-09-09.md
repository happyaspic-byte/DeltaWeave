# qSync F provider payload gate (2026-09-09)

이 문서는 F controller가 `deltaweave/share-swarm/1`의 실제 공급자 조각 전송을
판정하기 위한 작은 증거 계약이다. 현재 통합 source에서는 이 계측을 아직 외부
3-host 실행에 연결하지 않았으므로 이 checkpoint의 role manifest는 계속
`providers[].verified_bytes=0`, `providers[].verified_chunks=0`,
`full_f_claim=false`를 기록한다. 이 문서의 fixture와 gate 회귀검사는 실행 준비를
검증하며 실제 공급자 전송 성공을 뜻하지 않는다.

## 관측 순서

최종 실행은 다음 순서로 진행한다.

1. Linux owner가 run-owned private namespace에서 중복되지 않는 결정적 8 MiB
   fixture를 만들고 SHA-256/크기를 고정한다. 현재 controller의
   `write_fixture`가 64 KiB 단위로 이 fixture를 만든다.
2. Windows RW가 같은 source의 검증된 Windows binary로 가입하고 fixture를
   초기 동기화한다. owner와 RW의 `transferred_bytes` 및 verified chunk counter가
   증가했는지 각각 확인한다.
3. RO join 직전에 owner/RW counter와 관측 시각을 baseline으로 저장한다. 그 뒤
   RO를 join시키고, RO 파일 hash/크기 확인이 끝난 직후 두 counter를 다시 읽는다.
4. controller adapter가 각 provider의 baseline/end delta와 그 delta를 구성하는
   source-tagged swarm event를 비밀 없는 JSON으로 정규화한다. roster 수, 등록
   supplier 수, CAS preseed, legacy `share/3` aggregate counter는 payload로
   사용할 수 없다.
5. RO의 수신 바이트가 fixture 크기와 같고 preexisting fixture chunk 및 reused
   bytes/chunks가 0인지 확인한다. provider 두 곳의 delta가 각각 0보다 크고,
   event byte/chunk 합계와 정확히 같아야 한다. 두 delta 합계는 RO 수신량 이상이어야
   한다.

## 정규화 증거

`scripts/qsync_three_host_coordination.py --require-provider-payload`는 기존 RW/RO
역할 증거와 함께 다음 필드를 엄격히 검사한다. JSON object의 허용되지 않은 필드는
거부하므로 key, bearer, endpoint, 원격 debug chain을 이 파일에 추가할 수 없다.

```json
{
  "schema_version": 1,
  "scope": "share_swarm_provider_payload_window",
  "status": "pass",
  "full_f_claim": false,
  "run_id": "<numeric controller run id>",
  "source_sha": "<40 lowercase hex>",
  "fixture": {"sha256": "<fixture sha256>", "size_bytes": 8388608},
  "payload_window": {
    "started_utc": "<controller-observed timestamp>",
    "finished_utc": "<controller-observed timestamp>",
    "other_payload_observed": false
  },
  "providers": [
    {
      "role": "owner-provider",
      "protocol": "deltaweave/share-swarm/1",
      "source_tag": "share_swarm_v1",
      "counter_scope": "share_swarm_provider",
      "observed": true,
      "baseline_observed_utc": "<timestamp>",
      "end_observed_utc": "<timestamp>",
      "baseline_transferred_bytes": 0,
      "end_transferred_bytes": 4194304,
      "baseline_verified_chunks": 0,
      "end_verified_chunks": 64,
      "events": [
        {
          "observed_utc": "<timestamp>",
          "protocol": "deltaweave/share-swarm/1",
          "source_tag": "share_swarm_v1",
          "verified": true,
          "bytes": 4194304,
          "chunks": 64
        }
      ]
    },
    {
      "role": "rw-provider",
      "protocol": "deltaweave/share-swarm/1",
      "source_tag": "share_swarm_v1",
      "counter_scope": "share_swarm_provider",
      "observed": true,
      "baseline_observed_utc": "<timestamp>",
      "end_observed_utc": "<timestamp>",
      "baseline_transferred_bytes": 8192,
      "end_transferred_bytes": 4202496,
      "baseline_verified_chunks": 0,
      "end_verified_chunks": 64,
      "events": [
        {
          "observed_utc": "<timestamp>",
          "protocol": "deltaweave/share-swarm/1",
          "source_tag": "share_swarm_v1",
          "verified": true,
          "bytes": 4194304,
          "chunks": 64
        }
      ]
    }
  ],
  "consumer": {
    "role": "ro-consumer",
    "protocol": "deltaweave/share-swarm/1",
    "source_tag": "share_swarm_v1",
    "join_started_utc": "<timestamp>",
    "join_finished_utc": "<timestamp>",
    "file_hash_started_utc": "<timestamp>",
    "file_hash_finished_utc": "<timestamp>",
    "received_bytes": 8388608,
    "reused_bytes": 0,
    "reused_chunks": 0,
    "preexisting_fixture_chunks": 0,
    "file_hash_observed": true,
    "file_hash": "<fixture sha256>",
    "size_bytes": 8388608
  }
}
```

`source_tag=share_swarm_v1`는 controller adapter가 인증된
`ALPN_SWARM_V1`/typed share event에서만 만들 수 있는 고정 분류다. legacy
fallback event 또는 `share/3` control/manifest 이벤트는 이 gate에서
`provider_attribution_invalid`로 거부한다. event byte/chunk 합계는 provider
counter delta와 일치해야 하며, event 관측 시각은 baseline/end 및 RO join부터
파일 hash 완료까지의 창 안에 있어야 한다. 이 시간은 controller가 trace를 받은
UTC 시각이며 서로 다른 host의 wall clock 동기화 증거로 해석하지 않는다.

실패는 다음 고정 범주만 출력한다: `provider_payload_missing`,
`provider_payload_invalid`, `provider_source_mismatch`,
`provider_attribution_invalid`, `provider_counter_invalid`,
`provider_window_missing`, `provider_file_mismatch`. 원본 exception, 경로,
endpoint, key는 출력하지 않는다.

## 실행 경계와 남은 의존성

현재 검증 가능한 명령은 다음과 같다.

```text
python3 -m py_compile scripts/qsync_three_host_coordination.py \
  tests/tools/test_qsync_three_host_coordination.py
python3 -m unittest tests.tools.test_qsync_three_host_coordination -v
```

최종 실행에서는 Linux owner와 Windows RW가 살아 있고 `keepalive_enter`를 실제로
관찰한 뒤에만 fresh hosted Ubuntu RO를 dispatch한다. `payload_window`는 그
keepalive 창 안에 있어야 한다. RO 결과가 먼저 끝났거나 RW가 이미 종료된 경우에는
gate가 성공하지 않는다. hosted workflow의 RO key만 일회성 secret alias로 주입하며
owner admin/WinRM credential은 hosted runner에 전달하지 않는다.

A/control의 정확한 `transferred_bytes` publication과 source-tagged typed event
adapter, Windows keepalive의 비밀 없는 provider progress 관측, 그리고 E2/E3의
실제 두 provider payload가 아직 이 checkpoint에 없다. 따라서 현재 상태는
`three_host_transport_smoke_subset`이며, 이 gate의 단위 fixture pass는 물리 3-host,
N0/relay, bilateral revoke/pause drain, provider interruption, 또는 full F 완료가
아니다. 실제 실행 후에만 gate 입력을 만들고, 그때도 provider delta와 RO hash,
source/role/window를 함께 보존한다.
