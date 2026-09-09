# qSync F controller 경계 (2026-09-09)

이 문서는 로컬 controller와 GitHub Actions 사이의 **prepare/execute handoff**를 정의한다. 이번 checkpoint는 3개 물리 host의 최종 전송 증명이 아니다. Linux owner, 승인된 Windows RW provider, hosted Ubuntu RO consumer를 동시에 실제로 실행하는 F 검증은 E2/E3 payload와 다음 실행에서 별도로 판정한다.

## Prepare

controller는 먼저 private run parent 아래에서 결정적 8MiB fixture를 만든다. fixture는 64KiB chunk마다 고정 seed와 chunk/block 번호를 BLAKE2s한 바이트로 확장하므로 chunk 사이에 반복 데이터가 없다.

```text
python3 scripts/qsync_three_host_controller.py fixture \
  --output /검증전용/private/fixture-a.bin
sha256sum /검증전용/private/fixture-a.bin
python3 scripts/qsync_three_host_controller.py prepare \
  --repo happyaspic-byte/DeltaWeave \
  --ref integration/qbittorrent-sync-20260908 \
  --source-sha <40자리 source SHA> \
  --expected-file-hash <fixture SHA-256> \
  --expected-file-size 8388608 \
  --run-parent /검증전용/private \
  --state /검증전용/private/prepare-1/controller-state.json
```

`prepare`는 dispatch 전 기존 run ID를 기록하고, `workflow_dispatch`, source SHA, `qsync-<coordination id>` run-name marker가 모두 맞는 새 run만 선택한다. 준비 run은 Windows native artifact와 Linux role artifact를 다운로드한 뒤 각 manifest의 source/workflow SHA, target, hash, 크기를 검증한다. Linux와 Windows 실행 파일의 hash는 달라도 되며, 각 role/platform descriptor를 따로 보존한다. state에는 준비 실행 ID와 재사용할 `prebuilt_role_run_id`를 명시적으로 고정하고, 다운로드 후 Linux 실행 bit를 다시 설정한다.

Actions workflow의 `execution_mode: prepare`는 native artifact 검증, dashboard asset test/build, `cargo build --locked --all-features --release`, role manifest 생성과 artifact 업로드를 수행한다. 이 단계의 대기 예산은 최대 60분이다. controller state에는 share key를 쓰지 않는다.

## Execute

로컬 RW driver가 같은 state의 source SHA, coordination ID, fixture hash/size, `role: rw_provider`, `keepalive_observed: true`를 담은 작은 JSON readiness 파일을 만들고 flush한 뒤 controller를 호출한다. 단순 `ready=true` 문자열이나 등록 member 수는 readiness 증거가 아니다.

```text
python3 scripts/qsync_three_host_controller.py execute \
  --repo happyaspic-byte/DeltaWeave \
  --ref integration/qbittorrent-sync-20260908 \
  --state /검증전용/private/prepare-1/controller-state.json \
  --share-key-env QSYNC_F_RO_SHARE_KEY \
  --rw-ready-file /검증전용/private/prepare-1/rw-ready.json \
  --secret-name QSYNC_F_RO_<새 coordination 값>
```

share key는 이 프로세스의 memory vault에서만 다루고 Actions 입력·artifact·로그에 넣지 않는다. controller는 repository secret 이름을 조회해 이미 있으면 중단하고, 새로 만든 이름만 소유한 것으로 기록한다. 생성 응답이 유실되면 삭제하지 않고 state를 `pending`으로 남긴다. 성공한 secret은 hosted RO run과 redacted result download가 끝난 뒤에만 삭제한다. 삭제 결과가 불명확하면 `pass`를 쓰지 않는다.

execute workflow는 `prebuilt_role_run_id`의 role artifact를 직접 다운로드·검증한다. execute job은 native/Linux build를 다시 기다리지 않으며, `hosted-ro-consumer`는 그 검증 job 뒤에만 시작한다. hosted runner에는 RO key만 전달하고 Windows/owner-admin credential은 전달하지 않는다. controller는 RO result의 source SHA, `full_f_claim: false`, expected fixture hash/size, 관측된 file hash를 확인한 뒤 state를 `pass`로 기록한다.

## 정리와 판정

```text
python3 scripts/qsync_three_host_controller.py cleanup \
  --repo happyaspic-byte/DeltaWeave \
  --ref integration/qbittorrent-sync-20260908 \
  --state /검증전용/private/prepare-1/controller-state.json \
  --owner-stopped --remote-stopped --secret-released
```

세 handle이 모두 닫혔다는 관찰이 없으면 controller-owned run directory와 state를 보존한다. 강제 종료나 unknown ACK를 graceful drain으로 기록하지 않는다. 이 checkpoint에서는 외부 3-host 실행, 새 per-run secret 생성, E2/E3의 공급자별 payload 증명을 수행하지 않았다.

로컬 도구 검사는 다음 경계만 확인한다.

```text
python3 -m unittest discover -s tests/tools -p 'test_qsync_three_host*.py'
python3 -m py_compile scripts/qsync_three_host_controller.py scripts/qsync_three_host_role.py scripts/qsync_three_host_winrm_keepalive.py
```
