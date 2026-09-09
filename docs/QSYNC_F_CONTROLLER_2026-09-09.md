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

## Execute handoff

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

## Live run

실제 로컬 controller 실행은 준비 state에서 다음 명령으로 시작한다.

```text
python3 scripts/qsync_three_host_controller.py run \
  --repo happyaspic-byte/DeltaWeave \
  --ref integration/qbittorrent-sync-20260908 \
  --state /검증전용/private/prepare-1/controller-state.json \
  --owner-api-url-env QSYNC_F_OWNER_API_URL \
  --artifact-public-host-env QSYNC_F_ARTIFACT_PUBLIC_HOST \
  --winrm-host-env QSYNC_F_WINRM_HOST \
  --winrm-username-env QSYNC_F_WINRM_USERNAME \
  --winrm-password-env QSYNC_F_WINRM_PASSWORD \
  --winrm-destination-env QSYNC_F_WINRM_DESTINATION \
  --owner-bind-host 0.0.0.0 \
  --keepalive-seconds 300 \
  --ready-timeout-seconds 120
```

`run`은 state manifest에 고정된 Linux owner와 Windows RW binary를 run-owned private copy로 다시 검증한 뒤, 격리 Linux owner를 시작하고 실제 WinRM 호출을 supervisor thread가 보유한다. WinRM callback에서 member join, fixture hash/size, `keepalive_enter`를 모두 관찰하고 호출 thread가 아직 살아 있을 때만 RO key를 일회성 repository secret으로 만들어 hosted workflow를 dispatch한다. hosted workflow 전체가 끝나기를 기다리지 않고 정확히 `F hosted Ubuntu read-only consumer` job만 선택해 완료를 확인한다. hosted 역할에는 owner-admin 또는 Windows credential을 전달하지 않는다.

원격 Windows가 owner에 접근하는 실행에서는 `--owner-bind-host 0.0.0.0`처럼 controller에서 도달 가능한 bind 주소와 해당 주소를 허용하는 `QSYNC_F_PUBLIC_HOST` 환경을 운영자가 함께 지정해야 한다. 기본 `127.0.0.1`은 같은 호스트 실험용이며 원격 member의 도달성을 증명하지 않는다. owner API URL 환경값은 그 실제 도달 주소의 포트 템플릿이어야 하고 state/evidence에는 남지 않는다.

RW keepalive의 남은 시간만 hosted job 대기에 사용하며 전체 상한은 900초다. WinRM handle이 종료되지 않거나 owner 종료의 managed drain을 증명하지 못하면 state는 `pending`으로 남고 runtime을 삭제하지 않는다. 강제 종료와 unknown cleanup은 성공으로 승격하지 않는다. `run`의 live attestation은 외부 `rw-ready.json`보다 우선하며, 이전 `execute` 명령의 ready file은 handoff 호환성용으로만 남아 있다.

로컬 Linux owner의 `LocalWebProcess.stop()`은 controller가 보유한 해당 `Popen` 핸들에만 SIGTERM을 보내고, 30초 안에 종료를 관찰한 뒤 exit code를 저장한다. 신호를 실제로 보냈고 exit code가 0이며 강제 종료가 없을 때만 `graceful_drain_proven=true`로 기록한다. 이미 종료된 프로세스, 비정상 exit, 신호·대기·reap 실패는 프로세스 핸들을 잃지 않고 `pending` 경계로 남긴다. Windows RW의 console Ctrl+C 증거는 별도 WinRM helper가 담당하며, 이 POSIX local-process 판정으로 대체하지 않는다.

2026-09-09T11:06:48Z에 source `2f44d9fbfbe1c4779bd59f78fe9cc4ff27f41cd6`에서 빌드된 기존 Linux binary(SHA-256 `621d880c447a155c193eef9a8f2c4b11afcdf994a31c9b392d5c11d07bedefe1`, 32,322,600 bytes)를 새 run-owned copy로 고정해 local owner web start와 SIGTERM 종료를 실행했다. exit code 0, 비강제 종료, Popen 핸들 해제, owned 경로 제거가 확인됐다. 이 실행은 현재 통합 source의 provenance나 managed drain ACK를 검증하지 않으므로 `managed_drain_ack=unverified`, 3-host 주장은 false로 기록했다. 상세 결과는 `f-bootstrap/local-linux-owner-shutdown-20260909.json`이다.

이 live 경로의 `file_hash`는 가입 후 파일 내용과 크기를 확인하는 readiness 관측이며 공급자별 조각 payload나 CAS 기여를 측정하지 않는다. owner/RW 양쪽 CAS와 두 공급자의 실제 payload는 E2/E3 실행에서 별도 계측·판정해야 한다. 실제 계측 결과가 준비되면 [provider payload gate](QSYNC_F_PAYLOAD_2026-09-09.md)에 `--provider-evidence`와 `--require-provider-payload`를 함께 전달한다. 이 gate를 통과하기 전에는 role manifest의 provider counter를 채우지 않는다.

## 정리와 판정

```text
python3 scripts/qsync_three_host_controller.py cleanup \
  --repo happyaspic-byte/DeltaWeave \
  --ref integration/qbittorrent-sync-20260908 \
  --state /검증전용/private/prepare-1/controller-state.json \
  --owner-stopped --remote-stopped --secret-released
```

세 handle이 모두 닫혔다는 관찰이 없으면 controller-owned run directory와 state를 보존한다. 강제 종료나 unknown ACK를 graceful drain으로 기록하지 않는다. 이 checkpoint에서는 외부 3-host 실행, 새 per-run secret 생성, E2/E3의 공급자별 payload 증명을 수행하지 않았다. `run` 경로는 실제 역할 handle과 hosted job adapter를 연결하지만, 이 문서 checkpoint 자체가 3개 물리 host 동시 실행이나 share-swarm/1 다중 provider 증거를 의미하지 않는다.

durable state를 다시 읽는 중 오류가 나면 이전 안전 snapshot으로 pending 기록을 만들 수는 있지만, 이를 성공으로 승격하거나 run directory를 삭제하지 않는다. `state_refresh_failed`와 `state_invalid` 판정은 operator reconciliation이 필요한 경계다.

로컬 도구 검사는 다음 경계만 확인한다.

```text
python3 -m unittest discover -s tests/tools -p 'test_qsync_three_host*.py'
python3 -m py_compile scripts/qsync_three_host_controller.py scripts/qsync_three_host_role.py scripts/qsync_three_host_winrm_keepalive.py
```
