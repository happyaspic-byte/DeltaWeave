# Windows PC ↔ Synology DSM 릴리즈 테스트

이 문서는 DeltaWeave v0.4.0 바이너리의 Windows PC↔Synology 양방향 폴더
동기화를 검증하는 절차입니다. v0.4.0은 Merkle/버전 벡터 기반 pre-alpha
field preview이며, 중요한 데이터의 유일한 사본으로 사용하면 안 됩니다.
`Cargo.toml`의 workspace 버전도 `0.4.0`입니다. 절차와 기대 결과는 현재 소스 기준이며
릴리스 파일이나 특정 DSM 모델에서 이 문서의 모든 시험을 완료했다는 뜻은 아닙니다.

## 실제 실행 화면: 전·중·후·결과

아래 화면은 `scripts/render-doc-visuals.sh`에 보관된 기존 출력과 설명으로 만든
문서용 터미널 화면입니다. 스크립트는 전송·인덱스 출력을 v0.2.0, 양방향 동기화 출력을
v0.3.0 실행에서 가져왔다고 기록합니다. 현재 바이너리를 실행해 새로 수집하는 화면은
아니며, ARM64/Linux 출력이나 NAS 모양의 프롬프트만으로 물리 DSM 실행을 입증하지 않습니다.

![Windows 자체 테스트 전, 중, 후, 결과](assets/deltaweave-quickstart.gif)

각 단계의 큰 정적 화면과 ARM64 참고 화면은
[사용 화면 갤러리](USAGE_GALLERY.md)에서 확인합니다.

## 1. 패키지 선택

Windows PC에서는 다음 파일을 받습니다.

- `DeltaWeave-v0.4.0-windows-x86_64.zip`

Synology에 SSH로 접속하고 CPU 아키텍처를 확인합니다.

```bash
uname -m
```

| 출력 | 받을 패키지 |
| --- | --- |
| `x86_64` | `DeltaWeave-v0.4.0-synology-x86_64.tar.gz` |
| `aarch64` | `DeltaWeave-v0.4.0-synology-aarch64.tar.gz` |
| `armv7l` 등 | v0.4.0 미지원 |

모든 파일은 [GitHub Releases](https://github.com/happyaspic-byte/DeltaWeave/releases/tag/v0.4.0)에서
다운로드합니다.

2026-09-05 읽기 전용 확인에서 v0.4.0은 2026-09-04 게시된 pre-release였으며,
위 세 아카이브와 `SHA256SUMS.txt`가 실제 asset 목록에 있었습니다. 이후 버전을
검증할 때는 존재하는 릴리스와 asset 이름을 확인하고 아래 파일명을 함께 바꿉니다.

release workflow의 빌드 대상은 Windows `x86_64-pc-windows-msvc`, Synology용
`x86_64-unknown-linux-musl` 및 `aarch64-unknown-linux-musl`입니다. Windows runner의
패키지 `self-test`와 Ubuntu에서 Linux 컨테이너로 실행하는 musl 바이너리 `self-test`가
설정되어 있으며 ARM64에는 QEMU를 사용합니다. musl 패키지는 동적 interpreter가
없는지 검사합니다. 이 설정은 물리 NAS 모델별 DSM 호환성이나 실제 Windows↔NAS
네트워크 시험의 성공 증거가 아닙니다. ARMv7, Windows ARM64 전용 패키지는 없습니다.
Portainer 이미지 경로는 [별도 실행서](AI_PORTAINER_SETUP.md)를 따릅니다.

## 2. 다운로드 무결성 확인

릴리즈의 `SHA256SUMS.txt`에서 각 파일의 해시를 확인합니다.

Windows PowerShell:

```powershell
Get-FileHash .\DeltaWeave-v0.4.0-windows-x86_64.zip -Algorithm SHA256
```

Synology SSH:

```bash
sha256sum DeltaWeave-v0.4.0-synology-*.tar.gz
```

계산된 값이 `SHA256SUMS.txt`와 정확히 같아야 합니다.
아카이브는 비어 있는 테스트 설치 디렉터리에 풉니다. 기존 설치를 덮어쓸 때는 바이너리
교체 전에 receiver를 중지하고 identity·state·동기화 파일의 백업을 확보합니다.

## 3. 압축 해제와 장비별 self-test

Windows PowerShell:

```powershell
Expand-Archive .\DeltaWeave-v0.4.0-windows-x86_64.zip -DestinationPath C:\DeltaWeave
cd C:\DeltaWeave
.\deltaweave.exe --version
.\deltaweave.exe self-test
```

Synology SSH에서는 NAS 아키텍처에 맞는 파일명을 사용합니다.

```bash
mkdir -p /volume1/DeltaWeave
tar --no-same-owner -xzf DeltaWeave-v0.4.0-synology-x86_64.tar.gz \
  -C /volume1/DeltaWeave --strip-components=1
cd /volume1/DeltaWeave
chmod 755 ./deltaweave
./deltaweave --version
./deltaweave self-test
```

위 tar 예시는 `x86_64`용입니다. `aarch64` NAS에서는 파일명을 해당 아카이브로
바꿉니다. Windows zip은 실행 파일을 최상위에, tar는 버전별 디렉터리 아래에 담으므로
tar에만 `--strip-components=1`을 사용합니다. 아카이브에는 이 문서가 `TESTING.md`로
포함됩니다. 아래는 `self-test` 성공 JSON에서 확인할 필드의 예시입니다.

```json
{
  "index_rename_detected": true,
  "index_restart_verified": true,
  "sync_bidirectional_verified": true,
  "sync_conflicts_preserved": 1,
  "sync_delete_verified": true,
  "sync_restart_actions": 0,
  "status": "pass",
  "reused_extents": 16,
  "second_transfer_bytes": 257800
}
```

`status`가 `pass`, `reused_extents`가 0보다 크고 index/sync 검증 값이 위와 같아야
합니다. `16`과 `257800`은 참고 출력값이며 현재 실행의 `second_transfer_bytes`가
`final_size`보다 작은지도 확인합니다. `self-test`는 한 장비에서 임시 root/state와
direct-only QUIC peer를 만들어 검증하고 종료 시 임시 데이터를 정리합니다. 두 장비 간
연결·방화벽·권한·실제 폴더를 검증하는 뒤의 절차를 대신하지 않습니다.

기존 Windows 출력 참고 화면:

![기존 Windows x86-64 자체 테스트 출력](assets/deltaweave-self-test.png)

기존 ARM64/Linux 출력 참고 화면:

![기존 ARM64/Linux 자체 테스트 출력](assets/deltaweave-synology-self-test.png)

## 4. Windows 송신자 키 생성

Windows PowerShell에서 실행합니다.

```powershell
cd C:\DeltaWeave
.\deltaweave.exe init --identity .\sender.key
```

출력된 Windows 송신자의 `endpoint_id`를 복사합니다. `sender.key`는 비밀키이므로
공유하거나 Git에 올리면 안 됩니다.

## 5. Synology 수신기 실행

Windows에서 복사한 ID를 `<WINDOWS_ENDPOINT_ID>`에 넣습니다.

```bash
cd /volume1/DeltaWeave
mkdir -p received state
./deltaweave init --identity ./receiver.key
./deltaweave serve \
  --root ./received \
  --state ./state \
  --identity ./receiver.key \
  --allow-peer <WINDOWS_ENDPOINT_ID> \
  --bind 0.0.0.0:49152 \
  --direct-only
```

수신기 출력에서 다음 값을 복사합니다.

- Synology `endpoint_id`
- Windows에서 접근 가능한 `direct_addresses`의 `IP:UDP_PORT`

같은 LAN이면 Synology LAN IP를, Tailscale을 사용하면 Synology Tailscale IP가
포함된 주소를 선택합니다. `0.0.0.0`은 bind용 wildcard이므로 Windows의 `--direct`에
사용하지 않습니다. DSM 방화벽을 사용한다면 Windows 원본 주소에서 오는 고정 UDP
포트를 허용합니다. 예시의 `49152`는 명시적으로 고른 포트이며 CLI 기본값이 아닙니다.
`--bind`를 생략하면 포트가 재시작 때 바뀔 수 있습니다. `--direct-only`는 discovery와
relay를 사용하지 않으므로 Windows에서 접근 가능한 `--direct` 주소가 필수입니다.
`serve`는 terminal에 계속 실행해 둡니다. allowlist·bind·경로 변경은 새 프로세스 시작 때
적용되며 설정 파일을 자동으로 다시 읽지 않습니다.

## 6. Windows에서 실제 파일 전송

10 MB 이상의 테스트용 로그, ISO 복사본 또는 임시 파일을 권장합니다.

```powershell
.\deltaweave.exe push C:\Test\sample.bin `
  --remote-path validation/sample.bin `
  --peer <SYNOLOGY_ENDPOINT_ID> `
  --direct <SYNOLOGY_IP:UDP_PORT> `
  --identity .\sender.key `
  --direct-only
```

성공하면 Synology의 다음 위치에 파일이 생성됩니다.

```text
/volume1/DeltaWeave/received/validation/sample.bin
```

## 7. 양쪽 파일 해시 확인

Windows PowerShell:

```powershell
Get-FileHash C:\Test\sample.bin -Algorithm SHA256
```

Synology SSH:

```bash
sha256sum /volume1/DeltaWeave/received/validation/sample.bin
```

두 SHA-256 값이 같아야 합니다.

## 8. 델타 재전송 확인

테스트 파일에 소량의 데이터를 추가하고 같은 `push` 명령을 다시 실행합니다.

```powershell
[IO.File]::AppendAllText("C:\Test\sample.bin", "DeltaWeave delta test")
```

두 번째 전송 결과에서 다음을 확인합니다.

- `reused_extents`가 0보다 큼
- `transferred_bytes`가 전체 파일 크기보다 작음
- 변경 후 Windows와 Synology 파일의 SHA-256이 다시 동일함

## 9. 실제 양방향 폴더 동기화

아래 검증은 기존 `push`와 별개로 NAS→Windows, 동시 수정, 삭제, 무변경 재실행까지
확인합니다. private state와 identity는 동기화 root 밖에 둡니다.
두 장비 모두 기존 운영 폴더 대신 복사본 테스트 폴더를 사용합니다. 아래 명령은 NAS의
`serve --root ./received` 전체를 동기화하므로 앞서 전송한 `validation/sample.bin`도
Windows로 내려옵니다. `sync-once`에는 원격 root의 일부만 고르는 옵션이 없습니다.

Windows PowerShell:

```powershell
New-Item -ItemType Directory -Force C:\DeltaWeave-Sync | Out-Null
New-Item -ItemType Directory -Force C:\DeltaWeave-Private | Out-Null
Set-Content C:\DeltaWeave-Sync\windows-only.txt "from Windows"

.\deltaweave.exe sync-once `
  --root C:\DeltaWeave-Sync `
  --state C:\DeltaWeave-Private\state `
  --identity .\sender.key `
  --peer <SYNOLOGY_ENDPOINT_ID> `
  --direct <SYNOLOGY_IP:UDP_PORT> `
  --direct-only
```

Synology의 `serve --root` 아래에 NAS 전용 파일을 만든 뒤 같은 명령을 다시 실행합니다.

```bash
printf '%s\n' 'from Synology' > /volume1/DeltaWeave/received/nas-only.txt
```

두 번째 실행 후 다음을 확인합니다.

- Windows에 `nas-only.txt`, Synology에 `windows-only.txt`가 모두 존재한다.
- JSON의 `status`가 `pass`다.
- `desired_root`, `verified_local_root`, `verified_remote_root`가 정확히 같다.

동시 수정 테스트는 복사본 파일로만 수행합니다. 먼저 양쪽에 같은 `shared.txt`를 만들고
한 번 동기화한 뒤, 네트워크를 끊거나 다음 sync 전 양쪽 내용을 서로 다르게 수정합니다.
다시 `sync-once`를 실행하면 JSON `conflicts`에 원본 경로와
`shared.conflict-<hash>.txt`가 하나 기록되어야 하며 두 장비에 두 파일의 BLAKE3
내용 집합이 같아야 합니다.

삭제 전파는 Windows에서 `windows-only.txt`를 삭제하고 다시 실행해 확인합니다.
NAS 파일이 사라지고 JSON이 `pass`여야 합니다. 바로 한 번 더 실행했을 때는 다음
무변경 fast path가 정상입니다.

```json
{
  "merkle_queries": 1,
  "local_actions": 0,
  "remote_actions": 0,
  "pulled_bytes": 0,
  "pushed_bytes": 0,
  "status": "pass"
}
```

장기 시험은 `sync-once` 대신 같은 인자의 `sync --interval-seconds 5`를 사용합니다.
Windows 로컬 변경은 native watcher가 기본 750ms quiet window, 최대 5000ms debounce를
거쳐 다음 pass를 앞당깁니다. 성공한 pass 뒤 기본 5초 대기가 NAS 변경도 확인하지만
pass 실행 시간과 오류 재시도가 더해지므로 5초 이내 감지·완료를 보장하지 않습니다. 시작 JSON의
`local_change_detection`이 `native_watcher`인지 확인합니다. watcher를 열지 못하면
`polling_fallback`과 `watcher_error`가 출력되지만 동기화는 계속됩니다. 일시 오류는
기본 1초부터 최대 300초까지 지수 백오프로 재시도하며 `Ctrl+C`로 종료합니다.
`--interval-seconds`, `--debounce-ms`, `--max-debounce-ms`, `--max-backoff-seconds`는
CLI 시작 때 적용합니다. `sync-once`는 오류를 반환하며 자동으로 재시도하지 않습니다.

![기존 v0.3 양방향 동기화 출력으로 만든 참고 화면](assets/deltaweave-sync-lifecycle.gif)

## 10. 재현 가능한 장애 주입 검증

실제 장비에 적용하기 전에 저장소의 Linux/CI 호스트에서 같은 CLI 실행 파일의 자식
프로세스와 loopback QUIC을 사용하는 고정-seed 시나리오를 실행합니다. 아래 workspace는
각 실행마다 비어 있는 새 테스트 경로를 선택합니다. 하네스가 fixture와 테스트 identity를
쓰기 때문에 운영 경로나 이전 실행의 증거 폴더를 재사용하지 않습니다.

```bash
./scripts/fault-test.sh /tmp/deltaweave-fault-424242
```

wrapper는 저장소 루트로 이동해 `cargo run --locked -p deltaweave -- fault-test`를
실행하므로 Rust toolchain(현재 최소 1.91)과 checkout이 필요합니다. release workflow는
아카이브를 다시 빌드하고 `self-test`를 실행하지만 이 wrapper나
`scripts/verify-release.sh` 전체를 실행하는 단계는 없습니다. CI의 workspace test에는
`crates/deltaweave-cli/tests/fault_test.rs` 통합 테스트가 포함됩니다.

Windows 개발 환경에서는 PowerShell에서 같은 실행점을 직접 호출할 수 있습니다.

```powershell
cargo run --locked -p deltaweave -- fault-test --seed 424242 `
  --workspace C:\DeltaWeave-Fault-424242
```

릴리스 아카이브만 설치한 DSM에서는 다음과 같이 실행 파일을 직접 사용합니다.
이 경로에는 Rust toolchain이 필요하지 않으며 Windows도 설치한
`.\deltaweave.exe fault-test --seed 424242 --workspace C:\DeltaWeave-Fault-424242`를
직접 실행할 수 있습니다. wrapper script와 `cargo` 기반 검증 스크립트는 아카이브에 없습니다.

```bash
./deltaweave fault-test --seed 424242 \
  --workspace /volume1/DeltaWeave-Fault-424242
```

하네스는 한 호스트에 `windows`/`synology`라는 이름의 독립 root/state를 만들고
생성·수정·삭제·이름 변경을 고정 순서로 적용합니다. seed는 identity와 파일 bytes에
사용되며 이 순서를 무작위화하지 않습니다. `serve` 자식 프로세스를 `Child::kill`로
종료하고, 재시작·수렴 뒤 두 번째 Windows→Synology 방향의 전송에서도 `sync-once`
자식 프로세스를 강제 종료합니다. 동일 state를 다시 열어 최종 경로별 파일 해시와 양쪽
Merkle root를 비교하고 `restart_local_actions: 0`, `restart_remote_actions: 0`을 확인합니다.

현재 barrier 구현에는 한계가 있습니다. 기준값은 `state/chunks` 파일 수이지만 관측은
`state` 전체의 비어 있지 않은 파일 수를 세므로 기존 metadata/index 파일만으로 조건을
만족할 수 있습니다. 따라서 `remote_chunk_persisted_destination_absent`라는 report 값이
새 CAS chunk의 durable 기록 중단을 입증하지는 않습니다. 목적 파일 부재와 자식 프로세스의
생존을 폴링하며, receiver 시작 대기는 로그의 ready 확인 대신 약 2초간 생존 확인을
사용합니다. 오류 전파·재시작 시험의 결과와 정확한 전송 중단 시점의 보장을 구분합니다.

| 입력 | 기본값·조건 | 읽는 위치와 시점 |
| --- | --- | --- |
| `--seed` | `424242` | CLI 시작 시; 테스트 identity·내용 생성 |
| `--workspace` | 생략하면 임시 디렉터리 | CLI 시작 시; 증거를 보존하려면 새 경로를 명시 |
| `--payload-mib` | `16` | CLI 시작 시; 각 장애용 payload 크기 |
| `--force-failure` | 기본 false | 시나리오 완료 뒤 의도적으로 실패 종료 |
| `DELTAWEAVE_FAULT_SEED` | 미설정·빈 값이면 `424242` | `scripts/fault-test.sh`가 실행 때 `--seed`로 전달 |
| `DELTAWEAVE_FORCE_FAILURE` | 정확히 `1`일 때만 활성 | wrapper가 `--force-failure`로 전달; CLI가 직접 읽지 않음 |
| `RUST_LOG` | 미설정·잘못된 filter면 `warn,netwatch=error` | 각 CLI 프로세스 시작 시 stderr 로그 filter |
| `DELTAWEAVE_VERIFY_RUST_LOG` | 미설정·빈 값이면 `warn,netwatch=error` | `verify-release.sh`의 self-test 단계에만 `RUST_LOG`로 전달 |

실패 증거를 확인하려면 의도적 실패를 요청합니다.

```bash
DELTAWEAVE_FORCE_FAILURE=1 ./scripts/fault-test.sh /tmp/deltaweave-fault-failure
```

`report.json`에는 seed, ordered operations, fault points, 최종 root, root/state 경로,
peer log 경로가 기록됩니다. `logs/windows.log`, `logs/synology.log`, `roots/`,
`states/`, `identities/`도 명시한 workspace 아래 유지됩니다. 성공 실행도 workspace를
명시하면 보존됩니다. 아주 이른 실패에서는 report의 경로만 있고 로그나 root가 아직
생성되지 않았을 수 있습니다. `--workspace`를 생략하면 현재 구현은 성공·실패 모두
임시 디렉터리를 정리합니다. `verify-release.sh`도 EXIT trap에서 장애 시험 workspace를
삭제하므로 증거가 필요하면 위 독립 명령과 명시 경로를 사용합니다. 실패 시 자식 프로세스가
남을 수 있으므로 테스트 프로세스와 실행 경로를 확인합니다. 실패 후 동일 `--seed`와
새 workspace로 재현합니다. 운영
복구는 장애 전 사용한 기존 `--root`, `--state`, identity를 그대로 두고 receiver를
먼저 재시작한 뒤 원래 `sync-once` 명령을 다시 실행합니다. state를 삭제하거나 다른
root/replica에 재사용하지 마십시오. root/state 중첩, 불완전 scan, collision 등 기존
안전 거부는 우회되지 않습니다.

한계: 자동 검증은 한 호스트의 loopback transport에서 실제 CLI 자식 프로세스를
종료합니다. 두 디렉터리의 이름이 다른 OS를 실행한다는 뜻은 아닙니다. Windows/DSM
전원 차단, 디스크 고갈, 장시간 partition, DSM package, 서비스 관리자의 kill semantics는
검증하지 않습니다. 물리 장비 사이의 장기 시험과 백업은 계속 필요합니다.

## 11. 문제 해결

| 증상 | 확인 사항 |
| --- | --- |
| `Permission denied` | 오류 경로를 확인; 실행 파일의 execute bit, root/state 디렉터리 소유권·쓰기 권한, mount의 `noexec` 여부 확인 |
| `cannot execute binary file` | `uname -m`과 패키지 아키텍처가 일치하는지 확인 |
| 피어 거부 | Synology의 `--allow-peer`에 Windows 송신자 ID를 넣었는지 확인 |
| 연결 실패 | IP 도달성, Windows/DSM 방화벽, receiver의 현재 UDP 포트 확인; ID가 유지돼도 포트는 달라질 수 있음 |
| 키 권한 오류 | 오류에 명시된 장비·identity 파일을 확인; 이 예시의 NAS 키는 `chmod 600 ./receiver.key`, Windows 키는 공유하거나 NAS에 복사하지 않음 |
| `scan is incomplete` | 잠긴/변경 중인 파일을 닫고 retry 시간이 지난 뒤 다시 실행 |
| `path collision` | 대소문자·Unicode 정규화 후 같은 이름이 되는 파일을 수동 변경 |
| `causally stale` | 양쪽 최신 상태를 `sync-once`로 다시 병합하고 오래된 자동화 중지 |

테스트 종료는 Synology 수신기 터미널에서 `Ctrl+C`를 누릅니다. v0.4.0에는
검증형 양방향 폴더 동기화가 포함되지만 DSM SPK, Windows 서비스/설치 프로그램,
symlink materialization 또는 VFS는 포함되지 않습니다. 로컬 인덱스는
[별도 검증 절차](TESTING_LOCAL_INDEX.md)를 따릅니다.
