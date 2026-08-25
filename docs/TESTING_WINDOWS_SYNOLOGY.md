# Windows PC ↔ Synology DSM 릴리즈 테스트

이 문서는 DeltaWeave v0.1.0 바이너리를 Windows PC와 Synology NAS에서
검증하는 절차입니다. v0.1.0은 단일 파일 델타 전송을 검증하기 위한
pre-alpha이며, 중요한 데이터의 유일한 사본으로 사용하면 안 됩니다.

## 1. 패키지 선택

Windows PC에서는 다음 파일을 받습니다.

- `DeltaWeave-v0.1.0-windows-x86_64.zip`

Synology에 SSH로 접속하고 CPU 아키텍처를 확인합니다.

```bash
uname -m
```

| 출력 | 받을 패키지 |
| --- | --- |
| `x86_64` | `DeltaWeave-v0.1.0-synology-x86_64.tar.gz` |
| `aarch64` | `DeltaWeave-v0.1.0-synology-aarch64.tar.gz` |
| `armv7l` 등 | v0.1.0 미지원 |

모든 파일은 [GitHub Releases](https://github.com/happyaspic-byte/DeltaWeave/releases/tag/v0.1.0)에서
다운로드합니다.

## 2. 다운로드 무결성 확인

릴리즈의 `SHA256SUMS.txt`에서 각 파일의 해시를 확인합니다.

Windows PowerShell:

```powershell
Get-FileHash .\DeltaWeave-v0.1.0-windows-x86_64.zip -Algorithm SHA256
```

Synology SSH:

```bash
sha256sum DeltaWeave-v0.1.0-synology-*.tar.gz
```

계산된 값이 `SHA256SUMS.txt`와 정확히 같아야 합니다.

## 3. 압축 해제와 장비별 self-test

Windows PowerShell:

```powershell
Expand-Archive .\DeltaWeave-v0.1.0-windows-x86_64.zip -DestinationPath C:\DeltaWeave
cd C:\DeltaWeave
.\deltaweave.exe self-test
```

Synology SSH에서는 NAS 아키텍처에 맞는 파일명을 사용합니다.

```bash
mkdir -p /volume1/DeltaWeave
tar -xzf DeltaWeave-v0.1.0-synology-x86_64.tar.gz \
  -C /volume1/DeltaWeave --strip-components=1
cd /volume1/DeltaWeave
chmod 755 ./deltaweave
./deltaweave self-test
```

두 장비 모두 아래 항목을 포함한 JSON을 출력해야 합니다.

```json
{
  "status": "pass",
  "reused_extents": 1,
  "second_transfer_bytes": 123456
}
```

수치는 장비와 청킹 결과에 따라 달라지며 `status`가 `pass`,
`reused_extents`가 0보다 크면 성공입니다.

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
  --direct-only
```

수신기 출력에서 다음 값을 복사합니다.

- Synology `endpoint_id`
- Windows에서 접근 가능한 `direct_addresses`의 `IP:UDP_PORT`

같은 LAN이면 Synology LAN IP를, Tailscale을 사용하면 Synology Tailscale IP가
포함된 주소를 선택합니다. DSM 방화벽을 사용한다면 출력된 UDP 포트를 허용합니다.

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

## 문제 해결

| 증상 | 확인 사항 |
| --- | --- |
| `Permission denied` | Synology에서 `chmod 755 ./deltaweave` 실행 |
| `cannot execute binary file` | `uname -m`과 패키지 아키텍처가 일치하는지 확인 |
| 피어 거부 | Synology의 `--allow-peer`에 Windows 송신자 ID를 넣었는지 확인 |
| 연결 실패 | IP 도달성, Windows/DSM 방화벽, 출력된 UDP 포트 확인 |
| 키 권한 오류 | Synology에서 `chmod 600 sender.key receiver.key` 실행 |

테스트 종료는 Synology 수신기 터미널에서 `Ctrl+C`를 누릅니다. v0.1.0에는
아직 백그라운드 서비스, 폴더 실시간 감시, 완전한 양방향 동기화, DSM SPK,
Windows 설치 프로그램 또는 VFS가 포함되지 않습니다.
