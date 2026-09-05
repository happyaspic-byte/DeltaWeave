# DeltaWeave Synology Portainer AI 설치 실행서

이 문서는 **AI 운영 에이전트가 읽고 Synology NAS의 Portainer에 DeltaWeave
수신기를 안전하게 배포·검증하기 위한 실행 계약(runbook)**이다. 사람에게 일반적인
Docker 사용법을 설명하는 문서가 아니다. 에이전트는 추측으로 값을 만들지 말고 아래
게이트를 순서대로 통과해야 한다.

> **현재 소스 범위:** workspace 버전은 `0.4.0`이다. QUIC 델타 전송, 영속 로컬 인덱스,
> Merkle 부분 탐색, 버전 벡터, 양방향 동기화와 conflict copy를 제공하는
> pre-alpha field preview이며 `fault-test` CLI가 포함된다. DSM SPK, OS 서비스/설치 프로그램, symlink
> materialization과 VFS는 아직 없다. 중요한 데이터의 유일한 사본으로 사용하지 않는다.
> 아래는 대상 NAS에서 수행할 검증 절차이며 특정 DSM 모델의 검증 완료 선언이 아니다.

![Windows에서 Synology Portainer로 배포하고 검증하는 흐름](assets/portainer-flow.svg)

기존 버전의 출력으로 만든 문서용 화면은
[사용 화면 갤러리](USAGE_GALLERY.md)에서 확인한다. 이미지의 ARM64/Linux 표시는
물리 Synology NAS에서 실행했다는 증거가 아니며 현재 이미지 검증을 대신하지 않는다.

## 1. 에이전트의 완료 조건

다음 항목을 모두 증거와 함께 보고해야 설치 완료로 간주한다.

1. NAS가 `x86_64` 또는 `aarch64`이고 Docker Standalone 환경임을 확인했다.
2. Windows와 컨테이너 이미지의 `self-test`가 모두 `"status": "pass"`다.
3. Portainer Stack의 `deltaweave-receiver`가 재시작 정책과 영속 경로를 사용해 실행 중이다.
4. 로그에 `"status": "ready"`, 고정된 `endpoint_id`, 하나 이상의
   `direct_addresses`가 출력된다.
5. Windows의 `sync-once`로 Windows→NAS와 NAS→Windows 파일이 모두 전파되고
   세 Merkle root(`desired/local/remote`)가 같다.
6. 동시 수정이 두 장비에 같은 conflict copy를 만들고, 삭제가 반대편에 전파되며,
   마지막 무변경 실행이 양쪽 action/전송 0으로 끝난다.
7. 컨테이너가 privileged 모드, Docker 소켓 마운트 또는 `--allow-any-authenticated`를
   사용하지 않는다.
8. 컨테이너 재시작 뒤 endpoint ID가 유지되고 새 `direct_addresses`를 확인한 Windows의 다음 `sync-once`가
   0-action 또는 필요한 복구 action 후 같은 root로 끝난다.

## 2. 절대 준수할 안전 규칙

- Portainer URL, API 키, GitHub PAT, DeltaWeave `*.key`의 내용을 출력·커밋·채팅 전송하지 않는다.
- 기존 `/volume1/docker/deltaweave`를 삭제하거나 초기화하지 않는다.
- `/data/config/receiver.key`가 바뀌면 NAS endpoint ID도 바뀌므로 업데이트 때 보존한다.
- Stack 파일의 `cap_drop: ALL`, `no-new-privileges`, `read_only`, 비-root UID/GID를 제거하지 않는다.
- `DELTAWEAVE_ALLOWED_PEER`에는 사용자가 확인한 Windows sender endpoint ID만 넣는다.
- Portainer와 DeltaWeave를 인터넷에 직접 노출하지 않는다. LAN 또는 Tailscale 경로를 사용한다.
- 지원되지 않는 CPU, Swarm/Kubernetes, 권한 부족, 해시 불일치가 나오면 우회하지 말고 중단해 보고한다.

## 3. 필요한 입력과 자동 탐색

에이전트는 먼저 가능한 값을 읽기 전용으로 탐색하고, 찾을 수 없는 필수값만 사용자에게
질문한다.

| 변수 | 예시 | 획득 방법 |
| --- | --- | --- |
| `PORTAINER_URL` | `https://nas.example:9443` | 사용자 또는 기존 연결 설정 |
| Portainer 인증 | API key 권장 | 비밀 저장소/사용자 제공; 로그 금지 |
| `DELTAWEAVE_ALLOWED_PEER` | 64자 endpoint ID | Windows의 `init` JSON 출력 |
| `DELTAWEAVE_DATA_DIR` | `/volume1/docker/deltaweave` | NAS 볼륨 확인 후 결정 |
| `PUID`, `PGID` | `1026`, `100` | 해당 데이터 디렉터리 소유 계정의 `id` 결과 |
| NAS 접속 주소 | LAN 또는 Tailscale IP | `direct_addresses`와 실제 라우팅 비교 |

## 4. 사전 점검

NAS SSH 권한이 있으면 다음을 실행한다. 읽기 전용 명령부터 실행하고 출력에 비밀값이
없는지 확인한다.

```bash
uname -m
docker info --format '{{.OSType}}/{{.Architecture}} {{.ServerVersion}}'
docker compose version
df -h /volume1
```

이 실행서의 이미지 대상은 `x86_64`(`linux/amd64`)와 `aarch64`(`linux/arm64`)뿐이다.
CPU가 맞아도 DSM에서 Container Manager/Docker와 host networking을 실제 사용할 수
있어야 한다. 저장소의 Container workflow는 Ubuntu runner에서 두 이미지를 빌드하고
`self-test`를 실행하도록 구성되어 있으며 ARM64에는 QEMU를 사용한다. 물리 NAS 모델별
호환성·장기 실행 검증은 별도로 필요하다. Portainer의 대상 Environment가 이 NAS의
**Docker Standalone**인지 확인하고 이 실행서에서는 사전 빌드 이미지를 사용한다.
NAS의 `docker compose` CLI가 없으면 버전을 미확인으로 기록하고 Portainer의 Compose
처리 지원 여부를 확인한다.

Windows PowerShell에서 릴리즈 바이너리를 먼저 검증한다.

```powershell
cd C:\DeltaWeave
.\deltaweave.exe self-test
.\deltaweave.exe init --identity .\data\sender.key
```

`self-test`의 `status`가 `pass`인지 확인하고 `init` JSON의 `endpoint_id`만
`DELTAWEAVE_ALLOWED_PEER`로 기록한다. `sender.key` 내용은 읽거나 전송하지 않는다.

![기존 Windows x86-64 self-test 출력으로 만든 참고 화면](assets/deltaweave-self-test.png)

## 5. NAS 영속 디렉터리 준비

NAS에서 DeltaWeave를 소유할 기존 비-root DSM 계정을 선택한다. 숫자 UID/GID를 확인한 뒤
새 테스트 경로에 디렉터리를 만든다. `<DSM_USER>`를 실제 계정명으로 교체한다.
아래 `install` 명령의 옵션을 NAS가 지원하는지도 확인한다. 기존 경로에는 적용하지 않는다.

```bash
PUID_VALUE="$(id -u <DSM_USER>)"
PGID_VALUE="$(id -g <DSM_USER>)"
sudo install -d -m 0700 -o "$PUID_VALUE" -g "$PGID_VALUE" \
  /volume1/docker/deltaweave \
  /volume1/docker/deltaweave/config \
  /volume1/docker/deltaweave/index \
  /volume1/docker/deltaweave/received \
  /volume1/docker/deltaweave/state
```

위 값을 Portainer 환경변수 `PUID`, `PGID`로 사용한다. 경로가 이미 존재한다면
소유권과 내용부터 확인하며 재귀 `chown`, 삭제 또는 덮어쓰기를 자동 실행하지 않는다.

## 6. 컨테이너 이미지 확인

Compose의 기본 이미지 참조는 다음과 같다.

```text
ghcr.io/happyaspic-byte/deltaweave:main
```

2026-09-05 읽기 전용 registry 확인에서 `main`과
`sha-75ffab74ee3de4dd1d071c13ab04ae26fb4e6e5b`의 manifest에 `linux/amd64`와
`linux/arm64`가 있었다. 이는 NAS에서의 실행 확인이 아니다. `main`은 이동 태그이므로
실제 사용할 이미지의 manifest·digest를 다시 확인하고 아래 검증에도 같은 참조를 사용한다.
Container workflow는 `main` branch에서 이미지 테스트가 성공하면 `main` 및
`sha-<40자 전체 COMMIT_SHA>` 태그를 게시하도록 구성되어 있다. 모든 commit이나
GitHub Release에 대응하는 이미지 태그가 있다고 가정하지 않는다.

인증이 필요한 이미지일 때만 Portainer의 **Registries**에 `ghcr.io`를 추가한다. GitHub
사용자명과 `read:packages` 권한의 PAT (classic)을 사용하고 토큰을 Stack 환경변수나
Compose 파일에 넣지 않는다. 공개 이미지는 익명 pull이 가능하며 네트워크 오류나
존재하지 않는 태그를 인증 오류로 처리하지 않는다.
[GitHub Container Registry 인증 문서](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-to-the-container-registry)를
따르고, 필수 인증 수단이 없으면 에이전트는 사용자에게 요청하고 중단한다.

배포 전에 NAS에서 이미지를 독립 검증할 수 있다.

```bash
docker pull ghcr.io/happyaspic-byte/deltaweave:main
docker image inspect ghcr.io/happyaspic-byte/deltaweave:main --format '{{json .RepoDigests}}'
docker run --rm ghcr.io/happyaspic-byte/deltaweave:main --version
docker run --rm ghcr.io/happyaspic-byte/deltaweave:main self-test
```

반드시 `"status": "pass"`를 확인한다. 호환 manifest가 있을 때 NAS CPU에 맞는 이미지가
선택된다. Dockerfile의 `DELTAWEAVE_VERSION` 빌드 인자 기본값은 아직 `0.3.0`이므로
OCI version label만으로 바이너리 버전을 판정하지 않는다. `--version`과 digest를 기록한다.

![기존 ARM64/Linux self-test 출력으로 만든 참고 화면](assets/deltaweave-synology-self-test.png)

## 7. Portainer Stack 배포

Portainer에서 다음 값으로 Git repository Stack을 생성한다.

| 항목 | 값 |
| --- | --- |
| Name | `deltaweave` |
| Repository URL | `https://github.com/happyaspic-byte/DeltaWeave` |
| Repository reference | `refs/heads/main` |
| Compose path | `deploy/portainer/compose.yml` |

아래는 Stack 입력 예시다. 비어 있으면 Compose가 거부하는 필수값은
`DELTAWEAVE_ALLOWED_PEER`, `PUID`, `PGID` 세 개다.

```dotenv
DELTAWEAVE_ALLOWED_PEER=<WINDOWS_INIT에서_얻은_ENDPOINT_ID>
DELTAWEAVE_DATA_DIR=/volume1/docker/deltaweave
PUID=<DSM_ACCOUNT_UID>
PGID=<DSM_ACCOUNT_GID>
```

선택 변수:

```dotenv
DELTAWEAVE_IMAGE=ghcr.io/happyaspic-byte/deltaweave:main
DELTAWEAVE_LOG_LEVEL=warn,deltaweave_net=info,netwatch=error
```

| Stack 변수 | 기본값·조건 | 적용 시점과 용도 |
| --- | --- | --- |
| `DELTAWEAVE_ALLOWED_PEER` | 기본값 없음, 필수 | Stack 생성/redeploy 때 `serve --allow-peer` 한 개로 전달 |
| `PUID`, `PGID` | 기본값 없음, 둘 다 필수 | 컨테이너 생성 때 실행 UID/GID 지정; 디렉터리 소유권을 변경하지 않음 |
| `DELTAWEAVE_DATA_DIR` | 미설정·빈 값이면 `/volume1/docker/deltaweave` | 컨테이너 생성 때 `/data`에 bind mount |
| `DELTAWEAVE_IMAGE` | 미설정·빈 값이면 `ghcr.io/happyaspic-byte/deltaweave:main` | 생성/redeploy 때 사용할 이미지 참조 |
| `DELTAWEAVE_LOG_LEVEL` | 미설정·빈 값이면 `warn,deltaweave_net=info,netwatch=error` | 컨테이너의 `RUST_LOG`로 전달; CLI 시작 때 읽음 |

이 값들은 Compose 치환 입력이며 DeltaWeave가 같은 이름의 환경변수를 직접 읽는
설정 체계가 아니다. 변경 후 기존 컨테이너를 단순 restart하는 것만으로 새 설정이
적용되지는 않는다. `PORTAINER_URL`과 Portainer 인증도 배포 도구의 입력이다.
이 Compose에는 사용자 지정 config 파일, 포트 또는 bind 주소 환경변수가 없다.
Dockerfile 단독 실행은 UID/GID `65532:65532`와 `self-test`가 기본이며 Stack은 이를
지정 UID/GID와 `serve`로 바꾼다. `deploy/portainer/docker-compose.yml`은 같은 내용의
대체 파일명이다.

Portainer API/MCP를 사용할 수 있으면 동일한 값으로 Git Stack을 생성해도 된다. 단,
인증 오류를 무시하거나 비밀값을 응답 본문에 노출하지 않는다. API가 없으면 Portainer
UI에서 **Stacks → Add stack → Git repository → Deploy the stack** 순서로 진행한다.

## 8. 배포 직후 검증

Portainer의 Container 상태와 로그를 확인한다. 정상 시작 로그는 다음 형태다.

```json
{
  "status": "ready",
  "endpoint_id": "<SYNOLOGY_ENDPOINT_ID>",
  "direct_addresses": ["<NAS_IP:UDP_PORT>"],
  "relay_urls": []
}
```

실제 NAS LAN 또는 Tailscale IP가 포함된 `IP:UDP_PORT`를 선택한다. DSM 방화벽이
활성화되어 있으면 그 UDP 포트를 Windows 원본 주소에서만 허용한다. 컨테이너는 host
network를 사용하므로 Compose에 port mapping을 추가하지 않는다.

기본 Compose는 `--bind`를 지정하지 않아 UDP 포트가 재시작 때 바뀔 수 있다.
고정 포트가 필요하면 배포할 Compose의 `command`에 CLI 옵션 `--bind`와
`0.0.0.0:49152`를 추가하고 해당 UDP 포트의 사용 가능 여부와 방화벽을 확인한다.
이는 선택적 Compose 수정이며 기본 설정에는 포함되지 않는다.

컨테이너를 한 번 재시작하고 `endpoint_id`가 동일한지 확인한다. 달라졌다면
`/data` 마운트, identity 경로와 파일 보존 여부를 확인한다. ID가 같아도 새
`direct_addresses`에 맞게 Windows의 `--direct`와 방화벽 설정을 갱신해야 한다.

## 9. Windows↔NAS 종단간 양방향 시험

먼저 `push`로 10 MiB 이상의 **복사본 테스트 파일**에 대한 CDC 델타 경로를
검증해도 된다. 아래 파일과 폴더는 모두 운영 데이터와 분리한 테스트 사본이어야 한다.

```powershell
.\deltaweave.exe push C:\Test\sample.bin `
  --remote-path validation/sample.bin `
  --peer <SYNOLOGY_ENDPOINT_ID> `
  --direct <SYNOLOGY_IP:UDP_PORT> `
  --identity .\data\sender.key `
  --direct-only
```

무결성을 비교한다.

```powershell
Get-FileHash C:\Test\sample.bin -Algorithm SHA256
```

```bash
sha256sum /volume1/docker/deltaweave/received/validation/sample.bin
```

두 값이 같아야 한다. 이어서 Windows 파일에 소량을 추가하고 같은 `push`를 다시 실행한다.

```powershell
[IO.File]::AppendAllText("C:\Test\sample.bin", "DeltaWeave delta test")
```

두 번째 receipt의 `reused_extents > 0`, `transferred_bytes < 전체 파일 크기`와 변경 후
양쪽 SHA-256 일치를 확인한다.

Windows private state는 동기화 root 밖에 둔다. 아래 `sync-once`는 NAS의
`/data/received` 전체와 동기화하므로 앞서 push한 `validation/sample.bin`도 Windows로
내려온다. `--remote-path`는 `push`의 파일 경로이며 `sync-once`의 원격 폴더 선택 옵션은 없다.

```powershell
New-Item -ItemType Directory -Force C:\DeltaWeave-Sync | Out-Null
New-Item -ItemType Directory -Force C:\DeltaWeave-Private | Out-Null
Set-Content C:\DeltaWeave-Sync\windows-only.txt "from Windows"

.\deltaweave.exe sync-once `
  --root C:\DeltaWeave-Sync `
  --state C:\DeltaWeave-Private\state `
  --identity .\data\sender.key `
  --peer <SYNOLOGY_ENDPOINT_ID> `
  --direct <SYNOLOGY_IP:UDP_PORT> `
  --direct-only
```

NAS의 `/volume1/docker/deltaweave/received/nas-only.txt`에 복사본 파일을 만든 뒤
같은 명령을 다시 실행한다. 양쪽 전용 파일이 반대편에 나타나고 JSON의
`desired_root`, `verified_local_root`, `verified_remote_root`가 같아야 한다.

이어 다음 안전 시나리오를 순서대로 수행한다.

1. 같은 `shared.txt`를 한 번 동기화한다.
2. 다음 sync 전에 Windows/NAS의 내용을 서로 다르게 수정한다.
3. `sync-once`가 두 장비에 동일한 `.conflict-<hash>` 파일을 만들었는지 확인한다.
4. Windows 전용 파일을 삭제하고 다시 실행해 NAS에서도 사라졌는지 확인한다.
5. 한 번 더 실행해 `merkle_queries=1`, 양쪽 action 0, 전송 0인지 확인한다.

그 후에만 컨테이너 안에서 별도 진단 인덱스로 수신 폴더를 검사할 수 있다.

```bash
docker exec deltaweave-receiver deltaweave scan \
  --root /data/received \
  --state /data/index/received.redb \
  --identity /data/config/receiver.key \
  --include-records
```

`report.issues`와 `report.collisions`가 비어 있고 live record가 포함되어야 한다.
이 `scan`은 동기화 파일을 읽고 별도 진단 DB를 생성·갱신한다. `--state`는 여기서는
redb 파일이고, `serve`/`sync-once`에서는 private 디렉터리다. 실행 중인 receiver의
`/data/state/index.redb`를 진단 DB로 열지 않는다. 지속 동기화는 Windows에서 같은
인자의 `sync --interval-seconds 5`를 사용한다. 기본 local watcher quiet window는
750ms, 최대 debounce는 5000ms이며 성공한 pass 뒤 5초 대기로 원격 변경도 확인한다.
실행 시간·watcher 상태·오류 backoff 때문에 5초 이내 완료를 보장하지 않는다.
시작 JSON의 `local_change_detection`이 `native_watcher`인지
기록하고, `polling_fallback`이면 `watcher_error`도 보고한다. 자세한 실패 판정은
[로컬 인덱스 검증서](TESTING_LOCAL_INDEX.md)를 따른다.

## 10. 업데이트, 롤백, 백업

- 업데이트: 진행 중인 동기화와 receiver를 중지해 일관된 `/volume1/docker/deltaweave`
  백업을 확보하고 새 이미지를 검증한 뒤 Stack을
  redeploy한다. **볼륨을 제거하지 않는다.**
- 롤백: registry에 존재하고 정상 동작했던 `ghcr.io/happyaspic-byte/deltaweave:sha-<40자 전체 COMMIT_SHA>`
  또는 digest를 사용한다. 구버전과 현재 state의 호환성을 백업 사본에서 확인한 뒤
  `DELTAWEAVE_IMAGE`로 지정해 redeploy한다. 임의 버전 간 state 호환성을 가정하지 않는다.
- 최소 백업 대상: `config/receiver.key`, `state/`, `received/`. `index/`는 이 실행서에서
  만드는 별도 진단 DB이며 `scan`으로 다시 만들 수 있다. 권위 있는 인덱스
  `state/index.redb`는 `state/` 백업에
  반드시 포함한다. Windows의 identity와 private state도 별도로 보존한다.
- `main`은 pre-alpha 이동 태그다. 반복 가능한 장기 배포에는 검증한 `sha-...` 태그나
  이미지 digest를 고정한다.

## 11. AI 최종 보고 형식

비밀값과 전체 endpoint ID는 마스킹하고 다음 형식으로 보고한다.

```text
DeltaWeave Portainer 배포 결과: PASS 또는 FAIL
NAS: <model/arch>, Docker <version>, Portainer <version>
Image: <tag 또는 digest>
Container: running 여부 / restart 검증
Receiver ID: 앞 8자...뒤 4자, 재시작 후 동일 여부
Address: <LAN 또는 Tailscale IP>:<UDP port>
Self-test: status / reused_extents / transferred bytes
Cross-device: 양방향 파일 PASS/FAIL, 세 Merkle root 일치 PASS/FAIL
Conflict/delete/no-op: conflict copy / 삭제 전파 / 0-action 재실행
Delta retry: reused_extents / transferred bytes / SHA-256 일치
Local index: generation / live records / issues / restart 유지 여부
Persistence: config/state/received 경로 및 백업 여부
남은 위험 또는 차단 사항: <없음 또는 구체적 오류>
```

문제가 생기면 [Windows PC ↔ Synology 테스트 문서](https://github.com/happyaspic-byte/DeltaWeave/blob/main/docs/TESTING_WINDOWS_SYNOLOGY.md)의
문제 해결 표도 함께 확인한다. 릴리스 아카이브에서는 같은 문서가 `TESTING.md`로 제공된다.
