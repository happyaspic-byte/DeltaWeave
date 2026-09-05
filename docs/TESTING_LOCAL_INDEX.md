# DeltaWeave 로컬 인덱스 검증

이 문서는 Windows PC와 Synology/Linux에서 `scan`·`watch` 기능을 검증하는
절차다. 명령과 기본값은 현재 저장소의 v0.4.0 구현을 기준으로 한다.
`scan`·`watch`는 로컬 인덱스만 갱신한다. 다른 장비와의 양방향 전송에는
`sync-once`·`sync`를 사용하며, 절차는 [Windows/Synology 검증 안내](https://github.com/happyaspic-byte/DeltaWeave/blob/main/docs/TESTING_WINDOWS_SYNOLOGY.md)에 있다.
릴리스 아카이브에서는 장비 간 검증 안내가 `TESTING.md`로 제공된다.
반드시 복사본 테스트 디렉터리를 사용한다. 아래 Windows/DSM 절차는 현재
문서 수정에서 해당 하드웨어로 재실행한 결과가 아닌 운영자 검증 절차다.

## 과거 실행 예시: 전·중·후·결과

아래 GIF는 v0.2.0 실행값으로 기록된 세 파일 스캔과 native watcher 이벤트를
문서용 터미널 그림으로 렌더링한 예시다. 생성기는
`scripts/render-doc-visuals.sh`이며, 고정된 출력 발췌를 렌더링할 뿐 CLI를 실행하지
않는다. 서로 다른 스캔을 묶은 화면이므로 generation이 한 실행처럼 이어지지
않으며, 현재 바이너리의 실행 증거나 전체 JSON으로 해석하지 않는다.

![로컬 인덱스 실행 전, 중, 후, 결과](assets/deltaweave-index-lifecycle.gif)

각 프레임을 확대해서 보려면 [사용 화면 갤러리](USAGE_GALLERY.md)를 연다.

- 전: 시험 root와 private state를 분리한다.
- 중: 초기 스캔 후 `status=watching`을 확인한다.
- 후: 파일 생성이 `native_events`와 `watch_scan`으로 기록된다.
- 결과: authoritative scan의 `issues`, `collisions`, retry가 비어 있다.

## 합격 기준

- 최초 스캔이 루트 자체를 제외한 인덱싱 가능한 파일·디렉터리 수를 보고한다.
  `files_hashed`는 이번 스캔에서 해시에 성공한 일반 파일 수다.
- 변경 없는 다음 스캔은 `changes: []`다.
- 전체 `scan`은 변경 없는 파일도 다시 해시하므로 `files_hashed=0`이 합격 조건은 아니다.
- 이름 변경은 안정 파일 ID와 내용·메타데이터가 일치하고 후보가 유일할 때
  `renamed`로 상관관계가 잡힌다. 그 외에는 생성·삭제로 기록될 수 있다.
- 삭제 후 이전 경로는 사라지지 않고 tombstone으로 남는다.
- 프로세스 재시작 후 generation과 records가 유지된다.
- Windows 공유 잠금 등 일반 파일의 해시 읽기 실패는 retry queue에 남는다.
  디렉터리 열거 실패는 기존 레코드를 보존하지만 루트 자체를 열거하지 못하면
  명령이 실패한다.
- Linux의 대소문자/Unicode 충돌은 두 레코드를 모두 보존하고 `collisions`로 보고한다.
- `watch`는 이벤트 폭주를 debounce하며, 주기 전체 스캔을 계속 수행한다.

## Windows PowerShell

릴리스 압축을 푼 디렉터리에서 실행한다.

```powershell
$Root = "C:\DeltaWeave-Test\root"
$Private = "C:\DeltaWeave-Test\private"
New-Item -ItemType Directory -Force $Root, $Private | Out-Null
[IO.File]::WriteAllText((Join-Path $Root "before.txt"), "first version")

.\deltaweave.exe scan `
  --root $Root `
  --state (Join-Path $Private "index.redb") `
  --identity (Join-Path $Private "node.key") `
  --include-records
```

`--include-records` 결과는 `report`, `records`, `retries`를 출력한다. `report`에는
`status` 필드 대신 `generation`, `live_records`, `files_hashed`, `changes`,
`collisions`, `issues`가 있다. `report.issues`가 비어 있고
`report.live_records`와 제외 경로 및 루트 자체를 뺀 실제 항목 수가 같아야 한다.
`--include-records`를 생략하면 같은 report 객체가 최상위 JSON으로 출력된다.
DB는 canonical root와 identity에서 파생한 replica ID에 연결되므로 재실행에는
같은 root, DB, 키를 사용한다. `watch`가 DB를 열고 있는 동안 같은 DB로 별도
`scan`을 실행하지 않는다.

`--hash-workers`는 양수로 지정하며 기본값은 사용 가능한 CPU 수를 최대 8개로
제한한다. `--ignore`는 반복 가능한 경로 옵션이며 glob 패턴이 아니다. 상대
경로는 실행 디렉터리를 기준으로 해석한다. identity 파일은 자동으로 제외하며,
DB가 root 하위 폴더에 있으면 그 폴더도 제외한다. DB가 root 바로 아래 있으면
DB 파일만 제외한다. 이미 인덱싱된 경로를 나중에 제외해도 이전 레코드는 유지된다.

이름 변경과 삭제를 각각 수행한 뒤 같은 `scan`을 다시 실행한다.

```powershell
Rename-Item (Join-Path $Root "before.txt") "after.txt"
# scan 명령 재실행: changes에 kind=renamed 확인
Remove-Item (Join-Path $Root "after.txt")
# scan 명령 재실행: changes에 kind=deleted, tombstones 증가 확인
```

Windows 공유 잠금 재시도는 별도 시험 파일로 확인한다.

```powershell
$Locked = Join-Path $Root "locked.bin"
[IO.File]::WriteAllText($Locked, "locked data")
$Handle = [IO.File]::Open($Locked, 'Open', 'ReadWrite', 'None')
# 이 상태에서 scan 실행: hash_failed issue와 retries_queued 증가 확인
$Handle.Dispose()
Start-Sleep -Seconds 1
# scan 재실행: 파일 인덱싱 성공과 retries_queued 감소 확인
```

반복 실패하면 retry 대기 시간이 늘어난다. 너무 일찍 다시 검사하면
`retry_deferred`가 나오므로 `--include-records`의 `retries[].not_before_ms`
(Unix epoch 밀리초)를 확인한다.

연속 감시는 다음과 같이 실행하고 다른 PowerShell 창에서 파일을 생성·수정·이름
변경한다. 기본값은 750 ms quiet window, 5초 최대 debounce, 10분 전체 검증,
watcher 장애 시 5초 폴링이다.

```powershell
.\deltaweave.exe watch `
  --root $Root `
  --state (Join-Path $Private "index.redb") `
  --identity (Join-Path $Private "node.key")
```

native watcher가 정상인 경우 파일 변경 뒤 `watch_scan` JSON과 0보다 큰
`native_events`를 확인한다. 이벤트 수는 OS와 작업에 따라 달라진다.
`periodic_scan`은 기본 600초마다 전체 해시 검증을 수행하며,
`rescan_required=true`는 해당 출력에서 전체 스캔을 수행했다는 뜻이다.
종료는 `Ctrl+C`를 사용한다.

![v0.2.0 native watcher 파일 생성 감지 예시](assets/deltaweave-index-watch.png)

## Synology 또는 Linux

동기화 시험 루트와 private state를 분리한다.

```bash
mkdir -p /volume1/DeltaWeave-Test/root /volume1/DeltaWeave-Test/private
printf 'first version\n' > /volume1/DeltaWeave-Test/root/before.txt

./deltaweave scan \
  --root /volume1/DeltaWeave-Test/root \
  --state /volume1/DeltaWeave-Test/private/index.redb \
  --identity /volume1/DeltaWeave-Test/private/node.key
```

대소문자 충돌을 지원하는 파일시스템에서는 다음 두 이름을 만든 뒤 스캔한다.

```bash
printf 'upper\n' > /volume1/DeltaWeave-Test/root/Report.txt
printf 'lower\n' > /volume1/DeltaWeave-Test/root/report.txt
```

결과의 한 collision group에 두 경로가 모두 있어야 하며 `live_records`도 둘을
각각 계산해야 한다. 충돌을 자동으로 이름 변경하거나 삭제하지 않는 것이 정상이다.

연속 감시는 다음 명령으로 확인한다.

```bash
./deltaweave watch \
  --root /volume1/DeltaWeave-Test/root \
  --state /volume1/DeltaWeave-Test/private/index.redb \
  --identity /volume1/DeltaWeave-Test/private/node.key
```

inotify 한도 부족 등으로 native watcher 생성에 실패하면 초기 JSON의
`status`가 `polling_fallback`이고 `watcher_error`가 원인을 설명한다.
실행 중 watcher 오류나 모호한 이벤트도 `watcher_degraded=true`로 전환한다.
이 경우 기본 5초 간격으로 전체 `fallback_scan`을 수행한다. 같은 시점에
주기 검증도 도래하면 이벤트 이름은 `periodic_scan`이다. 스캔 실행 시간에
따라 출력 간격이 늘어날 수 있다.

## Portainer 컨테이너에서 받은 파일 검사

수신 컨테이너가 실행 중이면 별도 검사 DB로 `/data/received`를 한 번 검사할 수 있다.
받는 파일이 계속 바뀌면 일시적인 읽기 issue가 생길 수 있으므로 전송이 끝난
뒤 다시 확인한다. `serve`가 사용하는 `/data/state/index.redb`를 이 명령의
`--state`로 지정하지 않는다.

```bash
docker exec deltaweave-receiver deltaweave scan \
  --root /data/received \
  --state /data/index/received.redb \
  --identity /data/config/receiver.key \
  --include-records
```

컨테이너 재시작 후 같은 명령을 실행해 generation이 증가하고 기존 레코드가 유지되는지
확인한다. 기본 Compose의 `/data` 바인드 마운트에 검사 DB도 영속한다.
이 별도 검사 DB를 새로 만들면 이전 generation, tombstone, retry 이력은
복원되지 않는다. 수신기의 동기화 상태인 `/data/state`와 identity는 별도로 보존한다.

## 실패 보고에 포함할 내용

- OS/DSM 버전과 CPU 아키텍처
- DeltaWeave 버전 및 패키지 SHA-256
- 실행 명령(키 내용 제외)
- 전체 `ScanReport` JSON과 관련 로그
- 실제 디렉터리 트리, 기대한 change, 실제 change
- 재시작 전후 generation 및 retry 수

실패했다고 테스트 데이터를 삭제하거나 index DB를 초기화하지 않는다. DB와 최소 재현
디렉터리의 복사본을 보존한 뒤 이슈에 비밀값과 실제 사용자 파일이 포함되지 않았는지
확인한다.
