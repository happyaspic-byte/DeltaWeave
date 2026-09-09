# F 3-host qSync bootstrap 도구

이 문서는 `scripts/qsync_three_host_bootstrap.py`와
`scripts/qsync_three_host_role.py`의 사용 계약이다. 제품 crate의 동작을 대신하지
않으며, 현재 실행 결과를 full F 완료로 승격하지 않는다.

## 역할과 provenance

최종 토폴로지는 로컬 Linux owner, 승인된 Windows Server RW provider, GitHub-hosted
Ubuntu RO consumer다. 세 로컬 프로세스를 세 기기로 세지 않는다. role manifest는
각 플랫폼 descriptor에 `source_sha`, `workflow_sha`, `target`, `sha256`, `size_bytes`를
넣는다. Linux와 Windows executable hash가 다른 것은 정상이며 세 descriptor가 선택한
동일 source/workflow SHA를 가리키는지와 각 파일의 hash·크기를 모두 확인해야 한다.

`qsync_make_role_manifest.py`는 기존 native Windows passing manifest와 Linux binary를
검사한 뒤 owner/RO Linux 및 RW Windows descriptor를 만든다. source SHA만 비교하거나
Windows hash를 Linux binary에 강제하지 않는다. Windows 전달은
`qsync_three_host_winrm_member.ps1`를 WinRM NTLM message-encryption 세션에서 실행하고,
controller가 여는 one-use binary-only HTTP route에서 받은 파일을 Windows에서 다시
hash한다. Windows 경로는 controller에서 `pathlib.Path`로 변환하지 않고 opaque 문자열로
전달한다.

RO reusable workflow는 선택한 commit을 checkout하고 Linux binary를 새로 빌드한 뒤
`qsync_three_host_role.py`를 실행한다. role driver는 새 runner 임시 namespace와 private
profile을 만들고 local preview, key가 가리키는 owner의 online validate, join, 파일
hash, 종료 후 같은 state의 membership 재조회를 수행한다. workflow artifact에는 고정
phase와 hash·크기만 남긴다. RW는 hosted runner에서 WinRM으로 실행하지 않는다. local
controller가 승인 Windows에서 `qsync_three_host_winrm_keepalive.py`를 실행한 상태로
같은 source/ref의 workflow를 dispatch하고, workflow가 올린
`qsync-f-ro-evidence-<github_run_id>`만 내려받아 local gate에서 RW evidence와 비교한다.
RO 결과가 없어도, RW가 먼저 끝나도, gate는 3-host 성공으로 승격하지 않는다.

## 입력과 비밀 경계

JSON config에는 raw password, bearer, token, credential, endpoint URL을 넣지 않는다.
config는 다음과 같은 environment-name만 받는다.

```json
{
  "source_sha": "<40 lowercase hex>",
  "artifact_manifest": "/absolute/private/f-role-manifest.json",
  "run_parent": "/absolute/private/f-runs",
  "owner_api_url_env": "QSYNC_F_OWNER_API_URL",
  "artifact_public_host_env": "QSYNC_F_ARTIFACT_PUBLIC_HOST",
  "require_share_swarm": true,
  "require_relay": true,
  "roles": {
    "owner": {"runner": "local", "binary": "/absolute/linux/deltaweave", "binary_sha256": "<sha256>"},
    "rw_provider": {
      "runner": "winrm", "binary": "/absolute/windows/deltaweave.exe", "binary_sha256": "<sha256>",
      "destination_env": "QSYNC_F_WINRM_DESTINATION",
      "winrm_host_env": "QSYNC_F_WINRM_HOST",
      "winrm_username_env": "QSYNC_F_WINRM_USERNAME",
      "winrm_password_env": "QSYNC_F_WINRM_PASSWORD"
    },
    "ro_consumer": {"runner": "github_hosted", "binary_sha256": "<sha256>"}
  }
}
```

실제 비밀 값은 환경에만 잠시 존재하고 `SecretVault`에서 API/WinRM 요청 중에만
참조한다. 앱 자식에는 allowlist 된 runtime 변수와 새 `HOME`, `USERPROFILE`, XDG,
`TMP` profile만 전달한다. raw key, bearer, URL, credential, PowerShell stdout/stderr,
CLI argument는 로그·evidence·artifact에 쓰지 않는다. reusable workflow caller는
`ro_share_key_secret_name`에 매 실행마다 만든 secret 이름을 지정하고, 그 값을 호출
workflow의 `QSYNC_F_RO_SHARE_KEY` 별칭에만 매핑한다. workflow는 secret을 만들거나
덮어쓰거나 삭제하지 않는다. WinRM controller의 owner URL과 계정은 controller 환경에서만
`QSYNC_F_OWNER_API_URL`, `QSYNC_F_WINRM_USERNAME`, `QSYNC_F_WINRM_PASSWORD`로
주입한다. hosted RO job에는 owner 관리자·WinRM credential을 전달하지 않는다.

WinRM은 `Session.run_ps`를 사용하지 않는다. pywinrm의 해당 편의 메서드는
`powershell -encodedcommand ...`를 Windows command shell로 보내므로, wrapper의
UTF-16LE/Base64 길이는 메모리에서 측정하되, 실제 payload는 `powershell -Command -`의
stdin으로 4,096-byte 조각씩 보낸다. `Protocol.run_command`에는
`skip_cmd_shell=True`를 지정해 `powershell.exe`를 직접 실행한다. `open_shell`,
`send_command_input`, `get_command_output`, `cleanup_command`, `close_shell`을 모두
같은 호출에서 정리하며, 정리 실패나 512 KiB를 넘는 stdin payload는 고정 오류로
실패시킨다. 직접 command는 Windows의 32,767-byte 한계도 검사한다. 길이 검사는
payload를 기록하지 않고 숫자만 산출한다. 비밀 없는 대표 config에서 wrapper/stdin은
8,258 bytes, legacy UTF-16LE/Base64 payload는 22,024 bytes, 기존 run_ps command는
22,051 bytes, 직접 WinRS command는 84 bytes였고 command-shell 8,191-byte 경계를
넘는 payload를 4,096-byte 조각으로 전송하는 fake Protocol 및 승인 서버 hello에서
이 경계를 확인했다.

wrapper의 `ConfigB64`는 gzip으로 압축한 UTF-8 JSON을 Base64로 감싼 형식이며,
Windows `Decode-Config`도 같은 순서로 Base64 해제 후 gzip 해제를 수행한다. 승인된
Windows 서버에서 비밀 없는 fixture를 대상으로 이 decoder를 직접 실행한 read-only
roundtrip은 2026-09-09T04:52:02Z에 authenticated=true, roundtrip=true로 끝났다.
원격 파일·서비스·identity 변경은 없었고, 결과의 고정 SHA-256만
`/home/ubuntu/project/DeltaWeave-qsync-evidence/2026-09-09/f-bootstrap/` 아래에
보존했다.

## 실행 순서와 판정

```text
validate-config → validate-artifact → role manifest 생성
→ 각 role binary hash/source gate → 각 host self-test
→ owner create → RO/RW key issue → preview → validate → join
→ 실제 destination file hash/size → restart membership read → cleanup
```

`run`은 `--execute-external` 없이는 외부 host를 호출하지 않고 `blocked`를 기록한다.
WinRM role은 실제 native binary hash/self-test, fresh Windows root/data/profile, API
join, 파일 hash, restart membership를 원격 phase로 보고한다. GitHub RO는 reusable
workflow의 `execute_ro`가 명시적으로 true일 때만 secret을 읽는다. 실패하거나
response가 없는 phase는 `failed`/`pending`/`blocked`로 남기며 passing manifest로
바꾸지 않는다.

Windows helper는 앱을 test-owned dedicated console에 붙이고, console process list가
helper와 해당 child만 포함하는지 확인한 뒤 `CTRL_C_EVENT`를 보낸다. helper 자신의
handler는 신호를 소비하고 child만 앱 신호 경로를 처리하게 한다. child가 실제로
신호를 받은 뒤 exit code 0으로 종료하고 cleanup phase가 `signal=ctrl_c`를 내보내야
Windows role의 graceful signal을 통과시킨다. 이는 앱의 정상 종료·재시작 경계를
검증하는 것이며, 양 끝점의 managed revoke/pause drain acknowledgement나
share-swarm 공급자 drain을 대신하지 않는다.

`providers[].verified_chunks`와 `verified_bytes`는 실제
`deltaweave/share-swarm/1` provider payload 관측값이다. 이 bootstrap의 파일 hash
성공은 `file_hash_verified[role]`의 SHA-256과 `size_bytes`로만 기록한다. 따라서 현재
값은 0이며 파일 hash를 provider chunk 전송으로 과장하지 않는다. D/E의 N0 lookup,
member discovery, relay session/payload와 `share-swarm/1` payload hook가 실제로
연결된 뒤에만 해당 phase를 별도 증거로 채울 수 있다. legacy sync 경로로 대체하지
않는다.

정상 종료와 강제 종료는 `forced_termination_used`로 구분한다. graceful drain을
확인하지 못한 상태에서 complete/cleanup 성공을 주장하지 않는다. run 소유 namespace만
삭제하고, 종료·drain이 확인되지 않으면 state를 보존하고 cleanup을 pending으로
기록한다. Linux local process와 bilateral managed pause/revoke drain ACK adapter가
없는 경로는 `graceful_drain_proven=unverified`로 남긴다. Windows의
`graceful_signal=ctrl_c`는 별도로 앱 신호와 exit code를 입증하지만 원격 writer/reader
drain 완료를 의미하지 않는다. pre-existing 보호 상태는 실제 before/after 비교가
없으면 `unverified`로 남는다.

## 검증 범위

source `7fdc98b84929c07d7f2a3e26939cb46a6199fa2a`에서 빌드한 Linux release binary로
같은 호스트에서 owner와 한 RO member subprocess를 실제 실행한 smoke는
2026-09-09T04:37:46Z~04:37:50Z에
owner create → RO key 발급 → preview → online validate → join → fixture file
SHA-256/크기 확인까지 모두 통과했다. binary SHA-256은
`7df0b2002d2b3b41f09870281f63e834a41c4d41557e241fd448a159d661e503`, 크기는
`32245040` bytes이며 fixture는
`8b666f88f7b033f647f9b5ae66d668b7bb88376630dbecfb0fba757f4f84334c`, `262144`
bytes였다. 이는 두 local process의 API 경로 확인이며 3-host 실행이나 provider
payload 증거가 아니다. 두 process는 forced termination 없이 종료됐지만 managed
drain ACK adapter가 없어 smoke namespace는 보존하고 `drain_ack=unverified`로
기록했다. 상세 비밀 없는 기록은
`/home/ubuntu/project/DeltaWeave-qsync-evidence/2026-09-09/f-bootstrap/` 아래에
둔다.

다음은 이 checkpoint에서 실행하는 로컬 비밀 없는 검사다.

```text
python3 -m py_compile scripts/qsync_three_host_bootstrap.py scripts/qsync_three_host_coordination.py scripts/qsync_three_host_role.py scripts/qsync_three_host_winrm_keepalive.py tests/tools/test_qsync_three_host_coordination.py tests/tools/test_qsync_three_host_f_manifest.py
python3 -m unittest discover -s tests/tools -p 'test_qsync_three_host*.py' -v
```

GitHub reusable workflow dispatch, 새 encrypted secret 생성, 외부 3-host 실행, 실제
N0/relay, `share-swarm/1` 다중 provider payload, Edge/CDP와 full F acceptance matrix는
다음 source checkpoint에서 실행한다. 이 문서와 local checks는 실행 경로와 fail-closed
계약을 검증할 뿐 해당 미실행 항목의 성공을 뜻하지 않는다.

## Bounded WinRM과 Windows 로컬 종료 재검증

`_run_winrm_powershell`은 `get_command_output_raw`의 단일 Receive 응답만 사용한다.
open/run/stdin/Receive 각 호출 전에 monotonic 전체 deadline을 확인하고, Receive
operation timeout은 제한된 횟수로 부분 stdout/stderr 바이트와 마지막 고정 phase를
메모리에 유지한다. cleanup command/close shell은 별도 짧은 deadline에서 시도하고,
실패·불명확한 child 상태는 `pending`과 상태 보존으로 기록한다. 원격 출력은 고정
`FTRACE`/`FROLE` parser를 통과한 phase, count, elapsed, error class만 evidence에
남긴다. PowerShell helper의 stdout/stderr는 .NET `CopyToAsync(Stream.Null)`로
배수하며 callback scriptblock을 사용하지 않는다.

원격 complete 판정은 binary artifact의 SHA/크기와 destination fixture의 SHA/크기를
서로 다른 입력으로 비교한다. transport error class가 남거나 cleanup이 불완전하면
모든 phase가 보였어도 complete가 될 수 없다.

새 test-owned console 실행(retry9, 2026-09-09T07:03:33Z~07:06:38Z)은 source
`2f44d9fbfbe1c4779bd59f78fe9cc4ff27f41cd6`, Windows artifact
`186b9e177d8b3a8246ceb994d00402b827fb8481b26642edf4ce62983988453a`/33,488,896
bytes, generated script SHA
`e72532d74ebcda2b746d0079f01ff72f5de754d0fa01529bb87162d87e9c5ac2`를 사용했다.
owner create/key issue, member preview/validate/join, 실제 fixture SHA
`8b666f88f7b033f647f9b5ae66d668b7bb88376630dbecfb0fba757f4f84334c`/262,144 bytes,
첫 member와 owner의 `console_test_ready(count=2)`, Ctrl+C, exit 관측, stream drain,
console release는 통과했다. 재시작 console에서는 추가 1개가 고정 경로·부모 관계상
test-owned 시스템 PowerShell로 분류된 뒤 count=2로 회복했다. 이는 ACL helper가
실패 원인이라는 확정 증거가 아니다.

재시작 후 membership 조회와 최종 cleanup은 bounded 전체 timeout으로 완료되지 않아
결과는 `pending`이다. transport cleanup은 true였고 별도 격리 orphan 확인은
`matched=0, remaining=0`이었다. 따라서 동일 identity 재오픈·정상 cleanup pass,
3-host F, bilateral drain ACK, E `share-swarm/1` provider payload를 주장하지 않는다.
retry8은 pywinrm 없는 system Python으로 원격 실행 전 실패한 실행기 오류이며 별도
failed evidence로 보존했다. retry9 상세 고정 evidence는
`f-bootstrap/windows-local-graceful-2f-retry9/` 아래에 있다.

retry9의 transport/phase 기록은 최종 binary/fixture 독립 gate 보정 전 생성된 실행
기록이므로, 현재 checkpoint의 complete 결과로 재해석하지 않는다. 당시 gate와 28개
회귀 결과는 commit `df0ba52a058c97523ea119aa5f6296656fe44dd7`에서 다시 확인했고,
정확한 source hash와 명령 시각은 `f-bootstrap/winrm-bounded-contract-final/`
검증 ledger에 보존했다.

## 현재 bounded/reopen 및 동시 역할 checkpoint

위의 retry9와 28개 회귀 기록은 과거 checkpoint다. 현재 도구는 reopen HTTP 응답과
membership의 `share_id`, `role`, `permission`, 재조회 파일의 hash·크기를 함께
확인하고, `reopen_checks=63` 및 각 reopen trace의 정확히 한 번의 관측을 요구한다.
성공 phase에 오류 필드가 없으면 parser는 `error_class=none`으로 기록한다. RW
runner도 원본 Windows binary를 바로 전송하지 않고 run-owned 0700 복사본을 해시
검증한 뒤 사용한다. 정상적으로 원격 정리가 확인된 경우에만 그 복사본을 지우며,
timeout·forced·불명확한 상태에서는 `run_owned_copy_removed=false`와 상태 보존을
기록한다. coordination gate는 이 조건을 만족하지 않으면 pass를 만들지 않는다.

2026-09-09의 retry10은 source `2f44d9fbfbe1c4779bd59f78fe9cc4ff27f41cd6`, Windows
artifact SHA `186b9e177d8b3a8246ceb994d00402b827fb8481b26642edf4ce62983988453a`,
크기 `33488896` bytes를 사용했다. owner/member preview·validate·join과 최초
fixture hash `8b666f88f7b033f647f9b5ae66d668b7bb88376630dbecfb0fba757f4f84334c`,
`262144` bytes는 통과했지만 reopen membership의 HTTP status가 0이고 checks가
9로 끝나 전체 결과는 `pending`이었다. 별도 read-only process probe는
`authenticated=true`, `deltaweave_process_count=0`, `web_ready=false`였다.
bodyless GET 수정 뒤의 retry11은 reopen 전에 test-owned console에 분류할 수 없는
추가 process가 남아 `console_control_failed`로 안전하게 중단됐다. 전체 결과는
`pending`, transport cleanup은 true, 별도 probe는 동일하게 orphan 0이었다. 따라서
retry10/11 어느 것도 graceful reopen pass나 3-host pass를 증명하지 않는다.

콘솔 소유권을 짧게 재표본화하고 정확히 owner+target일 때만 Ctrl+C를 보내도록 한
retry12(07:53:30Z~07:54:35Z)는 member 재시작 뒤 membership과 fixture file hash,
`reopen_checks=63`을 통과했다. 마지막 재시작 member가 `stop_exit_timeout`에 걸려
강제 정리로 끝났으므로 전체 결과는 여전히 `pending`이며 graceful cleanup 증거가
아니다. 직후 읽기 전용 process probe(07:58:30Z)는 인증·조회에 성공했고
`deltaweave=0`, `web=0`, orphan 0을 확인했다. 이 결과는 남은 종료 제어 경계를
분리해 보여주며 3-host F 또는 E bilateral drain ACK를 증명하지 않는다.

`qsync_three_host_winrm_keepalive.py`는 local controller에서 실행하며,
`provenance-and-linux-build` 뒤 hosted Ubuntu RO job과 겹치는 keepalive 구간을 안전한
evidence로 남긴다. controller는 workflow run을 찾을 때 head/source SHA와 dispatch 시각을
대조하고, 해당 run의 RO artifact를 bounded poll로 내려받은 뒤 local coordination gate를
호출해야 한다. 이 저장소 checkpoint에는 role driver와 hosted RO workflow, 그리고 gate
계약만 포함되며 그 dispatch/download controller는 아직 구현·실행하지 않았다. gate는
RO의 `run_id`, RW의 `run_id`, source SHA, fixture hash·크기, 실제
keepalive 구간이 같은 실행을 가리키는지 확인한다. Linux/Windows executable hash는 달라도
되며 각 role의 `source_sha`, `workflow_sha`, `target`, hash, 크기를 따로 검증한다.
keepalive 구간이 RO join 및 파일 검증 phase와 실제로 겹치지 않으면 실패한다. workflow는
evidence 디렉터리를 runner에 미리 만들지 않고 각 driver가 0700으로 단독 생성하게 한다.
hosted runner에는 RO key만 per-run 별칭으로 주입하고 owner API, RW key, WinRM credential은
주입하지 않는다. controller, RO key, owner endpoint 중 하나라도 준비되지 않으면 역할은
`blocked`/`pending`이다. 이는 실행 가능한 handoff 경로를 제공하지만 이 환경에서 3 host,
bilateral drain ACK, E share-swarm provider payload의 성공을 주장하지 않는다.

현재 로컬 검증은 `py_compile` exit 0, `python3 -m unittest discover -s tests/tools
-p 'test_qsync_three_host*.py' -v` 36 tests exit 0, 두 workflow YAML parse exit 0,
`git diff --check` exit 0이다. `actionlint`는 이 실행 환경에 설치되어 있지 않아
사용하지 못했다. CI의 caller는 reusable workflow에 job-level `contents:read`와
`actions:read`만 전달하도록 수정됐고, Linux/Windows web test는 `npm --prefix web
test -- --run`, CLI self-test는 `cargo run --locked --all-features -p deltaweave --
self-test`를 사용한다. 이 수정으로 시작한 run `34324974244`는 source
`860b03f2ed151754551f781e6c12947425256bce`에서 Linux quality, Windows tests,
native artifact, ACL probe가 모두 success로 완료됐고, `qsync-three-host-bootstrap`
job은 `execute_ro=false`로 skipped였다. 따라서 이 run은 전체 CI/네이티브 artifact
검증의 success이지 외부 3-host F 실행이나 full F 판정이 아니다.
해당 run의 Windows native artifact manifest도 재다운로드해 SHA-256
`36011de777b1523d82e886964f19c39220c3b44ee0558d4e95d66aebc4e31227`, 크기
`34,064,896` bytes, target `x86_64-pc-windows-msvc`, source
`860b03f2ed151754551f781e6c12947425256bce`로 바이너리와 manifest가 일치함을
확인했다. 이는 retry12에서 사용한 이전 source
`2f44d9fbfbe1c4779bd59f78fe9cc4ff27f41cd6` artifact와 별도다.

같은 source의 test-owned Windows local retry17은
`2026-09-09T08:25:28Z`~`08:26:04Z`에 위 Windows artifact로 실행했다. 실행기
`runtime-tmp-windows-local-graceful-retry17.py`의 evidence 기록 SHA는
`08e4bd774fc3ad963e34aa1cc1c84f077ced604c9d065defa8dcb36e5c553272`이다. artifact
SHA/크기, owner create와 key/preview/validate/join, 실제 fixture 파일 hash/크기,
owner-online 상태에서의 member 재조회, 세 번의 소유 console Ctrl+C, child exit 0,
stream drain 및 console release가 모두 고정 phase로 확인됐고 `reopen_checks=63`,
remote status 0이었다. 파일과 재조회 파일은
`8b666f88f7b033f647f9b5ae66d668b7bb88376630dbecfb0fba757f4f84334c`/
`262144` bytes였다. 이 결과는 local Windows owner/member의 관리형 정상 종료와
동일 membership 재오픈을 증명하는 `pass`이며, evidence의
`managed_bilateral_drain_ack=unverified`를 유지한다. 따라서 3-host 동시 실행,
offline-owner drain, E `share-swarm/1` provider payload, full F는 증명하지 않는다.
이전 source의 retry13 owner-offline 결과는 별도 `pending`으로 보존한다.
