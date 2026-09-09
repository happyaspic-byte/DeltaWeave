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
phase와 hash·크기만 남긴다.

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
CLI argument는 로그·evidence·artifact에 쓰지 않는다. GitHub reusable workflow가
참조하는 새 run 전용 secret은 `QSYNC_F_RO_SHARE_KEY`이며 값을 만들거나 기존 secret을
덮어쓰지 않는다. WinRM controller의 owner URL과 계정은 controller 환경에서만
`QSYNC_F_OWNER_API_URL`, `QSYNC_F_WINRM_USERNAME`, `QSYNC_F_WINRM_PASSWORD`로
주입한다. hosted RO job에는 owner 관리자 credential을 전달하지 않는다.

WinRM은 `Session.run_ps`를 사용하지 않는다. pywinrm의 해당 편의 메서드는
`powershell -encodedcommand ...`를 Windows command shell로 보내므로, wrapper의
UTF-16LE/Base64 payload를 메모리에서 만들고 `Protocol.run_command`에
`skip_cmd_shell=True`를 지정해 `powershell.exe`를 직접 실행한다. `open_shell`,
`get_command_output`, `cleanup_command`, `close_shell`을 모두 같은 호출에서 정리하며,
정리 실패나 encoded argument·직접 process command의 한계를 넘는 입력은 고정 오류로
실패시킨다. encoded payload는 512 KiB, 전체 직접 command는 Windows의 32,767-byte
한계로 제한한다. 길이 검사는 payload를 기록하지 않고 숫자만 산출한다. 비밀 없는 대표
config에서 wrapper는 6,374 bytes,
encoded payload는 17,000 bytes, 기존 run_ps command는 17,027 bytes,
직접 WinRS command는 17,090 bytes였고 command-shell 8,191-byte 경계를 넘었으므로
이 경로가 필수임을 확인했다.

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
기록한다. 현재 이 subset에는 managed pause/revoke drain ACK adapter가 없으므로
프로세스가 정상 종료해도 `graceful_drain_proven=unverified`로 남기고 소유 state를
보존한다. pre-existing 보호 상태는 실제 before/after 비교가 없으면
`unverified`로 남는다.

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
python3 -m py_compile scripts/qsync_three_host_bootstrap.py scripts/qsync_three_host_role.py scripts/qsync_make_role_manifest.py
python3 -m unittest discover -s tests/tools -p 'test_qsync_three_host_bootstrap.py' -v
python3 -m unittest discover -s tests/tools -p 'test_qsync_three_host_f_manifest.py' -v
```

GitHub reusable workflow dispatch, 새 encrypted secret 생성, 외부 3-host 실행, 실제
N0/relay, `share-swarm/1` 다중 provider payload, Edge/CDP와 full F acceptance matrix는
다음 source checkpoint에서 실행한다. 이 문서와 local checks는 실행 경로와 fail-closed
계약을 검증할 뿐 해당 미실행 항목의 성공을 뜻하지 않는다.
