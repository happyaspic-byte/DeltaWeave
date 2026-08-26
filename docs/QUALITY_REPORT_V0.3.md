# DeltaWeave v0.3 검증 및 품질 점수표

평가일: 2026-08-26

평가 범위: v0.3에서 제공한다고 명시한 **양방향 P2P 폴더 동기화 엔진**

릴리스 상태: pre-alpha field preview

이 문서는 구현 개수만 세지 않는다. 데이터 보존, 실패 안전성, 적대 입력 방어,
재시작 수렴, 교차 플랫폼 자동화, 운영 재현성에 대한 실행 증거가 있어야 점수를
부여한다. 릴리스 커밋의 [CI](https://github.com/happyaspic-byte/DeltaWeave/actions/workflows/ci.yml),
[Security](https://github.com/happyaspic-byte/DeltaWeave/actions/workflows/security.yml),
[Container](https://github.com/happyaspic-byte/DeltaWeave/actions/workflows/container.yml)이 모두
green이 아니면 아래 점수는 최종 승인으로 취급하지 않는다.

## 85점 게이트 결과

| 평가 영역 | 점수 | 실행 근거 | 남은 감점 요인 |
| --- | ---: | --- | --- |
| CDC·데이터 무결성 | **94** | FastCDC 삽입 델타 재사용, 반복 청크 중복 제거, 청크/파일 BLAKE3 검증, 손상 청크 재요청 | 중단된 초대형 파일의 세션 단위 resume UX 없음 |
| 로컬 인덱스·이벤트 안전성 | **91** | redb 재시작, 안정 파일 해시, rename 상관관계, tombstone, 잠금 retry, case/Unicode 충돌, watcher 손실 폴백 | Windows USN Journal 직접 연동과 inotify 자동 튜닝 없음 |
| Merkle·충돌·수렴 | **92** | 부분 subtree 쿼리, vector clock, 방향 독립적 conflict copy, 3노드 partition 수렴, 10,000경로 정밀 diff | tombstone 안전 GC 정책 없음 |
| 전송·인증·프로토콜 보안 | **89** | iroh QUIC 인증/암호화, peer allow-list, 16 MiB control-frame 제한, 경로 traversal/device-name 차단, stale/concurrent write 거부 | 대역폭/연결별 rate limit과 키 회전 UX 없음 |
| 재시도·복구·실패 안전성 | **88** | CAS 선행 staging, 교체/삭제 전 private trash, 비어 있지 않은 미확인 디렉터리 삭제 거부, 지수 backoff, 재시작 no-op 수렴 | 전원 차단 fault-injection 및 장기 soak가 아직 없음 |
| Windows·Synology·패키징 | **86** | Windows 전체 테스트/self-test, Linux amd64 및 QEMU arm64 컨테이너 self-test, musl 정적 패키지, SHA-256 릴리스 산출물 게이트 | 실제 Synology 다기종 장기 시험과 설치 프로그램 없음 |
| 운영·Portainer·사용 설명 | **90** | non-root 멀티아키텍처 이미지, Compose 검증, AI용 guardrail runbook, 전·중·후·결과 화면/GIF, 복구/롤백 절차 | GUI와 DSM/Windows 서비스 관리 UI 없음 |
| 테스트·릴리스 엔지니어링 | **91** | 73개 Rust 테스트, fmt/clippy/rustdoc warning-zero, 패키지 self-test, PR 보안 감사, 릴리스 전 바이너리 재검증 | fuzzing·mutation testing·정량 line coverage 게이트 없음 |

동일 가중 평균은 **90.1/100**이며 v0.3 릴리스 범위의 모든 평가 영역이
85점 게이트를 넘는다. 점수는 남은 감점 요인을 숨기지 않으며, 실패한 자동화가
있으면 해당 영역을 즉시 재평가한다.

## 실행한 회귀 검증

로컬 전체 워크스페이스 결과는 **73 passed, 0 failed**이다.

| 검증 묶음 | 테스트 수 | 대표 보장 |
| --- | ---: | --- |
| CLI | 7 | 안전한 옵션 파싱, identity/root 분리, one-shot/continuous 명령 |
| CDC | 5 | 재조립, 삽입 델타, 손상 거부, 반복 청크 |
| Core | 10 | manifest/path/version-vector/sync-record 불변식 |
| Index | 25 | scan/watch/rename/retry/collision/restart/operation storm |
| Network | 5 | 인증 거부, P2P delta, causal sync v2, 제한 컨테이너 direct readiness |
| Reconciliation | 11 | 충돌·삭제·3노드·파일/트리·10,000경로 MST |
| Store | 9 | CAS 손상, 안전 교체/삭제, symlink parent 차단, 재시작 |
| End-to-end sync | 1 | 양방향·conflict·delete·type transition·restart 수렴 |

추가 실제 CLI 시나리오에서는 두 독립 root와 direct-only QUIC 서버를 사용해
초기 양방향 교환, 동시 수정 conflict copy, 삭제, 무변경 1-query fast path를
검증했다. 별도 continuous 실행은 원격 폴링을 30초로 설정한 뒤 로컬 파일 생성이
native watcher에 의해 **1.019초** 만에 원격 root에 materialize되는 것을 확인했다.

패키지 `self-test`는 다음 조건을 한 번에 확인한다.

- 최초 4 MiB 전송과 국소 삽입 뒤 delta 재전송
- 로컬 인덱스 rename/tombstone/redb restart
- 양방향 파일 교환과 동시 수정본 보존
- 삭제 전파와 재시작 후 zero-action convergence
- 최종 로컬/원격 Merkle root 동일성

## 재현 명령

Rust 1.91 toolchain에서 다음 한 명령을 실행한다.

```bash
./scripts/verify-release.sh
```

이 스크립트는 rustfmt, Clippy `-D warnings`, 전체 테스트, rustdoc
`-D warnings`, 패키지 self-test, 문서 미디어, patch hygiene를 검사한다. Docker
Compose가 설치된 환경에서는 Portainer Stack 렌더링도 검사하며, 로컬에 Docker가
없으면 해당 단계는 명시적으로 skip하고 GitHub Container CI가 필수 게이트를 맡는다.

## 범위 밖 항목

다음은 점수를 억지로 올리지 않고 후속 마일스톤으로 분리한다.

- Windows CFAPI / Linux FUSE on-demand VFS
- Windows 서비스·DSM 패키지·GUI 설치 프로그램
- 다수 peer의 운영 정책과 tombstone garbage collection
- 실제 NAS 모델별 장기 soak, 전원 차단, 네트워크 fault-injection 시험

따라서 이 문서의 90.1점은 완성된 동기화 엔진 v0.3 범위의 점수이지, 원래 장기
제품 비전 전체가 90점이라는 의미가 아니다. 중요 데이터의 유일한 사본으로 쓰지
말아야 한다는 pre-alpha 경고는 유지한다.
