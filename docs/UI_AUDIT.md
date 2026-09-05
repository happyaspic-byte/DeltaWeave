# DeltaWeave 웹 UI 구현·검증 기록

> 이 문서는 개별 worktree에서 수행한 당시의 검증 기록입니다. 6개 작업을 합친
> 현재 구현과 재검증 결과는 [통합 보고서](INTEGRATION_2026-09-05.md)를 기준으로 합니다.

검증일: 2026-09-05. 작업트리: `ui-update`, 기준 커밋 `75ffab7`.

**Design Read:** DeltaWeave 웹 UI는 PC·NAS의 로컬 폴더와 피어 연결을 관리하는 한국어 운영 화면으로, 밝은 바탕·navy 본문·teal 행동 강조를 사용해 실제 파일 상태와 안전한 동기화 순서를 읽기 쉽게 전달한다.

## 구현 범위와 기존 변경 보존

현재 브랜치에는 Rust CLI만 있었고 HTTP 제품 UI가 없었다. 별도 캐시 브랜치의 Tauri GUI는 다른 daemon과 네이티브 IPC에 의존했다. 사용자의 후속 지정인 “DeltaWeave 웹 ui”에 따라 현재 0.4.0 엔진에 연결되는 `deltaweave-web` 실행 파일을 추가했다. 기존 브랜치를 가져오거나 별도 랜딩 페이지를 만들지 않았다.

범위는 한 폴더의 실제 검사, 한 피어의 수신 허용, 직접 연결을 통한 1회 양방향 동기화, 검증 결과와 최근 작업 기록이다. 실행법과 두 기기 연결 순서는 [WEB_UI.md](WEB_UI.md)에 있다. 원래 CLI와 동기화 업무 규칙·저장 형식은 유지한다.

작업 시작 시 변경된 README, CLI `main.rs`, `output.rs`, `tests/output.rs`는 시작 시 SHA-256과 일치한다. Cargo manifest의 기존 `clap` 변경을 보존하면서 웹 크레이트 멤버를 추가했고, lockfile에는 웹 서버 의존성을 추가했다. 커밋·배포·외부 게시를 하지 않았다.

## 스킬과 디자인 판단

읽고 적용한 taste 스킬: `/home/ubuntu/.agents/skills/design-taste-frontend/SKILL.md`.
적용 규칙: §0 제품·사용자 해석, §1 맥락별 설정, §11 변경 전 감사와 브랜드 보존, §13 적용 범위 구분, §14 증거에 기반한 최종 점검. 제품 관리 화면이므로 랜딩 전용 히어로·사진·장식 모션 규칙은 제외했다.

| 설정 | 적용값 | 이유 |
| --- | ---: | --- |
| `DESIGN_VARIANCE` | 2 | 폴더 → 결과 → 연결 → 작업 기록의 예측 가능한 구조 |
| `MOTION_INTENSITY` | 1 | 파일 작업 상태를 정적으로 전달하고 장식 애니메이션을 쓰지 않음 |
| `VISUAL_DENSITY` | 5 | 전체 식별자·실제 표·진단을 유지하면서 입력과 결과를 구분 |

기존 CLI는 터미널 설정의 영향을 받으며 설정을 `1/1/7`로, 후보 Tauri GUI는 넓은 선형 폼을 근거로 `2/1/3`으로 추론했다. 웹 구현에는 두 흐름을 결합해 읽기 쉬운 중간 밀도를 적용했다. 기본 `8/6/4`를 기계적으로 적용하지 않았다.

공통 토큰은 `ui/styles.css`에 모았다. Segoe UI·한국어 시스템 글꼴, 본문 14px/행간 1.65, 제목 28px, 간격 4px 배수, 입력 모서리 8px·패널 12px를 사용한다. 배경 `#f3f5f7`, 본문 `#1b2430`, 보조 문구 `#576473`, 행동 `#09665f`, 오류 `#a12b28`이다. 그림자·장식 아이콘·배경 이미지는 사용하지 않는다. 지원 테마는 밝은 테마 하나다.

HTML/CSS/ES 모듈을 Rust 바이너리에 포함하므로 실행에 프런트엔드 패키지나 Node 서버가 필요하지 않다. 신규 운영 의존성 axum 0.8은 HTTP 라우팅·JSON·본문 제한을 맡으며 기존 Tokio 런타임을 사용한다. Node는 순수 UI 모델 테스트에만 필요하다.

## 개선된 사용자 흐름

- **접속과 복구:** 프로세스별 접속 링크의 토큰을 URL에서 지우고 탭에 보관한다. 최초 거부·연결 후 권한 거부를 구분하고 접속 키 입력란에 오류와 포커스를 제공한다. 연결 손실 시 마지막 결과임을 알리고 작업을 비활성화한다.
- **실제 상태:** 검사 전 값은 미확인으로 표시한다. 빈 폴더 검사로 확인된 0과 구분한다. 시작 접수 시 진행 상태를 먼저 표시하고 성공·실패·시간을 기록한다. 이전 성공 결과를 실패 때문에 지우지 않는다.
- **작업 위계:** 현재 폴더와 검사 행동을 먼저 보여주고, 실제 파일 표와 피어 입력을 나란히 둔다. 작은 화면에서는 같은 순서로 쌓인다. 전체 경로·ID·긴 한국어와 영문은 줄바꿈하거나 입력 내에서 선택할 수 있다.
- **피어 연결:** 상대 ID·직접 주소·양방향 변경 확인란을 검증한다. 확인란을 선택하면 실제 제출 버튼 상태가 갱신된다. 수신 중에는 같은 인덱스를 쓰는 검사·동기화를 차단한다. 수신 종료 오류는 명시적으로 다시 시도할 수 있다.
- **결과와 진단:** 실제 ScanReport/SyncReport를 사용한다. 양쪽 루트 해시가 검증된 경우에만 검증 완료로 표시하며 전송량·충돌 기록·원본 보고서를 제공한다. 거부된 검사의 실제 경로와 원인을 제한된 길이로 기록하고 생략 개수를 표시한다.
- **접근성:** 실제 label·aria-describedby·aria-invalid, 상태 알림, 한 개의 main landmark, 본문 바로가기, 3px 키보드 포커스, 44px 주요 조작 영역을 사용한다. 모달은 없다. 포커스와 입력값을 주기적 조회로 덮어쓰지 않는다.

## 핵심 변경 파일

| 파일 | 역할 |
| --- | --- |
| [web src/lib.rs](../crates/deltaweave-web/src/lib.rs), [main.rs](../crates/deltaweave-web/src/main.rs) | 실행 옵션·HTTP·인증·상태 수명 |
| [operations.rs](../crates/deltaweave-web/src/operations.rs) | 기존 엔진 연결, 작업 배타성·종료·진단 |
| [index.html](../crates/deltaweave-web/ui/index.html), [styles.css](../crates/deltaweave-web/ui/styles.css) | 의미 있는 화면 구조·공통 토큰·반응형·포커스 |
| [app.js](../crates/deltaweave-web/ui/app.js), [model.js](../crates/deltaweave-web/ui/model.js) | 인증·폼·실제 상태 렌더링·조회 순서 보호 |
| [api.rs](../crates/deltaweave-web/tests/api.rs), [browser.py](../crates/deltaweave-web/tests/browser.py), [model.test.js](../crates/deltaweave-web/ui/model.test.js) | 실제 API·피어·브라우저 및 순수 상태 회귀 검사 |

## 구현 중 발견해 해결한 회귀

1. 오래된 GET 응답이 새 POST 및 완료 결과를 덮어썼다. 실제 응답을 지연하는 브라우저 검사로 실패를 재현한 후 요청 세대 번호로 오래된 성공·오류를 무시하도록 수정했다.
2. 연결 후 401/403에서 인증 화면과 앱이 동시에 main landmark가 됐다. 인증 화면을 재접속 시 명명된 region으로 유지하고 접속 키에 포커스를 옮겼다. 실제 서버의 401 응답과 복구를 검증한다.
3. 거부된 스캔이 실제 충돌·재시도 경로를 숨겼다. 이전 성공 보고서를 유지하면서 작업 오류에 경로·원인·생략 수를 포함했다. 실제 충돌명, 잘못된 파일명, 읽기 실패 및 복구 테스트를 추가했다.
4. 수신 종료 후 iroh의 blocking 작업이 저장소를 잠시 소유할 수 있었다. 인덱스와 저장소 해제 확인 후에만 idle로 바꾸고 실패 시 중지 재시도를 제공한다.

백엔드 보안·동작 리뷰와 프런트엔드 동작·접근성 리뷰를 각각 독립 에이전트가 수행했으며, 발견 사항 수정 후 범위별 재검토에서 승인했다.

## 검증 명령과 증거

최종 로그·스크린샷 루트: `/tmp/deltaweave-web-evidence/`.
이 경로는 일회용 로컬 검증 자료이며 저장소에 포함하지 않는다. 모든 파일·신원·수신기는 임시 데이터로 생성했다.

| 검사 | 결과 | 로그 |
| --- | --- | --- |
| `cargo build --locked --workspace` | 통과 | `final-checks/build.log` |
| `cargo fmt --all -- --check` | 통과 | `final-checks/fmt.log` |
| `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` | 통과 | `final-checks/clippy.log` |
| `cargo test --locked --workspace --all-targets --all-features` | 133개 통과, 실패·무시 0개 | `final-checks/tests.log` |
| `node --test model.test.js` (`ui/`에서) | 8개 통과 | 터미널 결과 |
| `node --check crates/deltaweave-web/ui/app.js` | 통과 | 종료 코드 0 |
| 실제 브라우저 회귀 | 통과 | `browser-final.log`, `browser-final/browser-results.json` |

브라우저 검증은 `cargo build --locked -p deltaweave-web -p deltaweave` 후 다음 명령으로 실행했다.

```bash
python3 crates/deltaweave-web/tests/browser.py \
  --browser-cli /home/ubuntu/.npm/_npx/6de2aa2fded2970c/node_modules/.bin/agent-browser \
  --browser-executable /home/ubuntu/.cache/ms-playwright/chromium-1234/chrome-linux64/chrome \
  --no-sandbox \
  --evidence /tmp/deltaweave-web-evidence/browser-final
```

실제 2개 서버에서 빈 검사 → 한·영 파일 검사 → 필드 오류 → 허용되지 않은 피어 거부 → 수신 중지/재시작 → 양방향 동기화 → 재검사 → 서버 종료/재연결 실패를 조작했다. 양쪽에 파일 3개가 존재하고 각각 SHA-256이 일치했다. 실제 보낸 데이터 59 B, 받은 데이터 40 B였으며 양쪽 검증 루트가 목표 루트와 일치했다. 테스트 데이터가 제품의 초기 값으로 들어가지는 않는다.

오래된 상태 조회 회귀, 연결 후 실제 401, 입력 보존, 확인란·버튼 연결, Tab 포커스, 내비게이션·원본 보고서 열기, reduced-motion 설정을 확인했다. 의도한 인증 실패를 제외한 실행 구간에서 페이지 오류·콘솔 오류·실패 리소스가 없도록 회귀 스크립트가 검사한다. 서버 종료로 발생한 마지막 요청 실패는 복구 상태 검사의 예상 결과다.

| 화면 검증 | 결과·증거 |
| --- | --- |
| 360px·768px·1440px | 성공 결과·긴 한영 경로·작업 실패 기록에서 페이지 가로 넘침 없음 |
| 실제 200% 확대 | Chrome tabs.setZoom/getZoom=2; 1440 CSS px → 720, DPR 1 → 2. CSS zoom=1, visualViewport.scale=1. 초기·실제 파일 목록 상태 통과; `accessibility-final/report.md` |
| 밝은 테마 | 지원하는 유일한 테마로 검사 |
| 렌더링 접근성·대비 | axe 4.12.1: 인증 32개·검사 결과 47개·200% 47개 규칙 통과, 위반·미완료 0건. 실제 텍스트 57개 표본 최저 5.528:1, 13개 터치 영역·Tab 15회 확인. `accessibility-final/report.md` |

## 스크린샷과 한계

최종 캡처:

- `browser-final/web-initial-1440.png`: 연결 후 아직 검사하지 않은 상태.
- `browser-final/web-verified-360-light.png`, `web-verified-768-light.png`, `web-verified-1440-light.png`: 같은 실제 검증 결과의 반응형 화면.
- `browser-final/web-peer-error-768.png`: 피어 거부와 입력 보존.
- `browser-final/web-disconnected-768.png`: 서버 연결 손실과 마지막 결과 안내.
- `accessibility-final/populated-native-200.png`: 실제 파일 목록의 브라우저 200% 확대.
- `browser-zoom/native-200.png`: 검사 전 상태의 브라우저 200% 확대.

현재 브랜치에 이전 웹 화면이 없으므로 같은 제품 화면의 변경 전후 비교는 만들 수 없다. 초기 별도 Tauri 후보의 변경 전 캡처는 `/tmp/deltaweave-ui-audit-20260905/gui-before-{360,768,1440}-light.png`이며, 다른 네이티브 제품의 감사 자료다. 이를 이 웹 UI의 변경 전후 개선 증거로 계산하지 않는다. 초기 CLI·Tauri 조사 기록은 `initial-cli-and-tauri-audit.md`에 보관했다.

자동 접근성 검사는 확인한 상태의 결과이며 전체 WCAG 인증을 의미하지 않는다. 보조 기술의 음성 출력은 확인하지 않았다.

검증 환경은 Linux의 Chromium과 로컬 루프백 피어다. 한국어 글리프 검증을 위해 테스트 환경에 Noto CJK 글꼴을 설치했으며 제품 다운로드 의존성은 추가하지 않았다. 실제 Windows/macOS/NAS 브라우저, 별도 LAN 기기·방화벽·라우터 조건은 검증하지 않았다. 원격 웹 관리, 릴레이, 자동 반복 동기화, 다중 폴더 설정은 이번 범위에 없다. 성능 점수나 개선율은 측정·주장하지 않는다.
