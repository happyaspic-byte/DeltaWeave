# QSync Windows native artifact 실행계약

`.github/workflows/qsync-native-artifact.yml`은 `workflow_dispatch`에서 선택한 브랜치의 호출 commit(`github.sha`)만 `windows-latest`에서 checkout한다. 사용자 command 입력은 받지 않으며, job 시작 시 checkout SHA와 workflow SHA가 같은지 확인한다.

실행 순서는 고정되어 있다.

1. Node.js 22로 `npm --prefix web ci`와 `npm --prefix web run build`를 실행하고 `web/dist/index.html` 및 JavaScript asset을 확인한다.
2. Rust 1.91 MSVC target으로 `cargo build --locked --release --target x86_64-pc-windows-msvc -p deltaweave`를 실행한다. `DELTAWEAVE_REQUIRE_WEB_ASSETS=1`은 job 전체에서 유지되고 `Cargo.lock` 변경은 실패 처리한다.
3. 빌드된 `target/x86_64-pc-windows-msvc/release/deltaweave.exe self-test`를 실제 Windows runner에서 실행한다. exit code가 0이고 JSON `status`가 `pass`인 경우에만 artifact를 만든다. 실패한 실행은 passing artifact로 업로드되지 않는다.

성공 artifact에는 `deltaweave.exe`, `SHA256SUMS.txt`, `SOURCE_SHA.txt`, `native-verification-manifest.json`만 포함한다. manifest에는 source/workflow SHA, target, 고정 명령, 각 명령의 UTC 시작·종료 시각과 exit code, exe SHA-256이 기록된다. self-test 원문·키·endpoint·token은 artifact에 저장하지 않는다.

이 workflow는 설치기·서비스·방화벽·release/tag·GitHub release 업로드를 만들지 않으며, 기존 `ci.yml`을 수정하지 않는다. artifact의 SHA-256을 다시 확인한 뒤 최종 Windows/native 서버 검증에서 사용할 수 있다. 이 workflow 성공은 native 서버 간 실제 owner/RW/RO 동기화나 N0/relay 검증을 대신하지 않는다.
