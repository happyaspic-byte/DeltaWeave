# 6개 Goal 통합 기록

기준: main 75ffab74ee3de4dd1d071c13ab04ae26fb4e6e5b
통합 브랜치: integration/six-goals-20260905
원본 worktree의 브랜치, HEAD, 인덱스와 미커밋 파일은 변경하지 않는다.

## 원본 현황

| 작업 | 시작 상태 | 보존 커밋 |
| --- | --- | --- |
| bug-fix-update | 미커밋 변경, 기준 대비 추가 커밋 0 | ed1ac1ec83555c5d99b42748292539075e696584 |
| security-update | 미커밋 변경, 기준 대비 추가 커밋 0 | 1898d5b2f9b6b747d97748d09cbd8c06f6d17b4c |
| performance-update | 미커밋 변경, 기준 대비 추가 커밋 0 | 9269dd4c3fc7dc3eac10540dcd711e178105fae7 |
| test-hardening | 미커밋 변경, 기준 대비 추가 커밋 0 | 3e40c607c6175cbadfa0a4c80edfbde78e300363 |
| ui-update | 미커밋 변경, 기준 대비 추가 커밋 0 | aaba96515bc28d475f8835d9fb53296eca6874aa |
| docs-sync-update | 미커밋 변경, 기준 대비 추가 커밋 0 | 0c077d1a73478d043cc744cd02da569cee4690a3 |

## 통합 순서와 의존성

1. bug-fix-update: 비동기 로컬 스냅샷 재검사와 6개 회귀.
2. security-update: 중복 스냅샷 검사는 비동기 버전으로 통일하고 경로·권한·wire 방어 병합.
3. performance-update: Merkle prefix 조회 최적화와 측정 자료.
4. test-hardening: 보안 변경 위에서 저장소·causal 네트워크 회귀 확인.
5. ui-update: CLI 출력과 웹 API를 강화된 엔진 위에 연결.
6. docs-sync-update: 최종 구현에 맞춰 문서 충돌과 오래된 설명 정리.

## 경계

현재 main 작업 폴더에는 별도의 미커밋 웹 UI·엔진 변경이 있다. 이번 6개 작업과 다른 결과이므로 통합 입력으로 자동 포함하지 않았다. 기존 luvus 및 claude worktree도 보존한다.
각 작업 보고서의 과거 테스트 결과는 원본 작업의 증거이며 통합 결과의 통과 증거로 대체하지 않는다.
