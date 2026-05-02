# 테스트 규칙

## 파일 구조

- 테스트는 대상 파일과 같은 패키지에 `_test.go` 접미사로 둔다 (예: `wal.go` ↔ `wal_test.go`).
- 테스트 내부 헬퍼는 테스트 파일 내에 두며, 다른 프로덕션 코드에서 참조하지 않는다.

## 명명

- `Test<Feature>` 또는 `Test<Feature><Scenario>` 형식.
- 시나리오가 복합적이면 `TestAppend_AfterClose`처럼 언더스코어로 구분한다.

## 파일 시스템 / 외부 리소스

- 임시 디렉터리는 `t.TempDir()`을 사용한다. 수동 `os.MkdirTemp` + `defer os.RemoveAll` 금지.
- 테스트 종료 시 정리가 필요한 리소스는 `t.Cleanup()`에 등록한다.

## 에러 처리

- 테스트 셋업 단계의 에러는 `t.Fatalf`로 즉시 종료한다.
- 검증 실패는 `t.Fatalf` 또는 `t.Errorf`. 이후 단언이 의미 없어지면 `Fatalf`, 계속 가능하면 `Errorf`.
- 기대값과 실제값을 메시지에 포함: `t.Fatalf("expected %v, got %v", want, got)`.
- **에러를 반환하는 호출은 모두 받는다.** 셋업이든 정리(close)든 무시 금지. 다음 두 패턴이 흔한 누락 지점:
  - 셋업에서 데이터를 만드는 호출(`Append`/`Sync`/`Put`/`Delete`/`Write`/`CommitCompaction`): silent 실패 시 후속 검증이 비어 있는 결과를 보고 잘못된 진단으로 빠진다 — 반드시 `if err != nil { t.Fatalf(...) }`.
  - 끝 시점 `Close`: Close 실패는 fsync/durability 실패를 의미할 수 있어 데이터 손실을 가린다 — 반드시 받는다.
- 고루틴 내부에서는 `t.Fatalf`를 쓸 수 없다 — `t.Errorf`로 기록하고 흐름을 멈추지 않는다(또는 별도 채널로 에러를 모아 메인이 `Fatalf`).

## 동시성

- 동시성 검증이 필요한 테스트는 `-race` 플래그로 통과하도록 작성한다.
- 실제 시간 의존이 있는 코드는 인터벌을 짧게 잡되 (예: 50ms), 결과 검증에 충분한 대기시간을 둔다 (예: 100ms).

## 금지 사항

- 프로덕션 코드 수정을 테스트 통과 목적만으로 수행하지 않는다.
- `sleep`을 조건 확인에 남용하지 않는다. 가능한 한 채널/동기화 기반으로 검증한다.
