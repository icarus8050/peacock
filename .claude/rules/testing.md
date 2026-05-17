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

## 검증식이 invariant를 직접 잡는지 — false positive 회피

harness가 N개 자식 컴포넌트를 다룬다면, 검증식은 **"분배 자체가 일어났다"**를 직접 확인한다. "검증 후 값"만 보면 분배가 일부에서만 일어나도 우연히 통과하는 경로가 생긴다.

- 안티패턴: tickAll(3) 호출 후 `n.elapsed != 0` 같은 식 — 0번 노드만 3번 tick하고 나머지를 빼먹어도 나머지의 elapsed 초기값(0)이 통과 조건이 된다.
- 권장: 분배가 모든 N에서 일어났을 때만 성립하는 **차이값**을 검증. `tickAll(5)` 후 모든 노드 `elapsed == 5` — 한 노드라도 빠지면 그 노드의 elapsed는 5 미만이라 잡힌다.
- 일반화: 검증 후 값이 **호출 전 값 또는 시스템 zero/초기값과 같은 가능성**이 있다면 false positive 가능. 호출 전 값에서 명백히 갈라지는 값으로 검증식을 박는다.

## 검증식이 정상 분기 하나만 박지 않는지 — false negative 회피

위 섹션이 "분배를 빠뜨려도 통과"(false positive)를 막는다면, 이 섹션은 그 반대 — **정상 동작이 여러 형태로 나타날 수 있는데 그 중 하나만 invariant로 박아 다른 정상 분기를 fail로 만드는** false negative.

- 안티패턴: election cluster 테스트에서 "모든 노드 term=리더 term"을 검증. 그러나 startElection은 첫 grant로 quorum이 채워지면 루프가 break해 두 번째 peer엔 RequestVote 자체가 도달하지 않고 그 peer term은 초기값 0으로 남는다. 정상 동작인데 fail.
- 권장: invariant의 **최소 충분 조건**만 검증한다. "정확히 1 leader 등장 + leader 자신의 term=1"만 보고, follower 동기화는 다른 단계(heartbeat) 책임으로 분리.
- 일반화: 정상 동작이 여러 형태(early return, quorum break, 비동기 partial 적용)로 갈릴 수 있으면 **모든 형태에 공통된 부분**만 검증식에 박는다. 한 형태에만 성립하는 조건은 그 시나리오 전용 테스트로 분리하거나 setup으로 시나리오를 그 형태에 고정한다.

## 테스트 중복 점검

새 테스트가 기존 단일 컴포넌트 테스트와 본질적으로 같은 invariant를 검증하지 않는지 본다. harness 위에서 같은 invariant를 다시 검증하는 형태라면 — 그 테스트의 의도는 보통 **harness wiring**(예: idx→리소스 매핑이 안정적인지)이지 컴포넌트 동작 자체가 아니다. 의도를 테스트 이름·주석에 박아 차이를 드러낸다 (`TestX_RestartUsesSameDir`처럼). 이름이 단일 컴포넌트 테스트와 구분되지 않으면 둘 중 하나는 잉여.

## 금지 사항

- 프로덕션 코드 수정을 테스트 통과 목적만으로 수행하지 않는다.
- `sleep`을 조건 확인에 남용하지 않는다. 가능한 한 채널/동기화 기반으로 검증한다.
