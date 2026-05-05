# Go 코드 스타일 규칙

## 포맷

- `gofmt` 기준을 따른다. 구조체 필드는 같은 블록 내에서 타입 위치를 정렬한다.
- 줄 끝 공백, 불필요한 빈 줄 금지.

## 에러 처리

- 에러는 패키지 프리픽스를 붙여 래핑한다: `fmt.Errorf("wal: open: %w", err)`.
- 래핑된 에러 비교는 `errors.Is` / `errors.As`를 사용한다. `err == io.EOF` 같은 직접 비교는 지양한다.
- 경계(유저 입력, 외부 API) 바깥의 내부 코드에서는 발생 불가능한 경우에 대한 방어적 검증/분기를 추가하지 않는다.

### Close 호출의 에러 처리

위치에 따라 다르다 — 일률적으로 검사하거나 일률적으로 무시하지 않는다.

- **성공 경로의 Close** (영속성 확정 단계, 함수의 마지막 단계 등): **반드시 받는다**. `if err := f.Close(); err != nil { return ... }` 또는 `return f.Close()`. fsync 직후의 close, write 파일을 닫아 저장을 확정하는 경우가 해당.
- **에러-cleanup 경로의 Close** (이미 메인 에러를 잡은 뒤의 정리): **무시 OK** — 메인 에러를 마스킹하지 않는 Go 표준 관용구. 예:
  ```go
  if err := f.Sync(); err != nil {
      f.Close()  // 무시
      return fmt.Errorf("wal: fsync: %w", err)
  }
  ```
- **읽기 전용 파일의 `defer Close()`**: **무시 OK**. 버퍼된 쓰기가 없어 Close 실패는 의미 있는 신호를 거의 주지 않고, 호출자가 할 수 있는 일도 없다.
- 두 에러를 모두 호출자에 노출해야 할 동기가 명확하면 `errors.Join`을 쓴다 (Go 1.20+). 그 외엔 cleanup close 무시가 기본.

## 인터페이스

- 작은 단일 메서드 인터페이스를 선호한다 (예: `io.Reader`, `fmt.Stringer`).
- 인터페이스는 "소비자" 측에서 정의한다. 제공자 패키지에 두지 않는다.
  - 예: `syncable`은 `kv/syncer.go`(소비자)에 두며, `wal.go`(제공자)는 `syncable`의 존재를 몰라도 된다.
- 구현 타입에 `implements` 성격의 주석이나 명시는 불필요하다. 필요 시 컴파일 타임 검증 관용구만 사용: `var _ io.Reader = (*File)(nil)`.

## 리시버

- 가변 상태를 갖거나 `sync.Mutex`를 임베드한 타입은 포인터 리시버를 사용한다.
- 한 타입의 모든 메서드 리시버 종류는 통일한다 (값/포인터 혼용 금지).

## 주석

- Exported 식별자에 한해 doc 주석을 작성한다. 이름만으로 자명하면 생략한다.
- 코드를 그대로 풀어쓰는 주석(what)은 금지한다. **왜**가 비자명할 때만 주석을 쓴다.
- 현재 작업, 티켓 번호, 호출자 정보 등은 주석에 남기지 않는다 (PR 설명이나 커밋 메시지에 기록).
- 주석은 한국어로 작성한다. 바이너리 레이아웃 다이어그램의 필드명만 코드 식별자와 일치를 위해 영어 유지.

## 디자인

- 사용되지 않는 필드, 옵션, 콜백 훅은 남기지 말고 제거한다.
- 가상의 미래 요구사항을 위한 추상화 금지. 세 번 반복될 때까지는 중복을 허용한다.
- 책임이 혼재된 타입은 분리한다. 예: 로그 기록(`wal.WAL`)과 주기적 동기화(`kv.syncer`)는 분리.
- 기본값은 `applyDefaults()` 같은 정규화 함수 한 곳에서 관리하거나, `DefaultOptions` 팩토리로 제공한다. 같은 기본값을 여러 곳에 중복해 박지 않는다.
## 추상화 수준 일치

**하나의 함수는 하나의 추상화 수준이어야 한다. 한 함수 안에 높은 수준의 개념과 낮은 수준의 세부 구현이 공존해서는 안 된다.**

**함수는 위에서 아래로 이야기처럼 읽혀야 하며, 각 함수는 자신보다 한 단계 낮은 추상화 수준의 함수를 호출해야 한다.**

호출부는 "흐름 한 줄에 한 단계"로 읽힌다 — 단계 이름의 나열. 그 단계의 구현 디테일(루프, 바이트 조립, 분류 분기)은 한 단계 아래의 헬퍼 안에 가둔다.

### 추상화 수준의 구분

일반적으로 **고수준 / 중간 / 저수준** 셋으로 본다.

- **고수준 (High Level)** — 핵심 비즈니스 로직, **무엇(What)**을 하는지 기술. 예: `createOrder()`, `(*WAL).CommitCompaction()`.
- **중간 (Medium Level)** — 고수준 흐름을 구성하는 단계 이름. 예: `validateCompactionArgs(...)`, `writeManifest(...)`, `cleanupCompactedFiles(...)`.
- **저수준 (Low Level)** — 구체적인 구현 세부, **어떻게(How)**를 기술. 예: `db.execute("INSERT...")`, `binary.LittleEndian.PutUint64(...)`, `os.Remove(segmentPath(dir, s))`.

한 함수가 자신보다 두 단계 이상 낮은 호출을 직접 들고 있으면 추상화 수준이 섞인 것이다. 예: 고수준 함수 `CommitCompaction`이 직접 `os.Remove`(저수준)를 호출하면 안 되고, 중간 단계 `cleanupCompactedFiles`를 거쳐야 한다.

### 신호 — 분리 검토 대상

- 함수가 **여러 단계**(검증 → 변환 → 영속화 → 정리 등)를 들고 있는데 각 단계가 다른 결정/루프/맵 빌드를 직접 수행한다.
- 한 함수 안에서 **루프와 흐름이 번갈아** 등장한다 (e.g., `for ... { ... }` 다음 `if ... { ... }` 다음 `for ... { ... }`).
- 30~40행을 넘어가며 단계 사이 빈 줄로 구분된 블록이 여럿 있다.
- 인라인 주석으로 단계 경계를 표시해야 읽힌다 (`// 1단계 ...`, `// 다음 ...`).
- **들여쓰기가 3단계 이상**이고 각 단계가 서로 다른 의미 결정을 내린다 (e.g., `for` → `if err != nil` → `if errors.Is(...)` → 분기). 들여쓰기 깊이 자체가 추상화 수준이 섞여 있다는 신호.

### 분리 방법

각 단계를 이름 있는 헬퍼로 추출해 호출부가 **단계 이름의 나열**처럼 읽히게 한다. 헬퍼는 가능하면 순수 함수(상태 변경 없음)로 쪼개 단위 테스트 친화적으로.

```go
// Before: 60행, 5단계가 한 함수에 섞여 있음
func (w *WAL) CommitCompaction(...) error {
    w.mu.Lock(); defer w.mu.Unlock()
    if w.closed { ... }
    if checkpointSeq <= 0 { ... }
    if len(removedSeqs) == 0 { ... }
    sealed := make(map[int64]bool, ...)        // 봉인 검증 시작
    for _, s := range w.manifest.segments[:len(...)-1] { sealed[s] = true }
    for _, s := range removedSeqs { if !sealed[s] { ... } }
    newSegments := make([]int64, 0, ...)        // 새 매니페스트 빌드
    for _, s := range w.manifest.segments { if !removed[s] { ... } }
    nextManifest := &manifest{...}
    if err := writeManifest(...); err != nil { ... }
    w.manifest = nextManifest
    for _, s := range removedSeqs { os.Remove(...) }   // 정리
    if prevCheckpointSeq > 0 && ... { os.Remove(...) }
    return nil
}

// After: 흐름 7행, 각 단계는 이름 있는 헬퍼로
func (w *WAL) CommitCompaction(...) error {
    w.mu.Lock(); defer w.mu.Unlock()
    if w.closed { return ErrClosed }
    if err := validateCompactionArgs(w.manifest, checkpointSeq, removedSeqs); err != nil {
        return err
    }
    prev := w.manifest.checkpointSeq
    next := postCompactionManifest(w.manifest, checkpointSeq, removedSeqs)
    if err := writeManifest(w.dir, next); err != nil { w.closed = true; return ... }
    w.manifest = next
    cleanupCompactedFiles(w.dir, removedSeqs, prev, checkpointSeq)
    return nil
}
```

### 같이 활용할 패턴

- **저수준 세부의 헬퍼화**: 옵션 변환, 바이트 조립, 플래그 분기 등을 `walOptionsFrom`처럼 추출. `kv.Open`이 옵션 세부를 `walOptionsFrom`으로 감춰 "정규화 → 재생 → 오픈 → 시작" 흐름만 남기는 사례.
- **상태 접근 메서드화**: `w.manifest.segments[len(...)-1]` 같은 패턴이 두 곳 이상 등장하면 `m.activeSeq()` 같은 메서드로. 의도가 이름에 박혀 호출부가 짧아진다.
- **헬퍼 위치**: 호출부 바로 아래 또는 같은 파일 말미. 주제별 파일이 따로 있으면(`compact.go` 등) 토픽 친화적 위치 우선.

### 들여쓰기 평탄화

위 "신호"에서 들여쓰기 3단계 이상이 보이면 다음 세 패턴을 순서대로 적용한다.

1. **루프 본체 헬퍼화** — 루프 안에서 element별로 검증/분류/에러 분기를 하면 본체를 `requireX(args) error` 같은 헬퍼로 추출한다. 호출부 루프는 "각 원소에 X를 적용 → 첫 실패면 중단"이라는 한 줄 의도만 남는다.

2. **early-return 평탄화** — `if err != nil { if errors.Is(A) {...}; if errors.Is(B) {...}; ... }` 형태의 inverted 분기는 `if err == nil { return nil }`로 success-fast하고 이후 분류 분기를 직선 나열로 쓴다. 들여쓰기 한 단계가 사라진다.

3. **도메인 특수 케이스는 호출부로** — 검증/변환 함수 안에 `if item.special { continue }` 같은 예외 처리가 들어가면 함수가 두 가지 일(일반 처리 + 예외 처리)을 한다. 호출부에서 미리 partition(`sealed := items[:len(items)-1]` 같은 슬라이싱 또는 별도 메서드)해 함수는 "전체에 일관 적용"이라는 단일 의미만 갖게 한다.

```go
// Before: 3단계 들여쓰기 + 특수 케이스 + 두 단계 errors.Is
func verifyManifestArtifacts(dir string, m *manifest) error {
    if len(m.segments) == 0 { return ... }
    for _, s := range m.segments {
        if _, err := os.Stat(segmentPath(dir, s.seq)); err != nil {
            if errors.Is(err, os.ErrNotExist) {
                if s.seq == m.active().seq { continue } // 도메인 룰
                return fmt.Errorf("%w: ...", ErrMissingSegment)
            }
            return fmt.Errorf("...: %w", err)
        }
    }
    return nil
}

// After: 호출부에 도메인 룰, 루프는 한 줄, 분류는 직선.
func verifyManifestArtifacts(dir string, m *manifest) error {
    if len(m.segments) == 0 { return ... }
    sealed := m.segments[:len(m.segments)-1] // 활성은 Open이 만든다
    return verifySegmentsExist(dir, sealed)
}
func verifySegmentsExist(dir string, segments []segmentMeta) error {
    for _, s := range segments {
        if err := requireSegmentExists(dir, s.seq); err != nil {
            return err
        }
    }
    return nil
}
func requireSegmentExists(dir string, seq int64) error {
    _, err := os.Stat(segmentPath(dir, seq))
    if err == nil { return nil }
    if errors.Is(err, os.ErrNotExist) {
        return fmt.Errorf("%w: seq=%d", ErrMissingSegment, seq)
    }
    return fmt.Errorf("...: %w", err)
}
```

### 트레이드오프

- 헬퍼 신설 비용 vs jump-to-definition 부담: 헬퍼 이름이 self-explaining이면 호출부에서 점프 안 해도 흐름 이해됨 → 분리 권장.
- "한 곳에서 다 보고 싶다"는 동기는 함수가 짧을 때만 유효. 30행 이상이면 분리 이득이 거의 항상 큼.

## 함수 시그니처 — 반환값 응집

여러 값을 반환할 때 flat tuple로 늘어놓지 말고 **의미가 담긴 도메인 타입**으로 응집한다. 호출부는 "함수가 무엇을 반환했는가"를 타입 이름 한 곳에서 읽어야 한다.

### 신호 — struct로 묶을 후보

- 두 개 이상의 반환값이 **개념적으로 한 묶음** (같은 작업의 결과, 같은 컨텍스트의 속성).
- 호출부가 항상 두 값을 **같이 사용**하거나 **같이 무시**하는 형태.
- 시그니처가 `(*X, bool, error)`, `(int, int64, T, error)` 같은 4-튜플 이상으로 길어짐.
- 같은 함수의 여러 분기에서 반환할 때 `Entry{}, 0, ...` 같은 zero 패딩이 반복됨.

### 분리 기준 — error는 항상 별도

`(domainResult, error)` 형태가 Go 관용구. error는 도메인 객체에 섞지 않는다 (Go의 errors.Is/As 흐름과 충돌).

### 예시

```go
// Before: 의미 없는 3-tuple — 두 값의 관계가 시그니처에 안 보임
func scanSegment(target scanTarget) (*segState, bool, error)

ss, truncated, err := scanSegment(target)

// After: scanResult가 "한 segment 스캔의 결과"라는 도메인 개념을 박음
type scanResult struct {
    state     *segState
    truncated bool
}

func scanSegment(target scanTarget) (scanResult, error)

result, err := scanSegment(target)
// result.state, result.truncated — 의도가 자명
```

### 이름 규약

- Go 컨벤션의 `xxxResult` 접미사를 선호 (예: `scanResult`, `scanOutcome`보단 `scanResult`).
- 같은 도메인의 여러 레벨이 있으면 prefix로 추상도를 구분:
  - 한 entry read → `readResult` (entry 단위)
  - 한 segment scan → `scanResult` (segment 단위)

### 트레이드오프

- 두 값이 정말로 무관하면 분리 유지 (예: `io.Reader.Read(buf) (n int, err error)`의 `n`/`err` 같은 표준 관용구는 그대로).
- 1번만 호출되는 사적 helper의 2-tuple 반환은 묶지 않아도 무방.
- 도메인 타입을 만들 때 **하나의 응집된 의미**가 있어야 한다. 임의 묶음은 오히려 가독성을 해친다 — "왜 이 두 값이 같이 있는가?"가 타입 doc 한 줄로 답해져야.

## Zero Value

- Go의 zero value가 유효한 초기 상태이면, 명시적 초기화를 생략한다 (예: `sync.Mutex`, `bytes.Buffer`).
- 구조체 리터럴에서 불필요한 명시적 필드 설정을 피한다.

## 함수 배치

`gofmt`는 순서를 강제하지 않지만, 파일은 위에서 아래로 "큰 그림 → 세부"로 읽히게 배치한다.

- **톱-다운**: 호출자를 피호출자보다 위에 둔다. 형제 헬퍼끼리의 순서는 호출 순서를 따른다.
- **파일 내 배치 순서**: (1) 타입·상수·변수 → (2) 생성자/팩토리(`Open`, `New`, `DefaultOptions`) → (3) 핵심 공개 메서드 → (4) 수명주기 메서드(`Close`, `Stop`) → (5) 비공개 헬퍼(파일 말미 또는 유일한 호출자 바로 아래).
- 같은 리시버의 메서드는 인접 배치한다 (타입별로 섞지 않는다).
- 테스트 파일의 함수 순서는 원본 파일의 함수 순서를 반영한다 (`wal.go: Open → Append → Close` ⇒ `wal_test.go: TestOpen… → TestAppend… → TestClose…`).
