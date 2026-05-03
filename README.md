# Peacock

Go + Fiber 기반의 고가용성 분산 시스템 서버 (현재는 단일 노드 KV, Raft 기반 분산화는 진행 예정).

## 요구 사항

- Go 1.26+

## 프로젝트 구조

```
peacock/
├── main.go             # 진입점 (config → kv → server → handler 연결)
├── config/
│   └── config.go       # 환경변수 기반 설정 관리
├── server/
│   └── server.go       # Fiber 앱 생성 + Graceful Shutdown
├── handler/
│   ├── handler.go      # 라우트 등록 중앙 관리
│   ├── health.go       # 헬스체크 엔드포인트
│   └── kv.go           # KV CRUD 엔드포인트
├── kv/                 # WAL 기반 in-memory KV 스토어 (백그라운드 syncer/compactor)
└── wal/                # Write-Ahead Log (segment + manifest + checkpoint compaction)
```

## 빠른 시작

```bash
go mod tidy            # 의존성 동기화
go run main.go         # 서버 실행
```

기본적으로 `:3000`에서 시작하며, `data/` 디렉터리에 KV 데이터(WAL + checkpoint)를 보관한다.

빌드 후 실행:

```bash
go build -o peacock
./peacock
```

종료는 `Ctrl+C` (graceful shutdown).

## 환경변수

서버 설정은 모두 환경변수로 주입한다. 값을 비우면 기본값 사용.

### 서버

| 변수명             | 설명                          | 단위 | 기본값  |
|--------------------|-------------------------------|------|---------|
| `PORT`             | 서버 포트                      | -    | `3000`  |
| `READ_TIMEOUT`     | HTTP 읽기 타임아웃             | 초   | `5`     |
| `WRITE_TIMEOUT`    | HTTP 쓰기 타임아웃             | 초   | `10`    |
| `SHUTDOWN_TIMEOUT` | Graceful Shutdown 대기 시간    | 초   | `10`    |

### KV 스토어 / WAL

| 변수명                       | 설명                                                                                       | 단위    | 기본값   |
|------------------------------|--------------------------------------------------------------------------------------------|---------|----------|
| `KV_DIR`                     | KV 데이터/WAL 디렉터리                                                                      | -       | `data`   |
| `KV_SYNC_INTERVAL_MS`        | 백그라운드 fsync 간격                                                                       | 밀리초  | `100`    |
| `WAL_MAX_SEGMENT_SIZE_MB`    | WAL segment 롤 임계 크기. `0`이면 WAL 패키지 기본값(64MiB) 사용                              | MB      | `0`      |
| `KV_COMPACTION_TRIGGER`      | 봉인 segment 개수가 이 값 이상이면 백그라운드 압축 동작. `0`이면 KV 기본값(`4`) 사용          | 개      | `0`      |
| `KV_COMPACTION_INTERVAL_MS`  | 압축 트리거 폴링 간격. `0`이면 KV 기본값(`1000`) 사용                                        | 밀리초  | `0`      |

> **주의**: `KV_SYNC_INTERVAL_MS=0`을 명시하면 KV 패키지 기본값(`100ms`)이 적용된다 (즉시 fsync가 아님). 즉시 fsync가 필요하면 코드 레벨로 `Sync` API를 사용해야 한다.

## 실행 예시

기본 실행:

```bash
go run main.go
```

포트와 데이터 디렉터리 변경:

```bash
PORT=8080 KV_DIR=./mydata go run main.go
```

자주 쓰는 운영 시나리오 — fsync를 더 자주, 압축은 더 빨리:

```bash
KV_SYNC_INTERVAL_MS=20 \
KV_COMPACTION_TRIGGER=2 \
KV_COMPACTION_INTERVAL_MS=500 \
go run main.go
```

WAL segment를 작게 잡고 (compaction 동작 관찰용) 짧은 shutdown 타임아웃:

```bash
WAL_MAX_SEGMENT_SIZE_MB=1 \
SHUTDOWN_TIMEOUT=3 \
go run main.go
```

## API 엔드포인트

### Health

| Method | Path      | 설명                                | 응답 예시              |
|--------|-----------|-------------------------------------|------------------------|
| GET    | `/health` | Liveness probe                       | `{"status":"ok"}`     |
| GET    | `/ready`  | Readiness probe                      | `{"status":"ready"}`  |

### KV

| Method | Path        | 설명                       | 본문 / 응답                          |
|--------|-------------|----------------------------|--------------------------------------|
| GET    | `/kv/:key`  | 값 조회                    | 200 + raw body / 404                  |
| PUT    | `/kv/:key`  | 값 저장 (raw body = value) | 204 No Content                        |
| DELETE | `/kv/:key`  | 값 삭제                    | 204 No Content                        |

### 동작 확인

```bash
curl http://localhost:3000/health

curl -X PUT http://localhost:3000/kv/foo --data 'hello'
curl http://localhost:3000/kv/foo            # → hello
curl -X DELETE http://localhost:3000/kv/foo
curl -i http://localhost:3000/kv/foo         # → HTTP/1.1 404 Not Found
```

## 테스트

```bash
go test ./...                            # 전체 테스트
go test ./kv/                            # 특정 패키지
go test -run TestOpen ./wal/             # 단일 테스트
go test -race ./...                      # 동시성 검증
```

## Graceful Shutdown

`SIGINT`(Ctrl+C) 또는 `SIGTERM` 수신 시 진행 중인 HTTP 요청을 처리한 뒤 KV 스토어를 닫는다(`store.Close`가 백그라운드 syncer/compactor 정지 + 마지막 fsync). `SHUTDOWN_TIMEOUT` 내에 완료되지 않으면 강제 종료된다.

## 향후 확장 — Raft 기반 분산화

설계 문서: [`.claude/plans/raft.md`](.claude/plans/raft.md). 진행 시 `node.New(...) → node.Start()` 단일 흐름으로 `main.go`가 재구성되며, 다음 환경변수가 추가될 예정이다 (단일 노드도 "1노드 cluster"로 통합되므로 기본값으로 현재 동작이 유지됨).

| 변수명 (예정)                 | 설명                                           | 기본값 (예정)    |
|-------------------------------|------------------------------------------------|------------------|
| `NODE_ID`                     | 노드 식별자                                    | `node-1`         |
| `RAFT_ADDR`                   | 노드 간 gRPC 주소                               | `:4000`          |
| `RAFT_DIR`                    | Raft log / snapshot / hardstate 디렉터리        | `raft-data`      |
| `BOOTSTRAP`                   | 신규 클러스터 부트스트랩 여부                    | `true`           |
| `JOIN_ADDR`                   | 기존 클러스터 합류 시 leader 주소                | -                |
| `RAFT_HEARTBEAT_MS`           | leader heartbeat 간격                           | `100`            |
| `RAFT_ELECTION_MIN_MS`        | election timeout 하한                           | `300`            |
| `RAFT_ELECTION_MAX_MS`        | election timeout 상한                           | `600`            |
| `RAFT_SNAPSHOT_THRESHOLD`     | snapshot 트리거 entry 수                        | `10000`          |
