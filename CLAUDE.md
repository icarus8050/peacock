# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run Commands

```bash
go mod tidy          # install/sync dependencies
go run main.go       # run server (default :3000)
go build -o peacock  # build binary
go test ./...        # run all tests
go test ./handler/   # run tests in a specific package
go test -run TestFunctionName ./package/  # run a single test
```

## Environment Variables

Server config is loaded from env vars in `config/config.go`:
- `PORT` (default: `3000`), `READ_TIMEOUT` (default: `5s`), `WRITE_TIMEOUT` (default: `10s`), `SHUTDOWN_TIMEOUT` (default: `10s`)
- `KV_DIR` (default: `data`) — KV store + WAL directory.
- `KV_SYNC_INTERVAL_MS` (default: `100`) — background fsync interval in milliseconds.
- `WAL_MAX_SEGMENT_SIZE_MB` (default: `64`) — WAL segment 파일이 이 크기를 넘기면 다음 세그먼트로 롤한다. `0`을 주면 kv/wal 기본값을 사용한다.
- Timeout values are in seconds unless noted otherwise.

## Architecture

Go 1.26 + Fiber v2 web server with graceful shutdown.

**Startup flow:** `main.go` → `config.Load()` → `kv.Open(...)` → `server.New(cfg)` → `handler.Register(app, store)` → `server.Start()` → `store.Close()`

- **`config/`** — Env-var-based configuration. All settings have defaults.
- **`server/`** — Creates Fiber app, listens on port, handles graceful shutdown on SIGINT/SIGTERM.
- **`handler/`** — Route registration. `handler.go` is the central registry (`Register` function); each feature file (e.g. `health.go`, `kv.go`) defines its own routes and handlers. New feature handlers should follow this pattern: create a file, define a `registerXxx(app, deps...)` function, and call it from `Register`.
- **`wal/`** — Write-Ahead Log for storage engine recovery. Binary-encoded entries with CRC32 checksums. Uses buffered writes with `WAL.Sync()` for explicit fsync; callers needing periodic sync run their own goroutine. Entry format: `TotalLen(4B) | CRC32(4B) | Op(1B) | Index(8B) | TimeStamp(8B) | DataLen(4B) | Data` (little-endian). 로그는 `wal-NNNNNNNNNN.log` 세그먼트 파일로 분할되며, `Append`가 `Options.MaxSegmentSize`를 넘기면 flush+fsync 후 다음 시퀀스로 롤한다. 엔트리는 세그먼트 경계를 가로지르지 않는다 — 한계보다 큰 엔트리는 빈 세그먼트를 단독 점유해 기록되고, 다음 Append부터 다시 롤이 적용된다. 활성 세그먼트 목록의 단일 진실원은 디렉터리의 `manifest` 파일(바이너리, CRC32 보호)이다 — `Open`/`OpenReader`는 매니페스트를 신뢰해 그 외 고아 세그먼트는 무시하고, 매니페스트 손상(`ErrManifestCorrupt`)이나 참조 세그먼트 누락(`ErrMissingSegment`)은 fallback 없이 중단한다. 매니페스트 부재(첫 기동 또는 pre-manifest 데이터)는 `wal.Open`이 `listSegments`로 자동 마이그레이션한다 — `OpenReader`는 디스크를 변경하지 않으므로 같은 부재를 `os.ErrNotExist`로 보고한다. 매니페스트 갱신은 tmp+rename+dir-fsync로 원자적이며, 롤 시 새 세그먼트 파일 생성 → 매니페스트 갱신 → 활성 세그먼트 전환 순. 롤 경로(Flush+Sync+Close+Open+Manifest) 중 어느 단계든 실패하면 WAL을 복구 불가 상태로 만들어 닫아버리며, 호출자는 재오픈해야 한다. Core API: `Open`, `(*WAL).Append`, `(*WAL).Sync`, `(*WAL).Close`, `OpenReader`, `(*Reader).ReadEntry`, `(*Reader).Close`, `ErrManifestCorrupt`, `ErrMissingSegment`.
- **`kv/`** — In-memory key-value store backed by WAL. Writes append to WAL (buffered), an internal periodic syncer (`kv/syncer.go`) fsyncs on `KV_SYNC_INTERVAL_MS`. On `Open`, the WAL 세그먼트들이 순서대로 재생되어 맵을 복원한다; tail corruption (`ErrIncompleteEntry` / `ErrChecksumMismatch`) is treated as the end of the log. Entry.Data payload: `KeyLen(4B) | Key | Value` (Delete carries empty Value). The default `SyncInterval` (`100ms`) is owned by `kv` — `config` passes `0` to opt into it. `Options.MaxSegmentSize`에 값이 있으면 WAL 기본값(64MiB)을 덮어쓴다. Core API: `Open`, `Options`, `DefaultOptions`, `(*Store).Get`, `(*Store).Put`, `(*Store).Delete`, `(*Store).Close`. HTTP surface: `GET /kv/:key`, `PUT /kv/:key` (raw body = value), `DELETE /kv/:key`.

## Rules

프로젝트 규칙은 별도 파일로 분리되어 있다. 각 파일을 참조하여 작업한다.

- @.claude/rules/commit.md — 커밋 메시지 규칙
- @.claude/rules/go-style.md — Go 코드 스타일 규칙
- @.claude/rules/testing.md — 테스트 작성 규칙

## Workflow (피드백 루프)

복잡한 변경은 아래 사이클로 수행한다. 사이클을 돌 때마다 스킬·규칙이 점진적으로 보강되도록 마지막 단계(회고)를 항상 고려한다.

1. **계획 + 개발** — 주 에이전트. 아키텍처 판단이 비자명하면 `Plan` sub-agent 호출.
2. **독립 리뷰** — 비-trivial 변경(새 패키지, 공개 API 변경, ~50라인 이상) 후 `code-reviewer` sub-agent를 `Agent` 툴로 호출한다 (`.claude/agents/code-reviewer.md`). fresh context에서 `refactor-scan`/`self-review` 체크리스트로 검토.
3. **피드백 반영** — 주 에이전트가 리뷰 결과를 보고 수정. 사용자 확인 후 커밋.
4. **회고 (학습)** — 세션 말미 또는 "놓쳤다가 잡힌 문제"가 있었을 때 `/retrospect` 실행. 기존 스킬·규칙 보강을 우선하고, 꼭 필요할 때만 신규 스킬을 만든다. 한 세션에 과도하게 부풀리지 않는다 (1~2개 이내).

가벼운 단일 파일 변경은 (1)만으로 충분. (2)~(4)는 규모/복잡도에 맞춰 선택적으로 적용.
