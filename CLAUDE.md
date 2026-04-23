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
- Timeout values are in seconds unless noted otherwise.

## Architecture

Go 1.26 + Fiber v2 web server with graceful shutdown.

**Startup flow:** `main.go` → `config.Load()` → `kv.Open(...)` → `server.New(cfg)` → `handler.Register(app, store)` → `server.Start()` → `store.Close()`

- **`config/`** — Env-var-based configuration. All settings have defaults.
- **`server/`** — Creates Fiber app, listens on port, handles graceful shutdown on SIGINT/SIGTERM.
- **`handler/`** — Route registration. `handler.go` is the central registry (`Register` function); each feature file (e.g. `health.go`, `kv.go`) defines its own routes and handlers. New feature handlers should follow this pattern: create a file, define a `registerXxx(app, deps...)` function, and call it from `Register`.
- **`wal/`** — Write-Ahead Log for storage engine recovery. Binary-encoded entries with CRC32 checksums. Uses buffered writes with batch fsync (`WAL.Sync()` or a composed `Syncer` for background interval). Entry format: `TotalLen(4B) | CRC32(4B) | Op(1B) | Index(8B) | TimeStamp(8B) | DataLen(4B) | Data` (little-endian). Core API: `Open`, `(*WAL).Append`, `(*WAL).Sync`, `(*WAL).Close`, `NewReader`, `(*Reader).ReadEntry`, `(*Reader).Close`, `NewSyncer`, `(*Syncer).Start`, `(*Syncer).Stop`.
- **`kv/`** — In-memory key-value store backed by WAL. Writes append to WAL (buffered), a background `Syncer` fsyncs on `KV_SYNC_INTERVAL_MS`. On `Open`, the WAL is replayed into the map; tail corruption (`ErrIncompleteEntry` / `ErrChecksumMismatch`) is treated as the end of the log. Entry.Data payload: `KeyLen(4B) | Key | Value` (Delete carries empty Value). The default `SyncInterval` (`100ms`) is owned by `kv` — `config` passes `0` to opt into it. Core API: `Open`, `Options`, `DefaultOptions`, `(*Store).Get`, `(*Store).Put`, `(*Store).Delete`, `(*Store).Close`. HTTP surface: `GET /kv/:key`, `PUT /kv/:key` (raw body = value), `DELETE /kv/:key`.

## Rules

프로젝트 규칙은 별도 파일로 분리되어 있다. 각 파일을 참조하여 작업한다.

- @.claude/rules/commit.md — 커밋 메시지 규칙
- @.claude/rules/go-style.md — Go 코드 스타일 규칙
- @.claude/rules/testing.md — 테스트 작성 규칙
