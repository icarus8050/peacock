# Peacock

Go + Fiber 기반의 고가용성 분산 시스템 서버.

## 요구 사항

- Go 1.26+

## 프로젝트 구조

```
peacock/
├── main.go             # 진입점 (config → server → handler 연결)
├── config/
│   └── config.go       # 환경변수 기반 설정 관리
├── server/
│   └── server.go       # Fiber 앱 생성 + Graceful Shutdown
└── handler/
    ├── handler.go      # 라우트 등록 중앙 관리
    └── health.go       # 헬스체크 엔드포인트
```

## 빠른 시작

```bash
# 의존성 설치
go mod tidy

# 서버 실행
go run main.go
```

기본적으로 `:3000` 포트에서 서버가 시작됩니다.

## API 엔드포인트

| Method | Path      | 설명                         | 응답 예시              |
|--------|-----------|------------------------------|------------------------|
| GET    | `/health` | Liveness probe (프로세스 생존 확인) | `{"status":"ok"}`    |
| GET    | `/ready`  | Readiness probe (트래픽 수용 가능 여부) | `{"status":"ready"}` |

## 환경변수

| 변수명              | 설명                    | 기본값   |
|---------------------|-------------------------|----------|
| `PORT`              | 서버 포트               | `3000`   |
| `READ_TIMEOUT`      | 읽기 타임아웃 (초)      | `5`      |
| `WRITE_TIMEOUT`     | 쓰기 타임아웃 (초)      | `10`     |
| `SHUTDOWN_TIMEOUT`  | Graceful Shutdown 대기 시간 (초) | `10` |

```bash
# 예시: 포트와 타임아웃 변경
PORT=8080 SHUTDOWN_TIMEOUT=30 go run main.go
```

## Graceful Shutdown

`SIGINT`(Ctrl+C) 또는 `SIGTERM` 시그널 수신 시 진행 중인 요청을 처리한 후 안전하게 종료됩니다.
`SHUTDOWN_TIMEOUT` 내에 완료되지 않으면 강제 종료됩니다.

## 향후 확장 경로

```
현재 구조
 → middleware 추가 (로깅, 인증, CORS 등)
  → store 계층 (DB 연동)
   → internal/ 도입 (Clean Architecture)
    → cmd/ 분리 (멀티 바이너리)
     → cluster/discovery 패키지 (분산 시스템)
```

각 단계에서 `main.go`는 거의 변경 없이 패키지만 추가하면 됩니다.
