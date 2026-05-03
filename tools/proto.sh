#!/usr/bin/env bash
# proto/*.proto → raft/pb/*.pb.go 생성.
#
# 사전 요구:
#   brew install protobuf
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
#
# 호출:
#   ./tools/proto.sh

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Go 플러그인은 $GOPATH/bin에 깔리는데 PATH에 없을 수 있어 명시적으로 보강한다.
GOBIN="$(go env GOPATH)/bin"
export PATH="$GOBIN:$PATH"

if ! command -v protoc >/dev/null 2>&1; then
  echo "error: protoc not found (try: brew install protobuf)" >&2
  exit 1
fi
for plugin in protoc-gen-go protoc-gen-go-grpc; do
  if ! command -v "$plugin" >/dev/null 2>&1; then
    echo "error: $plugin not found in $GOBIN" >&2
    exit 1
  fi
done

OUT_DIR="raft/pb"
mkdir -p "$OUT_DIR"

# 기존 산출물 정리. 손으로 짠 파일은 raft/pb 바깥에 둔다.
find "$OUT_DIR" -name '*.pb.go' -delete

protoc \
  --proto_path=proto \
  --go_out=. \
  --go_opt=module=peacock \
  --go-grpc_out=. \
  --go-grpc_opt=module=peacock \
  proto/raft.proto

echo "generated:"
find "$OUT_DIR" -name '*.pb.go' | sort
