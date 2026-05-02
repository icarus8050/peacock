package kv

import (
	"errors"
	"fmt"
	"io"
	"os"

	"peacock/wal"
)

// replay는 opts가 가리키는 WAL을 스캔해 인메모리 맵을 복원한다. 마지막 엔트리의
// tail corruption(torn write 또는 CRC mismatch)은 로그의 끝으로 간주하며, 그
// 이전까지의 엔트리는 정상 적용된다.
//
// 복원된 맵과 다음으로 할당할 index를 반환한다.
func replay(opts wal.Options) (map[string][]byte, int64, error) {
	data := make(map[string][]byte)
	var lastIndex int64 = -1

	r, err := wal.OpenReader(opts)
	if errors.Is(err, os.ErrNotExist) {
		return data, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("kv: open reader: %w", err)
	}
	defer r.Close()

	for {
		entry, err := r.ReadEntry()
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, wal.ErrIncompleteEntry) || errors.Is(err, wal.ErrChecksumMismatch) {
			break
		}
		if err != nil {
			return nil, 0, fmt.Errorf("kv: replay: %w", err)
		}

		key, value, err := decodePayload(entry.Data)
		if err != nil {
			return nil, 0, fmt.Errorf("kv: decode at index %d: %w", entry.Index, err)
		}

		switch entry.Op {
		case wal.OpPut:
			data[key] = value
		case wal.OpDelete:
			delete(data, key)
		}

		lastIndex = entry.Index
	}

	return data, lastIndex + 1, nil
}
