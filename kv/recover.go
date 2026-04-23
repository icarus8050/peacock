package kv

import (
	"errors"
	"fmt"
	"io"
	"os"

	"peacock/wal"
)

// replay rebuilds the in-memory map by scanning the WAL at path. Tail
// corruption (torn write or CRC mismatch on the last entries) is treated
// as the end of the log — earlier entries are still applied.
//
// Returns the reconstructed map and the next index to assign.
func replay(path string) (map[string][]byte, int64, error) {
	data := make(map[string][]byte)
	var lastIndex int64 = -1

	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return data, 0, nil
	}

	r, err := wal.NewReader(path)
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
