package wal

import (
	"encoding/binary"
	"fmt"
	"os"
)

// newReader opens a single WAL segment file for reading. Test-only helper
// used when a test needs to inspect a specific segment file by path.
func newReader(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("wal: open: %w", err)
	}
	return &Reader{file: file}, nil
}

// decodeEntry decodes a full on-disk entry (TotalLen prefix + body). Test-only
// round-trip helper for Encode verification; production reads use decodeBody
// via Reader.readCurrent.
func decodeEntry(data []byte) (Entry, error) {
	if len(data) < lenSize+headerSize {
		return Entry{}, ErrIncompleteEntry
	}

	totalLen := int(binary.LittleEndian.Uint32(data[0:lenSize]))
	if len(data) < lenSize+totalLen {
		return Entry{}, ErrIncompleteEntry
	}

	return decodeBody(data[lenSize : lenSize+totalLen])
}
