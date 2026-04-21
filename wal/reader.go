package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

type Reader struct {
	file *os.File
}

// NewReader opens the WAL file at path for sequential reading.
func NewReader(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("wal: open: %w", err)
	}
	return &Reader{file: file}, nil
}

// ReadEntry reads the next entry from the WAL. Returns io.EOF when there
// are no more entries. Returns ErrIncompleteEntry or ErrChecksumMismatch
// if the entry is corrupt or partially written.
func (r *Reader) ReadEntry() (Entry, error) {
	var lenBuf [lenSize]byte
	_, err := io.ReadFull(r.file, lenBuf[:])
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return Entry{}, ErrIncompleteEntry
	}
	if errors.Is(err, io.EOF) {
		return Entry{}, io.EOF
	}
	if err != nil {
		return Entry{}, fmt.Errorf("wal: read length: %w", err)
	}

	totalLen := binary.LittleEndian.Uint32(lenBuf[:])
	if totalLen < uint32(headerSize) {
		return Entry{}, ErrIncompleteEntry
	}

	body := make([]byte, totalLen)
	if _, err := io.ReadFull(r.file, body); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return Entry{}, ErrIncompleteEntry
		}
		return Entry{}, fmt.Errorf("wal: read body: %w", err)
	}

	return decodeBody(body)
}

// Close closes the underlying file.
func (r *Reader) Close() error {
	return r.file.Close()
}
