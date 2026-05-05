package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

type Reader struct {
	file         *os.File
	paths        []string
	onCheckpoint bool
}

func OpenReader(opts Options) (*Reader, error) {
	opts = opts.withDefaults()

	paths, err := pathsForRead(opts.DirPath)
	if err != nil {
		return nil, err
	}

	first := paths.checkpoint
	rest := paths.segments
	onCheckpoint := true
	if first == "" {
		first = paths.segments[0]
		rest = paths.segments[1:]
		onCheckpoint = false
	}

	file, err := os.Open(first)
	if err != nil {
		return nil, fmt.Errorf("wal: open: %w", err)
	}
	return &Reader{file: file, paths: rest, onCheckpoint: onCheckpoint}, nil
}

func (r *Reader) ReadEntry() (Entry, error) {
	for {
		entry, err := r.readCurrent()
		// 체크포인트는 atomic write로 항상 완전해야 한다 — tail-style 에러는
		// 디스크 손상을 의미하므로 segment의 정상 tail truncation과 구별해 보고한다.
		// errors.Is(nil, X)는 false라 outer err != nil 가드는 불필요.
		if r.onCheckpoint && (errors.Is(err, ErrIncompleteEntry) || errors.Is(err, ErrChecksumMismatch)) {
			return Entry{}, fmt.Errorf("%w: %v", ErrCheckpointCorrupt, err)
		}
		if errors.Is(err, io.EOF) && len(r.paths) > 0 {
			if err := r.advance(); err != nil {
				return Entry{}, err
			}
			continue
		}
		return entry, err
	}
}

func (r *Reader) Close() error {
	return r.file.Close()
}

func (r *Reader) readCurrent() (Entry, error) {
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

func (r *Reader) advance() error {
	if err := r.file.Close(); err != nil {
		return fmt.Errorf("wal: close segment: %w", err)
	}
	next := r.paths[0]
	r.paths = r.paths[1:]
	file, err := os.Open(next)
	if err != nil {
		return fmt.Errorf("wal: open segment: %w", err)
	}
	r.file = file
	r.onCheckpoint = false
	return nil
}
