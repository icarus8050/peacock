package wal

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"sync"
)

var ErrClosed = errors.New("wal: closed")

type WAL struct {
	mu             sync.Mutex
	dir            string
	bufferSize     int
	maxSegmentSize int64
	seq            int64
	size           int64
	file           *os.File
	writer         *bufio.Writer
	closed         bool
}

func Open(opts Options) (*WAL, error) {
	opts = opts.withDefaults()

	if err := os.MkdirAll(opts.DirPath, 0755); err != nil {
		return nil, fmt.Errorf("wal: mkdir: %w", err)
	}

	seqs, err := listSegments(opts.DirPath)
	if err != nil {
		return nil, fmt.Errorf("wal: list segments: %w", err)
	}
	var seq int64 = 1
	if len(seqs) > 0 {
		seq = seqs[len(seqs)-1]
	}

	file, size, err := openSegment(opts.DirPath, seq)
	if err != nil {
		return nil, err
	}

	return &WAL{
		dir:            opts.DirPath,
		bufferSize:     opts.BufferSize,
		maxSegmentSize: opts.MaxSegmentSize,
		seq:            seq,
		size:           size,
		file:           file,
		writer:         bufio.NewWriterSize(file, opts.BufferSize),
	}, nil
}

func (w *WAL) Append(entry *Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrClosed
	}

	data := entry.Encode()
	entrySize := int64(len(data))

	// 비어 있지 않은 세그먼트가 이 엔트리로 한계를 넘기면 새 세그먼트로 롤.
	// 비어 있을 때는 한계보다 큰 엔트리라도 그대로 기록한다(무한 롤 방지).
	if w.size > 0 && w.size+entrySize > w.maxSegmentSize {
		if err := w.rollLocked(); err != nil {
			return err
		}
	}

	n, err := w.writer.Write(data)
	w.size += int64(n)
	return err
}

// rollLocked closes the current segment and opens the next one.
// close/open 실패 시 WAL은 복구 불가능한 상태가 되므로 closed로 전환한다.
// 호출자는 WAL을 닫고 재오픈해야 한다.
func (w *WAL) rollLocked() error {
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("wal: flush on roll: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: fsync on roll: %w", err)
	}
	if err := w.file.Close(); err != nil {
		w.closed = true
		return fmt.Errorf("wal: close on roll: %w", err)
	}

	nextSeq := w.seq + 1
	file, err := os.OpenFile(segmentPath(w.dir, nextSeq), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		w.closed = true
		return fmt.Errorf("wal: open next segment: %w", err)
	}

	w.seq = nextSeq
	w.size = 0
	w.file = file
	w.writer = bufio.NewWriterSize(file, w.bufferSize)
	return nil
}

// Sync flushes the buffer and fsyncs the underlying file.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrClosed
	}
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: fsync: %w", err)
	}
	return nil
}

// Close flushes, syncs, and closes the underlying file. Callers that run
// a background goroutine calling Sync must stop it before calling Close.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrClosed
	}
	w.closed = true

	if err := w.writer.Flush(); err != nil {
		w.file.Close()
		return fmt.Errorf("wal: flush on close: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		w.file.Close()
		return fmt.Errorf("wal: fsync on close: %w", err)
	}
	return w.file.Close()
}

func (w *WAL) path() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Name()
}
