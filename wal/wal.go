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
	manifest       *manifest
	closed         bool
}

func Open(opts Options) (*WAL, error) {
	opts = opts.withDefaults()

	if err := os.MkdirAll(opts.DirPath, 0755); err != nil {
		return nil, fmt.Errorf("wal: mkdir: %w", err)
	}

	m, err := loadOrInitManifest(opts.DirPath)
	if err != nil {
		return nil, err
	}

	seq := m.segments[len(m.segments)-1]
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
		manifest:       m,
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

// rollLocked closes the current segment and opens the next one. The new
// segment is recorded in the manifest before the WAL transitions to it so
// that a crash between roll and the next Append leaves the manifest as the
// authoritative segment list. close/open/manifest 실패 시 WAL은 복구 불가능한
// 상태가 되므로 closed로 전환한다. 호출자는 WAL을 닫고 재오픈해야 한다.
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

	nextSegments := make([]int64, 0, len(w.manifest.segments)+1)
	nextSegments = append(nextSegments, w.manifest.segments...)
	nextSegments = append(nextSegments, nextSeq)
	nextManifest := &manifest{
		generation: w.manifest.generation + 1,
		segments:   nextSegments,
	}
	if err := writeManifest(w.dir, nextManifest); err != nil {
		file.Close()
		// 매니페스트 갱신 실패 — 새 세그먼트 파일은 어떤 매니페스트에도 기록되지
		// 않은 고아다. OpenReader는 매니페스트를 신뢰해 무시하지만, 매니페스트가
		// 분실되어 다음 Open이 listSegments 마이그레이션 경로를 타면 이 고아가
		// 정상 세그먼트로 흡수된다. 즉시 정리해 그 위험을 없앤다.
		os.Remove(segmentPath(w.dir, nextSeq))
		w.closed = true
		return fmt.Errorf("wal: update manifest on roll: %w", err)
	}

	w.seq = nextSeq
	w.size = 0
	w.file = file
	w.writer = bufio.NewWriterSize(file, w.bufferSize)
	w.manifest = nextManifest
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
