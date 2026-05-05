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
	compacting     bool
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

	seq := m.activeSeq()
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

// Close는 백그라운드 goroutine이 Sync를 호출 중이라면 호출 전에 그 goroutine을
// 정지시켜야 한다.
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

// rollLocked는 현재 segment를 닫고 다음 segment를 연다. WAL이 새 segment로
// 전환되기 전에 매니페스트에 먼저 기록하므로, roll과 다음 Append 사이에 크래시가
// 나도 매니페스트가 권위 있는 segment 목록 역할을 한다. close/open/manifest 실패
// 시 WAL은 복구 불가능한 상태가 되므로 closed로 전환한다. 호출자는 WAL을 닫고
// 재오픈해야 한다.
func (w *WAL) rollLocked() error {
	if err := w.closeActiveForRollLocked(); err != nil {
		return err
	}
	ns, err := w.openNextSegmentLocked()
	if err != nil {
		return err
	}
	return w.commitRollLocked(ns)
}

// nextSegment는 막 열린 새 segment의 정체성 — 파일 핸들과 그 seq. commit 단계까지
// 같이 흐른다.
type nextSegment struct {
	file *os.File
	seq  int64
}

// closeActiveForRollLocked는 현재 활성 segment의 buffered write를 비우고 fsync 후
// 닫는다. close 실패는 복구 불가 — WAL을 closed로 전환한다.
func (w *WAL) closeActiveForRollLocked() error {
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
	return nil
}

// openNextSegmentLocked는 다음 seq의 segment 파일을 새로 만들어 연다.
// 아직 매니페스트에 등록되지 않은 고아 상태 — commit은 commitRollLocked가 수행.
func (w *WAL) openNextSegmentLocked() (nextSegment, error) {
	nextSeq := w.seq + 1
	file, err := os.OpenFile(segmentPath(w.dir, nextSeq), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		w.closed = true
		return nextSegment{}, fmt.Errorf("wal: open next segment: %w", err)
	}
	return nextSegment{file: file, seq: nextSeq}, nil
}

// commitRollLocked는 매니페스트에 먼저 기록한 뒤 인메모리 state를 갱신한다 —
// persist 실패 시 state는 손대지 않으므로 rollback 불필요. 매니페스트 갱신 실패는
// 복구 불가이므로 새 segment 파일을 정리하고 WAL을 closed로 전환한다 — 매니페스트
// 분실 후 listSegments 마이그레이션이 고아를 정상 segment로 흡수하는 위험을 없앤다.
func (w *WAL) commitRollLocked(ns nextSegment) error {
	nextManifest := postRollManifest(w.manifest, ns.seq)
	if err := writeManifest(w.dir, nextManifest); err != nil {
		ns.file.Close()
		os.Remove(segmentPath(w.dir, ns.seq))
		w.closed = true
		return fmt.Errorf("wal: update manifest on roll: %w", err)
	}
	w.seq = ns.seq
	w.size = 0
	w.file = ns.file
	w.writer = bufio.NewWriterSize(ns.file, w.bufferSize)
	w.manifest = nextManifest
	return nil
}

// postRollManifest는 prev에 newSeq를 tail에 append한 새 매니페스트를 만든다.
// generation은 +1, checkpointSeq는 그대로. compact.go의 postCompactionManifest와
// 짝을 이루며 매니페스트 파생 로직을 한 추상화 수준으로 통일한다.
func postRollManifest(prev *manifest, newSeq int64) *manifest {
	segments := make([]int64, 0, len(prev.segments)+1)
	segments = append(segments, prev.segments...)
	segments = append(segments, newSeq)
	return &manifest{
		generation:    prev.generation + 1,
		checkpointSeq: prev.checkpointSeq,
		segments:      segments,
	}
}
