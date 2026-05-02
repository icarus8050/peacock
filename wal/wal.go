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

// rollLocked는 현재 segment를 닫고 다음 segment를 연다. WAL이 새 segment로
// 전환되기 전에 매니페스트에 먼저 기록하므로, roll과 다음 Append 사이에 크래시가
// 나도 매니페스트가 권위 있는 segment 목록 역할을 한다. close/open/manifest 실패
// 시 WAL은 복구 불가능한 상태가 되므로 closed로 전환한다. 호출자는 WAL을 닫고
// 재오픈해야 한다.
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

	nextManifest := postRollManifest(w.manifest, nextSeq)
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

// CommitCompaction은 압축 결과를 매니페스트에 atomic하게 반영한다.
// 호출자는 호출 전 checkpointPath(dir, checkpointSeq)에 체크포인트 파일을
// 작성·fsync 완료한 상태여야 한다.
//
// removedSeqs는 새 매니페스트에서 제거할 봉인 segment seq들. 매니페스트 갱신이
// 성공하면 옛 segment 파일과 옛 체크포인트(이전 generation의 *.checkpoint)도
// 함께 정리된다 — 정리 실패는 매니페스트 밖이라 정확성 영향이 없으므로 진행을
// 막지 않는다.
//
// 매니페스트 갱신 실패 시 WAL은 복구 불가 상태(closed=true)로 전환되며 호출자는
// 재오픈해야 한다.
func (w *WAL) CommitCompaction(checkpointSeq int64, removedSeqs []int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrClosed
	}
	if err := validateCompactionArgs(w.manifest, checkpointSeq, removedSeqs); err != nil {
		return err
	}

	prevCheckpointSeq := w.manifest.checkpointSeq
	next := postCompactionManifest(w.manifest, checkpointSeq, removedSeqs)
	if err := writeManifest(w.dir, next); err != nil {
		w.closed = true
		return fmt.Errorf("wal: commit compaction manifest: %w", err)
	}
	w.manifest = next

	cleanupCompactedFiles(w.dir, removedSeqs, prevCheckpointSeq)
	return nil
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
