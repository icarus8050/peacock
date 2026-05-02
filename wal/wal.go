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

	nextSegments := make([]int64, 0, len(w.manifest.segments)+1)
	nextSegments = append(nextSegments, w.manifest.segments...)
	nextSegments = append(nextSegments, nextSeq)
	nextManifest := &manifest{
		generation:    w.manifest.generation + 1,
		checkpointSeq: w.manifest.checkpointSeq,
		segments:      nextSegments,
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
	if checkpointSeq <= 0 {
		return fmt.Errorf("wal: commit compaction: invalid checkpointSeq=%d", checkpointSeq)
	}
	if len(removedSeqs) == 0 {
		return fmt.Errorf("wal: commit compaction: removedSeqs empty")
	}

	// 활성 segment(매니페스트 마지막 원소)는 압축 대상이 아니다 — 봉인부에 속한
	// seq만 허용. 활성을 제거하면 w.seq/w.file과 매니페스트가 어긋난다.
	sealed := make(map[int64]bool, len(w.manifest.segments)-1)
	for _, s := range w.manifest.segments[:len(w.manifest.segments)-1] {
		sealed[s] = true
	}
	removed := make(map[int64]bool, len(removedSeqs))
	for _, s := range removedSeqs {
		if !sealed[s] {
			return fmt.Errorf("wal: commit compaction: seq=%d is not a sealed segment", s)
		}
		removed[s] = true
	}
	newSegments := make([]int64, 0, len(w.manifest.segments))
	for _, s := range w.manifest.segments {
		if !removed[s] {
			newSegments = append(newSegments, s)
		}
	}

	prevCheckpointSeq := w.manifest.checkpointSeq
	nextManifest := &manifest{
		generation:    w.manifest.generation + 1,
		checkpointSeq: checkpointSeq,
		segments:      newSegments,
	}
	if err := writeManifest(w.dir, nextManifest); err != nil {
		w.closed = true
		return fmt.Errorf("wal: commit compaction manifest: %w", err)
	}
	w.manifest = nextManifest

	// 매니페스트 commit 후의 옛 파일 정리. ENOENT(이미 지워짐, crash 후 재시도 등)와
	// 그 외 실패 모두 무시 — 매니페스트 밖 상태이므로 정확성 영향 없음. logger 도입
	// 시 정리 실패만 별도 logging하도록 hook할 자리.
	for _, s := range removedSeqs {
		os.Remove(segmentPath(w.dir, s))
	}
	// 현재 흐름에서는 새 압축이 항상 더 큰 seq를 사용하므로 prev != new가 보장되지만,
	// 같은 seq가 들어오는 경우(미래 변경) 옛 파일이 곧 새 파일이라 unlink하면 새
	// 체크포인트까지 사라지므로 가드를 둔다.
	if prevCheckpointSeq > 0 && prevCheckpointSeq != checkpointSeq {
		os.Remove(checkpointPath(w.dir, prevCheckpointSeq))
	}

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
