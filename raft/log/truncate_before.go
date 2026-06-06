package raftlog

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"

	"peacock/internal/fsutil"
)

// materializeBoundaryLocked는 bound segment에서 keepFromIndex..lastIndex 범위 entry를
// 새 seq 파일로 작성한다. 옛 boundary 파일은 그대로 두고, 매니페스트 commit이 단일
// 결정 포인트가 되도록 분리한다 — commit 전 크래시면 새 파일은 매니페스트 밖이라
// 무시된다. 반환은 새 segState (디스크 작성 + 인메모리 entries 재계산까지 완료).
func (l *Log) materializeBoundaryLocked(bound *segState, keepFromIndex uint64) (*segState, error) {
	if keepFromIndex <= bound.firstIndex || keepFromIndex > bound.lastIndex {
		return nil, fmt.Errorf("raftlog: materialize: keepFromIndex=%d outside (%d, %d]",
			keepFromIndex, bound.firstIndex, bound.lastIndex)
	}

	keepCount := bound.lastIndex - keepFromIndex + 1
	payloads := make([][]byte, 0, keepCount)
	terms := make([]uint64, 0, keepCount)
	for idx := keepFromIndex; idx <= bound.lastIndex; idx++ {
		e, _, err := l.readEntryLocked(bound, idx)
		if err != nil {
			return nil, fmt.Errorf("raftlog: materialize read idx=%d: %w", idx, err)
		}
		payloads = append(payloads, e.Encode())
		terms = append(terms, e.Term)
	}

	newSeq := l.maxSeqLocked() + 1
	newPath := segmentPath(l.dir, newSeq)
	if err := writeSegmentFile(newPath, payloads); err != nil {
		return nil, err
	}

	entries := make([]entryLoc, keepCount)
	var off int64
	for i, p := range payloads {
		entries[i] = entryLoc{offset: off, term: terms[i]}
		off += int64(len(p))
	}
	return &segState{
		seq:        newSeq,
		firstIndex: keepFromIndex,
		lastIndex:  bound.lastIndex,
		size:       off,
		entries:    entries,
	}, nil
}

// writeSegmentFile은 payloads(이미 인코딩된 entry 바이트들)을 path에 atomic하게 쓴다.
// tmp+fsync+rename+dir-fsync. 크래시 시 tmp 또는 새 path가 고아로 남을 수 있으나
// 매니페스트가 source of truth라 무해.
func writeSegmentFile(path string, payloads [][]byte) error {
	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("raftlog: open segment tmp: %w", err)
	}
	bw := bufio.NewWriter(f)
	for _, p := range payloads {
		if _, err := bw.Write(p); err != nil {
			f.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("raftlog: write segment tmp: %w", err)
		}
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("raftlog: flush segment tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("raftlog: fsync segment tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("raftlog: close segment tmp: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("raftlog: rename segment tmp: %w", err)
	}
	if err := fsutil.SyncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("raftlog: fsync segment dir: %w", err)
	}
	return nil
}

// maxSeqLocked는 segments 중 최대 seq를 반환한다. TruncateBefore가 boundary segment를
// 새 seq로 재작성하면 그 seq가 활성 segment seq보다 커질 수 있으므로, 다음 roll의
// 새 seq 할당은 활성이 아니라 maxSeq 기준이어야 한다.
func (l *Log) maxSeqLocked() int64 {
	var maxSeq int64
	for _, s := range l.segments {
		if s.seq > maxSeq {
			maxSeq = s.seq
		}
	}
	return maxSeq
}

// unlinkSegmentsLocked는 주어진 segment 파일들을 best-effort로 지운다. 호출자가
// "이들은 이미 매니페스트 밖이라 고아"임을 보장한 상태에서 부른다. unlink 실패는
// manifest가 source of truth라 디스크 낭비일 뿐 무해 — 무시한다.
func (l *Log) unlinkSegmentsLocked(segments []*segState) {
	for _, s := range segments {
		_ = os.Remove(segmentPath(l.dir, s.seq))
	}
}

// swapActiveToLocked는 현 activeFile을 닫고 path를 새 활성 파일(O_APPEND)로 연다.
// 어느 단계든 실패하면 Log를 closed로 전환하고 activeFile/Writer를 nil로 비워 — 후속
// 호출이 닫힌 fd에 접근하지 않게 한다(closed=true가 모든 진입점에서 ErrClosed로 차단하지만
// 안전 여분).
func (l *Log) swapActiveToLocked(path string) error {
	if err := l.activeFile.Close(); err != nil {
		l.activeFile = nil
		l.activeWriter = nil
		l.closed = true
		return fmt.Errorf("raftlog: close active for swap: %w", err)
	}
	active, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		l.activeFile = nil
		l.activeWriter = nil
		l.closed = true
		return fmt.Errorf("raftlog: open new active: %w", err)
	}
	l.activeFile = active
	l.activeWriter = bufio.NewWriterSize(active, l.opts.BufferSize)
	return nil
}
