package raftlog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

var (
	ErrClosed         = errors.New("raftlog: closed")
	ErrOutOfRange     = errors.New("raftlog: index out of range")
	ErrAppendGap      = errors.New("raftlog: append index gap")
	ErrAppendNonMono  = errors.New("raftlog: append index not monotonic")
	ErrAppendBadTerm  = errors.New("raftlog: append term regression")
	ErrAppendEmpty    = errors.New("raftlog: append empty batch")
	ErrTruncateActive = errors.New("raftlog: truncate would discard active state inconsistently")
)

// entryLoc는 단일 entry의 segment 내 위치와 term을 들고 있다.
// term을 함께 들면 Term(index) 조회가 디스크 접근 없이 끝난다.
type entryLoc struct {
	offset int64
	term   uint64
}

// segState는 한 segment의 인메모리 상태. 봉인/활성 segment 모두 동일한 형태.
// 활성 segment의 entries는 Append마다 자라고, 봉인 segment는 Open 시 한 번만 채워진다.
type segState struct {
	seq        int64
	firstIndex uint64 // 0 = 비어 있음
	lastIndex  uint64 // 0 = 비어 있음
	size       int64
	entries    []entryLoc
}

// Log는 Raft 합의의 영속 로그. 임의 인덱스 truncate를 지원해 conflicting entry
// 처리를 자연스럽게 한다.
//
// 동시성 모델: 모든 공개 메서드는 단일 sync.Mutex로 직렬화된다. raft.Node가 단일
// goroutine에서 호출하므로 경합은 없지만 외부 read 호출(Status/Entries 조회)을
// 안전하게 노출하기 위해 락을 둔다.
type Log struct {
	mu     sync.Mutex
	dir    string
	opts   Options
	closed bool

	manifest *manifest
	// segments[len-1]가 활성, 나머지는 봉인. 항상 최소 1개.
	segments []*segState

	activeFile   *os.File
	activeWriter *bufio.Writer
}

// Open은 dir의 raft log를 연다. 매니페스트가 없으면 빈 활성 segment 1개를 가진
// 새 로그를 만든다. 활성 segment의 tail corruption은 정상 종료의 신호로 보고
// 절단하지만, 봉인 segment의 corruption은 fatal로 보고한다.
func Open(opts Options) (*Log, error) {
	opts = opts.withDefaults()
	if err := os.MkdirAll(opts.DirPath, 0755); err != nil {
		return nil, fmt.Errorf("raftlog: mkdir: %w", err)
	}
	m, err := loadOrInitManifest(opts.DirPath)
	if err != nil {
		return nil, err
	}

	l := &Log{dir: opts.DirPath, opts: opts, manifest: m}
	stale, err := l.replaySegments()
	if err != nil {
		return nil, err
	}
	if err := l.openActiveLocked(); err != nil {
		return nil, err
	}
	if stale {
		if err := l.persistManifestLocked(); err != nil {
			l.activeFile.Close()
			return nil, err
		}
	}
	return l, nil
}

// replaySegments는 매니페스트의 모든 segment를 디스크에서 스캔해 인메모리 상태를
// 채운다. 활성 segment의 tail truncation은 stale=true로 알려 호출부가 매니페스트
// 갱신을 결정하게 한다.
func (l *Log) replaySegments() (stale bool, err error) {
	for i, sm := range l.manifest.segments {
		target := scanTarget{
			path:     segmentPath(l.dir, sm.seq),
			seq:      sm.seq,
			isActive: i == len(l.manifest.segments)-1,
		}
		ss, truncated, err := scanSegment(target)
		if err != nil {
			return false, err
		}
		if !target.isActive {
			if err := verifySealedMeta(sm, ss); err != nil {
				return false, err
			}
		}
		if truncated {
			stale = true
		}
		l.segments = append(l.segments, ss)
	}
	return stale, nil
}

// FirstIndex는 로그에 남아 있는 첫 entry의 index를 반환한다. 비어 있으면 0.
// snapshot truncation 후에는 firstIndex > 1이 될 수 있다(M2 이후).
func (l *Log) FirstIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, s := range l.segments {
		if s.firstIndex != 0 {
			return s.firstIndex
		}
	}
	return 0
}

// LastIndex는 로그의 마지막 entry index를 반환한다. 비어 있으면 0.
func (l *Log) LastIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastIndexLocked()
}

// LastTerm은 로그의 마지막 entry term을 반환한다. 비어 있으면 0.
func (l *Log) LastTerm() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	last := l.lastIndexLocked()
	if last == 0 {
		return 0
	}
	t, _ := l.termLocked(last)
	return t
}

// Term은 주어진 index의 entry term을 반환한다. index가 범위 밖이면 ErrOutOfRange.
func (l *Log) Term(index uint64) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.termLocked(index)
}

// Entries는 [lo, hi) 범위의 entry를 반환한다. maxBytes가 0이 아니면 누적 인코딩
// 크기가 그 한계에 도달하기 전까지만 반환한다(최소 1개는 항상 포함). lo가 범위
// 밖이면 ErrOutOfRange.
func (l *Log) Entries(lo, hi, maxBytes uint64) ([]Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if hi <= lo {
		return nil, nil
	}
	last := l.lastIndexLocked()
	first := l.firstIndexLocked()
	if first == 0 || lo < first || lo > last {
		return nil, ErrOutOfRange
	}
	if hi > last+1 {
		hi = last + 1
	}

	out := make([]Entry, 0, hi-lo)
	var bytes uint64
	for idx := lo; idx < hi; idx++ {
		segIdx, _, ok := l.findSegmentLocked(idx)
		if !ok {
			return nil, ErrOutOfRange
		}
		seg := l.segments[segIdx]
		entry, encoded, err := l.readEntryLocked(seg, idx)
		if err != nil {
			return nil, err
		}
		if maxBytes > 0 && len(out) > 0 && bytes+uint64(encoded) > maxBytes {
			break
		}
		out = append(out, entry)
		bytes += uint64(encoded)
	}
	return out, nil
}

// Append는 entries를 활성 segment에 추가한다. 호출자는 다음을 보장해야 한다:
//   - len(entries) > 0
//   - entries[i].Index == entries[i-1].Index + 1
//   - entries[0].Index == LastIndex()+1 (또는 비어 있으면 1+)
//   - 모든 term은 직전 entry의 term 이상 (강한 단조성은 검증하지 않음)
//
// 위반 시 즉시 에러를 반환하고 디스크에 아무것도 쓰지 않는다.
func (l *Log) Append(entries []Entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrClosed
	}
	if len(entries) == 0 {
		return ErrAppendEmpty
	}

	last := l.lastIndexLocked()
	expectedNext := last + 1
	if entries[0].Index != expectedNext {
		return fmt.Errorf("%w: got %d want %d", ErrAppendGap, entries[0].Index, expectedNext)
	}
	prevTerm := uint64(0)
	if last != 0 {
		t, err := l.termLocked(last)
		if err != nil {
			return err
		}
		prevTerm = t
	}
	for i := range entries {
		e := &entries[i]
		if i > 0 && e.Index != entries[i-1].Index+1 {
			return fmt.Errorf("%w: entry %d index=%d prev=%d", ErrAppendNonMono, i, e.Index, entries[i-1].Index)
		}
		if e.Term < prevTerm {
			return fmt.Errorf("%w: entry %d term=%d prev=%d", ErrAppendBadTerm, i, e.Term, prevTerm)
		}
		prevTerm = e.Term
	}

	for i := range entries {
		if err := l.appendOneLocked(&entries[i]); err != nil {
			return err
		}
	}
	return nil
}

// TruncateAfter는 (index, lastIndex] 범위의 entry를 모두 제거한다.
// index가 lastIndex 이상이면 no-op. index < firstIndex이면 ErrOutOfRange.
//
// 동작:
//  1. index보다 뒤에 시작하는 봉인 segment는 파일째 삭제하고 매니페스트에서 제거.
//  2. index를 포함하는 segment는 파일을 byte 오프셋으로 truncate하고 활성 segment로 전환.
//  3. 매니페스트를 새 generation으로 한 번에 갱신.
//
// 갱신 도중 크래시가 나면 매니페스트 swap이 단일 commit point가 된다 — 이전이면
// 변경 무효, 이후면 truncate 확정.
func (l *Log) TruncateAfter(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrClosed
	}

	last := l.lastIndexLocked()
	if index >= last {
		return nil
	}
	first := l.firstIndexLocked()
	if first != 0 && index < first-1 {
		// index+1 ~ first-1 사이의 truncate 요청 — 첫 entry보다도 앞을 자르려는 것.
		// Raft에서 이 케이스는 InstallSnapshot이 처리할 영역이라 일반 TruncateAfter로
		// 다루지 않는다.
		return fmt.Errorf("%w: index=%d below firstIndex=%d", ErrOutOfRange, index, first)
	}

	// 활성 segment의 buffered write를 먼저 비워야 디스크 truncate가 일관된다.
	if err := l.activeWriter.Flush(); err != nil {
		return fmt.Errorf("raftlog: flush before truncate: %w", err)
	}

	// 자를 지점: index+1을 포함하는 segment를 찾는다. index가 0이면 전체 truncate.
	cutIdx := index + 1
	cutSegIdx := -1
	for i, s := range l.segments {
		if s.firstIndex == 0 {
			continue // 빈 활성 segment
		}
		if cutIdx >= s.firstIndex && cutIdx <= s.lastIndex {
			cutSegIdx = i
			break
		}
	}

	switch {
	case cutSegIdx == -1 && index == 0:
		// 모든 entry 제거. 첫 segment를 비우고 그 뒤는 모두 삭제.
		if err := l.discardSegmentsAfterLocked(0); err != nil {
			return err
		}
		if err := l.resetActiveLocked(); err != nil {
			return err
		}
	case cutSegIdx == -1:
		// 도달 불가 — 위에서 index >= last로 걸러졌고 firstIndex 검증도 통과한 상태.
		return fmt.Errorf("raftlog: truncate cut point not found (index=%d)", index)
	default:
		// cutSegIdx의 entry[cutIdx-firstIndex]까지 잘라내고, 그 뒤 segment는 모두 삭제.
		seg := l.segments[cutSegIdx]
		cutOffset := seg.entries[cutIdx-seg.firstIndex].offset
		if err := l.discardSegmentsAfterLocked(cutSegIdx); err != nil {
			return err
		}
		if err := l.truncateSegmentToLocked(cutSegIdx, cutOffset, cutIdx-1); err != nil {
			return err
		}
	}

	return l.persistManifestLocked()
}

// TruncateBefore는 [firstIndex, index) 범위 entry를 제거한다. M2(snapshot)에서
// 구현한다. 현재는 호출 자체를 막지 않고 nop으로 둔다 — Open/Append 코드가 호출하지
// 않도록 invariant로 관리.
func (l *Log) TruncateBefore(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	// M2에서 구현. 지금은 호출 시점에 명시적으로 알 수 있게 panic 대신 에러.
	if index == 0 {
		return nil
	}
	return errors.New("raftlog: TruncateBefore not implemented (M2)")
}

// Sync는 활성 segment의 buffered write를 disk에 flush + fsync한다.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if err := l.activeWriter.Flush(); err != nil {
		return fmt.Errorf("raftlog: flush: %w", err)
	}
	if err := l.activeFile.Sync(); err != nil {
		return fmt.Errorf("raftlog: fsync: %w", err)
	}
	return nil
}

// Close는 buffered write를 flush + fsync하고 활성 파일을 닫는다.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	l.closed = true

	if err := l.activeWriter.Flush(); err != nil {
		l.activeFile.Close()
		return fmt.Errorf("raftlog: flush on close: %w", err)
	}
	if err := l.activeFile.Sync(); err != nil {
		l.activeFile.Close()
		return fmt.Errorf("raftlog: fsync on close: %w", err)
	}
	return l.activeFile.Close()
}

// ─── 내부 헬퍼 ─────────────────────────────────────────────────────────────

func (l *Log) firstIndexLocked() uint64 {
	for _, s := range l.segments {
		if s.firstIndex != 0 {
			return s.firstIndex
		}
	}
	return 0
}

func (l *Log) lastIndexLocked() uint64 {
	for i := len(l.segments) - 1; i >= 0; i-- {
		if l.segments[i].lastIndex != 0 {
			return l.segments[i].lastIndex
		}
	}
	return 0
}

func (l *Log) termLocked(index uint64) (uint64, error) {
	segIdx, _, ok := l.findSegmentLocked(index)
	if !ok {
		return 0, fmt.Errorf("%w: index=%d", ErrOutOfRange, index)
	}
	seg := l.segments[segIdx]
	return seg.entries[index-seg.firstIndex].term, nil
}

// findSegmentLocked는 index를 포함하는 segment의 위치를 반환한다.
// 인덱스 → segment 매핑은 단순 선형 탐색 — segment 수가 보통 수십 단위라 충분.
// 봉인 segment 수가 수천을 넘기면 binary search로 바꾼다.
func (l *Log) findSegmentLocked(index uint64) (segIdx int, posInSeg uint64, ok bool) {
	for i, s := range l.segments {
		if s.firstIndex == 0 {
			continue
		}
		if index >= s.firstIndex && index <= s.lastIndex {
			return i, index - s.firstIndex, true
		}
	}
	return -1, 0, false
}

// readEntryLocked는 seg의 index 위치 entry를 디스크에서 읽어온다. Append로 인해
// 활성 segment의 buffered 데이터는 아직 디스크에 없을 수 있으므로 활성 segment
// 읽기 전에 flush가 필요하다. 호출자가 보장해야 함.
//
// 반환값의 두 번째는 인코딩된 entry의 총 byte 수(TotalLen prefix 포함).
func (l *Log) readEntryLocked(seg *segState, index uint64) (Entry, int, error) {
	loc := seg.entries[index-seg.firstIndex]

	// 활성 segment의 buffered tail이 디스크에 안 가 있을 수 있어 flush한다.
	if seg.seq == l.activeSeqLocked() {
		if err := l.activeWriter.Flush(); err != nil {
			return Entry{}, 0, fmt.Errorf("raftlog: flush before read: %w", err)
		}
	}

	path := segmentPath(l.dir, seg.seq)
	f, err := os.Open(path)
	if err != nil {
		return Entry{}, 0, fmt.Errorf("raftlog: open segment for read: %w", err)
	}
	defer f.Close()

	if _, err := f.Seek(loc.offset, io.SeekStart); err != nil {
		return Entry{}, 0, fmt.Errorf("raftlog: seek: %w", err)
	}

	var lenBuf [lenSize]byte
	if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
		return Entry{}, 0, fmt.Errorf("raftlog: read length: %w", err)
	}
	totalLen := binary.LittleEndian.Uint32(lenBuf[:])

	body := make([]byte, totalLen)
	if _, err := io.ReadFull(f, body); err != nil {
		return Entry{}, 0, fmt.Errorf("raftlog: read body: %w", err)
	}

	entry, err := decodeBody(body)
	if err != nil {
		return Entry{}, 0, err
	}
	return entry, lenSize + int(totalLen), nil
}

func (l *Log) activeSeqLocked() int64 {
	return l.segments[len(l.segments)-1].seq
}

func (l *Log) appendOneLocked(e *Entry) error {
	encoded := e.Encode()
	entrySize := int64(len(encoded))

	active := l.segments[len(l.segments)-1]
	// 비어 있지 않은 활성 segment가 한계를 넘기면 새 segment로 롤.
	if active.size > 0 && active.size+entrySize > l.opts.MaxSegmentSize {
		if err := l.rollLocked(); err != nil {
			return err
		}
		active = l.segments[len(l.segments)-1]
	}

	if _, err := l.activeWriter.Write(encoded); err != nil {
		return fmt.Errorf("raftlog: write entry: %w", err)
	}

	loc := entryLoc{offset: active.size, term: e.Term}
	active.entries = append(active.entries, loc)
	if active.firstIndex == 0 {
		active.firstIndex = e.Index
	}
	active.lastIndex = e.Index
	active.size += entrySize
	return nil
}

func (l *Log) rollLocked() error {
	if err := l.activeWriter.Flush(); err != nil {
		return fmt.Errorf("raftlog: flush on roll: %w", err)
	}
	if err := l.activeFile.Sync(); err != nil {
		return fmt.Errorf("raftlog: fsync on roll: %w", err)
	}
	if err := l.activeFile.Close(); err != nil {
		l.closed = true
		return fmt.Errorf("raftlog: close on roll: %w", err)
	}

	nextSeq := l.activeSeqLocked() + 1
	file, err := os.OpenFile(segmentPath(l.dir, nextSeq), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		l.closed = true
		return fmt.Errorf("raftlog: open next segment: %w", err)
	}

	newSeg := &segState{seq: nextSeq}
	prevSegments := l.segments
	l.segments = append(l.segments, newSeg)
	l.activeFile = file
	l.activeWriter = bufio.NewWriterSize(file, l.opts.BufferSize)

	if err := l.persistManifestLocked(); err != nil {
		// 매니페스트 갱신 실패 — 새 segment 파일은 매니페스트 밖 고아이므로 정리.
		l.segments = prevSegments
		file.Close()
		os.Remove(segmentPath(l.dir, nextSeq))
		l.closed = true
		return fmt.Errorf("raftlog: update manifest on roll: %w", err)
	}
	return nil
}

// discardSegmentsAfterLocked는 segIdx 이후의 모든 segment 파일을 삭제하고 메모리에서도 제거한다.
// segIdx 자체는 남긴다. 활성 파일이 삭제 대상에 포함되면 닫는다.
func (l *Log) discardSegmentsAfterLocked(segIdx int) error {
	if segIdx >= len(l.segments)-1 {
		return nil
	}

	// 활성 파일을 먼저 닫는다 (활성은 항상 마지막 segment).
	if err := l.activeFile.Close(); err != nil {
		l.closed = true
		return fmt.Errorf("raftlog: close active before truncate: %w", err)
	}
	l.activeFile = nil
	l.activeWriter = nil

	for i := segIdx + 1; i < len(l.segments); i++ {
		if err := os.Remove(segmentPath(l.dir, l.segments[i].seq)); err != nil && !errors.Is(err, os.ErrNotExist) {
			l.closed = true
			return fmt.Errorf("raftlog: remove segment seq=%d: %w", l.segments[i].seq, err)
		}
	}
	l.segments = l.segments[:segIdx+1]
	return nil
}

// truncateSegmentToLocked는 segIdx의 파일을 cutOffset byte로 자르고 인메모리 entries도
// 자른 뒤 활성 segment로 전환한다. newLastIndex는 잘린 후의 마지막 entry index
// (자른 segment가 비어버리면 0).
func (l *Log) truncateSegmentToLocked(segIdx int, cutOffset int64, newLastIndex uint64) error {
	seg := l.segments[segIdx]
	path := segmentPath(l.dir, seg.seq)

	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		l.closed = true
		return fmt.Errorf("raftlog: open for truncate: %w", err)
	}
	if err := f.Truncate(cutOffset); err != nil {
		f.Close()
		l.closed = true
		return fmt.Errorf("raftlog: truncate file: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		l.closed = true
		return fmt.Errorf("raftlog: fsync after truncate: %w", err)
	}
	// O_APPEND로 다시 열어 활성으로 사용.
	if err := f.Close(); err != nil {
		l.closed = true
		return fmt.Errorf("raftlog: close after truncate: %w", err)
	}
	active, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		l.closed = true
		return fmt.Errorf("raftlog: reopen active after truncate: %w", err)
	}

	// 인메모리 entries 절단.
	if newLastIndex < seg.firstIndex {
		seg.entries = seg.entries[:0]
		seg.firstIndex = 0
		seg.lastIndex = 0
	} else {
		keep := newLastIndex - seg.firstIndex + 1
		seg.entries = seg.entries[:keep]
		seg.lastIndex = newLastIndex
	}
	seg.size = cutOffset

	l.activeFile = active
	l.activeWriter = bufio.NewWriterSize(active, l.opts.BufferSize)
	return nil
}

// resetActiveLocked는 활성 segment를 비운다 (truncate to 0).
func (l *Log) resetActiveLocked() error {
	seg := l.segments[len(l.segments)-1]
	path := segmentPath(l.dir, seg.seq)

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		l.closed = true
		return fmt.Errorf("raftlog: reset active: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		l.closed = true
		return fmt.Errorf("raftlog: fsync reset active: %w", err)
	}
	if err := f.Close(); err != nil {
		l.closed = true
		return fmt.Errorf("raftlog: close reset active: %w", err)
	}
	active, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		l.closed = true
		return fmt.Errorf("raftlog: reopen active after reset: %w", err)
	}

	seg.entries = seg.entries[:0]
	seg.firstIndex = 0
	seg.lastIndex = 0
	seg.size = 0

	l.activeFile = active
	l.activeWriter = bufio.NewWriterSize(active, l.opts.BufferSize)
	return nil
}

// persistManifestLocked는 현 segments 상태를 매니페스트로 영속화한다.
func (l *Log) persistManifestLocked() error {
	next := &manifest{
		generation: l.manifest.generation + 1,
		segments:   make([]segmentMeta, len(l.segments)),
	}
	for i, s := range l.segments {
		next.segments[i] = segmentMeta{
			seq:        s.seq,
			firstIndex: s.firstIndex,
			lastIndex:  s.lastIndex,
			size:       s.size,
		}
	}
	if err := writeManifest(l.dir, next); err != nil {
		return err
	}
	l.manifest = next
	return nil
}

// openActiveLocked는 활성 segment의 파일 핸들을 RDWR+APPEND로 연다.
// scanSegment가 size를 이미 정확히 채워두므로 이 함수는 그저 파일만 연다.
func (l *Log) openActiveLocked() error {
	active := l.segments[len(l.segments)-1]
	path := segmentPath(l.dir, active.seq)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("raftlog: open active segment: %w", err)
	}
	l.activeFile = file
	l.activeWriter = bufio.NewWriterSize(file, l.opts.BufferSize)
	return nil
}

// verifySealedMeta는 봉인 segment의 매니페스트 메타와 디스크 스캔 결과가 일치하는지
// 검증한다. 어긋나면 매니페스트 손상.
func verifySealedMeta(sm segmentMeta, ss *segState) error {
	if ss.firstIndex == sm.firstIndex && ss.lastIndex == sm.lastIndex && ss.size == sm.size {
		return nil
	}
	return fmt.Errorf("%w: sealed segment seq=%d disk=%d..%d/%dB manifest=%d..%d/%dB",
		ErrManifestCorrupt, sm.seq,
		ss.firstIndex, ss.lastIndex, ss.size,
		sm.firstIndex, sm.lastIndex, sm.size)
}

// scanTarget은 스캔 대상 segment의 정체성과 맥락을 표현한다 — 파일 위치, 식별자,
// 매니페스트 상의 역할(활성/봉인). 한 단위로 묶여 깊은 콜 체인을 흐른다.
type scanTarget struct {
	path     string // segment 파일 경로
	seq      int64  // segment seq (에러 메시지 식별자)
	isActive bool   // 활성/봉인 정책 분기 (tail 손상 처리)
}

type scanResult int

const (
	scanOK scanResult = iota
	scanEOF
	scanTailDamage
)

// scanSegment는 target.path의 segment 파일을 읽어 segState를 채운다.
// target.isActive=true이면 tail corruption(부분 entry, CRC 오류)을 정상 종료의 signal로
// 보고 그 지점에서 절단한다 — 절단된 size를 호출자에 알려 매니페스트 갱신을 유도.
// false면 corruption은 fatal.
func scanSegment(target scanTarget) (*segState, bool, error) {
	ss, truncated, err := readSegmentEntries(target)
	if err != nil {
		return nil, false, err
	}
	if truncated {
		if err := truncateTailTo(target.path, ss.size); err != nil {
			return nil, false, err
		}
	}
	return ss, truncated, nil
}

// readSegmentEntries는 segment 파일을 처음부터 읽어 인메모리 segState를 만든다.
// 활성 segment에서 tail corruption이 감지되면 truncated=true로 알리고 그 지점까지의
// 결과를 반환한다 — 디스크 파일 절단은 호출자(scanSegment)가 수행.
func readSegmentEntries(target scanTarget) (*segState, bool, error) {
	f, err := os.OpenFile(target.path, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return nil, false, fmt.Errorf("raftlog: open segment for scan: %w", err)
	}
	defer f.Close()

	br := bufio.NewReader(f)
	ss := &segState{seq: target.seq}
	var off int64

	for {
		entry, encoded, result, err := readNextEntry(br, target, off)
		if err != nil {
			return nil, false, err
		}
		if result == scanEOF {
			ss.size = off
			return ss, false, nil
		}
		if result == scanTailDamage {
			ss.size = off
			return ss, true, nil
		}
		ss.entries = append(ss.entries, entryLoc{offset: off, term: entry.Term})
		if ss.firstIndex == 0 {
			ss.firstIndex = entry.Index
		}
		ss.lastIndex = entry.Index
		off += int64(encoded)
	}
}

// readNextEntry는 br에서 다음 entry 하나를 읽어 분류한다.
//   - scanOK: entry/encoded가 유효, 호출자가 누적
//   - scanEOF: 정상 EOF (tail truncation 아님)
//   - scanTailDamage: 활성 segment의 tail corruption (header/body 절단 또는 CRC 오류)
//
// 봉인 segment에서 동일한 손상이 보이면 fatal 에러로 반환한다.
func readNextEntry(br *bufio.Reader, target scanTarget, off int64) (Entry, int, scanResult, error) {
	var lenBuf [lenSize]byte
	_, err := io.ReadFull(br, lenBuf[:])
	if errors.Is(err, io.EOF) {
		return noEntry(scanEOF, nil)
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return noEntry(tailOrSealed(target, off, "truncated header", nil))
	}
	if err != nil {
		return noEntry(0, fmt.Errorf("raftlog: read length: %w", err))
	}
	totalLen := binary.LittleEndian.Uint32(lenBuf[:])

	body := make([]byte, totalLen)
	if _, err := io.ReadFull(br, body); err != nil {
		return noEntry(tailOrSealed(target, off, "truncated body", nil))
	}

	entry, err := decodeBody(body)
	if errors.Is(err, ErrChecksumMismatch) || errors.Is(err, ErrIncompleteEntry) {
		return noEntry(tailOrSealed(target, off, "corrupt entry", err))
	}
	if err != nil {
		return noEntry(0, err)
	}
	return entry, lenSize + int(totalLen), scanOK, nil
}

// noEntry는 readNextEntry의 실패/종료 분기에서 entry/encoded 자리에 zero를 채워
// (Entry, int, scanResult, error) 4-tuple을 만드는 어댑터. 호출부에서 매번
// `Entry{}, 0`을 반복하지 않도록 한 곳에 모은다.
func noEntry(r scanResult, err error) (Entry, int, scanResult, error) {
	return Entry{}, 0, r, err
}

// tailOrSealed는 tail-style 손상에 대한 정책 분기를 한 곳으로 모은다 — 활성이면
// scanTailDamage로 정상 종료, 봉인이면 fatal 에러. 결과 값(scanResult)은 err == nil
// 일 때만 의미 있다.
func tailOrSealed(target scanTarget, off int64, msg string, src error) (scanResult, error) {
	if target.isActive {
		return scanTailDamage, nil
	}
	if src == nil {
		return 0, fmt.Errorf("raftlog: sealed segment seq=%d %s at %d", target.seq, msg, off)
	}
	return 0, fmt.Errorf("raftlog: sealed segment seq=%d %s at %d: %w", target.seq, msg, off, src)
}

// truncateTailTo는 활성 segment의 디스크 파일을 size 바이트로 절단해 다음 Append가
// 깨끗한 상태에서 시작하게 한다.
func truncateTailTo(path string, size int64) error {
	wf, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("raftlog: open for tail truncate: %w", err)
	}
	if err := wf.Truncate(size); err != nil {
		wf.Close()
		return fmt.Errorf("raftlog: truncate tail: %w", err)
	}
	if err := wf.Sync(); err != nil {
		wf.Close()
		return fmt.Errorf("raftlog: fsync tail truncate: %w", err)
	}
	if err := wf.Close(); err != nil {
		return fmt.Errorf("raftlog: close tail truncate: %w", err)
	}
	return nil
}
