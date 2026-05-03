package raftlog

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func openLog(t *testing.T, dir string) *Log {
	t.Helper()
	l, err := Open(Options{DirPath: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if !l.closed {
			if err := l.Close(); err != nil && !errors.Is(err, ErrClosed) {
				t.Logf("close: %v", err)
			}
		}
	})
	return l
}

func openLogWithOpts(t *testing.T, opts Options) *Log {
	t.Helper()
	l, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if !l.closed {
			if err := l.Close(); err != nil && !errors.Is(err, ErrClosed) {
				t.Logf("close: %v", err)
			}
		}
	})
	return l
}

func mkEntry(idx, term uint64, payload string) Entry {
	return Entry{Term: term, Index: idx, Type: EntryNormal, Data: []byte(payload)}
}

func appendOrFatal(t *testing.T, l *Log, entries ...Entry) {
	t.Helper()
	if err := l.Append(entries); err != nil {
		t.Fatalf("append %d entries: %v", len(entries), err)
	}
}

func TestOpen_NewDirCreatesEmptyLog(t *testing.T) {
	l := openLog(t, t.TempDir())

	if got := l.FirstIndex(); got != 0 {
		t.Fatalf("FirstIndex on new log: got %d, want 0", got)
	}
	if got := l.LastIndex(); got != 0 {
		t.Fatalf("LastIndex on new log: got %d, want 0", got)
	}
	if got := l.LastTerm(); got != 0 {
		t.Fatalf("LastTerm on new log: got %d, want 0", got)
	}
}

func TestAppend_BasicAndQuery(t *testing.T) {
	l := openLog(t, t.TempDir())

	appendOrFatal(t, l,
		mkEntry(1, 1, "a"),
		mkEntry(2, 1, "b"),
		mkEntry(3, 2, "c"),
	)

	if got := l.FirstIndex(); got != 1 {
		t.Errorf("FirstIndex: got %d, want 1", got)
	}
	if got := l.LastIndex(); got != 3 {
		t.Errorf("LastIndex: got %d, want 3", got)
	}
	if got := l.LastTerm(); got != 2 {
		t.Errorf("LastTerm: got %d, want 2", got)
	}

	for _, c := range []struct {
		index uint64
		term  uint64
	}{{1, 1}, {2, 1}, {3, 2}} {
		got, err := l.Term(c.index)
		if err != nil {
			t.Fatalf("Term(%d): %v", c.index, err)
		}
		if got != c.term {
			t.Errorf("Term(%d): got %d, want %d", c.index, got, c.term)
		}
	}

	entries, err := l.Entries(1, 4, 0)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("Entries len: got %d, want 3", len(entries))
	}
	for i, e := range entries {
		if e.Index != uint64(i+1) {
			t.Errorf("entries[%d].Index: got %d", i, e.Index)
		}
	}
}

func TestAppend_RejectsGapAndNonMonotonic(t *testing.T) {
	l := openLog(t, t.TempDir())
	appendOrFatal(t, l, mkEntry(1, 1, "a"))

	if err := l.Append([]Entry{mkEntry(3, 1, "skip")}); !errors.Is(err, ErrAppendGap) {
		t.Errorf("expected ErrAppendGap, got %v", err)
	}
	if err := l.Append([]Entry{mkEntry(2, 1, "x"), mkEntry(4, 1, "y")}); !errors.Is(err, ErrAppendNonMono) {
		t.Errorf("expected ErrAppendNonMono, got %v", err)
	}
	// term regression
	appendOrFatal(t, l, mkEntry(2, 5, "x"))
	if err := l.Append([]Entry{mkEntry(3, 4, "back")}); !errors.Is(err, ErrAppendBadTerm) {
		t.Errorf("expected ErrAppendBadTerm, got %v", err)
	}
	// empty
	if err := l.Append(nil); !errors.Is(err, ErrAppendEmpty) {
		t.Errorf("expected ErrAppendEmpty, got %v", err)
	}
}

func TestEntries_RangeAndMaxBytes(t *testing.T) {
	l := openLog(t, t.TempDir())
	for i := uint64(1); i <= 5; i++ {
		appendOrFatal(t, l, mkEntry(i, 1, fmt.Sprintf("payload-%d", i)))
	}

	es, err := l.Entries(2, 5, 0)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if len(es) != 3 || es[0].Index != 2 || es[2].Index != 4 {
		t.Fatalf("range: got indexes %v", indexes(es))
	}

	// hi 가 lastIndex+1을 넘으면 lastIndex+1로 클램프.
	es, err = l.Entries(3, 100, 0)
	if err != nil {
		t.Fatalf("Entries clamp: %v", err)
	}
	if len(es) != 3 || es[0].Index != 3 || es[2].Index != 5 {
		t.Fatalf("clamp: got indexes %v", indexes(es))
	}

	// hi <= lo → 빈 결과.
	es, err = l.Entries(3, 3, 0)
	if err != nil || es != nil {
		t.Fatalf("empty range: got %v err=%v", es, err)
	}

	// 범위 밖.
	if _, err := l.Entries(10, 12, 0); !errors.Is(err, ErrOutOfRange) {
		t.Errorf("out of range: got %v", err)
	}

	// maxBytes 검증 — 첫 entry는 항상 포함.
	es, err = l.Entries(1, 6, 1)
	if err != nil {
		t.Fatalf("Entries maxBytes: %v", err)
	}
	if len(es) != 1 {
		t.Fatalf("maxBytes=1: expected 1 entry, got %d", len(es))
	}
}

func TestTruncateAfter_ActiveSegmentOnly(t *testing.T) {
	l := openLog(t, t.TempDir())
	for i := uint64(1); i <= 5; i++ {
		appendOrFatal(t, l, mkEntry(i, 1, fmt.Sprintf("p%d", i)))
	}

	if err := l.TruncateAfter(3); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if got := l.LastIndex(); got != 3 {
		t.Errorf("LastIndex after truncate: got %d, want 3", got)
	}
	// 자른 자리부터 다시 append 가능.
	appendOrFatal(t, l, mkEntry(4, 2, "new4"))

	es, err := l.Entries(1, 5, 0)
	if err != nil {
		t.Fatalf("Entries after truncate: %v", err)
	}
	if len(es) != 4 {
		t.Fatalf("len: got %d, want 4", len(es))
	}
	if !bytes.Equal(es[3].Data, []byte("new4")) || es[3].Term != 2 {
		t.Errorf("entry 4: %+v", es[3])
	}
}

func TestTruncateAfter_AcrossSealedSegments(t *testing.T) {
	dir := t.TempDir()
	// MaxSegmentSize를 작게 잡아 segment가 여러 개 생기도록.
	l := openLogWithOpts(t, Options{DirPath: dir, MaxSegmentSize: 100})

	// 각 entry가 ~30B (header 25 + data 4) → segment당 entry 3개 정도.
	for i := uint64(1); i <= 10; i++ {
		appendOrFatal(t, l, mkEntry(i, 1, fmt.Sprintf("d%03d", i)))
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	segCountBefore := len(l.segments)
	if segCountBefore < 2 {
		t.Fatalf("expected multiple segments, got %d", segCountBefore)
	}

	// 봉인 segment 내부의 인덱스로 truncate.
	if err := l.TruncateAfter(4); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if got := l.LastIndex(); got != 4 {
		t.Fatalf("LastIndex: got %d want 4", got)
	}
	// 잘린 뒤 다음 entry append 가능해야 함.
	appendOrFatal(t, l, mkEntry(5, 2, "fresh"))
	got, err := l.Entries(5, 6, 0)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if len(got) != 1 || string(got[0].Data) != "fresh" || got[0].Term != 2 {
		t.Errorf("post-truncate entry: %+v", got)
	}

	// 잘린 segment의 파일이 디스크에서 사라졌는지 확인.
	files, _ := filepath.Glob(filepath.Join(dir, "log-*.seg"))
	if len(files) > segCountBefore {
		t.Errorf("orphan segment files: %v", files)
	}
}

func TestTruncateAfter_AllEntries(t *testing.T) {
	l := openLog(t, t.TempDir())
	for i := uint64(1); i <= 3; i++ {
		appendOrFatal(t, l, mkEntry(i, 1, "x"))
	}
	if err := l.TruncateAfter(0); err != nil {
		t.Fatalf("truncate(0): %v", err)
	}
	if got := l.LastIndex(); got != 0 {
		t.Errorf("LastIndex: got %d want 0", got)
	}
	if got := l.FirstIndex(); got != 0 {
		t.Errorf("FirstIndex: got %d want 0", got)
	}
	// 다시 처음부터 append.
	appendOrFatal(t, l, mkEntry(1, 5, "again"))
	if got := l.LastIndex(); got != 1 {
		t.Errorf("LastIndex post: got %d want 1", got)
	}
}

func TestTruncateAfter_NoOpAtOrBeyondLast(t *testing.T) {
	l := openLog(t, t.TempDir())
	for i := uint64(1); i <= 3; i++ {
		appendOrFatal(t, l, mkEntry(i, 1, "x"))
	}
	if err := l.TruncateAfter(3); err != nil {
		t.Fatalf("at-last: %v", err)
	}
	if err := l.TruncateAfter(99); err != nil {
		t.Fatalf("beyond-last: %v", err)
	}
	if got := l.LastIndex(); got != 3 {
		t.Errorf("LastIndex: got %d want 3", got)
	}
}

func TestReopen_PreservesEntriesAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	l := openLogWithOpts(t, Options{DirPath: dir, MaxSegmentSize: 80})
	for i := uint64(1); i <= 8; i++ {
		appendOrFatal(t, l, mkEntry(i, uint64((i-1)/3+1), fmt.Sprintf("p%d", i)))
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	l2 := openLogWithOpts(t, Options{DirPath: dir, MaxSegmentSize: 80})
	if got := l2.FirstIndex(); got != 1 {
		t.Errorf("FirstIndex after reopen: %d", got)
	}
	if got := l2.LastIndex(); got != 8 {
		t.Errorf("LastIndex after reopen: %d", got)
	}
	es, err := l2.Entries(1, 9, 0)
	if err != nil {
		t.Fatalf("Entries after reopen: %v", err)
	}
	if len(es) != 8 {
		t.Fatalf("entries len: %d", len(es))
	}
	for i, e := range es {
		want := fmt.Sprintf("p%d", i+1)
		if string(e.Data) != want {
			t.Errorf("entries[%d].Data: got %s want %s", i, e.Data, want)
		}
	}
}

func TestReopen_RecoversTailCorruption(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir)
	for i := uint64(1); i <= 3; i++ {
		appendOrFatal(t, l, mkEntry(i, 1, "x"))
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 활성 segment 끝에 garbage 주입.
	files, _ := filepath.Glob(filepath.Join(dir, "log-*.seg"))
	if len(files) == 0 {
		t.Fatal("no segment file")
	}
	f, err := os.OpenFile(files[len(files)-1], os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("open for corrupt: %v", err)
	}
	if _, err := f.Write([]byte{0xff, 0xff, 0xff, 0xff, 0x00}); err != nil {
		f.Close()
		t.Fatalf("write garbage: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close after corrupt: %v", err)
	}

	l2 := openLog(t, dir)
	if got := l2.LastIndex(); got != 3 {
		t.Errorf("LastIndex after tail corruption: got %d want 3", got)
	}
	// 잘린 자리부터 정상 append 되어야 함.
	appendOrFatal(t, l2, mkEntry(4, 2, "ok"))
	if got := l2.LastIndex(); got != 4 {
		t.Errorf("LastIndex after recovery+append: %d", got)
	}
}

func TestSync_FlushesBufferedWrites(t *testing.T) {
	dir := t.TempDir()
	l := openLog(t, dir)
	appendOrFatal(t, l, mkEntry(1, 1, "buffered"))

	// Sync 전에 디스크 크기는 0.
	files, _ := filepath.Glob(filepath.Join(dir, "log-*.seg"))
	if len(files) == 0 {
		t.Fatal("no segment file")
	}
	pre, _ := os.Stat(files[0])

	if err := l.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	post, _ := os.Stat(files[0])
	if post.Size() <= pre.Size() {
		// pre가 0이거나 매우 작아야 정상; post는 entry가 디스크로 갔어야 함.
		t.Errorf("file size after sync did not grow: pre=%d post=%d", pre.Size(), post.Size())
	}
}

func TestClose_Idempotency(t *testing.T) {
	l := openLog(t, t.TempDir())
	if err := l.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := l.Close(); !errors.Is(err, ErrClosed) {
		t.Errorf("second close: got %v want ErrClosed", err)
	}
	if err := l.Append([]Entry{mkEntry(1, 1, "x")}); !errors.Is(err, ErrClosed) {
		t.Errorf("append after close: got %v want ErrClosed", err)
	}
}

func TestSegmentRoll_PreservesContiguity(t *testing.T) {
	dir := t.TempDir()
	l := openLogWithOpts(t, Options{DirPath: dir, MaxSegmentSize: 60})
	for i := uint64(1); i <= 10; i++ {
		appendOrFatal(t, l, mkEntry(i, 1, fmt.Sprintf("p%d", i)))
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if got := len(l.segments); got < 2 {
		t.Fatalf("expected multiple segments, got %d", got)
	}

	// segment 경계 너머 read.
	es, err := l.Entries(1, 11, 0)
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	if len(es) != 10 {
		t.Fatalf("len: %d", len(es))
	}
	for i, e := range es {
		if e.Index != uint64(i+1) {
			t.Fatalf("entry[%d].Index: got %d", i, e.Index)
		}
	}
}

func indexes(es []Entry) []uint64 {
	out := make([]uint64, len(es))
	for i, e := range es {
		out[i] = e.Index
	}
	return out
}
