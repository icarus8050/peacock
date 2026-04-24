package wal

import (
	"errors"
	"io"
	"os"
	"sync"
	"testing"
)

func TestOpenClose(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen should work.
	w, err = Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	w.Close()
}

func TestAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}

	entries := []Entry{
		{Op: OpPut, Index: 1, CreatedAt: 1000, Data: []byte("k1:v1")},
		{Op: OpPut, Index: 2, CreatedAt: 2000, Data: []byte("k2:v2")},
		{Op: OpDelete, Index: 3, CreatedAt: 3000, Data: []byte("k1")},
	}
	for i := range entries {
		if err := w.Append(&entries[i]); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	path := w.path()
	w.Close()

	// Replay.
	r, err := newReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for i, want := range entries {
		got, err := r.ReadEntry()
		if err != nil {
			t.Fatalf("ReadEntry[%d]: %v", i, err)
		}
		if got.Op != want.Op || got.Index != want.Index || got.CreatedAt != want.CreatedAt || string(got.Data) != string(want.Data) {
			t.Fatalf("entry[%d] mismatch: got %+v, want %+v", i, got, want)
		}
	}

	if _, err := r.ReadEntry(); err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestAppendWithoutExplicitSync(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}

	e := Entry{Op: OpPut, Index: 1, CreatedAt: 1000, Data: []byte("k:v")}
	w.Append(&e)

	path := w.path()
	// Close should flush+sync.
	w.Close()

	r, err := newReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	got, err := r.ReadEntry()
	if err != nil {
		t.Fatalf("ReadEntry: %v", err)
	}
	if string(got.Data) != "k:v" {
		t.Fatalf("expected data 'k:v', got %q", got.Data)
	}
}

func TestMultipleAppendSyncCycles(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}

	e1 := Entry{Op: OpPut, Index: 1, CreatedAt: 1000, Data: []byte("a:1")}
	w.Append(&e1)
	w.Sync()

	e2 := Entry{Op: OpPut, Index: 2, CreatedAt: 2000, Data: []byte("b:2")}
	w.Append(&e2)
	w.Sync()

	path := w.path()
	w.Close()

	r, err := newReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	got1, _ := r.ReadEntry()
	got2, _ := r.ReadEntry()
	if string(got1.Data) != "a:1" || string(got2.Data) != "b:2" {
		t.Fatalf("unexpected data: %q, %q", got1.Data, got2.Data)
	}
}

func TestConcurrentAppend(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			e := Entry{Op: OpPut, Index: int64(i), CreatedAt: TimeStamp(i), Data: []byte("v")}
			w.Append(&e)
		}(i)
	}
	wg.Wait()
	w.Sync()

	path := w.path()
	w.Close()

	// Verify all entries can be read.
	r, err := newReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	count := 0
	for {
		_, err := r.ReadEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadEntry: %v", err)
		}
		count++
	}
	if count != n {
		t.Fatalf("expected %d entries, got %d", n, count)
	}
}

func TestSegmentRollingOnSize(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	// 각 엔트리 크기(~34B)보다 약간 큰 한계로 설정해 매 2~3개마다 롤이 일어나게 한다.
	opts.MaxSegmentSize = 80
	w, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const n = 10
	for i := 0; i < n; i++ {
		e := Entry{Op: OpPut, Index: int64(i), CreatedAt: TimeStamp(i), Data: []byte("v")}
		if err := w.Append(&e); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	seqs, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	if len(seqs) < 2 {
		t.Fatalf("expected rolling to produce multiple segments, got %d", len(seqs))
	}

	r, err := OpenReader(opts)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	count := 0
	for {
		entry, err := r.ReadEntry()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadEntry: %v", err)
		}
		if entry.Index != int64(count) {
			t.Fatalf("out-of-order replay: got %d, want %d", entry.Index, count)
		}
		count++
	}
	if count != n {
		t.Fatalf("expected %d entries, got %d", n, count)
	}
}

func TestReopenAppendsToLatestSegment(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MaxSegmentSize = 80

	w, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 5; i++ {
		e := Entry{Op: OpPut, Index: int64(i), CreatedAt: TimeStamp(i), Data: []byte("v")}
		if err := w.Append(&e); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	seqsBefore, _ := listSegments(dir)
	latest := seqsBefore[len(seqsBefore)-1]

	w, err = Open(opts)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	if w.path() != segmentPath(dir, latest) {
		t.Fatalf("expected active segment %s, got %s", segmentPath(dir, latest), w.path())
	}
	w.Close()
}

func TestOversizedEntryFitsInOwnSegment(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MaxSegmentSize = 50

	w, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	big := make([]byte, 500)
	e := Entry{Op: OpPut, Index: 1, CreatedAt: 1, Data: big}
	if err := w.Append(&e); err != nil {
		t.Fatalf("Append oversized: %v", err)
	}

	seqs, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	if len(seqs) != 1 {
		t.Fatalf("oversized entry should occupy a single segment, got %d segments", len(seqs))
	}

	// 다음 작은 엔트리는 한계를 넘겼으므로 새 세그먼트로 롤되어야 한다.
	small := Entry{Op: OpPut, Index: 2, CreatedAt: 2, Data: []byte("x")}
	if err := w.Append(&small); err != nil {
		t.Fatalf("Append small: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	seqs, err = listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments after roll: %v", err)
	}
	if len(seqs) != 2 {
		t.Fatalf("expected 2 segments after rolling, got %d", len(seqs))
	}

	r, err := OpenReader(opts)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	got, err := r.ReadEntry()
	if err != nil {
		t.Fatalf("ReadEntry: %v", err)
	}
	if len(got.Data) != len(big) {
		t.Fatalf("data len: got %d, want %d", len(got.Data), len(big))
	}
}

func TestAppendAtExactLimitDoesNotRoll(t *testing.T) {
	dir := t.TempDir()

	// 먼저 엔트리 하나의 인코딩 크기를 측정해 한계를 "두 개 정확히"로 맞춘다.
	probe := Entry{Op: OpPut, Index: 0, CreatedAt: 0, Data: []byte("v")}
	entrySize := int64(len(probe.Encode()))

	opts := DefaultOptions(dir)
	opts.MaxSegmentSize = entrySize * 2

	w, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for i := 0; i < 2; i++ {
		e := Entry{Op: OpPut, Index: int64(i), CreatedAt: TimeStamp(i), Data: []byte("v")}
		if err := w.Append(&e); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	seqs, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	if len(seqs) != 1 {
		t.Fatalf("expected 1 segment when size == limit, got %d", len(seqs))
	}
}

func TestConcurrentAppendWithRolling(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MaxSegmentSize = 100
	w, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	const n = 200
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			e := Entry{Op: OpPut, Index: int64(i), CreatedAt: TimeStamp(i), Data: []byte("v")}
			if err := w.Append(&e); err != nil {
				t.Errorf("Append[%d]: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	seqs, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	if len(seqs) < 2 {
		t.Fatalf("expected rolling under concurrent load, got %d segments", len(seqs))
	}

	r, err := OpenReader(opts)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	count := 0
	for {
		_, err := r.ReadEntry()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadEntry: %v", err)
		}
		count++
	}
	if count != n {
		t.Fatalf("expected %d entries, got %d", n, count)
	}
}

func TestReaderStopsOnMiddleSegmentCorruption(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MaxSegmentSize = 80

	w, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		e := Entry{Op: OpPut, Index: int64(i), CreatedAt: TimeStamp(i), Data: []byte("v")}
		if err := w.Append(&e); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	seqs, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	if len(seqs) < 3 {
		t.Fatalf("test setup: need at least 3 segments, got %d", len(seqs))
	}

	// 가운데 세그먼트의 첫 엔트리 페이로드 한 바이트를 뒤집어 CRC 깨뜨림.
	middle := segmentPath(dir, seqs[len(seqs)/2])
	data, err := os.ReadFile(middle)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	data[lenSize+crcSize] ^= 0xFF
	if err := os.WriteFile(middle, data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	r, err := OpenReader(opts)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	var gotErr error
	for {
		_, err := r.ReadEntry()
		if err != nil {
			gotErr = err
			break
		}
	}
	if !errors.Is(gotErr, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch propagated to caller, got %v", gotErr)
	}
}

func TestAppendAfterClose(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	e := Entry{Op: OpPut, Index: 1, CreatedAt: 1000, Data: []byte("k:v")}
	if err := w.Append(&e); err != ErrClosed {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}
