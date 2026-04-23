package wal

import (
	"io"
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
	path := w.Path()
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

	path := w.Path()
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

	path := w.Path()
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

	path := w.Path()
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
