package wal

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpointNameFormat(t *testing.T) {
	cases := []struct {
		seq  int64
		want string
	}{
		{1, "wal-0000000001.checkpoint"},
		{42, "wal-0000000042.checkpoint"},
		{1234567890, "wal-1234567890.checkpoint"},
	}
	for _, c := range cases {
		if got := checkpointName(c.seq); got != c.want {
			t.Fatalf("seq=%d: got %q, want %q", c.seq, got, c.want)
		}
	}
}

func TestWriteCheckpointEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.checkpoint")
	if err := WriteCheckpoint(path, nil); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("expected empty file, got %d bytes", info.Size())
	}
	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tmp file should be renamed away, stat err=%v", err)
	}
}

func TestWriteCheckpointRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rt.checkpoint")

	in := []*Entry{
		{Op: OpPut, Index: 1, CreatedAt: 100, Data: []byte("alpha")},
		{Op: OpPut, Index: 2, CreatedAt: 200, Data: []byte("beta-with-longer-payload")},
		{Op: OpPut, Index: 3, CreatedAt: 300, Data: []byte("")},
	}
	if err := WriteCheckpoint(path, in); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}

	r, err := newReader(path)
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer r.Close()

	for i, want := range in {
		got, err := r.ReadEntry()
		if err != nil {
			t.Fatalf("ReadEntry[%d]: %v", i, err)
		}
		if got.Op != want.Op || got.Index != want.Index || got.CreatedAt != want.CreatedAt || string(got.Data) != string(want.Data) {
			t.Fatalf("entry[%d]: got %+v, want %+v", i, got, want)
		}
	}
	if _, err := r.ReadEntry(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestWriteCheckpointOverwritesExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "overwrite.checkpoint")

	if err := os.WriteFile(path, []byte("garbage that should be truncated"), 0644); err != nil {
		t.Fatalf("WriteFile garbage: %v", err)
	}

	in := []*Entry{
		{Op: OpPut, Index: 1, CreatedAt: 1, Data: []byte("v")},
	}
	if err := WriteCheckpoint(path, in); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}

	r, err := newReader(path)
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer r.Close()

	got, err := r.ReadEntry()
	if err != nil {
		t.Fatalf("ReadEntry: %v", err)
	}
	if string(got.Data) != "v" {
		t.Fatalf("data: got %q, want %q", got.Data, "v")
	}
	if _, err := r.ReadEntry(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF after overwrite, got %v", err)
	}
}
