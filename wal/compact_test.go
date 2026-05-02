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

// keyOfRaw는 wal 단위 테스트에서 entry.Data 자체를 key로 쓰는 단순 추출기다.
// kv 페이로드 형식(KeyLen|Key|Value)을 wal 레이어에 들이지 않기 위해 별도로 정의.
func keyOfRaw(e *Entry) ([]byte, error) {
	return e.Data, nil
}

func TestCompactSkipsBelowTrigger(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })

	if err := w.Append(&Entry{Op: OpPut, Index: 1, CreatedAt: 1, Data: []byte("k1")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	// 봉인 segment가 없어 trigger=4 미달.
	ran, err := w.Compact(4, keyOfRaw)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if ran {
		t.Fatal("expected Compact to skip when below trigger")
	}
}

func TestCompactBuildsCheckpointFromSealed(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MaxSegmentSize = 50
	w, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })

	for i := 0; i < 10; i++ {
		if err := w.Append(&Entry{Op: OpPut, Index: int64(i), CreatedAt: TimeStamp(i), Data: []byte{byte('a' + i)}}); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	seqsBefore, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	if len(seqsBefore) < 5 {
		t.Fatalf("test setup: need ≥5 segments(4 sealed + active), got %d", len(seqsBefore))
	}

	ran, err := w.Compact(4, keyOfRaw)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if !ran {
		t.Fatal("expected Compact to run")
	}

	m, err := readManifest(dir)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if m.checkpointSeq == 0 {
		t.Fatal("expected manifest to reference a checkpoint")
	}
	if len(m.segments) != 1 {
		t.Fatalf("expected only active segment in manifest, got %v", m.segments)
	}
}

func TestCompactRespectsDeletes(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MaxSegmentSize = 50
	w, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })

	// k1 Put → k1 Delete → k2 Put. Delete 이후 압축에서 k1은 사라져야 한다.
	puts := [][]byte{[]byte("k1"), []byte("k1"), []byte("k2"), []byte("k3"), []byte("k4")}
	ops := []EntryType{OpPut, OpDelete, OpPut, OpPut, OpPut}
	for i, k := range puts {
		if err := w.Append(&Entry{Op: ops[i], Index: int64(i), CreatedAt: TimeStamp(i), Data: k}); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	for i := 0; i < 5; i++ {
		if err := w.Append(&Entry{Op: OpPut, Index: int64(100 + i), CreatedAt: TimeStamp(100 + i), Data: []byte{byte('a' + i)}}); err != nil {
			t.Fatalf("Append filler[%d]: %v", i, err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	ran, err := w.Compact(4, keyOfRaw)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if !ran {
		t.Fatal("expected Compact to run")
	}

	// 새 체크포인트를 직접 읽어 k1이 빠졌는지 확인.
	m, err := readManifest(dir)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	r, err := newReader(checkpointPath(dir, m.checkpointSeq))
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer r.Close()

	keys := map[string]bool{}
	for {
		entry, err := r.ReadEntry()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadEntry: %v", err)
		}
		keys[string(entry.Data)] = true
	}
	if keys["k1"] {
		t.Fatal("k1 should be absent (deleted before compaction)")
	}
	if !keys["k2"] {
		t.Fatal("k2 should be present")
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
