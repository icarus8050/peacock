package raftsnap

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// writeSnapshot은 meta+data를 commit하는 테스트 헬퍼.
func writeSnapshot(t *testing.T, s *Store, meta SnapshotMeta, data []byte) {
	t.Helper()
	w, err := s.Create(meta)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

// readLatest는 Latest로 데이터를 읽어 바이트로 반환하는 헬퍼.
func readLatest(t *testing.T, s *Store) (SnapshotMeta, []byte) {
	t.Helper()
	meta, rc, err := s.Latest()
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return meta, data
}

func TestMetaEncodeDecodeRoundtrip(t *testing.T) {
	want := SnapshotMeta{Index: 12345, Term: 7}
	got, err := decodeMeta(want.encode())
	if err != nil {
		t.Fatalf("decodeMeta: %v", err)
	}
	if got != want {
		t.Fatalf("expected %+v, got %+v", want, got)
	}
}

func TestLatestNoSnapshot(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, _, err := s.Latest(); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("expected ErrNoSnapshot, got %v", err)
	}
	if _, ok, err := s.LatestMeta(); err != nil || ok {
		t.Fatalf("expected (false, nil), got (%v, %v)", ok, err)
	}
}

func TestCreateCommitLatest(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	meta := SnapshotMeta{Index: 100, Term: 3}
	payload := []byte("hello snapshot payload")
	writeSnapshot(t, s, meta, payload)

	gotMeta, gotData := readLatest(t, s)
	if gotMeta != meta {
		t.Fatalf("expected meta %+v, got %+v", meta, gotMeta)
	}
	if string(gotData) != string(payload) {
		t.Fatalf("expected data %q, got %q", payload, gotData)
	}
}

func TestLatestMetaWithoutData(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	meta := SnapshotMeta{Index: 42, Term: 2}
	writeSnapshot(t, s, meta, []byte("x"))

	got, ok, err := s.LatestMeta()
	if err != nil || !ok {
		t.Fatalf("expected (true, nil), got (%v, %v)", ok, err)
	}
	if got != meta {
		t.Fatalf("expected %+v, got %+v", meta, got)
	}
}

func TestLatestPicksHighestIndex(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writeSnapshot(t, s, SnapshotMeta{Index: 50, Term: 2}, []byte("old"))
	writeSnapshot(t, s, SnapshotMeta{Index: 150, Term: 4}, []byte("new"))

	meta, data := readLatest(t, s)
	if meta.Index != 150 {
		t.Fatalf("expected latest index 150, got %d", meta.Index)
	}
	if string(data) != "new" {
		t.Fatalf("expected data %q, got %q", "new", data)
	}
}

func TestCommitPrunesOlder(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	old := SnapshotMeta{Index: 50, Term: 2}
	writeSnapshot(t, s, old, []byte("old"))
	writeSnapshot(t, s, SnapshotMeta{Index: 150, Term: 4}, []byte("new"))

	if _, err := os.Stat(filepath.Join(dir, metaName(old))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected old meta pruned, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, dataName(old))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected old data pruned, stat err=%v", err)
	}
}

func TestCancelDiscards(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w, err := s.Create(SnapshotMeta{Index: 10, Term: 1})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("discard me")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Cancel(); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected empty dir after cancel, got %d entries", len(entries))
	}
}

func TestWriterGuardAfterFinalize(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Cancel 후 Commit → ErrWriterDone (닫힌 파일 nil-deref 패닉 대신).
	w, err := s.Create(SnapshotMeta{Index: 1, Term: 1})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := w.Cancel(); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := w.Commit(); !errors.Is(err, ErrWriterDone) {
		t.Fatalf("expected ErrWriterDone on commit-after-cancel, got %v", err)
	}

	// 정상 Commit 후 재-Commit → ErrWriterDone.
	w2, err := s.Create(SnapshotMeta{Index: 2, Term: 1})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w2.Write([]byte("x")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w2.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := w2.Commit(); !errors.Is(err, ErrWriterDone) {
		t.Fatalf("expected ErrWriterDone on double-commit, got %v", err)
	}
}

func TestReopenSeesCommitted(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	meta := SnapshotMeta{Index: 77, Term: 5}
	writeSnapshot(t, s, meta, []byte("persist"))

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	gotMeta, gotData := readLatest(t, s2)
	if gotMeta != meta {
		t.Fatalf("expected %+v, got %+v", meta, gotMeta)
	}
	if string(gotData) != "persist" {
		t.Fatalf("expected %q, got %q", "persist", gotData)
	}
}

func TestOpenCleansOrphans(t *testing.T) {
	dir := t.TempDir()
	// 짝 메타 없는 데이터(메타 rename 전 크래시 흔적) + tmp 잔존.
	orphanData := filepath.Join(dir, dataName(SnapshotMeta{Index: 9, Term: 1}))
	if err := os.WriteFile(orphanData, []byte("orphan"), 0644); err != nil {
		t.Fatalf("write orphan data: %v", err)
	}
	tmp := filepath.Join(dir, dataName(SnapshotMeta{Index: 9, Term: 1})+tmpSuffix)
	if err := os.WriteFile(tmp, []byte("tmp"), 0644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}

	if _, err := Open(dir); err != nil {
		t.Fatalf("Open: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected orphans cleaned, got %d entries", len(entries))
	}
}

func TestOpenKeepsCommittedData(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	meta := SnapshotMeta{Index: 30, Term: 2}
	writeSnapshot(t, s, meta, []byte("keep"))

	// 잔존 tmp만 추가 — committed 데이터는 보존되어야 한다.
	tmp := filepath.Join(dir, dataName(SnapshotMeta{Index: 99, Term: 9})+tmpSuffix)
	if err := os.WriteFile(tmp, []byte("tmp"), 0644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := os.Stat(tmp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected tmp cleaned, stat err=%v", err)
	}
	gotMeta, gotData := readLatest(t, s2)
	if gotMeta != meta || string(gotData) != "keep" {
		t.Fatalf("committed snapshot not preserved: meta=%+v data=%q", gotMeta, gotData)
	}
}

func TestLatestCorruptMeta(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	meta := SnapshotMeta{Index: 20, Term: 1}
	writeSnapshot(t, s, meta, []byte("data"))

	// 메타 파일을 손상시킨다.
	metaPath := filepath.Join(dir, metaName(meta))
	if err := os.WriteFile(metaPath, []byte("garbage-not-valid-meta-bytes"), 0644); err != nil {
		t.Fatalf("corrupt meta: %v", err)
	}
	if _, _, err := s.Latest(); !errors.Is(err, ErrSnapshotCorrupt) {
		t.Fatalf("expected ErrSnapshotCorrupt, got %v", err)
	}
}

func TestLatestDataMissing(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	meta := SnapshotMeta{Index: 25, Term: 1}
	writeSnapshot(t, s, meta, []byte("data"))

	if err := os.Remove(filepath.Join(dir, dataName(meta))); err != nil {
		t.Fatalf("remove data: %v", err)
	}
	if _, _, err := s.Latest(); !errors.Is(err, ErrSnapshotCorrupt) {
		t.Fatalf("expected ErrSnapshotCorrupt, got %v", err)
	}
}
