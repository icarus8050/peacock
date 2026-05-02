package kv

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func TestPutGetDelete(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := s.Get("a")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("1")) {
		t.Fatalf("Get: got %q, want %q", got, "1")
	}

	if err := s.Delete("a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, err := s.Get("a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after delete: expected ErrNotFound, got %v", err)
	}
}

func TestOverwrite(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	if err := s.Put("k", []byte("v1")); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if err := s.Put("k", []byte("v2")); err != nil {
		t.Fatalf("Put v2: %v", err)
	}

	got, err := s.Get("k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("Get: got %q, want %q", got, "v2")
	}
}

func TestRecoveryAcrossSessions(t *testing.T) {
	dir := t.TempDir()

	s, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatalf("Put a: %v", err)
	}
	if err := s.Put("b", []byte("2")); err != nil {
		t.Fatalf("Put b: %v", err)
	}
	if err := s.Delete("a"); err != nil {
		t.Fatalf("Delete a: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer s2.Close()

	if _, err := s2.Get("a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get a after replay: expected ErrNotFound, got %v", err)
	}
	got, err := s2.Get("b")
	if err != nil {
		t.Fatalf("Get b: %v", err)
	}
	if !bytes.Equal(got, []byte("2")) {
		t.Fatalf("Get b: got %q, want %q", got, "2")
	}
}

func TestRecoveryEmptyDir(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	if _, err := s.Get("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get: expected ErrNotFound, got %v", err)
	}
}

func TestBackgroundSyncPersists(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.SyncInterval = 50 * time.Millisecond
	s, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// 백그라운드 syncer가 flush할 때까지 대기.
	time.Sleep(150 * time.Millisecond)

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer s2.Close()

	got, err := s2.Get("k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("Get: got %q, want %q", got, "v")
	}
}

func TestRecoveryAcrossRolledSegments(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MaxSegmentSize = 80

	s, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const n = 20
	for i := 0; i < n; i++ {
		key := string(rune('a' + (i % 26)))
		if err := s.Put(key+"-"+string(rune('0'+i%10)), []byte{byte(i)}); err != nil {
			t.Fatalf("Put[%d]: %v", i, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(opts)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer s2.Close()

	for i := 0; i < n; i++ {
		key := string(rune('a'+(i%26))) + "-" + string(rune('0'+i%10))
		got, err := s2.Get(key)
		if err != nil {
			t.Fatalf("Get[%d] %s: %v", i, key, err)
		}
		if !bytes.Equal(got, []byte{byte(i)}) {
			t.Fatalf("Get[%d] %s: got %v, want %v", i, key, got, []byte{byte(i)})
		}
	}
}

func TestCompactionRoundTrip(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MaxSegmentSize = 80
	opts.CompactionTrigger = 2
	opts.CompactionInterval = 20 * time.Millisecond

	s, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// 충분한 봉인 segment를 만들기 위해 puts/deletes를 다수 수행.
	for i := 0; i < 60; i++ {
		key := string(rune('a' + (i % 5)))
		if err := s.Put(key, []byte{byte(i)}); err != nil {
			t.Fatalf("Put[%d]: %v", i, err)
		}
	}
	// 일부 삭제
	if err := s.Delete("a"); err != nil {
		t.Fatalf("Delete a: %v", err)
	}

	// compactor가 한 번 이상 돌 때까지 대기.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entries, _ := walDirEntries(t, dir)
		if entries.checkpointPresent {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	entries, _ := walDirEntries(t, dir)
	if !entries.checkpointPresent {
		t.Fatal("expected checkpoint to be created by background compactor")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 재오픈 후 상태 검증 — 압축된 데이터가 정확히 복원돼야 한다.
	s2, err := Open(opts)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer s2.Close()

	if _, err := s2.Get("a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get a: expected ErrNotFound, got %v", err)
	}
	for _, k := range []string{"b", "c", "d", "e"} {
		got, err := s2.Get(k)
		if err != nil {
			t.Fatalf("Get %s: %v", k, err)
		}
		if len(got) != 1 {
			t.Fatalf("Get %s: got %v, expected 1 byte", k, got)
		}
	}
}

type walDirSnapshot struct {
	checkpointPresent bool
	segmentCount      int
}

func walDirEntries(t *testing.T, dir string) (walDirSnapshot, error) {
	t.Helper()
	es, err := os.ReadDir(dir)
	if err != nil {
		return walDirSnapshot{}, err
	}
	var snap walDirSnapshot
	for _, e := range es {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, ".checkpoint"):
			snap.checkpointPresent = true
		case strings.HasSuffix(name, ".log"):
			snap.segmentCount++
		}
	}
	return snap, nil
}

func TestEmptyValuePutVsDelete(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := s.Put("k", []byte{}); err != nil {
		t.Fatalf("Put empty: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer s2.Close()

	got, err := s2.Get("k")
	if err != nil {
		t.Fatalf("Get: expected present, got %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Get: got %q, want empty", got)
	}
}
