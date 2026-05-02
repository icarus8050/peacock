package wal

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
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

func TestOrphanCheckpointIgnoredOnReopen(t *testing.T) {
	// 시나리오: 압축이 체크포인트 파일을 작성한 직후 매니페스트 갱신 전에 죽음.
	// 디스크엔 *.checkpoint이 있지만 매니페스트는 옛 상태. 다음 Open은 매니페스트
	// 신뢰 정책에 따라 고아 체크포인트를 무시하고 옛 segments로 정상 가동해야 한다.
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MaxSegmentSize = 80
	w, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := w.Append(&Entry{Op: OpPut, Index: int64(i), CreatedAt: TimeStamp(i), Data: []byte("v")}); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 매니페스트 갱신 없이 체크포인트 파일만 작성 — 압축이 step 6.2 직전에 죽은 상태.
	seqs, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	cpSeq := seqs[len(seqs)-2] // 임의 봉인 seq
	orphan := []*Entry{{Op: OpPut, Index: 999, CreatedAt: 999, Data: []byte("orphan")}}
	if err := WriteCheckpoint(checkpointPath(dir, cpSeq), orphan); err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}

	// 재오픈 — 매니페스트는 여전히 옛 상태이므로 체크포인트는 무시되어야 한다.
	w2, err := Open(opts)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	if w2.manifest.checkpointSeq != 0 {
		t.Fatalf("expected manifest.checkpointSeq=0 (orphan ignored), got %d", w2.manifest.checkpointSeq)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close 2: %v", err)
	}
}

func TestStaleSegmentsIgnoredAfterCompaction(t *testing.T) {
	// 시나리오: 매니페스트 commit은 끝났지만 cleanupCompactedFiles가 진행되기 전에
	// 죽음. 옛 봉인 segments가 디스크에 남았지만 새 매니페스트는 그것을 참조하지
	// 않으므로 OpenReader에 보이지 않아야 한다.
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MaxSegmentSize = 80
	w, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })

	for i := 0; i < 5; i++ {
		if err := w.Append(&Entry{Op: OpPut, Index: int64(i), CreatedAt: TimeStamp(i), Data: []byte{byte('a' + i)}}); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// 압축 직전 봉인 segments 백업.
	seqsBefore, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	sealed := seqsBefore[:len(seqsBefore)-1]
	backups := make(map[int64][]byte)
	for _, s := range sealed {
		data, err := os.ReadFile(segmentPath(dir, s))
		if err != nil {
			t.Fatalf("ReadFile %d: %v", s, err)
		}
		backups[s] = data
	}

	if ran, err := w.Compact(2, keyOfRaw); err != nil {
		t.Fatalf("Compact: %v", err)
	} else if !ran {
		t.Fatal("expected Compact to run")
	}

	// cleanup 실패를 시뮬레이션 — 옛 segments를 디스크에 다시 써넣는다.
	for s, data := range backups {
		if err := os.WriteFile(segmentPath(dir, s), data, 0644); err != nil {
			t.Fatalf("restore %d: %v", s, err)
		}
	}

	// OpenReader가 매니페스트만 신뢰하면 옛 segments는 보이지 않아야 한다.
	r, err := OpenReader(opts)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	// 첫 entry는 체크포인트에서 — 옛 봉인 segments의 entry가 leaking 됐다면 체크포인트
	// entry 외에 추가 entry들이 보일 것이다.
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
	// state는 5개 키(a..e). 활성 segment는 비어 있을 수 있고 체크포인트만 있을 가능성.
	// 옛 봉인 segments(각 1+개 entry)가 보였다면 count가 5보다 훨씬 클 것.
	if count > 5 {
		t.Fatalf("orphan segments leaked into replay: count=%d (want ≤5)", count)
	}
}

func TestConcurrentAppendAndCompact(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MaxSegmentSize = 100
	w, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })

	const n = 200
	var wg sync.WaitGroup
	wg.Add(2)

	// Writer
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			if err := w.Append(&Entry{Op: OpPut, Index: int64(i), CreatedAt: TimeStamp(i), Data: []byte{byte(i % 256)}}); err != nil {
				t.Errorf("Append[%d]: %v", i, err)
				return
			}
		}
	}()

	// Compactor — 짧은 간격으로 반복 시도.
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if _, err := w.Compact(2, keyOfRaw); err != nil {
				t.Errorf("Compact: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()
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
