package raft

import (
	"bytes"
	"io"
	"testing"

	raftsnap "peacock/raft/snapshot"
)

// bufferSM은 Apply된 entry data를 이어붙여 들고, 그 바이트를 통째로 snapshot/restore하는
// 최소 mock state machine. SnapshotStore와 StateMachine의 round-trip 계약 검증용.
type bufferSM struct {
	data []byte
}

func (s *bufferSM) Apply(e Entry) (any, error) {
	s.data = append(s.data, e.Data...)
	return nil, nil
}

func (s *bufferSM) Snapshot() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.data)), nil
}

func (s *bufferSM) Restore(r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.data = b
	return nil
}

// StateMachine ↔ SnapshotStore ↔ raftsnap.Store 3자 계약: SM이 만든 스트림을 Store에
// 확정하고, 다시 읽어 빈 SM에 복원하면 상태가 일치하는가.
func TestSnapshotStoreRoundTrip(t *testing.T) {
	store, err := raftsnap.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var ss SnapshotStore = store

	src := &bufferSM{data: []byte("the quick brown fox")}
	meta := SnapshotMeta{Index: 10, Term: 2}

	rc, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	w, err := ss.Create(meta)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := io.Copy(w, rc); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Snapshot close: %v", err)
	}
	if err := w.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	gotMeta, ok, err := ss.LatestMeta()
	if err != nil || !ok {
		t.Fatalf("LatestMeta: ok=%v err=%v", ok, err)
	}
	if gotMeta != meta {
		t.Fatalf("expected meta %+v, got %+v", meta, gotMeta)
	}

	rMeta, data, err := ss.Latest()
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	dst := &bufferSM{}
	if err := dst.Restore(data); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if err := data.Close(); err != nil {
		t.Fatalf("data close: %v", err)
	}
	if rMeta != meta {
		t.Fatalf("expected restored meta %+v, got %+v", meta, rMeta)
	}
	if !bytes.Equal(dst.data, src.data) {
		t.Fatalf("restored state mismatch: got %q, want %q", dst.data, src.data)
	}
}
