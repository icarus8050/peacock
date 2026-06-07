package raft

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

// memSnap은 raft.SnapshotStore의 인메모리 테스트 더블. 한 개 snapshot만 보관(최신).
// 같은 인스턴스를 두 NewNode에 넘겨 재기동 restore를 시뮬레이션할 수 있다.
type memSnap struct {
	mu   sync.Mutex
	meta SnapshotMeta
	data []byte
	has  bool
}

func newMemSnap() *memSnap { return &memSnap{} }

func (s *memSnap) Create(meta SnapshotMeta) (SnapshotWriter, error) {
	return &memSnapWriter{store: s, meta: meta}, nil
}

func (s *memSnap) Latest() (SnapshotMeta, io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.has {
		return SnapshotMeta{}, nil, ErrNoSnapshot
	}
	return s.meta, io.NopCloser(bytes.NewReader(append([]byte(nil), s.data...))), nil
}

func (s *memSnap) LatestMeta() (SnapshotMeta, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.meta, s.has, nil
}

type memSnapWriter struct {
	store *memSnap
	meta  SnapshotMeta
	buf   bytes.Buffer
}

func (w *memSnapWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }

func (w *memSnapWriter) Commit() error {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	w.store.meta = w.meta
	w.store.data = append([]byte(nil), w.buf.Bytes()...)
	w.store.has = true
	return nil
}

func (w *memSnapWriter) Cancel() error { return nil }

// applyTo는 commitIndex를 올리고 apply 루프를 돌려 SM 적용 + snapshot 트리거를
// 동기로 수행한다 — 테스트가 commit 진전을 흉내 내는 헬퍼.
func (n *Node) applyTo(index uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.commitIndex = index
	n.applyCommittedLocked()
}

func mkRaftEntry(idx, term uint64, data []byte) Entry {
	return Entry{Index: idx, Term: term, Type: EntryNormal, Data: data}
}

// newRaftTestNodeWithDeps는 log/snap/sm을 명시 주입해 단일 노드를 만든다 — snapshot
// 트리거·restore 테스트가 세 의존성을 모두 통제해야 한다.
func newRaftTestNodeWithDeps(t *testing.T, lg Log, snap SnapshotStore, sm StateMachine) *Node {
	t.Helper()
	cfg := Config{
		ID:                 "node-1",
		Dir:                t.TempDir(),
		TickInterval:       1 * time.Millisecond,
		HeartbeatInterval:  2 * time.Millisecond,
		ElectionTimeoutMin: 5 * time.Millisecond,
		ElectionTimeoutMax: 10 * time.Millisecond,
	}
	n, err := NewNode(cfg, lg, sm, snap, &fakeTransport{}, []PeerInfo{{ID: "node-1"}})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
}

// newHubNode는 inMemHub에 연결된 노드를 만든다 — InstallSnapshot 2노드 시나리오용.
func newHubNode(t *testing.T, id NodeID, hub *inMemHub, lg Log, snap SnapshotStore, sm StateMachine) *Node {
	t.Helper()
	cfg := Config{
		ID:                 id,
		Dir:                t.TempDir(),
		TickInterval:       1 * time.Millisecond,
		HeartbeatInterval:  2 * time.Millisecond,
		ElectionTimeoutMin: 5 * time.Millisecond,
		ElectionTimeoutMax: 10 * time.Millisecond,
	}
	n, err := NewNode(cfg, lg, sm, snap, newInMemTransport(id, hub), []PeerInfo{{ID: "node-1"}, {ID: "node-2"}})
	if err != nil {
		t.Fatalf("NewNode %s: %v", id, err)
	}
	hub.Register(id, n)
	return n
}

func TestInstallSnapshot_FollowerInstalls(t *testing.T) {
	lg := newFakeLog()
	for i := uint64(1); i <= 3; i++ {
		if err := lg.Append([]Entry{mkRaftEntry(i, 1, []byte{byte(i)})}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	sm := &bufferSM{}
	n := newRaftTestNodeWithDeps(t, lg, newMemSnap(), sm)

	args := InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "node-2",
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
		Data:              bytes.NewReader([]byte("snapshot-state")),
	}
	reply, err := n.HandleInstallSnapshot(context.Background(), args)
	if err != nil {
		t.Fatalf("HandleInstallSnapshot: %v", err)
	}
	if reply.Term != 1 {
		t.Fatalf("reply term: got %d, want 1", reply.Term)
	}
	if !bytes.Equal(sm.data, []byte("snapshot-state")) {
		t.Fatalf("SM not restored: got %q", sm.data)
	}
	if got := lg.FirstIndex(); got != 6 {
		t.Fatalf("log not reset: FirstIndex got %d, want 6", got)
	}
	if n.lastApplied != 5 || n.commitIndex != 5 {
		t.Fatalf("progress: lastApplied=%d commitIndex=%d, want 5", n.lastApplied, n.commitIndex)
	}
}

func TestInstallSnapshot_IdempotentSkip(t *testing.T) {
	sm := &bufferSM{}
	n := newRaftTestNodeWithDeps(t, newFakeLog(), newMemSnap(), sm)
	n.commitIndex = 10 // 이미 더 앞서 있음

	args := InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "node-2",
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
		Data:              bytes.NewReader([]byte("stale")),
	}
	if _, err := n.HandleInstallSnapshot(context.Background(), args); err != nil {
		t.Fatalf("HandleInstallSnapshot: %v", err)
	}
	if len(sm.data) != 0 {
		t.Fatalf("stale snapshot should be skipped, SM got %q", sm.data)
	}
	if n.commitIndex != 10 {
		t.Fatalf("commitIndex should be unchanged: got %d, want 10", n.commitIndex)
	}
}

func TestInstallSnapshot_LaggingFollowerCatchesUp(t *testing.T) {
	hub := newInMemHub()

	fSM := &bufferSM{}
	follower := newHubNode(t, "node-2", hub, newFakeLog(), newMemSnap(), fSM)

	// leader: index=5 snapshot 보유, 로그는 그 경계까지 압축됨.
	lSnap := newMemSnap()
	w, err := lSnap.Create(SnapshotMeta{Index: 5, Term: 1})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := w.Write([]byte("leader-state")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	lLog := newFakeLog()
	if err := lLog.Reset(5, 1); err != nil { // 압축: FirstIndex=6, snapshotIndex=5
		t.Fatalf("reset: %v", err)
	}
	leader := newHubNode(t, "node-1", hub, lLog, lSnap, &bufferSM{})
	leader.currentTerm = 1
	setLeader(t, leader, "node-1", "node-2")
	leader.nextIndex["node-2"] = 1 // 경계(5)보다 뒤처짐 → InstallSnapshot 경로

	leader.mu.Lock()
	leader.sendAppendEntriesToLocked("node-2")
	leader.mu.Unlock()

	if !bytes.Equal(fSM.data, []byte("leader-state")) {
		t.Fatalf("follower not caught up via snapshot: SM got %q", fSM.data)
	}
	follower.mu.Lock()
	fApplied := follower.lastApplied
	follower.mu.Unlock()
	if fApplied != 5 {
		t.Fatalf("follower lastApplied: got %d, want 5", fApplied)
	}
	if leader.matchIndex["node-2"] != 5 || leader.nextIndex["node-2"] != 6 {
		t.Fatalf("leader progress for follower: match=%d next=%d, want 5/6",
			leader.matchIndex["node-2"], leader.nextIndex["node-2"])
	}
}

func TestInstallSnapshot_HigherTermReplyStepsDown(t *testing.T) {
	snap := newMemSnap()
	w, err := snap.Create(SnapshotMeta{Index: 5, Term: 1})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := w.Write([]byte("s")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	lg := newFakeLog()
	if err := lg.Reset(5, 1); err != nil {
		t.Fatalf("reset: %v", err)
	}
	n := newRaftTestNodeWithDeps(t, lg, snap, &bufferSM{})
	n.currentTerm = 1
	n.transport = &fakeTransport{
		snapReply: func(NodeID, InstallSnapshotArgs) (InstallSnapshotReply, error) {
			return InstallSnapshotReply{Term: 5}, nil // 더 큰 term
		},
	}
	setLeader(t, n, "node-1", "node-2")
	n.nextIndex["node-2"] = 1 // 경계보다 뒤처짐 → InstallSnapshot 송신

	n.mu.Lock()
	n.sendAppendEntriesToLocked("node-2")
	n.mu.Unlock()

	if n.role != RoleFollower {
		t.Fatalf("expected step down to follower, role=%v", n.role)
	}
	if n.currentTerm != 5 {
		t.Fatalf("expected currentTerm=5 after stepdown, got %d", n.currentTerm)
	}
}

func TestInstallSnapshot_EmptySnapshot(t *testing.T) {
	// 0바이트 snapshot도 정상 설치돼야 한다(meta만, data 청크 없음).
	sm := &bufferSM{}
	n := newRaftTestNodeWithDeps(t, newFakeLog(), newMemSnap(), sm)
	args := InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "node-2",
		LastIncludedIndex: 3,
		LastIncludedTerm:  1,
		Data:              bytes.NewReader(nil),
	}
	if _, err := n.HandleInstallSnapshot(context.Background(), args); err != nil {
		t.Fatalf("HandleInstallSnapshot: %v", err)
	}
	if len(sm.data) != 0 {
		t.Fatalf("empty snapshot should restore empty state, got %q", sm.data)
	}
	if n.lastApplied != 3 || n.commitIndex != 3 {
		t.Fatalf("progress: lastApplied=%d commitIndex=%d, want 3", n.lastApplied, n.commitIndex)
	}
}

func TestSnapshot_TriggersAndCompacts(t *testing.T) {
	lg := newFakeLog()
	snap := newMemSnap()
	sm := &bufferSM{}
	n := newRaftTestNodeWithDeps(t, lg, snap, sm)
	n.cfg.SnapshotThreshold = 5

	// 1..6 entry append 후 commit/apply → threshold(5) 도달로 snapshot 발동.
	for i := uint64(1); i <= 6; i++ {
		if err := lg.Append([]Entry{mkRaftEntry(i, 1, []byte{byte(i)})}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	n.applyTo(6)

	meta, ok, err := snap.LatestMeta()
	if err != nil || !ok {
		t.Fatalf("expected snapshot created, ok=%v err=%v", ok, err)
	}
	if meta.Index != 6 || meta.Term != 1 {
		t.Fatalf("snapshot meta: got {%d,%d}, want {6,1}", meta.Index, meta.Term)
	}
	// 로그가 경계까지 압축됐다 — FirstIndex가 snapshot index 다음.
	if got := lg.FirstIndex(); got != 7 {
		t.Fatalf("FirstIndex after compaction: got %d, want 7", got)
	}
	if got := lg.LastIndex(); got != 6 {
		t.Fatalf("LastIndex: got %d, want 6", got)
	}
}

func TestSnapshot_PreservesUncompactedTail(t *testing.T) {
	// commitIndex < LastIndex이면 snapshot은 lastApplied까지만 압축하고 tail은 남긴다
	// (compactLogLocked의 TruncateBefore 분기 — 운영의 주 경로).
	lg := newFakeLog()
	snap := newMemSnap()
	n := newRaftTestNodeWithDeps(t, lg, snap, &bufferSM{})
	n.cfg.SnapshotThreshold = 5

	for i := uint64(1); i <= 8; i++ {
		if err := lg.Append([]Entry{mkRaftEntry(i, 1, []byte{byte(i)})}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	n.applyTo(6) // commitIndex=6 < LastIndex=8

	meta, ok, err := snap.LatestMeta()
	if err != nil || !ok {
		t.Fatalf("expected snapshot, ok=%v err=%v", ok, err)
	}
	if meta.Index != 6 {
		t.Fatalf("snapshot index: got %d, want 6", meta.Index)
	}
	if got := lg.FirstIndex(); got != 7 {
		t.Fatalf("FirstIndex: got %d, want 7 (compacted to snapshot)", got)
	}
	if got := lg.LastIndex(); got != 8 {
		t.Fatalf("LastIndex: got %d, want 8 (tail preserved)", got)
	}
}

func TestSnapshot_RecompactsAfterCrashWindow(t *testing.T) {
	// 크래시 윈도: snapshot은 commit됐지만 로그 압축 전에 죽은 상태를 흉내 낸다 —
	// snap에는 index=4 snapshot, 로그는 1..6 미압축. 재기동 후 첫 apply에서 재압축돼
	// self-heal 되는지.
	snap := newMemSnap()
	w, err := snap.Create(SnapshotMeta{Index: 4, Term: 2})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := w.Write([]byte("state")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	lg := newFakeLog()
	for i := uint64(1); i <= 6; i++ {
		if err := lg.Append([]Entry{mkRaftEntry(i, 2, []byte{byte(i)})}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if got := lg.FirstIndex(); got != 1 {
		t.Fatalf("setup premise: log should be uncompacted (FirstIndex=1), got %d", got)
	}

	n := newRaftTestNodeWithDeps(t, lg, snap, &bufferSM{}) // restore → lastApplied=commitIndex=4
	n.cfg.SnapshotThreshold = 3
	n.applyTo(6)

	if got := lg.FirstIndex(); got != 7 {
		t.Fatalf("self-heal: FirstIndex should advance to 7 after recompaction, got %d", got)
	}
	meta, _, _ := snap.LatestMeta()
	if meta.Index != 6 {
		t.Fatalf("recompacted snapshot index: got %d, want 6", meta.Index)
	}
}

func TestSnapshot_DisabledByZeroThreshold(t *testing.T) {
	lg := newFakeLog()
	snap := newMemSnap()
	n := newRaftTestNodeWithDeps(t, lg, snap, &bufferSM{})
	// SnapshotThreshold 미설정(0) → 발동 안 함.

	for i := uint64(1); i <= 10; i++ {
		if err := lg.Append([]Entry{mkRaftEntry(i, 1, []byte{byte(i)})}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	n.applyTo(10)

	if _, ok, _ := snap.LatestMeta(); ok {
		t.Fatalf("expected no snapshot with threshold=0")
	}
	if got := lg.FirstIndex(); got != 1 {
		t.Fatalf("FirstIndex should be unchanged: got %d, want 1", got)
	}
}

func TestSnapshot_RestoreOnBoot(t *testing.T) {
	snap := newMemSnap()

	// 1차 노드: snapshot 생성.
	lg1 := newFakeLog()
	sm1 := &bufferSM{}
	n1 := newRaftTestNodeWithDeps(t, lg1, snap, sm1)
	n1.cfg.SnapshotThreshold = 3
	for i := uint64(1); i <= 4; i++ {
		if err := lg1.Append([]Entry{mkRaftEntry(i, 2, []byte{byte('a' + i)})}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	n1.applyTo(4)
	meta, ok, err := snap.LatestMeta()
	if err != nil || !ok {
		t.Fatalf("setup snapshot: ok=%v err=%v", ok, err)
	}

	// 2차 노드: 같은 snap을 주입해 부팅 → SM.Restore + lastApplied/commitIndex 복원.
	sm2 := &bufferSM{}
	n2 := newRaftTestNodeWithDeps(t, newFakeLog(), snap, sm2)

	if !bytes.Equal(sm2.data, sm1.data) {
		t.Fatalf("restored SM state mismatch: got %q, want %q", sm2.data, sm1.data)
	}
	if n2.lastApplied != meta.Index || n2.commitIndex != meta.Index {
		t.Fatalf("restore progress: lastApplied=%d commitIndex=%d, want %d",
			n2.lastApplied, n2.commitIndex, meta.Index)
	}
}
