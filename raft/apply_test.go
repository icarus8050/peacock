package raft

import (
	"context"
	"errors"
	"io"
	"testing"
)

// spySM은 Apply가 받은 entries를 순서대로 기록한다 — apply loop 검증용.
// errOnIndex > 0이면 그 index의 Apply가 에러 반환 (fatal 경로 검증용 — 직접 호출 시).
type spySM struct {
	applied    []Entry
	errOnIndex uint64
}

func (s *spySM) Apply(e Entry) (any, error) {
	s.applied = append(s.applied, e)
	if s.errOnIndex != 0 && e.Index == s.errOnIndex {
		return nil, errors.New("spy: apply failed")
	}
	return nil, nil
}

func (s *spySM) Snapshot() (io.ReadCloser, error) { return nil, errors.New("not implemented") }
func (s *spySM) Restore(io.Reader) error          { return errors.New("not implemented") }

func newRaftTestNodeWithSM(t *testing.T, peers []PeerInfo, tx Transport, lg Log, sm StateMachine) *Node {
	t.Helper()
	n := newRaftTestNode(t, peers, tx, lg)
	n.sm = sm
	return n
}

func TestApplyCommitted_AppliesAllNormalAndNoopInOrder(t *testing.T) {
	// commitIndex=3, lastApplied=0 → entries[1..3] 모두 Apply 호출. Normal/Noop 모두 호출되며
	// Noop은 결과/에러 무시(spy는 그래도 기록). entry 동일성(Index/Type/Data)까지 박는다 —
	// 단순 length+Index 검증은 Type/Data 뒤섞임을 못 잡는다.
	lg := newFakeLog()
	seedEntries := []Entry{
		{Term: 1, Index: 1, Type: EntryNoop},
		{Term: 1, Index: 2, Type: EntryNormal, Data: []byte("a")},
		{Term: 1, Index: 3, Type: EntryNormal, Data: []byte("b")},
	}
	if err := lg.Append(seedEntries); err != nil {
		t.Fatalf("seed: %v", err)
	}
	sm := &spySM{}
	n := newRaftTestNodeWithSM(t, nil, nil, lg, sm)
	n.commitIndex = 3

	n.applyCommittedLocked()

	if n.lastApplied != 3 {
		t.Fatalf("expected lastApplied=3, got %d", n.lastApplied)
	}
	if len(sm.applied) != 3 {
		t.Fatalf("expected 3 applied entries, got %d", len(sm.applied))
	}
	for i, want := range seedEntries {
		got := sm.applied[i]
		if got.Index != want.Index || got.Type != want.Type || string(got.Data) != string(want.Data) {
			t.Fatalf("applied[%d] = %+v, want %+v", i, got, want)
		}
	}
}

func TestApplyCommitted_IdempotentWhenCommitNotAdvanced(t *testing.T) {
	// 두 번째 호출은 lastApplied == commitIndex라 no-op.
	lg := newFakeLog()
	if err := lg.Append([]Entry{{Term: 1, Index: 1, Type: EntryNormal}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	sm := &spySM{}
	n := newRaftTestNodeWithSM(t, nil, nil, lg, sm)
	n.commitIndex = 1

	n.applyCommittedLocked()
	n.applyCommittedLocked()

	if len(sm.applied) != 1 {
		t.Fatalf("expected idempotent apply (1 call), got %d", len(sm.applied))
	}
}

func TestHandleAppendEntries_AdvancesCommitFromLeader(t *testing.T) {
	// follower가 새 entries와 함께 LeaderCommit을 받으면 min(LeaderCommit, lastNew)로
	// commitIndex 진전, apply도 그 자리까지 실행.
	lg := newFakeLog()
	sm := &spySM{}
	n := newRaftTestNodeWithSM(t, nil, nil, lg, sm)

	_, err := n.HandleAppendEntries(context.Background(), AppendEntriesArgs{
		Term: 1, LeaderID: "leader",
		PrevLogIndex: 0, PrevLogTerm: 0,
		LeaderCommit: 2,
		Entries: []Entry{
			{Term: 1, Index: 1, Type: EntryNoop},
			{Term: 1, Index: 2, Type: EntryNormal, Data: []byte("x")},
		},
	})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if n.commitIndex != 2 {
		t.Fatalf("expected commitIndex=2, got %d", n.commitIndex)
	}
	if n.lastApplied != 2 || len(sm.applied) != 2 {
		t.Fatalf("expected 2 applied, got lastApplied=%d applied=%d", n.lastApplied, len(sm.applied))
	}
}

func TestHandleAppendEntries_CommitCappedByLastNewEntry(t *testing.T) {
	// LeaderCommit이 새 batch보다 크면 lastNewEntry로 cap — leader가 commit 안 한 자리까지
	// follower가 단독으로 commit하지 않는다.
	lg := newFakeLog()
	if err := lg.Append([]Entry{
		{Term: 1, Index: 1, Type: EntryNoop},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	sm := &spySM{}
	n := newRaftTestNodeWithSM(t, nil, nil, lg, sm)

	_, err := n.HandleAppendEntries(context.Background(), AppendEntriesArgs{
		Term: 1, LeaderID: "leader",
		PrevLogIndex: 1, PrevLogTerm: 1,
		LeaderCommit: 99, // 과장된 값
		Entries:      []Entry{{Term: 1, Index: 2, Type: EntryNormal}},
	})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if n.commitIndex != 2 {
		t.Fatalf("expected commitIndex=2 (capped by lastNew), got %d", n.commitIndex)
	}
}

func TestHandleAppendEntries_CommitMonotonic(t *testing.T) {
	// LeaderCommit이 자기 commitIndex보다 작으면 되돌리지 않는다.
	lg := newFakeLog()
	if err := lg.Append([]Entry{
		{Term: 1, Index: 1, Type: EntryNoop},
		{Term: 1, Index: 2, Type: EntryNormal},
		{Term: 1, Index: 3, Type: EntryNormal},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	sm := &spySM{}
	n := newRaftTestNodeWithSM(t, nil, nil, lg, sm)
	n.commitIndex = 3
	n.lastApplied = 3 // 이미 모두 apply된 상태

	_, err := n.HandleAppendEntries(context.Background(), AppendEntriesArgs{
		Term: 1, LeaderID: "leader",
		PrevLogIndex: 3, PrevLogTerm: 1,
		LeaderCommit: 1, // 더 작은 값
	})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if n.commitIndex != 3 {
		t.Fatalf("commitIndex should be monotonic, expected 3, got %d", n.commitIndex)
	}
	if len(sm.applied) != 0 {
		t.Fatalf("no new apply expected, got %d calls", len(sm.applied))
	}
}
