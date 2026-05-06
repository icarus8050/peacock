package raft

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// stubLog는 빈 로그를 가장한 최소 Log 구현 — Node skeleton 테스트용.
type stubLog struct{}

func (stubLog) FirstIndex() uint64                              { return 0 }
func (stubLog) LastIndex() uint64                               { return 0 }
func (stubLog) LastTerm() uint64                                { return 0 }
func (stubLog) Term(uint64) (uint64, error)                     { return 0, nil }
func (stubLog) Entries(uint64, uint64, uint64) ([]Entry, error) { return nil, nil }
func (stubLog) Append([]Entry) error                            { return nil }
func (stubLog) TruncateAfter(uint64) error                      { return nil }
func (stubLog) TruncateBefore(uint64) error                     { return nil }
func (stubLog) Sync() error                                     { return nil }
func (stubLog) Close() error                                    { return nil }

type stubSM struct{}

func (stubSM) Apply(Entry) (any, error)         { return nil, nil }
func (stubSM) Snapshot() (io.ReadCloser, error) { return nil, errors.New("not implemented") }
func (stubSM) Restore(io.Reader) error          { return errors.New("not implemented") }

type stubTransport struct{}

func (stubTransport) SendRequestVote(context.Context, NodeID, RequestVoteArgs) (RequestVoteReply, error) {
	return RequestVoteReply{}, nil
}
func (stubTransport) SendAppendEntries(context.Context, NodeID, AppendEntriesArgs) (AppendEntriesReply, error) {
	return AppendEntriesReply{}, nil
}

func newTestNode(t *testing.T) *Node {
	t.Helper()
	dir := t.TempDir()
	cfg := Config{
		ID:                 "node-1",
		HeartbeatInterval:  20 * time.Millisecond,
		ElectionTimeoutMin: 50 * time.Millisecond,
		ElectionTimeoutMax: 100 * time.Millisecond,
		Dir:                dir,
	}
	n, err := NewNode(cfg, stubLog{}, stubSM{}, stubTransport{}, []PeerInfo{
		{ID: "node-1", Addr: ":1"},
		{ID: "node-2", Addr: ":2"},
		{ID: "node-3", Addr: ":3"},
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
}

func TestNewNode_RequiredFields(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Config)
	}{
		{"empty ID", func(c *Config) { c.ID = "" }},
		{"empty Dir", func(c *Config) { c.Dir = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{ID: "n1", Dir: t.TempDir()}
			tc.mut(&cfg)
			_, err := NewNode(cfg, stubLog{}, stubSM{}, stubTransport{}, nil)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}
}

func TestNewNode_RestoresHardState(t *testing.T) {
	dir := t.TempDir()
	if err := SaveHardState(dir, HardState{Term: 7, VotedFor: "node-2"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	cfg := Config{ID: "node-1", Dir: dir}
	n, err := NewNode(cfg, stubLog{}, stubSM{}, stubTransport{}, nil)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if n.currentTerm != 7 || n.votedFor != "node-2" {
		t.Fatalf("hardstate not restored: term=%d voted=%q", n.currentTerm, n.votedFor)
	}
	if n.role != RoleFollower {
		t.Fatalf("expected RoleFollower on boot, got %v", n.role)
	}
}

func TestNode_StartStop(t *testing.T) {
	n := newTestNode(t)
	n.Start()

	done := make(chan struct{})
	go func() {
		n.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("Stop did not return within timeout")
	}
}

func TestBecomeFollower_HigherTermPersists(t *testing.T) {
	n := newTestNode(t)
	n.currentTerm = 1
	n.votedFor = "node-1"

	if err := n.becomeFollower(5, "node-2"); err != nil {
		t.Fatalf("becomeFollower: %v", err)
	}
	if n.currentTerm != 5 {
		t.Fatalf("expected term=5, got %d", n.currentTerm)
	}
	if n.votedFor != "" {
		t.Fatalf("expected votedFor reset, got %q", n.votedFor)
	}
	if n.role != RoleFollower || n.leaderID != "node-2" {
		t.Fatalf("role/leader: %v %q", n.role, n.leaderID)
	}

	hs, err := LoadHardState(n.cfg.Dir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if hs.Term != 5 || hs.VotedFor != "" {
		t.Fatalf("hardstate not persisted: %+v", hs)
	}
}

func TestBecomeFollower_SameTermNoOpPersist(t *testing.T) {
	n := newTestNode(t)
	n.currentTerm = 3
	n.votedFor = "node-1"

	// Disk에 hardstate가 없어도 same-term 분기는 persist를 호출하지 않으므로 에러 없이 통과해야 함.
	if err := n.becomeFollower(3, "node-2"); err != nil {
		t.Fatalf("becomeFollower: %v", err)
	}
	if n.votedFor != "node-1" {
		t.Fatalf("votedFor must persist on same-term: got %q", n.votedFor)
	}
}

func TestBecomeFollower_LowerTermRejects(t *testing.T) {
	n := newTestNode(t)
	n.currentTerm = 5
	if err := n.becomeFollower(3, "node-2"); err == nil {
		t.Fatalf("expected error on lower term")
	}
}

func TestBecomeCandidate_IncrementsTermAndVotesSelf(t *testing.T) {
	n := newTestNode(t)
	n.currentTerm = 4

	if err := n.becomeCandidate(); err != nil {
		t.Fatalf("becomeCandidate: %v", err)
	}
	if n.currentTerm != 5 {
		t.Fatalf("expected term=5, got %d", n.currentTerm)
	}
	if n.votedFor != n.cfg.ID {
		t.Fatalf("expected votedFor=self, got %q", n.votedFor)
	}
	if n.role != RoleCandidate {
		t.Fatalf("expected RoleCandidate, got %v", n.role)
	}

	hs, err := LoadHardState(n.cfg.Dir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if hs.Term != 5 || hs.VotedFor != n.cfg.ID {
		t.Fatalf("hardstate not persisted: %+v", hs)
	}
}

func TestBecomeLeader_InitsNextAndMatchIndex(t *testing.T) {
	n := newTestNode(t)
	n.role = RoleCandidate
	n.becomeLeader()

	if n.role != RoleLeader || n.leaderID != n.cfg.ID {
		t.Fatalf("role/leader: %v %q", n.role, n.leaderID)
	}
	if len(n.nextIndex) != 3 || len(n.matchIndex) != 3 {
		t.Fatalf("nextIndex=%d matchIndex=%d, want 3/3", len(n.nextIndex), len(n.matchIndex))
	}
	for id, ni := range n.nextIndex {
		if ni != 1 { // stubLog.LastIndex() == 0, so nextIndex = 1
			t.Fatalf("nextIndex[%s]=%d, want 1", id, ni)
		}
	}
}
