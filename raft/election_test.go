package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeTransport은 election 단위 테스트에서 reply 시나리오를 결정적으로 통제하기 위한
// Transport 구현. nil 콜백은 zero reply를 돌려준다.
type fakeTransport struct {
	voteReply func(to NodeID, args RequestVoteArgs) (RequestVoteReply, error)
}

func (f *fakeTransport) SendRequestVote(_ context.Context, to NodeID, args RequestVoteArgs) (RequestVoteReply, error) {
	if f.voteReply == nil {
		return RequestVoteReply{}, nil
	}
	return f.voteReply(to, args)
}

func (f *fakeTransport) SendAppendEntries(_ context.Context, _ NodeID, _ AppendEntriesArgs) (AppendEntriesReply, error) {
	return AppendEntriesReply{}, nil
}

// fakeLog은 up-to-date 비교를 위해 lastIndex/lastTerm을 통제 가능한 Log 구현.
type fakeLog struct {
	stubLog
	lastIndex uint64
	lastTerm  uint64
}

func (l fakeLog) LastIndex() uint64 { return l.lastIndex }
func (l fakeLog) LastTerm() uint64  { return l.lastTerm }

func newElectionTestNode(t *testing.T, peers []PeerInfo, tx Transport, lg Log) *Node {
	t.Helper()
	cfg := Config{
		ID:                 "node-1",
		Dir:                t.TempDir(),
		TickInterval:       1 * time.Millisecond,
		HeartbeatInterval:  2 * time.Millisecond,
		ElectionTimeoutMin: 5 * time.Millisecond,
		ElectionTimeoutMax: 10 * time.Millisecond,
	}
	if lg == nil {
		lg = stubLog{}
	}
	if tx == nil {
		tx = &fakeTransport{}
	}
	n, err := NewNode(cfg, lg, stubSM{}, tx, peers)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
}

func TestStartElection_SingleNodeBecomesLeader(t *testing.T) {
	// 1노드 cluster — 자기 표만으로 quorum 달성, 즉시 leader.
	n := newElectionTestNode(t, []PeerInfo{{ID: "node-1"}}, nil, nil)
	n.startElectionLocked()
	if n.role != RoleLeader {
		t.Fatalf("expected RoleLeader, got %v", n.role)
	}
	if n.currentTerm != 1 {
		t.Fatalf("expected term=1, got %d", n.currentTerm)
	}
}

func TestStartElection_HigherTermReplyStepsDown(t *testing.T) {
	// 송신 도중 한 peer가 더 큰 term을 응답하면 즉시 follower 전이하고 election 중단.
	tx := &fakeTransport{
		voteReply: func(to NodeID, _ RequestVoteArgs) (RequestVoteReply, error) {
			return RequestVoteReply{Term: 99, VoteGranted: false}, nil
		},
	}
	n := newElectionTestNode(t, []PeerInfo{
		{ID: "node-1"}, {ID: "node-2"}, {ID: "node-3"},
	}, tx, nil)
	n.startElectionLocked()
	if n.role != RoleFollower {
		t.Fatalf("expected RoleFollower after higher term reply, got %v", n.role)
	}
	if n.currentTerm != 99 {
		t.Fatalf("expected term=99, got %d", n.currentTerm)
	}
}

func TestStartElection_TransportErrorsTreatedAsNoVote(t *testing.T) {
	// 모든 peer가 RPC 실패 — candidate에 머무르며 quorum 미달.
	tx := &fakeTransport{
		voteReply: func(to NodeID, _ RequestVoteArgs) (RequestVoteReply, error) {
			return RequestVoteReply{}, errors.New("network down")
		},
	}
	n := newElectionTestNode(t, []PeerInfo{
		{ID: "node-1"}, {ID: "node-2"}, {ID: "node-3"},
	}, tx, nil)
	n.startElectionLocked()
	if n.role != RoleCandidate {
		t.Fatalf("expected RoleCandidate, got %v", n.role)
	}
}

func TestOnTick_LeaderDoesNotRestartElection(t *testing.T) {
	// leader는 election timeout 도달 시 elapsed만 리셋하고 candidate로 가지 않는다.
	n := newElectionTestNode(t, []PeerInfo{
		{ID: "node-1"}, {ID: "node-2"}, {ID: "node-3"},
	}, nil, nil)
	n.role = RoleLeader
	n.leaderID = n.cfg.ID
	startTerm := n.currentTerm
	n.electionElapsedTicks = 0
	n.electionTimeoutTicks = 2

	n.onTickLocked()
	n.onTickLocked()

	if n.role != RoleLeader {
		t.Fatalf("expected leader to remain, got %v", n.role)
	}
	if n.currentTerm != startTerm {
		t.Fatalf("term should not change on leader timeout, got %d", n.currentTerm)
	}
	if n.electionElapsedTicks != 0 {
		t.Fatalf("expected elapsed reset, got %d", n.electionElapsedTicks)
	}
}

func TestHandleRequestVote_StaleTermRejects(t *testing.T) {
	n := newElectionTestNode(t, nil, nil, nil)
	n.currentTerm = 5

	reply, err := n.HandleRequestVote(context.Background(), RequestVoteArgs{
		Term: 3, CandidateID: "node-9",
	})
	if err != nil {
		t.Fatalf("HandleRequestVote: %v", err)
	}
	if reply.VoteGranted {
		t.Fatalf("vote should not be granted for stale term")
	}
	if reply.Term != 5 {
		t.Fatalf("reply should carry own term=5, got %d", reply.Term)
	}
}

func TestHandleRequestVote_HigherTermBecomesFollowerAndGrants(t *testing.T) {
	// 자기 term=2, 다른 candidate가 term=7로 옴 → follower로 점프 + 적격이면 grant.
	n := newElectionTestNode(t, nil, nil, nil)
	n.currentTerm = 2
	n.votedFor = "node-1"
	n.role = RoleCandidate

	reply, err := n.HandleRequestVote(context.Background(), RequestVoteArgs{
		Term: 7, CandidateID: "node-9",
	})
	if err != nil {
		t.Fatalf("HandleRequestVote: %v", err)
	}
	if !reply.VoteGranted {
		t.Fatalf("expected vote granted after term bump")
	}
	if n.currentTerm != 7 {
		t.Fatalf("expected term bumped to 7, got %d", n.currentTerm)
	}
	if n.role != RoleFollower {
		t.Fatalf("expected RoleFollower after higher term, got %v", n.role)
	}
	if n.votedFor != "node-9" {
		t.Fatalf("expected votedFor=node-9, got %q", n.votedFor)
	}
	hs, err := LoadHardState(n.cfg.Dir)
	if err != nil {
		t.Fatalf("LoadHardState: %v", err)
	}
	if hs.Term != 7 || hs.VotedFor != "node-9" {
		t.Fatalf("hardstate not persisted: %+v", hs)
	}
}

func TestHandleRequestVote_RejectsSecondCandidateInSameTerm(t *testing.T) {
	// 같은 term 안에서 이미 다른 candidate에게 표를 줬다면 두 번째는 거부.
	n := newElectionTestNode(t, nil, nil, nil)
	n.currentTerm = 4
	n.votedFor = "node-A"

	reply, err := n.HandleRequestVote(context.Background(), RequestVoteArgs{
		Term: 4, CandidateID: "node-B",
	})
	if err != nil {
		t.Fatalf("HandleRequestVote: %v", err)
	}
	if reply.VoteGranted {
		t.Fatalf("must not grant to second candidate in same term")
	}
	if n.votedFor != "node-A" {
		t.Fatalf("votedFor must stay node-A, got %q", n.votedFor)
	}
}

func TestHandleRequestVote_IdempotentGrantToSameCandidate(t *testing.T) {
	// 같은 candidate가 같은 term으로 두 번 요청해도 모두 grant.
	n := newElectionTestNode(t, nil, nil, nil)
	n.currentTerm = 4
	n.votedFor = "node-A"

	for i := 0; i < 2; i++ {
		reply, err := n.HandleRequestVote(context.Background(), RequestVoteArgs{
			Term: 4, CandidateID: "node-A",
		})
		if err != nil {
			t.Fatalf("HandleRequestVote #%d: %v", i, err)
		}
		if !reply.VoteGranted {
			t.Fatalf("call #%d: expected granted", i)
		}
	}
}

func TestHandleRequestVote_RejectsStaleLog(t *testing.T) {
	// 자기 로그가 (term=3, index=5)인데 candidate는 (term=3, index=3) — index가 짧으므로 거부.
	n := newElectionTestNode(t, nil, nil, fakeLog{lastIndex: 5, lastTerm: 3})

	reply, err := n.HandleRequestVote(context.Background(), RequestVoteArgs{
		Term: 4, CandidateID: "node-X",
		LastLogIndex: 3, LastLogTerm: 3,
	})
	if err != nil {
		t.Fatalf("HandleRequestVote: %v", err)
	}
	if reply.VoteGranted {
		t.Fatalf("stale log candidate should be rejected")
	}
	// term은 bump되었지만 vote는 거부되어야 한다.
	if n.currentTerm != 4 {
		t.Fatalf("term should still bump on higher-term reject, got %d", n.currentTerm)
	}
	if n.votedFor != "" {
		t.Fatalf("votedFor should remain unset after reject, got %q", n.votedFor)
	}
}

func TestHandleRequestVote_AcceptsEqualLog(t *testing.T) {
	// up-to-date 경계 — candidate (lastTerm, lastIndex)가 자기와 정확히 같으면 grant.
	n := newElectionTestNode(t, nil, nil, fakeLog{lastIndex: 5, lastTerm: 3})

	reply, err := n.HandleRequestVote(context.Background(), RequestVoteArgs{
		Term: 1, CandidateID: "node-X",
		LastLogIndex: 5, LastLogTerm: 3,
	})
	if err != nil {
		t.Fatalf("HandleRequestVote: %v", err)
	}
	if !reply.VoteGranted {
		t.Fatalf("equal log should grant")
	}
}

func TestHandleRequestVote_GrantResetsElectionTimeout(t *testing.T) {
	// grant 직후 elapsed가 0으로 리셋되어야 — 막 표를 준 노드가 곧장 candidate로
	// 가버리면 같은 term에 후보가 두 개 생긴다.
	n := newElectionTestNode(t, nil, nil, nil)
	n.electionElapsedTicks = 4

	if _, err := n.HandleRequestVote(context.Background(), RequestVoteArgs{
		Term: 1, CandidateID: "node-Z",
	}); err != nil {
		t.Fatalf("HandleRequestVote: %v", err)
	}
	if n.electionElapsedTicks != 0 {
		t.Fatalf("expected elapsed reset to 0 on grant, got %d", n.electionElapsedTicks)
	}
}
