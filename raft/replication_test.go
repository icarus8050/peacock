package raft

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

func TestHandleAppendEntries_StaleTermRejects(t *testing.T) {
	n := newRaftTestNode(t, nil, nil, nil)
	n.currentTerm = 5

	reply, err := n.HandleAppendEntries(context.Background(), AppendEntriesArgs{
		Term: 3, LeaderID: "node-X",
	})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if reply.Success {
		t.Fatalf("expected reject on stale term")
	}
	if reply.Term != 5 {
		t.Fatalf("reply should carry own term=5, got %d", reply.Term)
	}
}

func TestHandleAppendEntries_HigherTermBecomesFollower(t *testing.T) {
	n := newRaftTestNode(t, nil, nil, nil)
	n.currentTerm = 2
	n.role = RoleCandidate
	n.votedFor = "node-1"

	reply, err := n.HandleAppendEntries(context.Background(), AppendEntriesArgs{
		Term: 7, LeaderID: "node-Y",
	})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if !reply.Success {
		t.Fatalf("expected success on higher term heartbeat")
	}
	if n.role != RoleFollower {
		t.Fatalf("expected RoleFollower, got %v", n.role)
	}
	if n.currentTerm != 7 {
		t.Fatalf("expected term=7, got %d", n.currentTerm)
	}
	if n.leaderID != "node-Y" {
		t.Fatalf("expected leaderID=node-Y, got %q", n.leaderID)
	}
}

func TestHandleAppendEntries_ResetsElectionTimeout(t *testing.T) {
	// heartbeat 수신은 election cycle 시작을 신호 — elapsed가 0으로 리셋되어야
	// follower가 그 자리에서 candidate로 가지 않는다.
	n := newRaftTestNode(t, nil, nil, nil)
	n.electionElapsedTicks = 4

	if _, err := n.HandleAppendEntries(context.Background(), AppendEntriesArgs{
		Term: 1, LeaderID: "node-L",
	}); err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if n.electionElapsedTicks != 0 {
		t.Fatalf("expected elapsed reset to 0, got %d", n.electionElapsedTicks)
	}
}

func TestBecomeLeader_SendsImmediateHeartbeatToAllPeers(t *testing.T) {
	// becomeLeader 직후 모든 peer에 즉시 broadcast — 다음 tick까지 기다리면 그 사이
	// follower들이 election timeout으로 분열할 수 있다.
	var (
		mu       sync.Mutex
		received = make(map[NodeID]int)
	)
	tx := &fakeTransport{
		appendReply: func(to NodeID, _ AppendEntriesArgs) (AppendEntriesReply, error) {
			mu.Lock()
			received[to]++
			mu.Unlock()
			return AppendEntriesReply{Term: 1, Success: true}, nil
		},
	}
	n := newRaftTestNode(t, []PeerInfo{
		{ID: "node-1"}, {ID: "node-2"}, {ID: "node-3"},
	}, tx, nil)
	n.currentTerm = 1
	n.role = RoleCandidate

	n.becomeLeader()

	mu.Lock()
	defer mu.Unlock()
	if received["node-2"] != 1 || received["node-3"] != 1 {
		t.Fatalf("expected one heartbeat to each non-self peer, got %v", received)
	}
	if _, sentToSelf := received["node-1"]; sentToSelf {
		t.Fatalf("must not heartbeat self, got %v", received)
	}
}

func TestOnTick_LeaderSendsHeartbeatOnInterval(t *testing.T) {
	// heartbeatTicks마다 broadcast가 호출되는지 검증. cfg.HeartbeatInterval/TickInterval=2
	// (bootNode 기본값과 다른 newRaftTestNode 기본) — heartbeatTicks=2.
	var counter int64
	tx := &fakeTransport{
		appendReply: func(_ NodeID, _ AppendEntriesArgs) (AppendEntriesReply, error) {
			atomic.AddInt64(&counter, 1)
			return AppendEntriesReply{Term: 1, Success: true}, nil
		},
	}
	n := newRaftTestNode(t, []PeerInfo{
		{ID: "node-1"}, {ID: "node-2"}, {ID: "node-3"},
	}, tx, nil)
	n.currentTerm = 1
	n.role = RoleCandidate
	n.becomeLeader()         // 첫 heartbeat 1회 — non-self 2개 노드에 송신 = 2
	initial := atomic.LoadInt64(&counter)

	// heartbeatTicks 만큼 tick — 다시 broadcast.
	for i := 0; i < n.heartbeatTicks; i++ {
		n.onTickLocked()
	}
	after := atomic.LoadInt64(&counter)

	delta := after - initial
	if delta != 2 {
		t.Fatalf("expected 2 heartbeats (one to each non-self peer) after heartbeatTicks, got %d (initial=%d, after=%d)",
			delta, initial, after)
	}
}

func TestHeartbeatReply_HigherTermStepsDown(t *testing.T) {
	// peer가 더 큰 term을 응답하면 leader가 즉시 follower로 step down.
	tx := &fakeTransport{
		appendReply: func(_ NodeID, _ AppendEntriesArgs) (AppendEntriesReply, error) {
			return AppendEntriesReply{Term: 99, Success: false}, nil
		},
	}
	n := newRaftTestNode(t, []PeerInfo{
		{ID: "node-1"}, {ID: "node-2"},
	}, tx, nil)
	n.currentTerm = 1
	n.role = RoleLeader

	n.broadcastHeartbeatLocked()

	if n.role != RoleFollower {
		t.Fatalf("expected stepdown to follower, got %v", n.role)
	}
	if n.currentTerm != 99 {
		t.Fatalf("expected term=99, got %d", n.currentTerm)
	}
}
