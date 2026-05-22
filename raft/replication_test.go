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
	n.becomeLeader() // 첫 heartbeat 1회 — non-self 2개 노드에 송신 = 2
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

func TestBecomeLeader_AppendsNoopEntry(t *testing.T) {
	// becomeLeader는 자기 term의 noop entry를 log에 박는다 — 이후 quorum AppendEntries로
	// 이전 leader의 미commit entry까지 commit 가능해진다(논문 권장).
	lg := newFakeLog()
	tx := &fakeTransport{} // 응답 zero, broadcast 도중 stepdown 없음
	n := newRaftTestNode(t, []PeerInfo{{ID: "node-1"}}, tx, lg)
	n.currentTerm = 3
	n.role = RoleCandidate

	n.becomeLeader()

	if lg.LastIndex() != 1 {
		t.Fatalf("expected log to have noop entry at index 1, lastIndex=%d", lg.LastIndex())
	}
	entries, err := lg.Entries(1, 2, 0)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if len(entries) != 1 || entries[0].Type != EntryNoop || entries[0].Term != 3 {
		t.Fatalf("expected noop entry term=3, got %+v", entries)
	}
}

func TestBuildAppendEntriesArgs_FreshFollowerGetsAllEntries(t *testing.T) {
	// nextIndex=1인 follower는 prevLogIndex=0(log 처음 sentinel) + 모든 entries를 받는다.
	lg := newFakeLog()
	if err := lg.Append([]Entry{
		{Term: 1, Index: 1, Type: EntryNoop},
		{Term: 1, Index: 2, Type: EntryNormal, Data: []byte("x")},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	n := newRaftTestNode(t, []PeerInfo{{ID: "node-1"}, {ID: "node-2"}}, nil, lg)
	n.currentTerm = 1
	n.nextIndex = map[NodeID]uint64{"node-2": 1}

	args, err := n.buildAppendEntriesArgs("node-2")
	if err != nil {
		t.Fatalf("buildAppendEntriesArgs: %v", err)
	}
	if args.PrevLogIndex != 0 || args.PrevLogTerm != 0 {
		t.Fatalf("expected prev=(0,0) for fresh follower, got (%d,%d)",
			args.PrevLogIndex, args.PrevLogTerm)
	}
	if len(args.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(args.Entries))
	}
}

func TestBuildAppendEntriesArgs_CaughtUpFollowerGetsEmpty(t *testing.T) {
	// follower가 leader log의 끝까지 따라잡았으면(nextIndex == lastIndex+1) entries=nil
	// (heartbeat 케이스), prev는 leader log의 끝.
	lg := newFakeLog()
	if err := lg.Append([]Entry{
		{Term: 1, Index: 1, Type: EntryNoop},
		{Term: 2, Index: 2, Type: EntryNormal},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	n := newRaftTestNode(t, []PeerInfo{{ID: "node-1"}, {ID: "node-2"}}, nil, lg)
	n.currentTerm = 2
	n.nextIndex = map[NodeID]uint64{"node-2": 3}

	args, err := n.buildAppendEntriesArgs("node-2")
	if err != nil {
		t.Fatalf("buildAppendEntriesArgs: %v", err)
	}
	if args.PrevLogIndex != 2 || args.PrevLogTerm != 2 {
		t.Fatalf("expected prev=(2,2), got (%d,%d)", args.PrevLogIndex, args.PrevLogTerm)
	}
	if len(args.Entries) != 0 {
		t.Fatalf("expected empty entries for caught-up follower, got %d", len(args.Entries))
	}
}

func TestHandleAppendEntries_AcceptsAtEmptyLog(t *testing.T) {
	// 빈 log에 prev=(0,0) + 첫 entry는 일치 분기 — 항상 success + append.
	lg := newFakeLog()
	n := newRaftTestNode(t, nil, nil, lg)
	n.currentTerm = 1

	reply, err := n.HandleAppendEntries(context.Background(), AppendEntriesArgs{
		Term: 1, LeaderID: "node-L",
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []Entry{{Term: 1, Index: 1, Type: EntryNoop}},
	})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if !reply.Success {
		t.Fatalf("expected success on empty log + prev=(0,0)")
	}
	if lg.LastIndex() != 1 {
		t.Fatalf("entry should be appended, lastIndex=%d", lg.LastIndex())
	}
}

func TestHandleAppendEntries_RejectsShortLog(t *testing.T) {
	// follower log이 leader의 prevLogIndex보다 짧으면 일치 검사 실패 — reject.
	lg := newFakeLog() // 빈 log (lastIndex=0)
	n := newRaftTestNode(t, nil, nil, lg)
	n.currentTerm = 1

	reply, err := n.HandleAppendEntries(context.Background(), AppendEntriesArgs{
		Term: 1, LeaderID: "node-L",
		PrevLogIndex: 5, PrevLogTerm: 1, // follower엔 index 5가 없음
		Entries: []Entry{{Term: 1, Index: 6}},
	})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if reply.Success {
		t.Fatalf("expected reject — follower log too short")
	}
	if lg.LastIndex() != 0 {
		t.Fatalf("nothing should be appended on prev mismatch, lastIndex=%d", lg.LastIndex())
	}
}

func TestHandleAppendEntries_RejectsTermMismatchAtPrev(t *testing.T) {
	// prevLogIndex 위치는 있지만 term이 다르면 reject.
	lg := newFakeLog()
	if err := lg.Append([]Entry{{Term: 1, Index: 1, Type: EntryNoop}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	n := newRaftTestNode(t, nil, nil, lg)
	n.currentTerm = 2

	reply, err := n.HandleAppendEntries(context.Background(), AppendEntriesArgs{
		Term: 2, LeaderID: "node-L",
		PrevLogIndex: 1, PrevLogTerm: 99, // 실제 term=1과 다름
		Entries: []Entry{{Term: 2, Index: 2}},
	})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if reply.Success {
		t.Fatalf("expected reject on prev term mismatch")
	}
}

func TestSendAppendEntries_SuccessAdvancesMatchAndNextIndex(t *testing.T) {
	// 성공 응답 시 leader가 matchIndex/nextIndex를 진전.
	lg := newFakeLog()
	if err := lg.Append([]Entry{
		{Term: 1, Index: 1, Type: EntryNoop},
		{Term: 1, Index: 2, Type: EntryNormal},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	tx := &fakeTransport{
		appendReply: func(_ NodeID, _ AppendEntriesArgs) (AppendEntriesReply, error) {
			return AppendEntriesReply{Term: 1, Success: true}, nil
		},
	}
	n := newRaftTestNode(t, []PeerInfo{{ID: "node-1"}, {ID: "node-2"}}, tx, lg)
	n.currentTerm = 1
	n.role = RoleLeader
	n.nextIndex = map[NodeID]uint64{"node-2": 1}
	n.matchIndex = map[NodeID]uint64{"node-2": 0}

	n.sendAppendEntriesToLocked("node-2")

	if n.matchIndex["node-2"] != 2 {
		t.Fatalf("expected matchIndex=2, got %d", n.matchIndex["node-2"])
	}
	if n.nextIndex["node-2"] != 3 {
		t.Fatalf("expected nextIndex=3, got %d", n.nextIndex["node-2"])
	}
}

func TestSendAppendEntries_FailureDoesNotAdvance(t *testing.T) {
	// 실패 응답(prev 불일치)은 Phase 2b의 conflict resolution 자리 — 지금은 silent skip,
	// nextIndex/matchIndex 변화 없음.
	lg := newFakeLog()
	if err := lg.Append([]Entry{{Term: 1, Index: 1, Type: EntryNoop}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	tx := &fakeTransport{
		appendReply: func(_ NodeID, _ AppendEntriesArgs) (AppendEntriesReply, error) {
			return AppendEntriesReply{Term: 1, Success: false}, nil
		},
	}
	n := newRaftTestNode(t, []PeerInfo{{ID: "node-1"}, {ID: "node-2"}}, tx, lg)
	n.currentTerm = 1
	n.role = RoleLeader
	n.nextIndex = map[NodeID]uint64{"node-2": 1}
	n.matchIndex = map[NodeID]uint64{"node-2": 0}

	n.sendAppendEntriesToLocked("node-2")

	if n.matchIndex["node-2"] != 0 || n.nextIndex["node-2"] != 1 {
		t.Fatalf("expected no advance on failure, got match=%d next=%d",
			n.matchIndex["node-2"], n.nextIndex["node-2"])
	}
}

func TestHeartbeatReply_HigherTermStepsDown(t *testing.T) {
	// peer가 더 큰 term을 응답하면 leader가 즉시 follower로 step down.
	// becomeLeader 안의 broadcast가 자연 트리거 — 별도 호출 안 함.
	tx := &fakeTransport{
		appendReply: func(_ NodeID, _ AppendEntriesArgs) (AppendEntriesReply, error) {
			return AppendEntriesReply{Term: 99, Success: false}, nil
		},
	}
	n := newRaftTestNode(t, []PeerInfo{
		{ID: "node-1"}, {ID: "node-2"},
	}, tx, nil)
	n.currentTerm = 1
	n.role = RoleCandidate
	n.becomeLeader()

	if n.role != RoleFollower {
		t.Fatalf("expected stepdown to follower, got %v", n.role)
	}
	if n.currentTerm != 99 {
		t.Fatalf("expected term=99, got %d", n.currentTerm)
	}
}
