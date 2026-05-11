package raft

import (
	"context"
	"testing"
)

// fakeHandler는 transport 테스트용 RPCHandler — 미리 정한 reply를 돌려주고 호출 횟수와
// 마지막 args를 보존해 라우팅이 args를 손상 없이 전달했는지 검증할 수 있게 한다.
type fakeHandler struct {
	voteReply      RequestVoteReply
	appendReply    AppendEntriesReply
	voteCalls      int
	appendCalls    int
	lastVoteArgs   RequestVoteArgs
	lastAppendArgs AppendEntriesArgs
}

func (h *fakeHandler) HandleRequestVote(ctx context.Context, args RequestVoteArgs) (RequestVoteReply, error) {
	h.voteCalls++
	h.lastVoteArgs = args
	return h.voteReply, nil
}

func (h *fakeHandler) HandleAppendEntries(ctx context.Context, args AppendEntriesArgs) (AppendEntriesReply, error) {
	h.appendCalls++
	h.lastAppendArgs = args
	return h.appendReply, nil
}

func TestInMem_UnregisterStopsDelivery(t *testing.T) {
	hub := newInMemHub()
	h2 := &fakeHandler{}
	hub.Register("node-2", h2)
	hub.Unregister("node-2")

	t1 := newInMemTransport("node-1", hub)
	if _, err := t1.SendRequestVote(context.Background(), "node-2", RequestVoteArgs{}); err == nil {
		t.Fatalf("expected error after unregister")
	}
	if h2.voteCalls != 0 {
		t.Fatalf("handler should not be called after unregister, got %d", h2.voteCalls)
	}
}

func TestInMem_PartitionBothBlocksBoth(t *testing.T) {
	hub := newInMemHub()
	hub.Register("node-1", &fakeHandler{})
	hub.Register("node-2", &fakeHandler{})

	t1 := newInMemTransport("node-1", hub)
	t2 := newInMemTransport("node-2", hub)
	hub.PartitionBoth("node-1", "node-2")

	if _, err := t1.SendRequestVote(context.Background(), "node-2", RequestVoteArgs{}); err == nil {
		t.Fatalf("expected block from node-1 → node-2")
	}
	if _, err := t2.SendRequestVote(context.Background(), "node-1", RequestVoteArgs{}); err == nil {
		t.Fatalf("expected block from node-2 → node-1")
	}
}

func TestInMem_HealBothRestoresBoth(t *testing.T) {
	hub := newInMemHub()
	hub.Register("node-1", &fakeHandler{})
	hub.Register("node-2", &fakeHandler{})

	t1 := newInMemTransport("node-1", hub)
	t2 := newInMemTransport("node-2", hub)
	hub.PartitionBoth("node-1", "node-2")
	hub.HealBoth("node-1", "node-2")

	if _, err := t1.SendRequestVote(context.Background(), "node-2", RequestVoteArgs{}); err != nil {
		t.Fatalf("node-1 → node-2 should pass after HealBoth: %v", err)
	}
	if _, err := t2.SendRequestVote(context.Background(), "node-1", RequestVoteArgs{}); err != nil {
		t.Fatalf("node-2 → node-1 should pass after HealBoth: %v", err)
	}
}

func TestInMem_PartitionBlocks(t *testing.T) {
	hub := newInMemHub()
	h2 := &fakeHandler{}
	hub.Register("node-2", h2)

	t1 := newInMemTransport("node-1", hub)
	hub.Partition("node-1", "node-2")

	if _, err := t1.SendRequestVote(context.Background(), "node-2", RequestVoteArgs{}); err == nil {
		t.Fatalf("expected partition error, got nil")
	}
	if h2.voteCalls != 0 {
		t.Fatalf("handler should not be called when partitioned, got %d calls", h2.voteCalls)
	}
}

func TestInMem_PartitionDirectional(t *testing.T) {
	hub := newInMemHub()
	hub.Register("node-1", &fakeHandler{})
	hub.Register("node-2", &fakeHandler{})

	t1 := newInMemTransport("node-1", hub)
	t2 := newInMemTransport("node-2", hub)

	hub.Partition("node-1", "node-2") // 한 방향만 차단

	if _, err := t1.SendRequestVote(context.Background(), "node-2", RequestVoteArgs{}); err == nil {
		t.Fatalf("expected error from blocked direction")
	}
	if _, err := t2.SendRequestVote(context.Background(), "node-1", RequestVoteArgs{}); err != nil {
		t.Fatalf("opposite direction should pass: %v", err)
	}
}

func TestInMem_HealRestoresDirection(t *testing.T) {
	hub := newInMemHub()
	hub.Register("node-2", &fakeHandler{})

	t1 := newInMemTransport("node-1", hub)
	hub.Partition("node-1", "node-2")
	hub.Heal("node-1", "node-2")

	if _, err := t1.SendRequestVote(context.Background(), "node-2", RequestVoteArgs{}); err != nil {
		t.Fatalf("expected pass after heal: %v", err)
	}
}

func TestInMem_HealOnEmptyIsNoOp(t *testing.T) {
	hub := newInMemHub()
	// partition 미존재 상태에서 Heal 호출 — panic 없이 no-op 동작해야 함.
	hub.Heal("node-1", "node-2")
	hub.Register("node-2", &fakeHandler{})

	t1 := newInMemTransport("node-1", hub)
	if _, err := t1.SendRequestVote(context.Background(), "node-2", RequestVoteArgs{}); err != nil {
		t.Fatalf("expected pass after no-op heal: %v", err)
	}
}

func TestInMem_UnregisteredPeer(t *testing.T) {
	hub := newInMemHub()
	t1 := newInMemTransport("node-1", hub)

	if _, err := t1.SendRequestVote(context.Background(), "ghost", RequestVoteArgs{}); err == nil {
		t.Fatalf("expected error for unregistered peer")
	}
}

func TestInMem_Roundtrip(t *testing.T) {
	hub := newInMemHub()
	h2 := &fakeHandler{voteReply: RequestVoteReply{Term: 7, VoteGranted: true}}
	hub.Register("node-2", h2)

	t1 := newInMemTransport("node-1", hub)
	args := RequestVoteArgs{Term: 7, CandidateID: "node-1", LastLogIndex: 4, LastLogTerm: 6}
	reply, err := t1.SendRequestVote(context.Background(), "node-2", args)
	if err != nil {
		t.Fatalf("SendRequestVote: %v", err)
	}
	if reply.Term != 7 || !reply.VoteGranted {
		t.Fatalf("expected term=7 granted=true, got %+v", reply)
	}
	if h2.voteCalls != 1 {
		t.Fatalf("expected 1 vote call, got %d", h2.voteCalls)
	}
	if h2.lastVoteArgs != args {
		t.Fatalf("args not propagated: want %+v, got %+v", args, h2.lastVoteArgs)
	}
}

func TestInMem_AppendEntriesRoundtrip(t *testing.T) {
	hub := newInMemHub()
	h2 := &fakeHandler{appendReply: AppendEntriesReply{Term: 3, Success: true}}
	hub.Register("node-2", h2)

	t1 := newInMemTransport("node-1", hub)
	args := AppendEntriesArgs{Term: 3, LeaderID: "node-1", PrevLogIndex: 2, PrevLogTerm: 3, LeaderCommit: 1}
	reply, err := t1.SendAppendEntries(context.Background(), "node-2", args)
	if err != nil {
		t.Fatalf("SendAppendEntries: %v", err)
	}
	if reply.Term != 3 || !reply.Success {
		t.Fatalf("expected term=3 success=true, got %+v", reply)
	}
	if h2.appendCalls != 1 {
		t.Fatalf("expected 1 append call, got %d", h2.appendCalls)
	}
	if h2.lastAppendArgs.Term != args.Term ||
		h2.lastAppendArgs.LeaderID != args.LeaderID ||
		h2.lastAppendArgs.PrevLogIndex != args.PrevLogIndex ||
		h2.lastAppendArgs.PrevLogTerm != args.PrevLogTerm ||
		h2.lastAppendArgs.LeaderCommit != args.LeaderCommit {
		t.Fatalf("args not propagated: want %+v, got %+v", args, h2.lastAppendArgs)
	}
}

func TestInMem_AppendEntriesPartitionBlocks(t *testing.T) {
	hub := newInMemHub()
	h2 := &fakeHandler{}
	hub.Register("node-2", h2)

	t1 := newInMemTransport("node-1", hub)
	hub.Partition("node-1", "node-2")

	if _, err := t1.SendAppendEntries(context.Background(), "node-2", AppendEntriesArgs{}); err == nil {
		t.Fatalf("expected partition error, got nil")
	}
	if h2.appendCalls != 0 {
		t.Fatalf("handler should not be called when partitioned, got %d calls", h2.appendCalls)
	}
}
