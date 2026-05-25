package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPropose_NotLeaderReturnsError(t *testing.T) {
	// follower 노드에 Propose → ErrNotLeader, log에 entry 추가 없음.
	lg := newFakeLog()
	n := newRaftTestNode(t, []PeerInfo{{ID: "node-1"}, {ID: "node-2"}}, nil, lg)
	n.currentTerm = 1
	// role 기본값 = RoleFollower

	idx, err := n.Propose(context.Background(), []byte("x"))
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
	if idx != 0 {
		t.Fatalf("expected index=0 on rejection, got %d", idx)
	}
	if lg.LastIndex() != 0 {
		t.Fatalf("log should remain empty, lastIndex=%d", lg.LastIndex())
	}
}

func TestPropose_SingleNodeCommitsAndApplies(t *testing.T) {
	// 1노드 cluster — quorum=1. Propose 즉시 commit+apply 후 (index, nil) 반환.
	lg := newFakeLog()
	sm := &spySM{}
	n := newRaftTestNodeWithSM(t, []PeerInfo{{ID: "node-1"}}, nil, lg, sm)
	n.currentTerm = 1
	setLeader(t, n, "node-1") // 1노드라도 self를 명시 — leader invariant: nextIndex/matchIndex에 self 포함

	idx, err := n.Propose(context.Background(), []byte("hello"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if idx != 1 {
		t.Fatalf("expected index=1, got %d", idx)
	}
	if n.commitIndex != 1 || n.lastApplied != 1 {
		t.Fatalf("expected commit/apply=1/1, got %d/%d", n.commitIndex, n.lastApplied)
	}
	if len(sm.applied) != 1 || sm.applied[0].Type != EntryNormal || string(sm.applied[0].Data) != "hello" {
		t.Fatalf("expected sm.applied=[{Normal, hello}], got %+v", sm.applied)
	}
}

func TestPropose_StepDownNotifiesPending(t *testing.T) {
	// leader가 pending 상태에서 step-down → 등록된 outcome 채널에 ErrNotLeader가 떨어진다.
	// enqueueProposalLocked를 직접 호출해 "pending이 등록됐다"는 시점을 결정적으로 잡는다 —
	// Propose 동기 호출을 goroutine으로 돌리면 select 진입 시점을 sleep으로 추정해야 한다.
	lg := newFakeLog()
	if err := lg.Append([]Entry{{Term: 1, Index: 1, Type: EntryNoop}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	tx := &fakeTransport{} // 응답 zero — broadcast가 step-down 유발 안 함
	n := newRaftTestNode(t, []PeerInfo{{ID: "node-1"}, {ID: "node-2"}, {ID: "node-3"}}, tx, lg)
	n.currentTerm = 1
	setLeader(t, n, "node-2", "node-3")

	ch, _, err := n.enqueueProposalLocked([]byte("x"))
	if err != nil {
		t.Fatalf("enqueueProposalLocked: %v", err)
	}

	n.mu.Lock()
	if err := n.becomeFollower(2, ""); err != nil {
		t.Fatalf("becomeFollower: %v", err)
	}
	n.mu.Unlock()

	select {
	case out := <-ch:
		if !errors.Is(out.err, ErrNotLeader) {
			t.Fatalf("expected ErrNotLeader on step-down, got %v", out.err)
		}
	case <-time.After(time.Second):
		t.Fatalf("step-down did not notify pending outcome channel")
	}
}

func TestPropose_ContextCancelReturnsImmediately(t *testing.T) {
	// ctx 만료 시 caller가 즉시 풀려나고 ctx.Err 반환. 등록된 pending도 제거.
	lg := newFakeLog()
	if err := lg.Append([]Entry{{Term: 1, Index: 1, Type: EntryNoop}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	tx := &fakeTransport{} // zero reply — commit 안 됨, propose는 대기
	n := newRaftTestNode(t, []PeerInfo{{ID: "node-1"}, {ID: "node-2"}, {ID: "node-3"}}, tx, lg)
	n.currentTerm = 1
	setLeader(t, n, "node-2", "node-3")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	idx, err := n.Propose(ctx, []byte("x"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
	if idx == 0 {
		t.Fatalf("expected index > 0 (entry was appended), got 0")
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.pendingProposals[idx]; ok {
		t.Fatalf("pending entry not cleaned up after ctx cancel")
	}
}
