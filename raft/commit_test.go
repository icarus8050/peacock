package raft

import "testing"

// commit_test.go의 Figure 8 시나리오는 spySM과 결합해 carry-along을 sm.Apply 호출 시퀀스로
// 검증한다 — commitIndex 값 검증만으로는 "가드가 잘못돼 prior-term이 단독 commit돼도 두 번째
// 단계에서 어차피 같은 값"이라 false positive 가능. 첫 단계에서 sm.Apply 호출 0건 + 두 번째
// 단계에서 [1,2,3] 순서로 호출되는지 spy가 잡는다.

func TestMaybeAdvanceCommit_NotLeaderNoop(t *testing.T) {
	// follower/candidate는 commit advance를 시도하지 않는다 — leader 전용.
	lg := newFakeLog()
	if err := lg.Append([]Entry{{Term: 1, Index: 1, Type: EntryNoop}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	n := newRaftTestNode(t, []PeerInfo{{ID: "node-1"}, {ID: "node-2"}}, nil, lg)
	n.currentTerm = 1
	n.role = RoleFollower

	n.maybeAdvanceCommitLocked()

	if n.commitIndex != 0 {
		t.Fatalf("expected commitIndex unchanged (follower), got %d", n.commitIndex)
	}
}

func TestMaybeAdvanceCommit_AdvancesOnQuorumOfCurrentTerm(t *testing.T) {
	// 3노드 cluster: leader log [1:T1, 2:T1] (자기는 항상 동기). peer matchIndex=2.
	// quorum index=2, term=currentTerm=1 → commitIndex=2로 진전.
	lg := newFakeLog()
	if err := lg.Append([]Entry{
		{Term: 1, Index: 1, Type: EntryNoop},
		{Term: 1, Index: 2, Type: EntryNormal},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	n := newRaftTestNode(t, []PeerInfo{
		{ID: "node-1"}, {ID: "node-2"}, {ID: "node-3"},
	}, nil, lg)
	n.currentTerm = 1
	setLeader(t, n, "node-2", "node-3")
	n.matchIndex["node-2"] = 2

	n.maybeAdvanceCommitLocked()

	if n.commitIndex != 2 {
		t.Fatalf("expected commitIndex=2 (quorum of currentTerm), got %d", n.commitIndex)
	}
}

func TestMaybeAdvanceCommit_Figure8BlocksPriorTerm(t *testing.T) {
	// leader가 currentTerm=3인데 quorum이 잡힌 자리(index=2)의 entry term이 1이면
	// commit 안 됨(Figure 8 안전성). 자기 term entry가 quorum 잡혀야 진전.
	// spySM과 결합해 carry-along을 sm.Apply 호출 시퀀스로 검증 — 첫 단계 0건, 두 번째에 [1,2,3].
	lg := newFakeLog()
	if err := lg.Append([]Entry{
		{Term: 1, Index: 1, Type: EntryNormal},
		{Term: 1, Index: 2, Type: EntryNormal}, // 이전 term — Figure 8 가드
		{Term: 3, Index: 3, Type: EntryNoop},   // 자기 term entry
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	sm := &spySM{}
	n := newRaftTestNodeWithSM(t, []PeerInfo{
		{ID: "node-1"}, {ID: "node-2"}, {ID: "node-3"},
	}, nil, lg, sm)
	n.currentTerm = 3
	setLeader(t, n, "node-2", "node-3")
	// peer 한 명만 index=2까지 따라옴 — leader log 끝은 3이지만 quorum은 2.
	n.matchIndex["node-2"] = 2
	n.matchIndex["node-3"] = 0

	n.maybeAdvanceCommitLocked()
	n.applyCommittedLocked()

	if n.commitIndex != 0 {
		t.Fatalf("Figure 8: expected commitIndex unchanged at prior-term quorum, got %d", n.commitIndex)
	}
	if len(sm.applied) != 0 {
		t.Fatalf("Figure 8: expected 0 applies at prior-term quorum, got %d", len(sm.applied))
	}

	// 이제 peer가 index=3(자기 term)까지 따라오면 진전 — 이전 term entry도 함께 commit/apply.
	n.matchIndex["node-2"] = 3
	n.maybeAdvanceCommitLocked()
	n.applyCommittedLocked()

	if n.commitIndex != 3 {
		t.Fatalf("expected commitIndex=3 once currentTerm entry has quorum, got %d", n.commitIndex)
	}
	if len(sm.applied) != 3 {
		t.Fatalf("expected 3 applies (carry-along), got %d", len(sm.applied))
	}
	for i, want := range []uint64{1, 2, 3} {
		if sm.applied[i].Index != want {
			t.Fatalf("carry-along apply order: applied[%d].Index=%d, want %d",
				i, sm.applied[i].Index, want)
		}
	}
}

func TestQuorumMatchIndex_FiveNodes(t *testing.T) {
	// 5노드: leader=log.LastIndex(), peers matchIndex 분포 → 정렬 후 quorum 자리(과반)의 값.
	// matchIndex [10(자기), 8, 6, 4, 2] 정렬 desc → 10,8,6,4,2. quorum size=3 → index 2 → 6.
	lg := newFakeLog()
	if err := lg.Append([]Entry{
		{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}, {Term: 1, Index: 4},
		{Term: 1, Index: 5}, {Term: 1, Index: 6}, {Term: 1, Index: 7}, {Term: 1, Index: 8},
		{Term: 1, Index: 9}, {Term: 1, Index: 10},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	n := newRaftTestNode(t, []PeerInfo{
		{ID: "node-1"}, {ID: "node-2"}, {ID: "node-3"}, {ID: "node-4"}, {ID: "node-5"},
	}, nil, lg)
	n.currentTerm = 1
	setLeader(t, n, "node-2", "node-3", "node-4", "node-5")
	n.matchIndex["node-2"] = 8
	n.matchIndex["node-3"] = 6
	n.matchIndex["node-4"] = 4
	n.matchIndex["node-5"] = 2

	if got := n.quorumMatchIndex(); got != 6 {
		t.Fatalf("expected quorum index=6, got %d", got)
	}
}
