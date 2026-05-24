package raft

import (
	"fmt"
	"sort"
)

// maybeAdvanceCommitLocked는 leader가 자기 matchIndex 분포에서 quorum 만족 index를 찾아
// commitIndex를 거기까지 끌어올린다. Figure 8 안전성 가드 — 자기 currentTerm의 entry가
// 그 자리에 있어야 advance. 이전 term entry는 quorum이어도 단독 commit 안 됨.
//
// 자기 자신은 항상 matchIndex에 포함된다고 가정 — leader는 자기 log.LastIndex()를 자기
// matchIndex로 본다. quorum size = floor(N/2)+1, N은 voter 수.
//
// 호출자가 mu를 잡고 있어야 하며, leader 상태에서만 의미.
func (n *Node) maybeAdvanceCommitLocked() {
	if n.role != RoleLeader {
		return
	}
	candidate := n.quorumMatchIndex()
	if candidate <= n.commitIndex {
		return
	}
	term, err := n.log.Term(candidate)
	if err != nil {
		n.fatal(fmt.Errorf("raft: maybeAdvanceCommit: term(%d): %w", candidate, err))
	}
	if term != n.currentTerm {
		return // Figure 8: 이전 term entry는 단독 commit 안 됨.
	}
	n.commitIndex = candidate
}

// quorumMatchIndex는 leader의 matchIndex 분포에서 quorum 자리(과반)의 index를 반환한다.
// 자기 자신은 log.LastIndex()로 잡는다 — leader는 자기 log에 항상 동기.
//
// 호출자가 leader 상태(역할이 RoleLeader)를 보장한 뒤 호출한다 — leader라면 n.peers에는
// 항상 자기 자신이 포함되어 있으므로 sorted 슬라이스는 최소 1개.
func (n *Node) quorumMatchIndex() uint64 {
	sorted := make([]uint64, 0, len(n.peers))
	for id := range n.peers {
		if id == n.cfg.ID {
			sorted = append(sorted, n.log.LastIndex())
			continue
		}
		sorted = append(sorted, n.matchIndex[id])
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] > sorted[j] })
	// quorum size = floor(N/2)+1 → 정렬 후 (quorum-1)번째가 quorum 보장 최소값.
	quorumIdx := len(sorted) / 2
	return sorted[quorumIdx]
}
