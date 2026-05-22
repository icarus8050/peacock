package raft

import (
	"context"
	"fmt"
)

// broadcastAppendEntriesLocked는 leader가 다른 모든 peer에 AppendEntries를 동기로
// 송신한다. 각 peer는 nextIndex[id]를 기준으로 따로 entries를 구성 — heartbeat은
// "보낼 entries가 없음(빈 슬라이스)"인 특수 케이스다.
//
// 자기 mu를 잡은 채 송신 — election과 같은 패턴, 노드별 mu 분리로 데드락 없음.
//
// context.Background()는 in-memory transport용 임시 — gRPC transport(M1-F) 도입 시
// heartbeat interval 기반 WithTimeout으로 교체해야 한 죽은 peer가 leader를 멈추지 않는다.
func (n *Node) broadcastAppendEntriesLocked() {
	for id := range n.peers {
		if id == n.cfg.ID {
			continue
		}
		n.sendAppendEntriesToLocked(id)
	}
}

// sendAppendEntriesToLocked는 한 peer에 AppendEntries를 송신하고 응답을 처리한다.
// 성공 시 matchIndex/nextIndex를 진전, reply.Term이 더 크면 follower로 step down.
// reply.Success=false(prev log 불일치)는 Phase 2b의 conflict resolution 자리 —
// 지금은 silent skip, 다음 broadcast에서 같은 nextIndex로 재시도.
func (n *Node) sendAppendEntriesToLocked(id NodeID) {
	args, err := n.buildAppendEntriesArgs(id)
	if err != nil {
		return // leader 자기 log invariant 깨짐(자기 nextIndex가 자기 log 범위 밖) — 정상 흐름엔 없음, logger 도입 자리
	}
	reply, err := n.transport.SendAppendEntries(context.Background(), id, args)
	if err != nil {
		return
	}
	if reply.Term > n.currentTerm {
		_ = n.becomeFollower(reply.Term, "") // disk write 실패는 silent — 다음 heartbeat에서 재시도
		return
	}
	if !reply.Success {
		return // Phase 2b: nextIndex 깎기 + 재시도
	}
	n.matchIndex[id] = args.PrevLogIndex + uint64(len(args.Entries))
	n.nextIndex[id] = n.matchIndex[id] + 1
}

// buildAppendEntriesArgs는 nextIndex[id] 기준으로 prev/entries를 구성한다.
// prevLogIndex == 0이면 log 처음이라 prevLogTerm은 0(sentinel) — log.Term을 호출하지
// 않는다(범위 밖이라 ErrOutOfRange).
func (n *Node) buildAppendEntriesArgs(id NodeID) (AppendEntriesArgs, error) {
	nextIdx := n.nextIndex[id]
	prevLogIndex := nextIdx - 1
	var prevLogTerm uint64
	if prevLogIndex > 0 {
		t, err := n.log.Term(prevLogIndex)
		if err != nil {
			return AppendEntriesArgs{}, err
		}
		prevLogTerm = t
	}
	entries, err := n.log.Entries(nextIdx, n.log.LastIndex()+1, 0)
	if err != nil {
		return AppendEntriesArgs{}, err
	}
	return AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
	}, nil
}

// HandleAppendEntries는 인입 AppendEntries RPC를 처리한다(논문 fig.2). Phase 2a 범위:
//   - args.Term < currentTerm: reject.
//   - args.Term >= currentTerm: becomeFollower로 term/leader 갱신 + election timeout 리셋.
//   - prev log 일치 검사 후 일치하면 entries append (Phase 2a: 충돌 없음 가정 — leader가
//     nextIndex를 올바로 추적해 중복 entries 안 보낸다).
//
// Phase 2b 자리: 일치하는 prefix는 skip, 충돌하는 entries truncate-on-conflict.
// Phase 3 자리: leaderCommit 반영(commitIndex 진전).
func (n *Node) HandleAppendEntries(ctx context.Context, args AppendEntriesArgs) (AppendEntriesReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := AppendEntriesReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply, nil
	}
	if err := n.becomeFollower(args.Term, args.LeaderID); err != nil {
		return reply, fmt.Errorf("raft: handle append: become follower: %w", err)
	}
	reply.Term = n.currentTerm

	if !n.matchesPrevLog(args.PrevLogIndex, args.PrevLogTerm) {
		return reply, nil
	}
	if len(args.Entries) > 0 {
		if err := n.log.Append(args.Entries); err != nil {
			return reply, fmt.Errorf("raft: handle append: log append: %w", err)
		}
	}
	reply.Success = true
	return reply, nil
}

// matchesPrevLog는 follower의 log이 (prevLogIndex, prevLogTerm)에서 leader와 일치하는지
// 검사한다(논문 fig.2 AppendEntries receiver 2). prevLogIndex == 0은 log 처음이라
// 항상 일치.
func (n *Node) matchesPrevLog(prevLogIndex, prevLogTerm uint64) bool {
	if prevLogIndex == 0 {
		return true
	}
	if n.log.LastIndex() < prevLogIndex {
		return false
	}
	actualTerm, err := n.log.Term(prevLogIndex)
	if err != nil {
		return false
	}
	return actualTerm == prevLogTerm
}
