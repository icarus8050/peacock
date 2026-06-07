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
// context.Background()는 의도적 — deadline cap은 transport 어댑터 책임이다
// (예: GRPCTransport.withTimeout이 deadline 없는 ctx에 requestTimeout을 박는다).
// raft 코어는 transport-agnostic하게 유지.
func (n *Node) broadcastAppendEntriesLocked() {
	for id := range n.peers {
		if id == n.cfg.ID {
			continue
		}
		n.sendAppendEntriesToLocked(id)
	}
}

// sendAppendEntriesToLocked는 한 peer에 AppendEntries를 송신하고 응답을 처리한다.
// 성공 시 matchIndex/nextIndex를 진전, reply.Term이 더 크면 follower로 step down,
// reply.Success=false면 conflict hint로 nextIndex를 backoff해 다음 broadcast에서 재시도.
func (n *Node) sendAppendEntriesToLocked(id NodeID) {
	// follower가 압축 경계보다 뒤처졌으면 leader는 그 자리 entry를 더 이상 갖고 있지
	// 않다 — AppendEntries 대신 snapshot을 통째로 보낸다.
	if n.nextIndex[id] <= n.snapshotIndexLocked() {
		n.sendInstallSnapshotToLocked(id)
		return
	}
	args, err := n.buildAppendEntriesArgs(id)
	if err != nil {
		return // leader 자기 log invariant 깨짐(자기 nextIndex가 자기 log 범위 밖) — 정상 흐름엔 없음, logger 도입 자리
	}
	reply, err := n.transport.SendAppendEntries(context.Background(), id, args)
	if err != nil {
		return
	}
	if reply.Term > n.currentTerm {
		if err := n.becomeFollower(reply.Term, ""); err != nil {
			n.fatal(fmt.Errorf("raft: stepdown on AppendEntries reply: %w", err))
		}
		return
	}
	if !reply.Success {
		n.nextIndex[id] = n.backoffNextIndex(id, reply)
		return
	}
	n.matchIndex[id] = args.PrevLogIndex + uint64(len(args.Entries))
	n.nextIndex[id] = n.matchIndex[id] + 1
	n.maybeAdvanceCommitLocked()
	n.applyCommittedLocked()
}

// backoffNextIndex는 reply.Success=false 시 conflict hint를 이용해 다음 시도의 nextIndex를
// 계산한다(논문 §5.3 + Ongaro 박사논문의 conflictIndex/Term 최적화). 한 round-trip에 term
// 단위로 점프해 단순 nextIndex-- 대비 회복 횟수를 크게 줄인다.
//   - reply.ConflictTerm == 0: follower log이 짧다 — ConflictIndex(follower lastIndex+1)로 점프.
//   - leader가 ConflictTerm을 갖고 있음: 그 term의 마지막 entry+1로 점프 — 일치 prefix를 최대한 보존.
//   - leader가 ConflictTerm을 모름: ConflictIndex로 점프해 follower의 그 term 전체를 다음 RPC에서 truncate.
//   - hint가 비어 있으면(legacy/fallback): nextIndex-1, 단 1 미만으로 못 내려간다.
func (n *Node) backoffNextIndex(id NodeID, reply AppendEntriesReply) uint64 {
	switch {
	case reply.ConflictTerm == 0 && reply.ConflictIndex > 0:
		return reply.ConflictIndex
	case reply.ConflictTerm > 0:
		if last, found := n.lastIndexOfTerm(reply.ConflictTerm); found {
			return last + 1
		}
		return reply.ConflictIndex
	}
	cur := n.nextIndex[id]
	if cur > 1 {
		return cur - 1
	}
	return 1
}

// lastIndexOfTerm은 leader log에서 term의 마지막 index를 backward scan으로 찾는다. log
// term은 단조 비감소이므로 scan 도중 t < term이면 더 내려갈 필요 없다. 효율 개선은 후순위
// (raft/log 측에 index 추가). log.Term 실패는 disk-failure-is-fatal 정책으로 fatal —
// silent swallow는 leader가 ConflictIndex로 잘못 점프해 정확성에 영향.
func (n *Node) lastIndexOfTerm(term uint64) (uint64, bool) {
	for i := n.log.LastIndex(); i >= 1; i-- {
		t, err := n.log.Term(i)
		if err != nil {
			n.fatal(fmt.Errorf("raft: lastIndexOfTerm: term(%d): %w", i, err))
		}
		if t == term {
			return i, true
		}
		if t < term {
			return 0, false
		}
	}
	return 0, false
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
		LeaderCommit: n.commitIndex,
		Entries:      entries,
	}, nil
}

// HandleAppendEntries는 인입 AppendEntries RPC를 처리한다(논문 fig.2). 흐름:
//   - args.Term < currentTerm: reject.
//   - args.Term >= currentTerm: becomeFollower로 term/leader 갱신 + election timeout 리셋.
//   - prev log 일치 검사 — 불일치면 conflict hint를 채워 leader가 nextIndex를 backoff하게.
//   - 일치하면 entries 반영 — skip prefix + truncate-on-conflict + append.
//   - LeaderCommit 반영 — min(LeaderCommit, lastNewEntry.Index)로 commitIndex 진전 후 apply.
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

	if hint := n.checkPrevLog(args.PrevLogIndex, args.PrevLogTerm); hint != nil {
		reply.ConflictIndex = hint.Index
		reply.ConflictTerm = hint.Term
		return reply, nil
	}
	if err := n.applyAppendedEntries(args.Entries); err != nil {
		return reply, fmt.Errorf("raft: handle append: %w", err)
	}
	n.advanceCommitFromLeader(args)
	n.applyCommittedLocked()
	reply.Success = true
	return reply, nil
}

// advanceCommitFromLeader는 follower가 args.LeaderCommit을 받아 자기 commitIndex를 진전시킨다
// (논문 fig.2 AppendEntries receiver 5). lastNewEntry는 이 RPC가 가져온 batch의 마지막 entry,
// 비어 있으면 prev — leader가 아직 commit 안 한 자리까지 follower가 단독 commit하지 않도록 cap.
//
// **stale tail 의도**: heartbeat(entries=nil)이고 follower가 자체 tail(stale leader의 미합의
// entries)을 갖고 있더라도, cap이 args.PrevLogIndex라 그 tail은 commit하지 않는다. tail은
// 다음 leader의 conflict resolution에서 truncate/append로 정리된다.
func (n *Node) advanceCommitFromLeader(args AppendEntriesArgs) {
	if args.LeaderCommit <= n.commitIndex {
		return
	}
	lastNewIndex := args.PrevLogIndex + uint64(len(args.Entries))
	next := args.LeaderCommit
	if lastNewIndex < next {
		next = lastNewIndex
	}
	if next > n.commitIndex {
		n.commitIndex = next
	}
}

// conflictHint는 prev log 불일치 시 leader에게 돌려주는 backoff 안내. ConflictTerm == 0은
// "follower log이 짧다"는 신호.
type conflictHint struct {
	Index uint64
	Term  uint64
}

// checkPrevLog는 follower log이 (prevLogIndex, prevLogTerm)에서 leader와 일치하는지
// 검사한다(논문 fig.2 AppendEntries receiver 2). 일치하면 nil 반환, 불일치하면 leader가
// 한 번에 점프해 일치 prefix를 찾도록 conflict hint를 만들어 반환한다.
//
//   - prevLogIndex == 0: log 처음 — 항상 일치.
//   - follower lastIndex < prevLogIndex: log이 짧음. ConflictTerm=0, ConflictIndex=lastIndex+1.
//   - term 불일치: follower의 그 term이 처음 나타나는 index를 hint.ConflictIndex로,
//     follower의 그 term을 hint.ConflictTerm으로.
//
// log.Term 실패는 disk-failure-is-fatal 정책으로 fatal — silent swallow는 follower가
// 가짜 hint를 보내 leader가 잘못된 자리로 점프하게 만든다.
func (n *Node) checkPrevLog(prevLogIndex, prevLogTerm uint64) *conflictHint {
	if prevLogIndex == 0 {
		return nil
	}
	lastIdx := n.log.LastIndex()
	if lastIdx < prevLogIndex {
		return &conflictHint{Index: lastIdx + 1, Term: 0}
	}
	actualTerm, err := n.log.Term(prevLogIndex)
	if err != nil {
		n.fatal(fmt.Errorf("raft: checkPrevLog: term(%d): %w", prevLogIndex, err))
	}
	if actualTerm == prevLogTerm {
		return nil
	}
	firstIdx := n.firstIndexOfTerm(actualTerm, prevLogIndex)
	return &conflictHint{Index: firstIdx, Term: actualTerm}
}

// firstIndexOfTerm은 follower log에서 term이 처음 나타나는 index를 upto부터 거꾸로 찾는다
// (conflict hint 계산용). log term은 단조 비감소이므로 t < term인 자리를 만나면 그 다음이
// 시작점. 끝까지 같은 term이면 1을 반환. log.Term 실패는 disk-failure-is-fatal로 fatal.
func (n *Node) firstIndexOfTerm(term, upto uint64) uint64 {
	for i := upto; i >= 1; i-- {
		t, err := n.log.Term(i)
		if err != nil {
			n.fatal(fmt.Errorf("raft: firstIndexOfTerm: term(%d): %w", i, err))
		}
		if t != term {
			return i + 1
		}
	}
	return 1
}

// applyAppendedEntries는 leader가 보낸 entries를 follower log에 반영한다. 호출자는
// prev log 일치를 보장한 상태로 들어온다. 흐름: findFirstConflict로 일치 prefix를 세고
// 첫 충돌 자리를 찾는다 → 충돌이 있으면 그 자리부터 TruncateAfter → skip 후 잔여 entries를
// Append. heartbeat(entries 비어 있음)이면 아무 일도 안 한다.
func (n *Node) applyAppendedEntries(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	skip, conflictAt := n.findFirstConflict(entries)
	if conflictAt > 0 {
		if err := n.log.TruncateAfter(conflictAt - 1); err != nil {
			return fmt.Errorf("truncate after %d: %w", conflictAt-1, err)
		}
	}
	if skip == len(entries) {
		return nil
	}
	if err := n.log.Append(entries[skip:]); err != nil {
		return fmt.Errorf("append: %w", err)
	}
	return nil
}

// findFirstConflict는 entries를 앞에서부터 follower log과 비교해 (1) 일치하는 prefix의
// 개수와 (2) 첫 충돌 자리의 index를 찾는다. 충돌 없이 entries가 follower 끝을 넘어가면
// conflictAt=0(없음 sentinel). log.Term 실패는 disk-failure-is-fatal로 fatal.
func (n *Node) findFirstConflict(entries []Entry) (skipPrefix int, conflictAt uint64) {
	lastIdx := n.log.LastIndex()
	for skipPrefix < len(entries) {
		idx := entries[skipPrefix].Index
		if idx > lastIdx {
			return skipPrefix, 0
		}
		existing, err := n.log.Term(idx)
		if err != nil {
			n.fatal(fmt.Errorf("raft: findFirstConflict: term(%d): %w", idx, err))
		}
		if existing != entries[skipPrefix].Term {
			return skipPrefix, idx
		}
		skipPrefix++
	}
	return skipPrefix, 0
}
