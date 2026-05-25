package raft

import "fmt"

// 모든 role 전이 메서드는 run goroutine에서만 호출된다 — 락 불필요. term/votedFor가
// 바뀌면 디스크에 즉시 영속화한 뒤에야 RPC 응답·entry append가 외부에 보여야 하므로
// 호출자는 persist 결과를 받아야 한다 (논문 fig.2 invariant).

// becomeFollower는 노드를 follower로 전환한다. term이 currentTerm보다 크면 갱신
// (그리고 votedFor 초기화). leader는 알려진 leader ID(없으면 빈 문자열).
func (n *Node) becomeFollower(term uint64, leader NodeID) error {
	if term < n.currentTerm {
		return fmt.Errorf("raft: becomeFollower: term=%d < currentTerm=%d", term, n.currentTerm)
	}

	termChanged := term > n.currentTerm
	if termChanged {
		n.currentTerm = term
		n.votedFor = ""
	}
	n.role = RoleFollower
	n.leaderID = leader
	n.nextIndex = nil
	n.matchIndex = nil
	n.notifyPendingProposalsLocked(ErrNotLeader) // step-down 시 대기 중인 Propose 호출자 풀어준다
	n.resetElectionTimeout()                     // heartbeat·grant 수신을 election cycle 시작 신호로 인식

	if termChanged {
		return n.persistHardState()
	}
	return nil
}

// becomeCandidate는 노드를 candidate로 전환하고 term을 +1, 자기 자신에게 투표한다.
// election timeout에 의해 트리거된다.
func (n *Node) becomeCandidate() error {
	n.currentTerm++
	n.votedFor = n.cfg.ID
	n.role = RoleCandidate
	n.leaderID = ""
	n.nextIndex = nil
	n.matchIndex = nil
	return n.persistHardState()
}

// becomeLeader는 노드를 leader로 전환하고, noop entry를 append한 뒤
// nextIndex/matchIndex를 초기화하고 즉시 broadcast한다. 다음 tick까지 기다리면 그 사이
// follower의 election timeout이 닿아 분열 가능 — 즉시 broadcast로 회피. candidate
// 상태에서 quorum vote를 모은 직후에만 호출되어야 한다 (호출자가 보장).
//
// noop entry는 leader가 됐다는 사실 자체를 log에 박는다 — 논문 권장(이전 leader의
// 미commit entry를 자기 term에서 다시 quorum 받아 commit). term/votedFor는 변하지
// 않으므로 hardstate persist는 불필요. log append 실패는 disk-failure-is-fatal 정책으로
// fatal — 호출자(gatherVotes)가 에러를 받을 수 없는 비동기 경로다.
func (n *Node) becomeLeader() {
	n.role = RoleLeader
	n.leaderID = n.cfg.ID

	// nextIndex는 noop append *전*의 lastIndex+1로 잡아야 첫 broadcast의 prev가
	// follower의 lastIndex와 일치한다(happy path). noop append 후의 lastIndex로
	// 잡으면 prev가 노op 자체를 가리키게 되어 follower가 모르는 자리 → reject.
	preNoopLast := n.log.LastIndex()
	if err := n.log.Append([]Entry{{
		Term:  n.currentTerm,
		Index: preNoopLast + 1,
		Type:  EntryNoop,
	}}); err != nil {
		n.fatal(fmt.Errorf("raft: becomeLeader: append noop: %w", err))
	}
	n.nextIndex = make(map[NodeID]uint64, len(n.peers))
	n.matchIndex = make(map[NodeID]uint64, len(n.peers))
	for id := range n.peers {
		n.nextIndex[id] = preNoopLast + 1
		n.matchIndex[id] = 0
	}
	n.heartbeatElapsedTicks = 0
	n.broadcastAppendEntriesLocked()
	// 1노드 cluster: noop entry가 자기 자신만으로 quorum 만족 — 즉시 commit/apply 트리거.
	// 다중 노드면 quorumMatchIndex가 아직 부족해 no-op (첫 success 응답에서 진전).
	n.tryCommitAndApplyLocked()
}

// tryCommitAndApplyLocked는 commit 진전을 시도하고 가능한 apply까지 한 번에 처리하는 응집
// 헬퍼. leader가 자기 자신만으로 quorum을 만족시킬 수 있는 자리(becomeLeader의 noop,
// Propose의 새 entry)에서 호출 — broadcast 응답을 기다리지 않고 즉시 진행한다. 다중 노드
// 정상 경로에서는 maybeAdvanceCommit이 아직 quorum 부족으로 no-op이고, broadcast 응답
// 처리 자리에서 다시 호출돼 진전한다.
func (n *Node) tryCommitAndApplyLocked() {
	n.maybeAdvanceCommitLocked()
	n.applyCommittedLocked()
}

// persistHardState는 현재 currentTerm/votedFor를 디스크에 영속화한다. role 전이
// 메서드들이 term/votedFor 갱신 후 호출.
//
// 호출자의 실패 처리 정책 (의도된 비대칭):
//   - RPC handler 경로(HandleRequestVote 등): 에러를 호출자에게 반환 — 응답 의무가 있고
//     persist 실패는 invariant 깨짐이라 RPC 실패로 보고해야 한다.
//   - 비동기 timeout 경로(startElectionLocked 등): silent drop — 응답 의무가 없고
//     다음 election timeout에서 자연 재시도된다.
//
// logger 도입 시 후자도 최소한 로그는 남길 자리.
func (n *Node) persistHardState() error {
	return SaveHardState(n.cfg.Dir, HardState{
		Term:     n.currentTerm,
		VotedFor: n.votedFor,
	})
}
