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
	n.resetElectionTimeout() // heartbeat·grant 수신을 election cycle 시작 신호로 인식

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

// becomeLeader는 노드를 leader로 전환하고 nextIndex/matchIndex를 초기화한 뒤
// 즉시 첫 heartbeat를 broadcast한다 — 다음 tick까지 기다리면 그 사이 follower의
// election timeout이 닿아 분열 가능. candidate 상태에서 quorum vote를 모은 직후에만
// 호출되어야 한다 (호출자가 보장). term/votedFor는 변하지 않으므로 hardstate persist는
// 불필요.
func (n *Node) becomeLeader() {
	n.role = RoleLeader
	n.leaderID = n.cfg.ID
	lastIndex := n.log.LastIndex()
	n.nextIndex = make(map[NodeID]uint64, len(n.peers))
	n.matchIndex = make(map[NodeID]uint64, len(n.peers))
	for id := range n.peers {
		n.nextIndex[id] = lastIndex + 1
		n.matchIndex[id] = 0
	}
	n.heartbeatElapsedTicks = 0
	n.broadcastHeartbeatLocked()
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
