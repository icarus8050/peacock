package raft

import (
	"context"
	"fmt"
)

// startElectionLocked은 election timeout 도달 시 호출되어 candidate로 전환하고
// 다른 peer에 RequestVote를 동기로 송신해 표를 모은다.
//
// 자기 mu를 잡은 채 송신하지만 노드별 mu가 분리돼 있고 HandleRequestVote 안에서는
// 외부 송신을 하지 않으므로 데드락 없음 — 이 비대칭이 핵심 invariant.
func (n *Node) startElectionLocked() {
	if err := n.becomeCandidate(); err != nil {
		return // hardstate persist 실패 — 다음 timeout에서 재시도
	}
	n.resetElectionTimeout()
	n.gatherVotesLocked(n.buildRequestVoteArgs())
}

// buildRequestVoteArgs는 현재 term/log 상태로 RequestVote 인자를 만든다.
// becomeCandidate 직후 호출되어 args.Term == currentTerm이라는 invariant를 갖는다.
func (n *Node) buildRequestVoteArgs() RequestVoteArgs {
	return RequestVoteArgs{
		Term:         n.currentTerm,
		CandidateID:  n.cfg.ID,
		LastLogIndex: n.log.LastIndex(),
		LastLogTerm:  n.log.LastTerm(),
	}
}

// voteOutcome은 한 peer에 RequestVote를 보낸 결과의 도메인 분류. 3-state는
// invariant(reply 분류는 정확히 셋 중 하나)를 타입에 박는다 — bool 두 개로 풀면
// 의미 없는 4번째 조합이 생긴다.
type voteOutcome int

const (
	voteContinue  voteOutcome = iota // RPC 에러 또는 grant 거부 — 다음 peer로
	voteGranted                      // grant — count++ 후 quorum 검사
	voteTerminate                    // stepdown 또는 다른 cycle 전이 — 즉시 종료
)

// gatherVotesLocked는 자기 표 1로 시작해 다른 peer에 RequestVote를 동기로 송신하며
// 표를 모은다. quorum 도달 시 becomeLeader, requestVoteFrom이 voteTerminate를 신호하면
// 즉시 종료(stepdown 또는 다른 cycle 전이). 루프가 끝나도 quorum 미달이면 candidate
// 유지 — 다음 timeout에서 재시도.
//
// 초기 quorum 검사는 1노드 cluster 케이스(quorum=1)를 위해 — 루프가 0회 돌아 leader가
// 안 되는 걸 방지.
func (n *Node) gatherVotesLocked(args RequestVoteArgs) {
	votes := 1
	if votes >= n.quorum() {
		n.becomeLeader()
		return
	}
	for id := range n.peers {
		if id == n.cfg.ID {
			continue
		}
		switch n.requestVoteFrom(id, args) {
		case voteTerminate:
			return
		case voteGranted:
			votes++
			if votes >= n.quorum() {
				n.becomeLeader()
				return
			}
		case voteContinue:
		}
	}
}

// requestVoteFrom은 한 peer에 RequestVote를 송신하고 reply 의미를 voteOutcome으로
// 분류한다.
//
// 자기 mu를 잡은 채 동기 송신이라 send 중 자기 state는 변경 불가능 — 비동기 응답
// 모델에서 흔히 보는 "응답 도착 시 cycle 식별자 재검사"는 redundant. 동시성 모델을
// 바꿀 때 그 검사가 필요해진다.
//
// context.Background()는 in-memory transport용 임시 — gRPC transport(M1-F) 도입 시
// election timeout 기반 WithTimeout으로 교체해야 죽은 peer에 무한 대기하지 않는다.
func (n *Node) requestVoteFrom(id NodeID, args RequestVoteArgs) voteOutcome {
	reply, err := n.transport.SendRequestVote(context.Background(), id, args)
	if err != nil {
		return voteContinue
	}
	if reply.Term > n.currentTerm {
		_ = n.becomeFollower(reply.Term, "") // persist 실패는 임시 silent — logger 도입 자리
		return voteTerminate
	}
	if reply.VoteGranted {
		return voteGranted
	}
	return voteContinue
}

// HandleRequestVote는 인입 RequestVote RPC를 처리한다(논문 fig.2):
//   - args.Term < currentTerm: reject.
//   - args.Term > currentTerm: becomeFollower로 term 갱신 후 vote 평가.
//   - votedFor가 비었거나 candidateID와 같고 후보 로그가 up-to-date면 grant.
//
// grant 시 votedFor를 영속화하고 election timeout을 리셋한다 — 막 표를 준 노드가
// 곧바로 자기 election cycle을 일으키면 같은 term에 후보가 둘 생기기 때문.
func (n *Node) HandleRequestVote(ctx context.Context, args RequestVoteArgs) (RequestVoteReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := RequestVoteReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply, nil
	}
	if args.Term > n.currentTerm {
		if err := n.becomeFollower(args.Term, ""); err != nil {
			return reply, fmt.Errorf("raft: handle vote: become follower: %w", err)
		}
		reply.Term = n.currentTerm
	}

	canVote := n.votedFor == "" || n.votedFor == args.CandidateID
	if !canVote || !n.candidateLogUpToDate(args) {
		return reply, nil
	}
	n.votedFor = args.CandidateID
	if err := n.persistHardState(); err != nil {
		return reply, fmt.Errorf("raft: handle vote: persist: %w", err)
	}
	n.resetElectionTimeout()
	reply.VoteGranted = true
	return reply, nil
}

// candidateLogUpToDate는 candidate의 (lastTerm, lastIndex)가 receiver의 로그보다
// 같거나 더 최신인지 검사한다(논문 5.4.1).
func (n *Node) candidateLogUpToDate(args RequestVoteArgs) bool {
	myTerm := n.log.LastTerm()
	if args.LastLogTerm != myTerm {
		return args.LastLogTerm > myTerm
	}
	return args.LastLogIndex >= n.log.LastIndex()
}

// quorum은 클러스터의 majority size — len(peers)/2 + 1. commit advance도 같이 쓴다.
func (n *Node) quorum() int { return len(n.peers)/2 + 1 }
