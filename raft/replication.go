package raft

import (
	"context"
	"fmt"
)

// broadcastHeartbeatLocked는 leader가 다른 모든 peer에 빈 AppendEntries를 동기로
// 송신한다. follower의 election timeout을 리셋시켜 leader 안정을 유지하는 것이 목적.
// reply.Term이 더 크면 즉시 follower로 step down. self는 루프에서 제외.
//
// 자기 mu를 잡은 채 송신 — election과 같은 패턴, 노드별 mu 분리로 데드락 없음.
//
// context.Background()는 in-memory transport용 임시 — gRPC transport(M1-F) 도입 시
// heartbeat interval 기반 WithTimeout으로 교체해야 한 죽은 peer가 leader를 멈추지 않는다.
func (n *Node) broadcastHeartbeatLocked() {
	args := n.buildHeartbeatArgs()
	for id := range n.peers {
		if id == n.cfg.ID {
			continue
		}
		reply, err := n.transport.SendAppendEntries(context.Background(), id, args)
		if err != nil {
			continue
		}
		if reply.Term > n.currentTerm {
			_ = n.becomeFollower(reply.Term, "") // persist 실패는 임시 silent — logger 도입 자리
			return
		}
		// reply.Success는 Phase 2(nextIndex 깎기)에서 의미. 지금은 무시.
	}
}

// buildHeartbeatArgs는 빈 AppendEntries 인자를 만든다. Entries는 nil, PrevLogIndex/Term은
// 자기 로그 끝 — heartbeat는 새 entry를 보내지 않으므로 prev는 항상 follower와 일치한다는
// 가정(Phase 2에서 nextIndex 기반으로 재구성).
func (n *Node) buildHeartbeatArgs() AppendEntriesArgs {
	return AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: n.log.LastIndex(),
		PrevLogTerm:  n.log.LastTerm(),
		Entries:      nil,
	}
}

// HandleAppendEntries는 인입 AppendEntries RPC를 처리한다(논문 fig.2 — heartbeat
// 한정 부분 구현). Phase 1에선 entries가 비어 있다는 가정으로 prev log 일치 검사와
// entries append, leaderCommit 반영을 생략한다. 본 구현은 Phase 2 자리.
//
//   - args.Term < currentTerm: reject, 자기 term 반환.
//   - args.Term >= currentTerm: becomeFollower로 term/leader 갱신 + election timeout 리셋
//     (becomeFollower 안에서). heartbeat는 success=true로 응답.
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
	reply.Success = true
	return reply, nil
}
