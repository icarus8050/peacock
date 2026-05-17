package raft

import "context"

// HandleAppendEntries는 인입 AppendEntries RPC를 처리한다.
//
// election.go가 cluster_test에서 RPC 라우팅을 쓰려면 Node가 RPCHandler 인터페이스를
// 만족해야 한다. 현재는 자기 term만 채워 거절하는 stub — 본 구현(prev log 일치 검사,
// entries append, leaderCommit 반영, election timeout 리셋)은 다음 단계에서.
func (n *Node) HandleAppendEntries(ctx context.Context, args AppendEntriesArgs) (AppendEntriesReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return AppendEntriesReply{Term: n.currentTerm}, nil
}
