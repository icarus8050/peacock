package raft

import "context"

// 와이어 표현(pb)과 분리된 raft 코어 RPC 메시지 타입.
// transport 어댑터(grpc 등)가 경계에서 pb ↔ 이 타입을 변환한다.

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
	PreVote      bool // M1에서는 미사용
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	LeaderCommit uint64
	Entries      []Entry
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
	// follower가 어디까지 일치하는지 알리는 최적화 힌트. 정확성에는 영향 없음.
	ConflictIndex uint64
	ConflictTerm  uint64
}

// Transport는 raft.Node가 peer로 RPC를 보낼 때 쓰는 송신 측 인터페이스.
// 구현은 in-memory(테스트용) 또는 gRPC(transport 패키지) 중 하나.
type Transport interface {
	SendRequestVote(ctx context.Context, to NodeID, args RequestVoteArgs) (RequestVoteReply, error)
	SendAppendEntries(ctx context.Context, to NodeID, args AppendEntriesArgs) (AppendEntriesReply, error)
}

// RPCHandler는 transport가 인입 RPC를 raft.Node에 dispatch할 때 호출하는 콜백.
// raft.Node가 이 인터페이스를 구현해 transport에 주입된다.
type RPCHandler interface {
	HandleRequestVote(ctx context.Context, args RequestVoteArgs) (RequestVoteReply, error)
	HandleAppendEntries(ctx context.Context, args AppendEntriesArgs) (AppendEntriesReply, error)
}
