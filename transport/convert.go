package transport

import (
	"fmt"

	"peacock/raft"
	"peacock/raft/pb"
)

// pb ↔ raft 변환 헬퍼. 와이어 표현(pb)과 코어 타입(raft)을 transport 경계에서만 변환하고,
// raft 코어는 pb를 import하지 않는다(의존 방향 유지). EntryType 값은 raft/log와 proto에서
// 의도적으로 일치(Normal=1, ConfChange=2, Noop=3)하지만 named type이 달라 명시 캐스팅.

func entryToPb(e raft.Entry) *pb.Entry {
	return &pb.Entry{
		Term:  e.Term,
		Index: e.Index,
		Type:  pb.EntryType(e.Type),
		Data:  e.Data,
	}
}

func entryFromPb(e *pb.Entry) raft.Entry {
	return raft.Entry{
		Term:  e.GetTerm(),
		Index: e.GetIndex(),
		Type:  raft.EntryType(e.GetType()),
		Data:  e.GetData(),
	}
}

func entriesToPb(in []raft.Entry) []*pb.Entry {
	if len(in) == 0 {
		return nil
	}
	out := make([]*pb.Entry, len(in))
	for i := range in {
		out[i] = entryToPb(in[i])
	}
	return out
}

func entriesFromPb(in []*pb.Entry) []raft.Entry {
	if len(in) == 0 {
		return nil
	}
	out := make([]raft.Entry, len(in))
	for i := range in {
		out[i] = entryFromPb(in[i])
	}
	return out
}

func requestVoteToPb(a raft.RequestVoteArgs) *pb.RequestVoteRequest {
	return &pb.RequestVoteRequest{
		Term:         a.Term,
		CandidateId:  string(a.CandidateID),
		LastLogIndex: a.LastLogIndex,
		LastLogTerm:  a.LastLogTerm,
		PreVote:      a.PreVote,
	}
}

func requestVoteFromPb(r *pb.RequestVoteRequest) raft.RequestVoteArgs {
	return raft.RequestVoteArgs{
		Term:         r.GetTerm(),
		CandidateID:  raft.NodeID(r.GetCandidateId()),
		LastLogIndex: r.GetLastLogIndex(),
		LastLogTerm:  r.GetLastLogTerm(),
		PreVote:      r.GetPreVote(),
	}
}

func requestVoteReplyToPb(r raft.RequestVoteReply) *pb.RequestVoteResponse {
	return &pb.RequestVoteResponse{
		Term:        r.Term,
		VoteGranted: r.VoteGranted,
	}
}

func requestVoteReplyFromPb(r *pb.RequestVoteResponse) raft.RequestVoteReply {
	return raft.RequestVoteReply{
		Term:        r.GetTerm(),
		VoteGranted: r.GetVoteGranted(),
	}
}

func appendEntriesToPb(a raft.AppendEntriesArgs) *pb.AppendEntriesRequest {
	return &pb.AppendEntriesRequest{
		Term:         a.Term,
		LeaderId:     string(a.LeaderID),
		PrevLogIndex: a.PrevLogIndex,
		PrevLogTerm:  a.PrevLogTerm,
		LeaderCommit: a.LeaderCommit,
		Entries:      entriesToPb(a.Entries),
	}
}

func appendEntriesFromPb(r *pb.AppendEntriesRequest) raft.AppendEntriesArgs {
	return raft.AppendEntriesArgs{
		Term:         r.GetTerm(),
		LeaderID:     raft.NodeID(r.GetLeaderId()),
		PrevLogIndex: r.GetPrevLogIndex(),
		PrevLogTerm:  r.GetPrevLogTerm(),
		LeaderCommit: r.GetLeaderCommit(),
		Entries:      entriesFromPb(r.GetEntries()),
	}
}

func appendEntriesReplyToPb(r raft.AppendEntriesReply) *pb.AppendEntriesResponse {
	return &pb.AppendEntriesResponse{
		Term:          r.Term,
		Success:       r.Success,
		ConflictIndex: r.ConflictIndex,
		ConflictTerm:  r.ConflictTerm,
	}
}

func appendEntriesReplyFromPb(r *pb.AppendEntriesResponse) raft.AppendEntriesReply {
	return raft.AppendEntriesReply{
		Term:          r.GetTerm(),
		Success:       r.GetSuccess(),
		ConflictIndex: r.GetConflictIndex(),
		ConflictTerm:  r.GetConflictTerm(),
	}
}

// errf는 transport 패키지 에러를 일관 prefix로 감싼다.
func errf(format string, a ...any) error {
	return fmt.Errorf("transport: "+format, a...)
}
