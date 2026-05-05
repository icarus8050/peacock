package raft

import (
	raftlog "peacock/raft/log"
)

// NodeID는 클러스터 내 노드의 영속 식별자.
type NodeID string

// Role은 Raft 상태 머신의 현 역할.
type Role uint8

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
)

// PeerInfo는 클러스터 멤버의 정적 정보.
type PeerInfo struct {
	ID   NodeID
	Addr string
}

// Entry / EntryType은 raft/log 패키지의 동일 타입을 그대로 노출한다 — 두 곳에 같은
// 값 타입을 정의하면 모든 경계마다 변환 코드가 따라붙어 가독성을 해친다. raft/log는
// raft를 import하지 않으므로 의존 방향은 단방향이 유지된다.
type (
	Entry     = raftlog.Entry
	EntryType = raftlog.EntryType
)

const (
	EntryNormal     = raftlog.EntryNormal
	EntryConfChange = raftlog.EntryConfChange
	EntryNoop       = raftlog.EntryNoop
)
