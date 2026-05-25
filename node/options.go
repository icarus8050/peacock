package node

import (
	"net"
	"time"

	"peacock/raft"
	"peacock/transport"
)

// Options는 한 raft 노드를 부팅하기 위한 매개변수 묶음. RaftDir 하위에 raft log + hardstate가
// 저장되며, 디렉터리는 노드별로 분리되어야 한다(같은 디렉터리를 두 노드가 공유하면 영속 상태
// 깨짐). M1-F 범위에서는 정적 peers만 — 동적 멤버십은 M3.
type Options struct {
	ID       raft.NodeID
	RaftAddr string // gRPC 리스닝 주소 (예: "127.0.0.1:4001")
	RaftDir  string // raft log + hardstate 영속 디렉터리
	Peers    []raft.PeerInfo
	SM       raft.StateMachine

	// 선택 — 비어 있으면 합리적 기본.
	RaftConfig     raft.Config      // ID/Dir는 위 필드로 덮어쓰여 무시됨
	Dialer         transport.Dialer // 기본: DefaultDialer (plaintext)
	RequestTimeout time.Duration    // 기본: 500ms
	Listener       net.Listener     // 테스트용 주입(bufconn 등); nil이면 RaftAddr로 net.Listen
}
