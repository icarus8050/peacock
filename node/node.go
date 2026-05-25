package node

import (
	"fmt"
	"net"

	"peacock/raft"
	raftlog "peacock/raft/log"
	"peacock/transport"
)

// Node는 한 프로세스에서 raft 합의 + gRPC transport를 묶는 부트스트랩 facade.
// raft.Node, raft/log.Log, transport.GRPCTransport, transport.Server의 수명주기를 한 자리에서
// 관리한다. M1-F 범위는 정적 peers; 동적 멤버십 / KV 통합은 후속 마일스톤.
type Node struct {
	raft   *raft.Node
	log    *raftlog.Log
	tr     *transport.GRPCTransport
	server *transport.Server
}

// New는 Options로부터 raft 노드를 조립한다. 시작은 별도 Start 호출.
// 호출자 책임:
//   - opts.RaftDir이 비어 있지 않고 노드 전용일 것.
//   - opts.Peers에 자기 자신(opts.ID 매칭) 포함.
//   - opts.SM 주입.
//
// 리스너는 New 안에서 즉시 연다 — 주소 충돌은 부팅 실패로 노출시키는 게 안전. 자기 자신은
// transport peer 등록에서 제외(broadcast가 self skip).
func New(opts Options) (*Node, error) {
	if err := validate(opts); err != nil {
		return nil, err
	}

	lis := opts.Listener
	if lis == nil {
		var err error
		lis, err = net.Listen("tcp", opts.RaftAddr)
		if err != nil {
			return nil, fmt.Errorf("node: listen %s: %w", opts.RaftAddr, err)
		}
	}

	lg, err := raftlog.Open(raftlog.DefaultOptions(opts.RaftDir))
	if err != nil {
		_ = lis.Close()
		return nil, fmt.Errorf("node: open raft log: %w", err)
	}

	tr := transport.NewGRPCTransport(opts.Dialer, opts.RequestTimeout)
	for _, p := range opts.Peers {
		if p.ID == opts.ID {
			continue
		}
		tr.UpdatePeer(p.ID, p.Addr)
	}

	cfg := opts.RaftConfig
	cfg.ID = opts.ID
	cfg.Dir = opts.RaftDir

	rn, err := raft.NewNode(cfg, lg, opts.SM, tr, opts.Peers)
	if err != nil {
		_ = lg.Close()
		tr.Close()
		_ = lis.Close()
		return nil, fmt.Errorf("node: new raft: %w", err)
	}

	srv := transport.NewServer(lis, rn)

	return &Node{
		raft:   rn,
		log:    lg,
		tr:     tr,
		server: srv,
	}, nil
}

// Start는 gRPC 서버와 raft goroutine을 시작한다. Server.Serve는 별도 goroutine에서 blocking.
func (n *Node) Start() {
	go func() {
		// Serve 에러는 정상 Stop 시 grpc.ErrServerStopped 또는 nil — log 도입 시 분류.
		_ = n.server.Serve()
	}()
	n.raft.Start()
}

// Stop은 raft → server → transport → log 순으로 정지한다.
// raft 먼저 멈춰 outbound RPC를 끝낸 뒤 server를 graceful shutdown해 inbound를 마무리,
// transport conn과 log 파일은 마지막에 닫는다.
func (n *Node) Stop() error {
	n.raft.Stop()
	n.server.Stop()
	n.tr.Close()
	if err := n.log.Close(); err != nil {
		return fmt.Errorf("node: close log: %w", err)
	}
	return nil
}

// Raft는 내부 raft.Node를 노출 — 테스트가 Propose 등을 호출하기 위함.
func (n *Node) Raft() *raft.Node { return n.raft }

func validate(opts Options) error {
	if opts.ID == "" {
		return fmt.Errorf("node: Options.ID is empty")
	}
	if opts.RaftAddr == "" {
		return fmt.Errorf("node: Options.RaftAddr is empty")
	}
	if opts.RaftDir == "" {
		return fmt.Errorf("node: Options.RaftDir is empty")
	}
	if opts.SM == nil {
		return fmt.Errorf("node: Options.SM is nil")
	}
	if len(opts.Peers) == 0 {
		return fmt.Errorf("node: Options.Peers is empty")
	}
	if opts.RaftConfig.ID != "" && opts.RaftConfig.ID != opts.ID {
		return fmt.Errorf("node: RaftConfig.ID (%s) conflicts with Options.ID (%s) — leave RaftConfig.ID empty",
			opts.RaftConfig.ID, opts.ID)
	}
	if opts.RaftConfig.Dir != "" && opts.RaftConfig.Dir != opts.RaftDir {
		return fmt.Errorf("node: RaftConfig.Dir (%s) conflicts with Options.RaftDir (%s) — leave RaftConfig.Dir empty",
			opts.RaftConfig.Dir, opts.RaftDir)
	}
	for _, p := range opts.Peers {
		if p.ID == opts.ID {
			return nil
		}
	}
	return fmt.Errorf("node: Options.Peers must contain self (ID=%s)", opts.ID)
}
