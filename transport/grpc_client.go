package transport

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"peacock/raft"
	"peacock/raft/pb"
)

// snapshotChunkSize는 InstallSnapshot data를 나눠 보내는 청크 크기. snapshot 전체를 한
// gRPC 메시지에 담지 않기 위함.
const snapshotChunkSize = 64 * 1024

// Dialer는 peer 주소로 grpc.ClientConn을 만드는 함수. 프로덕션에선 grpc.NewClient에 TLS/DNS
// 옵션을 추가, 테스트에선 bufconn 기반 dialer 주입.
type Dialer func(ctx context.Context, addr string) (*grpc.ClientConn, error)

// DefaultDialer는 plaintext gRPC 연결을 만든다. TLS는 후순위 (plan §8).
func DefaultDialer(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// GRPCTransport는 raft.Transport를 구현한다 — peer로 RPC 송신. peer별 ClientConn을 lazy
// 캐시해 매 RPC마다 dial 비용을 피한다. 종료 시 모든 conn 닫는다.
//
// RPC 시점에 RPC별 타임아웃을 컨텍스트에 박는다 — 죽은 peer가 leader의 broadcast loop를
// 멈추지 않게 하기 위함. 호출 측이 ctx를 넘기지만 그 ctx에 deadline이 없으면 RequestTimeout
// 으로 자체 cap.
type GRPCTransport struct {
	dialer         Dialer
	requestTimeout time.Duration

	mu    sync.Mutex
	peers map[raft.NodeID]string // peer ID → addr
	conns map[raft.NodeID]*grpc.ClientConn
}

// NewGRPCTransport는 빈 transport를 만든다. peer 주소는 UpdatePeer로 등록.
func NewGRPCTransport(dialer Dialer, requestTimeout time.Duration) *GRPCTransport {
	if dialer == nil {
		dialer = DefaultDialer
	}
	if requestTimeout <= 0 {
		requestTimeout = 500 * time.Millisecond
	}
	return &GRPCTransport{
		dialer:         dialer,
		requestTimeout: requestTimeout,
		peers:          make(map[raft.NodeID]string),
		conns:          make(map[raft.NodeID]*grpc.ClientConn),
	}
}

// UpdatePeer는 peer의 주소를 등록한다. 기존 주소와 다르면 conn을 무효화해 다음 RPC에 재dial.
func (t *GRPCTransport) UpdatePeer(id raft.NodeID, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if old, ok := t.peers[id]; ok && old == addr {
		return
	}
	t.peers[id] = addr
	if c, ok := t.conns[id]; ok {
		_ = c.Close()
		delete(t.conns, id)
	}
}

// RemovePeer는 peer를 제거하고 그 conn을 닫는다.
func (t *GRPCTransport) RemovePeer(id raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.peers, id)
	if c, ok := t.conns[id]; ok {
		_ = c.Close()
		delete(t.conns, id)
	}
}

// Close는 모든 conn을 닫는다.
func (t *GRPCTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, c := range t.conns {
		_ = c.Close()
		delete(t.conns, id)
	}
}

// SendRequestVote은 raft.Transport 구현.
func (t *GRPCTransport) SendRequestVote(ctx context.Context, to raft.NodeID, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	conn, err := t.connFor(ctx, to)
	if err != nil {
		return raft.RequestVoteReply{}, err
	}
	ctx, cancel := t.withTimeout(ctx)
	defer cancel()
	resp, err := pb.NewRaftClient(conn).RequestVote(ctx, requestVoteToPb(args))
	if err != nil {
		return raft.RequestVoteReply{}, errf("RequestVote to %s: %w", to, err)
	}
	return requestVoteReplyFromPb(resp), nil
}

// SendAppendEntries은 raft.Transport 구현.
func (t *GRPCTransport) SendAppendEntries(ctx context.Context, to raft.NodeID, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	conn, err := t.connFor(ctx, to)
	if err != nil {
		return raft.AppendEntriesReply{}, err
	}
	ctx, cancel := t.withTimeout(ctx)
	defer cancel()
	resp, err := pb.NewRaftClient(conn).AppendEntries(ctx, appendEntriesToPb(args))
	if err != nil {
		return raft.AppendEntriesReply{}, errf("AppendEntries to %s: %w", to, err)
	}
	return appendEntriesReplyFromPb(resp), nil
}

// SendInstallSnapshot은 raft.Transport 구현 — client-streaming으로 meta 청크를 먼저,
// 이어 data를 snapshotChunkSize 단위로 나눠 보내고 응답을 받는다.
func (t *GRPCTransport) SendInstallSnapshot(ctx context.Context, to raft.NodeID, args raft.InstallSnapshotArgs) (raft.InstallSnapshotReply, error) {
	conn, err := t.connFor(ctx, to)
	if err != nil {
		return raft.InstallSnapshotReply{}, err
	}
	ctx, cancel := t.withTimeout(ctx)
	defer cancel()
	stream, err := pb.NewRaftClient(conn).InstallSnapshot(ctx)
	if err != nil {
		return raft.InstallSnapshotReply{}, errf("InstallSnapshot to %s: %w", to, err)
	}

	metaChunk := &pb.InstallSnapshotChunk{
		Payload: &pb.InstallSnapshotChunk_Meta{Meta: installSnapshotMetaToPb(args)},
	}
	if err := stream.Send(metaChunk); err != nil {
		return raft.InstallSnapshotReply{}, errf("InstallSnapshot meta to %s: %w", to, err)
	}
	if err := streamSnapshotData(stream, args.Data); err != nil {
		return raft.InstallSnapshotReply{}, errf("InstallSnapshot data to %s: %w", to, err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return raft.InstallSnapshotReply{}, errf("InstallSnapshot close to %s: %w", to, err)
	}
	return installSnapshotReplyFromPb(resp), nil
}

// streamSnapshotData는 data를 청크로 읽어 차례로 전송한다. Send가 io.EOF면 서버가
// 스트림을 일찍 닫은 것 — 실제 에러는 CloseAndRecv가 surface하므로 여기선 nil로
// 빠져나가 호출자가 CloseAndRecv로 진단을 받게 한다(gRPC client-streaming 관용구).
func streamSnapshotData(stream pb.Raft_InstallSnapshotClient, data io.Reader) error {
	buf := make([]byte, snapshotChunkSize)
	for {
		n, rerr := data.Read(buf)
		if n > 0 {
			chunk := &pb.InstallSnapshotChunk{
				Payload: &pb.InstallSnapshotChunk_Data{Data: append([]byte(nil), buf[:n]...)},
			}
			if err := stream.Send(chunk); err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
		}
		if errors.Is(rerr, io.EOF) {
			return nil
		}
		if rerr != nil {
			return rerr
		}
	}
}

// connFor는 peer의 ClientConn을 캐시에서 가져오거나 dial한다.
func (t *GRPCTransport) connFor(ctx context.Context, id raft.NodeID) (*grpc.ClientConn, error) {
	t.mu.Lock()
	if c, ok := t.conns[id]; ok {
		t.mu.Unlock()
		return c, nil
	}
	addr, ok := t.peers[id]
	if !ok {
		t.mu.Unlock()
		return nil, errf("unknown peer %s", id)
	}
	t.mu.Unlock()

	c, err := t.dialer(ctx, addr)
	if err != nil {
		return nil, errf("dial %s (%s): %w", id, addr, err)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	// race로 다른 호출이 먼저 등록했으면 그쪽을 쓰고 우리는 닫는다.
	if existing, ok := t.conns[id]; ok {
		_ = c.Close()
		return existing, nil
	}
	t.conns[id] = c
	return c, nil
}

// withTimeout는 호출 측 ctx에 deadline이 없으면 requestTimeout으로 cap.
func (t *GRPCTransport) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, t.requestTimeout)
}
