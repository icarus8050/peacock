package transport_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"peacock/node"
	"peacock/raft"
	"peacock/transport"
)

// 통합 테스트: bufconn 위에 3노드 raft cluster를 in-process로 띄워 election 수렴, Propose,
// quorum commit, 모든 노드 apply 카운트 일치를 진짜 gRPC 호출로 검증한다.

// countingSM은 적용한 Normal entry 수를 집계한다 — apply 일치 검증용. snapshot은 그
// 카운터를 8바이트로 직렬화/복원한다(InstallSnapshot catch-up 검증에 충분).
type countingSM struct {
	mu     sync.Mutex
	normal int
}

func (s *countingSM) Apply(e raft.Entry) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.Type == raft.EntryNormal {
		s.normal++
	}
	return nil, nil
}

func (s *countingSM) Snapshot() (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(s.normal))
	return io.NopCloser(bytes.NewReader(b[:])), nil
}

func (s *countingSM) Restore(r io.Reader) error {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.normal = int(binary.LittleEndian.Uint64(b[:]))
	return nil
}

func (s *countingSM) NormalCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.normal
}

func TestGRPCCluster_ElectionAndPropose(t *testing.T) {
	const n = 3
	cluster := newBufconnCluster(t, n)
	// cleanup은 newBufconnCluster 안에서 t.Cleanup으로 등록 — 부분 setup 실패에도 적용됨.

	cluster.waitForLeader(t, 5*time.Second)

	// race 환경에서 leader가 step down/재선출하는 짧은 transient를 retry로 흡수 —
	// 정상 시나리오는 첫 시도에 성공, election이 늦으면 다음 leader로 재시도.
	cluster.waitFor(t, 5*time.Second, "Propose committed by some leader", func() bool {
		leader := cluster.currentLeader()
		if leader == nil {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		_, err := leader.Raft().Propose(ctx, []byte("hello"))
		return err == nil
	})

	// 모든 노드에서 Normal apply 카운트가 1로 일치할 때까지 대기.
	cluster.waitFor(t, 3*time.Second, "all nodes apply Normal=1", func() bool {
		for _, sm := range cluster.sms {
			if sm.NormalCount() != 1 {
				return false
			}
		}
		return true
	})
}

// recordingHandler는 InstallSnapshot으로 받은 meta와 재조립된 data를 보존하는
// RPCHandler — gRPC client-streaming(청크 분할) ↔ server(재조립) 왕복을 검증한다.
type recordingHandler struct {
	mu        sync.Mutex
	snapArgs  raft.InstallSnapshotArgs
	snapData  []byte
	replyTerm uint64
}

func (h *recordingHandler) HandleRequestVote(context.Context, raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	return raft.RequestVoteReply{}, nil
}

func (h *recordingHandler) HandleAppendEntries(context.Context, raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	return raft.AppendEntriesReply{}, nil
}

func (h *recordingHandler) HandleInstallSnapshot(_ context.Context, args raft.InstallSnapshotArgs) (raft.InstallSnapshotReply, error) {
	data, err := io.ReadAll(args.Data)
	if err != nil {
		return raft.InstallSnapshotReply{}, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.snapArgs = args
	h.snapData = data
	return raft.InstallSnapshotReply{Term: h.replyTerm}, nil
}

func TestGRPCInstallSnapshot_StreamsMetaAndData(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	rec := &recordingHandler{replyTerm: 9}
	srv := transport.NewServer(lis, rec)
	go func() { _ = srv.Serve() }()
	t.Cleanup(srv.Stop)

	dialer := func(_ context.Context, _ string) (*grpc.ClientConn, error) {
		return grpc.NewClient(
			"passthrough:///bufconn",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(c context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(c)
			}),
		)
	}
	tr := transport.NewGRPCTransport(dialer, 2*time.Second)
	tr.UpdatePeer("node-2", "node-2")
	t.Cleanup(tr.Close)

	// chunk size(64KB)를 넘겨 다중 데이터 청크 경로를 강제.
	payload := bytes.Repeat([]byte("abcd"), 60*1024) // 240KB
	args := raft.InstallSnapshotArgs{
		Term:              3,
		LeaderID:          "node-1",
		LastIncludedIndex: 7,
		LastIncludedTerm:  2,
		Data:              bytes.NewReader(payload),
	}
	reply, err := tr.SendInstallSnapshot(context.Background(), "node-2", args)
	if err != nil {
		t.Fatalf("SendInstallSnapshot: %v", err)
	}
	if reply.Term != 9 {
		t.Fatalf("reply term: got %d, want 9", reply.Term)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.snapArgs.Term != 3 || rec.snapArgs.LeaderID != "node-1" ||
		rec.snapArgs.LastIncludedIndex != 7 || rec.snapArgs.LastIncludedTerm != 2 {
		t.Fatalf("meta mismatch: %+v", rec.snapArgs)
	}
	if !bytes.Equal(rec.snapData, payload) {
		t.Fatalf("data mismatch: got %d bytes, want %d", len(rec.snapData), len(payload))
	}
}

func TestGRPCInstallSnapshot_EmptySnapshot(t *testing.T) {
	// 0바이트 snapshot: meta만 보내고 data 청크 없음 → 서버가 빈 reader로 dispatch.
	lis := bufconn.Listen(1 << 20)
	rec := &recordingHandler{replyTerm: 2}
	srv := transport.NewServer(lis, rec)
	go func() { _ = srv.Serve() }()
	t.Cleanup(srv.Stop)

	dialer := func(_ context.Context, _ string) (*grpc.ClientConn, error) {
		return grpc.NewClient(
			"passthrough:///bufconn",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(c context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(c)
			}),
		)
	}
	tr := transport.NewGRPCTransport(dialer, 2*time.Second)
	tr.UpdatePeer("node-2", "node-2")
	t.Cleanup(tr.Close)

	args := raft.InstallSnapshotArgs{
		Term:              2,
		LeaderID:          "node-1",
		LastIncludedIndex: 4,
		LastIncludedTerm:  1,
		Data:              bytes.NewReader(nil),
	}
	if _, err := tr.SendInstallSnapshot(context.Background(), "node-2", args); err != nil {
		t.Fatalf("SendInstallSnapshot: %v", err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.snapData) != 0 {
		t.Fatalf("expected empty data, got %d bytes", len(rec.snapData))
	}
	if rec.snapArgs.LastIncludedIndex != 4 {
		t.Fatalf("meta mismatch: %+v", rec.snapArgs)
	}
}

// 통합 테스트: 3노드 중 2개만 띄워 quorum으로 다수 entry를 commit·snapshot·압축한 뒤,
// 뒤처진 3번째 노드를 띄우면 leader가 압축 경계 아래의 entry를 못 보내 InstallSnapshot으로
// 통째 catch-up시키는지를 진짜 gRPC + 디스크로 검증한다.
func TestGRPCCluster_InstallSnapshotCatchUp(t *testing.T) {
	const size = 3
	peers := make([]raft.PeerInfo, 0, size)
	for i := 1; i <= size; i++ {
		peers = append(peers, raft.PeerInfo{ID: raft.NodeID(fmt.Sprintf("node-%d", i)), Addr: fmt.Sprintf("node-%d", i)})
	}

	// started: 아직 안 뜬 peer로의 dial은 즉시 실패시킨다 — 죽은 peer 대기로 leader의
	// broadcast(mu 점유)가 막혀 election churn이 나는 것을 피한다.
	var startedMu sync.Mutex
	started := make(map[raft.NodeID]*bufconn.Listener)
	dialer := func(_ context.Context, addr string) (*grpc.ClientConn, error) {
		startedMu.Lock()
		lis, ok := started[raft.NodeID(addr)]
		startedMu.Unlock()
		if !ok {
			return nil, fmt.Errorf("peer %q not started", addr)
		}
		return grpc.NewClient(
			"passthrough:///bufconn",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(c context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(c)
			}),
		)
	}

	nodes := make([]*node.Node, size)
	sms := make([]*countingSM, size)
	t.Cleanup(func() {
		for _, n := range nodes {
			if n != nil {
				_ = n.Stop()
			}
		}
	})

	start := func(i int) {
		id := raft.NodeID(fmt.Sprintf("node-%d", i))
		lis := bufconn.Listen(1 << 20)
		sm := &countingSM{}
		nd, err := node.New(node.Options{
			ID:       id,
			RaftAddr: string(id),
			RaftDir:  t.TempDir(),
			Peers:    peers,
			SM:       sm,
			RaftConfig: raft.Config{
				TickInterval:       5 * time.Millisecond,
				HeartbeatInterval:  25 * time.Millisecond,
				ElectionTimeoutMin: 100 * time.Millisecond,
				ElectionTimeoutMax: 200 * time.Millisecond,
				SnapshotThreshold:  8,
			},
			Dialer:         dialer,
			RequestTimeout: 200 * time.Millisecond,
			Listener:       lis,
		})
		if err != nil {
			t.Fatalf("node.New %s: %v", id, err)
		}
		nd.Start()
		nodes[i-1] = nd
		sms[i-1] = sm
		startedMu.Lock()
		started[id] = lis
		startedMu.Unlock()
	}

	leaderOf := func(upto int) *node.Node {
		for _, n := range nodes[:upto] {
			if n != nil && n.Raft().Role() == raft.RoleLeader {
				return n
			}
		}
		return nil
	}
	waitFor := func(timeout time.Duration, what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("timeout waiting for: %s", what)
	}

	// 2노드만 시작 — quorum 2/3.
	start(1)
	start(2)
	waitFor(5*time.Second, "leader among node-1/2", func() bool { return leaderOf(2) != nil })

	// threshold(8)를 크게 넘게 propose → snapshot 발동 + 로그 압축.
	const proposeN = 25
	committed := 0
	waitFor(15*time.Second, "25 entries committed", func() bool {
		for committed < proposeN {
			leader := leaderOf(2)
			if leader == nil {
				return false
			}
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, err := leader.Raft().Propose(ctx, []byte{byte(committed)})
			cancel()
			if err != nil {
				return false
			}
			committed++
		}
		return true
	})
	waitFor(5*time.Second, "node-1/2 apply all", func() bool {
		return sms[0].NormalCount() == proposeN && sms[1].NormalCount() == proposeN
	})

	// 뒤처진 node-3 시작 — 빈 로그라 leader의 압축 경계보다 아래라서, leader는 사라진
	// entry 대신 snapshot을 통째로 보낸다. 압축으로 옛 entry가 없으므로 같은 카운트에
	// 도달했다면 InstallSnapshot 경로로 따라잡은 것.
	start(3)
	waitFor(10*time.Second, "node-3 catches up to 25 via snapshot", func() bool {
		return sms[2].NormalCount() == proposeN
	})
}

// 아래 헬퍼는 테스트 전용 cluster harness.

type bufconnCluster struct {
	nodes     []*node.Node
	sms       []*countingSM
	listeners map[raft.NodeID]*bufconn.Listener
}

func newBufconnCluster(t *testing.T, size int) *bufconnCluster {
	t.Helper()

	listeners := make(map[raft.NodeID]*bufconn.Listener, size)
	peers := make([]raft.PeerInfo, 0, size)
	for i := 1; i <= size; i++ {
		id := raft.NodeID(fmt.Sprintf("node-%d", i))
		listeners[id] = bufconn.Listen(1 << 20)
		peers = append(peers, raft.PeerInfo{ID: id, Addr: string(id)})
	}

	dialer := func(_ context.Context, addr string) (*grpc.ClientConn, error) {
		lis, ok := listeners[raft.NodeID(addr)]
		if !ok {
			return nil, fmt.Errorf("bufconn dialer: unknown peer %q", addr)
		}
		return grpc.NewClient(
			"passthrough:///bufconn",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(c context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(c)
			}),
		)
	}

	c := &bufconnCluster{
		listeners: listeners,
		sms:       make([]*countingSM, 0, size),
		nodes:     make([]*node.Node, 0, size),
	}
	// 부분 setup 실패에도 이미 Start된 노드가 leak되지 않도록 즉시 등록.
	t.Cleanup(c.stop)

	for i := 1; i <= size; i++ {
		id := raft.NodeID(fmt.Sprintf("node-%d", i))
		sm := &countingSM{}
		opts := node.Options{
			ID:       id,
			RaftAddr: string(id),
			RaftDir:  t.TempDir(),
			Peers:    peers,
			SM:       sm,
			RaftConfig: raft.Config{
				TickInterval:       5 * time.Millisecond,
				HeartbeatInterval:  25 * time.Millisecond,
				ElectionTimeoutMin: 100 * time.Millisecond,
				ElectionTimeoutMax: 200 * time.Millisecond,
			},
			Dialer:         dialer,
			RequestTimeout: 500 * time.Millisecond,
			Listener:       listeners[id],
		}
		nd, err := node.New(opts)
		if err != nil {
			t.Fatalf("node.New %s: %v", id, err)
		}
		nd.Start()
		c.nodes = append(c.nodes, nd)
		c.sms = append(c.sms, sm)
	}
	return c
}

func (c *bufconnCluster) stop() {
	for _, n := range c.nodes {
		_ = n.Stop()
	}
}

func (c *bufconnCluster) waitForLeader(t *testing.T, timeout time.Duration) *node.Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if leader := c.currentLeader(); leader != nil {
			return leader
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %v", timeout)
	return nil
}

func (c *bufconnCluster) currentLeader() *node.Node {
	for _, n := range c.nodes {
		if n.Raft().Role() == raft.RoleLeader {
			return n
		}
	}
	return nil
}

func (c *bufconnCluster) waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", what)
}
