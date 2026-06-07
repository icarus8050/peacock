package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"peacock/raft"
	"peacock/raft/pb"
)

// Server는 한 raft 노드의 gRPC 진입점. 들어온 RPC(RequestVote/AppendEntries/
// InstallSnapshot)를 raft.RPCHandler로 dispatch한다.
type Server struct {
	pb.UnimplementedRaftServer
	handler raft.RPCHandler
	gs      *grpc.Server
	lis     net.Listener
}

// NewServer는 listener와 raft handler로 gRPC 서버를 구성한다. 호출자가 lis 수명주기 책임 —
// Stop 시 gs.GracefulStop이 lis를 닫는다.
func NewServer(lis net.Listener, handler raft.RPCHandler, opts ...grpc.ServerOption) *Server {
	gs := grpc.NewServer(opts...)
	s := &Server{
		handler: handler,
		gs:      gs,
		lis:     lis,
	}
	pb.RegisterRaftServer(gs, s)
	return s
}

// Serve는 들어오는 연결을 받기 시작한다 — blocking. 보통 별도 goroutine에서 호출.
func (s *Server) Serve() error {
	if err := s.gs.Serve(s.lis); err != nil {
		return fmt.Errorf("transport: server serve: %w", err)
	}
	return nil
}

// Stop은 graceful shutdown으로 진행 중인 RPC 완료까지 기다린 뒤 리스너를 닫는다.
func (s *Server) Stop() {
	s.gs.GracefulStop()
}

// RequestVote는 인입 gRPC RPC를 raft.RPCHandler로 dispatch한다.
func (s *Server) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	reply, err := s.handler.HandleRequestVote(ctx, requestVoteFromPb(req))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "RequestVote: %v", err)
	}
	return requestVoteReplyToPb(reply), nil
}

// AppendEntries는 인입 gRPC RPC를 raft.RPCHandler로 dispatch한다.
func (s *Server) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	reply, err := s.handler.HandleAppendEntries(ctx, appendEntriesFromPb(req))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "AppendEntries: %v", err)
	}
	return appendEntriesReplyToPb(reply), nil
}

// InstallSnapshot은 client-streaming RPC를 받아 meta + data 청크를 모은 뒤 raft.RPCHandler로
// dispatch한다. M2는 data를 메모리에 모아 reader로 넘긴다(작은 snapshot 가정 — disk로
// 스트리밍하는 최적화는 후순위). 첫 청크는 meta여야 한다.
func (s *Server) InstallSnapshot(stream pb.Raft_InstallSnapshotServer) error {
	meta, data, err := recvSnapshotStream(stream)
	if err != nil {
		return err
	}
	reply, err := s.handler.HandleInstallSnapshot(stream.Context(), installSnapshotArgsFromPb(meta, data))
	if err != nil {
		return status.Errorf(codes.Internal, "InstallSnapshot: %v", err)
	}
	return stream.SendAndClose(installSnapshotReplyToPb(reply))
}

// recvSnapshotStream은 스트림에서 meta 청크와 data 청크들을 모아 반환한다.
func recvSnapshotStream(stream pb.Raft_InstallSnapshotServer) (*pb.SnapshotMeta, *bytes.Buffer, error) {
	var meta *pb.SnapshotMeta
	var buf bytes.Buffer
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, status.Errorf(codes.Internal, "InstallSnapshot recv: %v", err)
		}
		switch p := chunk.GetPayload().(type) {
		case *pb.InstallSnapshotChunk_Meta:
			meta = p.Meta
		case *pb.InstallSnapshotChunk_Data:
			buf.Write(p.Data)
		}
	}
	if meta == nil {
		return nil, nil, status.Error(codes.InvalidArgument, "InstallSnapshot: missing meta chunk")
	}
	return meta, &buf, nil
}
