// peacockd-demo는 정적 peers config로 raft 노드 한 개를 띄우는 M1 데모 바이너리다.
// 같은 바이너리를 N번 띄워(각자 다른 --id/--raft-addr/--dir, 같은 --peers) cluster를
// 만든다. state machine은 byte를 그대로 받아 카운트만 올리는 mock — KV 통합은 M4.
//
// 사용 예 (3노드):
//
//	./peacockd-demo --id=node-1 --raft-addr=127.0.0.1:4001 --http-addr=127.0.0.1:5001 \
//	    --dir=./raft-data/node-1 \
//	    --peers=node-1=127.0.0.1:4001,node-2=127.0.0.1:4002,node-3=127.0.0.1:4003
//
// leader로 propose:
//
//	curl -X POST --data-binary 'hello' http://127.0.0.1:5001/propose
//	curl http://127.0.0.1:5001/status
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"peacock/node"
	"peacock/raft"
)

func main() {
	opts := parseFlags()

	nd, sm, err := bootNode(opts)
	if err != nil {
		log.Fatalf("peacockd-demo: %v", err)
	}
	nd.Start()
	log.Printf("node %s started on %s (dir=%s)", opts.id, opts.raftAddr, opts.dir)

	stopRoleLog := make(chan struct{})
	go watchRole(nd, stopRoleLog)

	httpSrv := startProposeAPI(opts.httpAddr, nd, sm)
	log.Printf("propose API on http://%s/{propose,status}", opts.httpAddr)

	waitForSignal()
	log.Printf("shutdown signal received")

	close(stopRoleLog)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := httpSrv.Shutdown(ctx); err != nil {
		log.Printf("peacockd-demo: http shutdown: %v", err)
	}
	cancel()
	if err := nd.Stop(); err != nil {
		log.Printf("peacockd-demo: node.Stop: %v", err)
	}
}

type cliOptions struct {
	id                string
	raftAddr          string
	httpAddr          string
	dir               string
	peers             []raft.PeerInfo
	snapshotThreshold uint64
}

func parseFlags() cliOptions {
	var (
		id        = flag.String("id", "", "node id (e.g. node-1)")
		raftAddr  = flag.String("raft-addr", "", "gRPC listen address (e.g. 127.0.0.1:4001)")
		httpAddr  = flag.String("http-addr", "", "HTTP listen address for propose/status API (e.g. 127.0.0.1:5001)")
		dir       = flag.String("dir", "", "directory for raft log and hardstate")
		peersFlag = flag.String("peers", "", "comma-separated id=addr list (self 포함)")
		snapThr   = flag.Uint64("snapshot-threshold", 0, "snapshot 후 미적용 entry 임계 (0=비활성)")
	)
	flag.Parse()

	if *id == "" || *raftAddr == "" || *httpAddr == "" || *dir == "" || *peersFlag == "" {
		fmt.Fprintln(os.Stderr, "flags --id, --raft-addr, --http-addr, --dir, --peers are required")
		flag.Usage()
		os.Exit(2)
	}
	peers, err := parsePeers(*peersFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid --peers: %v\n", err)
		os.Exit(2)
	}
	return cliOptions{
		id:                *id,
		raftAddr:          *raftAddr,
		httpAddr:          *httpAddr,
		dir:               *dir,
		peers:             peers,
		snapshotThreshold: *snapThr,
	}
}

// parsePeers는 "id1=addr1,id2=addr2" 형태를 파싱한다.
func parsePeers(s string) ([]raft.PeerInfo, error) {
	parts := strings.Split(s, ",")
	out := make([]raft.PeerInfo, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		eq := strings.IndexByte(p, '=')
		if eq <= 0 || eq == len(p)-1 {
			return nil, fmt.Errorf("peer %q: want id=addr", p)
		}
		out = append(out, raft.PeerInfo{
			ID:   raft.NodeID(p[:eq]),
			Addr: p[eq+1:],
		})
	}
	if len(out) == 0 {
		return nil, errors.New("empty list")
	}
	return out, nil
}

func bootNode(opts cliOptions) (*node.Node, *demoSM, error) {
	sm := &demoSM{}
	nd, err := node.New(node.Options{
		ID:         raft.NodeID(opts.id),
		RaftAddr:   opts.raftAddr,
		RaftDir:    opts.dir,
		Peers:      opts.peers,
		SM:         sm,
		RaftConfig: raft.Config{SnapshotThreshold: opts.snapshotThreshold},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("node.New: %w", err)
	}
	return nd, sm, nil
}

// demoSM은 commit된 Normal entry 개수만 집계하는 mock state machine.
// noop entry는 무시 — 운영 의미가 없는 leader bookkeeping.
type demoSM struct {
	normalApplied atomic.Int64
}

func (s *demoSM) Apply(e raft.Entry) (any, error) {
	if e.Type == raft.EntryNormal {
		s.normalApplied.Add(1)
		log.Printf("apply: index=%d term=%d size=%d", e.Index, e.Term, len(e.Data))
	}
	return nil, nil
}

// Snapshot/Restore는 카운터를 8바이트로 직렬화/복원한다 — demoSM의 전체 상태가
// normalApplied 하나뿐이라 충분.
func (s *demoSM) Snapshot() (io.ReadCloser, error) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(s.normalApplied.Load()))
	return io.NopCloser(bytes.NewReader(buf[:])), nil
}
func (s *demoSM) Restore(r io.Reader) error {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return fmt.Errorf("demo restore: %w", err)
	}
	s.normalApplied.Store(int64(binary.LittleEndian.Uint64(buf[:])))
	return nil
}

func startProposeAPI(addr string, nd *node.Node, sm *demoSM) *http.Server {
	srv := &http.Server{
		Addr:    addr,
		Handler: newProposeMux(nd, sm),
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("peacockd-demo: http: %v", err)
		}
	}()
	return srv
}

func newProposeMux(nd *node.Node, sm *demoSM) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/propose", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		idx, err := nd.Raft().Propose(ctx, body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, "ok index=%d\n", idx)
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		fmt.Fprintf(w, "role=%s normalApplied=%d\n",
			roleName(nd.Raft().Role()), sm.normalApplied.Load())
	})
	return mux
}

// watchRole은 role 전이를 stderr 로그로 출력해 데모 시연을 쉽게 한다 —
// 운영용 hook이 아니라 데모 전용 polling이라 200ms 주기로 충분.
func watchRole(nd *node.Node, stop <-chan struct{}) {
	prev := nd.Raft().Role()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			cur := nd.Raft().Role()
			if cur != prev {
				log.Printf("role: %s -> %s", roleName(prev), roleName(cur))
				prev = cur
			}
		}
	}
}

func waitForSignal() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}

func roleName(r raft.Role) string {
	switch r {
	case raft.RoleFollower:
		return "follower"
	case raft.RoleCandidate:
		return "candidate"
	case raft.RoleLeader:
		return "leader"
	default:
		return "unknown"
	}
}
