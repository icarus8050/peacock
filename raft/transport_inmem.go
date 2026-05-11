package raft

import (
	"context"
	"fmt"
	"sync"
)

// inMemHub은 한 프로세스 안의 여러 in-memory transport 사이에서 RPC를 라우팅하는
// 중앙 레지스트리. 테스트 전용 — production 경로에선 쓰지 않는다.
type inMemHub struct {
	mu         sync.RWMutex
	handlers   map[NodeID]RPCHandler
	partitions map[NodeID]map[NodeID]bool // [from][to] = true → from→to RPC 차단
}

func newInMemHub() *inMemHub {
	return &inMemHub{
		handlers:   make(map[NodeID]RPCHandler),
		partitions: make(map[NodeID]map[NodeID]bool),
	}
}

// Register는 NodeID에 RPCHandler를 묶는다. 이후 to=id로 보낸 RPC는 이 handler로
// dispatch된다. 같은 id에 대한 재등록은 새 handler로 덮어쓴다.
func (h *inMemHub) Register(id NodeID, handler RPCHandler) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.handlers[id] = handler
}

// Unregister는 등록을 해제한다. 이후 to=id로 보낸 Send는 미등록 에러를 반환한다.
func (h *inMemHub) Unregister(id NodeID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.handlers, id)
}

// PartitionBoth는 a↔b 양방향을 차단하는 헬퍼.
func (h *inMemHub) PartitionBoth(a, b NodeID) {
	h.Partition(a, b)
	h.Partition(b, a)
}

// HealBoth는 a↔b 양방향 차단을 해제하는 헬퍼.
func (h *inMemHub) HealBoth(a, b NodeID) {
	h.Heal(a, b)
	h.Heal(b, a)
}

// Partition은 from→to 방향의 RPC를 차단한다(비대칭). 양방향이 필요하면 PartitionBoth.
func (h *inMemHub) Partition(from, to NodeID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.partitions[from] == nil {
		h.partitions[from] = make(map[NodeID]bool)
	}
	h.partitions[from][to] = true
}

// Heal은 Partition이 건 차단을 해제한다(같은 방향).
func (h *inMemHub) Heal(from, to NodeID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.partitions[from] != nil {
		delete(h.partitions[from], to)
	}
}

// route는 RPC를 dispatch할 handler를 찾는다. partition 상태와 등록 여부를 차례로
// 검사하고, 해석된 handler를 반환한다. lookup 시점의 상태만 본다 — 호출자가 handler를
// 호출하는 동안 partition이 바뀌어도 해당 RPC는 그대로 흐른다(in-flight 의미).
func (h *inMemHub) route(from, to NodeID) (RPCHandler, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.partitions[from] != nil && h.partitions[from][to] {
		return nil, fmt.Errorf("raft: in-mem: partitioned %s→%s", from, to)
	}
	handler, ok := h.handlers[to]
	if !ok {
		return nil, fmt.Errorf("raft: in-mem: peer not registered: %s", to)
	}
	return handler, nil
}

// inMemTransport은 Transport 인터페이스의 in-memory 구현. SendXxx는 hub에서 수신
// handler를 찾아 같은 goroutine에서 동기 호출한다.
type inMemTransport struct {
	self NodeID
	hub  *inMemHub
}

func newInMemTransport(self NodeID, hub *inMemHub) *inMemTransport {
	return &inMemTransport{self: self, hub: hub}
}

func (t *inMemTransport) SendRequestVote(ctx context.Context, to NodeID, args RequestVoteArgs) (RequestVoteReply, error) {
	handler, err := t.hub.route(t.self, to)
	if err != nil {
		return RequestVoteReply{}, err
	}
	return handler.HandleRequestVote(ctx, args)
}

func (t *inMemTransport) SendAppendEntries(ctx context.Context, to NodeID, args AppendEntriesArgs) (AppendEntriesReply, error) {
	handler, err := t.hub.route(t.self, to)
	if err != nil {
		return AppendEntriesReply{}, err
	}
	return handler.HandleAppendEntries(ctx, args)
}
