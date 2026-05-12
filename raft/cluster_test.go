package raft

import (
	"fmt"
	"testing"
	"time"
)

// cluster는 한 프로세스 안의 N개 Node를 inMemHub에 묶어 결정적으로 시뮬레이션하는
// 테스트 harness. Node.Start()를 호출하지 않으며, 시간은 tick/tickAll로만 진행된다 —
// 운영 tick goroutine 경로는 TestNode_StartStop이 별도로 검증한다.
type cluster struct {
	t     *testing.T
	hub   *inMemHub
	nodes []*Node
	ids   []NodeID
	dirs  []string
	alive []bool
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	hub := newInMemHub()
	ids := make([]NodeID, n)
	for i := range ids {
		ids[i] = NodeID(fmt.Sprintf("node-%d", i+1))
	}
	peers := makeStaticPeers(ids)

	dirs := make([]string, n)
	nodes := make([]*Node, n)
	alive := make([]bool, n)
	for i := 0; i < n; i++ {
		dirs[i] = t.TempDir()
		nodes[i] = bootNode(t, hub, ids[i], dirs[i], peers)
		alive[i] = true
	}
	return &cluster{t: t, hub: hub, nodes: nodes, ids: ids, dirs: dirs, alive: alive}
}

// tick은 한 노드를 n tick 진행시킨다. kill된 노드는 무시한다.
func (c *cluster) tick(idx, n int) {
	c.t.Helper()
	if !c.alive[idx] {
		return
	}
	for i := 0; i < n; i++ {
		c.nodes[idx].onTick()
	}
}

// tickAll은 살아있는 모든 노드를 n tick 진행시킨다.
func (c *cluster) tickAll(n int) {
	c.t.Helper()
	for i := 0; i < n; i++ {
		for j := range c.nodes {
			if c.alive[j] {
				c.nodes[j].onTick()
			}
		}
	}
}

// kill은 노드를 freeze한다 — 이후 tick에서 제외된다. hardstate는 dir에 남는다.
func (c *cluster) kill(idx int) {
	c.alive[idx] = false
}

// restart는 같은 dir로 Node를 다시 만들어 disk의 hardstate를 복원한다.
func (c *cluster) restart(idx int) {
	c.t.Helper()
	c.nodes[idx] = bootNode(c.t, c.hub, c.ids[idx], c.dirs[idx], makeStaticPeers(c.ids))
	c.alive[idx] = true
}

func (c *cluster) partition(from, to NodeID) { c.hub.Partition(from, to) }
func (c *cluster) partitionBoth(a, b NodeID) { c.hub.PartitionBoth(a, b) }
func (c *cluster) heal(from, to NodeID)      { c.hub.Heal(from, to) }
func (c *cluster) healBoth(a, b NodeID)      { c.hub.HealBoth(a, b) }

func bootNode(t *testing.T, hub *inMemHub, id NodeID, dir string, peers []PeerInfo) *Node {
	t.Helper()
	cfg := Config{
		ID:                 id,
		Dir:                dir,
		TickInterval:       1 * time.Millisecond,
		HeartbeatInterval:  2 * time.Millisecond,
		ElectionTimeoutMin: 5 * time.Millisecond,
		ElectionTimeoutMax: 10 * time.Millisecond,
	}
	node, err := NewNode(cfg, stubLog{}, stubSM{}, newInMemTransport(id, hub), peers)
	if err != nil {
		t.Fatalf("bootNode %s: %v", id, err)
	}
	return node
}

func makeStaticPeers(ids []NodeID) []PeerInfo {
	peers := make([]PeerInfo, len(ids))
	for i, id := range ids {
		peers[i] = PeerInfo{ID: id}
	}
	return peers
}

func TestCluster_TickAllAdvancesAllNodes(t *testing.T) {
	// tickAll의 cluster-level invariant — "각 노드에 정확히 N번씩 tick이 분배된다" —
	// 를 직접 검증한다. cycle reset 분기는 TestOnTick이 따로 본다 — timeout을 크게
	// 잡아 cycle에 닿지 않게 한 뒤 elapsed 카운터로 분배를 확인.
	c := newCluster(t, 3)
	for _, n := range c.nodes {
		n.electionTimeoutTicks = 100
		n.electionElapsedTicks = 0
	}
	c.tickAll(5)
	for i, n := range c.nodes {
		if n.electionElapsedTicks != 5 {
			t.Fatalf("node[%d]: tickAll(5) should advance elapsed to 5, got %d", i, n.electionElapsedTicks)
		}
	}
}

func TestCluster_KillStopsTicks(t *testing.T) {
	c := newCluster(t, 3)
	for _, n := range c.nodes {
		n.electionTimeoutTicks = 100
		n.electionElapsedTicks = 0
	}
	c.kill(1)
	c.tickAll(2)
	if c.nodes[1].electionElapsedTicks != 0 {
		t.Fatalf("killed node should not advance, got elapsed=%d", c.nodes[1].electionElapsedTicks)
	}
	for _, idx := range []int{0, 2} {
		if c.nodes[idx].electionElapsedTicks != 2 {
			t.Fatalf("alive node[%d] should have elapsed=2, got %d", idx, c.nodes[idx].electionElapsedTicks)
		}
	}
}

func TestCluster_RestartUsesSameDir(t *testing.T) {
	// harness가 idx별 dir을 안정적으로 묶고 있는지 — restart 후 새 Node가 같은
	// dir의 hardstate를 보게 된다. hardstate 복원 자체는 TestNewNode_RestoresHardState가
	// 따로 본다 — 이 테스트는 "idx→dir wiring"이라는 cluster-level invariant 전용.
	c := newCluster(t, 3)
	if err := SaveHardState(c.dirs[1], HardState{Term: 9, VotedFor: "node-2"}); err != nil {
		t.Fatalf("seed hardstate: %v", err)
	}
	c.kill(1)
	c.restart(1)
	if c.nodes[1].currentTerm != 9 || c.nodes[1].votedFor != "node-2" {
		t.Fatalf("restart did not preserve dir wiring: term=%d voted=%q",
			c.nodes[1].currentTerm, c.nodes[1].votedFor)
	}
}
