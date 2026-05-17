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

// kill은 노드를 freeze한다 — 이후 tick에서 제외되고 hub에서 unregister되어
// 다른 노드가 보낸 RPC도 라우팅 실패한다. hardstate는 dir에 남는다.
func (c *cluster) kill(idx int) {
	c.hub.Unregister(c.ids[idx])
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
	hub.Register(id, node)
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

func TestCluster_ElectsLeader(t *testing.T) {
	// 3노드에서 한 candidate에 quorum이 모이는 정상 경로를 검증한다.
	// 노드 0에 가장 짧은 timeout을 부여해 결정적으로 가장 먼저 candidate 진입.
	// 다른 두 노드의 timeout은 충분히 크게 잡아 같은 사이클에서 동시 candidate가 안 되게 한다.
	//
	// 주의: 첫 grant로 quorum이 채워지면 startElection 루프가 break하므로 두 번째
	// peer엔 RequestVote가 도달하지 않을 수 있다 — 그 peer의 term은 0으로 남는다.
	// 따라서 "모든 노드의 term=1"은 invariant가 아니다. leader 등장과 leader 자신의
	// term이 1이라는 점만 검증한다 — follower 동기화는 heartbeat 단계에서 본다.
	c := newCluster(t, 3)
	c.nodes[0].electionTimeoutTicks = 3
	c.nodes[1].electionTimeoutTicks = 100
	c.nodes[2].electionTimeoutTicks = 100
	for _, n := range c.nodes {
		n.electionElapsedTicks = 0
	}

	c.tickAll(3)

	leaders := 0
	for _, n := range c.nodes {
		if n.role == RoleLeader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaders)
	}
	if c.nodes[0].role != RoleLeader {
		t.Fatalf("expected node-0 to be leader, role=%v", c.nodes[0].role)
	}
	if c.nodes[0].currentTerm != 1 {
		t.Fatalf("leader term=%d, want 1", c.nodes[0].currentTerm)
	}
}

func TestCluster_LeaderHeartbeatStabilizes(t *testing.T) {
	// leader 등장 후 추가 tick에서도 1 leader가 유지된다는 안정성 검증.
	// Phase 1 heartbeat이 follower의 election timeout을 리셋시켜 분열을 막는다.
	//
	// election 사이클에서 false negative였던 "모든 노드 term=1" invariant가 여기선
	// 성립한다 — leader의 첫 heartbeat이 election loop break로 RequestVote를 못 받은
	// 노드까지도 term=1로 동기화.
	c := newCluster(t, 3)
	c.nodes[0].electionTimeoutTicks = 3
	c.nodes[1].electionTimeoutTicks = 100
	c.nodes[2].electionTimeoutTicks = 100
	for _, n := range c.nodes {
		n.electionElapsedTicks = 0
	}
	c.tickAll(3) // 노드 0이 leader가 됨

	c.tickAll(50) // heartbeat이 없으면 분열 가능, 있으면 안정

	leaders := 0
	for _, n := range c.nodes {
		if n.role == RoleLeader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("leader should be stable after heartbeats, got %d leaders", leaders)
	}
	if c.nodes[0].role != RoleLeader {
		t.Fatalf("node-0 should remain leader, got %v", c.nodes[0].role)
	}
	for i, n := range c.nodes {
		if n.currentTerm != 1 {
			t.Fatalf("node[%d] term=%d, want 1 (heartbeats sync all to leader term)", i, n.currentTerm)
		}
	}
}

func TestCluster_NoQuorumStaysCandidate(t *testing.T) {
	// 노드 0만 alive, 나머지는 kill — quorum 부족. 노드 0은 candidate에 머무른다.
	c := newCluster(t, 3)
	c.kill(1)
	c.kill(2)
	c.nodes[0].electionTimeoutTicks = 3
	c.nodes[0].electionElapsedTicks = 0

	c.tickAll(3)

	if c.nodes[0].role != RoleCandidate {
		t.Fatalf("expected candidate without quorum, got %v", c.nodes[0].role)
	}
	if c.nodes[0].currentTerm != 1 {
		t.Fatalf("expected term=1 after one election, got %d", c.nodes[0].currentTerm)
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
