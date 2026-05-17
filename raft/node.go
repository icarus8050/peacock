package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Config는 raft Node의 운영 파라미터. 사용자는 모두 time.Duration로 작성하고,
// NewNode가 cfg.TickInterval로 나눠 내부 tick 카운트로 변환한다 — 알고리즘 코드는
// 결정적 정수 산술만 다룬다. 모든 Interval은 TickInterval의 양의 정수배여야 하며,
// 어긋나면 NewNode가 명시 에러로 거절한다(silent rounding 회피).
//
// production 모드에서는 Start()가 TickInterval마다 Tick()을 호출하는 goroutine을
// 띄운다. 테스트는 Tick() 또는 onTick()을 직접 호출해 시간을 결정적으로 진행시킨다
// (etcd raft 패턴).
type Config struct {
	ID                 NodeID
	TickInterval       time.Duration
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	MaxAppendEntries   int
	Dir                string // hardstate 영속 디렉터리
}

func (c Config) withDefaults() Config {
	if c.TickInterval <= 0 {
		c.TickInterval = 10 * time.Millisecond
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 100 * time.Millisecond
	}
	if c.ElectionTimeoutMin <= 0 {
		c.ElectionTimeoutMin = 500 * time.Millisecond
	}
	if c.ElectionTimeoutMax <= 0 {
		c.ElectionTimeoutMax = 1000 * time.Millisecond
	}
	if c.MaxAppendEntries <= 0 {
		c.MaxAppendEntries = 64
	}
	return c
}

// ErrStopped는 정지된 Node에 대한 호출에 반환된다.
var ErrStopped = errors.New("raft: node stopped")

// Node는 Raft 알고리즘의 한 노드. tick goroutine과 외부 RPC handler 호출이
// 동시에 들어오므로 mu로 상태 접근을 직렬화한다 — 외부에서 호출 가능한 모든
// 진입점(onTick, HandleRequestVote, HandleAppendEntries)이 mu를 잡는다.
// 자기 mu를 잡은 채 다른 노드의 RPC handler를 호출해도 노드별 mu라 데드락 없음.
type Node struct {
	mu        sync.Mutex
	cfg       Config
	log       Log
	sm        StateMachine
	transport Transport
	peers     map[NodeID]PeerInfo

	// 영속 (HardState로 디스크에 저장됨)
	currentTerm uint64
	votedFor    NodeID

	// 휘발
	role     Role
	leaderID NodeID

	// leader 전용 (becomeLeader에서 초기화, 아닌 경우 nil)
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64

	// tick 단위로 정규화된 timing 파라미터 (NewNode에서 cfg.*Interval로부터 계산)
	heartbeatTicks          int
	electionTimeoutMinTicks int
	electionTimeoutMaxTicks int

	// election deadline 회계 (follower/candidate에서만 의미)
	electionElapsedTicks int
	electionTimeoutTicks int

	// heartbeat 회계 (leader에서만 의미 — heartbeatTicks 도달 시 broadcast)
	heartbeatElapsedTicks int

	rng *rand.Rand

	tickCh   chan struct{}
	stopCh   chan struct{}
	runDone  chan struct{}
	tickDone chan struct{}
}

// NewNode는 Config + 의존성으로 새 Node를 만든다. 시작은 별도 Start 호출.
// peers는 자기 자신을 포함한 정적 멤버 목록.
// hardstate가 디렉터리에 있으면 그 term/votedFor로 복원되고, 없으면 zero에서 시작.
func NewNode(cfg Config, lg Log, sm StateMachine, t Transport, peers []PeerInfo) (*Node, error) {
	cfg = cfg.withDefaults()
	if cfg.ID == "" {
		return nil, fmt.Errorf("raft: NewNode: ID is empty")
	}
	if cfg.Dir == "" {
		return nil, fmt.Errorf("raft: NewNode: Dir is empty")
	}
	if lg == nil {
		return nil, fmt.Errorf("raft: NewNode: Log is nil")
	}
	if sm == nil {
		return nil, fmt.Errorf("raft: NewNode: StateMachine is nil")
	}
	if t == nil {
		return nil, fmt.Errorf("raft: NewNode: Transport is nil")
	}

	heartbeatTicks, err := asTicks("HeartbeatInterval", cfg.HeartbeatInterval, cfg.TickInterval)
	if err != nil {
		return nil, err
	}
	electionMinTicks, err := asTicks("ElectionTimeoutMin", cfg.ElectionTimeoutMin, cfg.TickInterval)
	if err != nil {
		return nil, err
	}
	electionMaxTicks, err := asTicks("ElectionTimeoutMax", cfg.ElectionTimeoutMax, cfg.TickInterval)
	if err != nil {
		return nil, err
	}
	if electionMaxTicks < electionMinTicks {
		return nil, fmt.Errorf("raft: NewNode: ElectionTimeoutMax (%v) < ElectionTimeoutMin (%v)",
			cfg.ElectionTimeoutMax, cfg.ElectionTimeoutMin)
	}

	hs, err := LoadHardState(cfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("raft: load hardstate: %w", err)
	}

	peerMap := make(map[NodeID]PeerInfo, len(peers))
	for _, p := range peers {
		peerMap[p.ID] = p
	}

	n := &Node{
		cfg:                     cfg,
		log:                     lg,
		sm:                      sm,
		transport:               t,
		peers:                   peerMap,
		currentTerm:             hs.Term,
		votedFor:                hs.VotedFor,
		role:                    RoleFollower, // 부팅은 항상 follower로 시작
		heartbeatTicks:          heartbeatTicks,
		electionTimeoutMinTicks: electionMinTicks,
		electionTimeoutMaxTicks: electionMaxTicks,
		rng:                     rand.New(rand.NewSource(time.Now().UnixNano())),
		tickCh:                  make(chan struct{}, 1),
		stopCh:                  make(chan struct{}),
		runDone:                 make(chan struct{}),
		tickDone:                make(chan struct{}),
	}
	n.resetElectionTimeout()
	return n, nil
}

// asTicks는 Duration이 TickInterval의 양의 정수배임을 검증하고 tick 수로 변환한다.
// 정수배가 아니면 silent rounding 대신 명시 에러로 거절한다.
func asTicks(name string, d, tick time.Duration) (int, error) {
	if d <= 0 {
		return 0, fmt.Errorf("raft: %s (%v) must be positive", name, d)
	}
	if d%tick != 0 {
		return 0, fmt.Errorf("raft: %s (%v) must be a multiple of TickInterval (%v)", name, d, tick)
	}
	return int(d / tick), nil
}

// Start는 Node의 메인 goroutine과 production tick goroutine을 시작한다.
// 한 번만 호출 가능 — 재시작은 새 Node로.
func (n *Node) Start() {
	go n.run()
	go n.runTickLoop()
}

// Stop은 Node를 정지하고 두 goroutine 종료를 모두 기다린다.
func (n *Node) Stop() {
	close(n.stopCh)
	<-n.runDone
	<-n.tickDone
}

// Tick은 Node의 시간을 한 단위 진행시킨다. production tick goroutine이 자동으로
// 호출하지만, 테스트는 직접 호출해 시간을 결정적으로 제어한다.
// 비차단 — 처리 중인 tick이 있으면 이 tick은 drop된다(과한 누적 방지, etcd 패턴).
func (n *Node) Tick() {
	select {
	case n.tickCh <- struct{}{}:
	case <-n.stopCh:
	default:
	}
}

// runTickLoop은 production 모드의 자동 tick driver. cfg.TickInterval마다 Tick().
// 테스트는 이 goroutine 없이 Tick()을 직접 호출해 결정적으로 진행시킨다.
func (n *Node) runTickLoop() {
	defer close(n.tickDone)
	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.Tick()
		}
	}
}

// run은 Node의 메인 이벤트 루프 — tick 채널을 읽어 onTick에 디스패치한다.
func (n *Node) run() {
	defer close(n.runDone)
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.tickCh:
			n.onTick()
		}
	}
}

// onTick은 한 tick 진행을 처리한다. follower/candidate에서 election timeout이
// 도달하면 startElection으로 새 election cycle에 진입한다. leader는 timeout으로
// step down하지 않는다 — heartbeat tick은 별도 카운터로 다룬다(replication).
func (n *Node) onTick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.onTickLocked()
}

func (n *Node) onTickLocked() {
	if n.role == RoleLeader {
		n.onLeaderTickLocked()
		return
	}
	n.electionElapsedTicks++
	if n.electionElapsedTicks >= n.electionTimeoutTicks {
		n.startElectionLocked()
	}
}

// onLeaderTickLocked은 leader가 매 tick마다 호출한다. heartbeatTicks 도달 시 빈
// AppendEntries를 broadcast해 follower의 election timeout을 리셋시킨다. leader는
// election timeout으로 step down하지 않으므로 electionElapsedTicks는 건드리지 않는다.
func (n *Node) onLeaderTickLocked() {
	n.heartbeatElapsedTicks++
	if n.heartbeatElapsedTicks >= n.heartbeatTicks {
		n.heartbeatElapsedTicks = 0
		n.broadcastHeartbeatLocked()
	}
}

// resetElectionTimeout은 elapsed를 0으로 되돌리고 randomized timeout을 다시 고른다.
func (n *Node) resetElectionTimeout() {
	n.electionElapsedTicks = 0
	n.electionTimeoutTicks = pickElectionTimeout(n.rng, n.electionTimeoutMinTicks, n.electionTimeoutMaxTicks)
}
