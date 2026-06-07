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
	SnapshotThreshold  uint64 // 미적용 entry 수가 이 값 이상이면 snapshot. 0 = 비활성.
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
	snap      SnapshotStore
	transport Transport
	peers     map[NodeID]PeerInfo

	// 영속 (HardState로 디스크에 저장됨)
	currentTerm uint64
	votedFor    NodeID

	// 휘발
	role     Role
	leaderID NodeID

	// 휘발 — commit/apply 진행도. 0에서 시작, role 전이로 리셋되지 않음. 재기동 시에도 0부터.
	// commit/lastApplied 영속화는 논문상 휘발 OK이지만 idempotent SM 가정이 필요 — KV의
	// put/delete는 idempotent라 안전. snapshot 도입(M2) 시 lastApplied는 snapshot index에서 시작.
	commitIndex uint64
	lastApplied uint64

	// leader 전용 (becomeLeader에서 초기화, 아닌 경우 nil)
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64

	// leader 전용 — Propose 호출자에게 commit+apply 결과를 통지하는 채널 맵.
	// step-down(becomeFollower) 시 모두 ErrNotLeader로 통지하고 비운다.
	pendingProposals map[uint64]chan proposeOutcome

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
// snap에 저장된 snapshot이 있으면 SM.Restore로 상태를 복원하고 lastApplied/commitIndex를
// snapshot index로 끌어올린다.
func NewNode(cfg Config, lg Log, sm StateMachine, snap SnapshotStore, t Transport, peers []PeerInfo) (*Node, error) {
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
	if snap == nil {
		return nil, fmt.Errorf("raft: NewNode: SnapshotStore is nil")
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
		snap:                    snap,
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
	if err := n.restoreFromSnapshot(); err != nil {
		return nil, err
	}
	n.resetElectionTimeout()
	return n, nil
}

// restoreFromSnapshot은 저장된 snapshot이 있으면 SM에 복원하고 commit/apply 진행도를
// snapshot index로 끌어올린다 — snapshot에 흡수된 entry는 이미 적용된 것이므로 로그
// 재생은 snapshot index 다음부터 시작해야 한다. snapshot이 없으면 zero에서 시작.
func (n *Node) restoreFromSnapshot() error {
	meta, rc, err := n.snap.Latest()
	if errors.Is(err, ErrNoSnapshot) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("raft: restore: load snapshot: %w", err)
	}
	defer rc.Close()
	if err := n.sm.Restore(rc); err != nil {
		return fmt.Errorf("raft: restore: sm.Restore: %w", err)
	}
	n.lastApplied = meta.Index
	n.commitIndex = meta.Index
	return nil
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

// Role은 현재 노드의 role을 반환한다 — **테스트 진단 전용** snapshot view. production 코드가
// role을 분기 조건으로 쓰지 말 것 — role-aware 분기가 필요하면 Propose의 ErrNotLeader 또는
// 별도 leader-hint API로 표현한다. 이 getter로 다른 사적 상태(term, commitIndex 등)를 추가
// 노출하지 않도록 슬리퍼리 슬로프 경계.
func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// fatal은 회복 불가능한 disk write 실패를 받아 process를 종료한다. outbound RPC
// 응답을 처리하다 persist/log 실패가 나면 메모리만 갱신된 채 디스크는 옛값으로 남아
// 같은 term에 두 번 vote granted가 가능해진다(논문 fig.2 invariant 깨짐). 호출자에
// 에러를 전파할 수 없는 비동기 경로에서만 호출하고, RPC handler처럼 응답 의무가 있는
// 경로에선 에러를 그대로 반환한다. logger 도입 시 logger.Fatalf로 교체.
func (n *Node) fatal(err error) {
	panic(err)
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

// onLeaderTickLocked은 leader가 매 tick마다 호출한다. heartbeatTicks 도달 시
// AppendEntries를 broadcast — 보낼 entries 있으면 같이, 없으면 heartbeat(빈 entries).
// follower의 election timeout 리셋이 목적. leader는 election timeout으로 step down하지
// 않으므로 electionElapsedTicks는 건드리지 않는다.
func (n *Node) onLeaderTickLocked() {
	n.heartbeatElapsedTicks++
	if n.heartbeatElapsedTicks >= n.heartbeatTicks {
		n.heartbeatElapsedTicks = 0
		n.broadcastAppendEntriesLocked()
	}
}

// resetElectionTimeout은 elapsed를 0으로 되돌리고 randomized timeout을 다시 고른다.
func (n *Node) resetElectionTimeout() {
	n.electionElapsedTicks = 0
	n.electionTimeoutTicks = pickElectionTimeout(n.rng, n.electionTimeoutMinTicks, n.electionTimeoutMaxTicks)
}
