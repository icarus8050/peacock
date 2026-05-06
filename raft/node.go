package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"time"
)

// Config는 raft Node의 운영 파라미터. zero 값은 withDefaults에서 합리적 기본값으로
// 정규화된다.
type Config struct {
	ID                 NodeID
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	MaxAppendEntries   int
	Dir                string // hardstate 영속 디렉터리
}

func (c Config) withDefaults() Config {
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

// Node는 Raft 알고리즘의 한 노드. 모든 상태 변경은 단일 goroutine(run)에서
// 직렬화된다 — 외부는 채널/메서드를 통해 입력을 보내고 응답을 받는다.
type Node struct {
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

	rng              *rand.Rand
	electionDeadline time.Time

	stopCh chan struct{}
	doneCh chan struct{}
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

	hs, err := LoadHardState(cfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("raft: load hardstate: %w", err)
	}

	peerMap := make(map[NodeID]PeerInfo, len(peers))
	for _, p := range peers {
		peerMap[p.ID] = p
	}

	return &Node{
		cfg:         cfg,
		log:         lg,
		sm:          sm,
		transport:   t,
		peers:       peerMap,
		currentTerm: hs.Term,
		votedFor:    hs.VotedFor,
		role:        RoleFollower, // 부팅은 항상 follower로 시작
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
	}, nil
}

// Start는 Node의 메인 goroutine을 시작한다. 한 번만 호출 가능 — 재시작은 새 Node로.
func (n *Node) Start() {
	go n.run()
}

// Stop은 Node를 정지하고 메인 goroutine 종료를 기다린다.
func (n *Node) Stop() {
	close(n.stopCh)
	<-n.doneCh
}

// run은 Node의 메인 이벤트 루프 — 모든 상태 변경이 여기서 직렬화된다.
// 현재는 election deadline tick과 stop만 처리한다.
func (n *Node) run() {
	defer close(n.doneCh)

	n.resetElectionDeadline()
	timer := time.NewTimer(time.Until(n.electionDeadline))
	defer timer.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-timer.C:
			n.resetElectionDeadline()
			timer.Reset(time.Until(n.electionDeadline))
		}
	}
}

// resetElectionDeadline은 randomized timeout으로 다음 election deadline을 갱신한다.
func (n *Node) resetElectionDeadline() {
	timeout := pickElectionTimeout(n.rng, n.cfg.ElectionTimeoutMin, n.cfg.ElectionTimeoutMax)
	n.electionDeadline = time.Now().Add(timeout)
}
