package raft

import (
	"context"
	"errors"
	"fmt"
)

// ErrNotLeader는 Propose가 leader가 아닌 노드에 도착했거나, leader가 propose 진행 중 step
// down한 경우 반환된다. 호출자(클라이언트 어댑터)는 leaderID 힌트를 보고 redirect한다.
var ErrNotLeader = errors.New("raft: not leader")

// proposeOutcome은 Propose 호출자에게 통지되는 결과. apply가 끝났음을 알리는 신호 채널 역할.
// Phase 4에서는 err만 — apply 결과(any)는 M4 (KV 통합) 시 추가.
type proposeOutcome struct {
	err error
}

// Propose는 cmd를 leader log에 Normal entry로 append하고 commit+apply가 끝날 때까지 block한다.
// leader가 아니면 즉시 ErrNotLeader. step-down 시 등록된 pending에 ErrNotLeader 통지.
// ctx 만료 시 그 entry는 등록만 제거되고 log/apply는 이 leader에서 계속 진행될 수 있다 —
// at-least-once 시멘틱(호출자가 idempotency 보장 또는 client 측 dedupe).
//
// **ctx 만료 후 leader가 step-down하면** 이 leader의 미commit entry는 다음 leader 선택에 따라
// commit되거나(carry-along) truncate될 수 있다 — 호출자는 idempotency 키로 후속 조회/재시도해
// 결과를 확정한다.
//
// 반환값 (index, err):
//   - 성공: (entry index, nil). caller는 같은 index를 다른 노드에서 query 가능.
//   - ErrNotLeader: (0, ErrNotLeader). 다른 leader로 redirect 필요.
//   - ctx 만료: (entry index, ctx.Err()). entry는 이미 log에 있음 — Propose 재시도하면 중복.
//   - log append 실패: (0, error). entry 자체가 안 들어감 — 안전하게 재시도.
func (n *Node) Propose(ctx context.Context, cmd []byte) (uint64, error) {
	ch, index, err := n.enqueueProposalLocked(cmd)
	if err != nil {
		return 0, err
	}
	select {
	case <-ctx.Done():
		n.cancelPending(index)
		return index, ctx.Err()
	case out := <-ch:
		return index, out.err
	}
}

// enqueueProposalLocked는 mu를 잡아 leader 여부 확인 → log append → pending 등록 → broadcast
// 까지 한 임계 구간에서 처리한다. 채널은 buffered(1)라 apply/step-down 통지가 차단되지 않는다.
func (n *Node) enqueueProposalLocked(cmd []byte) (chan proposeOutcome, uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != RoleLeader {
		return nil, 0, ErrNotLeader
	}
	index := n.log.LastIndex() + 1
	entry := Entry{
		Term:  n.currentTerm,
		Index: index,
		Type:  EntryNormal,
		Data:  cmd,
	}
	if err := n.log.Append([]Entry{entry}); err != nil {
		return nil, 0, fmt.Errorf("raft: Propose: append: %w", err)
	}
	ch := make(chan proposeOutcome, 1)
	if n.pendingProposals == nil {
		n.pendingProposals = make(map[uint64]chan proposeOutcome)
	}
	n.pendingProposals[index] = ch
	n.broadcastAppendEntriesLocked()
	n.tryCommitAndApplyLocked() // 1노드면 즉시 진전, 다중 노드면 no-op
	return ch, index, nil
}

func (n *Node) cancelPending(index uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.pendingProposals, index)
}

// completeProposalLocked는 apply 시점에 호출 — 그 index의 pending이 있으면 outcome을 보낸다.
// channel은 buffered(1)이라 send는 차단되지 않으며, caller가 ctx 만료로 떠난 뒤라도 안전.
func (n *Node) completeProposalLocked(index uint64, err error) {
	ch, ok := n.pendingProposals[index]
	if !ok {
		return
	}
	ch <- proposeOutcome{err: err}
	delete(n.pendingProposals, index)
}

// notifyPendingProposalsLocked는 step-down(becomeFollower) 시 등록된 모든 pending에 통지하고
// 맵을 비운다. buffered(1) 채널이라 send 차단 없음. 호출 경로는 leader→follower만 — 논문상
// leader는 항상 (term 큰) leader 또는 (term 큰 후보의) follower로 step down하지 candidate로
// 직접 가지 않는다.
func (n *Node) notifyPendingProposalsLocked(err error) {
	for idx, ch := range n.pendingProposals {
		ch <- proposeOutcome{err: err}
		delete(n.pendingProposals, idx)
	}
}
