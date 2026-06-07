package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// maybeSnapshotLocked는 apply 진전 후 호출되어, 미적용 entry 누적이 임계에 도달하면
// snapshot을 만든다. mu를 잡은 채 동기로 수행 — SM.Snapshot과 디스크 fsync 동안 tick/
// RPC가 블록되지만 빈도가 낮고(임계마다 1회) state machine이 작다는 M2 가정. 큰 state로
// 가면 capture-then-persist(off-lock)로 분리할 자리.
//
// snapshot 실패는 fatal — 디스크 결함이거나 SM 직렬화 버그로, 조용히 넘기면 로그가
// 무한정 자란다.
func (n *Node) maybeSnapshotLocked() {
	if n.cfg.SnapshotThreshold == 0 || n.lastApplied == 0 {
		return
	}
	if n.lastApplied-n.snapshotIndexLocked() < n.cfg.SnapshotThreshold {
		return
	}
	if err := n.snapshotLocked(); err != nil {
		n.fatal(fmt.Errorf("raft: snapshot: %w", err))
	}
}

// snapshotLocked는 lastApplied 시점의 SM 상태를 snapshot으로 영속하고 로그 prefix를
// 압축한다. 순서가 crash-safety의 핵심: snapshot을 먼저 확정(Commit)한 뒤 로그를
// 자른다 — 역순이면 압축 후 크래시 시 snapshot도 로그도 없는 데이터 손실이 난다.
func (n *Node) snapshotLocked() error {
	index := n.lastApplied
	term, err := n.log.Term(index)
	if err != nil {
		return fmt.Errorf("term(%d): %w", index, err)
	}

	if err := n.persistSnapshotLocked(index, term); err != nil {
		return err
	}
	return n.compactLogLocked(index, term)
}

// persistSnapshotLocked는 SM의 직렬화 스트림을 SnapshotStore에 atomic하게 확정한다.
func (n *Node) persistSnapshotLocked(index, term uint64) error {
	rc, err := n.sm.Snapshot()
	if err != nil {
		return fmt.Errorf("sm.Snapshot: %w", err)
	}
	defer rc.Close()

	w, err := n.snap.Create(SnapshotMeta{Index: index, Term: term})
	if err != nil {
		return fmt.Errorf("snap.Create: %w", err)
	}
	if _, err := io.Copy(w, rc); err != nil {
		_ = w.Cancel()
		return fmt.Errorf("snap write: %w", err)
	}
	if err := w.Commit(); err != nil {
		return fmt.Errorf("snap commit: %w", err)
	}
	return nil
}

// compactLogLocked는 snapshot에 흡수된 prefix를 로그에서 제거한다. snapshot이 로그
// 전체를 덮으면(index == lastIndex) TruncateBefore가 거절하므로 Reset으로 통째로
// 갈아끼운다.
func (n *Node) compactLogLocked(index, term uint64) error {
	if index >= n.log.LastIndex() {
		return n.log.Reset(index, term)
	}
	return n.log.TruncateBefore(index)
}

// snapshotIndexLocked는 마지막 snapshot의 last-included index. 로그의 압축 경계로
// 표현된다 — FirstIndex가 경계+1이므로 경계는 FirstIndex-1. 경계가 없으면 0.
func (n *Node) snapshotIndexLocked() uint64 {
	first := n.log.FirstIndex()
	if first == 0 {
		return 0
	}
	return first - 1
}

// sendInstallSnapshotToLocked는 압축 경계보다 뒤처진 follower에게 snapshot을 송신한다.
// nextIndex[id] <= snapshotIndex라 leader가 그 자리의 entry를 더 이상 갖고 있지 않을 때
// sendAppendEntriesToLocked가 이 경로로 분기한다. 성공 시 follower는 snapshot index까지
// 따라잡은 것으로 보고 match/next를 갱신한다.
//
// AppendEntries와 동일하게 mu를 잡은 채 동기 송신 — 큰 snapshot 전송이 broadcast loop를
// 막아 다른 follower의 heartbeat를 지연시킬 수 있다(M2 소규모 가정; per-peer 분리는 후순위).
func (n *Node) sendInstallSnapshotToLocked(id NodeID) {
	meta, rc, err := n.snap.Latest()
	if errors.Is(err, ErrNoSnapshot) {
		return // 경계는 있는데 snapshot 파일이 없음 — 비정상이나 다음 라운드에 재시도
	}
	if err != nil {
		n.fatal(fmt.Errorf("raft: send snapshot: load: %w", err))
	}
	defer rc.Close()

	args := InstallSnapshotArgs{
		Term:              n.currentTerm,
		LeaderID:          n.cfg.ID,
		LastIncludedIndex: meta.Index,
		LastIncludedTerm:  meta.Term,
		Data:              rc,
	}
	reply, err := n.transport.SendInstallSnapshot(context.Background(), id, args)
	if err != nil {
		return
	}
	if reply.Term > n.currentTerm {
		if err := n.becomeFollower(reply.Term, ""); err != nil {
			n.fatal(fmt.Errorf("raft: stepdown on InstallSnapshot reply: %w", err))
		}
		return
	}
	n.matchIndex[id] = meta.Index
	n.nextIndex[id] = meta.Index + 1
	n.maybeAdvanceCommitLocked()
	n.applyCommittedLocked()
}

// HandleInstallSnapshot은 인입 InstallSnapshot RPC를 처리한다(논문 fig.13). term 검사 후
// snapshot을 영속·복원하고 로그를 경계에서 재시작한다. 이미 그 index 이상을 commit한
// 상태면(stale/중복 RPC) 멱등하게 skip — 재시도가 안전하다.
func (n *Node) HandleInstallSnapshot(ctx context.Context, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := InstallSnapshotReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply, nil
	}
	if err := n.becomeFollower(args.Term, args.LeaderID); err != nil {
		return reply, fmt.Errorf("raft: handle snapshot: become follower: %w", err)
	}
	reply.Term = n.currentTerm

	if args.LastIncludedIndex <= n.commitIndex {
		return reply, nil // 이미 보유 — 멱등 skip
	}
	if err := n.installSnapshotLocked(args); err != nil {
		return reply, fmt.Errorf("raft: handle snapshot: %w", err)
	}
	return reply, nil
}

// installSnapshotLocked는 수신한 snapshot을 Store에 확정한다(persist 단계). Commit
// 이전 실패는 디스크에 아무것도 안 남기므로 호출자에 에러로 돌려 RPC 재시도가 가능하다.
// Commit 성공 후에는 snapshot이 durable해지므로 SM 복원·로그 재시작을 adopt 단계로
// 넘기고, 그 단계 실패는 fatal로 격상한다(아래 참조).
func (n *Node) installSnapshotLocked(args InstallSnapshotArgs) error {
	w, err := n.snap.Create(SnapshotMeta{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm})
	if err != nil {
		return fmt.Errorf("snap.Create: %w", err)
	}
	if _, err := io.Copy(w, args.Data); err != nil {
		_ = w.Cancel()
		return fmt.Errorf("snap write: %w", err)
	}
	if err := w.Commit(); err != nil {
		return fmt.Errorf("snap commit: %w", err)
	}
	n.adoptInstalledSnapshotLocked(args)
	return nil
}

// adoptInstalledSnapshotLocked는 commit된 snapshot을 SM에 복원하고 로그를 경계에서
// 재시작한 뒤 진행도를 갱신한다. **이 단계 실패는 fatal** — Commit으로 디스크엔 새
// snapshot이 확정됐는데 SM/로그가 옛 상태로 남으면 재기동 시 복구 불가 불일치
// (restoreFromSnapshot이 lastApplied를 올려도 로그 경계가 어긋남)다. disk-failure-is-fatal
// 정책으로 격상해 그 불일치를 디스크에 남기지 않는다.
func (n *Node) adoptInstalledSnapshotLocked(args InstallSnapshotArgs) {
	_, rc, err := n.snap.Latest()
	if err != nil {
		n.fatal(fmt.Errorf("raft: adopt snapshot: reload: %w", err))
	}
	defer rc.Close()
	if err := n.sm.Restore(rc); err != nil {
		n.fatal(fmt.Errorf("raft: adopt snapshot: sm.Restore: %w", err))
	}
	if err := n.log.Reset(args.LastIncludedIndex, args.LastIncludedTerm); err != nil {
		n.fatal(fmt.Errorf("raft: adopt snapshot: log.Reset: %w", err))
	}
	n.lastApplied = args.LastIncludedIndex
	n.commitIndex = args.LastIncludedIndex
}
