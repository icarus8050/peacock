package raft

import (
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
