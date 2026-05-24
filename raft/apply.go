package raft

import "fmt"

// applyCommittedLocked는 commitIndex > lastApplied 동안 SM.Apply를 호출하고 lastApplied를
// 진전시킨다. commit 진전이 일어난 자리(leader matchIndex advance, follower LeaderCommit
// 반영)에서 동기로 호출 — mu를 잡은 채.
//
// **mu 점유 시간 주의**: SM.Apply가 호출되는 동안 raft mu가 잡혀 있다 — Apply가 raft 메서드를
// 재진입(예: Propose, Status)하면 self-deadlock, 느린 Apply는 tick·RPC handler를 블록해
// spurious election 유발. Phase 4(propose 응답 라우팅)에서 별도 goroutine + ready 채널로
// 분리 예정. 현 단계에서는 SM이 raft 재진입을 안 하고 Apply가 빠르다는 가정.
//
// statemachine.go 명세대로 Apply는 모든 entry에 호출하되, Noop entry는 결과/에러를 무시한다.
// ConfChange는 raft가 직접 처리하므로 Apply에 도달하지 않는다(Phase 3 범위 밖).
//
// **Normal entry의 Apply 에러는 fatal** — deterministic SM 가정. apply error가 발생하면
// 도메인 로직 버그(non-deterministic 경로 또는 invariant 깨짐)로 보고 빠르게 종료해 운영자가
// 인지하게 한다. cluster 전체가 같은 entry에서 같은 panic을 일으킬 위험은 deterministic SM에선
// 정상 동작(잘못된 entry 자체를 commit한 게 잘못). Phase 4에서 propose 응답 채널 도입 시
// 정책 재검토 자리.
func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		next := n.lastApplied + 1
		entry, err := n.entryAt(next)
		if err != nil {
			n.fatal(fmt.Errorf("raft: apply: %w", err))
		}
		_, applyErr := n.sm.Apply(entry)
		if entry.Type == EntryNormal && applyErr != nil {
			n.fatal(fmt.Errorf("raft: apply: sm.Apply(idx=%d): %w", next, applyErr))
		}
		n.lastApplied = next
	}
}

// entryAt은 정확히 index 한 자리의 entry를 가져온다. log.Entries [lo, hi) 반열림 구간이라
// hi=index+1로 호출. 없으면 에러 — apply 직전이라 반드시 있어야 한다.
func (n *Node) entryAt(index uint64) (Entry, error) {
	entries, err := n.log.Entries(index, index+1, 0)
	if err != nil {
		return Entry{}, fmt.Errorf("raft: entryAt: log entries(%d): %w", index, err)
	}
	if len(entries) == 0 {
		return Entry{}, fmt.Errorf("raft: entryAt: index %d missing in log", index)
	}
	return entries[0], nil
}
