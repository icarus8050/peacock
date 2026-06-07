package raft

import (
	"fmt"
	"sync"
)

// fakeLog은 메모리 슬라이스 기반 Log — replication 테스트가 실제 Append/Entries/Term
// 동작을 요구해 stubLog로는 부족하다. zero 시작은 빈 로그. 동시 호출은 mu로 직렬화 —
// election.go가 자기 mu를 든 채 다른 노드의 HandleAppendEntries로 진입하므로 같은
// log이 두 노드 사이에서 동시 접근되진 않지만, race detector를 통과시키려 mu 사용.
type fakeLog struct {
	mu        sync.Mutex
	entries   []Entry
	snapIndex uint64 // 압축 경계 (last-included); 0 = 없음
	snapTerm  uint64
}

func newFakeLog() *fakeLog { return &fakeLog{} }

// seedLog는 newFakeLog + seedTo의 단축 — literal 사용처(election의 up-to-date 검사)에서 호출.
func seedLog(lastIndex, lastTerm uint64) *fakeLog {
	l := newFakeLog()
	l.seedTo(lastIndex, lastTerm)
	return l
}

// seedTo는 로그가 (lastIndex, lastTerm)까지 채워진 상태를 시뮬레이션한다. 1번부터
// lastIndex까지 모두 term=lastTerm인 entries로 채운다 — election의 up-to-date 검사
// 같은 시나리오용.
func (l *fakeLog) seedTo(lastIndex, lastTerm uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = make([]Entry, 0, lastIndex)
	for i := uint64(1); i <= lastIndex; i++ {
		l.entries = append(l.entries, Entry{Index: i, Term: lastTerm})
	}
}

func (l *fakeLog) FirstIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) == 0 {
		if l.snapIndex != 0 {
			return l.snapIndex + 1
		}
		return 0
	}
	return l.entries[0].Index
}

func (l *fakeLog) LastIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) == 0 {
		return l.snapIndex
	}
	return l.entries[len(l.entries)-1].Index
}

func (l *fakeLog) LastTerm() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) == 0 {
		return l.snapTerm
	}
	return l.entries[len(l.entries)-1].Term
}

func (l *fakeLog) Term(index uint64) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index != 0 && index == l.snapIndex {
		return l.snapTerm, nil
	}
	for _, e := range l.entries {
		if e.Index == index {
			return e.Term, nil
		}
	}
	return 0, fmt.Errorf("fakeLog: term out of range: %d", index)
}

func (l *fakeLog) Entries(lo, hi, _ uint64) ([]Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []Entry
	for _, e := range l.entries {
		if e.Index >= lo && e.Index < hi {
			out = append(out, e)
		}
	}
	return out, nil
}

func (l *fakeLog) Append(entries []Entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entries...)
	return nil
}

func (l *fakeLog) TruncateAfter(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var keep []Entry
	for _, e := range l.entries {
		if e.Index <= index {
			keep = append(keep, e)
		}
	}
	l.entries = keep
	return nil
}

func (l *fakeLog) TruncateBefore(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var keep []Entry
	for _, e := range l.entries {
		if e.Index > index {
			keep = append(keep, e)
		} else if e.Index == index {
			l.snapIndex = e.Index
			l.snapTerm = e.Term
		}
	}
	l.entries = keep
	return nil
}

func (l *fakeLog) Reset(index, term uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = nil
	l.snapIndex = index
	l.snapTerm = term
	return nil
}

func (l *fakeLog) Sync() error  { return nil }
func (l *fakeLog) Close() error { return nil }
