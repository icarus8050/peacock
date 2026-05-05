package raft

// Log는 Raft 알고리즘이 영속 로그를 다루기 위한 소비자 측 인터페이스.
// raft/log 패키지의 *raftlog.Log가 이 인터페이스를 만족한다.
//
// 이 인터페이스는 raft가 disk 구현 세부에 의존하지 않게 하기 위한 것이다.
// 테스트는 in-memory 구현으로 갈아끼울 수 있다.
type Log interface {
	// FirstIndex는 로그에 남아 있는 첫 entry index. 비어 있으면 0.
	// snapshot truncation 후에는 1보다 클 수 있다.
	FirstIndex() uint64

	// LastIndex는 로그의 마지막 entry index. 비어 있으면 0.
	LastIndex() uint64

	// LastTerm은 로그의 마지막 entry term. 비어 있으면 0.
	LastTerm() uint64

	// Term은 주어진 index의 term을 반환. 범위 밖이면 에러.
	Term(index uint64) (uint64, error)

	// Entries는 [lo, hi) 범위의 entry를 반환. maxBytes>0이면 누적 인코딩 크기가
	// 한계에 도달하기 전까지만 반환(최소 1개는 항상).
	Entries(lo, hi, maxBytes uint64) ([]Entry, error)

	// Append는 entries를 로그 끝에 추가. 호출자는 비어 있지 않은 batch, 인덱스
	// 연속성, term 단조성을 보장해야 한다.
	Append(entries []Entry) error

	// TruncateAfter는 (index, lastIndex] 범위 entry를 모두 제거.
	// conflicting entries를 잘라낼 때 쓴다.
	TruncateAfter(index uint64) error

	// TruncateBefore는 [firstIndex, index) 범위 entry를 제거.
	// snapshot 적용 후 prefix 압축에 쓴다.
	TruncateBefore(index uint64) error

	// Sync는 buffered write를 디스크에 flush + fsync.
	Sync() error

	// Close는 로그를 닫는다.
	Close() error
}
