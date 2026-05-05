package raft

import "io"

// StateMachine은 commit된 entry를 적용해 도메인 상태를 갱신하는 소비자.
// raft 패키지는 ConfChange entry를 직접 처리하므로 Apply에는 Normal/Noop만 도달한다.
type StateMachine interface {
	// Apply는 commit된 entry를 적용하고 결과를 반환한다.
	// 반환값은 해당 entry를 propose한 호출자에게 그대로 전달된다.
	// Noop entry는 결과/에러 모두 무시된다.
	Apply(entry Entry) (result any, err error)

	// Snapshot은 현 상태의 직렬화 스트림을 반환한다.
	// 호출자는 ReadCloser를 반드시 닫는다. M2까지는 호출되지 않는다.
	Snapshot() (io.ReadCloser, error)

	// Restore는 Snapshot이 만든 스트림으로부터 상태를 복원한다.
	// M2까지는 호출되지 않는다.
	Restore(io.Reader) error
}
