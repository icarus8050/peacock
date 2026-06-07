package raft

import (
	"io"

	raftsnap "peacock/raft/snapshot"
)

// SnapshotMeta / SnapshotWriter는 raft/snapshot 패키지의 동일 타입을 그대로 노출한다 —
// 두 곳에 같은 타입을 정의하면 경계마다 변환 코드가 붙는다. raft/snapshot은 raft를
// import하지 않으므로 의존 방향은 단방향이 유지된다(raft/log의 Entry와 동일 패턴).
type (
	SnapshotMeta   = raftsnap.SnapshotMeta
	SnapshotWriter = raftsnap.Writer
)

// SnapshotStore는 raft가 snapshot을 영속하기 위한 소비자 측 인터페이스.
// raft/snapshot 패키지의 *raftsnap.Store가 이를 만족한다.
//
// SnapshotMeta의 Index/Term은 raft.Node가 채운다 — snapshot 시점의 lastApplied와 그
// term. StateMachine은 바이트 스트림만 제공하고 meta를 모른다.
type SnapshotStore interface {
	// Create는 meta의 snapshot을 쓰기 위한 Writer를 연다. 데이터는 Commit 시점에
	// atomic하게 확정된다.
	Create(meta SnapshotMeta) (SnapshotWriter, error)

	// Latest는 가장 큰 Index의 snapshot 메타와 데이터 스트림을 반환한다.
	// 호출자는 ReadCloser를 닫는다. snapshot이 없으면 raftsnap.ErrNoSnapshot.
	Latest() (SnapshotMeta, io.ReadCloser, error)

	// LatestMeta는 가장 큰 Index의 메타만 반환한다(데이터 미오픈). 없으면 ok=false.
	LatestMeta() (meta SnapshotMeta, ok bool, err error)
}

// ErrNoSnapshot은 저장된 snapshot이 없을 때 Latest가 반환한다(raftsnap 재노출).
var ErrNoSnapshot = raftsnap.ErrNoSnapshot

var _ SnapshotStore = (*raftsnap.Store)(nil)
