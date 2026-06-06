// Package raftsnap는 Raft snapshot의 디스크 영속 계층이다.
//
// 한 snapshot은 두 파일로 표현된다 — 메타(`snap-meta-...`, 바이너리+CRC32)와
// 데이터(`snap-data-...`, state machine이 직렬화한 바이트). 메타 파일이 마지막에
// rename되어 commit 포인트가 되므로, 메타가 존재하고 그 CRC가 맞고 짝 데이터가
// 있을 때만 유효한 snapshot으로 간주한다. 그 외 잔존 파일(데이터만, tmp)은 고아로
// 무시·정리된다.
//
// raft 패키지는 이 패키지를 직접 import하지 않는다 — raft가 consumer 측에서
// SnapshotStore 인터페이스를 정의하고 *Store가 그것을 만족한다. 타입은 raft가
// alias로 재노출해 경계 변환 코드를 없앤다(raft/log의 Entry와 동일 패턴).
package raftsnap
