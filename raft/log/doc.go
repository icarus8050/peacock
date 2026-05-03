// Package raftlog는 Raft 전용 영속 로그 스토리지를 담는다.
// 임의 인덱스 truncate(conflicting entry 처리)와 prefix 압축(snapshot 적용 후)을 지원한다.
package raftlog
