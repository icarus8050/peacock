package raft

import (
	"math/rand"
	"time"
)

// pickElectionTimeout은 [min, max) 사이에서 균등 분포로 랜덤한 timeout을 고른다.
// max <= min이면 min을 그대로 반환한다(테스트에서 결정적 값을 강제할 때 사용).
//
// Raft 논문 권장: heartbeat의 ~10배 + 충분한 randomization으로 split vote 회피.
func pickElectionTimeout(rng *rand.Rand, min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	return min + time.Duration(rng.Int63n(int64(max-min)))
}
