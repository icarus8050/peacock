package kv

import "time"

const (
	defaultSyncInterval       = 100 * time.Millisecond
	defaultCompactionTrigger  = 4
	defaultCompactionInterval = 1 * time.Second
)

type Options struct {
	DirPath            string
	SyncInterval       time.Duration
	MaxSegmentSize     int64
	CompactionTrigger  int
	CompactionInterval time.Duration
	OnSyncError        func(error)
	OnCompactionError  func(error)
}

func DefaultOptions(dir string) Options {
	return Options{
		DirPath:            dir,
		SyncInterval:       defaultSyncInterval,
		CompactionTrigger:  defaultCompactionTrigger,
		CompactionInterval: defaultCompactionInterval,
	}
}

func (o Options) withDefaults() Options {
	if o.SyncInterval <= 0 {
		o.SyncInterval = defaultSyncInterval
	}
	if o.CompactionTrigger <= 0 {
		o.CompactionTrigger = defaultCompactionTrigger
	}
	if o.CompactionInterval <= 0 {
		o.CompactionInterval = defaultCompactionInterval
	}
	// MaxSegmentSize는 0이면 wal 패키지의 디폴트(64MiB)에 위임 — walOptionsFrom 참조.
	return o
}
