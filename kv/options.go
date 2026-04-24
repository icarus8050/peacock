package kv

import "time"

const defaultSyncInterval = 100 * time.Millisecond

type Options struct {
	DirPath        string
	SyncInterval   time.Duration
	MaxSegmentSize int64
	OnSyncError    func(error)
}

func DefaultOptions(dir string) Options {
	return Options{
		DirPath:      dir,
		SyncInterval: defaultSyncInterval,
	}
}

func (o Options) withDefaults() Options {
	if o.SyncInterval <= 0 {
		o.SyncInterval = defaultSyncInterval
	}
	return o
}
