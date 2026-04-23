package kv

import "time"

type syncable interface {
	Sync() error
}

type syncer struct {
	target   syncable
	interval time.Duration
	onError  func(error)
	stopCh   chan struct{}
	doneCh   chan struct{}
}

func newSyncer(target syncable, interval time.Duration, onError func(error)) *syncer {
	return &syncer{
		target:   target,
		interval: interval,
		onError:  onError,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

func (s *syncer) start() {
	go s.loop()
}

func (s *syncer) stop() {
	close(s.stopCh)
	<-s.doneCh
}

func (s *syncer) loop() {
	defer close(s.doneCh)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.target.Sync(); err != nil && s.onError != nil {
				s.onError(err)
			}
		case <-s.stopCh:
			return
		}
	}
}
