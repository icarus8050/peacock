package wal

import "time"

type Syncable interface {
	Sync() error
}

type Syncer struct {
	target   Syncable
	interval time.Duration
	onError  func(error)
	stopCh   chan struct{}
	doneCh   chan struct{}
}

func NewSyncer(target Syncable, interval time.Duration, onError func(error)) *Syncer {
	return &Syncer{
		target:   target,
		interval: interval,
		onError:  onError,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Start launches the background sync goroutine.
func (s *Syncer) Start() {
	go s.loop()
}

// Stop signals the background goroutine to stop and waits for it to finish.
func (s *Syncer) Stop() {
	close(s.stopCh)
	<-s.doneCh
}

func (s *Syncer) loop() {
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
