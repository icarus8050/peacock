package kv

import (
	"encoding/binary"
	"time"

	"peacock/wal"
)

// compactable은 compactor가 의존하는 wal.WAL의 부분 인터페이스이다 (소비자 측 정의).
type compactable interface {
	Compact(trigger int, keyOf func(*wal.Entry) ([]byte, error)) (bool, error)
}

type compactor struct {
	target   compactable
	trigger  int
	interval time.Duration
	onError  func(error)
	stopCh   chan struct{}
	doneCh   chan struct{}
}

func newCompactor(target compactable, trigger int, interval time.Duration, onError func(error)) *compactor {
	return &compactor{
		target:   target,
		trigger:  trigger,
		interval: interval,
		onError:  onError,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

func (c *compactor) start() {
	go c.loop()
}

func (c *compactor) stop() {
	close(c.stopCh)
	<-c.doneCh
}

func (c *compactor) loop() {
	defer close(c.doneCh)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if _, err := c.target.Compact(c.trigger, keyFromEntry); err != nil && c.onError != nil {
				c.onError(err)
			}
		case <-c.stopCh:
			return
		}
	}
}

// keyFromEntry는 wal.Entry.Data에서 KV 페이로드의 key 부분을 추출한다.
// 형식: KeyLen(4) | Key | Value (codec.go의 encodePayload와 일치).
func keyFromEntry(e *wal.Entry) ([]byte, error) {
	if len(e.Data) < keyLenSize {
		return nil, ErrCorruptPayload
	}
	keyLen := int(binary.LittleEndian.Uint32(e.Data[:keyLenSize]))
	if len(e.Data) < keyLenSize+keyLen {
		return nil, ErrCorruptPayload
	}
	return e.Data[keyLenSize : keyLenSize+keyLen], nil
}
