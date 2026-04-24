package kv

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"peacock/wal"
)

var ErrNotFound = errors.New("kv: not found")

type Store struct {
	mu        sync.RWMutex
	data      map[string][]byte
	wal       *wal.WAL
	syncer    *syncer
	nextIndex int64
}

// Open recovers the store from the WAL at opts.DirPath and starts a
// background syncer that fsyncs the WAL on opts.SyncInterval.
func Open(opts Options) (*Store, error) {
	opts = opts.withDefaults()
	walOpts := walOptionsFrom(opts)

	data, nextIndex, err := replay(walOpts)
	if err != nil {
		return nil, err
	}

	w, err := wal.Open(walOpts)
	if err != nil {
		return nil, fmt.Errorf("kv: open wal: %w", err)
	}

	s := &Store{
		data:      data,
		wal:       w,
		nextIndex: nextIndex,
	}
	s.syncer = newSyncer(w, opts.SyncInterval, opts.OnSyncError)
	s.syncer.start()

	return s, nil
}

func (s *Store) Put(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := &wal.Entry{
		Op:        wal.OpPut,
		Index:     s.nextIndex,
		CreatedAt: wal.TimeStamp(time.Now().UnixNano()),
		Data:      encodePayload(key, value),
	}
	if err := s.wal.Append(entry); err != nil {
		return fmt.Errorf("kv: append put: %w", err)
	}

	s.nextIndex++
	s.data[key] = append([]byte(nil), value...)
	return nil
}

// Get returns the value for key. Returns ErrNotFound if the key is absent.
// The returned slice is a defensive copy and may be mutated by the caller.
func (s *Store) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), v...), nil
}

func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := &wal.Entry{
		Op:        wal.OpDelete,
		Index:     s.nextIndex,
		CreatedAt: wal.TimeStamp(time.Now().UnixNano()),
		Data:      encodePayload(key, nil),
	}
	if err := s.wal.Append(entry); err != nil {
		return fmt.Errorf("kv: append delete: %w", err)
	}

	s.nextIndex++
	delete(s.data, key)
	return nil
}

// Close stops the background syncer and closes the underlying WAL, which
// flushes any buffered writes to disk.
func (s *Store) Close() error {
	s.syncer.stop()
	return s.wal.Close()
}

func walOptionsFrom(opts Options) wal.Options {
	walOpts := wal.DefaultOptions(opts.DirPath)
	if opts.MaxSegmentSize > 0 {
		walOpts.MaxSegmentSize = opts.MaxSegmentSize
	}
	return walOpts
}
