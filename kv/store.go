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
	compactor *compactor
	nextIndex int64
}

// Open은 opts.DirPath의 WAL로부터 store를 복구하고, opts.SyncInterval 주기로
// WAL을 fsync하는 백그라운드 syncer를 시작한다.
func Open(opts Options) (*Store, error) {
	opts = opts.withDefaults()
	walOpts := walOptionsFrom(opts)

	replayed, err := replay(walOpts)
	if err != nil {
		return nil, err
	}

	w, err := wal.Open(walOpts)
	if err != nil {
		return nil, fmt.Errorf("kv: open wal: %w", err)
	}

	s := &Store{
		data:      replayed.data,
		wal:       w,
		nextIndex: replayed.nextIndex,
	}
	s.syncer = newSyncer(w, opts.SyncInterval, opts.OnSyncError)
	s.syncer.start()
	s.compactor = newCompactor(w, opts.CompactionTrigger, opts.CompactionInterval, opts.OnCompactionError)
	s.compactor.start()

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

// Get은 키가 없으면 ErrNotFound를 반환한다. 반환되는 슬라이스는 방어 복사본이라
// 호출자가 수정해도 store 내부 상태에 영향이 없다.
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

// Close는 백그라운드 compactor와 syncer를 차례로 정지한 뒤 내부 WAL을 닫는다.
// 진행 중인 압축이 있으면 그 사이클이 끝날 때까지 기다리므로 큰 sealed 모음이
// 있을 경우 Close가 지연될 수 있다. WAL 닫기 과정에서 버퍼에 남아있던 쓰기가
// 디스크로 flush된다.
func (s *Store) Close() error {
	s.compactor.stop()
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
