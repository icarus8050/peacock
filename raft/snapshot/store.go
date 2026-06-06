package raftsnap

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	metaPrefix = "snap-meta-"
	dataPrefix = "snap-data-"
	tmpSuffix  = ".tmp"
)

// ErrNoSnapshot은 저장된 snapshot이 하나도 없을 때 Latest가 반환한다.
var ErrNoSnapshot = errors.New("raftsnap: no snapshot")

// Store는 한 디렉터리에 snapshot 파일들을 영속한다. 항상 최신(가장 큰 Index)
// 하나만 유효하게 유지하며, Commit 성공 시 그보다 낮은 Index의 snapshot은 정리한다.
type Store struct {
	dir string
	mu  sync.Mutex
}

// Open은 dir을 snapshot 저장소로 연다. 디렉터리가 없으면 만들고, 직전 크래시가
// 남긴 잔존 파일(.tmp, 짝 메타 없는 데이터)을 정리한다.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("raftsnap: mkdir: %w", err)
	}
	s := &Store{dir: dir}
	if err := s.cleanupOrphans(); err != nil {
		return nil, err
	}
	return s, nil
}

// Create는 meta의 snapshot을 쓰기 위한 Writer를 연다. 데이터는 tmp 파일에 누적되고
// Commit 시점에 atomic하게 확정된다.
func (s *Store) Create(meta SnapshotMeta) (Writer, error) {
	return newWriter(s, meta)
}

// Latest는 가장 큰 Index의 snapshot 메타와 데이터 스트림을 반환한다. 호출자는
// ReadCloser를 반드시 닫는다. snapshot이 없으면 ErrNoSnapshot.
func (s *Store) Latest() (SnapshotMeta, io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	meta, ok, err := s.latestMetaLocked()
	if err != nil {
		return SnapshotMeta{}, nil, err
	}
	if !ok {
		return SnapshotMeta{}, nil, ErrNoSnapshot
	}

	f, err := os.Open(filepath.Join(s.dir, dataName(meta)))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return SnapshotMeta{}, nil, fmt.Errorf("%w: data missing for index=%d", ErrSnapshotCorrupt, meta.Index)
		}
		return SnapshotMeta{}, nil, fmt.Errorf("raftsnap: open data: %w", err)
	}
	return meta, f, nil
}

// LatestMeta는 가장 큰 Index의 snapshot 메타만 반환한다(데이터 미오픈).
// snapshot이 없으면 ok=false.
func (s *Store) LatestMeta() (meta SnapshotMeta, ok bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latestMetaLocked()
}

// latestMetaLocked는 메타 파일들을 스캔해 가장 큰 Index의 유효 메타를 읽어 반환한다.
// 메타 파일이 손상(CRC/magic)이면 ErrSnapshotCorrupt로 중단 — atomic 기록이라 부분
// 손상은 정상 신호가 아니다.
func (s *Store) latestMetaLocked() (SnapshotMeta, bool, error) {
	metas, err := s.listMetasLocked()
	if err != nil {
		return SnapshotMeta{}, false, err
	}
	if len(metas) == 0 {
		return SnapshotMeta{}, false, nil
	}
	top := metas[len(metas)-1]

	buf, err := os.ReadFile(filepath.Join(s.dir, metaName(top)))
	if err != nil {
		return SnapshotMeta{}, false, fmt.Errorf("raftsnap: read meta: %w", err)
	}
	meta, err := decodeMeta(buf)
	if err != nil {
		return SnapshotMeta{}, false, err
	}
	return meta, true, nil
}

// listMetasLocked는 디렉터리의 모든 메타 파일명을 파싱해 Index 오름차순으로 반환한다.
// 파일 내용은 읽지 않는다 — 파일명에 박힌 Index/Term만 본다.
func (s *Store) listMetasLocked() ([]SnapshotMeta, error) {
	names, err := metaFileNames(s.dir)
	if err != nil {
		return nil, err
	}
	metas := make([]SnapshotMeta, 0, len(names))
	for _, name := range names {
		m, ok := parseMetaName(name)
		if !ok {
			continue
		}
		metas = append(metas, m)
	}
	sort.Slice(metas, func(i, j int) bool { return metas[i].Index < metas[j].Index })
	return metas, nil
}

// pruneBelowLocked는 keep.Index 미만의 commit된 snapshot 파일(메타·데이터)을
// best-effort로 지운다. 메타를 먼저 지워 "latest" 후보에서 즉시 빠지게 한다.
// 잔존 .tmp 정리는 Open의 cleanupOrphans 책임이라 여기서 다루지 않는다.
func (s *Store) pruneBelowLocked(keep SnapshotMeta) {
	metas, err := s.listMetasLocked()
	if err != nil {
		return
	}
	for _, m := range metas {
		if m.Index >= keep.Index {
			continue
		}
		_ = os.Remove(filepath.Join(s.dir, metaName(m)))
		_ = os.Remove(filepath.Join(s.dir, dataName(m)))
	}
}

// cleanupOrphans는 Open 시점에 잔존 파일을 정리한다 — 모든 .tmp, 그리고 짝 메타가
// 없는 데이터 파일(메타 rename 전 크래시의 흔적). 메타는 commit 포인트이므로 메타가
// 있는 데이터는 보존한다.
func (s *Store) cleanupOrphans() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("raftsnap: readdir: %w", err)
	}
	committed := committedDataIDs(entries)
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, tmpSuffix):
			_ = os.Remove(filepath.Join(s.dir, name))
		case strings.HasPrefix(name, dataPrefix) && !committed[strings.TrimPrefix(name, dataPrefix)]:
			_ = os.Remove(filepath.Join(s.dir, name))
		}
	}
	return nil
}

// committedDataIDs는 commit된 메타 파일들의 idPart 집합을 반환한다 — 짝 데이터가
// 보존 대상인지(메타 있음) 고아인지(메타 없음) 판정하는 기준.
func committedDataIDs(entries []os.DirEntry) map[string]bool {
	ids := make(map[string]bool)
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, metaPrefix) && !strings.HasSuffix(name, tmpSuffix) {
			ids[strings.TrimPrefix(name, metaPrefix)] = true
		}
	}
	return ids
}
