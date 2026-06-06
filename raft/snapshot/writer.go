package raftsnap

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"peacock/internal/fsutil"
)

const writerBufferSize = 64 * 1024

// ErrWriterDone은 이미 Commit/Cancel된 Writer에 다시 Commit을 호출했을 때 반환된다.
var ErrWriterDone = errors.New("raftsnap: writer already finalized")

// Writer는 한 snapshot의 데이터 스트림을 받아 Commit 시점에 atomic하게 확정한다.
// 로컬 snapshot 생성(io.Copy)과 InstallSnapshot 수신(chunk별 Write) 양쪽이 같은
// 인터페이스를 쓴다. Commit 또는 Cancel 중 정확히 하나를 호출해야 한다.
//
// 동시성 계약: 하나의 Writer는 단일 고루틴에서 Write들 → Commit/Cancel 순서로만
// 쓴다. Store.mu는 디렉터리(파일 목록·prune)를 보호할 뿐 Writer 인스턴스의 buffer를
// 보호하지 않으므로, Write와 Commit을 다른 고루틴에서 부르면 bufio.Writer에 동시
// 접근이 생긴다. raft에서 snapshot 생성·InstallSnapshot 수신은 apply 루프와 같은
// 고루틴이라 이 계약이 자연히 성립한다.
type Writer interface {
	io.Writer
	// Commit은 데이터를 fsync한 뒤 메타를 마지막에 rename해 snapshot을 확정하고,
	// 더 낮은 Index의 옛 snapshot을 정리한다.
	Commit() error
	// Cancel은 미확정 tmp 데이터를 버린다.
	Cancel() error
}

type writer struct {
	store   *Store
	meta    SnapshotMeta
	tmpPath string
	f       *os.File
	bw      *bufio.Writer
	done    bool
}

func newWriter(s *Store, meta SnapshotMeta) (*writer, error) {
	tmpPath := filepath.Join(s.dir, dataName(meta)+tmpSuffix)
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("raftsnap: open data tmp: %w", err)
	}
	return &writer{
		store:   s,
		meta:    meta,
		tmpPath: tmpPath,
		f:       f,
		bw:      bufio.NewWriterSize(f, writerBufferSize),
	}, nil
}

func (w *writer) Write(p []byte) (int, error) {
	n, err := w.bw.Write(p)
	if err != nil {
		return n, fmt.Errorf("raftsnap: write data: %w", err)
	}
	return n, nil
}

// Commit 순서: 데이터 tmp flush+fsync+rename → dir fsync → 메타 tmp write+fsync+rename
// → dir fsync → 옛 snapshot 정리. 메타 rename이 단일 commit 포인트 — 그 전 크래시면
// 데이터는 짝 메타가 없어 고아(다음 Open이 정리), 그 후 크래시면 정상 확정.
func (w *writer) Commit() error {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()

	if w.done {
		return ErrWriterDone
	}
	w.done = true

	if err := w.finalizeData(); err != nil {
		return err
	}
	if err := w.writeMeta(); err != nil {
		return err
	}
	w.store.pruneBelowLocked(w.meta)
	return nil
}

// finalizeData는 데이터 tmp를 정식 이름으로 확정한다. rename 성공 후 dir-fsync가
// 실패하면 정식 데이터 파일은 남지만 짝 메타가 아직 없어 고아 상태 — 다음 Open의
// cleanupOrphans가 정리하므로 crash-safety는 유지된다(discard는 tmp만 지우므로 이
// 단계 이후엔 정리 대상이 아니다).
func (w *writer) finalizeData() error {
	if err := w.bw.Flush(); err != nil {
		w.discard()
		return fmt.Errorf("raftsnap: flush data: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		w.discard()
		return fmt.Errorf("raftsnap: fsync data: %w", err)
	}
	if err := w.f.Close(); err != nil {
		w.f = nil
		_ = os.Remove(w.tmpPath)
		return fmt.Errorf("raftsnap: close data: %w", err)
	}
	w.f = nil

	dataPath := filepath.Join(w.store.dir, dataName(w.meta))
	if err := os.Rename(w.tmpPath, dataPath); err != nil {
		_ = os.Remove(w.tmpPath)
		return fmt.Errorf("raftsnap: rename data: %w", err)
	}
	if err := fsutil.SyncDir(w.store.dir); err != nil {
		return fmt.Errorf("raftsnap: fsync dir: %w", err)
	}
	return nil
}

func (w *writer) writeMeta() error {
	metaTmp := filepath.Join(w.store.dir, metaName(w.meta)+tmpSuffix)
	metaPath := filepath.Join(w.store.dir, metaName(w.meta))

	f, err := os.OpenFile(metaTmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("raftsnap: open meta tmp: %w", err)
	}
	if _, err := f.Write(w.meta.encode()); err != nil {
		f.Close()
		_ = os.Remove(metaTmp)
		return fmt.Errorf("raftsnap: write meta tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		_ = os.Remove(metaTmp)
		return fmt.Errorf("raftsnap: fsync meta tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(metaTmp)
		return fmt.Errorf("raftsnap: close meta tmp: %w", err)
	}
	if err := os.Rename(metaTmp, metaPath); err != nil {
		_ = os.Remove(metaTmp)
		return fmt.Errorf("raftsnap: rename meta: %w", err)
	}
	if err := fsutil.SyncDir(w.store.dir); err != nil {
		return fmt.Errorf("raftsnap: fsync dir: %w", err)
	}
	return nil
}

// Cancel은 미확정 데이터를 버린다. 이미 Commit/Cancel된 Writer면 no-op — 정리 의도가
// 이미 충족됐다(Commit 부분 실패 경로는 자체적으로 tmp를 정리한다).
func (w *writer) Cancel() error {
	if w.done {
		return nil
	}
	w.done = true
	w.discard()
	return nil
}

func (w *writer) discard() {
	if w.f != nil {
		_ = w.f.Close()
		w.f = nil
	}
	_ = os.Remove(w.tmpPath)
}
