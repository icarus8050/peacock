package wal

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
)

const checkpointSuffix = ".checkpoint"

// checkpointName returns the on-disk file name for a checkpoint covering up to
// seq. 형식은 segment 파일과 동일한 패턴(wal-NNNNNNNNNN)을 쓰되, suffix만 다르게
// 두어 디렉터리에서 시각적으로 구별된다.
func checkpointName(seq int64) string {
	return fmt.Sprintf("%s%0*d%s", segmentPrefix, segmentDigits, seq, checkpointSuffix)
}

func checkpointPath(dir string, seq int64) string {
	return filepath.Join(dir, checkpointName(seq))
}

// WriteCheckpoint writes the entries to path using the standard wal.Entry
// binary format. 정확성은 매니페스트(상위 commit 포인트)가 보장하지만, 최종
// 경로에 부분 파일이 남지 않도록 tmp+rename 패턴을 사용한다 — 매니페스트 쓰기와
// 동일한 관용구. 디렉터리 fsync는 후속 writeManifest 단계에서 함께 처리된다.
//
// entry 수가 많을 때 syscall 폭증을 막기 위해 bufio.Writer로 사용자 공간에서
// 배치한다 — WAL.Append와 같은 패턴.
func WriteCheckpoint(path string, entries []*Entry) error {
	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("wal: create checkpoint tmp: %w", err)
	}
	bw := bufio.NewWriterSize(f, defaultBufferSize)
	for _, e := range entries {
		if _, err := bw.Write(e.Encode()); err != nil {
			f.Close()
			return fmt.Errorf("wal: write checkpoint tmp: %w", err)
		}
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return fmt.Errorf("wal: flush checkpoint tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("wal: fsync checkpoint tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("wal: close checkpoint tmp: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("wal: rename checkpoint: %w", err)
	}
	return nil
}
