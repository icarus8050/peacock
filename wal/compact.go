package wal

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrCheckpointCorrupt는 체크포인트 파일에서 부분 entry 또는 CRC mismatch가
// 감지됐을 때 반환된다. 체크포인트는 atomic write(tmp+rename)로 항상 완전해야
// 하므로 이런 에러는 segment의 정상 tail truncation과 다른 의미 — 디스크 손상이나
// 외부 변조로 간주해 호출자가 fatal하게 처리해야 한다.
var ErrCheckpointCorrupt = errors.New("wal: checkpoint corrupt")

const checkpointSuffix = ".checkpoint"

// checkpointName은 seq까지를 흡수한 체크포인트의 디스크 파일명을 반환한다.
// 형식은 segment 파일과 동일한 패턴(wal-NNNNNNNNNN)을 쓰되, suffix만 다르게
// 두어 디렉터리에서 시각적으로 구별된다.
func checkpointName(seq int64) string {
	return fmt.Sprintf("%s%0*d%s", segmentPrefix, segmentDigits, seq, checkpointSuffix)
}

func checkpointPath(dir string, seq int64) string {
	return filepath.Join(dir, checkpointName(seq))
}

// WriteCheckpoint는 entries를 표준 wal.Entry 바이너리 형식으로 path에 쓴다.
// 정확성은 매니페스트(상위 commit 포인트)가 보장하지만, 최종 경로에 부분 파일이
// 남지 않도록 tmp+rename 패턴을 사용한다 — 매니페스트 쓰기와 동일한 관용구.
// 디렉터리 fsync는 후속 writeManifest 단계에서 함께 처리된다.
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
