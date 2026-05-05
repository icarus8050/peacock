package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
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

// compactionPlan은 압축 시작 시 매니페스트 상태의 스냅샷이다. 호출자는 이 계획에
// 따라 락 밖에서 파일을 읽고, 결과 entries를 만들어 CommitCompaction을 호출한다.
type compactionPlan struct {
	sealedSeqs       []int64
	sealedFiles      []string
	checkpointFile   string // 옛 체크포인트 경로 (없으면 빈 문자열)
	newCheckpointSeq int64
}

// CommitCompaction은 압축 결과를 매니페스트에 atomic하게 반영한다.
// 호출자는 호출 전 checkpointPath(dir, checkpointSeq)에 체크포인트 파일을
// 작성·fsync 완료한 상태여야 한다.
//
// removedSeqs는 새 매니페스트에서 제거할 봉인 segment seq들. 매니페스트 갱신이
// 성공하면 옛 segment 파일과 옛 체크포인트(이전 generation의 *.checkpoint)도
// 함께 정리된다 — 정리 실패는 매니페스트 밖이라 정확성 영향이 없으므로 진행을
// 막지 않는다.
//
// 매니페스트 갱신 실패 시 WAL은 복구 불가 상태(closed=true)로 전환되며 호출자는
// 재오픈해야 한다.
func (w *WAL) CommitCompaction(checkpointSeq int64, removedSeqs []int64) error {
	prevCheckpointSeq, err := w.commitCompactionManifest(checkpointSeq, removedSeqs)
	if err != nil {
		return err
	}
	// 매니페스트 밖의 옛 파일 정리는 정확성에 영향이 없으므로 락 밖에서 수행한다 —
	// fsync가 끼는 commit critical section을 짧게 유지해 동시 Append 대기를 줄인다.
	cleanupCompactedFiles(w.dir, removedSeqs, prevCheckpointSeq)
	return nil
}

func (w *WAL) commitCompactionManifest(checkpointSeq int64, removedSeqs []int64) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, ErrClosed
	}
	if err := validateCompactionArgs(w.manifest, checkpointSeq, removedSeqs); err != nil {
		return 0, err
	}

	prevCheckpointSeq := w.manifest.checkpointSeq
	next := postCompactionManifest(w.manifest, checkpointSeq, removedSeqs)
	if err := writeManifest(w.dir, next); err != nil {
		w.closed = true
		return 0, fmt.Errorf("wal: commit compaction manifest: %w", err)
	}
	w.manifest = next
	return prevCheckpointSeq, nil
}

// validateCompactionArgs는 압축 commit 입력의 유효성을 검사한다. 활성 segment를
// 제거 대상에 포함하면 w.seq/w.file과 매니페스트가 어긋나므로 봉인부 seq만 허용한다.
func validateCompactionArgs(m *manifest, checkpointSeq int64, removedSeqs []int64) error {
	if checkpointSeq <= 0 {
		return fmt.Errorf("wal: commit compaction: invalid checkpointSeq=%d", checkpointSeq)
	}
	if len(removedSeqs) == 0 {
		return fmt.Errorf("wal: commit compaction: removedSeqs empty")
	}
	sealed := m.sealedSeqs()
	sealedSet := make(map[int64]bool, len(sealed))
	for _, s := range sealed {
		sealedSet[s] = true
	}
	for _, s := range removedSeqs {
		if !sealedSet[s] {
			return fmt.Errorf("wal: commit compaction: seq=%d is not a sealed segment", s)
		}
	}
	return nil
}

// postCompactionManifest는 prev에서 removedSeqs를 빼고 checkpointSeq를 갱신한
// 새 매니페스트를 만든다. generation은 +1.
func postCompactionManifest(prev *manifest, checkpointSeq int64, removedSeqs []int64) *manifest {
	removed := make(map[int64]bool, len(removedSeqs))
	for _, s := range removedSeqs {
		removed[s] = true
	}
	next := make([]int64, 0, len(prev.segments))
	for _, s := range prev.segments {
		if !removed[s] {
			next = append(next, s)
		}
	}
	return &manifest{
		generation:    prev.generation + 1,
		checkpointSeq: checkpointSeq,
		segments:      next,
	}
}

// cleanupCompactedFiles는 매니페스트 commit 후의 옛 파일 정리를 수행한다.
// ENOENT(이미 지워짐, crash 후 재시도 등)와 그 외 실패 모두 무시 — 매니페스트 밖
// 상태이므로 정확성 영향 없음. logger 도입 시 정리 실패만 별도 logging하도록
// hook할 자리.
//
// invariant: prev checkpointSeq는 그것을 만든 압축 사이클에서 sealed의 max였고
// 그 사이클이 그 seq를 sealed에서 제거했다. 그러므로 다음 압축의 sealed에 다시
// 들어올 수 없고 newCheckpointSeq와 절대 같지 않다. 가드는 두지 않는다.
func cleanupCompactedFiles(dir string, removedSeqs []int64, prevCheckpointSeq int64) {
	for _, s := range removedSeqs {
		os.Remove(segmentPath(dir, s))
	}
	if prevCheckpointSeq > 0 {
		os.Remove(checkpointPath(dir, prevCheckpointSeq))
	}
}

// Compact는 trigger 임계 도달 시 한 번의 압축 사이클을 수행한다. 옛 체크포인트(있다면)와
// 봉인 segment를 읽어 keyOf로 dedupe된 키별 최신 상태를 빌드한 뒤 새 체크포인트로
// atomic하게 교체한다. OpDelete로 끝난 키는 새 체크포인트에서 제외된다.
//
// trigger 미달이거나 다른 goroutine이 이미 압축 중이면 (false, nil)을 반환하고
// 아무 것도 하지 않는다 — 동시 호출은 직렬화된다.
//
// 모든 키가 OpDelete로 끝났으면 빈 entries로 0바이트 체크포인트를 만들고
// 매니페스트에 commit한다 (replay 시 EOF로 즉시 다음 segment로 advance).
func (w *WAL) Compact(trigger int, keyOf func(*Entry) ([]byte, error)) (bool, error) {
	plan, ok := w.beginCompaction(trigger)
	if !ok {
		return false, nil
	}
	defer w.endCompaction()

	entries, err := buildCompactedEntries(plan, keyOf)
	if err != nil {
		return false, err
	}
	if err := WriteCheckpoint(checkpointPath(w.dir, plan.newCheckpointSeq), entries); err != nil {
		return false, fmt.Errorf("wal: compact write checkpoint: %w", err)
	}
	if err := w.CommitCompaction(plan.newCheckpointSeq, plan.sealedSeqs); err != nil {
		return false, err
	}
	return true, nil
}

// buildCompactedEntries는 plan의 옛 체크포인트와 봉인 segment를 keyOf 기준으로
// dedupe해 새 체크포인트에 쓸 entry 목록을 만든다. OpDelete로 끝난 키는 결과에서
// 제외된다. 락 밖에서 호출되며 디스크만 읽는다.
func buildCompactedEntries(plan compactionPlan, keyOf func(*Entry) ([]byte, error)) ([]*Entry, error) {
	state := make(map[string]Entry)
	if err := applyCheckpointFile(state, plan.checkpointFile, keyOf); err != nil {
		return nil, err
	}
	for _, path := range plan.sealedFiles {
		if err := applySealedSegment(state, path, keyOf); err != nil {
			return nil, err
		}
	}
	entries := make([]*Entry, 0, len(state))
	for _, e := range state {
		entries = append(entries, &e)
	}
	return entries, nil
}

// beginCompaction은 락 안에서 매니페스트 스냅샷을 떠 압축 계획을 만든다.
// trigger 미달, 이미 진행 중인 압축이 있거나 closed면 (zero, false).
func (w *WAL) beginCompaction(trigger int) (compactionPlan, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed || w.compacting {
		return compactionPlan{}, false
	}
	sealed := w.manifest.sealedSeqs()
	if len(sealed) < trigger {
		return compactionPlan{}, false
	}
	w.compacting = true
	plan := compactionPlan{
		sealedSeqs:       append([]int64(nil), sealed...),
		newCheckpointSeq: sealed[len(sealed)-1],
	}
	plan.sealedFiles = make([]string, len(sealed))
	for i, s := range sealed {
		plan.sealedFiles[i] = segmentPath(w.dir, s)
	}
	if w.manifest.checkpointSeq > 0 {
		plan.checkpointFile = checkpointPath(w.dir, w.manifest.checkpointSeq)
	}
	return plan, true
}

func (w *WAL) endCompaction() {
	w.mu.Lock()
	w.compacting = false
	w.mu.Unlock()
}

// applyCheckpointFile은 옛 체크포인트의 entries를 state에 적용한다. 체크포인트는
// atomic write로 항상 완전해야 하므로 어떤 손상도 fatal로 보고된다.
func applyCheckpointFile(state map[string]Entry, path string, keyOf func(*Entry) ([]byte, error)) error {
	if path == "" {
		return nil
	}
	return applyFile(state, path, true, keyOf)
}

// applySealedSegment는 봉인 segment의 entries를 state에 적용한다. tail truncation
// (ErrIncompleteEntry/ErrChecksumMismatch)은 정상 로그 끝으로 간주 — kv replay와
// 동일 정책.
func applySealedSegment(state map[string]Entry, path string, keyOf func(*Entry) ([]byte, error)) error {
	return applyFile(state, path, false, keyOf)
}

func applyFile(state map[string]Entry, path string, isCheckpoint bool, keyOf func(*Entry) ([]byte, error)) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("wal: compact open: %w", err)
	}
	defer file.Close()

	r := &Reader{file: file, onCheckpoint: isCheckpoint}
	for {
		entry, err := r.ReadEntry()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if !isCheckpoint && (errors.Is(err, ErrIncompleteEntry) || errors.Is(err, ErrChecksumMismatch)) {
			return nil
		}
		if err != nil {
			return err
		}
		key, err := keyOf(&entry)
		if err != nil {
			return fmt.Errorf("wal: compact keyOf: %w", err)
		}
		switch entry.Op {
		case OpPut:
			state[string(key)] = entry
		case OpDelete:
			delete(state, string(key))
		}
	}
}
