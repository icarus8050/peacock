package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	segmentPrefix = "wal-"
	segmentSuffix = ".log"
	segmentDigits = 10
)

func listSegments(dir string) ([]int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var seqs []int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, segmentPrefix) || !strings.HasSuffix(name, segmentSuffix) {
			continue
		}
		numStr := strings.TrimSuffix(strings.TrimPrefix(name, segmentPrefix), segmentSuffix)
		n, err := strconv.ParseInt(numStr, 10, 64)
		if err != nil {
			continue
		}
		seqs = append(seqs, n)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs, nil
}

func openSegment(dir string, seq int64) (*os.File, int64, error) {
	file, err := os.OpenFile(segmentPath(dir, seq), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, 0, fmt.Errorf("wal: open segment: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, 0, fmt.Errorf("wal: stat segment: %w", err)
	}
	return file, info.Size(), nil
}

func segmentPath(dir string, seq int64) string {
	return filepath.Join(dir, segmentName(seq))
}

func segmentName(seq int64) string {
	return fmt.Sprintf("%s%0*d%s", segmentPrefix, segmentDigits, seq, segmentSuffix)
}
