package raftlog

import (
	"fmt"
	"path/filepath"
)

const (
	segmentPrefix = "log-"
	segmentSuffix = ".seg"
	segmentDigits = 10
)

func segmentName(seq int64) string {
	return fmt.Sprintf("%s%0*d%s", segmentPrefix, segmentDigits, seq, segmentSuffix)
}

func segmentPath(dir string, seq int64) string {
	return filepath.Join(dir, segmentName(seq))
}
