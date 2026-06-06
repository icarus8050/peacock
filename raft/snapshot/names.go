package raftsnap

import (
	"fmt"
	"os"
	"strings"
)

// 파일명 규약: <prefix>I<index>-T<term>. Index/Term을 20자리 zero-pad해 사전식 정렬이
// Index 정렬과 일치하게 하고, 사람이 디렉터리만 봐도 어느 지점의 snapshot인지 읽을 수
// 있게 한다(메타 내용은 바이너리라 직접 안 읽힘). Index/Term은 메타 파일 안에도
// 동일하게 기록되며, 그쪽이 CRC로 보호되는 source of truth다.

func idPart(m SnapshotMeta) string {
	return fmt.Sprintf("I%020d-T%020d", m.Index, m.Term)
}

func metaName(m SnapshotMeta) string {
	return metaPrefix + idPart(m)
}

func dataName(m SnapshotMeta) string {
	return dataPrefix + idPart(m)
}

// parseMetaName은 메타 파일명에서 Index/Term을 복원한다. 규약에 안 맞으면 ok=false.
func parseMetaName(name string) (SnapshotMeta, bool) {
	id, found := strings.CutPrefix(name, metaPrefix)
	if !found {
		return SnapshotMeta{}, false
	}
	var index, term uint64
	if _, err := fmt.Sscanf(id, "I%020d-T%020d", &index, &term); err != nil {
		return SnapshotMeta{}, false
	}
	return SnapshotMeta{Index: index, Term: term}, true
}

// metaFileNames는 dir의 commit된 메타 파일명들을 반환한다(.tmp 제외).
func metaFileNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("raftsnap: readdir: %w", err)
	}
	var names []string
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, metaPrefix) && !strings.HasSuffix(name, tmpSuffix) {
			names = append(names, name)
		}
	}
	return names, nil
}
