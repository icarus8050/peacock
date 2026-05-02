package wal

import (
	"encoding/binary"
	"fmt"
	"os"
)

// newReader는 특정 segment 파일을 path로 직접 들여다봐야 할 때 쓰는 테스트 헬퍼다.
func newReader(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("wal: open: %w", err)
	}
	return &Reader{file: file}, nil
}

// decodeEntry는 디스크상의 전체 entry(TotalLen prefix + body)를 디코드한다.
// Encode 검증용 라운드트립 테스트 헬퍼이며, 프로덕션 read는 Reader.readCurrent를
// 통해 decodeBody를 사용한다.
func decodeEntry(data []byte) (Entry, error) {
	if len(data) < lenSize+headerSize {
		return Entry{}, ErrIncompleteEntry
	}

	totalLen := int(binary.LittleEndian.Uint32(data[0:lenSize]))
	if len(data) < lenSize+totalLen {
		return Entry{}, ErrIncompleteEntry
	}

	return decodeBody(data[lenSize : lenSize+totalLen])
}
