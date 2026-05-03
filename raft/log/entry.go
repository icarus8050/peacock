package raftlog

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

type EntryType uint8

const (
	EntryNormal     EntryType = 1
	EntryConfChange EntryType = 2
	EntryNoop       EntryType = 3
)

const (
	lenSize     = 4
	crcSize     = 4
	termSize    = 8
	indexSize   = 8
	typeSize    = 1
	dataLenSize = 4

	headerSize = crcSize + termSize + indexSize + typeSize + dataLenSize
)

var (
	ErrChecksumMismatch = errors.New("raftlog: checksum mismatch")
	ErrIncompleteEntry  = errors.New("raftlog: incomplete entry")
	ErrInvalidEntry     = errors.New("raftlog: invalid entry")
)

type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

// Encode는 entry를 디스크 바이너리 형식으로 직렬화한다.
//
// 레이아웃 (little-endian):
//
//	TotalLen(4) | CRC32(4) | Term(8) | Index(8) | Type(1) | DataLen(4) | Data(var)
func (e *Entry) Encode() []byte {
	totalLen := headerSize + len(e.Data)
	buf := make([]byte, lenSize+totalLen)

	binary.LittleEndian.PutUint32(buf[0:lenSize], uint32(totalLen))

	off := lenSize + crcSize
	binary.LittleEndian.PutUint64(buf[off:off+termSize], e.Term)
	off += termSize
	binary.LittleEndian.PutUint64(buf[off:off+indexSize], e.Index)
	off += indexSize
	buf[off] = byte(e.Type)
	off += typeSize
	binary.LittleEndian.PutUint32(buf[off:off+dataLenSize], uint32(len(e.Data)))
	off += dataLenSize
	copy(buf[off:], e.Data)

	checksum := crc32.ChecksumIEEE(buf[lenSize+crcSize:])
	binary.LittleEndian.PutUint32(buf[lenSize:lenSize+crcSize], checksum)
	return buf
}

func decodeBody(body []byte) (Entry, error) {
	if len(body) < headerSize {
		return Entry{}, ErrIncompleteEntry
	}

	storedCRC := binary.LittleEndian.Uint32(body[0:crcSize])
	payload := body[crcSize:]

	if crc32.ChecksumIEEE(payload) != storedCRC {
		return Entry{}, ErrChecksumMismatch
	}

	off := 0
	term := binary.LittleEndian.Uint64(payload[off : off+termSize])
	off += termSize
	index := binary.LittleEndian.Uint64(payload[off : off+indexSize])
	off += indexSize
	typ := EntryType(payload[off])
	off += typeSize
	dataLen := binary.LittleEndian.Uint32(payload[off : off+dataLenSize])
	off += dataLenSize

	if len(payload) < off+int(dataLen) {
		return Entry{}, ErrIncompleteEntry
	}

	data := make([]byte, dataLen)
	copy(data, payload[off:off+int(dataLen)])

	return Entry{Term: term, Index: index, Type: typ, Data: data}, nil
}
