package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

type EntryType uint8
type TimeStamp int64

const (
	OpPut    EntryType = 0x01
	OpDelete EntryType = 0x02
)

const (
	lenSize       = 4
	crcSize       = 4
	opSize        = 1
	indexSize     = 8
	timeStampSize = 8
	dataLenSize   = 4

	headerSize = crcSize + opSize + indexSize + timeStampSize + dataLenSize
)

var (
	ErrChecksumMismatch = errors.New("wal: checksum mismatch")
	ErrIncompleteEntry  = errors.New("wal: incomplete entry")
)

type Entry struct {
	Op        EntryType
	Index     int64
	CreatedAt TimeStamp
	Data      []byte
}

// Encode serializes the entry into the on-disk binary format.
//
// Layout (little-endian):
//
//	TotalLen(4) | CRC32(4) | Op(1) | Index(8) | TimeStamp(8) | DataLen(4) | Data(var)
func (e *Entry) Encode() []byte {
	totalLen := headerSize + len(e.Data)
	buf := make([]byte, lenSize+totalLen)

	binary.LittleEndian.PutUint32(buf[0:lenSize], uint32(totalLen))

	off := lenSize + crcSize
	buf[off] = byte(e.Op)
	off += opSize
	binary.LittleEndian.PutUint64(buf[off:off+indexSize], uint64(e.Index))
	off += indexSize
	binary.LittleEndian.PutUint64(buf[off:off+timeStampSize], uint64(e.CreatedAt))
	off += timeStampSize
	binary.LittleEndian.PutUint32(buf[off:off+dataLenSize], uint32(len(e.Data)))
	off += dataLenSize
	copy(buf[off:], e.Data)

	checksum := crc32.ChecksumIEEE(buf[lenSize+crcSize:])
	binary.LittleEndian.PutUint32(buf[lenSize:lenSize+crcSize], checksum)

	return buf
}

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

// decodeBody decodes the entry body (CRC + payload), without the TotalLen prefix.
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
	op := EntryType(payload[off])
	off += opSize
	index := int64(binary.LittleEndian.Uint64(payload[off : off+indexSize]))
	off += indexSize
	ts := TimeStamp(binary.LittleEndian.Uint64(payload[off : off+timeStampSize]))
	off += timeStampSize
	dataLen := binary.LittleEndian.Uint32(payload[off : off+dataLenSize])
	off += dataLenSize

	if len(payload) < off+int(dataLen) {
		return Entry{}, ErrIncompleteEntry
	}

	entryData := make([]byte, dataLen)
	copy(entryData, payload[off:off+int(dataLen)])

	return Entry{Op: op, Index: index, CreatedAt: ts, Data: entryData}, nil
}
