package raftsnap

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	metaMagic   = "PCSN" // Peacock SNapshot
	metaVersion = uint16(1)

	metaMagicSize    = 4
	metaVersionSize  = 2
	metaReservedSize = 2
	metaIndexSize    = 8
	metaTermSize     = 8
	metaCRCSize      = 4

	metaSize = metaMagicSize + metaVersionSize + metaReservedSize +
		metaIndexSize + metaTermSize + metaCRCSize
)

// ErrSnapshotCorrupt는 메타 파일의 magic/version/CRC가 맞지 않거나, 메타가 가리키는
// 데이터 파일이 없을 때 반환된다. tail truncation 같은 정상 신호가 아니라 fatal —
// snapshot은 통째로 atomic하게 쓰이므로 부분 손상은 곧 버그/디스크 결함이다.
var ErrSnapshotCorrupt = errors.New("raftsnap: snapshot corrupt")

// SnapshotMeta는 한 snapshot의 식별 메타데이터. Index/Term은 snapshot에 포함된
// 마지막 entry(last included index/term)다. 멤버십 configuration은 M3에서 추가한다.
type SnapshotMeta struct {
	Index uint64
	Term  uint64
}

// encode는 메타를 디스크 바이너리 형식으로 직렬화한다.
//
// 레이아웃 (little-endian):
//
//	Magic(4) | Version(2) | Reserved(2) | Index(8) | Term(8) | CRC32(4)
func (m SnapshotMeta) encode() []byte {
	buf := make([]byte, metaSize)

	off := 0
	copy(buf[off:off+metaMagicSize], metaMagic)
	off += metaMagicSize
	binary.LittleEndian.PutUint16(buf[off:off+metaVersionSize], metaVersion)
	off += metaVersionSize
	binary.LittleEndian.PutUint16(buf[off:off+metaReservedSize], 0)
	off += metaReservedSize
	binary.LittleEndian.PutUint64(buf[off:off+metaIndexSize], m.Index)
	off += metaIndexSize
	binary.LittleEndian.PutUint64(buf[off:off+metaTermSize], m.Term)
	off += metaTermSize

	checksum := crc32.ChecksumIEEE(buf[:off])
	binary.LittleEndian.PutUint32(buf[off:off+metaCRCSize], checksum)
	return buf
}

func decodeMeta(buf []byte) (SnapshotMeta, error) {
	if len(buf) != metaSize {
		return SnapshotMeta{}, fmt.Errorf("%w: size %d (want %d)", ErrSnapshotCorrupt, len(buf), metaSize)
	}

	off := 0
	if string(buf[off:off+metaMagicSize]) != metaMagic {
		return SnapshotMeta{}, fmt.Errorf("%w: bad magic", ErrSnapshotCorrupt)
	}
	off += metaMagicSize

	version := binary.LittleEndian.Uint16(buf[off : off+metaVersionSize])
	off += metaVersionSize
	if version != metaVersion {
		return SnapshotMeta{}, fmt.Errorf("%w: unsupported version %d", ErrSnapshotCorrupt, version)
	}

	// reserved는 strict하게 0만 허용한다. 향후 포맷 확장(M3 멤버십 config 등)은
	// reserved 재해석이 아니라 metaVersion 증가로 처리 — version 필드가 forward-compat
	// 레버이고, reserved는 "알 수 없는 플래그 없음"을 보증하는 자리다.
	reserved := binary.LittleEndian.Uint16(buf[off : off+metaReservedSize])
	off += metaReservedSize
	if reserved != 0 {
		return SnapshotMeta{}, fmt.Errorf("%w: reserved=%d", ErrSnapshotCorrupt, reserved)
	}

	index := binary.LittleEndian.Uint64(buf[off : off+metaIndexSize])
	off += metaIndexSize
	term := binary.LittleEndian.Uint64(buf[off : off+metaTermSize])
	off += metaTermSize

	storedCRC := binary.LittleEndian.Uint32(buf[off : off+metaCRCSize])
	if crc32.ChecksumIEEE(buf[:off]) != storedCRC {
		return SnapshotMeta{}, fmt.Errorf("%w: crc mismatch", ErrSnapshotCorrupt)
	}

	return SnapshotMeta{Index: index, Term: term}, nil
}
