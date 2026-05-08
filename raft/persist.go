package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"peacock/internal/fsutil"
)

// HardState는 reboot 사이에 영속화되어야 하는 최소 상태(Raft 논문 fig.2의 영속 상태
// 중 log[]를 제외한 부분). log[]는 raft/log 패키지가 책임진다.
type HardState struct {
	Term     uint64
	VotedFor NodeID
}

const (
	hardstateMagic    = "PCHS"
	hardstateVersion  = uint16(1)
	hardstateFileName = "hardstate"
	hardstateTmpName  = "hardstate.tmp"

	// 필드 byte 크기 (고정 부분)
	hardstateMagicSize    = 4 // "PCHS"
	hardstateVersionSize  = 2 // uint16
	hardstateTermSize     = 8 // uint64
	hardstateVotedLenSize = 2 // uint16
	hardstateCRCSize      = 4 // CRC32

	hardstateMinSize = hardstateMagicSize + hardstateVersionSize + hardstateTermSize + hardstateVotedLenSize + hardstateCRCSize
)

// ErrHardstateCorrupt는 hardstate 파일을 디코드하지 못했음을 알린다.
var ErrHardstateCorrupt = errors.New("raft: hardstate corrupt")

// LoadHardState는 dir/hardstate 파일에서 HardState를 읽어온다. 파일이 없으면
// 첫 부팅으로 보고 zero value를 반환한다. 파싱 실패는 ErrHardstateCorrupt.
func LoadHardState(dir string) (HardState, error) {
	path := filepath.Join(dir, hardstateFileName)
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return HardState{}, nil
		}
		return HardState{}, fmt.Errorf("raft: open hardstate: %w", err)
	}
	defer f.Close()

	buf, err := io.ReadAll(f)
	if err != nil {
		return HardState{}, fmt.Errorf("raft: read hardstate: %w", err)
	}
	return decodeHardState(buf)
}

// SaveHardState는 hardstate를 atomic하게 dir에 쓴다(tmp+rename+dir fsync).
// rename의 영속성은 디렉터리 fsync로 보장한다 — wal/manifest와 동일한 패턴.
func SaveHardState(dir string, hs HardState) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("raft: mkdir hardstate dir: %w", err)
	}

	buf := encodeHardState(hs)
	tmpPath := filepath.Join(dir, hardstateTmpName)
	finalPath := filepath.Join(dir, hardstateFileName)

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("raft: open hardstate tmp: %w", err)
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return fmt.Errorf("raft: write hardstate tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("raft: fsync hardstate tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("raft: close hardstate tmp: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("raft: rename hardstate: %w", err)
	}
	if err := fsutil.SyncDir(dir); err != nil {
		return fmt.Errorf("raft: fsync hardstate dir: %w", err)
	}
	return nil
}

// 레이아웃 (little-endian):
//
//	Magic(4)="PCHS" | Version(2) | Term(8) | VotedForLen(2) | VotedFor(var) | CRC32(4)
func encodeHardState(hs HardState) []byte {
	voted := []byte(hs.VotedFor)
	total := hardstateMinSize + len(voted)
	buf := make([]byte, total)

	off := 0
	copy(buf[off:off+hardstateMagicSize], hardstateMagic)
	off += hardstateMagicSize
	binary.LittleEndian.PutUint16(buf[off:off+hardstateVersionSize], hardstateVersion)
	off += hardstateVersionSize
	binary.LittleEndian.PutUint64(buf[off:off+hardstateTermSize], hs.Term)
	off += hardstateTermSize
	binary.LittleEndian.PutUint16(buf[off:off+hardstateVotedLenSize], uint16(len(voted)))
	off += hardstateVotedLenSize
	copy(buf[off:off+len(voted)], voted)
	off += len(voted)
	binary.LittleEndian.PutUint32(buf[off:off+hardstateCRCSize], crc32.ChecksumIEEE(buf[:off]))
	return buf
}

func decodeHardState(data []byte) (HardState, error) {
	if len(data) < hardstateMinSize {
		return HardState{}, fmt.Errorf("%w: too short (%d)", ErrHardstateCorrupt, len(data))
	}
	if string(data[:hardstateMagicSize]) != hardstateMagic {
		return HardState{}, fmt.Errorf("%w: bad magic %q", ErrHardstateCorrupt, data[:hardstateMagicSize])
	}
	off := hardstateMagicSize
	version := binary.LittleEndian.Uint16(data[off : off+hardstateVersionSize])
	if version != hardstateVersion {
		return HardState{}, fmt.Errorf("%w: bad version %d", ErrHardstateCorrupt, version)
	}
	off += hardstateVersionSize
	term := binary.LittleEndian.Uint64(data[off : off+hardstateTermSize])
	off += hardstateTermSize
	votedLen := int(binary.LittleEndian.Uint16(data[off : off+hardstateVotedLenSize]))
	off += hardstateVotedLenSize
	if len(data) < off+votedLen+hardstateCRCSize {
		return HardState{}, fmt.Errorf("%w: votedFor truncated", ErrHardstateCorrupt)
	}
	voted := string(data[off : off+votedLen])
	off += votedLen
	storedCRC := binary.LittleEndian.Uint32(data[off : off+hardstateCRCSize])
	if crc32.ChecksumIEEE(data[:off]) != storedCRC {
		return HardState{}, fmt.Errorf("%w: crc mismatch", ErrHardstateCorrupt)
	}
	return HardState{Term: term, VotedFor: NodeID(voted)}, nil
}
