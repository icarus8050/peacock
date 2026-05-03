package raftlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

const (
	manifestFileName    = "manifest"
	manifestTmpFileName = "manifest.tmp"

	manifestMagic   = "PCRL" // Peacock Raft Log
	manifestVersion = uint16(1)

	manifestMagicSize        = 4
	manifestVersionSize      = 2
	manifestReservedSize     = 2
	manifestGenerationSize   = 8
	manifestSegmentCountSize = 4

	// segmentMeta record: seq(8) | firstIndex(8) | lastIndex(8) | size(8)
	manifestRecordSize = 32

	manifestCRCSize = 4

	manifestHeaderSize = manifestMagicSize + manifestVersionSize + manifestReservedSize +
		manifestGenerationSize + manifestSegmentCountSize
	manifestMinSize = manifestHeaderSize + manifestCRCSize
)

var (
	ErrManifestCorrupt = errors.New("raftlog: manifest corrupt")
	ErrMissingSegment  = errors.New("raftlog: segment referenced by manifest is missing")
)

// segmentMeta는 단일 segment의 인덱스 범위를 표현한다. 매니페스트가 이 값을 들고
// 있으면 truncate/replay 시 디스크 스캔 없이 어느 segment가 영향받는지 결정할 수
// 있다. firstIndex==0, lastIndex==0은 "비어 있음"을 뜻한다 (Raft index는 1부터).
type segmentMeta struct {
	seq        int64
	firstIndex uint64
	lastIndex  uint64
	size       int64
}

type manifest struct {
	generation uint64
	segments   []segmentMeta
}

func (m *manifest) active() *segmentMeta {
	return &m.segments[len(m.segments)-1]
}

// loadOrInitManifest는 dir의 매니페스트를 로드한다. 없으면 빈 활성 segment 1개를
// 가진 매니페스트를 생성해 한 번 영속화한다. 손상된 매니페스트는 자동 복구하지
// 않고 호출자에 그대로 전달한다.
func loadOrInitManifest(dir string) (*manifest, error) {
	m, err := readManifest(dir)
	if err != nil {
		return nil, err
	}
	if m != nil {
		if err := verifyManifestArtifacts(dir, m); err != nil {
			return nil, err
		}
		return m, nil
	}

	next := &manifest{
		generation: 1,
		segments:   []segmentMeta{{seq: 1}},
	}
	if err := writeManifest(dir, next); err != nil {
		return nil, err
	}
	// 활성 segment 파일이 매니페스트보다 늦게 생성되어도 OK — Open 시 openSegment가 만든다.
	return next, nil
}

func verifyManifestArtifacts(dir string, m *manifest) error {
	if len(m.segments) == 0 {
		return fmt.Errorf("%w: empty segment list", ErrManifestCorrupt)
	}
	for _, s := range m.segments {
		path := segmentPath(dir, s.seq)
		if _, err := os.Stat(path); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// 활성 segment(마지막)는 Open이 곧 생성하므로 누락 허용.
				if s.seq == m.active().seq {
					continue
				}
				return fmt.Errorf("%w: seq=%d", ErrMissingSegment, s.seq)
			}
			return fmt.Errorf("raftlog: stat segment: %w", err)
		}
	}
	return nil
}

// encode는 매니페스트를 디스크 바이너리 형식으로 직렬화한다.
//
// 레이아웃 (little-endian):
//
//	Magic(4) | Version(2) | Reserved(2) | Generation(8) | SegmentCount(4)
//	| (Seq(8) | FirstIndex(8) | LastIndex(8) | Size(8))*N | CRC32(4)
func (m *manifest) encode() []byte {
	size := manifestHeaderSize + len(m.segments)*manifestRecordSize + manifestCRCSize
	buf := make([]byte, size)

	off := 0
	copy(buf[off:off+manifestMagicSize], manifestMagic)
	off += manifestMagicSize
	binary.LittleEndian.PutUint16(buf[off:off+manifestVersionSize], manifestVersion)
	off += manifestVersionSize
	binary.LittleEndian.PutUint16(buf[off:off+manifestReservedSize], 0)
	off += manifestReservedSize
	binary.LittleEndian.PutUint64(buf[off:off+manifestGenerationSize], m.generation)
	off += manifestGenerationSize
	binary.LittleEndian.PutUint32(buf[off:off+manifestSegmentCountSize], uint32(len(m.segments)))
	off += manifestSegmentCountSize

	for _, s := range m.segments {
		binary.LittleEndian.PutUint64(buf[off:off+8], uint64(s.seq))
		off += 8
		binary.LittleEndian.PutUint64(buf[off:off+8], s.firstIndex)
		off += 8
		binary.LittleEndian.PutUint64(buf[off:off+8], s.lastIndex)
		off += 8
		binary.LittleEndian.PutUint64(buf[off:off+8], uint64(s.size))
		off += 8
	}

	checksum := crc32.ChecksumIEEE(buf[:off])
	binary.LittleEndian.PutUint32(buf[off:off+manifestCRCSize], checksum)
	return buf
}

func decodeManifest(buf []byte) (*manifest, error) {
	if len(buf) < manifestMinSize {
		return nil, fmt.Errorf("%w: short buffer (%d bytes)", ErrManifestCorrupt, len(buf))
	}

	off := 0
	if string(buf[off:off+manifestMagicSize]) != manifestMagic {
		return nil, fmt.Errorf("%w: bad magic", ErrManifestCorrupt)
	}
	off += manifestMagicSize

	version := binary.LittleEndian.Uint16(buf[off : off+manifestVersionSize])
	off += manifestVersionSize
	if version != manifestVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrManifestCorrupt, version)
	}

	reserved := binary.LittleEndian.Uint16(buf[off : off+manifestReservedSize])
	off += manifestReservedSize
	if reserved != 0 {
		return nil, fmt.Errorf("%w: reserved=%d", ErrManifestCorrupt, reserved)
	}

	generation := binary.LittleEndian.Uint64(buf[off : off+manifestGenerationSize])
	off += manifestGenerationSize

	segmentCount := binary.LittleEndian.Uint32(buf[off : off+manifestSegmentCountSize])
	off += manifestSegmentCountSize

	expectedSize := manifestHeaderSize + int(segmentCount)*manifestRecordSize + manifestCRCSize
	if len(buf) != expectedSize {
		return nil, fmt.Errorf("%w: length mismatch (got %d, want %d)", ErrManifestCorrupt, len(buf), expectedSize)
	}

	segments := make([]segmentMeta, segmentCount)
	for i := range segments {
		segments[i].seq = int64(binary.LittleEndian.Uint64(buf[off : off+8]))
		off += 8
		segments[i].firstIndex = binary.LittleEndian.Uint64(buf[off : off+8])
		off += 8
		segments[i].lastIndex = binary.LittleEndian.Uint64(buf[off : off+8])
		off += 8
		segments[i].size = int64(binary.LittleEndian.Uint64(buf[off : off+8]))
		off += 8
	}

	storedCRC := binary.LittleEndian.Uint32(buf[off : off+manifestCRCSize])
	if crc32.ChecksumIEEE(buf[:off]) != storedCRC {
		return nil, fmt.Errorf("%w: crc mismatch", ErrManifestCorrupt)
	}

	return &manifest{generation: generation, segments: segments}, nil
}

func readManifest(dir string) (*manifest, error) {
	buf, err := os.ReadFile(filepath.Join(dir, manifestFileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("raftlog: read manifest: %w", err)
	}
	return decodeManifest(buf)
}

// writeManifest는 tmp+rename으로 매니페스트를 atomic하게 교체한다.
func writeManifest(dir string, m *manifest) error {
	buf := m.encode()
	tmpPath := filepath.Join(dir, manifestTmpFileName)
	finalPath := filepath.Join(dir, manifestFileName)

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("raftlog: open manifest tmp: %w", err)
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return fmt.Errorf("raftlog: write manifest tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("raftlog: fsync manifest tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("raftlog: close manifest tmp: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("raftlog: rename manifest: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("raftlog: fsync manifest dir: %w", err)
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}
