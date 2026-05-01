package wal

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

	manifestMagic   = "PCKM"
	manifestVersion = uint16(1)

	manifestMagicSize        = 4
	manifestVersionSize      = 2
	manifestReservedSize     = 2
	manifestGenerationSize   = 8
	manifestSegmentCountSize = 4
	manifestRecordSize       = 8
	manifestCRCSize          = 4

	manifestHeaderSize = manifestMagicSize + manifestVersionSize + manifestReservedSize +
		manifestGenerationSize + manifestSegmentCountSize
	manifestMinSize = manifestHeaderSize + manifestCRCSize
)

// ErrManifestCorrupt is returned when the manifest fails structural or CRC validation.
var ErrManifestCorrupt = errors.New("wal: manifest corrupt")

// ErrMissingSegment is returned when the manifest references a segment file
// that does not exist on disk. Recovery requires manual intervention so we
// refuse to start rather than silently scanning the directory.
var ErrMissingSegment = errors.New("wal: segment referenced by manifest is missing")

type manifest struct {
	generation uint64
	segments   []int64
}

// loadOrInitManifest loads the manifest from dir, verifying that every
// referenced segment still exists. When no manifest is present (fresh
// install or pre-manifest deployment), it is initialized from the on-disk
// segment list and persisted once. Always returns a manifest with at
// least one segment so callers can derive the active seq.
//
// 손상된 매니페스트(ErrManifestCorrupt)는 자동 복구하지 않고 그대로 호출자에
// 전달한다. 운영자가 원인을 확인한 뒤 수동 개입하도록 한 정책이다. 빈 segments를
// 가진 매니페스트는 현 코드가 만들 수 없는 invariant 위반이므로 동일하게 corrupt로
// 분류한다 — 진짜 마이그레이션 대상은 m == nil(매니페스트 부재)뿐이다.
func loadOrInitManifest(dir string) (*manifest, error) {
	m, err := readManifest(dir)
	if err != nil {
		return nil, err
	}

	if m != nil {
		if len(m.segments) == 0 {
			return nil, fmt.Errorf("%w: empty segment list", ErrManifestCorrupt)
		}
		if err := verifySegmentsExist(dir, m.segments); err != nil {
			return nil, err
		}
		return m, nil
	}

	diskSeqs, err := listSegments(dir)
	if err != nil {
		return nil, fmt.Errorf("wal: list segments: %w", err)
	}
	if len(diskSeqs) == 0 {
		diskSeqs = []int64{1}
	}
	next := &manifest{generation: 1, segments: diskSeqs}
	if err := writeManifest(dir, next); err != nil {
		return nil, err
	}
	return next, nil
}

// segmentsForRead returns the segment seqs to replay in order. The manifest
// is the source of truth when present; the directory scan fallback only
// covers the pre-manifest case (read attempted before any wal.Open ever
// ran). Corrupt manifest or segments referenced but missing from disk
// surface as errors — silently scanning around the problem would mask data
// loss. When neither manifest nor segments exist, returns a wrapped
// os.ErrNotExist so callers can treat "no log to read" as a single
// condition.
func segmentsForRead(dir string) ([]int64, error) {
	m, err := readManifest(dir)
	if err != nil {
		return nil, err
	}
	if m != nil {
		if len(m.segments) == 0 {
			return nil, fmt.Errorf("%w: empty segment list", ErrManifestCorrupt)
		}
		if err := verifySegmentsExist(dir, m.segments); err != nil {
			return nil, err
		}
		return m.segments, nil
	}
	seqs, err := listSegments(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("wal: open: %w", os.ErrNotExist)
		}
		return nil, fmt.Errorf("wal: list segments: %w", err)
	}
	if len(seqs) == 0 {
		return nil, fmt.Errorf("wal: open: %w", os.ErrNotExist)
	}
	return seqs, nil
}

// verifySegmentsExist는 매니페스트가 참조하는 모든 세그먼트가 디스크에 존재하는지
// 확인한다. 누락 시 ErrMissingSegment(wrap)로 중단해 매니페스트와 디스크 뷰가
// 일치하는 상태에서만 진행하도록 강제한다.
func verifySegmentsExist(dir string, segments []int64) error {
	for _, seq := range segments {
		if _, err := os.Stat(segmentPath(dir, seq)); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("%w: seq=%d", ErrMissingSegment, seq)
			}
			return fmt.Errorf("wal: stat segment: %w", err)
		}
	}
	return nil
}

// encode serializes the manifest into the on-disk binary format.
//
// Layout (little-endian):
//
//	Magic(4) | Version(2) | Reserved(2) | Generation(8) | SegmentCount(4) | Seq(8)... | CRC32(4)
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

	for _, seq := range m.segments {
		binary.LittleEndian.PutUint64(buf[off:off+manifestRecordSize], uint64(seq))
		off += manifestRecordSize
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

	segments := make([]int64, segmentCount)
	for i := range segments {
		segments[i] = int64(binary.LittleEndian.Uint64(buf[off : off+manifestRecordSize]))
		off += manifestRecordSize
	}

	storedCRC := binary.LittleEndian.Uint32(buf[off : off+manifestCRCSize])
	if crc32.ChecksumIEEE(buf[:off]) != storedCRC {
		return nil, fmt.Errorf("%w: crc mismatch", ErrManifestCorrupt)
	}

	return &manifest{generation: generation, segments: segments}, nil
}

// readManifest loads the manifest from dir. Returns (nil, nil) when the
// manifest file is absent so callers can distinguish "no manifest yet" from
// real I/O errors.
func readManifest(dir string) (*manifest, error) {
	buf, err := os.ReadFile(filepath.Join(dir, manifestFileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("wal: read manifest: %w", err)
	}
	return decodeManifest(buf)
}

// writeManifest atomically replaces the manifest file via tmp+rename so a
// crash mid-update never produces a torn file. Recovery relies on POSIX
// rename atomicity plus a directory fsync to make the rename durable.
func writeManifest(dir string, m *manifest) error {
	buf := m.encode()
	tmpPath := filepath.Join(dir, manifestTmpFileName)
	finalPath := filepath.Join(dir, manifestFileName)

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("wal: open manifest tmp: %w", err)
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return fmt.Errorf("wal: write manifest tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("wal: fsync manifest tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("wal: close manifest tmp: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("wal: rename manifest: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("wal: fsync manifest dir: %w", err)
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
