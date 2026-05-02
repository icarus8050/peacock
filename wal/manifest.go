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
	manifestVersion = uint16(2)

	manifestMagicSize         = 4
	manifestVersionSize       = 2
	manifestReservedSize      = 2
	manifestGenerationSize    = 8
	manifestCheckpointSeqSize = 8
	manifestSegmentCountSize  = 4
	manifestRecordSize        = 8
	manifestCRCSize           = 4

	manifestHeaderSize = manifestMagicSize + manifestVersionSize + manifestReservedSize +
		manifestGenerationSize + manifestCheckpointSeqSize + manifestSegmentCountSize
	manifestMinSize = manifestHeaderSize + manifestCRCSize
)

// ErrManifestCorrupt은 매니페스트의 구조 또는 CRC 검증에 실패했을 때 반환된다.
var ErrManifestCorrupt = errors.New("wal: manifest corrupt")

// ErrMissingSegment은 매니페스트가 참조하는 segment 파일이 디스크에 없을 때
// 반환된다. 복구는 수동 개입을 요하므로 디렉터리 스캔으로 우회하지 않고 기동을
// 거부한다.
var ErrMissingSegment = errors.New("wal: segment referenced by manifest is missing")

// ErrMissingCheckpoint은 매니페스트가 체크포인트를 참조(checkpointSeq > 0)하지만
// 디스크에 해당 *.checkpoint 파일이 없을 때 반환된다.
var ErrMissingCheckpoint = errors.New("wal: checkpoint referenced by manifest is missing")

type manifest struct {
	generation    uint64
	checkpointSeq int64
	segments      []int64
}

// loadOrInitManifest는 dir의 매니페스트를 로드하면서 참조 segment가 모두 디스크에
// 존재하는지 검증한다. 매니페스트가 없으면(첫 설치 또는 pre-manifest 데이터)
// 디스크의 segment 목록으로 초기화해 한 번 영속화한다. 항상 최소 1개 segment를
// 가진 매니페스트를 반환하므로 호출자는 활성 seq를 안전하게 도출할 수 있다.
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
		if m.checkpointSeq > 0 {
			if err := verifyCheckpointExists(dir, m.checkpointSeq); err != nil {
				return nil, err
			}
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
	next := &manifest{generation: 1, checkpointSeq: 0, segments: diskSeqs}
	if err := writeManifest(dir, next); err != nil {
		return nil, err
	}
	return next, nil
}

// pathsForRead는 replay할 파일 경로 목록을 순서대로 반환한다. 매니페스트가 있으면
// 그것이 단일 진실원: 체크포인트(있다면)를 먼저, 그 뒤에 segments가 매니페스트
// 순서대로 따라온다. 매니페스트가 없을 때만 디렉터리 스캔으로 fallback하며
// 이는 첫 기동/pre-manifest 데이터만 다룬다. 매니페스트 손상, 체크포인트 누락,
// segment 누락은 모두 에러로 중단한다 — 디렉터리 스캔으로 우회하면 데이터 손실을
// 가릴 수 있다. 매니페스트도 segment도 없으면 wrapped os.ErrNotExist를 반환해
// 호출자가 "읽을 로그 없음"을 단일 조건으로 처리할 수 있게 한다.
func pathsForRead(dir string) ([]string, error) {
	m, err := readManifest(dir)
	if err != nil {
		return nil, err
	}
	if m != nil {
		if len(m.segments) == 0 {
			return nil, fmt.Errorf("%w: empty segment list", ErrManifestCorrupt)
		}
		if m.checkpointSeq > 0 {
			if err := verifyCheckpointExists(dir, m.checkpointSeq); err != nil {
				return nil, err
			}
		}
		if err := verifySegmentsExist(dir, m.segments); err != nil {
			return nil, err
		}
		paths := make([]string, 0, len(m.segments)+1)
		if m.checkpointSeq > 0 {
			paths = append(paths, checkpointPath(dir, m.checkpointSeq))
		}
		for _, s := range m.segments {
			paths = append(paths, segmentPath(dir, s))
		}
		return paths, nil
	}
	// 매니페스트 부재 = 첫 기동 또는 pre-manifest 데이터. 정의상 체크포인트는 매니페스트
	// commit 후에만 의미를 가지므로 매니페스트 없이 디스크에 *.checkpoint 파일이 떠
	// 있더라도 그것은 commit 안 된 고아 — 무시한다. listSegments는 .log suffix만
	// 보므로 checkpoint 파일은 자연스럽게 제외된다.
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
	paths := make([]string, len(seqs))
	for i, s := range seqs {
		paths[i] = segmentPath(dir, s)
	}
	return paths, nil
}

// verifyCheckpointExists는 매니페스트가 참조하는 체크포인트 파일이 디스크에
// 존재하는지 확인한다. 누락 시 ErrMissingCheckpoint(wrap)로 중단.
func verifyCheckpointExists(dir string, seq int64) error {
	if _, err := os.Stat(checkpointPath(dir, seq)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: seq=%d", ErrMissingCheckpoint, seq)
		}
		return fmt.Errorf("wal: stat checkpoint: %w", err)
	}
	return nil
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

// encode는 매니페스트를 디스크 바이너리 형식으로 직렬화한다.
//
// 레이아웃 (little-endian):
//
//	Magic(4) | Version(2) | Reserved(2) | Generation(8) | CheckpointSeq(8) | SegmentCount(4) | Seq(8)... | CRC32(4)
//
// CheckpointSeq=0이면 체크포인트 없음. >0이면 wal-NNNNNNNNNN.checkpoint 파일이
// 매니페스트가 가리키는 segments보다 먼저 replay된다.
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
	binary.LittleEndian.PutUint64(buf[off:off+manifestCheckpointSeqSize], uint64(m.checkpointSeq))
	off += manifestCheckpointSeqSize
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

	checkpointSeq := int64(binary.LittleEndian.Uint64(buf[off : off+manifestCheckpointSeqSize]))
	off += manifestCheckpointSeqSize
	if checkpointSeq < 0 {
		return nil, fmt.Errorf("%w: negative checkpointSeq=%d", ErrManifestCorrupt, checkpointSeq)
	}

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

	return &manifest{generation: generation, checkpointSeq: checkpointSeq, segments: segments}, nil
}

// readManifest는 dir의 매니페스트를 로드한다. 매니페스트 파일이 없으면 (nil, nil)을
// 반환해 호출자가 "매니페스트 없음"을 진짜 I/O 에러와 구별할 수 있게 한다.
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

// writeManifest는 tmp+rename으로 매니페스트 파일을 atomic하게 교체해 갱신 도중
// 크래시가 발생해도 torn file이 남지 않게 한다. 복구는 POSIX rename의 원자성과
// 디렉터리 fsync로 rename의 영속성을 보장하는 데 의존한다.
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
