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

// activeSeq는 현재 활성 segment seq를 반환한다 — invariant상 매니페스트 마지막 원소.
func (m *manifest) activeSeq() int64 {
	return m.segments[len(m.segments)-1]
}

// sealedSeqs는 봉인된(활성 제외) segment seq 슬라이스를 반환한다.
func (m *manifest) sealedSeqs() []int64 {
	return m.segments[:len(m.segments)-1]
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
		if err := verifyManifestArtifacts(dir, m); err != nil {
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

// pathsForRead는 replay에 쓸 체크포인트(있으면)와 segment 경로 목록을 반환한다.
// 매니페스트가 있으면 그것이 단일 진실원: checkpointSeq > 0이면 그 체크포인트가
// 가장 먼저 replay되고 이어서 segments가 매니페스트 순서대로 따라온다.
// 매니페스트가 없을 때만 디렉터리 스캔으로 fallback하며 이는 첫 기동/pre-manifest
// 데이터만 다룬다. 매니페스트 손상, 체크포인트 누락, segment 누락은 모두 에러로
// 중단한다 — 디렉터리 스캔으로 우회하면 데이터 손실을 가릴 수 있다. 매니페스트도
// segment도 없으면 wrapped os.ErrNotExist를 반환해 호출자가 "읽을 로그 없음"을
// 단일 조건으로 처리할 수 있게 한다.
// readPaths는 replay에 쓸 파일 경로 묶음 — 체크포인트(있으면)와 segment들. 둘은
// 항상 같이 흐른다(체크포인트 먼저 replay → segments 순서대로). checkpoint가 빈
// 문자열이면 체크포인트 없음.
type readPaths struct {
	checkpoint string
	segments   []string
}

func pathsForRead(dir string) (readPaths, error) {
	m, err := readManifest(dir)
	if err != nil {
		return readPaths{}, err
	}
	if m == nil {
		return pathsFromDiskScan(dir)
	}
	if err := verifyManifestArtifacts(dir, m); err != nil {
		return readPaths{}, err
	}
	paths := readPaths{segments: segmentsToPaths(dir, m.segments)}
	if m.checkpointSeq > 0 {
		paths.checkpoint = checkpointPath(dir, m.checkpointSeq)
	}
	return paths, nil
}

// pathsFromDiskScan은 매니페스트가 없을 때(첫 기동 또는 pre-manifest 데이터) 디스크의
// segment 파일 목록으로 fallback한다. 정의상 체크포인트는 매니페스트 commit 후에만
// 의미를 가지므로 매니페스트 없이 디스크에 *.checkpoint 파일이 떠 있더라도 그것은
// commit 안 된 고아 — 무시한다. listSegments는 .log suffix만 보므로 checkpoint
// 파일은 자연스럽게 제외된다. segment도 없으면 wrapped os.ErrNotExist.
func pathsFromDiskScan(dir string) (readPaths, error) {
	seqs, err := listSegments(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return readPaths{}, fmt.Errorf("wal: open: %w", os.ErrNotExist)
		}
		return readPaths{}, fmt.Errorf("wal: list segments: %w", err)
	}
	if len(seqs) == 0 {
		return readPaths{}, fmt.Errorf("wal: open: %w", os.ErrNotExist)
	}
	return readPaths{segments: segmentsToPaths(dir, seqs)}, nil
}

// segmentsToPaths는 segment seq 목록을 디스크 경로 목록으로 매핑한다.
func segmentsToPaths(dir string, seqs []int64) []string {
	paths := make([]string, len(seqs))
	for i, s := range seqs {
		paths[i] = segmentPath(dir, s)
	}
	return paths
}

// verifyManifestArtifacts는 매니페스트의 invariant와 참조 파일 존재를 검증한다.
// 같은 정책을 read/write 경로 양쪽이 공유하도록 한 군데로 묶어, 한쪽만 갱신되어
// 정책 비대칭이 생기는 것을 막는다.
func verifyManifestArtifacts(dir string, m *manifest) error {
	if len(m.segments) == 0 {
		return fmt.Errorf("%w: empty segment list", ErrManifestCorrupt)
	}
	if m.checkpointSeq > 0 {
		if err := verifyCheckpointExists(dir, m.checkpointSeq); err != nil {
			return err
		}
	}
	return verifySegmentsExist(dir, m.segments)
}

// verifyCheckpointExists는 매니페스트가 참조하는 체크포인트 파일이 디스크에
// 존재하는지 확인한다. 누락 시 ErrMissingCheckpoint(wrap)로 중단.
func verifyCheckpointExists(dir string, seq int64) error {
	_, err := os.Stat(checkpointPath(dir, seq))
	if err == nil {
		return nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: seq=%d", ErrMissingCheckpoint, seq)
	}
	return fmt.Errorf("wal: stat checkpoint: %w", err)
}

// verifySegmentsExist는 매니페스트가 참조하는 모든 세그먼트가 디스크에 존재하는지
// 확인한다. 누락 시 ErrMissingSegment(wrap)로 중단해 매니페스트와 디스크 뷰가
// 일치하는 상태에서만 진행하도록 강제한다.
func verifySegmentsExist(dir string, segments []int64) error {
	for _, seq := range segments {
		if err := requireSegmentExists(dir, seq); err != nil {
			return err
		}
	}
	return nil
}

// requireSegmentExists는 단일 segment 파일이 존재하는지 확인하고 부재/접근 오류를
// 도메인 에러(ErrMissingSegment 또는 wrap)로 분류한다.
func requireSegmentExists(dir string, seq int64) error {
	_, err := os.Stat(segmentPath(dir, seq))
	if err == nil {
		return nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: seq=%d", ErrMissingSegment, seq)
	}
	return fmt.Errorf("wal: stat segment: %w", err)
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
