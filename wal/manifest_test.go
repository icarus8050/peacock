package wal

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestManifestEncodeRoundTrip(t *testing.T) {
	cases := []struct {
		name     string
		segments []int64
	}{
		{"empty", nil},
		{"single", []int64{1}},
		{"many", []int64{1, 2, 5, 100, 1234567890}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			in := &manifest{generation: 42, segments: c.segments}
			out, err := decodeManifest(in.encode())
			if err != nil {
				t.Fatalf("decodeManifest: %v", err)
			}
			if out.generation != in.generation {
				t.Fatalf("generation: got %d, want %d", out.generation, in.generation)
			}
			if !slices.Equal(out.segments, c.segments) {
				t.Fatalf("segments: got %v, want %v", out.segments, c.segments)
			}
		})
	}
}

func TestManifestDecodeDetectsCorruption(t *testing.T) {
	in := &manifest{generation: 1, segments: []int64{1, 2, 3}}
	buf := in.encode()

	for i := range buf {
		corrupt := append([]byte(nil), buf...)
		corrupt[i] ^= 0xFF
		if _, err := decodeManifest(corrupt); !errors.Is(err, ErrManifestCorrupt) {
			t.Fatalf("byte %d flip: expected ErrManifestCorrupt, got %v", i, err)
		}
	}
}

func TestManifestDecodeShortBuffer(t *testing.T) {
	in := &manifest{generation: 1, segments: []int64{1}}
	buf := in.encode()
	for n := 0; n < len(buf); n++ {
		if _, err := decodeManifest(buf[:n]); !errors.Is(err, ErrManifestCorrupt) {
			t.Fatalf("len=%d: expected ErrManifestCorrupt, got %v", n, err)
		}
	}
}

func TestManifestDecodeBadMagic(t *testing.T) {
	in := &manifest{generation: 1}
	buf := in.encode()
	copy(buf[:manifestMagicSize], []byte("XXXX"))
	if _, err := decodeManifest(buf); !errors.Is(err, ErrManifestCorrupt) {
		t.Fatalf("expected ErrManifestCorrupt, got %v", err)
	}
}

func TestManifestAtomicWrite(t *testing.T) {
	dir := t.TempDir()

	m1 := &manifest{generation: 1, segments: []int64{1, 2}}
	if err := writeManifest(dir, m1); err != nil {
		t.Fatalf("writeManifest: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, manifestTmpFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tmp file should not exist after write: %v", err)
	}
	got, err := readManifest(dir)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if got.generation != 1 || !slices.Equal(got.segments, []int64{1, 2}) {
		t.Fatalf("unexpected manifest after first write: %+v", got)
	}

	m2 := &manifest{generation: 2, segments: []int64{1, 2, 3}}
	if err := writeManifest(dir, m2); err != nil {
		t.Fatalf("writeManifest 2: %v", err)
	}
	got, err = readManifest(dir)
	if err != nil {
		t.Fatalf("readManifest 2: %v", err)
	}
	if got.generation != 2 || !slices.Equal(got.segments, []int64{1, 2, 3}) {
		t.Fatalf("unexpected manifest after second write: %+v", got)
	}
}

func TestReadManifestMissing(t *testing.T) {
	dir := t.TempDir()
	got, err := readManifest(dir)
	if err != nil {
		t.Fatalf("readManifest on empty dir: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

