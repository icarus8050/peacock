package raft

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadHardState_MissingFile(t *testing.T) {
	dir := t.TempDir()
	hs, err := LoadHardState(dir)
	if err != nil {
		t.Fatalf("expected nil err on missing file, got %v", err)
	}
	if hs != (HardState{}) {
		t.Fatalf("expected zero HardState, got %+v", hs)
	}
}

func TestSaveLoadHardState_Roundtrip(t *testing.T) {
	dir := t.TempDir()
	cases := []HardState{
		{Term: 0, VotedFor: ""},
		{Term: 1, VotedFor: ""},
		{Term: 42, VotedFor: "node-2"},
		{Term: 1 << 40, VotedFor: "n-with-some-longer-id"},
	}
	for _, want := range cases {
		if err := SaveHardState(dir, want); err != nil {
			t.Fatalf("save %+v: %v", want, err)
		}
		got, err := LoadHardState(dir)
		if err != nil {
			t.Fatalf("load %+v: %v", want, err)
		}
		if got != want {
			t.Fatalf("roundtrip: want %+v got %+v", want, got)
		}
	}
}

func TestSaveHardState_OverwritesPrevious(t *testing.T) {
	dir := t.TempDir()
	if err := SaveHardState(dir, HardState{Term: 1, VotedFor: "n1"}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := SaveHardState(dir, HardState{Term: 2, VotedFor: "n2"}); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := LoadHardState(dir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Term != 2 || got.VotedFor != "n2" {
		t.Fatalf("want term=2 voted=n2, got %+v", got)
	}
}

func TestLoadHardState_BadMagic(t *testing.T) {
	dir := t.TempDir()
	bogus := append([]byte("XXXX"), make([]byte, hardstateMinSize)...)
	if err := os.WriteFile(filepath.Join(dir, hardstateFileName), bogus, 0644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	_, err := LoadHardState(dir)
	if !errors.Is(err, ErrHardstateCorrupt) {
		t.Fatalf("want ErrHardstateCorrupt, got %v", err)
	}
}

func TestLoadHardState_TruncatedBody(t *testing.T) {
	dir := t.TempDir()
	full := encodeHardState(HardState{Term: 7, VotedFor: "node-7"})
	if err := os.WriteFile(filepath.Join(dir, hardstateFileName), full[:len(full)-2], 0644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	_, err := LoadHardState(dir)
	if !errors.Is(err, ErrHardstateCorrupt) {
		t.Fatalf("want ErrHardstateCorrupt, got %v", err)
	}
}

func TestLoadHardState_CRCMismatch(t *testing.T) {
	dir := t.TempDir()
	buf := encodeHardState(HardState{Term: 5, VotedFor: "x"})
	buf[len(buf)-1] ^= 0xFF
	if err := os.WriteFile(filepath.Join(dir, hardstateFileName), buf, 0644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	_, err := LoadHardState(dir)
	if !errors.Is(err, ErrHardstateCorrupt) {
		t.Fatalf("want ErrHardstateCorrupt, got %v", err)
	}
}

func TestLoadHardState_BadVersion(t *testing.T) {
	dir := t.TempDir()
	buf := encodeHardState(HardState{Term: 5, VotedFor: ""})
	binary.LittleEndian.PutUint16(buf[4:6], 0xFFFF)
	// CRC를 새로 계산해 박아 version 검증 분기까지 도달시킨다.
	crcOff := len(buf) - 4
	binary.LittleEndian.PutUint32(buf[crcOff:], crc32.ChecksumIEEE(buf[:crcOff]))
	if err := os.WriteFile(filepath.Join(dir, hardstateFileName), buf, 0644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	_, err := LoadHardState(dir)
	if !errors.Is(err, ErrHardstateCorrupt) {
		t.Fatalf("want ErrHardstateCorrupt, got %v", err)
	}
}
