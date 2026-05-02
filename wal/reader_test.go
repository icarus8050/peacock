package wal

import (
	"errors"
	"io"
	"os"
	"testing"
)

func TestOpenReaderNotExist(t *testing.T) {
	dir := t.TempDir()

	_, err := OpenReader(DefaultOptions(dir))
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected wrapped os.ErrNotExist, got %v", err)
	}
}

func TestReadEmptyFile(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	path := w.path()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := newReader(path)
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer r.Close()

	if _, err := r.ReadEntry(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestReadCorruptEntry(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	e1 := Entry{Op: OpPut, Index: 1, CreatedAt: 1000, Data: []byte("good")}
	e2 := Entry{Op: OpPut, Index: 2, CreatedAt: 2000, Data: []byte("bad")}
	if err := w.Append(&e1); err != nil {
		t.Fatalf("Append e1: %v", err)
	}
	if err := w.Append(&e2); err != nil {
		t.Fatalf("Append e2: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	path := w.path()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 두 번째 entry의 첫 payload 바이트(Op)를 뒤집어 CRC mismatch 유도.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	secondPayloadStart := len(e1.Encode()) + lenSize + crcSize
	data[secondPayloadStart] ^= 0xFF
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	r, err := newReader(path)
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer r.Close()

	got, err := r.ReadEntry()
	if err != nil {
		t.Fatalf("first ReadEntry: %v", err)
	}
	if string(got.Data) != "good" {
		t.Fatalf("expected data 'good', got %q", got.Data)
	}

	if _, err := r.ReadEntry(); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}
}

func TestReadPartialWrite(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	e := Entry{Op: OpPut, Index: 1, CreatedAt: 1000, Data: []byte("full")}
	if err := w.Append(&e); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	path := w.path()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 부분 TotalLen(3바이트)만 append해 크래시 상황을 시뮬레이션.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write([]byte{0x10, 0x00, 0x00}); err != nil {
		t.Fatalf("Write partial: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close partial: %v", err)
	}

	r, err := newReader(path)
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer r.Close()

	got, err := r.ReadEntry()
	if err != nil {
		t.Fatalf("ReadEntry: %v", err)
	}
	if string(got.Data) != "full" {
		t.Fatalf("expected data 'full', got %q", got.Data)
	}

	if _, err := r.ReadEntry(); !errors.Is(err, ErrIncompleteEntry) {
		t.Fatalf("expected ErrIncompleteEntry, got %v", err)
	}
}

func TestRecoveryTruncation(t *testing.T) {
	dir := t.TempDir()
	path := segmentPath(dir, 1)

	w, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	entries := []Entry{
		{Op: OpPut, Index: 1, CreatedAt: 1000, Data: []byte("1")},
		{Op: OpPut, Index: 2, CreatedAt: 2000, Data: []byte("2")},
		{Op: OpPut, Index: 3, CreatedAt: 3000, Data: []byte("3")},
	}
	for i := range entries {
		if err := w.Append(&entries[i]); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 세 번째 entry의 첫 payload 바이트(Op)를 뒤집어 CRC mismatch 유도.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	thirdPayloadStart := len(entries[0].Encode()) + len(entries[1].Encode()) + lenSize + crcSize
	data[thirdPayloadStart] ^= 0xFF
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	r, err := newReader(path)
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	var validCount int
	for {
		if _, err := r.ReadEntry(); err != nil {
			break
		}
		validCount++
	}
	r.Close()

	if validCount != 2 {
		t.Fatalf("expected 2 valid entries, got %d", validCount)
	}

	truncateAt := int64(len(entries[0].Encode()) + len(entries[1].Encode()))
	if err := os.Truncate(path, truncateAt); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	w, err = Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	newEntry := Entry{Op: OpPut, Index: 3, CreatedAt: 3000, Data: []byte("d")}
	if err := w.Append(&newEntry); err != nil {
		t.Fatalf("Append new: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync new: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close new: %v", err)
	}

	r, err = newReader(path)
	if err != nil {
		t.Fatalf("newReader after replay: %v", err)
	}
	defer r.Close()

	expectedData := []string{"1", "2", "d"}
	for i, wantData := range expectedData {
		got, err := r.ReadEntry()
		if err != nil {
			t.Fatalf("ReadEntry[%d]: %v", i, err)
		}
		if string(got.Data) != wantData {
			t.Fatalf("entry[%d] data: got %q, want %q", i, got.Data, wantData)
		}
	}

	if _, err := r.ReadEntry(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF after all entries, got %v", err)
	}
}
