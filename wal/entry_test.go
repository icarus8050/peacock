package wal

import (
	"testing"
)

func TestEncodeDecodePut(t *testing.T) {
	e := Entry{Op: OpPut, Index: 1, CreatedAt: 1000, Data: []byte("hello:world")}
	got, err := decodeEntry(e.Encode())
	if err != nil {
		t.Fatalf("decodeEntry: %v", err)
	}
	assertEntry(t, e, got)
}

func TestEncodeDecodeDelete(t *testing.T) {
	e := Entry{Op: OpDelete, Index: 2, CreatedAt: 2000, Data: nil}
	got, err := decodeEntry(e.Encode())
	if err != nil {
		t.Fatalf("decodeEntry: %v", err)
	}
	assertEntry(t, e, got)
}

func TestEncodeDecodeLargeData(t *testing.T) {
	d := make([]byte, 1<<20) // 1MB
	for i := range d {
		d[i] = byte(i % 251)
	}
	e := Entry{Op: OpPut, Index: 3, CreatedAt: 3000, Data: d}
	got, err := decodeEntry(e.Encode())
	if err != nil {
		t.Fatalf("decodeEntry: %v", err)
	}
	assertEntry(t, e, got)
}

func TestEncodeDecodeEmptyData(t *testing.T) {
	e := Entry{Op: OpPut, Index: 4, CreatedAt: 4000, Data: nil}
	got, err := decodeEntry(e.Encode())
	if err != nil {
		t.Fatalf("decodeEntry: %v", err)
	}
	if got.Op != e.Op {
		t.Fatalf("op mismatch: got %v, want %v", got.Op, e.Op)
	}
	if len(got.Data) != 0 {
		t.Fatalf("data should be empty, got %q", got.Data)
	}
}

func TestDecodeChecksumMismatch(t *testing.T) {
	e := Entry{Op: OpPut, Index: 5, CreatedAt: 5000, Data: []byte("v")}
	data := e.Encode()

	// Flip a byte in the payload (after TotalLen + CRC).
	data[10] ^= 0xFF

	_, err := decodeEntry(data)
	if err != ErrChecksumMismatch {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}
}

func TestDecodeIncompleteEntry(t *testing.T) {
	e := Entry{Op: OpPut, Index: 6, CreatedAt: 6000, Data: []byte("value")}
	data := e.Encode()

	_, err := decodeEntry(data[:8])
	if err != ErrIncompleteEntry {
		t.Fatalf("expected ErrIncompleteEntry, got %v", err)
	}
}

func assertEntry(t *testing.T, want, got Entry) {
	t.Helper()
	if got.Op != want.Op {
		t.Fatalf("op mismatch: got %v, want %v", got.Op, want.Op)
	}
	if got.Index != want.Index {
		t.Fatalf("index mismatch: got %d, want %d", got.Index, want.Index)
	}
	if got.CreatedAt != want.CreatedAt {
		t.Fatalf("timestamp mismatch: got %d, want %d", got.CreatedAt, want.CreatedAt)
	}
	if string(got.Data) != string(want.Data) {
		t.Fatalf("data mismatch: got %q, want %q", got.Data, want.Data)
	}
}
