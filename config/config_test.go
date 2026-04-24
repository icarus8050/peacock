package config

import "testing"

func TestLoadWALMaxSegmentSizeDefault(t *testing.T) {
	t.Setenv("WAL_MAX_SEGMENT_SIZE_MB", "")

	cfg := Load()
	if cfg.WALMaxSegmentSize != 0 {
		t.Fatalf("unset: got %d, want 0 (delegate to kv/wal default)", cfg.WALMaxSegmentSize)
	}
}

func TestLoadWALMaxSegmentSizeParses(t *testing.T) {
	t.Setenv("WAL_MAX_SEGMENT_SIZE_MB", "32")

	cfg := Load()
	want := int64(32) * 1024 * 1024
	if cfg.WALMaxSegmentSize != want {
		t.Fatalf("got %d, want %d", cfg.WALMaxSegmentSize, want)
	}
}

func TestLoadWALMaxSegmentSizeInvalid(t *testing.T) {
	t.Setenv("WAL_MAX_SEGMENT_SIZE_MB", "not-a-number")

	cfg := Load()
	if cfg.WALMaxSegmentSize != 0 {
		t.Fatalf("invalid: got %d, want fallback 0", cfg.WALMaxSegmentSize)
	}
}
