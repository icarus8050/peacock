package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port                 string
	ReadTimeout          time.Duration
	WriteTimeout         time.Duration
	ShutdownTimeout      time.Duration
	KVDir                string
	KVSyncInterval       time.Duration
	WALMaxSegmentSize    int64
	KVCompactionTrigger  int
	KVCompactionInterval time.Duration
}

func Load() *Config {
	return &Config{
		Port:                 envOrDefault("PORT", "3000"),
		ReadTimeout:          durationOrDefault("READ_TIMEOUT", 5*time.Second),
		WriteTimeout:         durationOrDefault("WRITE_TIMEOUT", 10*time.Second),
		ShutdownTimeout:      durationOrDefault("SHUTDOWN_TIMEOUT", 10*time.Second),
		KVDir:                envOrDefault("KV_DIR", "data"),
		KVSyncInterval:       millisOrDefault("KV_SYNC_INTERVAL_MS", 0),        // 0 = kv 패키지 기본값 위임
		WALMaxSegmentSize:    megabytesOrDefault("WAL_MAX_SEGMENT_SIZE_MB", 0), // 0 = kv/wal 기본값 위임
		KVCompactionTrigger:  intOrDefault("KV_COMPACTION_TRIGGER", 0),         // 0 = kv 기본값 위임
		KVCompactionInterval: millisOrDefault("KV_COMPACTION_INTERVAL_MS", 0),  // 0 = kv 기본값 위임
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func durationOrDefault(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	sec, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return time.Duration(sec) * time.Second
}

func millisOrDefault(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	ms, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return time.Duration(ms) * time.Millisecond
}

func megabytesOrDefault(key string, fallback int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	mb, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fallback
	}
	return mb * 1024 * 1024
}

func intOrDefault(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}
