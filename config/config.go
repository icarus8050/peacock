package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
	KVDir           string
	KVSyncInterval  time.Duration
}

func Load() *Config {
	return &Config{
		Port:            envOrDefault("PORT", "3000"),
		ReadTimeout:     durationOrDefault("READ_TIMEOUT", 5*time.Second),
		WriteTimeout:    durationOrDefault("WRITE_TIMEOUT", 10*time.Second),
		ShutdownTimeout: durationOrDefault("SHUTDOWN_TIMEOUT", 10*time.Second),
		KVDir:           envOrDefault("KV_DIR", "data"),
		KVSyncInterval:  millisOrDefault("KV_SYNC_INTERVAL_MS", 0), // 0 = kv 패키지 기본값 위임
	}
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
