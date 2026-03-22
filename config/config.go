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
}

func Load() *Config {
	return &Config{
		Port:            envOrDefault("PORT", "3000"),
		ReadTimeout:     durationOrDefault("READ_TIMEOUT", 5*time.Second),
		WriteTimeout:    durationOrDefault("WRITE_TIMEOUT", 10*time.Second),
		ShutdownTimeout: durationOrDefault("SHUTDOWN_TIMEOUT", 10*time.Second),
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
