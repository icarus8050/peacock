package main

import (
	"log"

	"peacock/config"
	"peacock/handler"
	"peacock/kv"
	"peacock/server"
)

func main() {
	cfg := config.Load()

	store, err := kv.Open(kv.Options{
		DirPath:            cfg.KVDir,
		SyncInterval:       cfg.KVSyncInterval,
		MaxSegmentSize:     cfg.WALMaxSegmentSize,
		CompactionTrigger:  cfg.KVCompactionTrigger,
		CompactionInterval: cfg.KVCompactionInterval,
		OnSyncError: func(err error) {
			log.Printf("kv: background sync: %v", err)
		},
		OnCompactionError: func(err error) {
			log.Printf("kv: background compaction: %v", err)
		},
	})
	if err != nil {
		log.Fatalf("kv open: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("kv close: %v", err)
		}
	}()

	srv := server.New(cfg)
	handler.Register(srv.App, store)

	if err := srv.Start(); err != nil {
		log.Printf("server: %v", err)
	}
}
