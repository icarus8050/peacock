package main

import (
	"log"

	"peacock/config"
	"peacock/handler"
	"peacock/server"
)

func main() {
	cfg := config.Load()

	srv := server.New(cfg)

	handler.Register(srv.App)

	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}
}
