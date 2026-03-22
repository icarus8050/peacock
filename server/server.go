package server

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gofiber/fiber/v2"

	"peacock/config"
)

type Server struct {
	App *fiber.App
	Cfg *config.Config
}

func New(cfg *config.Config) *Server {
	app := fiber.New(fiber.Config{
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})

	return &Server{
		App: app,
		Cfg: cfg,
	}
}

func (s *Server) Start() error {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		addr := fmt.Sprintf(":%s", s.Cfg.Port)
		log.Printf("server starting on %s", addr)
		if err := s.App.Listen(addr); err != nil {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("server error: %w", err)
	case sig := <-quit:
		log.Printf("received signal %s, shutting down...", sig)
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.Cfg.ShutdownTimeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- s.App.Shutdown()
	}()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("shutdown error: %w", err)
		}
		log.Println("server stopped gracefully")
	case <-ctx.Done():
		return fmt.Errorf("shutdown timed out")
	}

	return nil
}
