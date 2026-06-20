package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/turbk/turbk/internal/config"
	"github.com/turbk/turbk/internal/httpapi"
	"github.com/turbk/turbk/internal/repository"
	"github.com/turbk/turbk/internal/state"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "Path to YAML config file")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := config.Load(configPath)
	if err != nil {
		logger.Error("load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := state.Open(ctx, cfg)
	if err != nil {
		logger.Error("open state store", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	repo, err := repository.Open(ctx, cfg)
	if err != nil {
		logger.Error("open repository", "error", err)
		os.Exit(1)
	}
	defer repo.Close()

	api := httpapi.New(cfg, store, repo, logger)
	api.StartScheduler(ctx)
	server := &http.Server{
		Addr:              cfg.Server.Listen,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("turbk server starting", "listen", cfg.Server.Listen, "state_dir", cfg.Paths.StateDir, "repo_dir", cfg.Paths.RepoDir, "web_dir", cfg.Server.WebDir)
		errCh <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("shutdown server", "error", err)
			os.Exit(1)
		}
		logger.Info("turbk server stopped")
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	}
}
