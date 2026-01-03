package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/aws"
	"github.com/open-sspm/open-sspm/internal/connectors/datadog"
	"github.com/open-sspm/open-sspm/internal/connectors/entra"
	"github.com/open-sspm/open-sspm/internal/connectors/github"
	"github.com/open-sspm/open-sspm/internal/connectors/okta"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/connectors/vault"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	httpapp "github.com/open-sspm/open-sspm/internal/http"
	"github.com/open-sspm/open-sspm/internal/http/handlers"
	"github.com/open-sspm/open-sspm/internal/sync"
	"github.com/spf13/cobra"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Run the HTTP server.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runServe()
	},
}

func runServe() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	queries := gen.New(pool)

	reg := registry.NewRegistry()
	if err := reg.Register(okta.NewDefinition(cfg.SyncOktaWorkers)); err != nil {
		return err
	}
	if err := reg.Register(&entra.Definition{}); err != nil {
		return err
	}
	if err := reg.Register(github.NewDefinition(cfg.SyncGitHubWorkers)); err != nil {
		return err
	}
	if err := reg.Register(datadog.NewDefinition(cfg.SyncDatadogWorkers)); err != nil {
		return err
	}
	if err := reg.Register(&aws.Definition{}); err != nil {
		return err
	}
	if err := reg.Register(&vault.Definition{}); err != nil {
		return err
	}

	dbRunner := sync.NewDBRunner(pool, reg)
	dbRunner.SetGlobalEvalMode(cfg.GlobalEvalMode)

	var syncer handlers.SyncRunner
	if cfg.ResyncEnabled {
		switch strings.ToLower(strings.TrimSpace(cfg.ResyncMode)) {
		case "signal":
			syncer = sync.NewResyncSignalRunner(pool)
		default:
			syncer = sync.NewTryRunOnceLockRunner(pool, dbRunner)
		}
	} else {
		syncer = nil
	}

	srv, err := httpapp.NewEchoServer(cfg, queries, syncer, reg)
	if err != nil {
		return err
	}

	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		slog.Info("listening", "addr", cfg.HTTPAddr)
		errCh <- srv.StartServer(httpServer)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}
