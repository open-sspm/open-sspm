package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/aws"
	"github.com/open-sspm/open-sspm/internal/connectors/datadog"
	"github.com/open-sspm/open-sspm/internal/connectors/entra"
	"github.com/open-sspm/open-sspm/internal/connectors/github"
	"github.com/open-sspm/open-sspm/internal/connectors/okta"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/connectors/vault"
	"github.com/open-sspm/open-sspm/internal/sync"
	"github.com/spf13/cobra"
)

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Run the background sync loop.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runWorker()
	},
}

func runWorker() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if cfg.SyncInterval <= 0 {
		return errors.New("SYNC_INTERVAL must be > 0 to run the worker")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

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

	runner := sync.NewDBRunner(pool, reg)
	runner.SetReporter(&sync.LogReporter{})

	slog.Info("sync worker started", "interval", cfg.SyncInterval)
	scheduler := sync.Scheduler{Runner: runner, Interval: cfg.SyncInterval}
	scheduler.Run(ctx)
	return nil
}
