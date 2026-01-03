package main

import (
	"context"
	"errors"
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

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Run a one-off sync against Okta, Microsoft Entra ID, GitHub, Datadog, and AWS Identity Center (if configured).",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runSync()
	},
}

func runSync() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

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

	dbRunner := sync.NewDBRunner(pool, reg)
	dbRunner.SetReporter(&sync.LogReporter{})
	dbRunner.SetGlobalEvalMode(cfg.GlobalEvalMode)
	runner := sync.NewBlockingRunOnceLockRunner(pool, dbRunner)

	syncErr := runner.RunOnce(ctx)
	if syncErr == nil {
		return nil
	}
	if errors.Is(syncErr, context.Canceled) {
		return &exitError{code: 130, err: syncErr, silent: true}
	}
	return &exitError{code: 1, err: syncErr, silent: false}
}
