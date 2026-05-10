package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/open-sspm/open-sspm/internal/config"
	oktaingest "github.com/open-sspm/open-sspm/internal/ingest/okta"
	"github.com/open-sspm/open-sspm/internal/metrics"
	"github.com/spf13/cobra"
)

var workerIngestCmd = &cobra.Command{
	Use:   "worker-ingest",
	Short: "Run background ingest queue processors.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runWorkerIngest()
	},
}

func runWorkerIngest() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if !cfg.SyncDiscoveryEnabled {
		return errors.New("SYNC_DISCOVERY_ENABLED must be true to run the ingest worker")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runtimeDeps, err := openRuntimeDependencies(ctx, cfg)
	if err != nil {
		return err
	}
	defer runtimeDeps.pool.Close()
	oktaPushInboxQueue, err := openOktaPushInboxQueue(ctx, cfg)
	if err != nil {
		return err
	}
	if oktaPushInboxQueue != nil {
		defer func() { _ = oktaPushInboxQueue.Close() }()
	}
	queries := runtimeDeps.queries

	metricsServer, metricsErrCh := metrics.StartServer(ctx, cfg.MetricsAddr, oktaPushMetricsRefresh(queries))
	errCh := make(chan error, 1)
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		if err := oktaingest.RunLoopWithQueue(ctx, queries, runtimeDeps.pool, oktaingest.DefaultConfig(), oktaPushInboxQueue); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

	slog.Info("ingest worker started")

	var runErr error
	if metricsErrCh == nil {
		select {
		case <-ctx.Done():
		case runErr = <-errCh:
			stop()
		}
	} else {
		select {
		case <-ctx.Done():
		case runErr = <-errCh:
			stop()
		case err := <-metricsErrCh:
			if err != nil {
				slog.Error("metrics server failed", "err", err)
				runErr = err
				stop()
			}
		}
	}

	<-doneCh
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if metricsServer != nil {
		_ = metricsServer.Shutdown(shutdownCtx)
	}
	return runErr
}
