package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/metrics"
)

const (
	workerShutdownTimeout     = 5 * time.Second
	workerCriticalErrorBuffer = 8
)

type workerLane interface {
	Name() string
	ValidateConfig(config.Config) error
	MetricsRefresh(*runtimeDependencies, config.Config) metrics.RefreshFunc
	Start(context.Context, *workerHostRun, *runtimeDependencies, config.Config) error
}

type workerHostOptions struct {
	loadConfig func() (config.Config, error)
	openDeps   func(context.Context, config.Config) (*runtimeDependencies, error)
	newContext func() (context.Context, context.CancelFunc)
}

type workerHostRun struct {
	laneName string
	ctx      context.Context
	stop     context.CancelFunc
	errCh    chan error

	wg sync.WaitGroup

	cleanupMu sync.Mutex
	cleanups  []workerCleanup
}

type workerCleanup struct {
	name string
	fn   func(context.Context) error
}

func runWorkerHost(lane workerLane) error {
	return runWorkerHostWithOptions(lane, workerHostOptions{})
}

func runWorkerHostWithOptions(lane workerLane, opts workerHostOptions) error {
	if lane == nil {
		return errors.New("worker lane is not configured")
	}
	loadConfig := opts.loadConfig
	if loadConfig == nil {
		loadConfig = config.Load
	}
	openDeps := opts.openDeps
	if openDeps == nil {
		openDeps = openRuntimeDependencies
	}
	newContext := opts.newContext
	if newContext == nil {
		newContext = func() (context.Context, context.CancelFunc) {
			return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		}
	}

	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if err := lane.ValidateConfig(cfg); err != nil {
		return err
	}

	ctx, stop := newContext()
	defer stop()

	runtimeDeps, err := openDeps(ctx, cfg)
	if err != nil {
		return err
	}
	defer runtimeDeps.pool.Close()

	run := newWorkerHostRun(ctx, stop, lane.Name())
	run.observeLaneStarted()
	defer run.observeLaneStopped()

	metricsServer, metricsErrCh := metrics.StartServer(ctx, cfg.MetricsAddr, lane.MetricsRefresh(runtimeDeps, cfg))
	if metricsErrCh != nil {
		run.Go("metrics", true, func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return nil
			case err := <-metricsErrCh:
				return err
			}
		})
	}
	defer shutdownMetricsServer(metricsServer)

	if err := lane.Start(ctx, run, runtimeDeps, cfg); err != nil {
		return err
	}

	slog.Info("worker lane started", "lane", lane.Name())
	runErr := run.Wait()
	cleanupErr := run.Cleanup()
	return errors.Join(runErr, cleanupErr)
}

func newWorkerHostRun(ctx context.Context, stop context.CancelFunc, laneName string) *workerHostRun {
	return &workerHostRun{
		laneName: laneName,
		ctx:      ctx,
		stop:     stop,
		// The host returns the first critical error and logs any overflow.
		// Keep this comfortably above the expected critical components per lane.
		errCh: make(chan error, workerCriticalErrorBuffer),
	}
}

func (r *workerHostRun) Go(component string, critical bool, fn func(context.Context) error) {
	if r == nil || fn == nil {
		return
	}
	component = stringsTrimDefault(component, "worker")
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		err := fn(r.ctx)
		if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || r.ctx.Err() != nil {
			return
		}
		if !critical {
			slog.Warn("worker component failed", "lane", r.laneName, "component", component, "err", err)
			return
		}
		metrics.WorkerLaneFailuresTotal.WithLabelValues(r.laneName, component).Inc()
		select {
		case r.errCh <- fmt.Errorf("%s failed: %w", component, err):
		default:
			slog.Error("worker critical component failed after error channel filled", "lane", r.laneName, "component", component, "err", err)
		}
		r.stop()
	}()
}

// OnShutdown registers cleanup that runs after Wait returns. Lanes should
// register handlers synchronously during Start; handlers added after Cleanup
// snapshots the list will not run for that shutdown.
func (r *workerHostRun) OnShutdown(name string, fn func(context.Context) error) {
	if r == nil || fn == nil {
		return
	}
	r.cleanupMu.Lock()
	defer r.cleanupMu.Unlock()
	r.cleanups = append(r.cleanups, workerCleanup{name: stringsTrimDefault(name, "cleanup"), fn: fn})
}

func (r *workerHostRun) Wait() error {
	if r == nil {
		return nil
	}
	var runErr error
	// An external shutdown can win this select before a near-simultaneous
	// component failure reaches errCh. In that case the goroutine still logs the
	// failure, but Wait returns nil because shutdown was already in progress.
	select {
	case <-r.ctx.Done():
		select {
		case runErr = <-r.errCh:
		default:
		}
	case runErr = <-r.errCh:
		r.stop()
	}
	r.wg.Wait()
	return runErr
}

func (r *workerHostRun) Cleanup() error {
	if r == nil {
		return nil
	}
	r.cleanupMu.Lock()
	cleanups := append([]workerCleanup(nil), r.cleanups...)
	r.cleanupMu.Unlock()

	var errs []error
	for i := len(cleanups) - 1; i >= 0; i-- {
		cleanup := cleanups[i]
		cleanupCtx, cancel := context.WithTimeout(context.Background(), workerShutdownTimeout)
		err := cleanup.fn(cleanupCtx)
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("%s cleanup: %w", cleanup.name, err))
		}
	}
	return errors.Join(errs...)
}

func (r *workerHostRun) ObserveLoopTick() {
	metrics.WorkerLaneLastLoopTickTimestamp.WithLabelValues(r.laneName).Set(float64(time.Now().Unix()))
}

func (r *workerHostRun) ObserveClaimAttempt() {
	metrics.WorkerLaneLastClaimAttemptTimestamp.WithLabelValues(r.laneName).Set(float64(time.Now().Unix()))
}

func (r *workerHostRun) ObserveListenerConnect() {
	metrics.WorkerLaneLastListenerConnectTimestamp.WithLabelValues(r.laneName).Set(float64(time.Now().Unix()))
}

func (r *workerHostRun) ObserveLeaseLost() {
	metrics.WorkerLaneLeaseLostTotal.WithLabelValues(r.laneName).Inc()
}

func (r *workerHostRun) observeLaneStarted() {
	metrics.WorkerLaneUp.WithLabelValues(r.laneName).Set(1)
}

func (r *workerHostRun) observeLaneStopped() {
	metrics.WorkerLaneUp.WithLabelValues(r.laneName).Set(0)
}

func shutdownMetricsServer(server *http.Server) {
	if server == nil {
		return
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), workerShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Warn("metrics server shutdown failed", "err", err)
	}
}

func stringsTrimDefault(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}
