package sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type DBRunner struct {
	pool           *pgxpool.Pool
	q              *gen.Queries
	registry       *registry.ConnectorRegistry
	reporter       registry.Reporter
	policy         *RunPolicy
	globalEvalMode string
}

func NewDBRunner(pool *pgxpool.Pool, reg *registry.ConnectorRegistry) *DBRunner {
	q := gen.New(pool)
	return &DBRunner{
		pool:     pool,
		q:        q,
		registry: reg,
	}
}

func (r *DBRunner) SetReporter(reporter registry.Reporter) {
	r.reporter = reporter
}

func (r *DBRunner) SetRunPolicy(policy RunPolicy) {
	r.policy = &policy
}

func (r *DBRunner) SetGlobalEvalMode(mode string) {
	r.globalEvalMode = strings.TrimSpace(mode)
}

func (r *DBRunner) RunOnce(ctx context.Context) error {
	if r == nil || r.q == nil || r.pool == nil {
		return ErrNoEnabledConnectors
	}

	configs, err := r.q.ListConnectorConfigs(ctx)
	if err != nil {
		return err
	}

	orchestrator := NewOrchestrator(r.pool)
	if r.reporter != nil {
		orchestrator.SetReporter(r.reporter)
	}
	if strings.TrimSpace(r.globalEvalMode) != "" {
		orchestrator.SetGlobalEvalMode(r.globalEvalMode)
	}

	var (
		errList          []error
		integrationCount int
		enabledKinds     []string
		disabledKinds    []string
		skippedKinds     []string
		deferred         []string
		planned          []string
	)

	for _, cfgRow := range configs {
		kind := strings.TrimSpace(cfgRow.Kind)
		if kind == "" {
			continue
		}

		if !cfgRow.Enabled {
			disabledKinds = append(disabledKinds, kind)
			continue
		}
		enabledKinds = append(enabledKinds, kind)

		def, ok := r.registry.Get(kind)
		if !ok {
			slog.Warn("unknown connector kind skipped", "kind", kind)
			skippedKinds = append(skippedKinds, kind)
			continue
		}

		cfg, err := def.DecodeConfig(cfgRow.Config)
		if err != nil {
			errList = append(errList, fmt.Errorf("%s config: %w", kind, err))
			skippedKinds = append(skippedKinds, kind)
			continue
		}

		if err := def.ValidateConfig(cfg); err != nil {
			errList = append(errList, fmt.Errorf("%s config: %w", kind, err))
			skippedKinds = append(skippedKinds, kind)
			continue
		}

		integration, err := def.NewIntegration(cfg)
		if err != nil {
			errList = append(errList, fmt.Errorf("%s integration: %w", kind, err))
			skippedKinds = append(skippedKinds, kind)
			continue
		}

		if integration == nil {
			// Not syncable (e.g. Vault stub)
			skippedKinds = append(skippedKinds, kind)
			continue
		}

		if r.policy != nil && !IsForcedSync(ctx) {
			runKind := strings.ToLower(strings.TrimSpace(integration.Kind()))
			runName := strings.TrimSpace(integration.Name())
			shouldRun, reason, err := r.shouldRunIntegration(ctx, runKind, runName)
			if err != nil {
				errList = append(errList, fmt.Errorf("%s scheduling: %w", kind, err))
				skippedKinds = append(skippedKinds, kind)
				continue
			}
			if !shouldRun {
				deferred = append(deferred, fmt.Sprintf("%s/%s (%s)", runKind, runName, reason))
				continue
			}
		}

		if err := orchestrator.AddIntegration(integration); err != nil {
			errList = append(errList, err)
			skippedKinds = append(skippedKinds, kind)
			continue
		}
		integrationCount++
		planned = append(planned, strings.TrimSpace(integration.Kind())+"/"+strings.TrimSpace(integration.Name()))
	}

	if r.reporter != nil {
		if len(enabledKinds) > 0 {
			r.reporter.Report(registry.Event{Source: "sync", Stage: "plan", Message: "enabled connectors: " + strings.Join(enabledKinds, ", ")})
		}
		if len(disabledKinds) > 0 {
			r.reporter.Report(registry.Event{Source: "sync", Stage: "plan", Message: "disabled connectors: " + strings.Join(disabledKinds, ", ")})
		}
		if len(skippedKinds) > 0 {
			r.reporter.Report(registry.Event{Source: "sync", Stage: "plan", Message: "skipped connectors: " + strings.Join(skippedKinds, ", ")})
		}
		if len(deferred) > 0 {
			r.reporter.Report(registry.Event{Source: "sync", Stage: "plan", Message: "deferred integrations: " + strings.Join(deferred, ", ")})
		}
		if len(planned) > 0 {
			r.reporter.Report(registry.Event{Source: "sync", Stage: "plan", Message: "planned integrations: " + strings.Join(planned, ", ")})
		}
	}

	if integrationCount == 0 {
		if len(errList) > 0 {
			return errors.Join(ErrNoEnabledConnectors, errors.Join(errList...))
		}
		if len(deferred) > 0 {
			return ErrNoConnectorsDue
		}
		return ErrNoEnabledConnectors
	}

	runErr := orchestrator.RunOnce(ctx)
	if len(errList) > 0 {
		return errors.Join(runErr, errors.Join(errList...))
	}
	return runErr
}

func (r *DBRunner) shouldRunIntegration(ctx context.Context, kind, name string) (bool, string, error) {
	if r == nil || r.q == nil || r.policy == nil {
		return true, "", nil
	}
	if IsForcedSync(ctx) {
		return true, "", nil
	}

	kind = strings.ToLower(strings.TrimSpace(kind))
	name = strings.TrimSpace(name)
	if kind == "" || name == "" {
		return true, "", nil
	}

	now := r.policy.now()
	interval := r.policy.intervalForKind(kind)

	history, err := r.q.ListRecentFinishedSyncRunsBySource(ctx, gen.ListRecentFinishedSyncRunsBySourceParams{
		SourceKind: kind,
		SourceName: name,
		Limit:      r.policy.recentCap(),
	})
	if err != nil {
		return false, "", err
	}
	if len(history) == 0 {
		return true, "no history", nil
	}

	latest := history[0]
	if !latest.FinishedAt.Valid {
		return true, "no finished_at", nil
	}

	var (
		lastSuccessAt time.Time
		failures      int
	)
	for _, run := range history {
		if !run.FinishedAt.Valid {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(run.Status), "success") {
			lastSuccessAt = run.FinishedAt.Time
			break
		}
		failures++
	}

	var (
		intervalDue  = true
		intervalNext time.Time
	)
	if interval > 0 && failures == 0 && !lastSuccessAt.IsZero() {
		intervalNext = lastSuccessAt.Add(interval)
		intervalDue = !now.Before(intervalNext)
	}

	var (
		backoffDue  = true
		backoffNext time.Time
	)
	if failures > 0 && r.policy.FailureBackoffBase > 0 {
		delay := failureBackoffDelay(r.policy.FailureBackoffBase, failures, r.policy.FailureBackoffMax)
		if delay > 0 {
			backoffNext = latest.FinishedAt.Time.Add(delay)
			backoffDue = !now.Before(backoffNext)
		}
	}

	if intervalDue && backoffDue {
		return true, "due", nil
	}

	next := time.Time{}
	if !intervalDue {
		next = intervalNext
	}
	if !backoffDue {
		if next.IsZero() || backoffNext.After(next) {
			next = backoffNext
		}
	}

	if !backoffDue && !next.IsZero() {
		return false, fmt.Sprintf("backoff until %s (failures=%d)", next.Format(time.RFC3339), failures), nil
	}
	if !intervalDue && !next.IsZero() {
		return false, fmt.Sprintf("not due until %s", next.Format(time.RFC3339)), nil
	}
	return false, "not due", nil
}
