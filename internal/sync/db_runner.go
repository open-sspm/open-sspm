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
	locks          LockManager
	mode           registry.RunMode
}

type integrationCandidate struct {
	integration registry.Integration
	kind        string
	runKind     string
	runName     string
}

type syncRunHistoryKey struct {
	kind string
	name string
}

type syncRunHistory struct {
	status          string
	finishedAt      time.Time
	finishedAtValid bool
}

func NewDBRunner(pool *pgxpool.Pool, reg *registry.ConnectorRegistry) *DBRunner {
	q := gen.New(pool)
	return &DBRunner{
		pool:     pool,
		q:        q,
		registry: reg,
		mode:     registry.RunModeFull,
	}
}

func (r *DBRunner) SetReporter(reporter registry.Reporter) {
	r.reporter = reporter
}

func (r *DBRunner) SetLockManager(m LockManager) {
	r.locks = m
}

func (r *DBRunner) SetRunPolicy(policy RunPolicy) {
	r.policy = &policy
}

func (r *DBRunner) SetGlobalEvalMode(mode string) {
	r.globalEvalMode = strings.TrimSpace(mode)
}

func (r *DBRunner) SetRunMode(mode registry.RunMode) {
	r.mode = mode.Normalize()
}

func (r *DBRunner) RunOnce(ctx context.Context) error {
	if r == nil || r.q == nil || r.pool == nil {
		return ErrNoEnabledConnectors
	}

	configs, err := r.q.ListConnectorConfigs(ctx)
	if err != nil {
		return err
	}

	orchestrator := NewOrchestrator(r.pool, r.registry)
	if r.reporter != nil {
		orchestrator.SetReporter(r.reporter)
	}
	if r.locks == nil {
		locks, err := NewLockManager(r.pool, LockManagerConfig{})
		if err != nil {
			return err
		}
		r.locks = locks
	}
	orchestrator.SetLockManager(r.locks)
	if strings.TrimSpace(r.globalEvalMode) != "" {
		orchestrator.SetGlobalEvalMode(r.globalEvalMode)
	}
	orchestrator.SetRunMode(r.runMode())

	forcedSync := IsForcedSync(ctx)
	requestedConnectorKind, requestedSourceName, hasRequestedScope := ConnectorScopeFromContext(ctx)
	var (
		errList          []error
		integrationCount int
		enabledKinds     []string
		disabledKinds    []string
		skippedKinds     []string
		deferred         []string
		planned          []string
		candidates       []integrationCandidate
	)

	for _, cfgRow := range configs {
		kind := strings.TrimSpace(cfgRow.Kind)
		if kind == "" {
			continue
		}
		if !connectorKindMatchesRequestedScope(kind, requestedConnectorKind, hasRequestedScope) {
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

		if !r.integrationSupportsRunMode(integration) {
			skippedKinds = append(skippedKinds, kind)
			continue
		}

		runKind := r.integrationRunSourceKind(integration)
		runName := strings.TrimSpace(integration.Name())
		candidates = append(candidates, integrationCandidate{
			integration: integration,
			kind:        kind,
			runKind:     runKind,
			runName:     runName,
		})
	}

	var historyBySource map[syncRunHistoryKey][]syncRunHistory
	if r.policy != nil && !forcedSync && len(candidates) > 0 {
		keys := make([]syncRunHistoryKey, 0, len(candidates))
		for _, candidate := range candidates {
			if candidate.runKind == "" || candidate.runName == "" {
				continue
			}
			keys = append(keys, syncRunHistoryKey{kind: candidate.runKind, name: candidate.runName})
		}
		if len(keys) > 0 {
			var historyErr error
			historyBySource, historyErr = r.listRecentFinishedSyncRunsForSources(ctx, keys)
			if historyErr != nil {
				slog.Warn("sync history lookup failed; falling back to per-integration queries", "err", historyErr)
				historyBySource = nil
			}
		}
	}

	for _, candidate := range candidates {
		if hasRequestedScope && !matchesRequestedConnectorScope(candidate.kind, candidate.runName, requestedConnectorKind, requestedSourceName) {
			continue
		}
		if r.policy != nil && !forcedSync {
			var (
				shouldRun bool
				reason    string
				err       error
			)
			if historyBySource != nil {
				history := historyBySource[syncRunHistoryKey{kind: candidate.runKind, name: candidate.runName}]
				shouldRun, reason, err = r.shouldRunIntegrationFromHistory(candidate.runKind, candidate.runName, history)
			} else {
				shouldRun, reason, err = r.shouldRunIntegration(ctx, candidate.runKind, candidate.runName)
			}
			if err != nil {
				errList = append(errList, fmt.Errorf("%s scheduling: %w", candidate.kind, err))
				skippedKinds = append(skippedKinds, candidate.kind)
				continue
			}
			if !shouldRun {
				deferred = append(deferred, fmt.Sprintf("%s/%s (%s)", candidate.runKind, candidate.runName, reason))
				continue
			}
		}

		if err := orchestrator.AddIntegration(candidate.integration); err != nil {
			errList = append(errList, err)
			skippedKinds = append(skippedKinds, candidate.kind)
			continue
		}
		integrationCount++
		planned = append(planned, candidate.runKind+"/"+candidate.runName)
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
		return noIntegrationRunError(errList, deferred, hasRequestedScope)
	}

	runErr := orchestrator.RunOnce(ctx)
	if len(errList) > 0 {
		return errors.Join(runErr, errors.Join(errList...))
	}
	return runErr
}

func (r *DBRunner) runMode() registry.RunMode {
	if r == nil {
		return registry.RunModeFull
	}
	return r.mode.Normalize()
}

func (r *DBRunner) integrationRunSourceKind(integration registry.Integration) string {
	if integration == nil {
		return ""
	}
	return registry.SyncRunSourceKind(integration.Kind(), r.runMode())
}

func (r *DBRunner) integrationSupportsRunMode(integration registry.Integration) bool {
	mode := r.runMode()
	if integration == nil {
		return false
	}

	modeAware, ok := integration.(registry.ModeAwareIntegration)
	if ok {
		return modeAware.SupportsRunMode(mode)
	}

	// Legacy integrations default to full-only.
	return mode == registry.RunModeFull
}

func matchesRequestedConnectorScope(kind, sourceName, requestedKind, requestedSourceName string) bool {
	kind = strings.ToLower(strings.TrimSpace(kind))
	sourceName = strings.TrimSpace(sourceName)
	requestedKind = strings.ToLower(strings.TrimSpace(requestedKind))
	requestedSourceName = strings.TrimSpace(requestedSourceName)
	if kind == "" || sourceName == "" || requestedKind == "" || requestedSourceName == "" {
		return false
	}
	return kind == requestedKind && strings.EqualFold(sourceName, requestedSourceName)
}

func connectorKindMatchesRequestedScope(kind, requestedKind string, hasRequestedScope bool) bool {
	if !hasRequestedScope {
		return true
	}
	kind = strings.ToLower(strings.TrimSpace(kind))
	requestedKind = strings.ToLower(strings.TrimSpace(requestedKind))
	if kind == "" || requestedKind == "" {
		return false
	}
	return kind == requestedKind
}

func noIntegrationRunError(errList []error, deferred []string, hasRequestedScope bool) error {
	if len(errList) > 0 {
		return errors.Join(ErrNoEnabledConnectors, errors.Join(errList...))
	}
	if len(deferred) > 0 || hasRequestedScope {
		return ErrNoConnectorsDue
	}
	return ErrNoEnabledConnectors
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

	history, err := r.q.ListRecentFinishedSyncRunsBySource(ctx, gen.ListRecentFinishedSyncRunsBySourceParams{
		SourceKind: kind,
		SourceName: name,
		Limit:      r.policy.recentCap(),
	})
	if err != nil {
		return false, "", err
	}
	parsed := make([]syncRunHistory, 0, len(history))
	for _, run := range history {
		parsed = append(parsed, syncRunHistory{
			status:          run.Status,
			finishedAt:      run.FinishedAt.Time,
			finishedAtValid: run.FinishedAt.Valid,
		})
	}

	return r.shouldRunIntegrationFromHistory(kind, name, parsed)
}

func (r *DBRunner) listRecentFinishedSyncRunsForSources(ctx context.Context, sources []syncRunHistoryKey) (map[syncRunHistoryKey][]syncRunHistory, error) {
	if r == nil || r.q == nil || r.policy == nil {
		return map[syncRunHistoryKey][]syncRunHistory{}, nil
	}

	unique := make(map[syncRunHistoryKey]struct{}, len(sources))
	for _, source := range sources {
		kind := strings.ToLower(strings.TrimSpace(source.kind))
		name := strings.TrimSpace(source.name)
		if kind == "" || name == "" {
			continue
		}
		unique[syncRunHistoryKey{kind: kind, name: name}] = struct{}{}
	}
	if len(unique) == 0 {
		return map[syncRunHistoryKey][]syncRunHistory{}, nil
	}

	sourceKinds := make([]string, 0, len(unique))
	sourceNames := make([]string, 0, len(unique))
	for key := range unique {
		sourceKinds = append(sourceKinds, key.kind)
		sourceNames = append(sourceNames, key.name)
	}

	rows, err := r.q.ListRecentFinishedSyncRunsForSources(ctx, gen.ListRecentFinishedSyncRunsForSourcesParams{
		LimitRows:   r.policy.recentCap(),
		SourceKinds: sourceKinds,
		SourceNames: sourceNames,
	})
	if err != nil {
		return nil, err
	}

	historyBySource := make(map[syncRunHistoryKey][]syncRunHistory, len(unique))
	for _, row := range rows {
		key := syncRunHistoryKey{kind: row.SourceKind, name: row.SourceName}
		historyBySource[key] = append(historyBySource[key], syncRunHistory{
			status:          row.Status,
			finishedAt:      row.FinishedAt.Time,
			finishedAtValid: row.FinishedAt.Valid,
		})
	}
	return historyBySource, nil
}

func (r *DBRunner) shouldRunIntegrationFromHistory(kind, name string, history []syncRunHistory) (bool, string, error) {
	if r == nil || r.policy == nil {
		return true, "", nil
	}
	kind = strings.ToLower(strings.TrimSpace(kind))
	name = strings.TrimSpace(name)
	if kind == "" || name == "" {
		return true, "", nil
	}

	now := r.policy.now()
	interval := r.policy.intervalForKind(kind)

	if len(history) == 0 {
		return true, "no history", nil
	}

	latest := history[0]
	if !latest.finishedAtValid {
		return true, "no finished_at", nil
	}

	var (
		lastSuccessAt time.Time
		failures      int
	)
	for _, run := range history {
		if !run.finishedAtValid {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(run.status), "success") {
			lastSuccessAt = run.finishedAt
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
			backoffNext = latest.finishedAt.Add(delay)
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
