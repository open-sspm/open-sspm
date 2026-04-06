package readmodels

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type Projector struct {
	pool *pgxpool.Pool
	q    *gen.Queries
	cfg  RefreshConfig
}

type sourceState struct {
	sourceKind       string
	sourceName       string
	enabled          bool
	configured       bool
	discoveryEnabled bool
}

type refreshConfigContextKey struct{}

func NewProjector(pool *pgxpool.Pool, q *gen.Queries, cfg RefreshConfig) *Projector {
	// Projectors that own a pool must start from pool-backed queries and derive
	// tx-scoped queries inside withQueries. Callers that already hold scoped
	// queries should pass pool=nil (for example via ProjectorFromContext).
	if pool != nil {
		q = gen.New(pool)
	}

	return &Projector{
		pool: pool,
		q:    q,
		cfg:  cfg,
	}
}

func WithRefreshConfig(ctx context.Context, cfg RefreshConfig) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, refreshConfigContextKey{}, cfg)
}

func ProjectorFromContext(ctx context.Context, q *gen.Queries) *Projector {
	if ctx == nil || q == nil {
		return nil
	}
	cfg, ok := ctx.Value(refreshConfigContextKey{}).(RefreshConfig)
	if !ok {
		return nil
	}
	return NewProjector(nil, q, cfg)
}

func (p *Projector) RefreshConnectorSourceState(ctx context.Context) error {
	return p.withQueries(ctx, func(q *gen.Queries) error {
		return refreshConnectorSourceState(ctx, q, p.cfg)
	})
}

func (p *Projector) RefreshDiscoverySource(ctx context.Context, sourceKind, sourceName string) error {
	return p.withQueries(ctx, func(q *gen.Queries) error {
		return refreshDiscoverySource(ctx, q, sourceKind, sourceName)
	})
}

func (p *Projector) RefreshAppAssetSource(ctx context.Context, sourceKind, sourceName string) error {
	return p.withQueries(ctx, func(q *gen.Queries) error {
		return refreshAppAssetSource(ctx, q, sourceKind, sourceName)
	})
}

func (p *Projector) RefreshSourceReadModels(ctx context.Context, sourceKind, sourceName string) error {
	return p.withQueries(ctx, func(q *gen.Queries) error {
		if err := refreshDiscoverySource(ctx, q, sourceKind, sourceName); err != nil {
			return err
		}
		return refreshAppAssetSource(ctx, q, sourceKind, sourceName)
	})
}

func (p *Projector) StoredReadModelsNeedRebuild(ctx context.Context) (bool, error) {
	if p == nil || p.q == nil {
		return false, nil
	}
	return p.q.StoredReadModelsNeedRebuild(ctx)
}

func (p *Projector) RebuildAllReadModels(ctx context.Context) error {
	return p.withQueries(ctx, func(q *gen.Queries) error {
		if _, err := q.RefreshAllSaaSAppReadModels(ctx); err != nil {
			return err
		}
		if _, err := q.RefreshAllAppAssetReadModels(ctx); err != nil {
			return err
		}
		return refreshConnectorSourceState(ctx, q, p.cfg)
	})
}

func (p *Projector) withQueries(ctx context.Context, fn func(*gen.Queries) error) error {
	if p == nil || p.q == nil {
		return nil
	}
	if p.pool == nil {
		return fn(p.q)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if err := fn(p.q.WithTx(tx)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func refreshConnectorSourceState(ctx context.Context, q *gen.Queries, cfg RefreshConfig) error {
	configRows, err := q.ListConnectorConfigs(ctx)
	if err != nil {
		return err
	}
	secretRows, err := q.ListConnectorSecrets(ctx)
	if err != nil {
		return err
	}
	runRows, err := q.ListLatestSuccessfulSyncRunsBySource(ctx)
	if err != nil {
		return err
	}

	lastSuccessBySource := make(map[string]pgtype.Timestamptz, len(runRows))
	for _, row := range runRows {
		key := sourceKey(row.SourceKind, row.SourceName)
		lastSuccessBySource[key] = row.LastSuccessAt
	}

	if err := q.DeleteConnectorSourceStateAll(ctx); err != nil {
		return err
	}

	secretPresence := configstore.SecretPresenceByKind(secretRows)
	for _, row := range configRows {
		state, ok, err := buildSourceState(row, secretPresence[normalizeConfigKind(row.Kind)])
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		lastSuccess := lastSuccessBySource[sourceKey(state.sourceKind, state.sourceName)]
		freshUntil := pgtype.Timestamptz{}
		if lastSuccess.Valid {
			freshUntil = pgtype.Timestamptz{
				Time:  lastSuccess.Time.UTC().Add(FreshnessWindow(cfg, state.sourceKind)),
				Valid: true,
			}
		}

		if err := q.UpsertConnectorSourceState(ctx, gen.UpsertConnectorSourceStateParams{
			SourceKind:       state.sourceKind,
			SourceName:       state.sourceName,
			Enabled:          state.enabled,
			Configured:       state.configured,
			DiscoveryEnabled: state.discoveryEnabled,
			LastSuccessAt:    lastSuccess,
			FreshUntilAt:     freshUntil,
		}); err != nil {
			return err
		}
	}

	return nil
}

func refreshDiscoverySource(ctx context.Context, q *gen.Queries, sourceKind, sourceName string) error {
	sourceKind = normalizeSourceKind(sourceKind)
	sourceName = strings.TrimSpace(sourceName)
	if sourceKind == "" || sourceName == "" {
		return nil
	}
	_, err := q.RefreshSaaSAppReadModelsBySource(ctx, gen.RefreshSaaSAppReadModelsBySourceParams{
		SourceKind: sourceKind,
		SourceName: sourceName,
	})
	return err
}

func refreshAppAssetSource(ctx context.Context, q *gen.Queries, sourceKind, sourceName string) error {
	sourceKind = normalizeSourceKind(sourceKind)
	sourceName = strings.TrimSpace(sourceName)
	if sourceKind == "" || sourceName == "" {
		return nil
	}
	_, err := q.RefreshAppAssetReadModelsBySource(ctx, gen.RefreshAppAssetReadModelsBySourceParams{
		SourceKind: sourceKind,
		SourceName: sourceName,
	})
	return err
}

func FreshnessWindow(cfg RefreshConfig, kind string) time.Duration {
	interval := cfg.SyncInterval

	switch normalizeConfigKind(kind) {
	case configstore.KindOkta:
		if cfg.SyncOktaInterval > 0 {
			interval = cfg.SyncOktaInterval
		}
	case configstore.KindEntra:
		if cfg.SyncEntraInterval > 0 {
			interval = cfg.SyncEntraInterval
		}
	case configstore.KindGoogleWorkspace:
		if cfg.SyncGoogleWorkspaceInterval > 0 {
			interval = cfg.SyncGoogleWorkspaceInterval
		}
	case configstore.KindGitHub:
		if cfg.SyncGitHubInterval > 0 {
			interval = cfg.SyncGitHubInterval
		}
	case configstore.KindDatadog:
		if cfg.SyncDatadogInterval > 0 {
			interval = cfg.SyncDatadogInterval
		}
	case configstore.KindAWSIdentityCenter:
		if cfg.SyncAWSInterval > 0 {
			interval = cfg.SyncAWSInterval
		}
	}

	if interval <= 0 {
		interval = 15 * time.Minute
	}
	return max(interval*2, 30*time.Minute)
}

func buildSourceState(row gen.ConnectorConfig, presence map[string]bool) (sourceState, bool, error) {
	kind := normalizeConfigKind(row.Kind)
	if kind == "" {
		return sourceState{}, false, nil
	}

	resolvedRaw, err := configstore.ResolveConfigWithSecretPresence(kind, row.Config, presence)
	if err != nil {
		return sourceState{}, false, err
	}

	switch kind {
	case configstore.KindOkta:
		cfg, err := configstore.DecodeOktaConfig(resolvedRaw)
		if err != nil {
			return sourceState{}, false, err
		}
		cfg = cfg.Normalized()
		sourceName := strings.TrimSpace(cfg.Domain)
		if sourceName == "" {
			return sourceState{}, false, nil
		}
		return sourceState{
			sourceKind:       normalizeSourceKind(kind),
			sourceName:       sourceName,
			enabled:          row.Enabled,
			configured:       cfg.Validate() == nil,
			discoveryEnabled: cfg.DiscoveryEnabled,
		}, true, nil
	case configstore.KindEntra:
		cfg, err := configstore.DecodeEntraConfig(resolvedRaw)
		if err != nil {
			return sourceState{}, false, err
		}
		cfg = cfg.Normalized()
		sourceName := strings.TrimSpace(cfg.TenantID)
		if sourceName == "" {
			return sourceState{}, false, nil
		}
		return sourceState{
			sourceKind:       normalizeSourceKind(kind),
			sourceName:       sourceName,
			enabled:          row.Enabled,
			configured:       cfg.Validate() == nil,
			discoveryEnabled: cfg.DiscoveryEnabled,
		}, true, nil
	case configstore.KindGoogleWorkspace:
		cfg, err := configstore.DecodeGoogleWorkspaceConfig(resolvedRaw)
		if err != nil {
			return sourceState{}, false, err
		}
		cfg = cfg.Normalized()
		sourceName := strings.TrimSpace(cfg.CustomerID)
		if sourceName == "" {
			return sourceState{}, false, nil
		}
		return sourceState{
			sourceKind:       normalizeSourceKind(kind),
			sourceName:       sourceName,
			enabled:          row.Enabled,
			configured:       cfg.Validate() == nil,
			discoveryEnabled: cfg.DiscoveryEnabled,
		}, true, nil
	case configstore.KindGitHub:
		cfg, err := configstore.DecodeGitHubConfig(resolvedRaw)
		if err != nil {
			return sourceState{}, false, err
		}
		cfg = cfg.Normalized()
		sourceName := strings.TrimSpace(cfg.Org)
		if sourceName == "" {
			return sourceState{}, false, nil
		}
		return sourceState{
			sourceKind: normalizeSourceKind(kind),
			sourceName: sourceName,
			enabled:    row.Enabled,
			configured: cfg.Validate() == nil,
		}, true, nil
	case configstore.KindDatadog:
		cfg, err := configstore.DecodeDatadogConfig(resolvedRaw)
		if err != nil {
			return sourceState{}, false, err
		}
		cfg = cfg.Normalized()
		sourceName := strings.TrimSpace(cfg.Site)
		if sourceName == "" {
			return sourceState{}, false, nil
		}
		return sourceState{
			sourceKind: normalizeSourceKind(kind),
			sourceName: sourceName,
			enabled:    row.Enabled,
			configured: cfg.Validate() == nil,
		}, true, nil
	case configstore.KindAWSIdentityCenter:
		cfg, err := configstore.DecodeAWSIdentityCenterConfig(resolvedRaw)
		if err != nil {
			return sourceState{}, false, err
		}
		cfg = cfg.Normalized()
		sourceName := strings.TrimSpace(cfg.Name)
		if sourceName == "" {
			sourceName = strings.TrimSpace(cfg.Region)
		}
		if sourceName == "" {
			return sourceState{}, false, nil
		}
		return sourceState{
			sourceKind: normalizeSourceKind(kind),
			sourceName: sourceName,
			enabled:    row.Enabled,
			configured: cfg.Validate() == nil,
		}, true, nil
	case configstore.KindVault:
		cfg, err := configstore.DecodeVaultConfig(resolvedRaw)
		if err != nil {
			return sourceState{}, false, err
		}
		cfg = cfg.Normalized()
		sourceName := strings.TrimSpace(cfg.SourceName())
		if sourceName == "" {
			return sourceState{}, false, nil
		}
		return sourceState{
			sourceKind: normalizeSourceKind(kind),
			sourceName: sourceName,
			enabled:    row.Enabled,
			configured: cfg.Validate() == nil,
		}, true, nil
	default:
		return sourceState{}, false, nil
	}
}

func sourceKey(kind, name string) string {
	return normalizeSourceKind(kind) + "\x00" + strings.ToLower(strings.TrimSpace(name))
}

func normalizeConfigKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "aws":
		return configstore.KindAWSIdentityCenter
	default:
		return strings.ToLower(strings.TrimSpace(kind))
	}
}

func normalizeSourceKind(kind string) string {
	switch normalizeConfigKind(kind) {
	case configstore.KindAWSIdentityCenter:
		return "aws"
	default:
		return normalizeConfigKind(kind)
	}
}
