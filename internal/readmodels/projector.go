package readmodels

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/riskpolicy"
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

func (p *Projector) RefreshSaaSAppRiskReadModelsBySource(ctx context.Context, sourceKind, sourceName string) error {
	return p.withQueries(ctx, func(q *gen.Queries) error {
		return refreshSaaSAppRiskReadModelsBySource(ctx, q, sourceKind, sourceName)
	})
}

func (p *Projector) RefreshAllSaaSAppRiskReadModels(ctx context.Context) error {
	return p.withQueries(ctx, func(q *gen.Queries) error {
		return refreshAllSaaSAppRiskReadModels(ctx, q)
	})
}

func (p *Projector) RefreshSaaSAppRiskReadModelByID(ctx context.Context, saasAppID int64) error {
	return p.withQueries(ctx, func(q *gen.Queries) error {
		return refreshSaaSAppRiskReadModelByID(ctx, q, saasAppID)
	})
}

func (p *Projector) RefreshCredentialArtifactRiskReadModelsBySource(ctx context.Context, sourceKind, sourceName string) error {
	return p.withQueries(ctx, func(q *gen.Queries) error {
		return refreshCredentialArtifactRiskReadModelsBySource(ctx, q, sourceKind, sourceName)
	})
}

func (p *Projector) RefreshAllCredentialArtifactRiskReadModels(ctx context.Context) error {
	return p.withQueries(ctx, func(q *gen.Queries) error {
		return refreshAllCredentialArtifactRiskReadModels(ctx, q)
	})
}

func (p *Projector) RefreshAppAssetSource(ctx context.Context, sourceKind, sourceName string) error {
	return p.withQueries(ctx, func(q *gen.Queries) error {
		return refreshAppAssetSource(ctx, q, sourceKind, sourceName)
	})
}

func (p *Projector) RefreshNonHumanPrincipalSourceReadModels(ctx context.Context, sourceKind, sourceName string) error {
	return p.withQueries(ctx, func(q *gen.Queries) error {
		return refreshNonHumanPrincipalSource(ctx, q, sourceKind, sourceName)
	})
}

func (p *Projector) RefreshAllNonHumanPrincipalReadModels(ctx context.Context) error {
	return p.withQueries(ctx, func(q *gen.Queries) error {
		return refreshAllNonHumanPrincipalReadModels(ctx, q)
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
		if err := refreshConnectorSourceState(ctx, q, p.cfg); err != nil {
			return err
		}
		if err := refreshAllSaaSAppRiskReadModels(ctx, q); err != nil {
			return err
		}
		if err := refreshAllCredentialArtifactRiskReadModels(ctx, q); err != nil {
			return err
		}
		return refreshAllNonHumanPrincipalReadModels(ctx, q)
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

type saasAppRiskInputRow struct {
	saasAppID                     int64
	canonicalKey                  string
	displayName                   string
	primaryDomain                 string
	vendorName                    string
	category                      string
	sourceKind                    string
	sourceName                    string
	actors30d                     int64
	hasPrivilegedScope            bool
	hasConfidentialScope          bool
	managedState                  string
	managedReason                 string
	ownerIdentityID               int64
	governanceState               string
	reviewDisposition             string
	followUpDueDate               pgtype.Date
	configuredBusinessCriticality string
	configuredDataClassification  string
	connectorBindingConfigured    bool
	connectorBindingEnabled       bool
	connectorBindingStale         bool
	connectorBindingHealthy       bool
}

type nonHumanPrincipalRiskInputRow struct {
	principalRef                 string
	identityID                   int64
	appAssetID                   int64
	principalType                string
	sourceKind                   string
	sourceName                   string
	displayName                  string
	secondaryName                string
	primaryEmail                 string
	linkedAssetsCount            int64
	linkedCredentialsCount       int64
	lastSeenAt                   pgtype.Timestamptz
	activityState                string
	freshnessState               string
	governanceState              string
	accountableOwnerIdentityID   int64
	accountableOwnerDisplayName  string
	accountableOwnerPrimaryEmail string
	ownerPresence                string
	hasCriticalCredential        bool
	hasHighRiskCredential        bool
	hasExpiredCredential         bool
	hasExpiringCredential        bool
	hasUnusedCredential          bool
	hasStaleEvidence             bool
}

type credentialArtifactRiskInputRow struct {
	credentialArtifactID  int64
	sourceKind            string
	sourceName            string
	credentialKind        string
	status                string
	expiresAtSource       pgtype.Timestamptz
	lastUsedAtSource      pgtype.Timestamptz
	createdAtSource       pgtype.Timestamptz
	createdByExternalID   string
	createdByDisplayName  string
	approvedByExternalID  string
	approvedByDisplayName string
	assetRefKind          string
	assetRefExternalID    string
	scopeJSON             []byte
}

func refreshAllSaaSAppRiskReadModels(ctx context.Context, q *gen.Queries) error {
	rows, err := q.ListAllSaaSAppRiskInputs(ctx)
	if err != nil {
		return err
	}
	inputs := make([]saasAppRiskInputRow, 0, len(rows))
	for _, row := range rows {
		inputs = append(inputs, saasAppRiskInputRow{
			saasAppID:                     row.SaasAppID,
			canonicalKey:                  row.CanonicalKey,
			displayName:                   row.DisplayName,
			primaryDomain:                 row.PrimaryDomain,
			vendorName:                    row.VendorName,
			category:                      row.Category,
			sourceKind:                    row.SourceKind,
			sourceName:                    row.SourceName,
			actors30d:                     row.Actors30d,
			hasPrivilegedScope:            row.HasPrivilegedScope,
			hasConfidentialScope:          row.HasConfidentialScope,
			managedState:                  row.ManagedState,
			managedReason:                 row.ManagedReason,
			ownerIdentityID:               row.OwnerIdentityID,
			governanceState:               row.GovernanceState,
			reviewDisposition:             row.ReviewDisposition,
			followUpDueDate:               row.FollowUpDueDate,
			configuredBusinessCriticality: row.ConfiguredBusinessCriticality,
			configuredDataClassification:  row.ConfiguredDataClassification,
			connectorBindingConfigured:    row.ConnectorBindingConfigured,
			connectorBindingEnabled:       row.ConnectorBindingEnabled,
			connectorBindingStale:         row.ConnectorBindingStale,
			connectorBindingHealthy:       row.ConnectorBindingHealthy,
		})
	}
	return refreshSaaSAppRiskReadModels(ctx, q, inputs)
}

func refreshSaaSAppRiskReadModelsBySource(ctx context.Context, q *gen.Queries, sourceKind, sourceName string) error {
	sourceKind = normalizeSourceKind(sourceKind)
	sourceName = strings.TrimSpace(sourceName)
	if sourceKind == "" || sourceName == "" {
		return nil
	}
	rows, err := q.ListSaaSAppRiskInputsBySource(ctx, gen.ListSaaSAppRiskInputsBySourceParams{
		SourceKind: sourceKind,
		SourceName: sourceName,
	})
	if err != nil {
		return err
	}
	inputs := make([]saasAppRiskInputRow, 0, len(rows))
	for _, row := range rows {
		inputs = append(inputs, saasAppRiskInputRow{
			saasAppID:                     row.SaasAppID,
			canonicalKey:                  row.CanonicalKey,
			displayName:                   row.DisplayName,
			primaryDomain:                 row.PrimaryDomain,
			vendorName:                    row.VendorName,
			category:                      row.Category,
			sourceKind:                    row.SourceKind,
			sourceName:                    row.SourceName,
			actors30d:                     row.Actors30d,
			hasPrivilegedScope:            row.HasPrivilegedScope,
			hasConfidentialScope:          row.HasConfidentialScope,
			managedState:                  row.ManagedState,
			managedReason:                 row.ManagedReason,
			ownerIdentityID:               row.OwnerIdentityID,
			governanceState:               row.GovernanceState,
			reviewDisposition:             row.ReviewDisposition,
			followUpDueDate:               row.FollowUpDueDate,
			configuredBusinessCriticality: row.ConfiguredBusinessCriticality,
			configuredDataClassification:  row.ConfiguredDataClassification,
			connectorBindingConfigured:    row.ConnectorBindingConfigured,
			connectorBindingEnabled:       row.ConnectorBindingEnabled,
			connectorBindingStale:         row.ConnectorBindingStale,
			connectorBindingHealthy:       row.ConnectorBindingHealthy,
		})
	}
	return refreshSaaSAppRiskReadModels(ctx, q, inputs)
}

func refreshSaaSAppRiskReadModelByID(ctx context.Context, q *gen.Queries, saasAppID int64) error {
	if saasAppID <= 0 {
		return nil
	}
	row, err := q.GetSaaSAppRiskInputByID(ctx, saasAppID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	return refreshSaaSAppRiskReadModels(ctx, q, []saasAppRiskInputRow{{
		saasAppID:                     row.SaasAppID,
		canonicalKey:                  row.CanonicalKey,
		displayName:                   row.DisplayName,
		primaryDomain:                 row.PrimaryDomain,
		vendorName:                    row.VendorName,
		category:                      row.Category,
		sourceKind:                    row.SourceKind,
		sourceName:                    row.SourceName,
		actors30d:                     row.Actors30d,
		hasPrivilegedScope:            row.HasPrivilegedScope,
		hasConfidentialScope:          row.HasConfidentialScope,
		managedState:                  row.ManagedState,
		managedReason:                 row.ManagedReason,
		ownerIdentityID:               row.OwnerIdentityID,
		governanceState:               row.GovernanceState,
		reviewDisposition:             row.ReviewDisposition,
		followUpDueDate:               row.FollowUpDueDate,
		configuredBusinessCriticality: row.ConfiguredBusinessCriticality,
		configuredDataClassification:  row.ConfiguredDataClassification,
		connectorBindingConfigured:    row.ConnectorBindingConfigured,
		connectorBindingEnabled:       row.ConnectorBindingEnabled,
		connectorBindingStale:         row.ConnectorBindingStale,
		connectorBindingHealthy:       row.ConnectorBindingHealthy,
	}})
}

func refreshSaaSAppRiskReadModels(ctx context.Context, q *gen.Queries, rows []saasAppRiskInputRow) error {
	if len(rows) == 0 {
		return nil
	}
	registry, err := riskpolicy.BuiltinRegistry()
	if err != nil {
		return err
	}

	params := gen.UpsertSaaSAppRiskReadModelsBulkParams{
		SaasAppIds:                     make([]int64, 0, len(rows)),
		RiskScores:                     make([]int32, 0, len(rows)),
		RiskLevels:                     make([]string, 0, len(rows)),
		RiskRanks:                      make([]int32, 0, len(rows)),
		SuggestedBusinessCriticalities: make([]string, 0, len(rows)),
		SuggestedDataClassifications:   make([]string, 0, len(rows)),
		EffectiveBusinessCriticalities: make([]string, 0, len(rows)),
		EffectiveDataClassifications:   make([]string, 0, len(rows)),
		PolicyPacksJsons:               make([][]byte, 0, len(rows)),
	}
	for _, row := range rows {
		result, err := registry.EvaluateSaaS(saasPolicyInput(row))
		if err != nil {
			return err
		}
		policyPacksJSON, err := json.Marshal(result.PolicyPacks)
		if err != nil {
			return err
		}

		params.SaasAppIds = append(params.SaasAppIds, row.saasAppID)
		params.RiskScores = append(params.RiskScores, int32(result.RiskScore))
		params.RiskLevels = append(params.RiskLevels, result.RiskLevel)
		params.RiskRanks = append(params.RiskRanks, int32(result.RiskRank))
		params.SuggestedBusinessCriticalities = append(params.SuggestedBusinessCriticalities, result.SuggestedBusinessCriticality)
		params.SuggestedDataClassifications = append(params.SuggestedDataClassifications, result.SuggestedDataClassification)
		params.EffectiveBusinessCriticalities = append(params.EffectiveBusinessCriticalities, result.EffectiveBusinessCriticality)
		params.EffectiveDataClassifications = append(params.EffectiveDataClassifications, result.EffectiveDataClassification)
		params.PolicyPacksJsons = append(params.PolicyPacksJsons, policyPacksJSON)
	}

	_, err = q.UpsertSaaSAppRiskReadModelsBulk(ctx, params)
	return err
}

func saasPolicyInput(row saasAppRiskInputRow) riskpolicy.SaaSInput {
	return riskpolicy.SaaSInput{
		CanonicalKey:                  row.canonicalKey,
		DisplayName:                   row.displayName,
		PrimaryDomain:                 row.primaryDomain,
		VendorName:                    row.vendorName,
		Category:                      row.category,
		SourceKind:                    row.sourceKind,
		SourceName:                    row.sourceName,
		Actors30d:                     row.actors30d,
		HasPrivilegedScope:            row.hasPrivilegedScope,
		HasConfidentialScope:          row.hasConfidentialScope,
		ManagedState:                  row.managedState,
		ManagedReason:                 row.managedReason,
		OwnerIdentityID:               row.ownerIdentityID,
		GovernanceState:               row.governanceState,
		ReviewDisposition:             row.reviewDisposition,
		FollowUpDueDate:               datePtr(row.followUpDueDate),
		ConfiguredBusinessCriticality: row.configuredBusinessCriticality,
		ConfiguredDataClassification:  row.configuredDataClassification,
		ConnectorBindingConfigured:    row.connectorBindingConfigured,
		ConnectorBindingEnabled:       row.connectorBindingEnabled,
		ConnectorBindingStale:         row.connectorBindingStale,
		ConnectorBindingHealthy:       row.connectorBindingHealthy,
	}
}

func refreshAllCredentialArtifactRiskReadModels(ctx context.Context, q *gen.Queries) error {
	rows, err := q.ListAllCredentialArtifactRiskInputs(ctx)
	if err != nil {
		return err
	}
	inputs := make([]credentialArtifactRiskInputRow, 0, len(rows))
	for _, row := range rows {
		inputs = append(inputs, credentialArtifactRiskInputRowFromAllRow(row))
	}
	if err := q.DeleteAllCredentialArtifactRiskReadModels(ctx); err != nil {
		return err
	}
	return refreshCredentialArtifactRiskReadModels(ctx, q, inputs)
}

func refreshCredentialArtifactRiskReadModelsBySource(ctx context.Context, q *gen.Queries, sourceKind, sourceName string) error {
	sourceKind = normalizeSourceKind(sourceKind)
	sourceName = strings.TrimSpace(sourceName)
	if sourceKind == "" || sourceName == "" {
		return nil
	}
	arg := gen.ListCredentialArtifactRiskInputsBySourceParams{
		SourceKind: sourceKind,
		SourceName: sourceName,
	}
	rows, err := q.ListCredentialArtifactRiskInputsBySource(ctx, arg)
	if err != nil {
		return err
	}
	inputs := make([]credentialArtifactRiskInputRow, 0, len(rows))
	for _, row := range rows {
		inputs = append(inputs, credentialArtifactRiskInputRowFromSourceRow(row))
	}
	if err := q.DeleteCredentialArtifactRiskReadModelsBySource(ctx, gen.DeleteCredentialArtifactRiskReadModelsBySourceParams(arg)); err != nil {
		return err
	}
	return refreshCredentialArtifactRiskReadModels(ctx, q, inputs)
}

func refreshCredentialArtifactRiskReadModels(ctx context.Context, q *gen.Queries, rows []credentialArtifactRiskInputRow) error {
	if len(rows) == 0 {
		return nil
	}
	registry, err := riskpolicy.BuiltinRegistry()
	if err != nil {
		return err
	}

	evaluatedAt := time.Now().UTC()
	params := gen.UpsertCredentialArtifactRiskReadModelsBulkParams{
		CredentialArtifactIds: make([]int64, 0, len(rows)),
		RiskLevels:            make([]string, 0, len(rows)),
		RiskRanks:             make([]int32, 0, len(rows)),
		RiskSignalsJsons:      make([][]byte, 0, len(rows)),
		PolicyPacksJsons:      make([][]byte, 0, len(rows)),
	}
	for _, row := range rows {
		result, err := registry.EvaluateCredential(credentialArtifactPolicyInput(row, evaluatedAt))
		if err != nil {
			return err
		}
		riskSignalsJSON, err := json.Marshal(result.Signals)
		if err != nil {
			return err
		}
		policyPacksJSON, err := json.Marshal(result.PolicyPacks)
		if err != nil {
			return err
		}

		params.CredentialArtifactIds = append(params.CredentialArtifactIds, row.credentialArtifactID)
		params.RiskLevels = append(params.RiskLevels, result.RiskLevel)
		params.RiskRanks = append(params.RiskRanks, int32(result.RiskRank))
		params.RiskSignalsJsons = append(params.RiskSignalsJsons, riskSignalsJSON)
		params.PolicyPacksJsons = append(params.PolicyPacksJsons, policyPacksJSON)
	}

	_, err = q.UpsertCredentialArtifactRiskReadModelsBulk(ctx, params)
	return err
}

func credentialArtifactPolicyInput(row credentialArtifactRiskInputRow, evaluatedAt time.Time) riskpolicy.CredentialInput {
	return riskpolicy.CredentialInput{
		SourceKind:            row.sourceKind,
		SourceName:            row.sourceName,
		CredentialKind:        row.credentialKind,
		Status:                row.status,
		ExpiresAt:             timestamptzPtr(row.expiresAtSource),
		LastUsedAt:            timestamptzPtr(row.lastUsedAtSource),
		CreatedAt:             timestamptzPtr(row.createdAtSource),
		CreatedByExternalID:   row.createdByExternalID,
		CreatedByDisplayName:  row.createdByDisplayName,
		ApprovedByExternalID:  row.approvedByExternalID,
		ApprovedByDisplayName: row.approvedByDisplayName,
		AssetRefKind:          row.assetRefKind,
		AssetRefExternalID:    row.assetRefExternalID,
		ScopeJSON:             row.scopeJSON,
		EvaluatedAt:           evaluatedAt,
	}
}

func credentialArtifactRiskInputRowFromAllRow(row gen.ListAllCredentialArtifactRiskInputsRow) credentialArtifactRiskInputRow {
	return credentialArtifactRiskInputRow{
		credentialArtifactID:  row.CredentialArtifactID,
		sourceKind:            row.SourceKind,
		sourceName:            row.SourceName,
		credentialKind:        row.CredentialKind,
		status:                row.Status,
		expiresAtSource:       row.ExpiresAtSource,
		lastUsedAtSource:      row.LastUsedAtSource,
		createdAtSource:       row.CreatedAtSource,
		createdByExternalID:   row.CreatedByExternalID,
		createdByDisplayName:  row.CreatedByDisplayName,
		approvedByExternalID:  row.ApprovedByExternalID,
		approvedByDisplayName: row.ApprovedByDisplayName,
		assetRefKind:          row.AssetRefKind,
		assetRefExternalID:    row.AssetRefExternalID,
		scopeJSON:             row.ScopeJson,
	}
}

func credentialArtifactRiskInputRowFromSourceRow(row gen.ListCredentialArtifactRiskInputsBySourceRow) credentialArtifactRiskInputRow {
	return credentialArtifactRiskInputRow{
		credentialArtifactID:  row.CredentialArtifactID,
		sourceKind:            row.SourceKind,
		sourceName:            row.SourceName,
		credentialKind:        row.CredentialKind,
		status:                row.Status,
		expiresAtSource:       row.ExpiresAtSource,
		lastUsedAtSource:      row.LastUsedAtSource,
		createdAtSource:       row.CreatedAtSource,
		createdByExternalID:   row.CreatedByExternalID,
		createdByDisplayName:  row.CreatedByDisplayName,
		approvedByExternalID:  row.ApprovedByExternalID,
		approvedByDisplayName: row.ApprovedByDisplayName,
		assetRefKind:          row.AssetRefKind,
		assetRefExternalID:    row.AssetRefExternalID,
		scopeJSON:             row.ScopeJson,
	}
}

func datePtr(value pgtype.Date) *time.Time {
	if !value.Valid {
		return nil
	}
	t := value.Time.UTC()
	return &t
}

func timestamptzPtr(value pgtype.Timestamptz) *time.Time {
	if !value.Valid {
		return nil
	}
	t := value.Time.UTC()
	return &t
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

func refreshNonHumanPrincipalSource(ctx context.Context, q *gen.Queries, sourceKind, sourceName string) error {
	sourceKind = normalizeSourceKind(sourceKind)
	sourceName = strings.TrimSpace(sourceName)
	if sourceKind == "" || sourceName == "" {
		return nil
	}
	arg := gen.ListNonHumanPrincipalRiskInputsBySourceParams{
		SourceKind: sourceKind,
		SourceName: sourceName,
	}
	rows, err := q.ListNonHumanPrincipalRiskInputsBySource(ctx, arg)
	if err != nil {
		return err
	}
	inputs := make([]nonHumanPrincipalRiskInputRow, 0, len(rows))
	for _, row := range rows {
		inputs = append(inputs, nonHumanPrincipalRiskInputRowFromSourceRow(row))
	}
	if err := q.DeleteNonHumanPrincipalPolicyReadModelsBySource(ctx, gen.DeleteNonHumanPrincipalPolicyReadModelsBySourceParams(arg)); err != nil {
		return err
	}
	return refreshNonHumanPrincipalReadModels(ctx, q, inputs)
}

func refreshAllNonHumanPrincipalReadModels(ctx context.Context, q *gen.Queries) error {
	rows, err := q.ListAllNonHumanPrincipalRiskInputs(ctx)
	if err != nil {
		return err
	}
	inputs := make([]nonHumanPrincipalRiskInputRow, 0, len(rows))
	for _, row := range rows {
		inputs = append(inputs, nonHumanPrincipalRiskInputRowFromAllRow(row))
	}
	if err := q.DeleteAllNonHumanPrincipalReadModels(ctx); err != nil {
		return err
	}
	return refreshNonHumanPrincipalReadModels(ctx, q, inputs)
}

func refreshNonHumanPrincipalReadModels(ctx context.Context, q *gen.Queries, rows []nonHumanPrincipalRiskInputRow) error {
	if len(rows) == 0 {
		return nil
	}
	registry, err := riskpolicy.BuiltinRegistry()
	if err != nil {
		return err
	}

	params := gen.UpsertNonHumanPrincipalReadModelsBulkParams{
		PrincipalRefs:                 make([]string, 0, len(rows)),
		IdentityIds:                   make([]int64, 0, len(rows)),
		AppAssetIds:                   make([]int64, 0, len(rows)),
		PrincipalTypes:                make([]string, 0, len(rows)),
		SourceKinds:                   make([]string, 0, len(rows)),
		SourceNames:                   make([]string, 0, len(rows)),
		DisplayNames:                  make([]string, 0, len(rows)),
		SecondaryNames:                make([]string, 0, len(rows)),
		LinkedAssetsCounts:            make([]int64, 0, len(rows)),
		LinkedCredentialsCounts:       make([]int64, 0, len(rows)),
		LastSeenAts:                   make([]pgtype.Timestamptz, 0, len(rows)),
		ActivityStates:                make([]string, 0, len(rows)),
		FreshnessStates:               make([]string, 0, len(rows)),
		GovernanceStates:              make([]string, 0, len(rows)),
		AccountableOwnerIdentityIds:   make([]int64, 0, len(rows)),
		AccountableOwnerDisplayNames:  make([]string, 0, len(rows)),
		AccountableOwnerPrimaryEmails: make([]string, 0, len(rows)),
		OwnerPresences:                make([]string, 0, len(rows)),
		HasCriticalCredentials:        make([]bool, 0, len(rows)),
		HasHighRiskCredentials:        make([]bool, 0, len(rows)),
		HasExpiredCredentials:         make([]bool, 0, len(rows)),
		HasExpiringCredentials:        make([]bool, 0, len(rows)),
		HasUnusedCredentials:          make([]bool, 0, len(rows)),
		HasStaleEvidences:             make([]bool, 0, len(rows)),
		RiskReasonCounts:              make([]int32, 0, len(rows)),
		RiskLevels:                    make([]string, 0, len(rows)),
		RiskSignalsJsons:              make([][]byte, 0, len(rows)),
		PolicyPacksJsons:              make([][]byte, 0, len(rows)),
	}
	for _, row := range rows {
		result, err := registry.EvaluateIdentity(nonHumanPrincipalPolicyInput(row))
		if err != nil {
			return err
		}
		riskSignalsJSON, err := json.Marshal(result.Signals)
		if err != nil {
			return err
		}
		policyPacksJSON, err := json.Marshal(result.PolicyPacks)
		if err != nil {
			return err
		}

		params.PrincipalRefs = append(params.PrincipalRefs, row.principalRef)
		params.IdentityIds = append(params.IdentityIds, row.identityID)
		params.AppAssetIds = append(params.AppAssetIds, row.appAssetID)
		params.PrincipalTypes = append(params.PrincipalTypes, row.principalType)
		params.SourceKinds = append(params.SourceKinds, row.sourceKind)
		params.SourceNames = append(params.SourceNames, row.sourceName)
		params.DisplayNames = append(params.DisplayNames, row.displayName)
		params.SecondaryNames = append(params.SecondaryNames, row.secondaryName)
		params.LinkedAssetsCounts = append(params.LinkedAssetsCounts, row.linkedAssetsCount)
		params.LinkedCredentialsCounts = append(params.LinkedCredentialsCounts, row.linkedCredentialsCount)
		params.LastSeenAts = append(params.LastSeenAts, row.lastSeenAt)
		params.ActivityStates = append(params.ActivityStates, row.activityState)
		params.FreshnessStates = append(params.FreshnessStates, row.freshnessState)
		params.GovernanceStates = append(params.GovernanceStates, row.governanceState)
		params.AccountableOwnerIdentityIds = append(params.AccountableOwnerIdentityIds, row.accountableOwnerIdentityID)
		params.AccountableOwnerDisplayNames = append(params.AccountableOwnerDisplayNames, row.accountableOwnerDisplayName)
		params.AccountableOwnerPrimaryEmails = append(params.AccountableOwnerPrimaryEmails, row.accountableOwnerPrimaryEmail)
		params.OwnerPresences = append(params.OwnerPresences, row.ownerPresence)
		params.HasCriticalCredentials = append(params.HasCriticalCredentials, row.hasCriticalCredential)
		params.HasHighRiskCredentials = append(params.HasHighRiskCredentials, row.hasHighRiskCredential)
		params.HasExpiredCredentials = append(params.HasExpiredCredentials, row.hasExpiredCredential)
		params.HasExpiringCredentials = append(params.HasExpiringCredentials, row.hasExpiringCredential)
		params.HasUnusedCredentials = append(params.HasUnusedCredentials, row.hasUnusedCredential)
		params.HasStaleEvidences = append(params.HasStaleEvidences, row.hasStaleEvidence)
		params.RiskReasonCounts = append(params.RiskReasonCounts, int32(result.RiskReasonCount))
		params.RiskLevels = append(params.RiskLevels, result.RiskLevel)
		params.RiskSignalsJsons = append(params.RiskSignalsJsons, riskSignalsJSON)
		params.PolicyPacksJsons = append(params.PolicyPacksJsons, policyPacksJSON)
	}

	_, err = q.UpsertNonHumanPrincipalReadModelsBulk(ctx, params)
	return err
}

func nonHumanPrincipalPolicyInput(row nonHumanPrincipalRiskInputRow) riskpolicy.IdentityInput {
	return riskpolicy.IdentityInput{
		IdentityID:             row.identityID,
		PrincipalRef:           row.principalRef,
		PrincipalType:          row.principalType,
		SourceKind:             row.sourceKind,
		SourceName:             row.sourceName,
		DisplayName:            row.displayName,
		PrimaryEmail:           row.primaryEmail,
		LastSeenAt:             timestamptzPtr(row.lastSeenAt),
		OwnerPresence:          row.ownerPresence,
		GovernanceState:        row.governanceState,
		LinkedAssetsCount:      row.linkedAssetsCount,
		LinkedCredentialsCount: row.linkedCredentialsCount,
		HasCriticalCredential:  row.hasCriticalCredential,
		HasHighRiskCredential:  row.hasHighRiskCredential,
		HasExpiredCredential:   row.hasExpiredCredential,
		HasExpiringCredential:  row.hasExpiringCredential,
		HasUnusedCredential:    row.hasUnusedCredential,
		HasStaleEvidence:       row.hasStaleEvidence,
	}
}

func nonHumanPrincipalRiskInputRowFromAllRow(row gen.ListAllNonHumanPrincipalRiskInputsRow) nonHumanPrincipalRiskInputRow {
	return nonHumanPrincipalRiskInputRow{
		principalRef:                 row.PrincipalRef,
		identityID:                   row.IdentityID,
		appAssetID:                   row.AppAssetID,
		principalType:                row.PrincipalType,
		sourceKind:                   row.SourceKind,
		sourceName:                   row.SourceName,
		displayName:                  row.DisplayName,
		secondaryName:                row.SecondaryName,
		primaryEmail:                 row.PrimaryEmail,
		linkedAssetsCount:            row.LinkedAssetsCount,
		linkedCredentialsCount:       row.LinkedCredentialsCount,
		lastSeenAt:                   row.LastSeenAt,
		activityState:                row.ActivityState,
		freshnessState:               row.FreshnessState,
		governanceState:              row.GovernanceState,
		accountableOwnerIdentityID:   row.AccountableOwnerIdentityID,
		accountableOwnerDisplayName:  row.AccountableOwnerDisplayName,
		accountableOwnerPrimaryEmail: row.AccountableOwnerPrimaryEmail,
		ownerPresence:                row.OwnerPresence,
		hasCriticalCredential:        row.HasCriticalCredential,
		hasHighRiskCredential:        row.HasHighRiskCredential,
		hasExpiredCredential:         row.HasExpiredCredential,
		hasExpiringCredential:        row.HasExpiringCredential,
		hasUnusedCredential:          row.HasUnusedCredential,
		hasStaleEvidence:             row.HasStaleEvidence,
	}
}

func nonHumanPrincipalRiskInputRowFromSourceRow(row gen.ListNonHumanPrincipalRiskInputsBySourceRow) nonHumanPrincipalRiskInputRow {
	return nonHumanPrincipalRiskInputRow{
		principalRef:                 row.PrincipalRef,
		identityID:                   row.IdentityID,
		appAssetID:                   row.AppAssetID,
		principalType:                row.PrincipalType,
		sourceKind:                   row.SourceKind,
		sourceName:                   row.SourceName,
		displayName:                  row.DisplayName,
		secondaryName:                row.SecondaryName,
		primaryEmail:                 row.PrimaryEmail,
		linkedAssetsCount:            row.LinkedAssetsCount,
		linkedCredentialsCount:       row.LinkedCredentialsCount,
		lastSeenAt:                   row.LastSeenAt,
		activityState:                row.ActivityState,
		freshnessState:               row.FreshnessState,
		governanceState:              row.GovernanceState,
		accountableOwnerIdentityID:   row.AccountableOwnerIdentityID,
		accountableOwnerDisplayName:  row.AccountableOwnerDisplayName,
		accountableOwnerPrimaryEmail: row.AccountableOwnerPrimaryEmail,
		ownerPresence:                row.OwnerPresence,
		hasCriticalCredential:        row.HasCriticalCredential,
		hasHighRiskCredential:        row.HasHighRiskCredential,
		hasExpiredCredential:         row.HasExpiredCredential,
		hasExpiringCredential:        row.HasExpiringCredential,
		hasUnusedCredential:          row.HasUnusedCredential,
		hasStaleEvidence:             row.HasStaleEvidence,
	}
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
