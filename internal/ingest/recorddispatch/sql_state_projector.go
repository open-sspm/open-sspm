package recorddispatch

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
	"github.com/open-sspm/open-sspm/internal/records"
)

type SQLStateProjector struct {
	q      *gen.Queries
	runID  int64
	counts map[string]int64
}

func NewSQLStateProjector(q *gen.Queries, runID int64) *SQLStateProjector {
	return &SQLStateProjector{q: q, runID: runID, counts: make(map[string]int64)}
}

func (p *SQLStateProjector) Counts() map[string]int64 {
	out := make(map[string]int64)
	if p == nil {
		return out
	}
	for key, value := range p.counts {
		out[key] = value
	}
	return out
}

func (p *SQLStateProjector) addCount(key string, value int64) {
	if p.counts == nil {
		p.counts = make(map[string]int64)
	}
	p.counts[key] += value
}

func (p *SQLStateProjector) UpsertState(ctx context.Context, record records.StateUpsert) error {
	if err := p.validate(record.SourceKind()); err != nil {
		return err
	}
	sourceKind := record.SourceKind()
	sourceName := record.SourceName()

	switch payload := record.Payload.(type) {
	case records.IdentityPayload:
		entityCategory := stringAttr(payload.ProviderAttrs, "entity_category", registry.EntityCategoryUser)
		return p.upsertSourceAccount(ctx, sourceKind, sourceName, registry.SourceAccountRow{
			ExternalID:      payload.ExternalID,
			Email:           payload.Email,
			DisplayName:     payload.DisplayName,
			AccountKind:     stringAttr(payload.ProviderAttrs, "account_kind", registry.AccountKindHuman),
			EntityCategory:  entityCategory,
			RawJSON:         sourceAccountRaw(payload.ToEnvelope(), payload.Status, entityCategory),
			LastLoginAt:     timestamptzAttr(payload.ProviderAttrs, "last_login_at"),
			LastLoginIP:     stringAttr(payload.ProviderAttrs, "last_login_ip", ""),
			LastLoginRegion: stringAttr(payload.ProviderAttrs, "last_login_region", ""),
		})
	case records.GroupPayload:
		entityCategory := stringAttr(payload.ProviderAttrs, "entity_category", registry.EntityCategoryGroup)
		return p.upsertSourceAccount(ctx, sourceKind, sourceName, registry.SourceAccountRow{
			ExternalID:      payload.ExternalID,
			Email:           stringAttr(payload.ProviderAttrs, "email", ""),
			DisplayName:     payload.DisplayName,
			AccountKind:     stringAttr(payload.ProviderAttrs, "account_kind", registry.AccountKindUnknown),
			EntityCategory:  entityCategory,
			RawJSON:         sourceAccountRaw(payload.ToEnvelope(), stringAttr(payload.ProviderAttrs, "status", ""), entityCategory),
			LastLoginAt:     timestamptzAttr(payload.ProviderAttrs, "last_login_at"),
			LastLoginIP:     stringAttr(payload.ProviderAttrs, "last_login_ip", ""),
			LastLoginRegion: stringAttr(payload.ProviderAttrs, "last_login_region", ""),
		})
	case records.ServicePrincipalPayload:
		entityCategory := stringAttr(payload.ProviderAttrs, "entity_category", registry.EntityCategoryServicePrincipal)
		return p.upsertSourceAccount(ctx, sourceKind, sourceName, registry.SourceAccountRow{
			ExternalID:      payload.ExternalID,
			Email:           payload.Email,
			DisplayName:     payload.DisplayName,
			AccountKind:     stringAttr(payload.ProviderAttrs, "account_kind", registry.AccountKindService),
			EntityCategory:  entityCategory,
			RawJSON:         sourceAccountRaw(payload.ToEnvelope(), payload.Status, entityCategory),
			LastLoginAt:     timestamptzAttr(payload.ProviderAttrs, "last_login_at"),
			LastLoginIP:     stringAttr(payload.ProviderAttrs, "last_login_ip", ""),
			LastLoginRegion: stringAttr(payload.ProviderAttrs, "last_login_region", ""),
		})
	case records.EntitlementPayload:
		return p.upsertGenericEntitlement(ctx, sourceKind, sourceName, payload)
	case records.AppAssetPayload:
		return p.upsertAppAsset(ctx, sourceKind, sourceName, payload)
	case records.AppAssetOwnerPayload:
		return p.upsertAppAssetOwner(ctx, sourceKind, sourceName, payload)
	case records.CredentialPayload:
		return p.upsertCredential(ctx, sourceKind, sourceName, payload)
	case records.CredentialAuditEventPayload:
		return p.upsertCredentialAuditEvent(ctx, sourceKind, sourceName, payload)
	case records.DiscoveryEvidencePayload:
		return p.upsertDiscoveryEvidence(ctx, sourceKind, sourceName, record, payload)
	default:
		return fmt.Errorf("unsupported SQL state payload %T", record.Payload)
	}
}

func (p *SQLStateProjector) DeleteState(ctx context.Context, record records.StateDelete) error {
	if err := p.validate(record.SourceKind()); err != nil {
		return err
	}
	key := firstNonEmptyTrimmed(record.Key, record.ProviderID)
	if key == "" {
		return errors.New("state delete key or provider id is required")
	}

	switch record.Resource {
	case records.ResourceIdentity, records.ResourceGroup, records.ResourceServicePrincipal:
		_, err := p.q.ExpireSourceAccountsByExternalIDs(ctx, gen.ExpireSourceAccountsByExternalIDsParams{
			ExpiredRunID: p.runID,
			SourceKind:   record.SourceKind(),
			SourceName:   record.SourceName(),
			ExternalIds:  []string{key},
		})
		return err
	case records.ResourceAppAsset:
		assetKind, externalID, ok := strings.Cut(key, ":")
		if !ok || strings.TrimSpace(assetKind) == "" || strings.TrimSpace(externalID) == "" {
			return fmt.Errorf("app asset delete key %q must be asset_kind:external_id", key)
		}
		_, err := p.q.ExpireAppAssetsBySourceKindAndExternalIDs(ctx, gen.ExpireAppAssetsBySourceKindAndExternalIDsParams{
			ExpiredRunID: p.runID,
			SourceKind:   record.SourceKind(),
			SourceName:   record.SourceName(),
			AssetKind:    strings.TrimSpace(assetKind),
			ExternalIds:  []string{strings.TrimSpace(externalID)},
		})
		return err
	default:
		return fmt.Errorf("state delete for %s is not implemented", record.Resource)
	}
}

func (p *SQLStateProjector) BeginSnapshot(context.Context, records.SnapshotBegin) error {
	return nil
}

func (p *SQLStateProjector) CompleteSnapshot(ctx context.Context, record records.SnapshotComplete) error {
	if err := p.validate(record.SourceKind()); err != nil {
		return err
	}
	if !record.Complete {
		return nil
	}
	sourceKind := record.SourceKind()
	sourceName := record.SourceName()
	runID := registry.PgInt8(p.runID)

	switch record.Resource {
	case records.ResourceIdentity, records.ResourceGroup, records.ResourceServicePrincipal:
		observed, err := p.q.PromoteSourceAccountsSeenInRun(ctx, gen.PromoteSourceAccountsSeenInRunParams{
			LastObservedRunID: runID,
			SourceKind:        sourceKind,
			SourceName:        sourceName,
		})
		if err != nil {
			return err
		}
		p.addCount("source_accounts_observed", observed)
		if record.ExpireAbsent {
			expired, err := p.q.ExpireSourceAccountsNotSeenInRun(ctx, gen.ExpireSourceAccountsNotSeenInRunParams{
				ExpiredRunID: runID,
				SourceKind:   sourceKind,
				SourceName:   sourceName,
			})
			p.addCount("source_accounts_expired", expired)
			return err
		}
	case records.ResourceEntitlement:
		observed, err := p.q.PromoteEntitlementsSeenInRunBySource(ctx, gen.PromoteEntitlementsSeenInRunBySourceParams{
			LastObservedRunID: runID,
			SourceKind:        sourceKind,
			SourceName:        sourceName,
		})
		if err != nil {
			return err
		}
		p.addCount("entitlements_observed", observed)
		if record.ExpireAbsent {
			expired, err := p.q.ExpireEntitlementsNotSeenInRunBySource(ctx, gen.ExpireEntitlementsNotSeenInRunBySourceParams{
				ExpiredRunID: runID,
				SourceKind:   sourceKind,
				SourceName:   sourceName,
			})
			p.addCount("entitlements_expired", expired)
			return err
		}
	case records.ResourceAppAsset:
		observed, err := p.q.PromoteAppAssetsSeenInRunBySource(ctx, gen.PromoteAppAssetsSeenInRunBySourceParams{
			LastObservedRunID: p.runID,
			SourceKind:        sourceKind,
			SourceName:        sourceName,
		})
		if err != nil {
			return err
		}
		p.addCount("app_assets_observed", observed)
		if record.ExpireAbsent {
			expired, err := p.q.ExpireAppAssetsNotSeenInRunBySource(ctx, gen.ExpireAppAssetsNotSeenInRunBySourceParams{
				ExpiredRunID: p.runID,
				SourceKind:   sourceKind,
				SourceName:   sourceName,
			})
			p.addCount("app_assets_expired", expired)
			return err
		}
	case records.ResourceAppAssetOwner:
		observed, err := p.q.PromoteAppAssetOwnersSeenInRunBySource(ctx, gen.PromoteAppAssetOwnersSeenInRunBySourceParams{
			LastObservedRunID: p.runID,
			SourceKind:        sourceKind,
			SourceName:        sourceName,
		})
		if err != nil {
			return err
		}
		p.addCount("app_asset_owners_observed", observed)
		if record.ExpireAbsent {
			expired, err := p.q.ExpireAppAssetOwnersNotSeenInRunBySource(ctx, gen.ExpireAppAssetOwnersNotSeenInRunBySourceParams{
				ExpiredRunID: p.runID,
				SourceKind:   sourceKind,
				SourceName:   sourceName,
			})
			p.addCount("app_asset_owners_expired", expired)
			return err
		}
	case records.ResourceCredential:
		observed, err := p.q.PromoteCredentialArtifactsSeenInRunBySource(ctx, gen.PromoteCredentialArtifactsSeenInRunBySourceParams{
			LastObservedRunID: p.runID,
			SourceKind:        sourceKind,
			SourceName:        sourceName,
		})
		if err != nil {
			return err
		}
		p.addCount("credential_artifacts_observed", observed)
		if record.ExpireAbsent {
			expired, err := p.q.ExpireCredentialArtifactsNotSeenInRunBySource(ctx, gen.ExpireCredentialArtifactsNotSeenInRunBySourceParams{
				ExpiredRunID: p.runID,
				SourceKind:   sourceKind,
				SourceName:   sourceName,
			})
			p.addCount("credential_artifacts_expired", expired)
			return err
		}
	case records.ResourceDiscoveryEvidence:
		observed, err := p.q.PromoteSaaSAppSourcesSeenInRunBySource(ctx, gen.PromoteSaaSAppSourcesSeenInRunBySourceParams{
			LastObservedRunID: p.runID,
			SourceKind:        sourceKind,
			SourceName:        sourceName,
		})
		if err != nil {
			return err
		}
		p.addCount("saas_app_sources_observed", observed)
		observed, err = p.q.PromoteSaaSAppEventsSeenInRunBySource(ctx, gen.PromoteSaaSAppEventsSeenInRunBySourceParams{
			LastObservedRunID: p.runID,
			SourceKind:        sourceKind,
			SourceName:        sourceName,
		})
		if err != nil {
			return err
		}
		p.addCount("saas_app_events_observed", observed)
		if record.ExpireAbsent {
			expired, err := p.q.ExpireSaaSAppSourcesNotSeenInRunBySource(ctx, gen.ExpireSaaSAppSourcesNotSeenInRunBySourceParams{
				ExpiredRunID: p.runID,
				SourceKind:   sourceKind,
				SourceName:   sourceName,
			})
			if err != nil {
				return err
			}
			p.addCount("saas_app_sources_expired", expired)
			expired, err = p.q.ExpireSaaSAppEventsNotSeenInRunBySource(ctx, gen.ExpireSaaSAppEventsNotSeenInRunBySourceParams{
				ExpiredRunID: p.runID,
				SourceKind:   sourceKind,
				SourceName:   sourceName,
			})
			p.addCount("saas_app_events_expired", expired)
			return err
		}
	case records.ResourceAuditEvent:
		return nil
	default:
		return fmt.Errorf("unsupported SQL snapshot resource %s", record.Resource)
	}
	return nil
}

func (p *SQLStateProjector) validate(sourceKind string) error {
	if p == nil || p.q == nil {
		return errors.New("SQL state projector is not configured")
	}
	if p.runID <= 0 {
		return errors.New("SQL state projector run id is required")
	}
	if strings.TrimSpace(sourceKind) == "" {
		return errors.New("SQL state projector source kind is required")
	}
	return nil
}

func (p *SQLStateProjector) upsertSourceAccount(ctx context.Context, sourceKind, sourceName string, row registry.SourceAccountRow) error {
	_, err := registry.WriteSourceAccountRows(ctx, p.q, registry.WriteSourceAccountRowsParams{
		SourceKind: sourceKind,
		SourceName: sourceName,
		RunID:      p.runID,
		Rows:       []registry.SourceAccountRow{row},
	})
	return err
}

func (p *SQLStateProjector) upsertGenericEntitlement(ctx context.Context, sourceKind, sourceName string, payload records.EntitlementPayload) error {
	resource := strings.TrimSpace(payload.Target.ExternalID)
	if resource == "" {
		resource = strings.TrimSpace(payload.Scope)
	}
	_, err := registry.WriteEntitlementRows(ctx, p.q, registry.WriteEntitlementRowsParams{
		SourceKind: sourceKind,
		SourceName: sourceName,
		RunID:      p.runID,
		Rows: []registry.EntitlementRow{{
			AccountExternalID: strings.TrimSpace(payload.Subject.ExternalID),
			Kind:              strings.TrimSpace(payload.Kind),
			Resource:          resource,
			Permission:        strings.TrimSpace(payload.Permission),
			RawJSON:           jsonBytes(payload.ToEnvelope()),
		}},
	})
	return err
}

func (p *SQLStateProjector) upsertAppAsset(ctx context.Context, sourceKind, sourceName string, payload records.AppAssetPayload) error {
	_, err := p.q.UpsertAppAssetsBulkBySource(ctx, gen.UpsertAppAssetsBulkBySourceParams{
		SourceKind:        sourceKind,
		SourceName:        sourceName,
		SeenInRunID:       p.runID,
		AssetKinds:        []string{strings.TrimSpace(payload.AssetKind)},
		ExternalIds:       []string{strings.TrimSpace(payload.ExternalID)},
		ParentExternalIds: []string{strings.TrimSpace(payload.ParentExternalID)},
		DisplayNames:      []string{strings.TrimSpace(payload.DisplayName)},
		Statuses:          []string{strings.TrimSpace(payload.Status)},
		CreatedAtSources:  []pgtype.Timestamptz{timeToTimestamptz(payload.CreatedAtSource)},
		UpdatedAtSources:  []pgtype.Timestamptz{timeToTimestamptz(payload.UpdatedAtSource)},
		RawJsons:          [][]byte{payloadRaw(payload.ToEnvelope())},
	})
	return err
}

func (p *SQLStateProjector) upsertAppAssetOwner(ctx context.Context, sourceKind, sourceName string, payload records.AppAssetOwnerPayload) error {
	_, err := p.q.UpsertAppAssetOwnersBulkBySource(ctx, gen.UpsertAppAssetOwnersBulkBySourceParams{
		SourceKind:        sourceKind,
		SourceName:        sourceName,
		SeenInRunID:       p.runID,
		AssetKinds:        []string{strings.TrimSpace(payload.AssetKind)},
		AssetExternalIds:  []string{strings.TrimSpace(payload.AssetExternalID)},
		OwnerKinds:        []string{strings.TrimSpace(payload.OwnerKind)},
		OwnerExternalIds:  []string{strings.TrimSpace(payload.OwnerExternalID)},
		OwnerDisplayNames: []string{strings.TrimSpace(payload.OwnerDisplayName)},
		OwnerEmails:       []string{strings.TrimSpace(payload.OwnerEmail)},
		RawJsons:          [][]byte{payloadRaw(payload.ToEnvelope())},
	})
	return err
}

func (p *SQLStateProjector) upsertCredential(ctx context.Context, sourceKind, sourceName string, payload records.CredentialPayload) error {
	_, err := p.q.UpsertCredentialArtifactsBulkBySource(ctx, gen.UpsertCredentialArtifactsBulkBySourceParams{
		SourceKind:             sourceKind,
		SourceName:             sourceName,
		SeenInRunID:            p.runID,
		AssetRefKinds:          []string{strings.TrimSpace(payload.AssetRefKind)},
		AssetRefExternalIds:    []string{strings.TrimSpace(payload.AssetRefExternalID)},
		CredentialKinds:        []string{strings.TrimSpace(payload.CredentialKind)},
		ExternalIds:            []string{strings.TrimSpace(payload.ExternalID)},
		DisplayNames:           []string{strings.TrimSpace(payload.DisplayName)},
		Fingerprints:           []string{strings.TrimSpace(payload.Fingerprint)},
		ScopeJsons:             [][]byte{scopeJSON(payload.ScopeJSON)},
		Statuses:               []string{strings.TrimSpace(payload.Status)},
		CreatedAtSources:       []pgtype.Timestamptz{timeToTimestamptz(payload.CreatedAtSource)},
		ExpiresAtSources:       []pgtype.Timestamptz{timeToTimestamptz(payload.ExpiresAtSource)},
		LastUsedAtSources:      []pgtype.Timestamptz{timeToTimestamptz(payload.LastUsedAtSource)},
		CreatedByKinds:         []string{strings.TrimSpace(payload.CreatedBy.Kind)},
		CreatedByExternalIds:   []string{strings.TrimSpace(payload.CreatedBy.ExternalID)},
		CreatedByDisplayNames:  []string{strings.TrimSpace(payload.CreatedBy.DisplayName)},
		ApprovedByKinds:        []string{strings.TrimSpace(payload.ApprovedBy.Kind)},
		ApprovedByExternalIds:  []string{strings.TrimSpace(payload.ApprovedBy.ExternalID)},
		ApprovedByDisplayNames: []string{strings.TrimSpace(payload.ApprovedBy.DisplayName)},
		RawJsons:               [][]byte{payloadRaw(payload.ToEnvelope())},
	})
	return err
}

func (p *SQLStateProjector) upsertCredentialAuditEvent(ctx context.Context, sourceKind, sourceName string, payload records.CredentialAuditEventPayload) error {
	_, err := p.q.UpsertCredentialAuditEventsBulkBySource(ctx, gen.UpsertCredentialAuditEventsBulkBySourceParams{
		SourceKind:            sourceKind,
		SourceName:            sourceName,
		EventExternalIds:      []string{strings.TrimSpace(payload.EventExternalID)},
		EventTypes:            []string{strings.TrimSpace(payload.EventType)},
		EventTimes:            []pgtype.Timestamptz{timeToTimestamptz(payload.EventTime)},
		ActorKinds:            []string{strings.TrimSpace(payload.ActorKind)},
		ActorExternalIds:      []string{strings.TrimSpace(payload.ActorExternalID)},
		ActorDisplayNames:     []string{strings.TrimSpace(payload.ActorDisplayName)},
		TargetKinds:           []string{strings.TrimSpace(payload.TargetKind)},
		TargetExternalIds:     []string{strings.TrimSpace(payload.TargetExternalID)},
		TargetDisplayNames:    []string{strings.TrimSpace(payload.TargetDisplayName)},
		CredentialKinds:       []string{strings.TrimSpace(payload.CredentialKind)},
		CredentialExternalIds: []string{strings.TrimSpace(payload.CredentialExternalID)},
		RawJsons:              [][]byte{payloadRaw(payload.ToEnvelope())},
	})
	return err
}

func (p *SQLStateProjector) upsertDiscoveryEvidence(ctx context.Context, sourceKind, sourceName string, record records.StateUpsert, payload records.DiscoveryEvidencePayload) error {
	params := discovery.WriteRowsParams{
		SourceKind: sourceKind,
		SourceName: sourceName,
		RunID:      p.runID,
	}
	switch strings.TrimSpace(payload.Kind) {
	case records.DiscoveryEvidenceKindSource:
		params.Sources = []discovery.SourceRow{{
			CanonicalKey:     strings.TrimSpace(payload.CanonicalKey),
			SourceAppID:      strings.TrimSpace(payload.SourceAppID),
			SourceAppName:    strings.TrimSpace(payload.SourceAppName),
			SourceAppDomain:  strings.TrimSpace(payload.SourceAppDomain),
			SourceVendorName: strings.TrimSpace(payload.SourceVendorName),
			SourceCategory:   strings.TrimSpace(payload.SourceCategory),
			SeenAt:           observedAt(record.ObservedAt, payload.ObservedAt),
		}}
	case records.DiscoveryEvidenceKindEvent:
		params.Events = []discovery.EventRow{{
			CanonicalKey:     strings.TrimSpace(payload.CanonicalKey),
			SignalKind:       strings.TrimSpace(payload.SignalKind),
			EventExternalID:  strings.TrimSpace(payload.EventExternalID),
			SourceAppID:      strings.TrimSpace(payload.SourceAppID),
			SourceAppName:    strings.TrimSpace(payload.SourceAppName),
			SourceAppDomain:  strings.TrimSpace(payload.SourceAppDomain),
			SourceVendorName: strings.TrimSpace(payload.SourceVendorName),
			SourceCategory:   strings.TrimSpace(payload.SourceCategory),
			ActorExternalID:  strings.TrimSpace(payload.ActorExternalID),
			ActorEmail:       strings.TrimSpace(payload.ActorEmail),
			ActorDisplayName: strings.TrimSpace(payload.ActorDisplayName),
			ObservedAt:       observedAt(record.ObservedAt, payload.ObservedAt),
			Scopes:           payload.Scopes,
			RawJSON:          payloadRaw(payload.ToEnvelope()),
		}}
	default:
		return fmt.Errorf("unsupported discovery evidence kind %q", payload.Kind)
	}
	return discovery.WriteRows(ctx, p.q, params)
}

func timeToTimestamptz(t time.Time) pgtype.Timestamptz {
	if t.IsZero() {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: t.UTC(), Valid: true}
}

func scopeJSON(raw []byte) []byte {
	if len(raw) == 0 {
		return []byte("{}")
	}
	return registry.NormalizeJSON(raw)
}

func sourceAccountRaw(envelope records.ResourceEnvelope, status, entityCategory string) []byte {
	raw := records.MapFromJSON(payloadRaw(envelope))
	if status = strings.TrimSpace(status); status != "" {
		raw["status"] = status
	}
	if entityCategory = strings.TrimSpace(entityCategory); entityCategory != "" {
		raw["entity_category"] = entityCategory
	}
	return jsonBytes(raw)
}

func firstNonEmptyTrimmed(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
