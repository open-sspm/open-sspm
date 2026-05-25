package recorddispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
	"github.com/open-sspm/open-sspm/internal/records"
)

type OktaStateProjector struct {
	q     *gen.Queries
	runID int64
}

func NewOktaStateProjector(q *gen.Queries, runID int64) *OktaStateProjector {
	return &OktaStateProjector{q: q, runID: runID}
}

func (p *OktaStateProjector) UpsertState(ctx context.Context, record records.StateUpsert) error {
	if p == nil || p.q == nil {
		return errors.New("okta state projector is not configured")
	}
	if p.runID <= 0 {
		return errors.New("okta state projector run id is required")
	}
	if strings.TrimSpace(record.SourceKind()) != "okta" {
		return fmt.Errorf("okta state projector cannot project source kind %q", record.SourceKind())
	}

	switch payload := record.Payload.(type) {
	case records.IdentityPayload:
		return p.upsertIdentity(ctx, record, payload)
	case records.GroupPayload:
		return p.upsertGroup(ctx, record, payload)
	case records.ApplicationPayload:
		return p.upsertApplication(ctx, record, payload)
	case records.EntitlementPayload:
		return p.upsertEntitlement(ctx, record, payload)
	case records.DiscoveryEvidencePayload:
		return p.upsertDiscoveryEvidence(ctx, record, payload)
	default:
		return fmt.Errorf("unsupported okta state payload %T", record.Payload)
	}
}

func (p *OktaStateProjector) DeleteState(ctx context.Context, record records.StateDelete) error {
	if p == nil || p.q == nil {
		return errors.New("okta state projector is not configured")
	}
	if record.Resource != records.ResourceIdentity {
		return fmt.Errorf("okta state delete for %s is not implemented", record.Resource)
	}
	key := strings.TrimSpace(record.Key)
	if key == "" {
		key = strings.TrimSpace(record.ProviderID)
	}
	if key == "" {
		return errors.New("okta state delete key or provider id is required")
	}
	sourceName := record.SourceName()
	if sourceName == "" {
		return errors.New("okta state delete source name is required")
	}
	_, err := p.q.ExpireSourceAccountsByExternalIDs(ctx, gen.ExpireSourceAccountsByExternalIDsParams{
		ExpiredRunID: p.runID,
		SourceKind:   "okta",
		SourceName:   sourceName,
		ExternalIds:  []string{key},
	})
	return err
}

func (p *OktaStateProjector) BeginSnapshot(context.Context, records.SnapshotBegin) error {
	return nil
}

func (p *OktaStateProjector) CompleteSnapshot(ctx context.Context, record records.SnapshotComplete) error {
	if p == nil || p.q == nil {
		return errors.New("okta state projector is not configured")
	}
	if p.runID <= 0 {
		return errors.New("okta state projector run id is required")
	}
	if !record.Complete {
		return nil
	}

	runID := registry.PgInt8(p.runID)
	switch record.Resource {
	case records.ResourceIdentity:
		if _, err := p.q.PromoteSourceAccountsSeenInRun(ctx, gen.PromoteSourceAccountsSeenInRunParams{
			LastObservedRunID: runID,
			SourceKind:        "okta",
			SourceName:        record.SourceName(),
		}); err != nil {
			return err
		}
		if record.ExpireAbsent {
			_, err := p.q.ExpireSourceAccountsNotSeenInRun(ctx, gen.ExpireSourceAccountsNotSeenInRunParams{
				ExpiredRunID: runID,
				SourceKind:   "okta",
				SourceName:   record.SourceName(),
			})
			return err
		}
	case records.ResourceGroup:
		if _, err := p.q.PromoteOktaGroupsSeenInRunBySource(ctx, gen.PromoteOktaGroupsSeenInRunBySourceParams{
			LastObservedRunID: p.runID,
			SourceKind:        "okta",
			SourceName:        record.SourceName(),
		}); err != nil {
			return err
		}
		if _, err := p.q.PromoteOktaGroupMembershipsSeenInRunBySource(ctx, gen.PromoteOktaGroupMembershipsSeenInRunBySourceParams{
			LastObservedRunID: p.runID,
			SourceKind:        "okta",
			SourceName:        record.SourceName(),
		}); err != nil {
			return err
		}
		if record.ExpireAbsent {
			if _, err := p.q.ExpireOktaGroupsNotSeenInRunBySource(ctx, gen.ExpireOktaGroupsNotSeenInRunBySourceParams{
				ExpiredRunID: p.runID,
				SourceKind:   "okta",
				SourceName:   record.SourceName(),
			}); err != nil {
				return err
			}
			_, err := p.q.ExpireOktaGroupMembershipsNotSeenInRunBySource(ctx, gen.ExpireOktaGroupMembershipsNotSeenInRunBySourceParams{
				ExpiredRunID: p.runID,
				SourceKind:   "okta",
				SourceName:   record.SourceName(),
			})
			return err
		}
	case records.ResourceApplication:
		if _, err := p.q.PromoteOktaAppsSeenInRunBySource(ctx, gen.PromoteOktaAppsSeenInRunBySourceParams{
			LastObservedRunID: p.runID,
			SourceKind:        "okta",
			SourceName:        record.SourceName(),
		}); err != nil {
			return err
		}
		if record.ExpireAbsent {
			_, err := p.q.ExpireOktaAppsNotSeenInRunBySource(ctx, gen.ExpireOktaAppsNotSeenInRunBySourceParams{
				ExpiredRunID: p.runID,
				SourceKind:   "okta",
				SourceName:   record.SourceName(),
			})
			return err
		}
	case records.ResourceEntitlement:
		if _, err := p.q.PromoteOktaAppAssignmentsSeenInRunBySource(ctx, gen.PromoteOktaAppAssignmentsSeenInRunBySourceParams{
			LastObservedRunID: p.runID,
			SourceKind:        "okta",
			SourceName:        record.SourceName(),
		}); err != nil {
			return err
		}
		if _, err := p.q.PromoteOktaAppGroupAssignmentsSeenInRunBySource(ctx, gen.PromoteOktaAppGroupAssignmentsSeenInRunBySourceParams{
			LastObservedRunID: p.runID,
			SourceKind:        "okta",
			SourceName:        record.SourceName(),
		}); err != nil {
			return err
		}
		if _, err := p.q.PromoteEntitlementsSeenInRunBySource(ctx, gen.PromoteEntitlementsSeenInRunBySourceParams{
			LastObservedRunID: runID,
			SourceKind:        "okta",
			SourceName:        record.SourceName(),
		}); err != nil {
			return err
		}
		if record.ExpireAbsent {
			if _, err := p.q.ExpireOktaAppAssignmentsNotSeenInRunBySource(ctx, gen.ExpireOktaAppAssignmentsNotSeenInRunBySourceParams{
				ExpiredRunID: p.runID,
				SourceKind:   "okta",
				SourceName:   record.SourceName(),
			}); err != nil {
				return err
			}
			if _, err := p.q.ExpireOktaAppGroupAssignmentsNotSeenInRunBySource(ctx, gen.ExpireOktaAppGroupAssignmentsNotSeenInRunBySourceParams{
				ExpiredRunID: p.runID,
				SourceKind:   "okta",
				SourceName:   record.SourceName(),
			}); err != nil {
				return err
			}
			_, err := p.q.ExpireEntitlementsNotSeenInRunBySource(ctx, gen.ExpireEntitlementsNotSeenInRunBySourceParams{
				ExpiredRunID: runID,
				SourceKind:   "okta",
				SourceName:   record.SourceName(),
			})
			return err
		}
	case records.ResourceDiscoveryEvidence:
		if _, err := p.q.PromoteSaaSAppSourcesSeenInRunBySource(ctx, gen.PromoteSaaSAppSourcesSeenInRunBySourceParams{
			LastObservedRunID: p.runID,
			SourceKind:        "okta",
			SourceName:        record.SourceName(),
		}); err != nil {
			return err
		}
		if _, err := p.q.PromoteSaaSAppEventsSeenInRunBySource(ctx, gen.PromoteSaaSAppEventsSeenInRunBySourceParams{
			LastObservedRunID: p.runID,
			SourceKind:        "okta",
			SourceName:        record.SourceName(),
		}); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported okta snapshot resource %s", record.Resource)
	}
	return nil
}

func (p *OktaStateProjector) upsertIdentity(ctx context.Context, record records.StateUpsert, payload records.IdentityPayload) error {
	raw := payloadRaw(payload.ToEnvelope())
	return p.upsertAccount(ctx, record.SourceName(), registry.SourceAccountRow{
		ExternalID:      payload.ExternalID,
		Email:           payload.Email,
		DisplayName:     payload.DisplayName,
		AccountKind:     stringAttr(payload.ProviderAttrs, "account_kind", registry.AccountKindHuman),
		EntityCategory:  stringAttr(payload.ProviderAttrs, "entity_category", registry.EntityCategoryUser),
		RawJSON:         raw,
		LastLoginAt:     timestamptzAttr(payload.ProviderAttrs, "last_login_at"),
		LastLoginIP:     stringAttr(payload.ProviderAttrs, "last_login_ip", ""),
		LastLoginRegion: stringAttr(payload.ProviderAttrs, "last_login_region", ""),
	})
}

func (p *OktaStateProjector) upsertGroup(ctx context.Context, record records.StateUpsert, payload records.GroupPayload) error {
	if _, err := p.q.UpsertOktaGroupsBulk(ctx, gen.UpsertOktaGroupsBulkParams{
		SeenInRunID: p.runID,
		SourceKind:  "okta",
		SourceName:  record.SourceName(),
		ExternalIds: []string{strings.TrimSpace(payload.ExternalID)},
		Names:       []string{strings.TrimSpace(payload.DisplayName)},
		Types:       []string{strings.TrimSpace(payload.Type)},
		RawJsons:    [][]byte{payloadRaw(payload.ToEnvelope())},
	}); err != nil {
		return err
	}
	return p.upsertGroupAccount(ctx, record.SourceName(), payload)
}

func (p *OktaStateProjector) upsertApplication(ctx context.Context, record records.StateUpsert, payload records.ApplicationPayload) error {
	_, err := p.q.UpsertOktaAppsBulk(ctx, gen.UpsertOktaAppsBulkParams{
		SeenInRunID: p.runID,
		SourceKind:  "okta",
		SourceName:  record.SourceName(),
		ExternalIds: []string{strings.TrimSpace(payload.ExternalID)},
		Labels:      []string{strings.TrimSpace(payload.DisplayName)},
		Names:       []string{strings.TrimSpace(payload.Name)},
		Statuses:    []string{strings.TrimSpace(payload.Status)},
		SignOnModes: []string{strings.TrimSpace(payload.SignOnMode)},
		RawJsons:    [][]byte{payloadRaw(payload.ToEnvelope())},
	})
	return err
}

func (p *OktaStateProjector) upsertEntitlement(ctx context.Context, record records.StateUpsert, payload records.EntitlementPayload) error {
	switch strings.TrimSpace(payload.Kind) {
	case records.EntitlementKindOktaGroupMembership:
		return p.upsertGroupMembership(ctx, payload)
	case records.EntitlementKindOktaAppUserAssignment:
		return p.upsertAppUserAssignment(ctx, record.SourceName(), payload)
	case records.EntitlementKindOktaAppGroupAssignment:
		return p.upsertAppGroupAssignment(ctx, record.SourceName(), payload)
	default:
		return fmt.Errorf("unsupported okta entitlement kind %q", payload.Kind)
	}
}

func (p *OktaStateProjector) upsertDiscoveryEvidence(ctx context.Context, record records.StateUpsert, payload records.DiscoveryEvidencePayload) error {
	params := discovery.WriteRowsParams{
		SourceKind: "okta",
		SourceName: record.SourceName(),
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
		return fmt.Errorf("unsupported okta discovery evidence kind %q", payload.Kind)
	}
	return discovery.WriteRows(ctx, p.q, params)
}

func observedAt(fallback, preferred time.Time) time.Time {
	if !preferred.IsZero() {
		return preferred.UTC()
	}
	if !fallback.IsZero() {
		return fallback.UTC()
	}
	return time.Now().UTC()
}

func (p *OktaStateProjector) upsertGroupMembership(ctx context.Context, payload records.EntitlementPayload) error {
	_, err := p.q.UpsertOktaGroupMembershipsBulkByOktaAccountExternalIDs(ctx, gen.UpsertOktaGroupMembershipsBulkByOktaAccountExternalIDsParams{
		SeenInRunID:            p.runID,
		OktaAccountExternalIds: []string{strings.TrimSpace(payload.Subject.ExternalID)},
		OktaGroupExternalIds:   []string{strings.TrimSpace(payload.Target.ExternalID)},
	})
	return err
}

func (p *OktaStateProjector) upsertAppUserAssignment(ctx context.Context, sourceName string, payload records.EntitlementPayload) error {
	if _, err := p.q.UpsertOktaAppAssignmentsBulkByOktaAccountExternalIDs(ctx, gen.UpsertOktaAppAssignmentsBulkByOktaAccountExternalIDsParams{
		SeenInRunID:            p.runID,
		OktaAccountExternalIds: []string{strings.TrimSpace(payload.Subject.ExternalID)},
		OktaAppExternalIds:     []string{strings.TrimSpace(payload.Target.ExternalID)},
		Scopes:                 []string{strings.TrimSpace(payload.Scope)},
		ProfileJsons:           [][]byte{jsonBytes(payload.Profile)},
		RawJsons:               [][]byte{payloadRaw(payload.ToEnvelope())},
	}); err != nil {
		return err
	}
	_, err := registry.WriteEntitlementRows(ctx, p.q, registry.WriteEntitlementRowsParams{
		SourceKind: "okta",
		SourceName: sourceName,
		RunID:      p.runID,
		Rows: []registry.EntitlementRow{{
			AccountExternalID: strings.TrimSpace(payload.Subject.ExternalID),
			Kind:              "application_assignment",
			Resource:          strings.TrimSpace(payload.Target.ExternalID),
			Permission:        strings.TrimSpace(payload.Scope),
			RawJSON:           payloadRaw(payload.ToEnvelope()),
		}},
	})
	return err
}

func (p *OktaStateProjector) upsertAppGroupAssignment(ctx context.Context, sourceName string, payload records.EntitlementPayload) error {
	priority, err := int32Priority(payload.Priority)
	if err != nil {
		return err
	}
	if _, err := p.q.UpsertOktaAppGroupAssignmentsBulkByExternalIDs(ctx, gen.UpsertOktaAppGroupAssignmentsBulkByExternalIDsParams{
		SeenInRunID:          p.runID,
		SourceKind:           "okta",
		SourceName:           sourceName,
		OktaAppExternalIds:   []string{strings.TrimSpace(payload.Target.ExternalID)},
		OktaGroupExternalIds: []string{strings.TrimSpace(payload.Subject.ExternalID)},
		Priorities:           []int32{priority},
		ProfileJsons:         [][]byte{jsonBytes(payload.Profile)},
		RawJsons:             [][]byte{payloadRaw(payload.ToEnvelope())},
	}); err != nil {
		return err
	}
	_, err = registry.WriteEntitlementRows(ctx, p.q, registry.WriteEntitlementRowsParams{
		SourceKind: "okta",
		SourceName: sourceName,
		RunID:      p.runID,
		Rows: []registry.EntitlementRow{{
			AccountExternalID: oktaGroupAccountExternalID(payload.Subject.ExternalID),
			Kind:              "application_assignment",
			Resource:          strings.TrimSpace(payload.Target.ExternalID),
			Permission:        strconv.Itoa(payload.Priority),
			RawJSON:           payloadRaw(payload.ToEnvelope()),
		}},
	})
	return err
}

func (p *OktaStateProjector) upsertGroupAccount(ctx context.Context, sourceName string, payload records.GroupPayload) error {
	display := strings.TrimSpace(payload.DisplayName)
	if display == "" {
		display = strings.TrimSpace(payload.ExternalID)
	}
	return p.upsertAccount(ctx, sourceName, registry.SourceAccountRow{
		ExternalID:     oktaGroupAccountExternalID(payload.ExternalID),
		DisplayName:    display,
		AccountKind:    registry.AccountKindService,
		EntityCategory: registry.EntityCategoryGroup,
		RawJSON:        registry.WithEntityCategory(payloadRaw(payload.ToEnvelope()), registry.EntityCategoryGroup),
	})
}

func (p *OktaStateProjector) upsertAccount(ctx context.Context, sourceName string, row registry.SourceAccountRow) error {
	_, err := registry.WriteSourceAccountRows(ctx, p.q, registry.WriteSourceAccountRowsParams{
		SourceKind: "okta",
		SourceName: sourceName,
		RunID:      p.runID,
		Rows:       []registry.SourceAccountRow{row},
	})
	return err
}

func payloadRaw(envelope records.ResourceEnvelope) []byte {
	if len(envelope.Raw) > 0 {
		return jsonBytes(envelope.Raw)
	}
	return jsonBytes(envelope)
}

func jsonBytes(value any) []byte {
	if value == nil {
		return []byte("{}")
	}
	b, err := json.Marshal(value)
	if err != nil || len(b) == 0 {
		return []byte("{}")
	}
	return b
}

func stringAttr(attrs map[string]any, key, fallback string) string {
	if attrs == nil {
		return fallback
	}
	if v, ok := attrs[key].(string); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return fallback
}

func timestamptzAttr(attrs map[string]any, key string) pgtype.Timestamptz {
	if attrs == nil {
		return pgtype.Timestamptz{}
	}
	switch v := attrs[key].(type) {
	case time.Time:
		if !v.IsZero() {
			return pgtype.Timestamptz{Time: v.UTC(), Valid: true}
		}
	case *time.Time:
		if v != nil && !v.IsZero() {
			return pgtype.Timestamptz{Time: v.UTC(), Valid: true}
		}
	case string:
		if parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(v)); err == nil {
			return pgtype.Timestamptz{Time: parsed.UTC(), Valid: true}
		}
	}
	return pgtype.Timestamptz{}
}

func oktaGroupAccountExternalID(groupID string) string {
	groupID = strings.TrimSpace(groupID)
	if groupID == "" {
		return ""
	}
	if strings.HasPrefix(groupID, "group:") {
		return groupID
	}
	return "group:" + groupID
}

func int32Priority(priority int) (int32, error) {
	const (
		minInt32 = -1 << 31
		maxInt32 = 1<<31 - 1
	)
	if priority < minInt32 || priority > maxInt32 {
		return 0, fmt.Errorf("okta app group assignment priority %d overflows int32", priority)
	}
	return int32(priority), nil
}
