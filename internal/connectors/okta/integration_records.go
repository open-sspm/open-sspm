package okta

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/capabilities"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
	"github.com/open-sspm/open-sspm/internal/ingest/recorddispatch"
	"github.com/open-sspm/open-sspm/internal/metrics"
	"github.com/open-sspm/open-sspm/internal/records"
)

func beginOktaSnapshot(ctx context.Context, emitter capabilities.RecordEmitter, sourceName string, resource records.ResourceName, runID int64) error {
	if emitter == nil {
		return fmt.Errorf("okta snapshot begin requires record emitter")
	}
	return emitter.BeginSnapshot(ctx, records.SnapshotBegin{
		Source:         records.SourceRef{Kind: "okta", Name: sourceName},
		Resource:       resource,
		Scope:          records.FullScope{Resource: resource},
		Complete:       true,
		StartedAt:      time.Now().UTC(),
		DedupeKeyValue: fmt.Sprintf("okta:%s:snapshot:%s:begin:%d", strings.TrimSpace(sourceName), resource, runID),
	})
}

func completeOktaSnapshot(ctx context.Context, emitter capabilities.RecordEmitter, sourceName string, resource records.ResourceName, runID int64, expireAbsent bool) error {
	if emitter == nil {
		return fmt.Errorf("okta snapshot complete requires record emitter")
	}
	return emitter.CompleteSnapshot(ctx, records.SnapshotComplete{
		Source:         records.SourceRef{Kind: "okta", Name: sourceName},
		Resource:       resource,
		Scope:          records.FullScope{Resource: resource},
		Complete:       true,
		ExpireAbsent:   expireAbsent,
		FinishedAt:     time.Now().UTC(),
		DedupeKeyValue: fmt.Sprintf("okta:%s:snapshot:%s:complete:%d", strings.TrimSpace(sourceName), resource, runID),
	})
}

func finalizeOktaRecordSnapshots(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, runID int64, sourceName string, duration time.Duration) error {
	if q == nil {
		return fmt.Errorf("okta snapshot finalization requires queries")
	}
	if pool == nil {
		return fmt.Errorf("okta snapshot finalization requires database pool")
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	qtx := q.WithTx(tx)
	projector := recorddispatch.NewOktaStateProjector(qtx, runID)
	emitter := recorddispatch.NewDispatcher(nil, projector)

	// ResourceEntitlement finalizes Okta assignment tables and the canonical
	// entitlements derived from them. Expiry queries only touch active rows, so
	// a prior completion for this run is idempotent.
	for _, resource := range []records.ResourceName{
		records.ResourceIdentity,
		records.ResourceGroup,
		records.ResourceApplication,
		records.ResourceEntitlement,
	} {
		if err := completeOktaSnapshot(ctx, emitter, sourceName, resource, runID, true); err != nil {
			return err
		}
	}

	if err := registry.FinalizeRunCountsInTx(ctx, qtx, runID, projector.Counts(), duration, "okta", sourceName); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (i *OktaIntegration) syncOktaAccountsRecords(ctx context.Context, emitter capabilities.RecordEmitter, report func(registry.Event), runID int64, users []User) error {
	if len(users) == 0 {
		report(registry.Event{Source: "okta", Stage: "sync-users", Current: 0, Total: 0, Message: "no users to sync"})
		return nil
	}
	for idx, user := range users {
		id := strings.TrimSpace(user.ID)
		if id == "" {
			continue
		}
		attrs := map[string]any{
			"account_kind":    oktaAccountKind(user),
			"entity_category": registry.EntityCategoryUser,
		}
		if user.LastLoginAt != nil && !user.LastLoginAt.IsZero() {
			attrs["last_login_at"] = user.LastLoginAt.UTC()
		}
		if err := emitter.UpsertState(ctx, records.StateUpsert{
			Source:         records.SourceRef{Kind: "okta", Name: i.sourceName},
			Resource:       records.ResourceIdentity,
			Key:            id,
			ProviderID:     id,
			ObservedAt:     time.Now().UTC(),
			DedupeKeyValue: "okta:identity:" + id,
			Payload: records.IdentityPayload{
				ExternalID:    id,
				Email:         user.Email,
				DisplayName:   user.DisplayName,
				Status:        user.Status,
				ProviderAttrs: attrs,
				Raw:           rawMap(user.RawJSON),
			},
		}); err != nil {
			return fmt.Errorf("emit okta account %s state: %w", id, err)
		}
		report(registry.Event{
			Source:  "okta",
			Stage:   "sync-users",
			Current: int64(idx + 1),
			Total:   int64(len(users)),
			Message: fmt.Sprintf("users %d/%d", idx+1, len(users)),
		})
	}
	return nil
}

func (i *OktaIntegration) syncOktaGroupsRecords(ctx context.Context, emitter capabilities.RecordEmitter, report func(registry.Event), runID int64) error {
	groups, err := i.client.ListGroups(ctx)
	if err != nil {
		return fmt.Errorf("okta list groups: %w", err)
	}
	report(registry.Event{Source: "okta", Stage: "sync-groups", Current: 0, Total: int64(len(groups)), Message: fmt.Sprintf("syncing %d groups", len(groups))})

	for _, group := range groups {
		id := strings.TrimSpace(group.ID)
		if id == "" {
			continue
		}
		if err := emitter.UpsertState(ctx, groupStateRecord(i.sourceName, group)); err != nil {
			return fmt.Errorf("emit okta group %s state: %w", id, err)
		}
	}

	workers := min(len(groups), i.workers)
	if workers < 1 {
		workers = 1
	}

	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup
	jobs := make(chan Group, len(groups))
	var done int64

	worker := func() {
		defer wg.Done()
		for group := range jobs {
			if jobCtx.Err() != nil {
				return
			}
			userExternalIDs, err := i.client.ListGroupUserIDs(jobCtx, group.ID)
			if err != nil {
				errOnce.Do(func() {
					firstErr = fmt.Errorf("okta group %s users: %w", group.ID, err)
					cancel()
				})
				return
			}
			for _, userExternalID := range userExternalIDs {
				userExternalID = strings.TrimSpace(userExternalID)
				if userExternalID == "" {
					continue
				}
				if err := emitter.UpsertState(jobCtx, records.StateUpsert{
					Source:         records.SourceRef{Kind: "okta", Name: i.sourceName},
					Resource:       records.ResourceEntitlement,
					Key:            "group_membership:" + userExternalID + ":" + strings.TrimSpace(group.ID),
					ProviderID:     userExternalID + ":" + strings.TrimSpace(group.ID),
					ObservedAt:     time.Now().UTC(),
					DedupeKeyValue: "okta:group_membership:" + userExternalID + ":" + strings.TrimSpace(group.ID),
					Payload: records.EntitlementPayload{
						ExternalID: "group_membership:" + userExternalID + ":" + strings.TrimSpace(group.ID),
						Kind:       records.EntitlementKindOktaGroupMembership,
						Subject:    records.ResourceRef{Resource: records.ResourceIdentity, ExternalID: userExternalID},
						Target:     records.ResourceRef{Resource: records.ResourceGroup, ExternalID: strings.TrimSpace(group.ID), DisplayName: strings.TrimSpace(group.Name)},
						Permission: "member",
						Raw:        rawMap(group.RawJSON),
					},
				}); err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("emit okta group membership for group %s: %w", group.ID, err)
						cancel()
					})
					return
				}
			}
			n := atomic.AddInt64(&done, 1)
			report(registry.Event{
				Source:  "okta",
				Stage:   "sync-groups",
				Current: n,
				Total:   int64(len(groups)),
				Message: fmt.Sprintf("groups %d/%d", n, len(groups)),
			})
		}
	}

	for j := 0; j < workers; j++ {
		wg.Add(1)
		go worker()
	}
	for _, group := range groups {
		if strings.TrimSpace(group.ID) != "" {
			jobs <- group
		}
	}
	close(jobs)
	wg.Wait()
	return firstErr
}

func (i *OktaIntegration) syncOktaAppAssignmentsRecords(ctx context.Context, emitter capabilities.RecordEmitter, report func(registry.Event), runID int64) ([]string, error) {
	apps, err := i.client.ListApps(ctx)
	if err != nil {
		return nil, fmt.Errorf("okta list apps: %w", err)
	}
	validApps := make([]App, 0, len(apps))
	for _, app := range apps {
		if strings.TrimSpace(app.ID) != "" {
			validApps = append(validApps, app)
		}
	}
	report(registry.Event{Source: "okta", Stage: "sync-app-assignments", Current: 0, Total: int64(len(validApps)), Message: fmt.Sprintf("syncing %d apps", len(validApps))})

	appExternalIDs := make([]string, 0, len(validApps))
	for _, app := range validApps {
		appExternalIDs = append(appExternalIDs, strings.TrimSpace(app.ID))
		if err := emitter.UpsertState(ctx, appStateRecord(i.sourceName, app)); err != nil {
			return nil, fmt.Errorf("emit okta app %s state: %w", app.ID, err)
		}
	}
	if len(validApps) == 0 {
		return appExternalIDs, nil
	}

	workers := min(len(validApps), i.workers)
	if workers < 1 {
		workers = 1
	}

	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup
	jobs := make(chan App, len(validApps))
	var done int64

	worker := func() {
		defer wg.Done()
		for app := range jobs {
			if jobCtx.Err() != nil {
				return
			}
			assignments, err := i.client.ListApplicationAccounts(jobCtx, app.ID)
			if err != nil {
				errOnce.Do(func() {
					firstErr = fmt.Errorf("okta app %s accounts: %w", app.ID, err)
					cancel()
				})
				return
			}
			for _, assignment := range assignments {
				accountID := strings.TrimSpace(assignment.AccountID)
				if accountID == "" {
					continue
				}
				if err := emitter.UpsertState(jobCtx, appUserAssignmentRecord(i.sourceName, app, assignment)); err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("emit okta app assignment for app %s: %w", app.ID, err)
						cancel()
					})
					return
				}
			}
			n := atomic.AddInt64(&done, 1)
			report(registry.Event{
				Source:  "okta",
				Stage:   "sync-app-assignments",
				Current: n,
				Total:   int64(len(validApps)),
				Message: fmt.Sprintf("apps %d/%d", n, len(validApps)),
			})
		}
	}

	for j := 0; j < workers; j++ {
		wg.Add(1)
		go worker()
	}
	for _, app := range validApps {
		jobs <- app
	}
	close(jobs)
	wg.Wait()
	return appExternalIDs, firstErr
}

func (i *OktaIntegration) syncOktaAppGroupAssignmentsRecords(ctx context.Context, emitter capabilities.RecordEmitter, report func(registry.Event), runID int64, appExternalIDs []string) error {
	if len(appExternalIDs) == 0 {
		report(registry.Event{Source: "okta", Stage: "sync-app-group-assignments", Current: 0, Total: 0, Message: "no apps to sync"})
		return nil
	}
	report(registry.Event{Source: "okta", Stage: "sync-app-group-assignments", Current: 0, Total: int64(len(appExternalIDs)), Message: fmt.Sprintf("syncing %d apps", len(appExternalIDs))})

	workers := min(len(appExternalIDs), i.workers)
	if workers < 1 {
		workers = 1
	}

	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup
	jobs := make(chan string, len(appExternalIDs))
	var done int64

	worker := func() {
		defer wg.Done()
		for appExternalID := range jobs {
			if jobCtx.Err() != nil {
				return
			}
			assignments, err := i.client.ListApplicationGroupAssignments(jobCtx, appExternalID)
			if err != nil {
				errOnce.Do(func() {
					firstErr = fmt.Errorf("okta app %s group assignments: %w", appExternalID, err)
					cancel()
				})
				return
			}
			for _, assignment := range assignments {
				if strings.TrimSpace(assignment.Group.ID) == "" {
					continue
				}
				if err := emitter.UpsertState(jobCtx, groupStateRecord(i.sourceName, assignment.Group)); err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("emit okta app group %s state: %w", assignment.Group.ID, err)
						cancel()
					})
					return
				}
				if err := emitter.UpsertState(jobCtx, appGroupAssignmentRecord(i.sourceName, appExternalID, assignment)); err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("emit okta app group assignment for app %s: %w", appExternalID, err)
						cancel()
					})
					return
				}
			}
			n := atomic.AddInt64(&done, 1)
			report(registry.Event{
				Source:  "okta",
				Stage:   "sync-app-group-assignments",
				Current: n,
				Total:   int64(len(appExternalIDs)),
				Message: fmt.Sprintf("apps %d/%d", n, len(appExternalIDs)),
			})
		}
	}

	for j := 0; j < workers; j++ {
		wg.Add(1)
		go worker()
	}
	for _, externalID := range appExternalIDs {
		if strings.TrimSpace(externalID) != "" {
			jobs <- strings.TrimSpace(externalID)
		}
	}
	close(jobs)
	wg.Wait()
	return firstErr
}

func (i *OktaIntegration) syncDiscoveryRecords(ctx context.Context, q *gen.Queries, emitter capabilities.RecordEmitter, report func(registry.Event), runID int64) error {
	report(registry.Event{Source: "okta", Stage: "list-discovery-events", Current: 0, Total: 1, Message: "listing discovery events"})

	now := time.Now().UTC()
	since := now.Add(-7 * 24 * time.Hour)
	latestObservedAt, err := q.GetLatestSaaSDiscoveryObservedAtBySource(ctx, gen.GetLatestSaaSDiscoveryObservedAtBySourceParams{
		SourceKind: "okta",
		SourceName: i.sourceName,
	})
	if err != nil {
		metrics.DiscoveryIngestFailuresTotal.WithLabelValues("okta", "idp_sso", "watermark_query_error").Inc()
		return fmt.Errorf("query latest discovery watermark: %w", err)
	}
	if latestObservedAt.Valid {
		candidate := latestObservedAt.Time.UTC().Add(-15 * time.Minute)
		if candidate.After(since) {
			since = candidate
		}
	}

	events, err := i.client.ListSystemLogEventsSince(ctx, since)
	if err != nil {
		metrics.DiscoveryIngestFailuresTotal.WithLabelValues("okta", "idp_sso", "api_error").Inc()
		return fmt.Errorf("okta list system log events: %w", err)
	}
	report(registry.Event{
		Source:  "okta",
		Stage:   "list-discovery-events",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("found %d events since %s", len(events), since.Format(time.RFC3339)),
	})

	report(registry.Event{Source: "okta", Stage: "normalize-discovery", Current: 0, Total: 1, Message: "normalizing discovery events"})
	sources, normalizedEvents := NormalizeDiscoveryEvents(events, i.sourceName, now)
	report(registry.Event{
		Source:  "okta",
		Stage:   "normalize-discovery",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("normalized %d source rows and %d events", len(sources), len(normalizedEvents)),
	})

	total := len(sources) + len(normalizedEvents)
	report(registry.Event{Source: "okta", Stage: "write-discovery", Current: 0, Total: int64(total), Message: fmt.Sprintf("writing %d discovery records", total)})

	written := 0
	for _, record := range DiscoveryEvidenceRecordsFromRows(i.sourceName, sources, normalizedEvents) {
		if err := emitter.UpsertState(ctx, record); err != nil {
			metrics.DiscoveryIngestFailuresTotal.WithLabelValues("okta", "idp_sso", "db_error").Inc()
			return fmt.Errorf("emit okta discovery evidence %s: %w", record.Key, err)
		}
		written++
	}
	if total == 0 {
		report(registry.Event{Source: "okta", Stage: "write-discovery", Current: 0, Total: 0, Message: "no discovery records to write"})
		return nil
	}
	report(registry.Event{Source: "okta", Stage: "write-discovery", Current: int64(written), Total: int64(total), Message: fmt.Sprintf("discovery records %d/%d", written, total)})
	return nil
}

func DiscoveryEvidenceRecords(sourceName string, events []SystemLogEvent, now time.Time) []records.StateUpsert {
	sources, normalizedEvents := NormalizeDiscoveryEvents(events, sourceName, now)
	return DiscoveryEvidenceRecordsFromRows(sourceName, sources, normalizedEvents)
}

func DiscoveryEvidenceRecordsFromRows(sourceName string, sources []discovery.SourceRow, events []discovery.EventRow) []records.StateUpsert {
	out := make([]records.StateUpsert, 0, len(sources)+len(events))
	for _, source := range sources {
		out = append(out, discoverySourceRecord(sourceName, source))
	}
	for _, event := range events {
		out = append(out, discoveryEventRecord(sourceName, event))
	}
	return out
}

func groupStateRecord(sourceName string, group Group) records.StateUpsert {
	id := strings.TrimSpace(group.ID)
	return records.StateUpsert{
		Source:         records.SourceRef{Kind: "okta", Name: sourceName},
		Resource:       records.ResourceGroup,
		Key:            id,
		ProviderID:     id,
		ObservedAt:     time.Now().UTC(),
		DedupeKeyValue: "okta:group:" + id,
		Payload: records.GroupPayload{
			ExternalID:  id,
			DisplayName: group.Name,
			Type:        group.Type,
			Raw:         rawMap(group.RawJSON),
		},
	}
}

func appStateRecord(sourceName string, app App) records.StateUpsert {
	id := strings.TrimSpace(app.ID)
	return records.StateUpsert{
		Source:         records.SourceRef{Kind: "okta", Name: sourceName},
		Resource:       records.ResourceApplication,
		Key:            id,
		ProviderID:     id,
		ObservedAt:     time.Now().UTC(),
		DedupeKeyValue: "okta:application:" + id,
		Payload: records.ApplicationPayload{
			ExternalID:  id,
			DisplayName: app.Label,
			Name:        app.Name,
			Status:      app.Status,
			SignOnMode:  app.SignOnMode,
			Raw:         rawMap(app.RawJSON),
		},
	}
}

func appUserAssignmentRecord(sourceName string, app App, assignment AppAccountAssignment) records.StateUpsert {
	accountID := strings.TrimSpace(assignment.AccountID)
	appID := strings.TrimSpace(app.ID)
	key := "app_user_assignment:" + accountID + ":" + appID
	return records.StateUpsert{
		Source:         records.SourceRef{Kind: "okta", Name: sourceName},
		Resource:       records.ResourceEntitlement,
		Key:            key,
		ProviderID:     key,
		ObservedAt:     time.Now().UTC(),
		DedupeKeyValue: "okta:" + key,
		Payload: records.EntitlementPayload{
			ExternalID: key,
			Kind:       records.EntitlementKindOktaAppUserAssignment,
			Subject:    records.ResourceRef{Resource: records.ResourceIdentity, ExternalID: accountID},
			Target:     records.ResourceRef{Resource: records.ResourceApplication, ExternalID: appID, DisplayName: strings.TrimSpace(app.Label)},
			Scope:      assignment.Scope,
			Profile:    rawMap(assignment.ProfileJSON),
			Raw:        rawMap(assignment.RawJSON),
		},
	}
}

func appGroupAssignmentRecord(sourceName, appExternalID string, assignment AppGroupAssignment) records.StateUpsert {
	groupID := strings.TrimSpace(assignment.Group.ID)
	appID := strings.TrimSpace(appExternalID)
	key := "app_group_assignment:" + groupID + ":" + appID
	return records.StateUpsert{
		Source:         records.SourceRef{Kind: "okta", Name: sourceName},
		Resource:       records.ResourceEntitlement,
		Key:            key,
		ProviderID:     key,
		ObservedAt:     time.Now().UTC(),
		DedupeKeyValue: "okta:" + key,
		Payload: records.EntitlementPayload{
			ExternalID: key,
			Kind:       records.EntitlementKindOktaAppGroupAssignment,
			Subject:    records.ResourceRef{Resource: records.ResourceGroup, ExternalID: groupID, DisplayName: strings.TrimSpace(assignment.Group.Name)},
			Target:     records.ResourceRef{Resource: records.ResourceApplication, ExternalID: appID},
			Priority:   int(assignment.Priority),
			Profile:    rawMap(assignment.ProfileJSON),
			Raw:        rawMap(assignment.RawJSON),
		},
	}
}

func discoverySourceRecord(sourceName string, row discovery.SourceRow) records.StateUpsert {
	key := "source:" + strings.TrimSpace(row.SourceAppID)
	return records.StateUpsert{
		Source:         records.SourceRef{Kind: "okta", Name: sourceName},
		Resource:       records.ResourceDiscoveryEvidence,
		Key:            key,
		ProviderID:     strings.TrimSpace(row.SourceAppID),
		ObservedAt:     row.SeenAt.UTC(),
		DedupeKeyValue: "okta:discovery:" + key,
		Payload: records.DiscoveryEvidencePayload{
			ExternalID:       key,
			Kind:             records.DiscoveryEvidenceKindSource,
			CanonicalKey:     row.CanonicalKey,
			SourceAppID:      row.SourceAppID,
			SourceAppName:    row.SourceAppName,
			SourceAppDomain:  row.SourceAppDomain,
			SourceVendorName: row.SourceVendorName,
			SourceCategory:   row.SourceCategory,
			ObservedAt:       row.SeenAt,
		},
	}
}

func discoveryEventRecord(sourceName string, row discovery.EventRow) records.StateUpsert {
	key := "event:" + strings.TrimSpace(row.SignalKind) + ":" + strings.TrimSpace(row.EventExternalID)
	return records.StateUpsert{
		Source:         records.SourceRef{Kind: "okta", Name: sourceName},
		Resource:       records.ResourceDiscoveryEvidence,
		Key:            key,
		ProviderID:     strings.TrimSpace(row.EventExternalID),
		ObservedAt:     row.ObservedAt.UTC(),
		DedupeKeyValue: "okta:discovery:" + key,
		Payload: records.DiscoveryEvidencePayload{
			ExternalID:       key,
			Kind:             records.DiscoveryEvidenceKindEvent,
			CanonicalKey:     row.CanonicalKey,
			SignalKind:       row.SignalKind,
			EventExternalID:  row.EventExternalID,
			SourceAppID:      row.SourceAppID,
			SourceAppName:    row.SourceAppName,
			SourceAppDomain:  row.SourceAppDomain,
			SourceVendorName: row.SourceVendorName,
			SourceCategory:   row.SourceCategory,
			ActorExternalID:  row.ActorExternalID,
			ActorEmail:       row.ActorEmail,
			ActorDisplayName: row.ActorDisplayName,
			ObservedAt:       row.ObservedAt,
			Scopes:           row.Scopes,
			Raw:              rawMap(row.RawJSON),
		},
	}
}

func rawMap(raw []byte) map[string]any {
	out := map[string]any{}
	if len(raw) == 0 {
		return out
	}
	if err := json.Unmarshal(raw, &out); err != nil || out == nil {
		return map[string]any{}
	}
	return out
}
