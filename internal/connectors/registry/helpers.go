package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/readmodels"
)

const (
	SyncStatusError    = "error"
	SyncStatusCanceled = "canceled"

	SyncErrorKindAPI             = "api"
	SyncErrorKindDB              = "db"
	SyncErrorKindContextCanceled = "context_canceled"
	SyncErrorKindStaleReclaimed  = "stale_reclaimed"
	SyncErrorKindUnknown         = "unknown"

	syncRunReclaimedMessage = "reclaimed stale running sync run before starting a new sync"
)

func ConnectorLockKey(kind, name string) int64 {
	kind = strings.ToLower(strings.TrimSpace(kind))
	name = strings.ToLower(strings.TrimSpace(name))

	h := fnv.New64a()
	_, _ = h.Write([]byte(kind))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(name))
	return int64(h.Sum64())
}

func StartSyncRun(ctx context.Context, q *gen.Queries, sourceKind, sourceName string) (int64, error) {
	if q == nil {
		return 0, errors.New("sync run start could not be persisted: queries is nil")
	}

	sourceKind = strings.TrimSpace(sourceKind)
	sourceName = strings.TrimSpace(sourceName)
	if sourceKind == "" || sourceName == "" {
		return 0, fmt.Errorf("sync run start requires source kind and source name, got %q/%q", sourceKind, sourceName)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	// Connector locks and sync jobs serialize runs per scope, so any existing
	// running row for the same scope is orphaned and can be reclaimed.
	if _, err := q.ReclaimRunningSyncRunsBySource(ctx, gen.ReclaimRunningSyncRunsBySourceParams{
		SourceKinds: SyncRunScopeKinds(sourceKind),
		SourceName:  sourceName,
		Message:     syncRunReclaimedMessage,
		ErrorKind:   SyncErrorKindStaleReclaimed,
	}); err != nil {
		return 0, fmt.Errorf("reclaim stale sync runs for %s/%s: %w", sourceKind, sourceName, err)
	}

	runID, err := q.CreateSyncRun(ctx, gen.CreateSyncRunParams{
		SourceKind: sourceKind,
		SourceName: sourceName,
	})
	if err != nil {
		return 0, fmt.Errorf("create sync run for %s/%s: %w", sourceKind, sourceName, err)
	}
	return runID, nil
}

func FailSyncRun(ctx context.Context, q *gen.Queries, runID int64, err error, errorKind string) error {
	if err == nil {
		return nil
	}
	if q == nil {
		return errors.Join(err, errors.New("sync run failure could not be persisted: queries is nil"))
	}
	if runID == 0 {
		return errors.Join(err, errors.New("sync run failure could not be persisted: run id is zero"))
	}

	status := SyncStatusError
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		status = SyncStatusCanceled
		errorKind = SyncErrorKindContextCanceled
	}

	errorKind = strings.TrimSpace(errorKind)
	if errorKind == "" {
		errorKind = SyncErrorKindUnknown
	}

	msg := strings.TrimSpace(err.Error())
	if msg == "" {
		msg = "sync failed"
	}

	finishCtx := ctx
	if finishCtx == nil || finishCtx.Err() != nil {
		var cancel context.CancelFunc
		finishCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
	}

	if persistErr := q.FailSyncRun(finishCtx, gen.FailSyncRunParams{
		ID:        runID,
		Status:    status,
		Message:   msg,
		ErrorKind: errorKind,
	}); persistErr != nil {
		wrapped := fmt.Errorf("mark sync run %d failed: %w", runID, persistErr)
		slog.Error("failed to persist sync run failure", "run_id", runID, "err", wrapped)
		return errors.Join(err, wrapped)
	}
	return err
}

func ReportAndFailSyncRun(ctx context.Context, q *gen.Queries, runID int64, report func(Event), event Event, err error, errorKind string) error {
	if err == nil {
		return nil
	}
	if event.Message == "" {
		event.Message = err.Error()
	}
	if event.Err == nil {
		event.Err = err
	}
	if report != nil {
		report(event)
	}
	return FailSyncRun(ctx, q, runID, err, errorKind)
}

func FinalizeOktaRun(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, runID int64, sourceName string, duration time.Duration, finalizeDiscovery bool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	qtx := q.WithTx(tx)

	counts := map[string]int64{}

	observed, err := qtx.PromoteOktaAccountsSeenInRun(ctx, PgInt8(runID))
	if err != nil {
		return err
	}
	counts["okta_accounts_observed"] = observed

	expired, err := qtx.ExpireOktaAccountsNotSeenInRun(ctx, PgInt8(runID))
	if err != nil {
		return err
	}
	counts["okta_accounts_expired"] = expired

	observed, err = qtx.PromoteOktaGroupsSeenInRun(ctx, PgInt8(runID))
	if err != nil {
		return err
	}
	counts["okta_groups_observed"] = observed

	expired, err = qtx.ExpireOktaGroupsNotSeenInRun(ctx, PgInt8(runID))
	if err != nil {
		return err
	}
	counts["okta_groups_expired"] = expired

	observed, err = qtx.PromoteOktaGroupMembershipsSeenInRun(ctx, PgInt8(runID))
	if err != nil {
		return err
	}
	counts["okta_group_memberships_observed"] = observed

	expired, err = qtx.ExpireOktaGroupMembershipsNotSeenInRun(ctx, PgInt8(runID))
	if err != nil {
		return err
	}
	counts["okta_group_memberships_expired"] = expired

	observed, err = qtx.PromoteOktaAppsSeenInRun(ctx, PgInt8(runID))
	if err != nil {
		return err
	}
	counts["okta_apps_observed"] = observed

	expired, err = qtx.ExpireOktaAppsNotSeenInRun(ctx, PgInt8(runID))
	if err != nil {
		return err
	}
	counts["okta_apps_expired"] = expired

	observed, err = qtx.PromoteOktaAppAssignmentsSeenInRun(ctx, PgInt8(runID))
	if err != nil {
		return err
	}
	counts["okta_app_assignments_observed"] = observed

	expired, err = qtx.ExpireOktaAppAssignmentsNotSeenInRun(ctx, PgInt8(runID))
	if err != nil {
		return err
	}
	counts["okta_app_assignments_expired"] = expired

	observed, err = qtx.PromoteOktaAppGroupAssignmentsSeenInRun(ctx, PgInt8(runID))
	if err != nil {
		return err
	}
	counts["okta_app_group_assignments_observed"] = observed

	expired, err = qtx.ExpireOktaAppGroupAssignmentsNotSeenInRun(ctx, PgInt8(runID))
	if err != nil {
		return err
	}
	counts["okta_app_group_assignments_expired"] = expired

	if finalizeDiscovery {
		observed, err = qtx.PromoteSaaSAppSourcesSeenInRunBySource(ctx, gen.PromoteSaaSAppSourcesSeenInRunBySourceParams{
			LastObservedRunID: runID,
			SourceKind:        "okta",
			SourceName:        sourceName,
		})
		if err != nil {
			return err
		}
		counts["saas_app_sources_observed"] = observed

		expired, err = qtx.ExpireSaaSAppSourcesNotSeenInRunBySource(ctx, gen.ExpireSaaSAppSourcesNotSeenInRunBySourceParams{
			ExpiredRunID: runID,
			SourceKind:   "okta",
			SourceName:   sourceName,
		})
		if err != nil {
			return err
		}
		counts["saas_app_sources_expired"] = expired

		observed, err = qtx.PromoteSaaSAppEventsSeenInRunBySource(ctx, gen.PromoteSaaSAppEventsSeenInRunBySourceParams{
			LastObservedRunID: runID,
			SourceKind:        "okta",
			SourceName:        sourceName,
		})
		if err != nil {
			return err
		}
		counts["saas_app_events_observed"] = observed

		expired, err = qtx.ExpireSaaSAppEventsNotSeenInRunBySource(ctx, gen.ExpireSaaSAppEventsNotSeenInRunBySourceParams{
			ExpiredRunID: runID,
			SourceKind:   "okta",
			SourceName:   sourceName,
		})
		if err != nil {
			return err
		}
		counts["saas_app_events_expired"] = expired
	}

	if err := finalizeRunCountsInTx(ctx, qtx, runID, counts, duration, "okta", sourceName); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

func FinalizeAppRun(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, runID int64, sourceKind, sourceName string, duration time.Duration, finalizeDiscovery bool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	qtx := q.WithTx(tx)

	counts := map[string]int64{}

	runIDKey := PgInt8(runID)

	observed, err := qtx.PromoteSourceAccountsSeenInRun(ctx, gen.PromoteSourceAccountsSeenInRunParams{
		LastObservedRunID: runIDKey,
		SourceKind:        sourceKind,
		SourceName:        sourceName,
	})
	if err != nil {
		return err
	}
	counts["source_accounts_observed"] = observed

	expired, err := qtx.ExpireSourceAccountsNotSeenInRun(ctx, gen.ExpireSourceAccountsNotSeenInRunParams{
		ExpiredRunID: runIDKey,
		SourceKind:   sourceKind,
		SourceName:   sourceName,
	})
	if err != nil {
		return err
	}
	counts["source_accounts_expired"] = expired

	observed, err = qtx.PromoteEntitlementsSeenInRunBySource(ctx, gen.PromoteEntitlementsSeenInRunBySourceParams{
		LastObservedRunID: runIDKey,
		SourceKind:        sourceKind,
		SourceName:        sourceName,
	})
	if err != nil {
		return err
	}
	counts["entitlements_observed"] = observed

	expired, err = qtx.ExpireEntitlementsNotSeenInRunBySource(ctx, gen.ExpireEntitlementsNotSeenInRunBySourceParams{
		ExpiredRunID: runIDKey,
		SourceKind:   sourceKind,
		SourceName:   sourceName,
	})
	if err != nil {
		return err
	}
	counts["entitlements_expired"] = expired

	observed, err = qtx.PromoteAppAssetsSeenInRunBySource(ctx, gen.PromoteAppAssetsSeenInRunBySourceParams{
		LastObservedRunID: runID,
		SourceKind:        sourceKind,
		SourceName:        sourceName,
	})
	if err != nil {
		return err
	}
	counts["app_assets_observed"] = observed

	expired, err = qtx.ExpireAppAssetsNotSeenInRunBySource(ctx, gen.ExpireAppAssetsNotSeenInRunBySourceParams{
		ExpiredRunID: runID,
		SourceKind:   sourceKind,
		SourceName:   sourceName,
	})
	if err != nil {
		return err
	}
	counts["app_assets_expired"] = expired

	observed, err = qtx.PromoteAppAssetOwnersSeenInRunBySource(ctx, gen.PromoteAppAssetOwnersSeenInRunBySourceParams{
		LastObservedRunID: runID,
		SourceKind:        sourceKind,
		SourceName:        sourceName,
	})
	if err != nil {
		return err
	}
	counts["app_asset_owners_observed"] = observed

	expired, err = qtx.ExpireAppAssetOwnersNotSeenInRunBySource(ctx, gen.ExpireAppAssetOwnersNotSeenInRunBySourceParams{
		ExpiredRunID: runID,
		SourceKind:   sourceKind,
		SourceName:   sourceName,
	})
	if err != nil {
		return err
	}
	counts["app_asset_owners_expired"] = expired

	observed, err = qtx.PromoteCredentialArtifactsSeenInRunBySource(ctx, gen.PromoteCredentialArtifactsSeenInRunBySourceParams{
		LastObservedRunID: runID,
		SourceKind:        sourceKind,
		SourceName:        sourceName,
	})
	if err != nil {
		return err
	}
	counts["credential_artifacts_observed"] = observed

	expired, err = qtx.ExpireCredentialArtifactsNotSeenInRunBySource(ctx, gen.ExpireCredentialArtifactsNotSeenInRunBySourceParams{
		ExpiredRunID: runID,
		SourceKind:   sourceKind,
		SourceName:   sourceName,
	})
	if err != nil {
		return err
	}
	counts["credential_artifacts_expired"] = expired

	if finalizeDiscovery {
		observed, err = qtx.PromoteSaaSAppSourcesSeenInRunBySource(ctx, gen.PromoteSaaSAppSourcesSeenInRunBySourceParams{
			LastObservedRunID: runID,
			SourceKind:        sourceKind,
			SourceName:        sourceName,
		})
		if err != nil {
			return err
		}
		counts["saas_app_sources_observed"] = observed

		expired, err = qtx.ExpireSaaSAppSourcesNotSeenInRunBySource(ctx, gen.ExpireSaaSAppSourcesNotSeenInRunBySourceParams{
			ExpiredRunID: runID,
			SourceKind:   sourceKind,
			SourceName:   sourceName,
		})
		if err != nil {
			return err
		}
		counts["saas_app_sources_expired"] = expired

		observed, err = qtx.PromoteSaaSAppEventsSeenInRunBySource(ctx, gen.PromoteSaaSAppEventsSeenInRunBySourceParams{
			LastObservedRunID: runID,
			SourceKind:        sourceKind,
			SourceName:        sourceName,
		})
		if err != nil {
			return err
		}
		counts["saas_app_events_observed"] = observed

		expired, err = qtx.ExpireSaaSAppEventsNotSeenInRunBySource(ctx, gen.ExpireSaaSAppEventsNotSeenInRunBySourceParams{
			ExpiredRunID: runID,
			SourceKind:   sourceKind,
			SourceName:   sourceName,
		})
		if err != nil {
			return err
		}
		counts["saas_app_events_expired"] = expired
	}

	if err := finalizeRunCountsInTx(ctx, qtx, runID, counts, duration, sourceKind, sourceName); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

type AppAssetDeltaDelete struct {
	AssetKind   string
	ExternalIDs []string
}

type ConnectorDeltaCursorUpdate struct {
	Resource  string
	DeltaLink string
}

type AppDeltaFinalizeOptions struct {
	ResetDeltaCursors             bool
	ExpireAbsent                  bool
	DeletedAccountExternalIDs     []string
	DeletedAppAssets              []AppAssetDeltaDelete
	CredentialAssetRefExternalIDs []string
	DeltaCursors                  []ConnectorDeltaCursorUpdate
}

func FinalizeAppDeltaRun(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, runID int64, sourceKind, sourceName string, duration time.Duration, opts AppDeltaFinalizeOptions) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	qtx := q.WithTx(tx)

	counts := map[string]int64{}
	runIDKey := PgInt8(runID)

	observed, err := qtx.PromoteSourceAccountsSeenInRun(ctx, gen.PromoteSourceAccountsSeenInRunParams{
		LastObservedRunID: runIDKey,
		SourceKind:        sourceKind,
		SourceName:        sourceName,
	})
	if err != nil {
		return err
	}
	counts["source_accounts_observed"] = observed

	if opts.ExpireAbsent {
		expired, err := qtx.ExpireSourceAccountsNotSeenInRun(ctx, gen.ExpireSourceAccountsNotSeenInRunParams{
			ExpiredRunID: runIDKey,
			SourceKind:   sourceKind,
			SourceName:   sourceName,
		})
		if err != nil {
			return err
		}
		counts["source_accounts_expired"] = expired
	} else if len(opts.DeletedAccountExternalIDs) > 0 {
		expired, err := qtx.ExpireSourceAccountsByExternalIDs(ctx, gen.ExpireSourceAccountsByExternalIDsParams{
			ExpiredRunID: runID,
			SourceKind:   sourceKind,
			SourceName:   sourceName,
			ExternalIds:  distinctNonEmptyStrings(opts.DeletedAccountExternalIDs),
		})
		if err != nil {
			return err
		}
		counts["source_accounts_expired"] = expired
	} else {
		counts["source_accounts_expired"] = 0
	}

	// Entra delta still reconciles relationship tables from the current active
	// app inventory each run, so owners and entitlements keep snapshot expiry.
	observed, err = qtx.PromoteEntitlementsSeenInRunBySource(ctx, gen.PromoteEntitlementsSeenInRunBySourceParams{
		LastObservedRunID: runIDKey,
		SourceKind:        sourceKind,
		SourceName:        sourceName,
	})
	if err != nil {
		return err
	}
	counts["entitlements_observed"] = observed

	expired, err := qtx.ExpireEntitlementsNotSeenInRunBySource(ctx, gen.ExpireEntitlementsNotSeenInRunBySourceParams{
		ExpiredRunID: runIDKey,
		SourceKind:   sourceKind,
		SourceName:   sourceName,
	})
	if err != nil {
		return err
	}
	counts["entitlements_expired"] = expired

	observed, err = qtx.PromoteAppAssetsSeenInRunBySource(ctx, gen.PromoteAppAssetsSeenInRunBySourceParams{
		LastObservedRunID: runID,
		SourceKind:        sourceKind,
		SourceName:        sourceName,
	})
	if err != nil {
		return err
	}
	counts["app_assets_observed"] = observed

	var appAssetsExpired int64
	if opts.ExpireAbsent {
		appAssetsExpired, err = qtx.ExpireAppAssetsNotSeenInRunBySource(ctx, gen.ExpireAppAssetsNotSeenInRunBySourceParams{
			ExpiredRunID: runID,
			SourceKind:   sourceKind,
			SourceName:   sourceName,
		})
		if err != nil {
			return err
		}
	} else {
		for _, deletion := range opts.DeletedAppAssets {
			externalIDs := distinctNonEmptyStrings(deletion.ExternalIDs)
			if strings.TrimSpace(deletion.AssetKind) == "" || len(externalIDs) == 0 {
				continue
			}
			n, err := qtx.ExpireAppAssetsBySourceKindAndExternalIDs(ctx, gen.ExpireAppAssetsBySourceKindAndExternalIDsParams{
				ExpiredRunID: runID,
				SourceKind:   sourceKind,
				SourceName:   sourceName,
				AssetKind:    deletion.AssetKind,
				ExternalIds:  externalIDs,
			})
			if err != nil {
				return err
			}
			appAssetsExpired += n
		}
	}
	counts["app_assets_expired"] = appAssetsExpired

	observed, err = qtx.PromoteAppAssetOwnersSeenInRunBySource(ctx, gen.PromoteAppAssetOwnersSeenInRunBySourceParams{
		LastObservedRunID: runID,
		SourceKind:        sourceKind,
		SourceName:        sourceName,
	})
	if err != nil {
		return err
	}
	counts["app_asset_owners_observed"] = observed

	expired, err = qtx.ExpireAppAssetOwnersNotSeenInRunBySource(ctx, gen.ExpireAppAssetOwnersNotSeenInRunBySourceParams{
		ExpiredRunID: runID,
		SourceKind:   sourceKind,
		SourceName:   sourceName,
	})
	if err != nil {
		return err
	}
	counts["app_asset_owners_expired"] = expired

	observed, err = qtx.PromoteCredentialArtifactsSeenInRunBySource(ctx, gen.PromoteCredentialArtifactsSeenInRunBySourceParams{
		LastObservedRunID: runID,
		SourceKind:        sourceKind,
		SourceName:        sourceName,
	})
	if err != nil {
		return err
	}
	counts["credential_artifacts_observed"] = observed

	if opts.ExpireAbsent {
		expired, err = qtx.ExpireCredentialArtifactsNotSeenInRunBySource(ctx, gen.ExpireCredentialArtifactsNotSeenInRunBySourceParams{
			ExpiredRunID: runID,
			SourceKind:   sourceKind,
			SourceName:   sourceName,
		})
		if err != nil {
			return err
		}
		counts["credential_artifacts_expired"] = expired
	} else if len(opts.CredentialAssetRefExternalIDs) > 0 {
		expired, err = qtx.ExpireCredentialArtifactsForAssetRefsNotSeenInRunBySource(ctx, gen.ExpireCredentialArtifactsForAssetRefsNotSeenInRunBySourceParams{
			ExpiredRunID:        runID,
			SourceKind:          sourceKind,
			SourceName:          sourceName,
			AssetRefExternalIds: distinctNonEmptyStrings(opts.CredentialAssetRefExternalIDs),
		})
		if err != nil {
			return err
		}
		counts["credential_artifacts_expired"] = expired
	} else {
		counts["credential_artifacts_expired"] = 0
	}

	refreshed, err := qtx.RefreshCredentialArtifactLifecycleStatusesBySource(ctx, gen.RefreshCredentialArtifactLifecycleStatusesBySourceParams{
		SourceKind: sourceKind,
		SourceName: sourceName,
	})
	if err != nil {
		return err
	}
	counts["credential_artifact_statuses_refreshed"] = refreshed

	if opts.ResetDeltaCursors {
		if err := qtx.DeleteConnectorDeltaStatesBySource(ctx, gen.DeleteConnectorDeltaStatesBySourceParams{
			SourceKind: sourceKind,
			SourceName: sourceName,
		}); err != nil {
			return err
		}
	}

	for _, cursor := range opts.DeltaCursors {
		resource := strings.TrimSpace(cursor.Resource)
		deltaLink := strings.TrimSpace(cursor.DeltaLink)
		if resource == "" || deltaLink == "" {
			continue
		}
		if err := qtx.UpsertConnectorDeltaState(ctx, gen.UpsertConnectorDeltaStateParams{
			SourceKind:       sourceKind,
			SourceName:       sourceName,
			Resource:         resource,
			DeltaLink:        deltaLink,
			LastSuccessRunID: PgInt8(runID),
		}); err != nil {
			return err
		}
	}

	if err := finalizeRunCountsInTx(ctx, qtx, runID, counts, duration, sourceKind, sourceName); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

func FinalizeDiscoveryRun(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, runID int64, sourceKind, sourceName string, duration time.Duration) error {
	return FinalizeDiscoveryRunWithCounts(ctx, q, pool, runID, sourceKind, sourceName, duration, nil)
}

func FinalizeDiscoveryRunWithCounts(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, runID int64, sourceKind, sourceName string, duration time.Duration, extraCounts map[string]int64) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	qtx := q.WithTx(tx)

	counts := map[string]int64{}
	observed, err := qtx.PromoteSaaSAppSourcesSeenInRunBySource(ctx, gen.PromoteSaaSAppSourcesSeenInRunBySourceParams{
		LastObservedRunID: runID,
		SourceKind:        sourceKind,
		SourceName:        sourceName,
	})
	if err != nil {
		return err
	}
	counts["saas_app_sources_observed"] = observed

	expired, err := qtx.ExpireSaaSAppSourcesNotSeenInRunBySource(ctx, gen.ExpireSaaSAppSourcesNotSeenInRunBySourceParams{
		ExpiredRunID: runID,
		SourceKind:   sourceKind,
		SourceName:   sourceName,
	})
	if err != nil {
		return err
	}
	counts["saas_app_sources_expired"] = expired

	observed, err = qtx.PromoteSaaSAppEventsSeenInRunBySource(ctx, gen.PromoteSaaSAppEventsSeenInRunBySourceParams{
		LastObservedRunID: runID,
		SourceKind:        sourceKind,
		SourceName:        sourceName,
	})
	if err != nil {
		return err
	}
	counts["saas_app_events_observed"] = observed

	expired, err = qtx.ExpireSaaSAppEventsNotSeenInRunBySource(ctx, gen.ExpireSaaSAppEventsNotSeenInRunBySourceParams{
		ExpiredRunID: runID,
		SourceKind:   sourceKind,
		SourceName:   sourceName,
	})
	if err != nil {
		return err
	}
	counts["saas_app_events_expired"] = expired

	for key, count := range extraCounts {
		counts[key] += count
	}

	if err := finalizeRunCountsInTx(ctx, qtx, runID, counts, duration, sourceKind, sourceName); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

func PgTimestamptzPtr(t *time.Time) pgtype.Timestamptz {
	if t == nil || t.IsZero() {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: *t, Valid: true}
}

func PgInt8(v int64) pgtype.Int8 {
	return pgtype.Int8{Int64: v, Valid: true}
}

func MarshalJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Errorf("registry: marshal json: %w", err))
	}
	return b
}

func finalizeRunCountsInTx(ctx context.Context, q *gen.Queries, runID int64, counts map[string]int64, duration time.Duration, sourceKind, sourceName string) error {
	stats := MarshalJSON(map[string]any{
		"counts":      counts,
		"duration_ms": duration.Milliseconds(),
	})
	return finalizeSuccessfulRunInTx(ctx, q, runID, stats, sourceKind, sourceName)
}

func finalizeSuccessfulRunInTx(ctx context.Context, q *gen.Queries, runID int64, stats []byte, sourceKind, sourceName string) error {
	if err := refreshPreSuccessSourceReadModelsInTx(ctx, q, sourceKind, sourceName); err != nil {
		return err
	}
	if err := q.MarkSyncRunSuccess(ctx, gen.MarkSyncRunSuccessParams{ID: runID, Stats: stats}); err != nil {
		return err
	}
	return refreshPostSuccessReadModelsInTx(ctx, q, sourceKind, sourceName)
}

func refreshPreSuccessSourceReadModelsInTx(ctx context.Context, q *gen.Queries, sourceKind, sourceName string) error {
	projector := readmodels.ProjectorFromContext(ctx, q)
	if projector == nil {
		return nil
	}
	if err := projector.RefreshDiscoverySource(ctx, sourceKind, sourceName); err != nil {
		return err
	}
	return projector.RefreshAppAssetSource(ctx, sourceKind, sourceName)
}

func refreshPostSuccessReadModelsInTx(ctx context.Context, q *gen.Queries, sourceKind, sourceName string) error {
	if err := refreshConnectorSourceStateInTx(ctx, q); err != nil {
		return err
	}
	if err := refreshSaaSAppRiskReadModelsInTx(ctx, q, sourceKind, sourceName); err != nil {
		return err
	}
	if err := refreshCredentialArtifactRiskReadModelsInTx(ctx, q, sourceKind, sourceName); err != nil {
		return err
	}
	return refreshNonHumanPrincipalReadModelsInTx(ctx, q, sourceKind, sourceName)
}

func refreshConnectorSourceStateInTx(ctx context.Context, q *gen.Queries) error {
	projector := readmodels.ProjectorFromContext(ctx, q)
	if projector == nil {
		return nil
	}
	return projector.RefreshConnectorSourceState(ctx)
}

func refreshSaaSAppRiskReadModelsInTx(ctx context.Context, q *gen.Queries, sourceKind, sourceName string) error {
	projector := readmodels.ProjectorFromContext(ctx, q)
	if projector == nil {
		return nil
	}
	return projector.RefreshSaaSAppRiskReadModelsBySource(ctx, sourceKind, sourceName)
}

func refreshCredentialArtifactRiskReadModelsInTx(ctx context.Context, q *gen.Queries, sourceKind, sourceName string) error {
	projector := readmodels.ProjectorFromContext(ctx, q)
	if projector == nil {
		return nil
	}
	return projector.RefreshCredentialArtifactRiskReadModelsBySource(ctx, sourceKind, sourceName)
}

func refreshNonHumanPrincipalReadModelsInTx(ctx context.Context, q *gen.Queries, sourceKind, sourceName string) error {
	projector := readmodels.ProjectorFromContext(ctx, q)
	if projector == nil {
		return nil
	}
	return projector.RefreshNonHumanPrincipalSourceReadModels(ctx, sourceKind, sourceName)
}

func NormalizeJSON(b []byte) []byte {
	if len(b) == 0 {
		return []byte("{}")
	}
	return b
}

func distinctNonEmptyStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
