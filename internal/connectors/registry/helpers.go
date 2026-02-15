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
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

const (
	SyncStatusError    = "error"
	SyncStatusCanceled = "canceled"

	SyncErrorKindAPI             = "api"
	SyncErrorKindDB              = "db"
	SyncErrorKindContextCanceled = "context_canceled"
	SyncErrorKindUnknown         = "unknown"
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

func FinalizeOktaRun(ctx context.Context, deps IntegrationDeps, runID int64, sourceName string, duration time.Duration, finalizeDiscovery bool) error {
	tx, err := deps.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	qtx := deps.Q.WithTx(tx)

	counts := map[string]int64{}

	observed, err := qtx.PromoteIdPUsersSeenInRun(ctx, PgInt8(runID))
	if err != nil {
		return err
	}
	counts["idp_users_observed"] = observed

	expired, err := qtx.ExpireIdPUsersNotSeenInRun(ctx, PgInt8(runID))
	if err != nil {
		return err
	}
	counts["idp_users_expired"] = expired

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

	observed, err = qtx.PromoteOktaUserGroupsSeenInRun(ctx, PgInt8(runID))
	if err != nil {
		return err
	}
	counts["okta_user_groups_observed"] = observed

	expired, err = qtx.ExpireOktaUserGroupsNotSeenInRun(ctx, PgInt8(runID))
	if err != nil {
		return err
	}
	counts["okta_user_groups_expired"] = expired

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

	observed, err = qtx.PromoteOktaUserAppAssignmentsSeenInRun(ctx, PgInt8(runID))
	if err != nil {
		return err
	}
	counts["okta_user_app_assignments_observed"] = observed

	expired, err = qtx.ExpireOktaUserAppAssignmentsNotSeenInRun(ctx, PgInt8(runID))
	if err != nil {
		return err
	}
	counts["okta_user_app_assignments_expired"] = expired

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

	stats := MarshalJSON(map[string]any{
		"counts":      counts,
		"duration_ms": duration.Milliseconds(),
	})
	if err := qtx.MarkSyncRunSuccess(ctx, gen.MarkSyncRunSuccessParams{ID: runID, Stats: stats}); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func FinalizeAppRun(ctx context.Context, deps IntegrationDeps, runID int64, sourceKind, sourceName string, duration time.Duration, finalizeDiscovery bool) error {
	tx, err := deps.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	qtx := deps.Q.WithTx(tx)

	counts := map[string]int64{}

	runIDKey := PgInt8(runID)

	observed, err := qtx.PromoteAppUsersSeenInRun(ctx, gen.PromoteAppUsersSeenInRunParams{
		LastObservedRunID: runIDKey,
		SourceKind:        sourceKind,
		SourceName:        sourceName,
	})
	if err != nil {
		return err
	}
	counts["app_users_observed"] = observed

	expired, err := qtx.ExpireAppUsersNotSeenInRun(ctx, gen.ExpireAppUsersNotSeenInRunParams{
		ExpiredRunID: runIDKey,
		SourceKind:   sourceKind,
		SourceName:   sourceName,
	})
	if err != nil {
		return err
	}
	counts["app_users_expired"] = expired

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

	stats := MarshalJSON(map[string]any{
		"counts":      counts,
		"duration_ms": duration.Milliseconds(),
	})
	if err := qtx.MarkSyncRunSuccess(ctx, gen.MarkSyncRunSuccessParams{ID: runID, Stats: stats}); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func FinalizeDiscoveryRun(ctx context.Context, deps IntegrationDeps, runID int64, sourceKind, sourceName string, duration time.Duration) error {
	tx, err := deps.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	qtx := deps.Q.WithTx(tx)

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

	stats := MarshalJSON(map[string]any{
		"counts":      counts,
		"duration_ms": duration.Milliseconds(),
	})
	if err := qtx.MarkSyncRunSuccess(ctx, gen.MarkSyncRunSuccessParams{ID: runID, Stats: stats}); err != nil {
		return err
	}

	return tx.Commit(ctx)
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

func NormalizeJSON(b []byte) []byte {
	if len(b) == 0 {
		return []byte("{}")
	}
	return b
}
