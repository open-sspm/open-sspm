package registry

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type SourceAccountRow struct {
	ExternalID      string
	Email           string
	DisplayName     string
	AccountKind     string
	EntityCategory  string
	RawJSON         []byte
	LastLoginAt     pgtype.Timestamptz
	LastLoginIP     string
	LastLoginRegion string
}

type WriteSourceAccountRowsParams struct {
	SourceKind string
	SourceName string
	RunID      int64
	BatchSize  int
	Rows       []SourceAccountRow
}

func WriteSourceAccountRows(ctx context.Context, q *gen.Queries, params WriteSourceAccountRowsParams) (int, error) {
	if len(params.Rows) == 0 {
		return 0, nil
	}
	if q == nil {
		return 0, fmt.Errorf("write source accounts for %s/%s: queries is nil", params.SourceKind, params.SourceName)
	}

	batchSize := params.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	written := 0
	for start := 0; start < len(params.Rows); start += batchSize {
		end := min(start+batchSize, len(params.Rows))
		batch := params.Rows[start:end]

		externalIDs := make([]string, 0, len(batch))
		emails := make([]string, 0, len(batch))
		displayNames := make([]string, 0, len(batch))
		accountKinds := make([]string, 0, len(batch))
		entityCategories := make([]string, 0, len(batch))
		rawJSONs := make([][]byte, 0, len(batch))
		lastLoginAts := make([]pgtype.Timestamptz, 0, len(batch))
		lastLoginIPs := make([]string, 0, len(batch))
		lastLoginRegions := make([]string, 0, len(batch))

		for _, row := range batch {
			externalID := strings.TrimSpace(row.ExternalID)
			if externalID == "" {
				continue
			}
			externalIDs = append(externalIDs, externalID)
			emails = append(emails, row.Email)
			displayNames = append(displayNames, row.DisplayName)
			accountKinds = append(accountKinds, row.AccountKind)
			entityCategories = append(entityCategories, row.EntityCategory)
			rawJSONs = append(rawJSONs, NormalizeJSON(row.RawJSON))
			lastLoginAts = append(lastLoginAts, row.LastLoginAt)
			lastLoginIPs = append(lastLoginIPs, row.LastLoginIP)
			lastLoginRegions = append(lastLoginRegions, row.LastLoginRegion)
		}
		if len(externalIDs) == 0 {
			continue
		}

		affected, err := q.UpsertSourceAccountsBulkBySource(ctx, gen.UpsertSourceAccountsBulkBySourceParams{
			SourceKind:       params.SourceKind,
			SourceName:       params.SourceName,
			SeenInRunID:      params.RunID,
			ExternalIds:      externalIDs,
			Emails:           emails,
			DisplayNames:     displayNames,
			AccountKinds:     accountKinds,
			EntityCategories: entityCategories,
			RawJsons:         rawJSONs,
			LastLoginAts:     lastLoginAts,
			LastLoginIps:     lastLoginIPs,
			LastLoginRegions: lastLoginRegions,
		})
		if err != nil {
			return written, err
		}
		written += int(affected)
	}
	return written, nil
}

type EntitlementRow struct {
	AccountExternalID string
	Kind              string
	Resource          string
	Permission        string
	RawJSON           []byte
}

type WriteEntitlementRowsParams struct {
	SourceKind string
	SourceName string
	RunID      int64
	BatchSize  int
	Rows       []EntitlementRow
}

func WriteEntitlementRows(ctx context.Context, q *gen.Queries, params WriteEntitlementRowsParams) (int, error) {
	if len(params.Rows) == 0 {
		return 0, nil
	}
	if q == nil {
		return 0, fmt.Errorf("write entitlements for %s/%s: queries is nil", params.SourceKind, params.SourceName)
	}

	batchSize := params.BatchSize
	if batchSize <= 0 {
		batchSize = 5000
	}

	written := 0
	for start := 0; start < len(params.Rows); start += batchSize {
		end := min(start+batchSize, len(params.Rows))
		batch := params.Rows[start:end]

		accountExternalIDs := make([]string, 0, len(batch))
		kinds := make([]string, 0, len(batch))
		resources := make([]string, 0, len(batch))
		permissions := make([]string, 0, len(batch))
		rawJSONs := make([][]byte, 0, len(batch))

		for _, row := range batch {
			accountExternalID := strings.TrimSpace(row.AccountExternalID)
			if accountExternalID == "" {
				continue
			}
			accountExternalIDs = append(accountExternalIDs, accountExternalID)
			kinds = append(kinds, row.Kind)
			resources = append(resources, row.Resource)
			permissions = append(permissions, row.Permission)
			rawJSONs = append(rawJSONs, NormalizeJSON(row.RawJSON))
		}
		if len(accountExternalIDs) == 0 {
			continue
		}

		affected, err := q.UpsertEntitlementsBulkBySource(ctx, gen.UpsertEntitlementsBulkBySourceParams{
			SeenInRunID:        params.RunID,
			SourceKind:         params.SourceKind,
			SourceName:         params.SourceName,
			AccountExternalIds: accountExternalIDs,
			Kinds:              kinds,
			Resources:          resources,
			Permissions:        permissions,
			RawJsons:           rawJSONs,
		})
		if err != nil {
			return written, err
		}
		written += int(affected)
	}
	return written, nil
}
