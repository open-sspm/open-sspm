package datadog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	gosync "sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/matching"
)

type DatadogIntegration struct {
	adapter datadogAdapter
	site    string
	workers int
}

func NewDatadogIntegration(adapter datadogAdapter, site string, workers int) *DatadogIntegration {
	if workers < 1 {
		workers = 3
	}
	return &DatadogIntegration{
		adapter: adapter,
		site:    strings.TrimSpace(site),
		workers: workers,
	}
}

func (i *DatadogIntegration) Kind() string { return "datadog" }
func (i *DatadogIntegration) Name() string { return i.site }
func (i *DatadogIntegration) Role() registry.IntegrationRole {
	return registry.RoleApp
}

func (i *DatadogIntegration) InitEvents() []registry.Event {
	return []registry.Event{
		{Source: "datadog", Stage: "list-accounts", Current: 0, Total: 1, Message: "listing accounts"},
		{Source: "datadog", Stage: "list-roles", Current: 0, Total: 1, Message: "listing roles"},
		{Source: "datadog", Stage: "fetch-role-members", Current: 0, Total: registry.UnknownTotal, Message: "listing role members"},
		{Source: "datadog", Stage: "write-principals", Current: 0, Total: registry.UnknownTotal, Message: "writing principals"},
	}
}

func (i *DatadogIntegration) Run(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, report func(registry.Event), mode registry.RunMode) error {
	if mode.Normalize() == registry.RunModeTail {
		return i.runAuditTail(ctx, q, pool, report)
	}

	started := time.Now()
	slog.Info("syncing Datadog")

	runID, err := registry.StartSyncRun(ctx, q, "datadog", i.site)
	if err != nil {
		return err
	}

	accounts, err := i.adapter.ListAccounts(ctx)
	if err != nil {
		return registry.ReportAndFailSyncRun(ctx, q, runID, report, registry.Event{Source: "datadog", Stage: "list-accounts"}, err, registry.SyncErrorKindAPI)
	}
	report(registry.Event{Source: "datadog", Stage: "list-accounts", Current: 1, Total: 1, Message: fmt.Sprintf("found %d accounts", len(accounts))})

	roles, err := i.adapter.ListRoles(ctx)
	if err != nil {
		return registry.ReportAndFailSyncRun(ctx, q, runID, report, registry.Event{Source: "datadog", Stage: "list-roles"}, err, registry.SyncErrorKindAPI)
	}
	report(registry.Event{Source: "datadog", Stage: "list-roles", Current: 1, Total: 1, Message: fmt.Sprintf("found %d roles", len(roles))})

	rolesByAccountExternalID := make(map[string][]Role)
	if len(roles) > 0 {
		type roleMembersResult struct {
			role               Role
			accountExternalIDs []string
			err                error
		}

		rolesCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		jobs := make(chan Role, len(roles))
		results := make(chan roleMembersResult, len(roles))
		var rolesDone int64

		workers := min(len(roles), i.workers)
		if workers < 1 {
			workers = 1
		}

		report(registry.Event{
			Source:  "datadog",
			Stage:   "fetch-role-members",
			Current: 0,
			Total:   int64(len(roles)),
			Message: fmt.Sprintf("fetching members for %d roles", len(roles)),
		})

		var wg gosync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for role := range jobs {
					if rolesCtx.Err() != nil {
						return
					}
					accountExternalIDs, err := i.adapter.ListRoleMembers(rolesCtx, role.ID)
					if err != nil {
						results <- roleMembersResult{role: role, err: fmt.Errorf("datadog role %s members: %w", strings.TrimSpace(role.ID), err)}
						cancel()
						continue
					}
					n := atomic.AddInt64(&rolesDone, 1)
					report(registry.Event{
						Source:  "datadog",
						Stage:   "fetch-role-members",
						Current: n,
						Total:   int64(len(roles)),
						Message: fmt.Sprintf("roles %d/%d", n, len(roles)),
					})
					results <- roleMembersResult{role: role, accountExternalIDs: accountExternalIDs}
				}
			}()
		}

		for _, role := range roles {
			jobs <- role
		}
		close(jobs)
		wg.Wait()
		close(results)

		var firstErr error
		var firstNonCancelErr error
		for res := range results {
			if res.err != nil {
				if firstErr == nil {
					firstErr = res.err
				}
				if firstNonCancelErr == nil && !errors.Is(res.err, context.Canceled) {
					firstNonCancelErr = res.err
				}
				continue
			}
			roleID := strings.TrimSpace(res.role.ID)
			roleName := strings.TrimSpace(res.role.Name)
			for _, accountExternalID := range res.accountExternalIDs {
				accountExternalID = strings.TrimSpace(accountExternalID)
				if accountExternalID == "" {
					continue
				}
				rolesByAccountExternalID[accountExternalID] = append(rolesByAccountExternalID[accountExternalID], Role{ID: roleID, Name: roleName})
			}
		}
		if firstNonCancelErr != nil {
			firstErr = firstNonCancelErr
		}
		if firstErr != nil {
			return registry.ReportAndFailSyncRun(ctx, q, runID, report, registry.Event{Source: "datadog", Stage: "fetch-role-members"}, firstErr, registry.SyncErrorKindAPI)
		}
	}

	report(registry.Event{
		Source:  "datadog",
		Stage:   "write-principals",
		Current: 0,
		Total:   int64(len(accounts) + len(roles)),
		Message: fmt.Sprintf("writing %d principals", len(accounts)+len(roles)),
	})

	const userBatchSize = 1000
	totalPrincipals := len(accounts) + len(roles)
	accountRows := make([]registry.SourceAccountRow, 0, totalPrincipals)

	for _, account := range accounts {
		externalID := strings.TrimSpace(account.ExternalID)
		if externalID == "" {
			continue
		}
		display := strings.TrimSpace(account.DisplayName)
		if display == "" {
			display = externalID
		}
		accountRows = append(accountRows, registry.SourceAccountRow{
			ExternalID:     externalID,
			Email:          matching.NormalizeEmail(account.Email),
			DisplayName:    display,
			AccountKind:    account.AccountKind,
			EntityCategory: account.EntityCategory,
			RawJSON:        registry.WithEntityCategory(registry.NormalizeJSON(account.RawJSON), account.EntityCategory),
			LastLoginAt:    registry.PgTimestamptzPtr(account.LastLoginAt),
		})
	}

	for _, role := range roles {
		roleExternalID := datadogRoleExternalID(role.ID)
		if roleExternalID == "" {
			continue
		}
		display := strings.TrimSpace(role.Name)
		if display == "" {
			display = roleExternalID
		}
		accountRows = append(accountRows, registry.SourceAccountRow{
			ExternalID:     roleExternalID,
			DisplayName:    display,
			AccountKind:    registry.AccountKindService,
			EntityCategory: registry.EntityCategoryRole,
			RawJSON:        registry.WithEntityCategory(registry.NormalizeJSON(role.RawJSON), registry.EntityCategoryRole),
		})
	}

	if _, err := registry.WriteSourceAccountRows(ctx, q, registry.WriteSourceAccountRowsParams{
		SourceKind: "datadog",
		SourceName: i.site,
		RunID:      runID,
		BatchSize:  userBatchSize,
		Rows:       accountRows,
	}); err != nil {
		return registry.ReportAndFailSyncRun(ctx, q, runID, report, registry.Event{Source: "datadog", Stage: "write-principals"}, err, registry.SyncErrorKindDB)
	}
	if len(accountRows) > 0 {
		report(registry.Event{
			Source:  "datadog",
			Stage:   "write-principals",
			Current: int64(len(accountRows)),
			Total:   int64(len(accountRows)),
			Message: fmt.Sprintf("principals %d/%d", len(accountRows), len(accountRows)),
		})
	}

	const entitlementBatchSize = 5000
	entitlementRows := make([]registry.EntitlementRow, 0, len(accounts))

	for _, account := range accounts {
		accountExternalID := strings.TrimSpace(account.ExternalID)
		if accountExternalID == "" {
			continue
		}
		for _, role := range dedupeDatadogRoles(rolesByAccountExternalID[accountExternalID]) {
			roleID := strings.TrimSpace(role.ID)
			roleName := strings.TrimSpace(role.Name)
			externalID := roleID
			if externalID == "" {
				externalID = roleName
			}
			if externalID == "" {
				continue
			}
			entitlementRows = append(entitlementRows, registry.EntitlementRow{
				AccountExternalID: accountExternalID,
				Kind:              "datadog_role",
				Resource:          "datadog_role:" + externalID,
				Permission:        "member",
				RawJSON: registry.MarshalJSON(map[string]string{
					"role_id":   roleID,
					"role_name": roleName,
				}),
			})
		}
	}

	if _, err := registry.WriteEntitlementRows(ctx, q, registry.WriteEntitlementRowsParams{
		SourceKind: "datadog",
		SourceName: i.site,
		RunID:      runID,
		BatchSize:  entitlementBatchSize,
		Rows:       entitlementRows,
	}); err != nil {
		return registry.ReportAndFailSyncRun(ctx, q, runID, report, registry.Event{Source: "datadog", Stage: "write-principals"}, err, registry.SyncErrorKindDB)
	}

	if err := registry.FinalizeAppRun(ctx, q, pool, runID, "datadog", i.site, time.Since(started), false); err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}
	slog.Info("datadog sync complete", "accounts", len(accounts))
	return nil
}

type datadogRoleKey struct {
	id   string
	name string
}

func dedupeDatadogRoles(roles []Role) []Role {
	if len(roles) == 0 {
		return nil
	}
	seen := make(map[datadogRoleKey]Role, len(roles))
	for _, role := range roles {
		id := strings.TrimSpace(role.ID)
		name := strings.TrimSpace(role.Name)
		key := datadogRoleKey{id: id, name: name}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = Role{ID: id, Name: name}
	}
	out := make([]Role, 0, len(seen))
	for _, role := range seen {
		out = append(out, role)
	}
	return out
}
