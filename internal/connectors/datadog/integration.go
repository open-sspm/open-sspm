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

	"github.com/jackc/pgx/v5/pgtype"
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

func (i *DatadogIntegration) Run(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, report func(registry.Event), _ registry.RunMode) error {
	started := time.Now()
	slog.Info("syncing Datadog")

	runID, err := registry.StartSyncRun(ctx, q, "datadog", i.site)
	if err != nil {
		return err
	}

	accounts, err := i.adapter.ListAccounts(ctx)
	if err != nil {
		report(registry.Event{Source: "datadog", Stage: "list-accounts", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}
	report(registry.Event{Source: "datadog", Stage: "list-accounts", Current: 1, Total: 1, Message: fmt.Sprintf("found %d accounts", len(accounts))})

	roles, err := i.adapter.ListRoles(ctx)
	if err != nil {
		report(registry.Event{Source: "datadog", Stage: "list-roles", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
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
			report(registry.Event{Source: "datadog", Stage: "fetch-role-members", Message: firstErr.Error(), Err: firstErr})
			return registry.FailSyncRun(ctx, q, runID, firstErr, registry.SyncErrorKindAPI)
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
	externalIDs := make([]string, 0, totalPrincipals)
	emails := make([]string, 0, totalPrincipals)
	displayNames := make([]string, 0, totalPrincipals)
	accountKinds := make([]string, 0, totalPrincipals)
	entityCategories := make([]string, 0, totalPrincipals)
	rawJSONs := make([][]byte, 0, totalPrincipals)
	lastLoginAts := make([]pgtype.Timestamptz, 0, totalPrincipals)
	lastLoginIps := make([]string, 0, totalPrincipals)
	lastLoginRegions := make([]string, 0, totalPrincipals)

	for _, account := range accounts {
		externalID := strings.TrimSpace(account.ExternalID)
		if externalID == "" {
			continue
		}
		display := strings.TrimSpace(account.DisplayName)
		if display == "" {
			display = externalID
		}
		externalIDs = append(externalIDs, externalID)
		emails = append(emails, matching.NormalizeEmail(account.Email))
		displayNames = append(displayNames, display)
		accountKinds = append(accountKinds, account.AccountKind)
		entityCategories = append(entityCategories, account.EntityCategory)
		rawJSONs = append(rawJSONs, registry.WithEntityCategory(registry.NormalizeJSON(account.RawJSON), account.EntityCategory))
		lastLoginAts = append(lastLoginAts, registry.PgTimestamptzPtr(account.LastLoginAt))
		lastLoginIps = append(lastLoginIps, "")
		lastLoginRegions = append(lastLoginRegions, "")
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
		externalIDs = append(externalIDs, roleExternalID)
		emails = append(emails, "")
		displayNames = append(displayNames, display)
		accountKinds = append(accountKinds, registry.AccountKindService)
		entityCategories = append(entityCategories, registry.EntityCategoryRole)
		rawJSONs = append(rawJSONs, registry.WithEntityCategory(registry.NormalizeJSON(role.RawJSON), registry.EntityCategoryRole))
		lastLoginAts = append(lastLoginAts, pgtype.Timestamptz{})
		lastLoginIps = append(lastLoginIps, "")
		lastLoginRegions = append(lastLoginRegions, "")
	}

	for start := 0; start < len(externalIDs); start += userBatchSize {
		end := min(start+userBatchSize, len(externalIDs))
		_, err := q.UpsertSourceAccountsBulkBySource(ctx, gen.UpsertSourceAccountsBulkBySourceParams{
			SourceKind:       "datadog",
			SourceName:       i.site,
			SeenInRunID:      runID,
			ExternalIds:      externalIDs[start:end],
			Emails:           emails[start:end],
			DisplayNames:     displayNames[start:end],
			AccountKinds:     accountKinds[start:end],
			EntityCategories: entityCategories[start:end],
			RawJsons:         rawJSONs[start:end],
			LastLoginAts:     lastLoginAts[start:end],
			LastLoginIps:     lastLoginIps[start:end],
			LastLoginRegions: lastLoginRegions[start:end],
		})
		if err != nil {
			report(registry.Event{Source: "datadog", Stage: "write-principals", Message: err.Error(), Err: err})
			return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
		}
		report(registry.Event{
			Source:  "datadog",
			Stage:   "write-principals",
			Current: int64(end),
			Total:   int64(len(externalIDs)),
			Message: fmt.Sprintf("principals %d/%d", end, len(externalIDs)),
		})
	}

	const entitlementBatchSize = 5000
	entAccountExternalIDs := make([]string, 0, len(accounts))
	entKinds := make([]string, 0, len(accounts))
	entResources := make([]string, 0, len(accounts))
	entPermissions := make([]string, 0, len(accounts))
	entRawJSONs := make([][]byte, 0, len(accounts))

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
			entAccountExternalIDs = append(entAccountExternalIDs, accountExternalID)
			entKinds = append(entKinds, "datadog_role")
			entResources = append(entResources, "datadog_role:"+externalID)
			entPermissions = append(entPermissions, "member")
			entRawJSONs = append(entRawJSONs, registry.MarshalJSON(map[string]string{
				"role_id":   roleID,
				"role_name": roleName,
			}))
		}
	}

	for start := 0; start < len(entAccountExternalIDs); start += entitlementBatchSize {
		end := min(start+entitlementBatchSize, len(entAccountExternalIDs))
		_, err := q.UpsertEntitlementsBulkBySource(ctx, gen.UpsertEntitlementsBulkBySourceParams{
			SeenInRunID:        runID,
			SourceKind:         "datadog",
			SourceName:         i.site,
			AccountExternalIds: entAccountExternalIDs[start:end],
			Kinds:              entKinds[start:end],
			Resources:          entResources[start:end],
			Permissions:        entPermissions[start:end],
			RawJsons:           entRawJSONs[start:end],
		})
		if err != nil {
			report(registry.Event{Source: "datadog", Stage: "write-principals", Message: err.Error(), Err: err})
			return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
		}
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
