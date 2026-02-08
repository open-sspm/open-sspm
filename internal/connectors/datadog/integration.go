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
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/matching"
)

type DatadogIntegration struct {
	client  *Client
	site    string
	workers int
}

func NewDatadogIntegration(client *Client, site string, workers int) *DatadogIntegration {
	if workers < 1 {
		workers = 3
	}
	return &DatadogIntegration{
		client:  client,
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
		{Source: "datadog", Stage: "list-users", Current: 0, Total: 1, Message: "listing users"},
		{Source: "datadog", Stage: "list-roles", Current: 0, Total: 1, Message: "listing roles"},
		{Source: "datadog", Stage: "fetch-role-users", Current: 0, Total: registry.UnknownTotal, Message: "listing role users"},
		{Source: "datadog", Stage: "write-users", Current: 0, Total: registry.UnknownTotal, Message: "writing users"},
	}
}

func (i *DatadogIntegration) Run(ctx context.Context, deps registry.IntegrationDeps) error {
	started := time.Now()
	slog.Info("syncing Datadog")

	runID, err := deps.Q.CreateSyncRun(ctx, gen.CreateSyncRunParams{
		SourceKind: "datadog",
		SourceName: i.site,
	})
	if err != nil {
		return err
	}

	users, err := i.client.ListUsers(ctx)
	if err != nil {
		deps.Report(registry.Event{Source: "datadog", Stage: "list-users", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindAPI)
		return err
	}
	deps.Report(registry.Event{Source: "datadog", Stage: "list-users", Current: 1, Total: 1, Message: fmt.Sprintf("found %d users", len(users))})

	roles, err := i.client.ListRoles(ctx)
	if err != nil {
		deps.Report(registry.Event{Source: "datadog", Stage: "list-roles", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindAPI)
		return err
	}
	deps.Report(registry.Event{Source: "datadog", Stage: "list-roles", Current: 1, Total: 1, Message: fmt.Sprintf("found %d roles", len(roles))})

	rolesByUserExternalID := make(map[string][]Role)
	if len(roles) > 0 {
		type roleUsersResult struct {
			role  Role
			users []User
			err   error
		}

		rolesCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		jobs := make(chan Role, len(roles))
		results := make(chan roleUsersResult, len(roles))
		var rolesDone int64

		workers := i.workers
		if len(roles) < workers {
			workers = len(roles)
		}
		if workers < 1 {
			workers = 1
		}

		deps.Report(registry.Event{
			Source:  "datadog",
			Stage:   "fetch-role-users",
			Current: 0,
			Total:   int64(len(roles)),
			Message: fmt.Sprintf("fetching users for %d roles", len(roles)),
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
					users, err := i.client.ListRoleUsers(rolesCtx, role.ID)
					if err != nil {
						results <- roleUsersResult{role: role, err: fmt.Errorf("datadog role %s users: %w", strings.TrimSpace(role.ID), err)}
						cancel()
						continue
					}
					n := atomic.AddInt64(&rolesDone, 1)
					deps.Report(registry.Event{
						Source:  "datadog",
						Stage:   "fetch-role-users",
						Current: n,
						Total:   int64(len(roles)),
						Message: fmt.Sprintf("roles %d/%d", n, len(roles)),
					})
					results <- roleUsersResult{role: role, users: users}
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
			for _, user := range res.users {
				userID := strings.TrimSpace(user.ID)
				if userID == "" {
					continue
				}
				rolesByUserExternalID[userID] = append(rolesByUserExternalID[userID], Role{ID: roleID, Name: roleName})
			}
		}
		if firstNonCancelErr != nil {
			firstErr = firstNonCancelErr
		}
		if firstErr != nil {
			deps.Report(registry.Event{Source: "datadog", Stage: "fetch-role-users", Message: firstErr.Error(), Err: firstErr})
			registry.FailSyncRun(ctx, deps.Q, runID, firstErr, registry.SyncErrorKindAPI)
			return firstErr
		}
	}

	deps.Report(registry.Event{Source: "datadog", Stage: "write-users", Current: 0, Total: int64(len(users)), Message: fmt.Sprintf("writing %d users", len(users))})

	const userBatchSize = 1000
	externalIDs := make([]string, 0, len(users))
	emails := make([]string, 0, len(users))
	displayNames := make([]string, 0, len(users))
	rawJSONs := make([][]byte, 0, len(users))
	lastLoginAts := make([]pgtype.Timestamptz, 0, len(users))
	lastLoginIps := make([]string, 0, len(users))
	lastLoginRegions := make([]string, 0, len(users))

	for _, user := range users {
		externalID := strings.TrimSpace(user.ID)
		if externalID == "" {
			continue
		}
		userName := strings.TrimSpace(user.UserName)
		if userName == "" {
			userName = externalID
		}
		externalIDs = append(externalIDs, externalID)
		emails = append(emails, matching.NormalizeEmail(userName))
		displayNames = append(displayNames, userName)
		rawJSONs = append(rawJSONs, registry.NormalizeJSON(user.RawJSON))
		lastLoginAts = append(lastLoginAts, registry.PgTimestamptzPtr(user.LastLoginAt))
		lastLoginIps = append(lastLoginIps, "")
		lastLoginRegions = append(lastLoginRegions, "")
	}

	for start := 0; start < len(externalIDs); start += userBatchSize {
		end := start + userBatchSize
		if end > len(externalIDs) {
			end = len(externalIDs)
		}
		_, err := deps.Q.UpsertAppUsersBulkBySource(ctx, gen.UpsertAppUsersBulkBySourceParams{
			SourceKind:       "datadog",
			SourceName:       i.site,
			SeenInRunID:      runID,
			ExternalIds:      externalIDs[start:end],
			Emails:           emails[start:end],
			DisplayNames:     displayNames[start:end],
			RawJsons:         rawJSONs[start:end],
			LastLoginAts:     lastLoginAts[start:end],
			LastLoginIps:     lastLoginIps[start:end],
			LastLoginRegions: lastLoginRegions[start:end],
		})
		if err != nil {
			deps.Report(registry.Event{Source: "datadog", Stage: "write-users", Message: err.Error(), Err: err})
			registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
			return err
		}
		deps.Report(registry.Event{
			Source:  "datadog",
			Stage:   "write-users",
			Current: int64(end),
			Total:   int64(len(externalIDs)),
			Message: fmt.Sprintf("users %d/%d", end, len(externalIDs)),
		})
	}

	const entitlementBatchSize = 5000
	entAppUserExternalIDs := make([]string, 0, len(users))
	entKinds := make([]string, 0, len(users))
	entResources := make([]string, 0, len(users))
	entPermissions := make([]string, 0, len(users))
	entRawJSONs := make([][]byte, 0, len(users))

	for _, user := range users {
		userID := strings.TrimSpace(user.ID)
		if userID == "" {
			continue
		}
		for _, role := range dedupeDatadogRoles(rolesByUserExternalID[userID]) {
			roleID := strings.TrimSpace(role.ID)
			roleName := strings.TrimSpace(role.Name)
			externalID := roleID
			if externalID == "" {
				externalID = roleName
			}
			if externalID == "" {
				continue
			}
			entAppUserExternalIDs = append(entAppUserExternalIDs, userID)
			entKinds = append(entKinds, "datadog_role")
			entResources = append(entResources, "datadog_role:"+externalID)
			entPermissions = append(entPermissions, "member")
			entRawJSONs = append(entRawJSONs, registry.MarshalJSON(map[string]string{
				"role_id":   roleID,
				"role_name": roleName,
			}))
		}
	}

	for start := 0; start < len(entAppUserExternalIDs); start += entitlementBatchSize {
		end := start + entitlementBatchSize
		if end > len(entAppUserExternalIDs) {
			end = len(entAppUserExternalIDs)
		}
		_, err := deps.Q.UpsertEntitlementsBulkBySource(ctx, gen.UpsertEntitlementsBulkBySourceParams{
			SeenInRunID:        runID,
			SourceKind:         "datadog",
			SourceName:         i.site,
			AppUserExternalIds: entAppUserExternalIDs[start:end],
			Kinds:              entKinds[start:end],
			Resources:          entResources[start:end],
			Permissions:        entPermissions[start:end],
			RawJsons:           entRawJSONs[start:end],
		})
		if err != nil {
			deps.Report(registry.Event{Source: "datadog", Stage: "write-users", Message: err.Error(), Err: err})
			registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
			return err
		}
	}

	if err := registry.FinalizeAppRun(ctx, deps, runID, "datadog", i.site, time.Since(started), false); err != nil {
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
		return err
	}
	slog.Info("datadog sync complete", "users", len(users))
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
