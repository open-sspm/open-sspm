package okta

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/matching"
	"github.com/open-sspm/open-sspm/internal/rules/datasets"
	"github.com/open-sspm/open-sspm/internal/rules/engine"
)

type OktaIntegration struct {
	client     *Client
	sourceName string
	workers    int
	lastRunID  int64
}

func NewOktaIntegration(client *Client, sourceName string, workers int) *OktaIntegration {
	if workers < 1 {
		workers = 3
	}
	return &OktaIntegration{
		client:     client,
		sourceName: strings.TrimSpace(sourceName),
		workers:    workers,
	}
}

func (i *OktaIntegration) Kind() string { return "okta" }
func (i *OktaIntegration) Name() string { return i.sourceName }
func (i *OktaIntegration) Role() registry.IntegrationRole {
	return registry.RoleIdP
}

func (i *OktaIntegration) InitEvents() []registry.Event {
	return []registry.Event{
		{Source: "okta", Stage: "list-users", Current: 0, Total: 1, Message: "listing users"},
		{Source: "okta", Stage: "sync-users", Current: 0, Total: registry.UnknownTotal, Message: "syncing users"},
		{Source: "okta", Stage: "sync-groups", Current: 0, Total: registry.UnknownTotal, Message: "syncing groups"},
		{Source: "okta", Stage: "sync-app-assignments", Current: 0, Total: registry.UnknownTotal, Message: "syncing app assignments"},
		{Source: "okta", Stage: "sync-app-group-assignments", Current: 0, Total: registry.UnknownTotal, Message: "syncing app group assignments"},
		{Source: "okta", Stage: "evaluate-rules", Current: 0, Total: 1, Message: "evaluating rulesets"},
	}
}

func (i *OktaIntegration) Run(ctx context.Context, deps registry.IntegrationDeps) error {
	started := time.Now()
	runID, err := deps.Q.CreateSyncRun(ctx, gen.CreateSyncRunParams{
		SourceKind: "okta",
		SourceName: i.sourceName,
	})
	if err != nil {
		return err
	}
	i.lastRunID = runID

	users, err := i.client.ListUsers(ctx)
	if err != nil {
		err = fmt.Errorf("okta list users (/api/v1/users): %w", err)
		deps.Report(registry.Event{Source: "okta", Stage: "list-users", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindAPI)
		return err
	}
	deps.Report(registry.Event{Source: "okta", Stage: "list-users", Current: 1, Total: 1, Message: fmt.Sprintf("found %d users", len(users))})
	deps.Report(registry.Event{Source: "okta", Stage: "sync-users", Current: 0, Total: int64(len(users)), Message: fmt.Sprintf("syncing %d users", len(users))})

	if err := i.syncOktaIdpUsers(ctx, deps, runID, users); err != nil {
		deps.Report(registry.Event{Source: "okta", Stage: "sync-users", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
		return err
	}

	if err := i.syncOktaGroups(ctx, deps, runID); err != nil {
		deps.Report(registry.Event{Source: "okta", Stage: "sync-groups", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
		return err
	}

	appIDs, err := i.syncOktaAppAssignments(ctx, deps, runID)
	if err != nil {
		deps.Report(registry.Event{Source: "okta", Stage: "sync-app-assignments", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
		return err
	}

	if err := i.syncOktaAppGroupAssignments(ctx, deps, runID, appIDs); err != nil {
		deps.Report(registry.Event{Source: "okta", Stage: "sync-app-group-assignments", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
		return err
	}

	if err := registry.FinalizeOktaRun(ctx, deps, runID, time.Since(started)); err != nil {
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
		return err
	}

	slog.Info("okta sync complete", "users", len(users))
	return nil
}

func (i *OktaIntegration) EvaluateCompliance(ctx context.Context, deps registry.IntegrationDeps) error {
	if i == nil {
		return nil
	}

	runID := i.lastRunID
	if runID == 0 {
		return nil
	}

	oktaProvider := &datasets.OktaProvider{
		Client:  i.client,
		BaseURL: i.client.BaseURL,
		Token:   i.client.Token,
	}
	router := datasets.RouterProvider{
		Okta: oktaProvider,
	}
	e := engine.Engine{
		Q:        deps.Q,
		Datasets: router,
		Now:      time.Now,
	}
	if err := e.Run(ctx, engine.Context{
		ScopeKind:   "connector_instance",
		SourceKind:  "okta",
		SourceName:  i.sourceName,
		SyncRunID:   &runID,
		EvaluatedAt: time.Now(),
	}); err != nil {
		err = fmt.Errorf("okta v1 ruleset evaluations: %w", err)
		slog.Error("okta ruleset evaluations failed", "err", err)
		deps.Report(registry.Event{Source: "okta", Stage: "evaluate-rules", Current: 1, Total: 1, Message: err.Error(), Err: err})
		return nil
	}

	deps.Report(registry.Event{Source: "okta", Stage: "evaluate-rules", Current: 1, Total: 1, Message: "evaluations complete"})
	return nil
}

func (i *OktaIntegration) syncOktaIdpUsers(ctx context.Context, deps registry.IntegrationDeps, runID int64, users []User) error {
	if len(users) == 0 {
		deps.Report(registry.Event{Source: "okta", Stage: "sync-users", Current: 0, Total: 0, Message: "no users to sync"})
		return nil
	}

	const batchSize = 1000
	for start := 0; start < len(users); start += batchSize {
		end := start + batchSize
		if end > len(users) {
			end = len(users)
		}
		batch := users[start:end]

		externalIDs := make([]string, 0, len(batch))
		emails := make([]string, 0, len(batch))
		displayNames := make([]string, 0, len(batch))
		statuses := make([]string, 0, len(batch))
		rawJSONs := make([][]byte, 0, len(batch))
		lastLoginAts := make([]pgtype.Timestamptz, 0, len(batch))
		lastLoginIPs := make([]string, 0, len(batch))
		lastLoginRegions := make([]string, 0, len(batch))

		for _, user := range batch {
			id := strings.TrimSpace(user.ID)
			if id == "" {
				continue
			}
			externalIDs = append(externalIDs, id)
			emails = append(emails, matching.NormalizeEmail(user.Email))
			displayNames = append(displayNames, user.DisplayName)
			statuses = append(statuses, user.Status)
			rawJSONs = append(rawJSONs, registry.NormalizeJSON(user.RawJSON))
			lastLoginAts = append(lastLoginAts, registry.PgTimestamptzPtr(user.LastLoginAt))
			lastLoginIPs = append(lastLoginIPs, "")
			lastLoginRegions = append(lastLoginRegions, "")
		}
		if len(externalIDs) == 0 {
			continue
		}

		if _, err := deps.Q.UpsertIdPUsersBulk(ctx, gen.UpsertIdPUsersBulkParams{
			SeenInRunID:      runID,
			ExternalIds:      externalIDs,
			Emails:           emails,
			DisplayNames:     displayNames,
			Statuses:         statuses,
			RawJsons:         rawJSONs,
			LastLoginAts:     lastLoginAts,
			LastLoginIps:     lastLoginIPs,
			LastLoginRegions: lastLoginRegions,
		}); err != nil {
			return fmt.Errorf("upsert idp users: %w", err)
		}

		deps.Report(registry.Event{
			Source:  "okta",
			Stage:   "sync-users",
			Current: int64(end),
			Total:   int64(len(users)),
			Message: fmt.Sprintf("users %d/%d", end, len(users)),
		})
	}

	return nil
}

func (i *OktaIntegration) syncOktaGroups(ctx context.Context, deps registry.IntegrationDeps, runID int64) error {
	groups, err := i.client.ListGroups(ctx)
	if err != nil {
		return fmt.Errorf("okta list groups: %w", err)
	}
	deps.Report(registry.Event{Source: "okta", Stage: "sync-groups", Current: 0, Total: int64(len(groups)), Message: fmt.Sprintf("syncing %d groups", len(groups))})

	if len(groups) == 0 {
		return nil
	}

	const batchSize = 500
	for start := 0; start < len(groups); start += batchSize {
		end := start + batchSize
		if end > len(groups) {
			end = len(groups)
		}
		batch := groups[start:end]

		externalIDs := make([]string, 0, len(batch))
		names := make([]string, 0, len(batch))
		types := make([]string, 0, len(batch))
		rawJSONs := make([][]byte, 0, len(batch))
		for _, group := range batch {
			id := strings.TrimSpace(group.ID)
			if id == "" {
				continue
			}
			externalIDs = append(externalIDs, id)
			names = append(names, group.Name)
			types = append(types, group.Type)
			rawJSONs = append(rawJSONs, registry.NormalizeJSON(group.RawJSON))
		}
		if len(externalIDs) == 0 {
			continue
		}
		if _, err := deps.Q.UpsertOktaGroupsBulk(ctx, gen.UpsertOktaGroupsBulkParams{
			SeenInRunID: runID,
			ExternalIds: externalIDs,
			Names:       names,
			Types:       types,
			RawJsons:    rawJSONs,
		}); err != nil {
			return fmt.Errorf("upsert okta groups: %w", err)
		}
	}

	workers := i.workers
	if len(groups) < workers {
		workers = len(groups)
	}
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
			const membershipBatchSize = 5000
			for start := 0; start < len(userExternalIDs); start += membershipBatchSize {
				end := start + membershipBatchSize
				if end > len(userExternalIDs) {
					end = len(userExternalIDs)
				}
				idpExternalIDs := make([]string, 0, end-start)
				groupExternalIDs := make([]string, 0, end-start)
				for _, userExternalID := range userExternalIDs[start:end] {
					userExternalID = strings.TrimSpace(userExternalID)
					if userExternalID == "" {
						continue
					}
					idpExternalIDs = append(idpExternalIDs, userExternalID)
					groupExternalIDs = append(groupExternalIDs, group.ID)
				}
				if len(idpExternalIDs) == 0 {
					continue
				}
				if _, err := deps.Q.UpsertOktaUserGroupsBulkByExternalIDs(jobCtx, gen.UpsertOktaUserGroupsBulkByExternalIDsParams{
					SeenInRunID:          runID,
					IdpUserExternalIds:   idpExternalIDs,
					OktaGroupExternalIds: groupExternalIDs,
				}); err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("upsert okta user groups for group %s: %w", group.ID, err)
						cancel()
					})
					return
				}
			}
			n := atomic.AddInt64(&done, 1)
			deps.Report(registry.Event{
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
		if strings.TrimSpace(group.ID) == "" {
			continue
		}
		jobs <- group
	}
	close(jobs)
	wg.Wait()

	return firstErr
}

func (i *OktaIntegration) syncOktaAppAssignments(ctx context.Context, deps registry.IntegrationDeps, runID int64) ([]string, error) {
	apps, err := i.client.ListApps(ctx)
	if err != nil {
		return nil, fmt.Errorf("okta list apps: %w", err)
	}
	validApps := make([]App, 0, len(apps))
	for _, app := range apps {
		if strings.TrimSpace(app.ID) == "" {
			continue
		}
		validApps = append(validApps, app)
	}
	deps.Report(registry.Event{Source: "okta", Stage: "sync-app-assignments", Current: 0, Total: int64(len(validApps)), Message: fmt.Sprintf("syncing %d apps", len(validApps))})

	appExternalIDs := make([]string, 0, len(validApps))
	if len(validApps) == 0 {
		return appExternalIDs, nil
	}

	const batchSize = 500
	for start := 0; start < len(validApps); start += batchSize {
		end := start + batchSize
		if end > len(validApps) {
			end = len(validApps)
		}
		batch := validApps[start:end]

		externalIDs := make([]string, 0, len(batch))
		labels := make([]string, 0, len(batch))
		names := make([]string, 0, len(batch))
		statuses := make([]string, 0, len(batch))
		signOnModes := make([]string, 0, len(batch))
		rawJSONs := make([][]byte, 0, len(batch))

		for _, app := range batch {
			id := strings.TrimSpace(app.ID)
			if id == "" {
				continue
			}
			appExternalIDs = append(appExternalIDs, id)
			externalIDs = append(externalIDs, id)
			labels = append(labels, app.Label)
			names = append(names, app.Name)
			statuses = append(statuses, app.Status)
			signOnModes = append(signOnModes, app.SignOnMode)
			rawJSONs = append(rawJSONs, registry.NormalizeJSON(app.RawJSON))
		}
		if len(externalIDs) == 0 {
			continue
		}
		if _, err := deps.Q.UpsertOktaAppsBulk(ctx, gen.UpsertOktaAppsBulkParams{
			SeenInRunID: runID,
			ExternalIds: externalIDs,
			Labels:      labels,
			Names:       names,
			Statuses:    statuses,
			SignOnModes: signOnModes,
			RawJsons:    rawJSONs,
		}); err != nil {
			return nil, fmt.Errorf("upsert okta apps: %w", err)
		}
	}

	workers := i.workers
	if len(validApps) < workers {
		workers = len(validApps)
	}
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
			assignments, err := i.client.ListApplicationUsers(jobCtx, app.ID)
			if err != nil {
				errOnce.Do(func() {
					firstErr = fmt.Errorf("okta app %s users: %w", app.ID, err)
					cancel()
				})
				return
			}
			if len(assignments) == 0 {
				n := atomic.AddInt64(&done, 1)
				deps.Report(registry.Event{
					Source:  "okta",
					Stage:   "sync-app-assignments",
					Current: n,
					Total:   int64(len(validApps)),
					Message: fmt.Sprintf("apps %d/%d", n, len(validApps)),
				})
				continue
			}

			const assignmentBatchSize = 5000
			for start := 0; start < len(assignments); start += assignmentBatchSize {
				end := start + assignmentBatchSize
				if end > len(assignments) {
					end = len(assignments)
				}
				idpExternalIDs := make([]string, 0, end-start)
				oktaAppExternalIDs := make([]string, 0, end-start)
				scopes := make([]string, 0, end-start)
				profileJSONs := make([][]byte, 0, end-start)
				rawJSONs := make([][]byte, 0, end-start)
				for _, assignment := range assignments[start:end] {
					userID := strings.TrimSpace(assignment.UserID)
					if userID == "" {
						continue
					}
					idpExternalIDs = append(idpExternalIDs, userID)
					oktaAppExternalIDs = append(oktaAppExternalIDs, app.ID)
					scopes = append(scopes, assignment.Scope)
					profileJSONs = append(profileJSONs, registry.NormalizeJSON(assignment.ProfileJSON))
					rawJSONs = append(rawJSONs, registry.NormalizeJSON(assignment.RawJSON))
				}
				if len(idpExternalIDs) == 0 {
					continue
				}
				if _, err := deps.Q.UpsertOktaUserAppAssignmentsBulkByExternalIDs(jobCtx, gen.UpsertOktaUserAppAssignmentsBulkByExternalIDsParams{
					SeenInRunID:        runID,
					IdpUserExternalIds: idpExternalIDs,
					OktaAppExternalIds: oktaAppExternalIDs,
					Scopes:             scopes,
					ProfileJsons:       profileJSONs,
					RawJsons:           rawJSONs,
				}); err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("upsert okta app assignments for app %s: %w", app.ID, err)
						cancel()
					})
					return
				}
			}

			n := atomic.AddInt64(&done, 1)
			deps.Report(registry.Event{
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

func (i *OktaIntegration) syncOktaAppGroupAssignments(ctx context.Context, deps registry.IntegrationDeps, runID int64, appExternalIDs []string) error {
	if len(appExternalIDs) == 0 {
		deps.Report(registry.Event{Source: "okta", Stage: "sync-app-group-assignments", Current: 0, Total: 0, Message: "no apps to sync"})
		return nil
	}
	deps.Report(registry.Event{Source: "okta", Stage: "sync-app-group-assignments", Current: 0, Total: int64(len(appExternalIDs)), Message: fmt.Sprintf("syncing %d apps", len(appExternalIDs))})

	workers := i.workers
	if len(appExternalIDs) < workers {
		workers = len(appExternalIDs)
	}
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
			if len(assignments) > 0 {
				externalIDs := make([]string, 0, len(assignments))
				names := make([]string, 0, len(assignments))
				types := make([]string, 0, len(assignments))
				groupRawJSONs := make([][]byte, 0, len(assignments))
				for _, assignment := range assignments {
					group := assignment.Group
					id := strings.TrimSpace(group.ID)
					if id == "" {
						continue
					}
					externalIDs = append(externalIDs, id)
					names = append(names, group.Name)
					types = append(types, group.Type)
					groupRawJSONs = append(groupRawJSONs, registry.NormalizeJSON(group.RawJSON))
				}
				if len(externalIDs) > 0 {
					if _, err := deps.Q.UpsertOktaGroupsBulk(jobCtx, gen.UpsertOktaGroupsBulkParams{
						SeenInRunID: runID,
						ExternalIds: externalIDs,
						Names:       names,
						Types:       types,
						RawJsons:    groupRawJSONs,
					}); err != nil {
						errOnce.Do(func() {
							firstErr = fmt.Errorf("upsert okta groups for app %s: %w", appExternalID, err)
							cancel()
						})
						return
					}
				}

				const assignmentBatchSize = 5000
				for start := 0; start < len(assignments); start += assignmentBatchSize {
					end := start + assignmentBatchSize
					if end > len(assignments) {
						end = len(assignments)
					}
					oktaAppExternalIDs := make([]string, 0, end-start)
					groupExternalIDs := make([]string, 0, end-start)
					priorities := make([]int32, 0, end-start)
					profileJSONs := make([][]byte, 0, end-start)
					rawJSONs := make([][]byte, 0, end-start)
					for _, assignment := range assignments[start:end] {
						groupID := strings.TrimSpace(assignment.Group.ID)
						if groupID == "" {
							continue
						}
						oktaAppExternalIDs = append(oktaAppExternalIDs, appExternalID)
						groupExternalIDs = append(groupExternalIDs, groupID)
						priorities = append(priorities, int32(assignment.Priority))
						profileJSONs = append(profileJSONs, registry.NormalizeJSON(assignment.ProfileJSON))
						rawJSONs = append(rawJSONs, registry.NormalizeJSON(assignment.RawJSON))
					}
					if len(oktaAppExternalIDs) == 0 {
						continue
					}
					if _, err := deps.Q.UpsertOktaAppGroupAssignmentsBulkByExternalIDs(jobCtx, gen.UpsertOktaAppGroupAssignmentsBulkByExternalIDsParams{
						SeenInRunID:          runID,
						OktaAppExternalIds:   oktaAppExternalIDs,
						OktaGroupExternalIds: groupExternalIDs,
						Priorities:           priorities,
						ProfileJsons:         profileJSONs,
						RawJsons:             rawJSONs,
					}); err != nil {
						errOnce.Do(func() {
							firstErr = fmt.Errorf("upsert okta app group assignments for app %s: %w", appExternalID, err)
							cancel()
						})
						return
					}
				}
			}
			n := atomic.AddInt64(&done, 1)
			deps.Report(registry.Event{
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
		externalID = strings.TrimSpace(externalID)
		if externalID == "" {
			continue
		}
		jobs <- externalID
	}
	close(jobs)
	wg.Wait()

	return firstErr
}
