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
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
	"github.com/open-sspm/open-sspm/internal/matching"
	"github.com/open-sspm/open-sspm/internal/metrics"
	"github.com/open-sspm/open-sspm/internal/rules/datasets"
	"github.com/open-sspm/open-sspm/internal/rules/engine"
)

type OktaIntegration struct {
	client           *Client
	sourceName       string
	workers          int
	discoveryEnabled bool
	lastRunID        int64
}

func NewOktaIntegration(client *Client, sourceName string, workers int, discoveryEnabled bool) *OktaIntegration {
	if workers < 1 {
		workers = 3
	}
	return &OktaIntegration{
		client:           client,
		sourceName:       strings.TrimSpace(sourceName),
		workers:          workers,
		discoveryEnabled: discoveryEnabled,
	}
}

func (i *OktaIntegration) Kind() string { return "okta" }
func (i *OktaIntegration) Name() string { return i.sourceName }
func (i *OktaIntegration) Role() registry.IntegrationRole {
	return registry.RoleIdP
}

func (i *OktaIntegration) SupportsRunMode(mode registry.RunMode) bool {
	if i == nil {
		return false
	}
	switch mode.Normalize() {
	case registry.RunModeDiscovery:
		return i.discoveryEnabled
	default:
		return true
	}
}

func (i *OktaIntegration) InitEvents() []registry.Event {
	return []registry.Event{
		{Source: "okta", Stage: "list-users", Current: 0, Total: 1, Message: "listing users"},
		{Source: "okta", Stage: "sync-users", Current: 0, Total: registry.UnknownTotal, Message: "syncing users"},
		{Source: "okta", Stage: "sync-groups", Current: 0, Total: registry.UnknownTotal, Message: "syncing groups"},
		{Source: "okta", Stage: "sync-app-assignments", Current: 0, Total: registry.UnknownTotal, Message: "syncing app assignments"},
		{Source: "okta", Stage: "sync-app-group-assignments", Current: 0, Total: registry.UnknownTotal, Message: "syncing app group assignments"},
		{Source: "okta", Stage: "list-discovery-events", Current: 0, Total: 1, Message: "listing discovery events"},
		{Source: "okta", Stage: "normalize-discovery", Current: 0, Total: 1, Message: "normalizing discovery events"},
		{Source: "okta", Stage: "write-discovery", Current: 0, Total: registry.UnknownTotal, Message: "writing discovery data"},
		{Source: "okta", Stage: "evaluate-rules", Current: 0, Total: 1, Message: "evaluating rulesets"},
	}
}

func (i *OktaIntegration) Run(ctx context.Context, deps registry.IntegrationDeps) error {
	switch deps.Mode.Normalize() {
	case registry.RunModeDiscovery:
		if !i.SupportsRunMode(registry.RunModeDiscovery) {
			return nil
		}
		return i.runDiscovery(ctx, deps)
	default:
		return i.runFull(ctx, deps)
	}
}

func (i *OktaIntegration) runFull(ctx context.Context, deps registry.IntegrationDeps) error {
	started := time.Now()
	runID, err := deps.Q.CreateSyncRun(ctx, gen.CreateSyncRunParams{
		SourceKind: registry.SyncRunSourceKind("okta", registry.RunModeFull),
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
		return registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindAPI)
	}
	deps.Report(registry.Event{Source: "okta", Stage: "list-users", Current: 1, Total: 1, Message: fmt.Sprintf("found %d users", len(users))})
	deps.Report(registry.Event{Source: "okta", Stage: "sync-users", Current: 0, Total: int64(len(users)), Message: fmt.Sprintf("syncing %d users", len(users))})

	if err := i.syncOktaIdpUsers(ctx, deps, runID, users); err != nil {
		deps.Report(registry.Event{Source: "okta", Stage: "sync-users", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
	}

	if err := i.syncOktaGroups(ctx, deps, runID); err != nil {
		deps.Report(registry.Event{Source: "okta", Stage: "sync-groups", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
	}

	appIDs, err := i.syncOktaAppAssignments(ctx, deps, runID)
	if err != nil {
		deps.Report(registry.Event{Source: "okta", Stage: "sync-app-assignments", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
	}

	if err := i.syncOktaAppGroupAssignments(ctx, deps, runID, appIDs); err != nil {
		deps.Report(registry.Event{Source: "okta", Stage: "sync-app-group-assignments", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
	}

	if err := registry.FinalizeOktaRun(ctx, deps, runID, i.sourceName, time.Since(started), false); err != nil {
		return registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
	}

	slog.Info("okta sync complete", "users", len(users))
	return nil
}

func (i *OktaIntegration) runDiscovery(ctx context.Context, deps registry.IntegrationDeps) error {
	started := time.Now()
	runID, err := deps.Q.CreateSyncRun(ctx, gen.CreateSyncRunParams{
		SourceKind: registry.SyncRunSourceKind("okta", registry.RunModeDiscovery),
		SourceName: i.sourceName,
	})
	if err != nil {
		return err
	}
	if err := i.syncDiscovery(ctx, deps, runID); err != nil {
		deps.Report(registry.Event{Source: "okta", Stage: "write-discovery", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindUnknown)
	}
	if err := registry.FinalizeDiscoveryRun(ctx, deps, runID, "okta", i.sourceName, time.Since(started)); err != nil {
		return registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
	}
	slog.Info("okta discovery sync complete", "source", i.sourceName)
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
		err = fmt.Errorf("okta ruleset evaluations: %w", err)
		slog.Error("okta ruleset evaluations failed", "err", err)
		deps.Report(registry.Event{Source: "okta", Stage: "evaluate-rules", Current: 1, Total: 1, Message: err.Error(), Err: err})
		return err
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
		end := min(start+batchSize, len(users))
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

		if _, err := deps.Q.UpsertOktaAccountsBulk(ctx, gen.UpsertOktaAccountsBulkParams{
			SourceName:       i.sourceName,
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
		end := min(start+batchSize, len(groups))
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
			const membershipBatchSize = 5000
			for start := 0; start < len(userExternalIDs); start += membershipBatchSize {
				end := min(start+membershipBatchSize, len(userExternalIDs))
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
		end := min(start+batchSize, len(validApps))
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
				end := min(start+assignmentBatchSize, len(assignments))
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
					end := min(start+assignmentBatchSize, len(assignments))
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

type normalizedDiscoverySource struct {
	CanonicalKey     string
	SourceAppID      string
	SourceAppName    string
	SourceAppDomain  string
	SourceVendorName string
	SeenAt           time.Time
}

type normalizedDiscoveryEvent struct {
	CanonicalKey     string
	SignalKind       string
	EventExternalID  string
	SourceAppID      string
	SourceAppName    string
	SourceAppDomain  string
	SourceVendorName string
	ActorExternalID  string
	ActorEmail       string
	ActorDisplayName string
	ObservedAt       time.Time
	Scopes           []string
	RawJSON          []byte
}

func (i *OktaIntegration) syncDiscovery(ctx context.Context, deps registry.IntegrationDeps, runID int64) error {
	deps.Report(registry.Event{Source: "okta", Stage: "list-discovery-events", Current: 0, Total: 1, Message: "listing discovery events"})

	now := time.Now().UTC()
	since := now.Add(-7 * 24 * time.Hour)
	latestObservedAt, err := deps.Q.GetLatestSaaSDiscoveryObservedAtBySource(ctx, gen.GetLatestSaaSDiscoveryObservedAtBySourceParams{
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
	deps.Report(registry.Event{
		Source:  "okta",
		Stage:   "list-discovery-events",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("found %d events since %s", len(events), since.Format(time.RFC3339)),
	})

	deps.Report(registry.Event{Source: "okta", Stage: "normalize-discovery", Current: 0, Total: 1, Message: "normalizing discovery events"})
	sources, normalizedEvents := normalizeOktaDiscovery(events, i.sourceName, now)
	deps.Report(registry.Event{
		Source:  "okta",
		Stage:   "normalize-discovery",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("normalized %d source rows and %d events", len(sources), len(normalizedEvents)),
	})

	if err := i.writeDiscoveryRows(ctx, deps, runID, sources, normalizedEvents); err != nil {
		metrics.DiscoveryIngestFailuresTotal.WithLabelValues("okta", "idp_sso", "db_error").Inc()
		return err
	}
	if err := i.seedOktaAutoBindings(ctx, deps); err != nil {
		return err
	}
	return nil
}

func normalizeOktaDiscovery(events []SystemLogEvent, sourceName string, now time.Time) ([]normalizedDiscoverySource, []normalizedDiscoveryEvent) {
	sourceByID := make(map[string]normalizedDiscoverySource, len(events))
	normalizedEvents := make([]normalizedDiscoveryEvent, 0, len(events))

	for _, event := range events {
		sourceAppID := strings.TrimSpace(event.AppID)
		if sourceAppID == "" {
			sourceAppID = strings.TrimSpace(event.AppName)
		}
		if sourceAppID == "" {
			continue
		}

		sourceAppName := strings.TrimSpace(event.AppName)
		if sourceAppName == "" {
			sourceAppName = sourceAppID
		}
		sourceAppDomain := strings.TrimSpace(event.AppDomain)
		signalKind := oktaDiscoverySignalKind(event.EventType, sourceAppID != "")
		if signalKind == "" {
			continue
		}

		observedAt := event.Published.UTC()
		if observedAt.IsZero() {
			observedAt = now
		}

		metadata := discovery.BuildMetadata(discovery.CanonicalInput{
			SourceKind:    "okta",
			SourceName:    sourceName,
			SourceAppID:   sourceAppID,
			SourceAppName: sourceAppName,
			SourceDomain:  sourceAppDomain,
		})
		if metadata.VendorName == "" {
			metadata = discovery.BuildMetadata(discovery.CanonicalInput{
				SourceKind:       "okta",
				SourceName:       sourceName,
				SourceAppID:      sourceAppID,
				SourceAppName:    sourceAppName,
				SourceDomain:     sourceAppDomain,
				SourceVendorName: sourceAppName,
			})
		}

		current := sourceByID[sourceAppID]
		if current.SourceAppID == "" || observedAt.After(current.SeenAt) {
			sourceByID[sourceAppID] = normalizedDiscoverySource{
				CanonicalKey:     metadata.CanonicalKey,
				SourceAppID:      sourceAppID,
				SourceAppName:    sourceAppName,
				SourceAppDomain:  metadata.Domain,
				SourceVendorName: metadata.VendorName,
				SeenAt:           observedAt,
			}
		}

		normalizedEvents = append(normalizedEvents, normalizedDiscoveryEvent{
			CanonicalKey:     metadata.CanonicalKey,
			SignalKind:       signalKind,
			EventExternalID:  strings.TrimSpace(event.ID),
			SourceAppID:      sourceAppID,
			SourceAppName:    sourceAppName,
			SourceAppDomain:  metadata.Domain,
			SourceVendorName: metadata.VendorName,
			ActorExternalID:  strings.TrimSpace(event.ActorID),
			ActorEmail:       strings.ToLower(strings.TrimSpace(event.ActorEmail)),
			ActorDisplayName: strings.TrimSpace(event.ActorName),
			ObservedAt:       observedAt,
			Scopes:           discovery.NormalizeScopes(event.GrantedScopes),
			RawJSON:          registry.NormalizeJSON(event.RawJSON),
		})
	}

	sourceRows := make([]normalizedDiscoverySource, 0, len(sourceByID))
	for _, sourceRow := range sourceByID {
		sourceRows = append(sourceRows, sourceRow)
	}
	return sourceRows, normalizedEvents
}

func oktaDiscoverySignalKind(eventType string, hasApp bool) string {
	eventType = strings.ToLower(strings.TrimSpace(eventType))
	if eventType == "" {
		if hasApp {
			return discovery.SignalKindIDPSSO
		}
		return ""
	}
	if strings.Contains(eventType, "oauth") ||
		strings.Contains(eventType, "grant") ||
		strings.Contains(eventType, "consent") {
		return discovery.SignalKindOAuth
	}
	if hasApp {
		return discovery.SignalKindIDPSSO
	}
	return ""
}

func (i *OktaIntegration) writeDiscoveryRows(ctx context.Context, deps registry.IntegrationDeps, runID int64, sources []normalizedDiscoverySource, events []normalizedDiscoveryEvent) error {
	total := len(sources) + len(events)
	deps.Report(registry.Event{
		Source:  "okta",
		Stage:   "write-discovery",
		Current: 0,
		Total:   int64(total),
		Message: fmt.Sprintf("writing %d discovery records", total),
	})

	appMeta := map[string]discovery.AppMetadata{}
	firstSeenByKey := map[string]time.Time{}
	lastSeenByKey := map[string]time.Time{}
	addMeta := func(key string, seenAt time.Time, sample discovery.AppMetadata) {
		if key == "" {
			return
		}
		if _, ok := appMeta[key]; !ok {
			appMeta[key] = sample
			firstSeenByKey[key] = seenAt
			lastSeenByKey[key] = seenAt
			return
		}
		if seenAt.Before(firstSeenByKey[key]) {
			firstSeenByKey[key] = seenAt
		}
		if seenAt.After(lastSeenByKey[key]) {
			lastSeenByKey[key] = seenAt
		}
	}

	for _, source := range sources {
		meta := discovery.BuildMetadata(discovery.CanonicalInput{
			SourceKind:       "okta",
			SourceName:       i.sourceName,
			SourceAppID:      source.SourceAppID,
			SourceAppName:    source.SourceAppName,
			SourceDomain:     source.SourceAppDomain,
			SourceVendorName: source.SourceVendorName,
		})
		meta.CanonicalKey = source.CanonicalKey
		addMeta(source.CanonicalKey, source.SeenAt, meta)
	}
	for _, event := range events {
		meta := discovery.BuildMetadata(discovery.CanonicalInput{
			SourceKind:       "okta",
			SourceName:       i.sourceName,
			SourceAppID:      event.SourceAppID,
			SourceAppName:    event.SourceAppName,
			SourceDomain:     event.SourceAppDomain,
			SourceVendorName: event.SourceVendorName,
		})
		meta.CanonicalKey = event.CanonicalKey
		addMeta(event.CanonicalKey, event.ObservedAt, meta)
	}

	if len(appMeta) > 0 {
		canonicalKeys := make([]string, 0, len(appMeta))
		displayNames := make([]string, 0, len(appMeta))
		primaryDomains := make([]string, 0, len(appMeta))
		vendorNames := make([]string, 0, len(appMeta))
		firstSeenAts := make([]pgtype.Timestamptz, 0, len(appMeta))
		lastSeenAts := make([]pgtype.Timestamptz, 0, len(appMeta))

		for key, meta := range appMeta {
			canonicalKeys = append(canonicalKeys, key)
			displayNames = append(displayNames, meta.DisplayName)
			primaryDomains = append(primaryDomains, meta.Domain)
			vendorNames = append(vendorNames, meta.VendorName)
			firstSeenAt := firstSeenByKey[key]
			lastSeenAt := lastSeenByKey[key]
			firstSeenAts = append(firstSeenAts, registry.PgTimestamptzPtr(&firstSeenAt))
			lastSeenAts = append(lastSeenAts, registry.PgTimestamptzPtr(&lastSeenAt))
		}

		if _, err := deps.Q.UpsertSaaSAppsBulk(ctx, gen.UpsertSaaSAppsBulkParams{
			CanonicalKeys:  canonicalKeys,
			DisplayNames:   displayNames,
			PrimaryDomains: primaryDomains,
			VendorNames:    vendorNames,
			FirstSeenAts:   firstSeenAts,
			LastSeenAts:    lastSeenAts,
		}); err != nil {
			return fmt.Errorf("upsert saas apps: %w", err)
		}
	}

	written := 0
	if len(sources) > 0 {
		canonicalKeys := make([]string, 0, len(sources))
		sourceAppIDs := make([]string, 0, len(sources))
		sourceAppNames := make([]string, 0, len(sources))
		sourceAppDomains := make([]string, 0, len(sources))
		seenAts := make([]pgtype.Timestamptz, 0, len(sources))
		for _, source := range sources {
			canonicalKeys = append(canonicalKeys, source.CanonicalKey)
			sourceAppIDs = append(sourceAppIDs, source.SourceAppID)
			sourceAppNames = append(sourceAppNames, source.SourceAppName)
			sourceAppDomains = append(sourceAppDomains, source.SourceAppDomain)
			seenAts = append(seenAts, registry.PgTimestamptzPtr(&source.SeenAt))
		}
		if _, err := deps.Q.UpsertSaaSAppSourcesBulkBySource(ctx, gen.UpsertSaaSAppSourcesBulkBySourceParams{
			SourceKind:       "okta",
			SourceName:       i.sourceName,
			SeenInRunID:      runID,
			CanonicalKeys:    canonicalKeys,
			SourceAppIds:     sourceAppIDs,
			SourceAppNames:   sourceAppNames,
			SourceAppDomains: sourceAppDomains,
			SeenAts:          seenAts,
		}); err != nil {
			return fmt.Errorf("upsert saas app sources: %w", err)
		}
		written += len(sources)
		deps.Report(registry.Event{
			Source:  "okta",
			Stage:   "write-discovery",
			Current: int64(written),
			Total:   int64(total),
			Message: fmt.Sprintf("sources %d/%d", written, total),
		})
	}

	if len(events) > 0 {
		canonicalKeys := make([]string, 0, len(events))
		signalKinds := make([]string, 0, len(events))
		eventExternalIDs := make([]string, 0, len(events))
		sourceAppIDs := make([]string, 0, len(events))
		sourceAppNames := make([]string, 0, len(events))
		sourceAppDomains := make([]string, 0, len(events))
		actorExternalIDs := make([]string, 0, len(events))
		actorEmails := make([]string, 0, len(events))
		actorDisplayNames := make([]string, 0, len(events))
		observedAts := make([]pgtype.Timestamptz, 0, len(events))
		scopesJSONs := make([][]byte, 0, len(events))
		rawJSONs := make([][]byte, 0, len(events))
		ingestedBySignal := map[string]int{}
		for _, event := range events {
			canonicalKeys = append(canonicalKeys, event.CanonicalKey)
			signalKinds = append(signalKinds, event.SignalKind)
			eventExternalIDs = append(eventExternalIDs, event.EventExternalID)
			sourceAppIDs = append(sourceAppIDs, event.SourceAppID)
			sourceAppNames = append(sourceAppNames, event.SourceAppName)
			sourceAppDomains = append(sourceAppDomains, event.SourceAppDomain)
			actorExternalIDs = append(actorExternalIDs, event.ActorExternalID)
			actorEmails = append(actorEmails, event.ActorEmail)
			actorDisplayNames = append(actorDisplayNames, event.ActorDisplayName)
			observedAts = append(observedAts, registry.PgTimestamptzPtr(&event.ObservedAt))
			scopesJSONs = append(scopesJSONs, discovery.ScopesJSON(event.Scopes))
			rawJSONs = append(rawJSONs, registry.NormalizeJSON(event.RawJSON))
			ingestedBySignal[event.SignalKind]++
		}
		if _, err := deps.Q.UpsertSaaSAppEventsBulkBySource(ctx, gen.UpsertSaaSAppEventsBulkBySourceParams{
			SourceKind:        "okta",
			SourceName:        i.sourceName,
			SeenInRunID:       runID,
			CanonicalKeys:     canonicalKeys,
			SignalKinds:       signalKinds,
			EventExternalIds:  eventExternalIDs,
			SourceAppIds:      sourceAppIDs,
			SourceAppNames:    sourceAppNames,
			SourceAppDomains:  sourceAppDomains,
			ActorExternalIds:  actorExternalIDs,
			ActorEmails:       actorEmails,
			ActorDisplayNames: actorDisplayNames,
			ObservedAts:       observedAts,
			ScopesJsons:       scopesJSONs,
			RawJsons:          rawJSONs,
		}); err != nil {
			return fmt.Errorf("upsert saas app events: %w", err)
		}
		for signalKind, count := range ingestedBySignal {
			metrics.DiscoveryEventsIngestedTotal.WithLabelValues("okta", signalKind).Add(float64(count))
		}
		written += len(events)
		deps.Report(registry.Event{
			Source:  "okta",
			Stage:   "write-discovery",
			Current: int64(written),
			Total:   int64(total),
			Message: fmt.Sprintf("events %d/%d", written, total),
		})
	}

	if written == 0 {
		deps.Report(registry.Event{
			Source:  "okta",
			Stage:   "write-discovery",
			Current: 0,
			Total:   0,
			Message: "no discovery records to write",
		})
	}
	return nil
}

func (i *OktaIntegration) seedOktaAutoBindings(ctx context.Context, deps registry.IntegrationDeps) error {
	rows, err := deps.Q.ListMappedOktaDiscoveryAppsBySource(ctx, i.sourceName)
	if err != nil {
		return fmt.Errorf("list okta discovery auto-bind candidates: %w", err)
	}
	if len(rows) == 0 {
		return nil
	}

	connectorSourceByKind := map[string]string{}

	githubConfigRow, err := deps.Q.GetConnectorConfig(ctx, configstore.KindGitHub)
	if err == nil && githubConfigRow.Enabled {
		githubCfg, decodeErr := configstore.DecodeGitHubConfig(githubConfigRow.Config)
		if decodeErr == nil {
			githubCfg = githubCfg.Normalized()
			if githubCfg.Validate() == nil && strings.TrimSpace(githubCfg.Org) != "" {
				connectorSourceByKind[configstore.KindGitHub] = githubCfg.Org
			}
		}
	}

	datadogConfigRow, err := deps.Q.GetConnectorConfig(ctx, configstore.KindDatadog)
	if err == nil && datadogConfigRow.Enabled {
		datadogCfg, decodeErr := configstore.DecodeDatadogConfig(datadogConfigRow.Config)
		if decodeErr == nil {
			datadogCfg = datadogCfg.Normalized()
			if datadogCfg.Validate() == nil && strings.TrimSpace(datadogCfg.Site) != "" {
				connectorSourceByKind[configstore.KindDatadog] = datadogCfg.Site
			}
		}
	}

	boundCount := 0
	for _, row := range rows {
		connectorKind := strings.ToLower(strings.TrimSpace(row.IntegrationKind))
		connectorSource := strings.TrimSpace(connectorSourceByKind[connectorKind])
		if connectorSource == "" {
			continue
		}
		if err := deps.Q.UpsertSaaSAppBinding(ctx, gen.UpsertSaaSAppBindingParams{
			SaasAppID:           row.SaasAppID,
			ConnectorKind:       connectorKind,
			ConnectorSourceName: connectorSource,
			BindingSource:       "auto",
			Confidence:          0.8,
			IsPrimary:           false,
			CreatedByAuthUserID: pgtype.Int8{},
		}); err != nil {
			return fmt.Errorf("upsert okta auto binding for app %d: %w", row.SaasAppID, err)
		}
		boundCount++
	}

	if boundCount > 0 {
		if _, err := deps.Q.RecomputePrimarySaaSAppBindingsForAll(ctx); err != nil {
			return fmt.Errorf("recompute primary bindings: %w", err)
		}
	}
	return nil
}
