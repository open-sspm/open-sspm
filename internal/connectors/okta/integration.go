package okta

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/opensspm"
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

	userIDs, err := i.syncOktaIdpUsers(ctx, deps, runID, users)
	if err != nil {
		deps.Report(registry.Event{Source: "okta", Stage: "sync-users", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
		return err
	}

	if err := i.syncOktaGroups(ctx, deps, runID, userIDs); err != nil {
		deps.Report(registry.Event{Source: "okta", Stage: "sync-groups", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
		return err
	}

	appIDs, err := i.syncOktaAppAssignments(ctx, deps, runID, userIDs)
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
		Q: deps.Q,
		Datasets: opensspm.RuntimeDatasetProviderAdapter{
			Provider:      router,
			CapabilitiesV: datasets.RuntimeCapabilities(router),
		},
		Now: time.Now,
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

type oktaAppIDMap struct {
	mu   sync.Mutex
	data map[string]int64
}

type oktaUserIDMap struct {
	mu   sync.Mutex
	data map[string]int64
}

func (i *OktaIntegration) syncOktaIdpUsers(ctx context.Context, deps registry.IntegrationDeps, runID int64, users []User) (map[string]int64, error) {
	userIDs := &oktaUserIDMap{data: make(map[string]int64, len(users))}

	if len(users) == 0 {
		deps.Report(registry.Event{Source: "okta", Stage: "sync-users", Current: 0, Total: 0, Message: "no users to sync"})
		return userIDs.data, nil
	}

	workers := i.workers
	if len(users) < workers {
		workers = len(users)
	}
	if workers < 1 {
		workers = 1
	}

	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup
	jobs := make(chan User, len(users))
	var done int64

	worker := func() {
		defer wg.Done()
		for user := range jobs {
			if jobCtx.Err() != nil {
				return
			}
			if err := i.syncOktaIdpUser(jobCtx, deps, runID, user, userIDs); err != nil {
				errOnce.Do(func() {
					firstErr = err
					cancel()
				})
				return
			}
			n := atomic.AddInt64(&done, 1)
			deps.Report(registry.Event{
				Source:  "okta",
				Stage:   "sync-users",
				Current: n,
				Total:   int64(len(users)),
				Message: fmt.Sprintf("users %d/%d", n, len(users)),
			})
		}
	}

	for j := 0; j < workers; j++ {
		wg.Add(1)
		go worker()
	}

	for _, user := range users {
		jobs <- user
	}
	close(jobs)
	wg.Wait()

	return userIDs.data, firstErr
}

func (i *OktaIntegration) syncOktaIdpUser(ctx context.Context, deps registry.IntegrationDeps, runID int64, user User, userIDs *oktaUserIDMap) error {
	idpUser, err := deps.Q.UpsertIdPUser(ctx, gen.UpsertIdPUserParams{
		ExternalID:      user.ID,
		Email:           user.Email,
		DisplayName:     user.DisplayName,
		Status:          user.Status,
		RawJson:         user.RawJSON,
		LastLoginAt:     registry.PgTimestamptzPtr(user.LastLoginAt),
		LastLoginIp:     "",
		LastLoginRegion: "",
		SeenInRunID:     registry.PgInt8(runID),
	})
	if err != nil {
		return fmt.Errorf("upsert idp user %s: %w", user.ID, err)
	}

	userIDs.mu.Lock()
	userIDs.data[user.ID] = idpUser.ID
	userIDs.mu.Unlock()
	return nil
}

func (i *OktaIntegration) syncOktaGroups(ctx context.Context, deps registry.IntegrationDeps, runID int64, userIDs map[string]int64) error {
	groups, err := i.client.ListGroups(ctx)
	if err != nil {
		return fmt.Errorf("okta list groups: %w", err)
	}
	deps.Report(registry.Event{Source: "okta", Stage: "sync-groups", Current: 0, Total: int64(len(groups)), Message: fmt.Sprintf("syncing %d groups", len(groups))})

	if len(groups) == 0 {
		return nil
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
			groupRaw := registry.NormalizeJSON(group.RawJSON)
			dbGroup, err := deps.Q.UpsertOktaGroup(jobCtx, gen.UpsertOktaGroupParams{
				ExternalID:  group.ID,
				Name:        group.Name,
				Type:        group.Type,
				RawJson:     groupRaw,
				SeenInRunID: registry.PgInt8(runID),
			})
			if err != nil {
				errOnce.Do(func() {
					firstErr = fmt.Errorf("upsert okta group %s: %w", group.ID, err)
					cancel()
				})
				return
			}
			for _, userExternalID := range userExternalIDs {
				idpUserID, ok := userIDs[userExternalID]
				if !ok {
					continue
				}
				if err := deps.Q.InsertOktaUserGroup(jobCtx, gen.InsertOktaUserGroupParams{
					IdpUserID:   idpUserID,
					OktaGroupID: dbGroup.ID,
					SeenInRunID: registry.PgInt8(runID),
				}); err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("insert okta user group idp_user_id=%d okta_group_id=%d: %w", idpUserID, dbGroup.ID, err)
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

func (i *OktaIntegration) syncOktaAppAssignments(ctx context.Context, deps registry.IntegrationDeps, runID int64, userIDs map[string]int64) (map[string]int64, error) {
	apps, err := i.client.ListApps(ctx)
	if err != nil {
		return nil, fmt.Errorf("okta list apps: %w", err)
	}
	deps.Report(registry.Event{Source: "okta", Stage: "sync-app-assignments", Current: 0, Total: int64(len(apps)), Message: fmt.Sprintf("syncing %d apps", len(apps))})

	appIDs := &oktaAppIDMap{data: make(map[string]int64)}
	if len(apps) == 0 {
		return appIDs.data, nil
	}

	workers := i.workers
	if len(apps) < workers {
		workers = len(apps)
	}
	if workers < 1 {
		workers = 1
	}

	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup
	jobs := make(chan App, len(apps))
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
					Total:   int64(len(apps)),
					Message: fmt.Sprintf("apps %d/%d", n, len(apps)),
				})
				continue
			}

			appRow, err := deps.Q.UpsertOktaApp(jobCtx, gen.UpsertOktaAppParams{
				ExternalID:  app.ID,
				Label:       app.Label,
				Name:        app.Name,
				Status:      app.Status,
				SignOnMode:  app.SignOnMode,
				RawJson:     registry.NormalizeJSON(app.RawJSON),
				SeenInRunID: registry.PgInt8(runID),
			})
			if err != nil {
				errOnce.Do(func() {
					firstErr = fmt.Errorf("upsert okta app %s: %w", app.ID, err)
					cancel()
				})
				return
			}

			for _, assignment := range assignments {
				idpUserID, ok := userIDs[assignment.UserID]
				if !ok {
					continue
				}
				if err := deps.Q.InsertOktaUserAppAssignment(jobCtx, gen.InsertOktaUserAppAssignmentParams{
					IdpUserID:   idpUserID,
					OktaAppID:   appRow.ID,
					Scope:       assignment.Scope,
					ProfileJson: registry.NormalizeJSON(assignment.ProfileJSON),
					RawJson:     registry.NormalizeJSON(assignment.RawJSON),
					SeenInRunID: registry.PgInt8(runID),
				}); err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("insert okta user app assignment idp_user_id=%d okta_app_id=%d: %w", idpUserID, appRow.ID, err)
						cancel()
					})
					return
				}
			}

			appIDs.mu.Lock()
			appIDs.data[app.ID] = appRow.ID
			appIDs.mu.Unlock()

			n := atomic.AddInt64(&done, 1)
			deps.Report(registry.Event{
				Source:  "okta",
				Stage:   "sync-app-assignments",
				Current: n,
				Total:   int64(len(apps)),
				Message: fmt.Sprintf("apps %d/%d", n, len(apps)),
			})
		}
	}

	for j := 0; j < workers; j++ {
		wg.Add(1)
		go worker()
	}

	for _, app := range apps {
		if strings.TrimSpace(app.ID) == "" {
			continue
		}
		jobs <- app
	}
	close(jobs)
	wg.Wait()

	return appIDs.data, firstErr
}

func (i *OktaIntegration) syncOktaAppGroupAssignments(ctx context.Context, deps registry.IntegrationDeps, runID int64, appIDs map[string]int64) error {
	if len(appIDs) == 0 {
		deps.Report(registry.Event{Source: "okta", Stage: "sync-app-group-assignments", Current: 0, Total: 0, Message: "no apps to sync"})
		return nil
	}
	deps.Report(registry.Event{Source: "okta", Stage: "sync-app-group-assignments", Current: 0, Total: int64(len(appIDs)), Message: fmt.Sprintf("syncing %d apps", len(appIDs))})

	type appJob struct {
		externalID string
		internalID int64
	}

	workers := i.workers
	if len(appIDs) < workers {
		workers = len(appIDs)
	}
	if workers < 1 {
		workers = 1
	}

	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup
	jobs := make(chan appJob, len(appIDs))
	var done int64

	worker := func() {
		defer wg.Done()
		for job := range jobs {
			if jobCtx.Err() != nil {
				return
			}
			assignments, err := i.client.ListApplicationGroupAssignments(jobCtx, job.externalID)
			if err != nil {
				errOnce.Do(func() {
					firstErr = fmt.Errorf("okta app %s group assignments: %w", job.externalID, err)
					cancel()
				})
				return
			}
			for _, assignment := range assignments {
				group := assignment.Group
				if group.ID == "" {
					continue
				}
				groupRow, err := deps.Q.UpsertOktaGroup(jobCtx, gen.UpsertOktaGroupParams{
					ExternalID:  group.ID,
					Name:        group.Name,
					Type:        group.Type,
					RawJson:     registry.NormalizeJSON(group.RawJSON),
					SeenInRunID: registry.PgInt8(runID),
				})
				if err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("upsert okta group %s for app %s: %w", group.ID, job.externalID, err)
						cancel()
					})
					return
				}
				if err := deps.Q.InsertOktaAppGroupAssignment(jobCtx, gen.InsertOktaAppGroupAssignmentParams{
					OktaAppID:   job.internalID,
					OktaGroupID: groupRow.ID,
					Priority:    assignment.Priority,
					ProfileJson: registry.NormalizeJSON(assignment.ProfileJSON),
					RawJson:     registry.NormalizeJSON(assignment.RawJSON),
					SeenInRunID: registry.PgInt8(runID),
				}); err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("insert okta app group assignment okta_app_id=%d okta_group_id=%d: %w", job.internalID, groupRow.ID, err)
						cancel()
					})
					return
				}
			}
			n := atomic.AddInt64(&done, 1)
			deps.Report(registry.Event{
				Source:  "okta",
				Stage:   "sync-app-group-assignments",
				Current: n,
				Total:   int64(len(appIDs)),
				Message: fmt.Sprintf("apps %d/%d", n, len(appIDs)),
			})
		}
	}

	for j := 0; j < workers; j++ {
		wg.Add(1)
		go worker()
	}

	for externalID, internalID := range appIDs {
		jobs <- appJob{externalID: externalID, internalID: internalID}
	}
	close(jobs)
	wg.Wait()

	return firstErr
}
