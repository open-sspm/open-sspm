package github

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

type GitHubIntegration struct {
	client     *Client
	org        string
	enterprise string
	workers    int
	scim       bool
}

func NewGitHubIntegration(client *Client, org string, enterprise string, workers int, scimEnabled bool) *GitHubIntegration {
	if workers < 1 {
		workers = 6
	}
	return &GitHubIntegration{
		client:     client,
		org:        strings.TrimSpace(org),
		enterprise: strings.TrimSpace(enterprise),
		workers:    workers,
		scim:       scimEnabled,
	}
}

func (i *GitHubIntegration) Kind() string { return "github" }
func (i *GitHubIntegration) Name() string { return i.org }
func (i *GitHubIntegration) Role() registry.IntegrationRole {
	return registry.RoleApp
}

func (i *GitHubIntegration) InitEvents() []registry.Event {
	return []registry.Event{
		{Source: "github", Stage: "list-members", Current: 0, Total: 1, Message: "listing org members"},
		{Source: "github", Stage: "resolve-emails", Current: 0, Total: 1, Message: "resolving member emails"},
		{Source: "github", Stage: "list-teams", Current: 0, Total: 1, Message: "listing teams"},
		{Source: "github", Stage: "fetch-team-data", Current: 0, Total: registry.UnknownTotal, Message: "fetching team members/repos"},
		{Source: "github", Stage: "write-members", Current: 0, Total: registry.UnknownTotal, Message: "writing members"},
	}
}

func (i *GitHubIntegration) Run(ctx context.Context, deps registry.IntegrationDeps) error {
	started := time.Now()
	slog.Info("syncing GitHub", "org", i.org)

	runID, err := deps.Q.CreateSyncRun(ctx, gen.CreateSyncRunParams{
		SourceKind: "github",
		SourceName: i.org,
	})
	if err != nil {
		return err
	}

	members, err := i.client.ListOrgMembers(ctx, i.org)
	if err != nil {
		deps.Report(registry.Event{Source: "github", Stage: "list-members", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindAPI)
		return err
	}

	type externalIdentity struct {
		nameID       string
		scimUserName string
		scimEmail    string
		userEmail    string
	}
	externalByLogin := make(map[string]externalIdentity)

	samlIdentities, err := i.client.ListOrgSAMLExternalIdentities(ctx, i.org)
	if err != nil {
		slog.Warn("github saml external identities lookup failed", "org", i.org, "err", err)
		deps.Report(registry.Event{Source: "github", Stage: "resolve-emails", Message: fmt.Sprintf("saml identity lookup failed: %v", err), Err: err})
		if errors.Is(err, ErrNoSAMLIdentityProvider) && strings.TrimSpace(i.enterprise) != "" {
			slog.Info("github trying enterprise external identities", "enterprise", i.enterprise, "org", i.org)
			samlIdentities, err = i.client.ListEnterpriseSAMLExternalIdentities(ctx, i.enterprise)
			if err != nil {
				slog.Warn("github enterprise external identities lookup failed", "enterprise", i.enterprise, "err", err)
				deps.Report(registry.Event{Source: "github", Stage: "resolve-emails", Message: fmt.Sprintf("enterprise saml identity lookup failed: %v", err), Err: err})
			} else {
				for _, identity := range samlIdentities {
					login := strings.ToLower(strings.TrimSpace(identity.Login))
					nameID := strings.TrimSpace(identity.NameID)
					scimUserName := strings.TrimSpace(identity.SCIMUserName)
					scimEmail := strings.TrimSpace(identity.SCIMEmail)
					userEmail := strings.TrimSpace(identity.UserEmail)
					if login == "" || (nameID == "" && scimUserName == "" && scimEmail == "" && userEmail == "") {
						continue
					}
					externalByLogin[login] = externalIdentity{nameID: nameID, scimUserName: scimUserName, scimEmail: scimEmail, userEmail: userEmail}
				}
			}
		}
	} else {
		for _, identity := range samlIdentities {
			login := strings.ToLower(strings.TrimSpace(identity.Login))
			nameID := strings.TrimSpace(identity.NameID)
			scimUserName := strings.TrimSpace(identity.SCIMUserName)
			scimEmail := strings.TrimSpace(identity.SCIMEmail)
			userEmail := strings.TrimSpace(identity.UserEmail)
			if login == "" || (nameID == "" && scimUserName == "" && scimEmail == "" && userEmail == "") {
				continue
			}
			externalByLogin[login] = externalIdentity{nameID: nameID, scimUserName: scimUserName, scimEmail: scimEmail, userEmail: userEmail}
		}
	}

	scimByUserName := make(map[string]SCIMUser)
	scimByLogin := make(map[string]SCIMUser)
	if i.scim {
		scimUsers, err := i.client.ListOrgSCIMUsers(ctx, i.org)
		if err != nil {
			slog.Warn("github scim users lookup failed", "org", i.org, "err", err)
			deps.Report(registry.Event{Source: "github", Stage: "resolve-emails", Message: fmt.Sprintf("scim user lookup failed: %v", err), Err: err})
		} else {
			for _, u := range scimUsers {
				if v := strings.ToLower(strings.TrimSpace(u.UserName)); v != "" {
					scimByUserName[v] = u
				}
				if v := strings.ToLower(strings.TrimSpace(u.GitHubLogin)); v != "" {
					scimByLogin[v] = u
				}
			}
			if len(scimUsers) == 0 {
				slog.Warn("github scim enabled but returned 0 users", "org", i.org)
			}
		}
	}

	resolveEmail := func(login string) string {
		loginKey := strings.ToLower(strings.TrimSpace(login))
		if loginKey == "" {
			return ""
		}

		identity := externalByLogin[loginKey]
		nameID := strings.TrimSpace(identity.nameID)
		scimUserName := strings.TrimSpace(identity.scimUserName)
		scimEmail := strings.TrimSpace(identity.scimEmail)
		userEmail := strings.TrimSpace(identity.userEmail)

		if i.scim {
			if u, ok := scimByLogin[loginKey]; ok {
				return u.PreferredEmail()
			}
			if u, ok := scimByUserName[loginKey]; ok {
				return u.PreferredEmail()
			}
			if scimUserName != "" {
				if u, ok := scimByUserName[strings.ToLower(scimUserName)]; ok {
					return u.PreferredEmail()
				}
			}
			if nameID != "" {
				if u, ok := scimByUserName[strings.ToLower(nameID)]; ok {
					return u.PreferredEmail()
				}
			}
		}

		if scimUserName != "" && strings.Contains(scimUserName, "@") {
			return scimUserName
		}
		if scimEmail != "" && strings.Contains(scimEmail, "@") {
			return scimEmail
		}
		if userEmail != "" && strings.Contains(userEmail, "@") {
			return userEmail
		}
		if nameID != "" && strings.Contains(nameID, "@") {
			return nameID
		}
		return ""
	}

	resolvedEmails := 0
	for idx := range members {
		if email := strings.TrimSpace(resolveEmail(members[idx].Login)); email != "" {
			members[idx].Email = email
			resolvedEmails++
		}
	}
	emailsWithValue := 0
	for _, member := range members {
		if strings.TrimSpace(member.Email) != "" {
			emailsWithValue++
		}
	}
	deps.Report(registry.Event{Source: "github", Stage: "resolve-emails", Current: 1, Total: 1, Message: fmt.Sprintf("resolved %d/%d member emails via SAML/SCIM (total_with_email=%d scim=%t external_identities=%d scim_users=%d)", resolvedEmails, len(members), emailsWithValue, i.scim, len(externalByLogin), len(scimByUserName))})
	slog.Info("github resolved member emails via SAML/SCIM",
		"resolved", resolvedEmails,
		"total_members", len(members),
		"total_with_email", emailsWithValue,
		"scim", i.scim,
		"external_identities", len(externalByLogin),
		"scim_users", len(scimByUserName),
	)

	deps.Report(registry.Event{Source: "github", Stage: "list-members", Current: 1, Total: 1, Message: fmt.Sprintf("found %d members (%d with email)", len(members), emailsWithValue)})
	deps.Report(registry.Event{Source: "github", Stage: "write-members", Current: 0, Total: int64(len(members)), Message: fmt.Sprintf("writing %d members", len(members))})

	teams, err := i.client.ListTeams(ctx, i.org)
	if err != nil {
		deps.Report(registry.Event{Source: "github", Stage: "list-teams", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindAPI)
		return err
	}
	deps.Report(registry.Event{Source: "github", Stage: "list-teams", Current: 1, Total: 1, Message: fmt.Sprintf("found %d teams", len(teams))})
	deps.Report(registry.Event{Source: "github", Stage: "fetch-team-data", Current: 0, Total: int64(len(teams)), Message: fmt.Sprintf("fetching %d teams", len(teams))})

	teamMembers := make(map[string][]string)
	teamRepos := make(map[string][]TeamRepo)
	if len(teams) > 0 {
		type teamResult struct {
			slug    string
			members []TeamMember
			repos   []TeamRepo
			err     error
		}

		teamCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		jobs := make(chan Team, len(teams))
		results := make(chan teamResult, len(teams))
		var teamsDone int64

		workers := i.workers
		if len(teams) < workers {
			workers = len(teams)
		}
		if workers < 1 {
			workers = 1
		}

		var wg gosync.WaitGroup
		for j := 0; j < workers; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for team := range jobs {
					if teamCtx.Err() != nil {
						return
					}
					members, err := i.client.ListTeamMembers(teamCtx, i.org, team.Slug)
					if err != nil {
						results <- teamResult{slug: team.Slug, err: fmt.Errorf("github team %s members: %w", team.Slug, err)}
						cancel()
						continue
					}
					repos, err := i.client.ListTeamRepos(teamCtx, i.org, team.Slug)
					if err != nil {
						results <- teamResult{slug: team.Slug, err: fmt.Errorf("github team %s repos: %w", team.Slug, err)}
						cancel()
						continue
					}
					n := atomic.AddInt64(&teamsDone, 1)
					deps.Report(registry.Event{
						Source:  "github",
						Stage:   "fetch-team-data",
						Current: n,
						Total:   int64(len(teams)),
						Message: fmt.Sprintf("teams %d/%d", n, len(teams)),
					})
					results <- teamResult{slug: team.Slug, members: members, repos: repos}
				}
			}()
		}

		for _, team := range teams {
			jobs <- team
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
			for _, m := range res.members {
				teamMembers[m.Login] = append(teamMembers[m.Login], res.slug)
			}
			teamRepos[res.slug] = res.repos
		}

		if firstNonCancelErr != nil {
			firstErr = firstNonCancelErr
		}
		if firstErr != nil {
			deps.Report(registry.Event{Source: "github", Stage: "fetch-team-data", Message: firstErr.Error(), Err: firstErr})
			registry.FailSyncRun(ctx, deps.Q, runID, firstErr, registry.SyncErrorKindAPI)
			return firstErr
		}
	}

	deps.Report(registry.Event{Source: "github", Stage: "write-members", Current: 0, Total: int64(len(members)), Message: fmt.Sprintf("writing %d members", len(members))})

	const userBatchSize = 1000
	externalIDs := make([]string, 0, len(members))
	emails := make([]string, 0, len(members))
	displayNames := make([]string, 0, len(members))
	rawJSONs := make([][]byte, 0, len(members))
	lastLoginAts := make([]pgtype.Timestamptz, 0, len(members))
	lastLoginIps := make([]string, 0, len(members))
	lastLoginRegions := make([]string, 0, len(members))

	for _, member := range members {
		externalID := strings.TrimSpace(member.Login)
		if externalID == "" {
			continue
		}
		display := strings.TrimSpace(member.DisplayName)
		if display == "" {
			display = externalID
		}
		externalIDs = append(externalIDs, externalID)
		emails = append(emails, matching.NormalizeEmail(member.Email))
		displayNames = append(displayNames, display)
		rawJSONs = append(rawJSONs, member.RawJSON)
		lastLoginAts = append(lastLoginAts, pgtype.Timestamptz{})
		lastLoginIps = append(lastLoginIps, "")
		lastLoginRegions = append(lastLoginRegions, "")
	}

	for start := 0; start < len(externalIDs); start += userBatchSize {
		end := start + userBatchSize
		if end > len(externalIDs) {
			end = len(externalIDs)
		}
		_, err := deps.Q.UpsertAppUsersBulkBySource(ctx, gen.UpsertAppUsersBulkBySourceParams{
			SourceKind:       "github",
			SourceName:       i.org,
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
			deps.Report(registry.Event{Source: "github", Stage: "write-members", Message: err.Error(), Err: err})
			registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
			return err
		}
		deps.Report(registry.Event{
			Source:  "github",
			Stage:   "write-members",
			Current: int64(end),
			Total:   int64(len(externalIDs)),
			Message: fmt.Sprintf("members %d/%d", end, len(externalIDs)),
		})
	}

	const entitlementBatchSize = 5000
	entAppUserExternalIDs := make([]string, 0, len(members))
	entKinds := make([]string, 0, len(members))
	entResources := make([]string, 0, len(members))
	entPermissions := make([]string, 0, len(members))
	entRawJSONs := make([][]byte, 0, len(members))

	for _, member := range members {
		login := strings.TrimSpace(member.Login)
		if login == "" {
			continue
		}
		orgEntJSON := registry.MarshalJSON(map[string]string{"org": i.org, "role": member.Role})
		entAppUserExternalIDs = append(entAppUserExternalIDs, login)
		entKinds = append(entKinds, "github_org_role")
		entResources = append(entResources, "github_org:"+i.org)
		entPermissions = append(entPermissions, member.Role)
		entRawJSONs = append(entRawJSONs, orgEntJSON)

		for _, teamSlug := range teamMembers[login] {
			teamSlug = strings.TrimSpace(teamSlug)
			if teamSlug == "" {
				continue
			}
			teamJSON := registry.MarshalJSON(map[string]string{"team": teamSlug})
			entAppUserExternalIDs = append(entAppUserExternalIDs, login)
			entKinds = append(entKinds, "github_team_member")
			entResources = append(entResources, "github_team:"+i.org+"/"+teamSlug)
			entPermissions = append(entPermissions, "member")
			entRawJSONs = append(entRawJSONs, teamJSON)

			for _, repo := range teamRepos[teamSlug] {
				repoFullName := strings.TrimSpace(repo.FullName)
				if repoFullName == "" {
					continue
				}
				repoJSON := registry.MarshalJSON(map[string]string{
					"team":       teamSlug,
					"repo":       repoFullName,
					"permission": repo.Permission,
				})
				entAppUserExternalIDs = append(entAppUserExternalIDs, login)
				entKinds = append(entKinds, "github_team_repo_permission")
				entResources = append(entResources, "github_repo:"+repoFullName)
				entPermissions = append(entPermissions, repo.Permission)
				entRawJSONs = append(entRawJSONs, repoJSON)
			}
		}
	}

	for start := 0; start < len(entAppUserExternalIDs); start += entitlementBatchSize {
		end := start + entitlementBatchSize
		if end > len(entAppUserExternalIDs) {
			end = len(entAppUserExternalIDs)
		}
		_, err := deps.Q.UpsertEntitlementsBulkBySource(ctx, gen.UpsertEntitlementsBulkBySourceParams{
			SeenInRunID:        runID,
			SourceKind:         "github",
			SourceName:         i.org,
			AppUserExternalIds: entAppUserExternalIDs[start:end],
			Kinds:              entKinds[start:end],
			Resources:          entResources[start:end],
			Permissions:        entPermissions[start:end],
			RawJsons:           entRawJSONs[start:end],
		})
		if err != nil {
			deps.Report(registry.Event{Source: "github", Stage: "write-members", Message: err.Error(), Err: err})
			registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
			return err
		}
	}

	if err := registry.FinalizeAppRun(ctx, deps, runID, "github", i.org, time.Since(started)); err != nil {
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
		return err
	}
	slog.Info("github sync complete", "org", i.org, "members", len(members))
	return nil
}
