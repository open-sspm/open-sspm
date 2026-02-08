package github

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strconv"
	"strings"
	gosync "sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/matching"
)

const (
	githubAppAssetBatchSize   = 1000
	githubOwnerBatchSize      = 2000
	githubCredentialBatchSize = 2000
	githubAuditEventBatchSize = 2000
)

type GitHubIntegration struct {
	client     *Client
	org        string
	enterprise string
	workers    int
	scim       bool
}

type githubAppAssetUpsertRow struct {
	AssetKind        string
	ExternalID       string
	ParentExternalID string
	DisplayName      string
	Status           string
	CreatedAtSource  pgtype.Timestamptz
	UpdatedAtSource  pgtype.Timestamptz
	RawJSON          []byte
}

type githubAppAssetOwnerUpsertRow struct {
	AssetKind        string
	AssetExternalID  string
	OwnerKind        string
	OwnerExternalID  string
	OwnerDisplayName string
	OwnerEmail       string
	RawJSON          []byte
}

type githubCredentialArtifactUpsertRow struct {
	AssetRefKind          string
	AssetRefExternalID    string
	CredentialKind        string
	ExternalID            string
	DisplayName           string
	Fingerprint           string
	ScopeJSON             []byte
	Status                string
	CreatedAtSource       pgtype.Timestamptz
	ExpiresAtSource       pgtype.Timestamptz
	LastUsedAtSource      pgtype.Timestamptz
	CreatedByKind         string
	CreatedByExternalID   string
	CreatedByDisplayName  string
	ApprovedByKind        string
	ApprovedByExternalID  string
	ApprovedByDisplayName string
	RawJSON               []byte
}

type githubCredentialAuditEventUpsertRow struct {
	EventExternalID      string
	EventType            string
	EventTime            pgtype.Timestamptz
	ActorKind            string
	ActorExternalID      string
	ActorDisplayName     string
	TargetKind           string
	TargetExternalID     string
	TargetDisplayName    string
	CredentialKind       string
	CredentialExternalID string
	RawJSON              []byte
}

type githubProgrammaticSyncSummary struct {
	AppAssets   int
	Owners      int
	Credentials int
	AuditEvents int
}

type programmaticSyncError struct {
	kind string
	err  error
}

func (e *programmaticSyncError) Error() string {
	if e == nil || e.err == nil {
		return "programmatic sync failed"
	}
	return e.err.Error()
}

func (e *programmaticSyncError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
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
		{Source: "github", Stage: "list-programmatic-assets", Current: 0, Total: 1, Message: "listing GitHub repositories"},
		{Source: "github", Stage: "list-deploy-keys", Current: 0, Total: registry.UnknownTotal, Message: "listing repository deploy keys"},
		{Source: "github", Stage: "list-pat-governance", Current: 0, Total: 2, Message: "listing PAT governance datasets"},
		{Source: "github", Stage: "list-installations", Current: 0, Total: 1, Message: "listing GitHub app installations"},
		{Source: "github", Stage: "list-audit-events", Current: 0, Total: 1, Message: "listing GitHub audit events"},
		{Source: "github", Stage: "write-programmatic-assets", Current: 0, Total: registry.UnknownTotal, Message: "writing programmatic app assets"},
		{Source: "github", Stage: "write-programmatic-owners", Current: 0, Total: registry.UnknownTotal, Message: "writing programmatic asset owners"},
		{Source: "github", Stage: "write-programmatic-credentials", Current: 0, Total: registry.UnknownTotal, Message: "writing programmatic credential metadata"},
		{Source: "github", Stage: "write-programmatic-audit-events", Current: 0, Total: registry.UnknownTotal, Message: "writing programmatic credential audit events"},
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

	programmaticSummary, err := i.syncProgrammaticAccess(ctx, deps, runID)
	if err != nil {
		errorKind := registry.SyncErrorKindUnknown
		var programmaticErr *programmaticSyncError
		if errors.As(err, &programmaticErr) && strings.TrimSpace(programmaticErr.kind) != "" {
			errorKind = strings.TrimSpace(programmaticErr.kind)
		}
		registry.FailSyncRun(ctx, deps.Q, runID, err, errorKind)
		return err
	}

	if err := registry.FinalizeAppRun(ctx, deps, runID, "github", i.org, time.Since(started), false); err != nil {
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
		return err
	}
	slog.Info(
		"github sync complete",
		"org", i.org,
		"members", len(members),
		"programmatic_app_assets", programmaticSummary.AppAssets,
		"programmatic_owners", programmaticSummary.Owners,
		"programmatic_credentials", programmaticSummary.Credentials,
		"programmatic_audit_events", programmaticSummary.AuditEvents,
	)
	return nil
}

func (i *GitHubIntegration) syncProgrammaticAccess(ctx context.Context, deps registry.IntegrationDeps, runID int64) (githubProgrammaticSyncSummary, error) {
	summary := githubProgrammaticSyncSummary{}

	deps.Report(registry.Event{Source: "github", Stage: "list-programmatic-assets", Current: 0, Total: 1, Message: "listing repositories for deploy-key governance"})
	repositories, err := i.client.ListOrgRepos(ctx, i.org)
	if err != nil {
		wrapped := fmt.Errorf("github repository listing failed: %w", err)
		deps.Report(registry.Event{Source: "github", Stage: "list-programmatic-assets", Message: wrapped.Error(), Err: err})
		return summary, &programmaticSyncError{kind: registry.SyncErrorKindAPI, err: wrapped}
	} else {
		deps.Report(registry.Event{
			Source:  "github",
			Stage:   "list-programmatic-assets",
			Current: 1,
			Total:   1,
			Message: fmt.Sprintf("found %d repositories", len(repositories)),
		})
	}

	credentialRows := make([]githubCredentialArtifactUpsertRow, 0)
	if len(repositories) > 0 {
		deps.Report(registry.Event{Source: "github", Stage: "list-deploy-keys", Current: 0, Total: int64(len(repositories)), Message: fmt.Sprintf("listing deploy keys for %d repositories", len(repositories))})
		for idx, repo := range repositories {
			repoName := strings.TrimSpace(repo.Name)
			if repoName == "" {
				repoName = strings.TrimSpace(strings.TrimPrefix(repo.FullName, i.org+"/"))
			}
			if repoName == "" {
				deps.Report(registry.Event{Source: "github", Stage: "list-deploy-keys", Current: int64(idx + 1), Total: int64(len(repositories)), Message: fmt.Sprintf("skipping repository %q (missing name)", repo.FullName)})
				continue
			}

			keys, err := i.client.ListRepoDeployKeys(ctx, i.org, repoName)
			if err != nil {
				wrapped := fmt.Errorf("github deploy key lookup failed for %s: %w", repoName, err)
				deps.Report(registry.Event{Source: "github", Stage: "list-deploy-keys", Message: wrapped.Error(), Err: err})
				return summary, &programmaticSyncError{kind: registry.SyncErrorKindAPI, err: wrapped}
			}
			credentialRows = append(credentialRows, buildGitHubDeployKeyCredentialRows(i.org, repo, keys)...)
			deps.Report(registry.Event{Source: "github", Stage: "list-deploy-keys", Current: int64(idx + 1), Total: int64(len(repositories)), Message: fmt.Sprintf("deploy keys %d/%d", idx+1, len(repositories))})
		}
	}

	deps.Report(registry.Event{Source: "github", Stage: "list-pat-governance", Current: 0, Total: 2, Message: "listing fine-grained PAT requests"})
	patRequests, err := i.client.ListOrgPersonalAccessTokenRequests(ctx, i.org)
	if err != nil {
		if errors.Is(err, ErrDatasetUnavailable) {
			wrapped := fmt.Errorf("github pat request dataset unavailable; aborting sync to avoid expiring valid data: %w", err)
			deps.Report(registry.Event{Source: "github", Stage: "list-pat-governance", Message: wrapped.Error(), Err: err})
			return summary, &programmaticSyncError{kind: registry.SyncErrorKindAPI, err: wrapped}
		} else {
			wrapped := fmt.Errorf("github pat request listing failed: %w", err)
			deps.Report(registry.Event{Source: "github", Stage: "list-pat-governance", Message: wrapped.Error(), Err: err})
			return summary, &programmaticSyncError{kind: registry.SyncErrorKindAPI, err: wrapped}
		}
	} else {
		credentialRows = append(credentialRows, buildGitHubPATRequestCredentialRows(i.org, patRequests)...)
		deps.Report(registry.Event{Source: "github", Stage: "list-pat-governance", Current: 1, Total: 2, Message: fmt.Sprintf("found %d pat requests", len(patRequests))})
	}

	deps.Report(registry.Event{Source: "github", Stage: "list-pat-governance", Current: 1, Total: 2, Message: "listing approved fine-grained PATs"})
	pats, err := i.client.ListOrgPersonalAccessTokens(ctx, i.org)
	if err != nil {
		if errors.Is(err, ErrDatasetUnavailable) {
			wrapped := fmt.Errorf("github pat token dataset unavailable; aborting sync to avoid expiring valid data: %w", err)
			deps.Report(registry.Event{Source: "github", Stage: "list-pat-governance", Message: wrapped.Error(), Err: err})
			return summary, &programmaticSyncError{kind: registry.SyncErrorKindAPI, err: wrapped}
		} else {
			wrapped := fmt.Errorf("github pat token listing failed: %w", err)
			deps.Report(registry.Event{Source: "github", Stage: "list-pat-governance", Message: wrapped.Error(), Err: err})
			return summary, &programmaticSyncError{kind: registry.SyncErrorKindAPI, err: wrapped}
		}
	} else {
		credentialRows = append(credentialRows, buildGitHubPATCredentialRows(i.org, pats)...)
		deps.Report(registry.Event{Source: "github", Stage: "list-pat-governance", Current: 2, Total: 2, Message: fmt.Sprintf("found %d fine-grained pats", len(pats))})
	}

	deps.Report(registry.Event{Source: "github", Stage: "list-installations", Current: 0, Total: 1, Message: "listing GitHub app installations"})
	installationRows := make([]githubAppAssetUpsertRow, 0)
	installations, err := i.client.ListOrgInstallations(ctx, i.org)
	if err != nil {
		if errors.Is(err, ErrDatasetUnavailable) {
			wrapped := fmt.Errorf("github app installations dataset unavailable; aborting sync to avoid expiring valid data: %w", err)
			deps.Report(registry.Event{Source: "github", Stage: "list-installations", Message: wrapped.Error(), Err: err})
			return summary, &programmaticSyncError{kind: registry.SyncErrorKindAPI, err: wrapped}
		} else {
			wrapped := fmt.Errorf("github app installations listing failed: %w", err)
			deps.Report(registry.Event{Source: "github", Stage: "list-installations", Message: wrapped.Error(), Err: err})
			return summary, &programmaticSyncError{kind: registry.SyncErrorKindAPI, err: wrapped}
		}
	}
	deps.Report(registry.Event{Source: "github", Stage: "list-installations", Current: 1, Total: 1, Message: fmt.Sprintf("found %d installations", len(installations))})
	installationRows, ownerRows := buildGitHubInstallationRows(installations)

	deps.Report(registry.Event{Source: "github", Stage: "list-audit-events", Current: 0, Total: 1, Message: "listing org audit events"})
	var auditRows []githubCredentialAuditEventUpsertRow
	auditEvents, err := i.client.ListOrgAuditLog(ctx, i.org)
	if err != nil {
		if errors.Is(err, ErrDatasetUnavailable) {
			wrapped := fmt.Errorf("github audit log dataset unavailable; aborting sync to avoid expiring valid data: %w", err)
			deps.Report(registry.Event{Source: "github", Stage: "list-audit-events", Message: wrapped.Error(), Err: err})
			return summary, &programmaticSyncError{kind: registry.SyncErrorKindAPI, err: wrapped}
		} else {
			wrapped := fmt.Errorf("github audit log listing failed: %w", err)
			deps.Report(registry.Event{Source: "github", Stage: "list-audit-events", Message: wrapped.Error(), Err: err})
			return summary, &programmaticSyncError{kind: registry.SyncErrorKindAPI, err: wrapped}
		}
	} else {
		deps.Report(registry.Event{Source: "github", Stage: "list-audit-events", Current: 1, Total: 1, Message: fmt.Sprintf("found %d audit events", len(auditEvents))})
		auditRows = buildGitHubAuditEventRows(i.org, auditEvents)
	}

	if err := i.upsertProgrammaticAppAssets(ctx, deps, runID, installationRows); err != nil {
		deps.Report(registry.Event{Source: "github", Stage: "write-programmatic-assets", Message: err.Error(), Err: err})
		return summary, &programmaticSyncError{kind: registry.SyncErrorKindDB, err: fmt.Errorf("github app installation upsert failed: %w", err)}
	} else {
		summary.AppAssets = len(installationRows)
	}

	if err := i.upsertProgrammaticAssetOwners(ctx, deps, runID, ownerRows); err != nil {
		deps.Report(registry.Event{Source: "github", Stage: "write-programmatic-owners", Message: err.Error(), Err: err})
		return summary, &programmaticSyncError{kind: registry.SyncErrorKindDB, err: fmt.Errorf("github app installation owner upsert failed: %w", err)}
	} else {
		summary.Owners = len(ownerRows)
	}

	if err := i.upsertProgrammaticCredentials(ctx, deps, runID, credentialRows); err != nil {
		deps.Report(registry.Event{Source: "github", Stage: "write-programmatic-credentials", Message: err.Error(), Err: err})
		return summary, &programmaticSyncError{kind: registry.SyncErrorKindDB, err: fmt.Errorf("github credential metadata upsert failed: %w", err)}
	} else {
		summary.Credentials = len(credentialRows)
	}

	if err := i.upsertProgrammaticAuditEvents(ctx, deps, auditRows); err != nil {
		deps.Report(registry.Event{Source: "github", Stage: "write-programmatic-audit-events", Message: err.Error(), Err: err})
		return summary, &programmaticSyncError{kind: registry.SyncErrorKindDB, err: fmt.Errorf("github credential audit-event upsert failed: %w", err)}
	} else {
		summary.AuditEvents = len(auditRows)
	}

	return summary, nil
}

func buildGitHubInstallationRows(installations []AppInstallation) ([]githubAppAssetUpsertRow, []githubAppAssetOwnerUpsertRow) {
	assetRows := make([]githubAppAssetUpsertRow, 0, len(installations))
	ownerRows := make([]githubAppAssetOwnerUpsertRow, 0, len(installations))

	for _, installation := range installations {
		externalID := strconv.FormatInt(installation.ID, 10)
		if installation.ID <= 0 {
			continue
		}

		displayName := strings.TrimSpace(installation.AppName)
		if displayName == "" {
			displayName = strings.TrimSpace(installation.AppSlug)
		}
		if displayName == "" {
			displayName = externalID
		}
		if login := strings.TrimSpace(installation.AccountLogin); login != "" {
			displayName = fmt.Sprintf("%s (%s)", displayName, login)
		}

		parentExternalID := strings.TrimSpace(installation.AppSlug)
		if parentExternalID == "" && installation.AppID > 0 {
			parentExternalID = strconv.FormatInt(installation.AppID, 10)
		}

		status := "active"
		if strings.TrimSpace(installation.SuspendedAtRaw) != "" {
			status = "suspended"
		}

		assetRows = append(assetRows, githubAppAssetUpsertRow{
			AssetKind:        "github_app_installation",
			ExternalID:       externalID,
			ParentExternalID: parentExternalID,
			DisplayName:      displayName,
			Status:           status,
			CreatedAtSource:  parseGitHubTime(installation.CreatedAtRaw),
			UpdatedAtSource:  parseGitHubTime(installation.UpdatedAtRaw),
			RawJSON: registry.MarshalJSON(map[string]any{
				"id":                   installation.ID,
				"app_id":               installation.AppID,
				"app_slug":             strings.TrimSpace(installation.AppSlug),
				"app_name":             strings.TrimSpace(installation.AppName),
				"account_login":        strings.TrimSpace(installation.AccountLogin),
				"account_type":         strings.TrimSpace(installation.AccountType),
				"account_id":           installation.AccountID,
				"repository_selection": strings.TrimSpace(installation.RepositorySelection),
				"permissions":          installation.Permissions,
				"created_at":           strings.TrimSpace(installation.CreatedAtRaw),
				"updated_at":           strings.TrimSpace(installation.UpdatedAtRaw),
				"suspended_at":         strings.TrimSpace(installation.SuspendedAtRaw),
			}),
		})

		ownerExternalID := strings.TrimSpace(installation.AccountLogin)
		if ownerExternalID == "" && installation.AccountID > 0 {
			ownerExternalID = strconv.FormatInt(installation.AccountID, 10)
		}
		if ownerExternalID == "" {
			continue
		}
		ownerDisplayName := strings.TrimSpace(installation.AccountLogin)
		if ownerDisplayName == "" {
			ownerDisplayName = ownerExternalID
		}
		ownerRows = append(ownerRows, githubAppAssetOwnerUpsertRow{
			AssetKind:        "github_app_installation",
			AssetExternalID:  externalID,
			OwnerKind:        githubInstallationOwnerKind(installation.AccountType),
			OwnerExternalID:  ownerExternalID,
			OwnerDisplayName: ownerDisplayName,
			OwnerEmail:       "",
			RawJSON: registry.MarshalJSON(map[string]any{
				"account_login": strings.TrimSpace(installation.AccountLogin),
				"account_type":  strings.TrimSpace(installation.AccountType),
				"account_id":    installation.AccountID,
			}),
		})
	}

	return assetRows, ownerRows
}

func githubInstallationOwnerKind(accountType string) string {
	switch strings.ToLower(strings.TrimSpace(accountType)) {
	case "user":
		return "github_user"
	case "organization":
		return "github_org"
	default:
		return "unknown"
	}
}

func buildGitHubDeployKeyCredentialRows(org string, repo Repository, keys []DeployKey) []githubCredentialArtifactUpsertRow {
	rows := make([]githubCredentialArtifactUpsertRow, 0, len(keys))
	repository := strings.TrimSpace(repo.FullName)
	if repository == "" {
		repoName := strings.TrimSpace(repo.Name)
		if repoName != "" {
			repository = strings.TrimSpace(org) + "/" + repoName
		}
	}
	if repository == "" {
		return rows
	}

	for _, key := range keys {
		externalID := ""
		if key.ID > 0 {
			externalID = strconv.FormatInt(key.ID, 10)
		}
		if externalID == "" {
			externalID = syntheticGitHubAuditID(
				"deploy_key",
				repository,
				strings.TrimSpace(key.Title),
				strings.TrimSpace(key.Key),
				strings.TrimSpace(key.CreatedAtRaw),
			)
		}

		displayName := strings.TrimSpace(key.Title)
		if displayName == "" {
			displayName = externalID
		}
		fingerprint := githubDeployKeyFingerprint(key.Key)
		if fingerprint == "" {
			fingerprint = externalID
		}

		createdBy := strings.TrimSpace(key.AddedBy)
		createdByKind := ""
		if createdBy != "" {
			createdByKind = "github_user"
		}

		rows = append(rows, githubCredentialArtifactUpsertRow{
			AssetRefKind:       "repository",
			AssetRefExternalID: repository,
			CredentialKind:     "github_deploy_key",
			ExternalID:         externalID,
			DisplayName:        displayName,
			Fingerprint:        fingerprint,
			ScopeJSON: registry.MarshalJSON(map[string]any{
				"organization": repositoryOwner(repository),
				"repository":   repository,
				"read_only":    key.ReadOnly,
				"verified":     key.Verified,
			}),
			Status:               "active",
			CreatedAtSource:      parseGitHubTime(key.CreatedAtRaw),
			ExpiresAtSource:      pgtype.Timestamptz{},
			LastUsedAtSource:     parseGitHubTime(key.LastUsedAtRaw),
			CreatedByKind:        createdByKind,
			CreatedByExternalID:  createdBy,
			CreatedByDisplayName: createdBy,
			RawJSON: registry.MarshalJSON(map[string]any{
				"id":         key.ID,
				"title":      strings.TrimSpace(key.Title),
				"read_only":  key.ReadOnly,
				"verified":   key.Verified,
				"added_by":   strings.TrimSpace(key.AddedBy),
				"created_at": strings.TrimSpace(key.CreatedAtRaw),
				"last_used":  strings.TrimSpace(key.LastUsedAtRaw),
			}),
		})
	}

	return rows
}

func buildGitHubPATRequestCredentialRows(org string, requests []PersonalAccessTokenRequest) []githubCredentialArtifactUpsertRow {
	rows := make([]githubCredentialArtifactUpsertRow, 0, len(requests))
	organization := strings.TrimSpace(org)
	if organization == "" {
		return rows
	}

	for _, request := range requests {
		externalID := ""
		if request.ID > 0 {
			externalID = strconv.FormatInt(request.ID, 10)
		}
		if externalID == "" && request.TokenID > 0 {
			externalID = "token:" + strconv.FormatInt(request.TokenID, 10)
		}
		if externalID == "" {
			externalID = syntheticGitHubAuditID(
				"pat_request",
				organization,
				strings.TrimSpace(request.OwnerLogin),
				strings.TrimSpace(request.TokenName),
				strings.TrimSpace(request.CreatedAtRaw),
			)
		}

		displayName := strings.TrimSpace(request.TokenName)
		if displayName == "" {
			displayName = externalID
		}

		createdByKind, createdByExternalID, createdByDisplayName := githubActorIdentity(request.OwnerLogin, request.OwnerID)
		approvedByKind, approvedByExternalID, approvedByDisplayName := githubActorIdentity(request.ReviewerLogin, request.ReviewerID)
		fingerprint := externalID
		if request.TokenID > 0 {
			fingerprint = strconv.FormatInt(request.TokenID, 10)
		}

		rawJSON := request.RawJSON
		if len(rawJSON) == 0 {
			rawJSON = registry.MarshalJSON(map[string]any{
				"id":                   request.ID,
				"token_id":             request.TokenID,
				"token_name":           strings.TrimSpace(request.TokenName),
				"status":               strings.TrimSpace(request.Status),
				"owner_login":          strings.TrimSpace(request.OwnerLogin),
				"owner_id":             request.OwnerID,
				"repository_selection": strings.TrimSpace(request.RepositorySelection),
				"permissions":          request.Permissions,
				"created_at":           strings.TrimSpace(request.CreatedAtRaw),
				"updated_at":           strings.TrimSpace(request.UpdatedAtRaw),
				"last_used_at":         strings.TrimSpace(request.LastUsedAtRaw),
				"expires_at":           strings.TrimSpace(request.ExpiresAtRaw),
				"reviewer_login":       strings.TrimSpace(request.ReviewerLogin),
				"reviewer_id":          request.ReviewerID,
				"reviewed_at":          strings.TrimSpace(request.ReviewedAtRaw),
			})
		}

		rows = append(rows, githubCredentialArtifactUpsertRow{
			AssetRefKind:       "organization",
			AssetRefExternalID: organization,
			CredentialKind:     "github_pat_request",
			ExternalID:         externalID,
			DisplayName:        displayName,
			Fingerprint:        fingerprint,
			ScopeJSON: registry.MarshalJSON(map[string]any{
				"organization":         organization,
				"repository_selection": strings.TrimSpace(request.RepositorySelection),
				"permissions":          request.Permissions,
				"request_id":           request.ID,
				"token_id":             request.TokenID,
			}),
			Status:                normalizeGitHubPATRequestStatus(request.Status),
			CreatedAtSource:       parseGitHubTime(request.CreatedAtRaw),
			ExpiresAtSource:       parseGitHubTime(request.ExpiresAtRaw),
			LastUsedAtSource:      parseGitHubTime(request.LastUsedAtRaw),
			CreatedByKind:         createdByKind,
			CreatedByExternalID:   createdByExternalID,
			CreatedByDisplayName:  createdByDisplayName,
			ApprovedByKind:        approvedByKind,
			ApprovedByExternalID:  approvedByExternalID,
			ApprovedByDisplayName: approvedByDisplayName,
			RawJSON:               rawJSON,
		})
	}

	return rows
}

func buildGitHubPATCredentialRows(org string, pats []PersonalAccessToken) []githubCredentialArtifactUpsertRow {
	rows := make([]githubCredentialArtifactUpsertRow, 0, len(pats))
	organization := strings.TrimSpace(org)
	if organization == "" {
		return rows
	}

	for _, pat := range pats {
		externalID := ""
		if pat.ID > 0 {
			externalID = strconv.FormatInt(pat.ID, 10)
		}
		if externalID == "" {
			externalID = syntheticGitHubAuditID(
				"pat",
				organization,
				strings.TrimSpace(pat.OwnerLogin),
				strings.TrimSpace(pat.Name),
				strings.TrimSpace(pat.CreatedAtRaw),
			)
		}

		displayName := strings.TrimSpace(pat.Name)
		if displayName == "" {
			displayName = externalID
		}

		createdByKind, createdByExternalID, createdByDisplayName := githubActorIdentity(pat.OwnerLogin, pat.OwnerID)
		approvedByKind, approvedByExternalID, approvedByDisplayName := githubActorIdentity(pat.ReviewerLogin, pat.ReviewerID)

		rawJSON := pat.RawJSON
		if len(rawJSON) == 0 {
			rawJSON = registry.MarshalJSON(map[string]any{
				"id":                   pat.ID,
				"name":                 strings.TrimSpace(pat.Name),
				"status":               strings.TrimSpace(pat.Status),
				"owner_login":          strings.TrimSpace(pat.OwnerLogin),
				"owner_id":             pat.OwnerID,
				"repository_selection": strings.TrimSpace(pat.RepositorySelection),
				"permissions":          pat.Permissions,
				"created_at":           strings.TrimSpace(pat.CreatedAtRaw),
				"updated_at":           strings.TrimSpace(pat.UpdatedAtRaw),
				"last_used_at":         strings.TrimSpace(pat.LastUsedAtRaw),
				"expires_at":           strings.TrimSpace(pat.ExpiresAtRaw),
				"expired":              pat.Expired,
				"revoked":              pat.Revoked,
				"reviewer_login":       strings.TrimSpace(pat.ReviewerLogin),
				"reviewer_id":          pat.ReviewerID,
				"reviewed_at":          strings.TrimSpace(pat.ReviewedAtRaw),
			})
		}

		rows = append(rows, githubCredentialArtifactUpsertRow{
			AssetRefKind:       "organization",
			AssetRefExternalID: organization,
			CredentialKind:     "github_pat_fine_grained",
			ExternalID:         externalID,
			DisplayName:        displayName,
			Fingerprint:        externalID,
			ScopeJSON: registry.MarshalJSON(map[string]any{
				"organization":         organization,
				"repository_selection": strings.TrimSpace(pat.RepositorySelection),
				"permissions":          pat.Permissions,
			}),
			Status:                normalizeGitHubPATStatus(pat),
			CreatedAtSource:       parseGitHubTime(pat.CreatedAtRaw),
			ExpiresAtSource:       parseGitHubTime(pat.ExpiresAtRaw),
			LastUsedAtSource:      parseGitHubTime(pat.LastUsedAtRaw),
			CreatedByKind:         createdByKind,
			CreatedByExternalID:   createdByExternalID,
			CreatedByDisplayName:  createdByDisplayName,
			ApprovedByKind:        approvedByKind,
			ApprovedByExternalID:  approvedByExternalID,
			ApprovedByDisplayName: approvedByDisplayName,
			RawJSON:               rawJSON,
		})
	}

	return rows
}

func githubActorIdentity(login string, id int64) (string, string, string) {
	login = strings.TrimSpace(login)
	if login != "" {
		return "github_user", login, login
	}
	if id > 0 {
		actor := strconv.FormatInt(id, 10)
		return "github_user", actor, actor
	}
	return "", "", ""
}

func normalizeGitHubPATRequestStatus(raw string) string {
	status := strings.ToLower(strings.TrimSpace(raw))
	switch status {
	case "":
		return "pending_approval"
	case "pending", "pending_review", "pending_approval", "requested":
		return "pending_approval"
	case "approved", "granted", "active":
		return "approved"
	case "denied", "rejected":
		return "rejected"
	case "cancelled", "canceled":
		return "cancelled"
	case "revoked":
		return "revoked"
	case "expired":
		return "expired"
	default:
		return status
	}
}

func normalizeGitHubPATStatus(pat PersonalAccessToken) string {
	status := strings.ToLower(strings.TrimSpace(pat.Status))
	switch status {
	case "pending", "pending_review", "pending_approval", "requested":
		return "pending_approval"
	case "denied", "rejected":
		return "rejected"
	case "cancelled", "canceled":
		return "cancelled"
	case "revoked", "inactive":
		return "revoked"
	case "expired":
		return "expired"
	case "approved", "granted", "active":
		return "active"
	}

	if pat.Revoked {
		return "revoked"
	}
	if pat.Expired {
		return "expired"
	}
	expiresAt := parseGitHubTime(pat.ExpiresAtRaw)
	if expiresAt.Valid && expiresAt.Time.Before(time.Now()) {
		return "expired"
	}
	return "active"
}

func githubDeployKeyFingerprint(publicKey string) string {
	fields := strings.Fields(strings.TrimSpace(publicKey))
	if len(fields) >= 2 {
		return fields[1]
	}
	if len(fields) == 1 {
		return fields[0]
	}
	return ""
}

func repositoryOwner(fullName string) string {
	fullName = strings.TrimSpace(fullName)
	if fullName == "" {
		return ""
	}
	parts := strings.SplitN(fullName, "/", 2)
	return strings.TrimSpace(parts[0])
}

func buildGitHubAuditEventRows(org string, events []AuditLogEvent) []githubCredentialAuditEventUpsertRow {
	rows := make([]githubCredentialAuditEventUpsertRow, 0, len(events))

	for _, event := range events {
		eventTime := parseGitHubTime(event.CreatedAtRaw)
		if !eventTime.Valid {
			continue
		}

		eventType := strings.TrimSpace(event.Action)
		if eventType == "" {
			eventType = "github_audit_event"
		}

		actor := strings.TrimSpace(event.Actor)
		if actor == "" {
			actor = strings.TrimSpace(event.User)
		}
		actorKind := "unknown"
		if actor != "" {
			actorKind = "github_user"
		}

		targetExternalID := strings.TrimSpace(event.Repository)
		if targetExternalID == "" {
			targetExternalID = strings.TrimSpace(event.Repo)
		}
		targetKind := "organization"
		if targetExternalID == "" {
			targetExternalID = strings.TrimSpace(org)
		} else {
			targetKind = "repository"
		}

		credentialKind := githubCredentialKindFromAuditAction(event.Action)
		credentialExternalID := firstNonEmpty(
			strings.TrimSpace(event.DeployKeyID),
			strings.TrimSpace(event.KeyID),
			strings.TrimSpace(event.TokenID),
			strings.TrimSpace(event.PersonalTokenID),
			strings.TrimSpace(event.PATRequestID),
			strings.TrimSpace(event.RequestID),
		)
		if credentialKind == "" && credentialExternalID == "" {
			continue
		}

		eventExternalID := firstNonEmpty(strings.TrimSpace(event.DocumentID), strings.TrimSpace(event.ID))
		if eventExternalID == "" {
			eventExternalID = syntheticGitHubAuditID(
				"audit_event",
				eventType,
				strings.TrimSpace(event.CreatedAtRaw),
				actor,
				targetExternalID,
				credentialKind,
				credentialExternalID,
			)
		}

		rawJSON := event.RawJSON
		if len(rawJSON) == 0 {
			rawJSON = registry.MarshalJSON(map[string]any{
				"action":      strings.TrimSpace(event.Action),
				"actor":       strings.TrimSpace(event.Actor),
				"user":        strings.TrimSpace(event.User),
				"created_at":  strings.TrimSpace(event.CreatedAtRaw),
				"repository":  strings.TrimSpace(event.Repository),
				"repo":        strings.TrimSpace(event.Repo),
				"request_id":  strings.TrimSpace(event.RequestID),
				"pat_request": strings.TrimSpace(event.PATRequestID),
				"pat_id":      strings.TrimSpace(event.PersonalTokenID),
				"key_id":      strings.TrimSpace(event.KeyID),
				"deploy_key":  strings.TrimSpace(event.DeployKeyID),
				"token_id":    strings.TrimSpace(event.TokenID),
				"fingerprint": strings.TrimSpace(event.Fingerprint),
			})
		}

		rows = append(rows, githubCredentialAuditEventUpsertRow{
			EventExternalID:      eventExternalID,
			EventType:            eventType,
			EventTime:            eventTime,
			ActorKind:            actorKind,
			ActorExternalID:      actor,
			ActorDisplayName:     actor,
			TargetKind:           targetKind,
			TargetExternalID:     targetExternalID,
			TargetDisplayName:    targetExternalID,
			CredentialKind:       credentialKind,
			CredentialExternalID: credentialExternalID,
			RawJSON:              rawJSON,
		})
	}

	return rows
}

func githubCredentialKindFromAuditAction(action string) string {
	action = strings.ToLower(strings.TrimSpace(action))
	switch {
	case strings.Contains(action, "deploy_key"), strings.Contains(action, "public_key"), strings.Contains(action, "repository_key"):
		return "github_deploy_key"
	case strings.Contains(action, "pat_request"), strings.Contains(action, "personal_access_token_request"):
		return "github_pat_request"
	case strings.Contains(action, "personal_access_token"), strings.Contains(action, "fine_grained_pat"):
		return "github_pat_fine_grained"
	case strings.Contains(action, "oauth_authorization"), strings.Contains(action, "oauth_app"):
		return "github_oauth_app_token"
	default:
		return ""
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func syntheticGitHubAuditID(prefix string, parts ...string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(strings.TrimSpace(prefix)))
	for _, part := range parts {
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(strings.TrimSpace(part)))
	}
	return fmt.Sprintf("github:%s:%x", strings.TrimSpace(prefix), h.Sum64())
}

func parseGitHubTime(raw string) pgtype.Timestamptz {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return pgtype.Timestamptz{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		parsed, err = time.Parse(time.RFC3339, raw)
		if err != nil {
			return pgtype.Timestamptz{}
		}
	}
	return pgtype.Timestamptz{Time: parsed, Valid: true}
}

func (i *GitHubIntegration) upsertProgrammaticAppAssets(ctx context.Context, deps registry.IntegrationDeps, runID int64, rows []githubAppAssetUpsertRow) error {
	deps.Report(registry.Event{Source: "github", Stage: "write-programmatic-assets", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d programmatic app assets", len(rows))})
	if len(rows) == 0 {
		return nil
	}

	for start := 0; start < len(rows); start += githubAppAssetBatchSize {
		end := start + githubAppAssetBatchSize
		if end > len(rows) {
			end = len(rows)
		}

		batch := rows[start:end]
		assetKinds := make([]string, 0, len(batch))
		externalIDs := make([]string, 0, len(batch))
		parentExternalIDs := make([]string, 0, len(batch))
		displayNames := make([]string, 0, len(batch))
		statuses := make([]string, 0, len(batch))
		createdAtSources := make([]pgtype.Timestamptz, 0, len(batch))
		updatedAtSources := make([]pgtype.Timestamptz, 0, len(batch))
		rawJSONs := make([][]byte, 0, len(batch))
		for _, row := range batch {
			assetKinds = append(assetKinds, row.AssetKind)
			externalIDs = append(externalIDs, row.ExternalID)
			parentExternalIDs = append(parentExternalIDs, row.ParentExternalID)
			displayNames = append(displayNames, row.DisplayName)
			statuses = append(statuses, row.Status)
			createdAtSources = append(createdAtSources, row.CreatedAtSource)
			updatedAtSources = append(updatedAtSources, row.UpdatedAtSource)
			rawJSONs = append(rawJSONs, row.RawJSON)
		}

		if _, err := deps.Q.UpsertAppAssetsBulkBySource(ctx, gen.UpsertAppAssetsBulkBySourceParams{
			SourceKind:        "github",
			SourceName:        i.org,
			SeenInRunID:       runID,
			AssetKinds:        assetKinds,
			ExternalIds:       externalIDs,
			ParentExternalIds: parentExternalIDs,
			DisplayNames:      displayNames,
			Statuses:          statuses,
			CreatedAtSources:  createdAtSources,
			UpdatedAtSources:  updatedAtSources,
			RawJsons:          rawJSONs,
		}); err != nil {
			return err
		}

		deps.Report(registry.Event{Source: "github", Stage: "write-programmatic-assets", Current: int64(end), Total: int64(len(rows)), Message: fmt.Sprintf("programmatic app assets %d/%d", end, len(rows))})
	}

	return nil
}

func (i *GitHubIntegration) upsertProgrammaticAssetOwners(ctx context.Context, deps registry.IntegrationDeps, runID int64, rows []githubAppAssetOwnerUpsertRow) error {
	deps.Report(registry.Event{Source: "github", Stage: "write-programmatic-owners", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d programmatic app owners", len(rows))})
	if len(rows) == 0 {
		return nil
	}

	for start := 0; start < len(rows); start += githubOwnerBatchSize {
		end := start + githubOwnerBatchSize
		if end > len(rows) {
			end = len(rows)
		}

		batch := rows[start:end]
		assetKinds := make([]string, 0, len(batch))
		assetExternalIDs := make([]string, 0, len(batch))
		ownerKinds := make([]string, 0, len(batch))
		ownerExternalIDs := make([]string, 0, len(batch))
		ownerDisplayNames := make([]string, 0, len(batch))
		ownerEmails := make([]string, 0, len(batch))
		rawJSONs := make([][]byte, 0, len(batch))
		for _, row := range batch {
			assetKinds = append(assetKinds, row.AssetKind)
			assetExternalIDs = append(assetExternalIDs, row.AssetExternalID)
			ownerKinds = append(ownerKinds, row.OwnerKind)
			ownerExternalIDs = append(ownerExternalIDs, row.OwnerExternalID)
			ownerDisplayNames = append(ownerDisplayNames, row.OwnerDisplayName)
			ownerEmails = append(ownerEmails, row.OwnerEmail)
			rawJSONs = append(rawJSONs, row.RawJSON)
		}

		if _, err := deps.Q.UpsertAppAssetOwnersBulkBySource(ctx, gen.UpsertAppAssetOwnersBulkBySourceParams{
			SeenInRunID:       runID,
			SourceKind:        "github",
			SourceName:        i.org,
			AssetKinds:        assetKinds,
			AssetExternalIds:  assetExternalIDs,
			OwnerKinds:        ownerKinds,
			OwnerExternalIds:  ownerExternalIDs,
			OwnerDisplayNames: ownerDisplayNames,
			OwnerEmails:       ownerEmails,
			RawJsons:          rawJSONs,
		}); err != nil {
			return err
		}

		deps.Report(registry.Event{Source: "github", Stage: "write-programmatic-owners", Current: int64(end), Total: int64(len(rows)), Message: fmt.Sprintf("programmatic owners %d/%d", end, len(rows))})
	}

	return nil
}

func (i *GitHubIntegration) upsertProgrammaticCredentials(ctx context.Context, deps registry.IntegrationDeps, runID int64, rows []githubCredentialArtifactUpsertRow) error {
	deps.Report(registry.Event{Source: "github", Stage: "write-programmatic-credentials", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d programmatic credentials", len(rows))})
	if len(rows) == 0 {
		return nil
	}

	for start := 0; start < len(rows); start += githubCredentialBatchSize {
		end := start + githubCredentialBatchSize
		if end > len(rows) {
			end = len(rows)
		}

		batch := rows[start:end]
		assetRefKinds := make([]string, 0, len(batch))
		assetRefExternalIDs := make([]string, 0, len(batch))
		credentialKinds := make([]string, 0, len(batch))
		externalIDs := make([]string, 0, len(batch))
		displayNames := make([]string, 0, len(batch))
		fingerprints := make([]string, 0, len(batch))
		scopeJSONs := make([][]byte, 0, len(batch))
		statuses := make([]string, 0, len(batch))
		createdAtSources := make([]pgtype.Timestamptz, 0, len(batch))
		expiresAtSources := make([]pgtype.Timestamptz, 0, len(batch))
		lastUsedAtSources := make([]pgtype.Timestamptz, 0, len(batch))
		createdByKinds := make([]string, 0, len(batch))
		createdByExternalIDs := make([]string, 0, len(batch))
		createdByDisplayNames := make([]string, 0, len(batch))
		approvedByKinds := make([]string, 0, len(batch))
		approvedByExternalIDs := make([]string, 0, len(batch))
		approvedByDisplayNames := make([]string, 0, len(batch))
		rawJSONs := make([][]byte, 0, len(batch))
		for _, row := range batch {
			assetRefKinds = append(assetRefKinds, row.AssetRefKind)
			assetRefExternalIDs = append(assetRefExternalIDs, row.AssetRefExternalID)
			credentialKinds = append(credentialKinds, row.CredentialKind)
			externalIDs = append(externalIDs, row.ExternalID)
			displayNames = append(displayNames, row.DisplayName)
			fingerprints = append(fingerprints, row.Fingerprint)
			scopeJSONs = append(scopeJSONs, row.ScopeJSON)
			statuses = append(statuses, row.Status)
			createdAtSources = append(createdAtSources, row.CreatedAtSource)
			expiresAtSources = append(expiresAtSources, row.ExpiresAtSource)
			lastUsedAtSources = append(lastUsedAtSources, row.LastUsedAtSource)
			createdByKinds = append(createdByKinds, row.CreatedByKind)
			createdByExternalIDs = append(createdByExternalIDs, row.CreatedByExternalID)
			createdByDisplayNames = append(createdByDisplayNames, row.CreatedByDisplayName)
			approvedByKinds = append(approvedByKinds, row.ApprovedByKind)
			approvedByExternalIDs = append(approvedByExternalIDs, row.ApprovedByExternalID)
			approvedByDisplayNames = append(approvedByDisplayNames, row.ApprovedByDisplayName)
			rawJSONs = append(rawJSONs, row.RawJSON)
		}

		if _, err := deps.Q.UpsertCredentialArtifactsBulkBySource(ctx, gen.UpsertCredentialArtifactsBulkBySourceParams{
			SourceKind:             "github",
			SourceName:             i.org,
			SeenInRunID:            runID,
			AssetRefKinds:          assetRefKinds,
			AssetRefExternalIds:    assetRefExternalIDs,
			CredentialKinds:        credentialKinds,
			ExternalIds:            externalIDs,
			DisplayNames:           displayNames,
			Fingerprints:           fingerprints,
			ScopeJsons:             scopeJSONs,
			Statuses:               statuses,
			CreatedAtSources:       createdAtSources,
			ExpiresAtSources:       expiresAtSources,
			LastUsedAtSources:      lastUsedAtSources,
			CreatedByKinds:         createdByKinds,
			CreatedByExternalIds:   createdByExternalIDs,
			CreatedByDisplayNames:  createdByDisplayNames,
			ApprovedByKinds:        approvedByKinds,
			ApprovedByExternalIds:  approvedByExternalIDs,
			ApprovedByDisplayNames: approvedByDisplayNames,
			RawJsons:               rawJSONs,
		}); err != nil {
			return err
		}

		deps.Report(registry.Event{Source: "github", Stage: "write-programmatic-credentials", Current: int64(end), Total: int64(len(rows)), Message: fmt.Sprintf("programmatic credentials %d/%d", end, len(rows))})
	}

	return nil
}

func (i *GitHubIntegration) upsertProgrammaticAuditEvents(ctx context.Context, deps registry.IntegrationDeps, rows []githubCredentialAuditEventUpsertRow) error {
	deps.Report(registry.Event{Source: "github", Stage: "write-programmatic-audit-events", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d programmatic credential audit events", len(rows))})
	if len(rows) == 0 {
		return nil
	}

	for start := 0; start < len(rows); start += githubAuditEventBatchSize {
		end := start + githubAuditEventBatchSize
		if end > len(rows) {
			end = len(rows)
		}

		batch := rows[start:end]
		eventExternalIDs := make([]string, 0, len(batch))
		eventTypes := make([]string, 0, len(batch))
		eventTimes := make([]pgtype.Timestamptz, 0, len(batch))
		actorKinds := make([]string, 0, len(batch))
		actorExternalIDs := make([]string, 0, len(batch))
		actorDisplayNames := make([]string, 0, len(batch))
		targetKinds := make([]string, 0, len(batch))
		targetExternalIDs := make([]string, 0, len(batch))
		targetDisplayNames := make([]string, 0, len(batch))
		credentialKinds := make([]string, 0, len(batch))
		credentialExternalIDs := make([]string, 0, len(batch))
		rawJSONs := make([][]byte, 0, len(batch))
		for _, row := range batch {
			eventExternalIDs = append(eventExternalIDs, row.EventExternalID)
			eventTypes = append(eventTypes, row.EventType)
			eventTimes = append(eventTimes, row.EventTime)
			actorKinds = append(actorKinds, row.ActorKind)
			actorExternalIDs = append(actorExternalIDs, row.ActorExternalID)
			actorDisplayNames = append(actorDisplayNames, row.ActorDisplayName)
			targetKinds = append(targetKinds, row.TargetKind)
			targetExternalIDs = append(targetExternalIDs, row.TargetExternalID)
			targetDisplayNames = append(targetDisplayNames, row.TargetDisplayName)
			credentialKinds = append(credentialKinds, row.CredentialKind)
			credentialExternalIDs = append(credentialExternalIDs, row.CredentialExternalID)
			rawJSONs = append(rawJSONs, row.RawJSON)
		}

		if _, err := deps.Q.UpsertCredentialAuditEventsBulkBySource(ctx, gen.UpsertCredentialAuditEventsBulkBySourceParams{
			SourceKind:            "github",
			SourceName:            i.org,
			EventExternalIds:      eventExternalIDs,
			EventTypes:            eventTypes,
			EventTimes:            eventTimes,
			ActorKinds:            actorKinds,
			ActorExternalIds:      actorExternalIDs,
			ActorDisplayNames:     actorDisplayNames,
			TargetKinds:           targetKinds,
			TargetExternalIds:     targetExternalIDs,
			TargetDisplayNames:    targetDisplayNames,
			CredentialKinds:       credentialKinds,
			CredentialExternalIds: credentialExternalIDs,
			RawJsons:              rawJSONs,
		}); err != nil {
			return err
		}

		deps.Report(registry.Event{Source: "github", Stage: "write-programmatic-audit-events", Current: int64(end), Total: int64(len(rows)), Message: fmt.Sprintf("programmatic audit events %d/%d", end, len(rows))})
	}

	return nil
}
