package aws

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/matching"
)

type AWSIntegration struct {
	client     *Client
	sourceName string
}

func NewAWSIntegration(client *Client, sourceName string) *AWSIntegration {
	name := strings.TrimSpace(sourceName)
	if name == "" {
		name = "aws"
	}
	return &AWSIntegration{client: client, sourceName: name}
}

func (i *AWSIntegration) Kind() string { return "aws" }
func (i *AWSIntegration) Name() string { return i.sourceName }
func (i *AWSIntegration) Role() registry.IntegrationRole {
	return registry.RoleApp
}

func (i *AWSIntegration) InitEvents() []registry.Event {
	return []registry.Event{
		{Source: "aws", Stage: "list-users", Current: 0, Total: 1, Message: "listing identity center users"},
		{Source: "aws", Stage: "list-assignments", Current: 0, Total: 1, Message: "listing account assignments"},
		{Source: "aws", Stage: "write-users", Current: 0, Total: registry.UnknownTotal, Message: "writing users"},
	}
}

func (i *AWSIntegration) Run(ctx context.Context, deps registry.IntegrationDeps) error {
	started := time.Now()
	slog.Info("syncing AWS Identity Center")

	runID, err := deps.Q.CreateSyncRun(ctx, gen.CreateSyncRunParams{
		SourceKind: "aws",
		SourceName: i.sourceName,
	})
	if err != nil {
		return err
	}

	users, err := i.client.ListUsers(ctx)
	if err != nil {
		deps.Report(registry.Event{Source: "aws", Stage: "list-users", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindAPI)
		return err
	}
	deps.Report(registry.Event{Source: "aws", Stage: "list-users", Current: 1, Total: 1, Message: fmt.Sprintf("found %d users", len(users))})
	deps.Report(registry.Event{Source: "aws", Stage: "write-users", Current: 0, Total: int64(len(users)), Message: fmt.Sprintf("writing %d users", len(users))})

	entitlementsByUser, err := i.client.ListUserEntitlements(ctx)
	if err != nil {
		deps.Report(registry.Event{Source: "aws", Stage: "list-assignments", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindAPI)
		return err
	}
	deps.Report(registry.Event{Source: "aws", Stage: "list-assignments", Current: 1, Total: 1, Message: "assignments fetched"})

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
		email := matching.NormalizeEmail(user.Email)
		display := strings.TrimSpace(user.DisplayName)
		if display == "" {
			display = email
		}
		if display == "" {
			display = externalID
		}

		externalIDs = append(externalIDs, externalID)
		emails = append(emails, email)
		displayNames = append(displayNames, display)
		rawJSONs = append(rawJSONs, registry.NormalizeJSON(user.RawJSON))
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
			SourceKind:       "aws",
			SourceName:       i.sourceName,
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
			deps.Report(registry.Event{Source: "aws", Stage: "write-users", Message: err.Error(), Err: err})
			registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
			return err
		}
		deps.Report(registry.Event{
			Source:  "aws",
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
		facts := dedupeAWSEntitlements(entitlementsByUser[userID])
		for _, fact := range facts {
			permission := strings.TrimSpace(fact.PermissionSetName)
			if permission == "" {
				permission = strings.TrimSpace(fact.PermissionSetArn)
			}
			raw := map[string]string{
				"permission_set_arn": fact.PermissionSetArn,
				"assignment_source":  string(fact.AssignmentSource),
			}
			if groupID := strings.TrimSpace(fact.GroupID); groupID != "" {
				raw["group_id"] = groupID
			}
			entAppUserExternalIDs = append(entAppUserExternalIDs, userID)
			entKinds = append(entKinds, "aws_permission_set")
			entResources = append(entResources, "aws_account:"+strings.TrimSpace(fact.AccountID))
			entPermissions = append(entPermissions, permission)
			entRawJSONs = append(entRawJSONs, registry.MarshalJSON(raw))
		}
	}

	for start := 0; start < len(entAppUserExternalIDs); start += entitlementBatchSize {
		end := start + entitlementBatchSize
		if end > len(entAppUserExternalIDs) {
			end = len(entAppUserExternalIDs)
		}
		_, err := deps.Q.UpsertEntitlementsBulkBySource(ctx, gen.UpsertEntitlementsBulkBySourceParams{
			SeenInRunID:        runID,
			SourceKind:         "aws",
			SourceName:         i.sourceName,
			AppUserExternalIds: entAppUserExternalIDs[start:end],
			Kinds:              entKinds[start:end],
			Resources:          entResources[start:end],
			Permissions:        entPermissions[start:end],
			RawJsons:           entRawJSONs[start:end],
		})
		if err != nil {
			deps.Report(registry.Event{Source: "aws", Stage: "write-users", Message: err.Error(), Err: err})
			registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
			return err
		}
	}

	if err := registry.FinalizeAppRun(ctx, deps, runID, "aws", i.sourceName, time.Since(started), false); err != nil {
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
		return err
	}
	slog.Info("aws sync complete", "users", len(users))
	return nil
}

type awsEntitlementKey struct {
	accountID  string
	permission string
}

func dedupeAWSEntitlements(facts []EntitlementFact) []EntitlementFact {
	if len(facts) == 0 {
		return nil
	}
	seen := make(map[awsEntitlementKey]EntitlementFact)
	hasDirect := make(map[awsEntitlementKey]bool)

	for _, fact := range facts {
		accountID := strings.TrimSpace(fact.AccountID)
		permission := strings.TrimSpace(fact.PermissionSetName)
		if permission == "" {
			permission = strings.TrimSpace(fact.PermissionSetArn)
		}
		if accountID == "" || permission == "" {
			continue
		}
		key := awsEntitlementKey{accountID: accountID, permission: permission}
		if existing, ok := seen[key]; ok {
			if hasDirect[key] {
				continue
			}
			if fact.AssignmentSource == AssignmentSourceDirect {
				seen[key] = fact
				hasDirect[key] = true
				continue
			}
			seen[key] = existing
			continue
		}
		seen[key] = fact
		if fact.AssignmentSource == AssignmentSourceDirect {
			hasDirect[key] = true
		}
	}

	out := make([]EntitlementFact, 0, len(seen))
	for _, fact := range seen {
		out = append(out, fact)
	}
	return out
}
