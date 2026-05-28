package aws

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/identity"
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
		{Source: "aws", Stage: "list-groups", Current: 0, Total: 1, Message: "listing identity center groups"},
		{Source: "aws", Stage: "list-assignments", Current: 0, Total: 1, Message: "listing account assignments"},
		{Source: "aws", Stage: "write-users", Current: 0, Total: registry.UnknownTotal, Message: "writing principals"},
	}
}

func (i *AWSIntegration) Run(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, report func(registry.Event), mode registry.RunMode) error {
	if mode.Normalize() == registry.RunModeTail {
		return i.runCloudTrailTail(ctx, q, pool, report)
	}

	started := time.Now()
	slog.Info("syncing AWS Identity Center")

	runID, err := registry.StartSyncRun(ctx, q, "aws", i.sourceName)
	if err != nil {
		return err
	}

	users, err := i.client.ListUsers(ctx)
	if err != nil {
		return registry.ReportAndFailSyncRun(ctx, q, runID, report, registry.Event{Source: "aws", Stage: "list-users"}, err, registry.SyncErrorKindAPI)
	}
	report(registry.Event{Source: "aws", Stage: "list-users", Current: 1, Total: 1, Message: fmt.Sprintf("found %d users", len(users))})

	groups, err := i.client.ListGroups(ctx)
	if err != nil {
		return registry.ReportAndFailSyncRun(ctx, q, runID, report, registry.Event{Source: "aws", Stage: "list-groups"}, err, registry.SyncErrorKindAPI)
	}
	report(registry.Event{Source: "aws", Stage: "list-groups", Current: 1, Total: 1, Message: fmt.Sprintf("found %d groups", len(groups))})
	report(registry.Event{
		Source:  "aws",
		Stage:   "write-users",
		Current: 0,
		Total:   int64(len(users) + len(groups)),
		Message: fmt.Sprintf("writing %d principals", len(users)+len(groups)),
	})

	entitlementsByUser, err := i.client.ListUserEntitlements(ctx)
	if err != nil {
		return registry.ReportAndFailSyncRun(ctx, q, runID, report, registry.Event{Source: "aws", Stage: "list-assignments"}, err, registry.SyncErrorKindAPI)
	}
	report(registry.Event{Source: "aws", Stage: "list-assignments", Current: 1, Total: 1, Message: "assignments fetched"})

	const userBatchSize = 1000
	totalPrincipals := len(users) + len(groups)
	accountRows := make([]registry.SourceAccountRow, 0, totalPrincipals)

	for _, user := range users {
		externalID := strings.TrimSpace(user.ID)
		if externalID == "" {
			continue
		}
		email := identity.NormalizeEmail(user.Email)
		display := strings.TrimSpace(user.DisplayName)
		if display == "" {
			display = email
		}
		if display == "" {
			display = externalID
		}

		accountRows = append(accountRows, registry.SourceAccountRow{
			ExternalID:     externalID,
			Email:          email,
			DisplayName:    display,
			AccountKind:    awsUserAccountKind(user),
			EntityCategory: registry.EntityCategoryUser,
			RawJSON:        registry.WithEntityCategory(registry.NormalizeJSON(user.RawJSON), registry.EntityCategoryUser),
		})
	}

	for _, group := range groups {
		externalID := awsGroupExternalID(group.ID)
		if externalID == "" {
			continue
		}
		display := strings.TrimSpace(group.DisplayName)
		if display == "" {
			display = externalID
		}

		accountRows = append(accountRows, registry.SourceAccountRow{
			ExternalID:     externalID,
			DisplayName:    display,
			AccountKind:    registry.AccountKindService,
			EntityCategory: registry.EntityCategoryGroup,
			RawJSON:        registry.WithEntityCategory(registry.NormalizeJSON(group.RawJSON), registry.EntityCategoryGroup),
		})
	}

	if _, err := registry.WriteSourceAccountRows(ctx, q, registry.WriteSourceAccountRowsParams{
		SourceKind: "aws",
		SourceName: i.sourceName,
		RunID:      runID,
		BatchSize:  userBatchSize,
		Rows:       accountRows,
	}); err != nil {
		return registry.ReportAndFailSyncRun(ctx, q, runID, report, registry.Event{Source: "aws", Stage: "write-users"}, err, registry.SyncErrorKindDB)
	}
	if len(accountRows) > 0 {
		report(registry.Event{
			Source:  "aws",
			Stage:   "write-users",
			Current: int64(len(accountRows)),
			Total:   int64(len(accountRows)),
			Message: fmt.Sprintf("principals %d/%d", len(accountRows), len(accountRows)),
		})
	}

	const entitlementBatchSize = 5000
	entitlementRows := make([]registry.EntitlementRow, 0, len(users))

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
			entitlementRows = append(entitlementRows, registry.EntitlementRow{
				AccountExternalID: userID,
				Kind:              "aws_permission_set",
				Resource:          "aws_account:" + strings.TrimSpace(fact.AccountID),
				Permission:        permission,
				RawJSON:           registry.MarshalJSON(raw),
			})
		}
	}

	if _, err := registry.WriteEntitlementRows(ctx, q, registry.WriteEntitlementRowsParams{
		SourceKind: "aws",
		SourceName: i.sourceName,
		RunID:      runID,
		BatchSize:  entitlementBatchSize,
		Rows:       entitlementRows,
	}); err != nil {
		return registry.ReportAndFailSyncRun(ctx, q, runID, report, registry.Event{Source: "aws", Stage: "write-users"}, err, registry.SyncErrorKindDB)
	}

	if err := registry.FinalizeAppRun(ctx, q, pool, runID, "aws", i.sourceName, time.Since(started), false); err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
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
