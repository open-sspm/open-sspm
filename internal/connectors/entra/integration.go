package entra

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type EntraIntegration struct {
	client   *Client
	tenantID string
}

func NewEntraIntegration(client *Client, tenantID string) *EntraIntegration {
	return &EntraIntegration{
		client:   client,
		tenantID: strings.ToLower(strings.TrimSpace(tenantID)),
	}
}

func (i *EntraIntegration) Kind() string { return "entra" }
func (i *EntraIntegration) Name() string { return i.tenantID }
func (i *EntraIntegration) Role() registry.IntegrationRole {
	return registry.RoleApp
}

func (i *EntraIntegration) InitEvents() []registry.Event {
	return []registry.Event{
		{Source: "entra", Stage: "list-users", Current: 0, Total: 1, Message: "listing Entra users"},
		{Source: "entra", Stage: "write-users", Current: 0, Total: registry.UnknownTotal, Message: "writing Entra users"},
	}
}

func (i *EntraIntegration) Run(ctx context.Context, deps registry.IntegrationDeps) error {
	started := time.Now()
	log.Println("Syncing Microsoft Entra ID")

	runID, err := deps.Q.CreateSyncRun(ctx, gen.CreateSyncRunParams{
		SourceKind: "entra",
		SourceName: i.tenantID,
	})
	if err != nil {
		return err
	}

	users, err := i.client.ListUsers(ctx)
	if err != nil {
		deps.Report(registry.Event{Source: "entra", Stage: "list-users", Message: err.Error(), Err: err})
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindAPI)
		return err
	}
	deps.Report(registry.Event{Source: "entra", Stage: "list-users", Current: 1, Total: 1, Message: fmt.Sprintf("found %d users", len(users))})
	deps.Report(registry.Event{Source: "entra", Stage: "write-users", Current: 0, Total: int64(len(users)), Message: fmt.Sprintf("writing %d users", len(users))})

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

		email := normalizeEmail(preferredEmail(user))

		display := strings.TrimSpace(user.DisplayName)
		if display == "" {
			display = strings.TrimSpace(email)
		}
		if display == "" {
			display = externalID
		}

		raw, err := json.Marshal(sanitizedUser{
			ID:                 externalID,
			DisplayName:        strings.TrimSpace(user.DisplayName),
			Mail:               strings.TrimSpace(user.Mail),
			UserPrincipalName:  strings.TrimSpace(user.UserPrincipalName),
			OtherMails:         user.OtherMails,
			ProxyAddresses:     user.ProxyAddresses,
			UserType:           strings.TrimSpace(user.UserType),
			AccountEnabled:     user.AccountEnabled,
			Status:             entraAccountStatus(user.AccountEnabled),
			CreatedDateTimeRaw: strings.TrimSpace(user.CreatedDateTimeRaw),
		})
		if err != nil {
			deps.Report(registry.Event{Source: "entra", Stage: "write-users", Message: err.Error(), Err: err})
			registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindUnknown)
			return err
		}

		externalIDs = append(externalIDs, externalID)
		emails = append(emails, email)
		displayNames = append(displayNames, display)
		rawJSONs = append(rawJSONs, raw)
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
			SourceKind:       "entra",
			SourceName:       i.tenantID,
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
			deps.Report(registry.Event{Source: "entra", Stage: "write-users", Message: err.Error(), Err: err})
			registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
			return err
		}

		deps.Report(registry.Event{
			Source:  "entra",
			Stage:   "write-users",
			Current: int64(end),
			Total:   int64(len(externalIDs)),
			Message: fmt.Sprintf("users %d/%d", end, len(externalIDs)),
		})
	}

	if err := registry.FinalizeAppRun(ctx, deps, runID, "entra", i.tenantID, time.Since(started)); err != nil {
		registry.FailSyncRun(ctx, deps.Q, runID, err, registry.SyncErrorKindDB)
		return err
	}
	log.Printf("entra sync complete: %d users", len(externalIDs))
	return nil
}

type sanitizedUser struct {
	ID                string   `json:"id"`
	DisplayName       string   `json:"display_name,omitempty"`
	Mail              string   `json:"mail,omitempty"`
	UserPrincipalName string   `json:"user_principal_name,omitempty"`
	OtherMails        []string `json:"other_mails,omitempty"`
	ProxyAddresses    []string `json:"proxy_addresses,omitempty"`
	UserType          string   `json:"user_type,omitempty"`
	AccountEnabled    *bool    `json:"account_enabled,omitempty"`
	Status            string   `json:"status,omitempty"`
	// Kept as-is to avoid timezone parsing/format churn until needed.
	CreatedDateTimeRaw string `json:"created_date_time,omitempty"`
}

func entraAccountStatus(accountEnabled *bool) string {
	if accountEnabled == nil {
		return ""
	}
	if *accountEnabled {
		return "Active"
	}
	return "Inactive"
}
