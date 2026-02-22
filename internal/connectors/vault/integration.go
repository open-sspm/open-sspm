package vault

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

const (
	vaultUserBatchSize        = 1000
	vaultEntitlementBatchSize = 5000
	vaultAssetBatchSize       = 1000
)

type VaultIntegration struct {
	client        *Client
	sourceName    string
	scanAuthRoles bool
}

type vaultAccountUpsertRow struct {
	ExternalID  string
	Email       string
	DisplayName string
	AccountKind string
	Status      string
	RawJSON     []byte
}

type vaultEntitlementUpsertRow struct {
	AppUserExternalID string
	Kind              string
	Resource          string
	Permission        string
	RawJSON           []byte
}

type vaultAssetUpsertRow struct {
	AssetKind        string
	ExternalID       string
	ParentExternalID string
	DisplayName      string
	Status           string
	RawJSON          []byte
}

func NewVaultIntegration(client *Client, sourceName string, scanAuthRoles bool) *VaultIntegration {
	name := strings.TrimSpace(sourceName)
	if name == "" {
		name = "vault"
	}
	return &VaultIntegration{
		client:        client,
		sourceName:    name,
		scanAuthRoles: scanAuthRoles,
	}
}

func (i *VaultIntegration) Kind() string { return "vault" }
func (i *VaultIntegration) Name() string { return i.sourceName }
func (i *VaultIntegration) Role() registry.IntegrationRole {
	return registry.RoleApp
}

func (i *VaultIntegration) InitEvents() []registry.Event {
	return []registry.Event{
		{Source: "vault", Stage: "list-entities", Current: 0, Total: 1, Message: "listing Vault identity entities"},
		{Source: "vault", Stage: "list-groups", Current: 0, Total: 1, Message: "listing Vault identity groups"},
		{Source: "vault", Stage: "list-policies", Current: 0, Total: 1, Message: "listing Vault ACL policies"},
		{Source: "vault", Stage: "list-mounts", Current: 0, Total: 1, Message: "listing Vault auth and secrets mounts"},
		{Source: "vault", Stage: "list-auth-roles", Current: 0, Total: 1, Message: "listing Vault auth roles"},
		{Source: "vault", Stage: "write-users", Current: 0, Total: registry.UnknownTotal, Message: "writing Vault principals"},
		{Source: "vault", Stage: "write-entitlements", Current: 0, Total: registry.UnknownTotal, Message: "writing Vault policy and membership entitlements"},
		{Source: "vault", Stage: "write-assets", Current: 0, Total: registry.UnknownTotal, Message: "writing Vault mounts and auth role assets"},
	}
}

func (i *VaultIntegration) Run(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, report func(registry.Event), _ registry.RunMode) error {
	started := time.Now()
	slog.Info("syncing Vault", "source", i.sourceName)

	runID, err := q.CreateSyncRun(ctx, gen.CreateSyncRunParams{
		SourceKind: "vault",
		SourceName: i.sourceName,
	})
	if err != nil {
		return err
	}

	entities, err := i.client.ListEntities(ctx)
	if err != nil {
		report(registry.Event{Source: "vault", Stage: "list-entities", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}
	report(registry.Event{
		Source:  "vault",
		Stage:   "list-entities",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("found %d entities", len(entities)),
	})

	groups, err := i.client.ListGroups(ctx)
	if err != nil {
		report(registry.Event{Source: "vault", Stage: "list-groups", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}
	report(registry.Event{
		Source:  "vault",
		Stage:   "list-groups",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("found %d groups", len(groups)),
	})

	policies, err := i.client.ListACLPolicies(ctx)
	if err != nil {
		slog.Warn("vault ACL policy listing failed; continuing without policy inventory", "source", i.sourceName, "err", err)
		report(registry.Event{
			Source:  "vault",
			Stage:   "list-policies",
			Current: 1,
			Total:   1,
			Message: fmt.Sprintf("skipped ACL policy inventory: %v", err),
		})
		policies = nil
	} else {
		report(registry.Event{
			Source:  "vault",
			Stage:   "list-policies",
			Current: 1,
			Total:   1,
			Message: fmt.Sprintf("found %d ACL policies", len(policies)),
		})
	}

	authMounts, err := i.client.ListAuthMounts(ctx)
	if err != nil {
		report(registry.Event{Source: "vault", Stage: "list-mounts", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}
	secretsMounts, err := i.client.ListSecretsMounts(ctx)
	if err != nil {
		report(registry.Event{Source: "vault", Stage: "list-mounts", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
	}
	report(registry.Event{
		Source:  "vault",
		Stage:   "list-mounts",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("found %d auth mounts and %d secrets mounts", len(authMounts), len(secretsMounts)),
	})

	authRoles := make([]AuthRole, 0)
	if i.scanAuthRoles {
		authRoles, err = i.client.ListAuthRoles(ctx, authMounts)
		if err != nil {
			report(registry.Event{Source: "vault", Stage: "list-auth-roles", Message: err.Error(), Err: err})
			return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindAPI)
		}
	}
	report(registry.Event{
		Source:  "vault",
		Stage:   "list-auth-roles",
		Current: 1,
		Total:   1,
		Message: fmt.Sprintf("found %d auth roles", len(authRoles)),
	})

	accountRows := buildVaultAccountRows(entities, groups, authRoles)
	if err := upsertVaultAccounts(ctx, q, report, runID, i.sourceName, accountRows); err != nil {
		report(registry.Event{Source: "vault", Stage: "write-users", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	entitlementRows := buildVaultEntitlementRows(entities, groups, authRoles)
	if err := upsertVaultEntitlements(ctx, q, report, runID, i.sourceName, entitlementRows); err != nil {
		report(registry.Event{Source: "vault", Stage: "write-entitlements", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	assetRows := buildVaultAssetRows(authMounts, secretsMounts, authRoles)
	if err := upsertVaultAssets(ctx, q, report, runID, i.sourceName, assetRows); err != nil {
		report(registry.Event{Source: "vault", Stage: "write-assets", Message: err.Error(), Err: err})
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	if err := registry.FinalizeAppRun(ctx, q, pool, runID, "vault", i.sourceName, time.Since(started), false); err != nil {
		return registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
	}

	slog.Info(
		"vault sync complete",
		"source", i.sourceName,
		"entities", len(entities),
		"groups", len(groups),
		"policies", len(policies),
		"auth_mounts", len(authMounts),
		"secrets_mounts", len(secretsMounts),
		"auth_roles", len(authRoles),
		"accounts", len(accountRows),
		"entitlements", len(entitlementRows),
		"assets", len(assetRows),
	)
	return nil
}

func buildVaultAccountRows(entities []Entity, groups []Group, authRoles []AuthRole) []vaultAccountUpsertRow {
	byExternalID := make(map[string]vaultAccountUpsertRow, len(entities)+len(groups)+len(authRoles))

	for _, entity := range entities {
		entityID := strings.TrimSpace(entity.ID)
		if entityID == "" {
			continue
		}
		status := "active"
		if entity.Disabled {
			status = "disabled"
		}
		displayName := firstNonEmptyString(entity.Name, entityID)
		row := vaultAccountUpsertRow{
			ExternalID:  "entity:" + entityID,
			Email:       bestEntityEmail(entity),
			DisplayName: displayName,
			AccountKind: vaultEntityAccountKind(entity),
			Status:      status,
			RawJSON:     registry.WithEntityCategory(withAccountStatus(entity.RawJSON, status), registry.EntityCategoryEntity),
		}
		byExternalID[row.ExternalID] = row
	}

	for _, group := range groups {
		groupID := strings.TrimSpace(group.ID)
		groupExternalID := vaultGroupExternalID(groupID)
		if groupExternalID != "" {
			displayName := firstNonEmptyString(group.Name, groupID, groupExternalID)
			byExternalID[groupExternalID] = vaultAccountUpsertRow{
				ExternalID:  groupExternalID,
				DisplayName: displayName,
				AccountKind: registry.AccountKindService,
				Status:      "active",
				RawJSON:     registry.WithEntityCategory(withAccountStatus(group.RawJSON, "active"), registry.EntityCategoryGroup),
			}
		}

		for _, memberID := range group.MemberEntityIDs {
			memberID = strings.TrimSpace(memberID)
			if memberID == "" {
				continue
			}
			externalID := "entity:" + memberID
			if _, exists := byExternalID[externalID]; exists {
				continue
			}
			byExternalID[externalID] = vaultAccountUpsertRow{
				ExternalID:  externalID,
				DisplayName: memberID,
				AccountKind: registry.AccountKindUnknown,
				Status:      "active",
				RawJSON: registry.WithEntityCategory(registry.MarshalJSON(map[string]any{
					"id":     memberID,
					"status": "active",
					"source": "vault_group_membership",
				}), registry.EntityCategoryEntity),
			}
		}
	}

	for _, role := range authRoles {
		externalID := vaultRoleExternalID(role.AuthType, role.MountPath, role.Name)
		if externalID == "" {
			continue
		}
		row := vaultAccountUpsertRow{
			ExternalID:  externalID,
			DisplayName: firstNonEmptyString(role.Name, externalID),
			AccountKind: registry.AccountKindService,
			Status:      "active",
			RawJSON:     registry.WithEntityCategory(withAccountStatus(role.RawJSON, "active"), registry.EntityCategoryAuthRole),
		}
		byExternalID[row.ExternalID] = row
	}

	rows := make([]vaultAccountUpsertRow, 0, len(byExternalID))
	for _, row := range byExternalID {
		rows = append(rows, row)
	}
	return rows
}

func buildVaultEntitlementRows(entities []Entity, groups []Group, authRoles []AuthRole) []vaultEntitlementUpsertRow {
	seen := make(map[string]struct{})
	rows := make([]vaultEntitlementUpsertRow, 0)

	add := func(row vaultEntitlementUpsertRow) {
		row.AppUserExternalID = strings.TrimSpace(row.AppUserExternalID)
		row.Kind = strings.TrimSpace(row.Kind)
		row.Resource = strings.TrimSpace(row.Resource)
		row.Permission = strings.TrimSpace(row.Permission)
		if row.AppUserExternalID == "" || row.Kind == "" || row.Resource == "" || row.Permission == "" {
			return
		}
		key := row.AppUserExternalID + "::" + row.Kind + "::" + row.Resource + "::" + row.Permission
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		rows = append(rows, row)
	}

	for _, entity := range entities {
		entityID := strings.TrimSpace(entity.ID)
		if entityID == "" {
			continue
		}
		for _, policy := range entity.Policies {
			policy = strings.TrimSpace(policy)
			if policy == "" {
				continue
			}
			add(vaultEntitlementUpsertRow{
				AppUserExternalID: "entity:" + entityID,
				Kind:              "vault_entity_policy",
				Resource:          "vault_policy:" + policy,
				Permission:        "attached",
				RawJSON: registry.MarshalJSON(map[string]any{
					"entity_id": entityID,
					"policy":    policy,
				}),
			})
		}
	}

	for _, group := range groups {
		groupID := strings.TrimSpace(group.ID)
		groupRef := firstNonEmptyString(group.Name, groupID)
		if groupRef == "" {
			continue
		}
		groupResource := "vault_group:" + groupRef
		memberEntityIDs := dedupeStringSlice(group.MemberEntityIDs)
		for _, memberID := range memberEntityIDs {
			memberID = strings.TrimSpace(memberID)
			if memberID == "" {
				continue
			}
			add(vaultEntitlementUpsertRow{
				AppUserExternalID: "entity:" + memberID,
				Kind:              "vault_group_member",
				Resource:          groupResource,
				Permission:        "member",
				RawJSON: registry.MarshalJSON(map[string]any{
					"group_id":   groupID,
					"group_name": group.Name,
					"member_id":  memberID,
				}),
			})
			for _, policy := range dedupeStringSlice(group.Policies) {
				policy = strings.TrimSpace(policy)
				if policy == "" {
					continue
				}
				add(vaultEntitlementUpsertRow{
					AppUserExternalID: "entity:" + memberID,
					Kind:              "vault_group_policy",
					Resource:          "vault_policy:" + policy,
					Permission:        "attached",
					RawJSON: registry.MarshalJSON(map[string]any{
						"group_id":   groupID,
						"group_name": group.Name,
						"member_id":  memberID,
						"policy":     policy,
					}),
				})
			}
		}
	}

	for _, role := range authRoles {
		externalID := vaultRoleExternalID(role.AuthType, role.MountPath, role.Name)
		if externalID == "" {
			continue
		}
		for _, policy := range dedupeStringSlice(role.Policies) {
			policy = strings.TrimSpace(policy)
			if policy == "" {
				continue
			}
			add(vaultEntitlementUpsertRow{
				AppUserExternalID: externalID,
				Kind:              "vault_auth_role_policy",
				Resource:          "vault_policy:" + policy,
				Permission:        "attached",
				RawJSON: registry.MarshalJSON(map[string]any{
					"auth_type": role.AuthType,
					"mount":     role.MountPath,
					"role":      role.Name,
					"policy":    policy,
				}),
			})
		}
	}

	return rows
}

func buildVaultAssetRows(authMounts []AuthMount, secretsMounts []SecretsMount, authRoles []AuthRole) []vaultAssetUpsertRow {
	seen := make(map[string]struct{})
	rows := make([]vaultAssetUpsertRow, 0, len(authMounts)+len(secretsMounts)+len(authRoles))

	add := func(row vaultAssetUpsertRow) {
		row.AssetKind = strings.TrimSpace(row.AssetKind)
		row.ExternalID = strings.TrimSpace(row.ExternalID)
		row.ParentExternalID = strings.TrimSpace(row.ParentExternalID)
		row.DisplayName = strings.TrimSpace(row.DisplayName)
		row.Status = strings.TrimSpace(row.Status)
		if row.AssetKind == "" || row.ExternalID == "" {
			return
		}
		if row.DisplayName == "" {
			row.DisplayName = row.ExternalID
		}
		if row.Status == "" {
			row.Status = "active"
		}
		key := row.AssetKind + "::" + row.ExternalID
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		rows = append(rows, row)
	}

	for _, mount := range authMounts {
		add(vaultAssetUpsertRow{
			AssetKind:   "vault_auth_mount",
			ExternalID:  mount.Path,
			DisplayName: firstNonEmptyString(mount.Description, mount.Path),
			Status:      firstNonEmptyString(mount.Type, "active"),
			RawJSON:     mount.RawJSON,
		})
	}

	for _, mount := range secretsMounts {
		add(vaultAssetUpsertRow{
			AssetKind:   "vault_secrets_mount",
			ExternalID:  mount.Path,
			DisplayName: firstNonEmptyString(mount.Description, mount.Path),
			Status:      firstNonEmptyString(mount.Type, "active"),
			RawJSON:     mount.RawJSON,
		})
	}

	for _, role := range authRoles {
		externalID := vaultRoleExternalID(role.AuthType, role.MountPath, role.Name)
		if externalID == "" {
			continue
		}
		add(vaultAssetUpsertRow{
			AssetKind:        "vault_auth_role",
			ExternalID:       externalID,
			ParentExternalID: strings.TrimSpace(role.MountPath),
			DisplayName:      firstNonEmptyString(role.Name, externalID),
			Status:           firstNonEmptyString(role.AuthType, "active"),
			RawJSON:          role.RawJSON,
		})
	}

	return rows
}

func upsertVaultAccounts(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64, sourceName string, rows []vaultAccountUpsertRow) error {
	report(registry.Event{Source: "vault", Stage: "write-users", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d principals", len(rows))})
	if len(rows) == 0 {
		return nil
	}

	for start := 0; start < len(rows); start += vaultUserBatchSize {
		end := min(start+vaultUserBatchSize, len(rows))
		batch := rows[start:end]

		externalIDs := make([]string, 0, len(batch))
		emails := make([]string, 0, len(batch))
		displayNames := make([]string, 0, len(batch))
		accountKinds := make([]string, 0, len(batch))
		rawJSONs := make([][]byte, 0, len(batch))
		lastLoginAts := make([]pgtype.Timestamptz, 0, len(batch))
		lastLoginIPs := make([]string, 0, len(batch))
		lastLoginRegions := make([]string, 0, len(batch))

		for _, row := range batch {
			externalIDs = append(externalIDs, row.ExternalID)
			emails = append(emails, strings.ToLower(strings.TrimSpace(row.Email)))
			displayNames = append(displayNames, row.DisplayName)
			accountKinds = append(accountKinds, registry.NormalizeAccountKind(row.AccountKind))
			rawJSONs = append(rawJSONs, registry.NormalizeJSON(row.RawJSON))
			lastLoginAts = append(lastLoginAts, pgtype.Timestamptz{})
			lastLoginIPs = append(lastLoginIPs, "")
			lastLoginRegions = append(lastLoginRegions, "")
		}

		if _, err := q.UpsertAppUsersBulkBySource(ctx, gen.UpsertAppUsersBulkBySourceParams{
			SourceKind:       "vault",
			SourceName:       sourceName,
			SeenInRunID:      runID,
			ExternalIds:      externalIDs,
			Emails:           emails,
			DisplayNames:     displayNames,
			AccountKinds:     accountKinds,
			RawJsons:         rawJSONs,
			LastLoginAts:     lastLoginAts,
			LastLoginIps:     lastLoginIPs,
			LastLoginRegions: lastLoginRegions,
		}); err != nil {
			return err
		}

		report(registry.Event{
			Source:  "vault",
			Stage:   "write-users",
			Current: int64(end),
			Total:   int64(len(rows)),
			Message: fmt.Sprintf("principals %d/%d", end, len(rows)),
		})
	}

	return nil
}

func upsertVaultEntitlements(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64, sourceName string, rows []vaultEntitlementUpsertRow) error {
	report(registry.Event{Source: "vault", Stage: "write-entitlements", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d entitlements", len(rows))})
	if len(rows) == 0 {
		return nil
	}

	for start := 0; start < len(rows); start += vaultEntitlementBatchSize {
		end := min(start+vaultEntitlementBatchSize, len(rows))
		batch := rows[start:end]

		appUserExternalIDs := make([]string, 0, len(batch))
		kinds := make([]string, 0, len(batch))
		resources := make([]string, 0, len(batch))
		permissions := make([]string, 0, len(batch))
		rawJSONs := make([][]byte, 0, len(batch))

		for _, row := range batch {
			appUserExternalIDs = append(appUserExternalIDs, row.AppUserExternalID)
			kinds = append(kinds, row.Kind)
			resources = append(resources, row.Resource)
			permissions = append(permissions, row.Permission)
			rawJSONs = append(rawJSONs, registry.NormalizeJSON(row.RawJSON))
		}

		if _, err := q.UpsertEntitlementsBulkBySource(ctx, gen.UpsertEntitlementsBulkBySourceParams{
			SeenInRunID:        runID,
			SourceKind:         "vault",
			SourceName:         sourceName,
			AppUserExternalIds: appUserExternalIDs,
			Kinds:              kinds,
			Resources:          resources,
			Permissions:        permissions,
			RawJsons:           rawJSONs,
		}); err != nil {
			return err
		}

		report(registry.Event{
			Source:  "vault",
			Stage:   "write-entitlements",
			Current: int64(end),
			Total:   int64(len(rows)),
			Message: fmt.Sprintf("entitlements %d/%d", end, len(rows)),
		})
	}

	return nil
}

func upsertVaultAssets(ctx context.Context, q *gen.Queries, report func(registry.Event), runID int64, sourceName string, rows []vaultAssetUpsertRow) error {
	report(registry.Event{Source: "vault", Stage: "write-assets", Current: 0, Total: int64(len(rows)), Message: fmt.Sprintf("writing %d app assets", len(rows))})
	if len(rows) == 0 {
		return nil
	}

	for start := 0; start < len(rows); start += vaultAssetBatchSize {
		end := min(start+vaultAssetBatchSize, len(rows))
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
			createdAtSources = append(createdAtSources, pgtype.Timestamptz{})
			updatedAtSources = append(updatedAtSources, pgtype.Timestamptz{})
			rawJSONs = append(rawJSONs, registry.NormalizeJSON(row.RawJSON))
		}

		if _, err := q.UpsertAppAssetsBulkBySource(ctx, gen.UpsertAppAssetsBulkBySourceParams{
			SourceKind:        "vault",
			SourceName:        sourceName,
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

		report(registry.Event{
			Source:  "vault",
			Stage:   "write-assets",
			Current: int64(end),
			Total:   int64(len(rows)),
			Message: fmt.Sprintf("assets %d/%d", end, len(rows)),
		})
	}

	return nil
}

func bestEntityEmail(entity Entity) string {
	metadataCandidates := []string{
		entity.Metadata["email"],
		entity.Metadata["mail"],
		entity.Metadata["user_email"],
		entity.Metadata["upn"],
		entity.Metadata["user_principal_name"],
	}
	for _, candidate := range metadataCandidates {
		if email := normalizeEmail(candidate); email != "" {
			return email
		}
	}

	for _, alias := range entity.Aliases {
		aliasCandidates := []string{
			alias.Metadata["email"],
			alias.Metadata["mail"],
			alias.Metadata["upn"],
			alias.Name,
		}
		for _, candidate := range aliasCandidates {
			if email := normalizeEmail(candidate); email != "" {
				return email
			}
		}
	}
	return ""
}

func normalizeEmail(raw string) string {
	email := strings.ToLower(strings.TrimSpace(raw))
	if strings.Count(email, "@") != 1 {
		return ""
	}
	parts := strings.Split(email, "@")
	if len(parts) != 2 {
		return ""
	}
	if strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return ""
	}
	return email
}

func dedupeStringSlice(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func withAccountStatus(raw []byte, status string) []byte {
	status = strings.TrimSpace(status)
	if status == "" {
		status = "active"
	}
	payload := map[string]any{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &payload)
	}
	payload["status"] = status
	return registry.MarshalJSON(payload)
}

func vaultRoleExternalID(authType, mountPath, roleName string) string {
	authType = strings.TrimSpace(authType)
	mountPath = strings.Trim(strings.TrimSpace(mountPath), "/")
	roleName = strings.TrimSpace(roleName)
	if authType == "" || mountPath == "" || roleName == "" {
		return ""
	}
	return "role:" + authType + ":" + mountPath + ":" + roleName
}
