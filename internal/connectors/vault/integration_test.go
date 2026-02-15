package vault

import (
	"encoding/json"
	"testing"
)

func TestBuildVaultAccountRows(t *testing.T) {
	t.Parallel()

	entities := []Entity{
		{
			ID:       "e-1",
			Name:     "Entity One",
			Policies: []string{"default"},
			RawJSON:  []byte(`{"id":"e-1"}`),
		},
	}
	groups := []Group{
		{
			ID:              "g-1",
			Name:            "Team",
			MemberEntityIDs: []string{"e-1", "e-2"},
		},
	}
	authRoles := []AuthRole{
		{
			Name:      "ci-role",
			MountPath: "approle",
			AuthType:  "approle",
			RawJSON:   []byte(`{"role":"ci-role"}`),
		},
	}

	rows := buildVaultAccountRows(entities, groups, authRoles)
	if len(rows) != 3 {
		t.Fatalf("expected 3 account rows, got %d", len(rows))
	}

	byExternalID := make(map[string]vaultAccountUpsertRow, len(rows))
	for _, row := range rows {
		byExternalID[row.ExternalID] = row
	}

	if _, ok := byExternalID["entity:e-1"]; !ok {
		t.Fatalf("expected entity:e-1 account row")
	}
	if _, ok := byExternalID["entity:e-2"]; !ok {
		t.Fatalf("expected placeholder entity:e-2 account row from group membership")
	}
	if _, ok := byExternalID["approle:approle:ci-role"]; !ok {
		t.Fatalf("expected auth role principal row")
	}
}

func TestBuildVaultEntitlementRows(t *testing.T) {
	t.Parallel()

	entities := []Entity{
		{ID: "e-1", Policies: []string{"default", "default"}},
	}
	groups := []Group{
		{
			ID:              "g-1",
			Name:            "Team",
			Policies:        []string{"eng"},
			MemberEntityIDs: []string{"e-1"},
		},
	}
	authRoles := []AuthRole{
		{
			Name:      "ci-role",
			MountPath: "approle",
			AuthType:  "approle",
			Policies:  []string{"ci"},
		},
	}

	rows := buildVaultEntitlementRows(entities, groups, authRoles)
	if len(rows) != 4 {
		t.Fatalf("expected 4 entitlement rows, got %d", len(rows))
	}

	seenKinds := make(map[string]bool)
	for _, row := range rows {
		seenKinds[row.Kind] = true
		if row.RawJSON == nil || len(row.RawJSON) == 0 {
			t.Fatalf("expected raw json payload for row kind=%s", row.Kind)
		}
	}

	expectedKinds := []string{
		"vault_entity_policy",
		"vault_group_member",
		"vault_group_policy",
		"vault_auth_role_policy",
	}
	for _, kind := range expectedKinds {
		if !seenKinds[kind] {
			t.Fatalf("missing entitlement kind %s", kind)
		}
	}
}

func TestBuildVaultAssetRows(t *testing.T) {
	t.Parallel()

	authMounts := []AuthMount{
		{Path: "approle", Type: "approle", Description: "AppRole", RawJSON: []byte(`{"path":"approle"}`)},
	}
	secretMounts := []SecretsMount{
		{Path: "kv", Type: "kv", Description: "KV", RawJSON: []byte(`{"path":"kv"}`)},
	}
	authRoles := []AuthRole{
		{
			Name:      "ci-role",
			MountPath: "approle",
			AuthType:  "approle",
			RawJSON:   []byte(`{"role":"ci-role"}`),
		},
	}

	rows := buildVaultAssetRows(authMounts, secretMounts, authRoles)
	if len(rows) != 3 {
		t.Fatalf("expected 3 asset rows, got %d", len(rows))
	}

	byKind := make(map[string]int)
	for _, row := range rows {
		byKind[row.AssetKind]++
		if !json.Valid(row.RawJSON) {
			t.Fatalf("raw json is invalid for asset kind=%s", row.AssetKind)
		}
	}
	if byKind["vault_auth_mount"] != 1 {
		t.Fatalf("expected one vault_auth_mount row")
	}
	if byKind["vault_secrets_mount"] != 1 {
		t.Fatalf("expected one vault_secrets_mount row")
	}
	if byKind["vault_auth_role"] != 1 {
		t.Fatalf("expected one vault_auth_role row")
	}
}
