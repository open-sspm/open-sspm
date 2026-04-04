package handlers

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

func TestConnectorSecretBootstrapMigratesLegacyPlaintext(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, _ *Handlers) {
		raw := []byte(`{"domain":"acme.okta.com","token":"legacy-okta-token"}`)
		if _, err := q.UpdateConnectorConfig(ctx, gen.UpdateConnectorConfigParams{
			Kind:   configstore.KindOkta,
			Config: raw,
		}); err != nil {
			t.Fatalf("UpdateConnectorConfig() error = %v", err)
		}

		store := configstore.NewStore(pool, q, []byte(commandSearchTestConnectorSecretKey))
		if err := store.Bootstrap(ctx); err != nil {
			t.Fatalf("Bootstrap() error = %v", err)
		}

		row, err := q.GetConnectorConfig(ctx, configstore.KindOkta)
		if err != nil {
			t.Fatalf("GetConnectorConfig() error = %v", err)
		}
		if strings.Contains(string(row.Config), "legacy-okta-token") {
			t.Fatalf("public connector config still contains plaintext token: %s", row.Config)
		}

		secretRows, err := q.ListConnectorSecretsByKind(ctx, configstore.KindOkta)
		if err != nil {
			t.Fatalf("ListConnectorSecretsByKind() error = %v", err)
		}
		if len(secretRows) != 1 || secretRows[0].SecretName != "token" {
			t.Fatalf("secret rows = %#v, want one token secret", secretRows)
		}

		resolved, err := store.GetResolvedConnectorConfig(ctx, configstore.KindOkta)
		if err != nil {
			t.Fatalf("GetResolvedConnectorConfig() error = %v", err)
		}
		cfg, err := configstore.DecodeOktaConfig(resolved.ResolvedConfig)
		if err != nil {
			t.Fatalf("DecodeOktaConfig() error = %v", err)
		}
		if cfg.Token != "legacy-okta-token" {
			t.Fatalf("resolved token = %q, want %q", cfg.Token, "legacy-okta-token")
		}
	})
}

func TestConnectorSecretBootstrapFailsWithoutKeyForLegacySecrets(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, _ *Handlers) {
		raw := []byte(`{"domain":"acme.okta.com","token":"legacy-okta-token"}`)
		if _, err := q.UpdateConnectorConfig(ctx, gen.UpdateConnectorConfigParams{
			Kind:   configstore.KindOkta,
			Config: raw,
		}); err != nil {
			t.Fatalf("UpdateConnectorConfig() error = %v", err)
		}

		store := configstore.NewStore(pool, q, nil)
		err := store.Bootstrap(ctx)
		if !errors.Is(err, configstore.ErrConnectorSecretKeyRequired) {
			t.Fatalf("Bootstrap() error = %v, want ErrConnectorSecretKeyRequired", err)
		}
	})
}

func TestSaveConnectorConfigTxReplacesObsoleteVaultSecrets(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, _ *Handlers) {
		store := configstore.NewStore(pool, q, []byte(commandSearchTestConnectorSecretKey))

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin() error = %v", err)
		}
		qtx := q.WithTx(tx)
		if err := store.SaveConnectorConfigTx(ctx, qtx, configstore.KindVault, configstore.VaultConfig{
			Address:  "https://vault.example.com",
			AuthType: configstore.VaultAuthTypeToken,
			Token:    "vault-token",
		}); err != nil {
			t.Fatalf("SaveConnectorConfigTx(token) error = %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("Commit(token) error = %v", err)
		}

		tx, err = pool.Begin(ctx)
		if err != nil {
			t.Fatalf("Begin(approle) error = %v", err)
		}
		qtx = q.WithTx(tx)
		if err := store.SaveConnectorConfigTx(ctx, qtx, configstore.KindVault, configstore.VaultConfig{
			Address:         "https://vault.example.com",
			AuthType:        configstore.VaultAuthTypeAppRole,
			AppRoleRoleID:   "role-id",
			AppRoleSecretID: "approle-secret",
		}); err != nil {
			t.Fatalf("SaveConnectorConfigTx(approle) error = %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("Commit(approle) error = %v", err)
		}

		secretRows, err := q.ListConnectorSecretsByKind(ctx, configstore.KindVault)
		if err != nil {
			t.Fatalf("ListConnectorSecretsByKind() error = %v", err)
		}
		if len(secretRows) != 1 || secretRows[0].SecretName != "approle_secret_id" {
			t.Fatalf("secret rows = %#v, want one approle_secret_id secret", secretRows)
		}

		row, err := q.GetConnectorConfig(ctx, configstore.KindVault)
		if err != nil {
			t.Fatalf("GetConnectorConfig() error = %v", err)
		}
		if strings.Contains(string(row.Config), "vault-token") || strings.Contains(string(row.Config), "approle-secret") {
			t.Fatalf("public connector config still contains vault secret material: %s", row.Config)
		}

		resolved, err := store.GetResolvedConnectorConfig(ctx, configstore.KindVault)
		if err != nil {
			t.Fatalf("GetResolvedConnectorConfig() error = %v", err)
		}
		cfg, err := configstore.DecodeVaultConfig(resolved.ResolvedConfig)
		if err != nil {
			t.Fatalf("DecodeVaultConfig() error = %v", err)
		}
		if cfg.Token != "" {
			t.Fatalf("resolved token = %q, want blank", cfg.Token)
		}
		if cfg.AppRoleSecretID != "approle-secret" {
			t.Fatalf("resolved AppRoleSecretID = %q, want %q", cfg.AppRoleSecretID, "approle-secret")
		}
	})
}

func TestLoadConnectorSnapshotToleratesBrokenConnectorSecret(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{
			Org:   "acme",
			Token: "github-token",
		})
		upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindDatadog, true, configstore.DatadogConfig{
			Site:   "datadoghq.com",
			APIKey: "demo-api-key",
			AppKey: "demo-app-key",
		})

		if _, err := q.UpsertConnectorSecret(ctx, gen.UpsertConnectorSecretParams{
			Kind:       configstore.KindDatadog,
			SecretName: "app_key",
			Ciphertext: []byte{0x00},
			Nonce:      []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b},
			Version:    configstore.ConnectorSecretVersionAES256GCM,
		}); err != nil {
			t.Fatalf("UpsertConnectorSecret() error = %v", err)
		}

		states, err := h.Registry.LoadStates(ctx, h.Q)
		if err != nil {
			t.Fatalf("LoadStates() error = %v", err)
		}

		byKind := make(map[string]bool, len(states))
		for _, state := range states {
			byKind[state.Definition.Kind()] = true
			switch state.Definition.Kind() {
			case configstore.KindGitHub:
				if !state.Enabled || !state.Configured || state.ConfigError != "" {
					t.Fatalf("github state = %#v", state)
				}
				if state.SourceName != "acme" {
					t.Fatalf("github source name = %q, want %q", state.SourceName, "acme")
				}
			case configstore.KindDatadog:
				if !state.Enabled {
					t.Fatalf("datadog state should remain enabled: %#v", state)
				}
				if state.Configured {
					t.Fatalf("datadog state should not be configured when secret resolution fails: %#v", state)
				}
				if !strings.Contains(state.ConfigError, "decrypt connector secret datadog.app_key") {
					t.Fatalf("datadog ConfigError = %q", state.ConfigError)
				}
			}
		}
		if !byKind[configstore.KindGitHub] || !byKind[configstore.KindDatadog] {
			t.Fatalf("LoadStates() missing expected connectors: %#v", byKind)
		}

		snap, err := h.LoadConnectorSnapshot(ctx)
		if err != nil {
			t.Fatalf("LoadConnectorSnapshot() error = %v", err)
		}
		if !snap.GitHubConfigured || snap.GitHub.Org != "acme" {
			t.Fatalf("GitHub snapshot = %#v", snap.GitHub)
		}
		if snap.DatadogConfigured {
			t.Fatalf("Datadog snapshot should remain unconfigured: %#v", snap.Datadog)
		}
	})
}
