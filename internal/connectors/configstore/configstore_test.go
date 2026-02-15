package configstore

import "testing"

func TestVaultConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  VaultConfig
		wantErr bool
	}{
		{
			name: "token auth valid",
			config: VaultConfig{
				Address: "https://vault.example.com",
				Token:   "s.test",
			},
		},
		{
			name: "token auth missing token",
			config: VaultConfig{
				Address:  "https://vault.example.com",
				AuthType: VaultAuthTypeToken,
			},
			wantErr: true,
		},
		{
			name: "approle auth valid",
			config: VaultConfig{
				Address:         "https://vault.example.com",
				AuthType:        VaultAuthTypeAppRole,
				AppRoleRoleID:   "role-id",
				AppRoleSecretID: "secret-id",
			},
		},
		{
			name: "approle auth missing secret id",
			config: VaultConfig{
				Address:       "https://vault.example.com",
				AuthType:      VaultAuthTypeAppRole,
				AppRoleRoleID: "role-id",
			},
			wantErr: true,
		},
		{
			name: "invalid CA cert",
			config: VaultConfig{
				Address:      "https://vault.example.com",
				Token:        "s.test",
				TLSCACertPEM: "not-pem",
			},
			wantErr: true,
		},
		{
			name: "missing address",
			config: VaultConfig{
				Token: "s.test",
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := test.config.Validate()
			if test.wantErr && err == nil {
				t.Fatalf("Validate() error = nil, want error")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("Validate() error = %v, want nil", err)
			}
		})
	}
}

func TestMergeVaultConfig(t *testing.T) {
	t.Parallel()

	existing := VaultConfig{
		Address:          "https://vault.example.com",
		AuthType:         VaultAuthTypeToken,
		Token:            "s.old",
		ScanAuthRoles:    true,
		TLSCACertPEM:     "-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----",
		TLSSkipVerify:    false,
		AppRoleMountPath: "custom-approle",
		AppRoleRoleID:    "old-role",
		AppRoleSecretID:  "old-secret",
	}

	mergedToken := MergeVaultConfig(existing, VaultConfig{
		Address:       "vault.internal",
		AuthType:      VaultAuthTypeToken,
		Token:         "",
		ScanAuthRoles: false,
		TLSSkipVerify: true,
	})
	if mergedToken.Token != "s.old" {
		t.Fatalf("token should be preserved when update token is blank")
	}
	if mergedToken.Address != "vault.internal" {
		t.Fatalf("unexpected merged address = %q", mergedToken.Address)
	}
	if mergedToken.TLSCACertPEM == "" {
		t.Fatalf("existing CA cert should be preserved when update is blank")
	}
	if mergedToken.ScanAuthRoles {
		t.Fatalf("scan auth roles should reflect explicit false update")
	}
	if !mergedToken.TLSSkipVerify {
		t.Fatalf("tls skip verify should reflect update")
	}

	mergedAppRole := MergeVaultConfig(existing, VaultConfig{
		AuthType:        VaultAuthTypeAppRole,
		AppRoleRoleID:   "new-role",
		AppRoleSecretID: "",
	})
	if mergedAppRole.Token != "" {
		t.Fatalf("token should be cleared when switching to approle auth")
	}
	if mergedAppRole.AppRoleRoleID != "new-role" {
		t.Fatalf("expected new approle role id, got %q", mergedAppRole.AppRoleRoleID)
	}
	if mergedAppRole.AppRoleSecretID != "old-secret" {
		t.Fatalf("approle secret should be preserved when update is blank")
	}
	if mergedAppRole.AppRoleMountPath != "custom-approle" {
		t.Fatalf("expected approle mount path to be preserved, got %q", mergedAppRole.AppRoleMountPath)
	}

	mergedAppRoleCustomMount := MergeVaultConfig(existing, VaultConfig{
		AuthType:         VaultAuthTypeAppRole,
		AppRoleMountPath: "/platform/approle/",
		AppRoleRoleID:    "new-role",
		AppRoleSecretID:  "new-secret",
	})
	if mergedAppRoleCustomMount.AppRoleMountPath != "platform/approle" {
		t.Fatalf("expected normalized custom approle mount path, got %q", mergedAppRoleCustomMount.AppRoleMountPath)
	}
}

func TestDecodeVaultConfigDefaultsScanAuthRoles(t *testing.T) {
	t.Parallel()

	cfg, err := DecodeVaultConfig(nil)
	if err != nil {
		t.Fatalf("DecodeVaultConfig(nil) error = %v", err)
	}
	if !cfg.ScanAuthRoles {
		t.Fatalf("scan_auth_roles default should be true")
	}

	cfg, err = DecodeVaultConfig([]byte(`{"scan_auth_roles":false}`))
	if err != nil {
		t.Fatalf("DecodeVaultConfig(explicit false) error = %v", err)
	}
	if cfg.ScanAuthRoles {
		t.Fatalf("scan_auth_roles should respect explicit false")
	}
}
