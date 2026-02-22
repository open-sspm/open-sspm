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

func TestGoogleWorkspaceConfigValidate(t *testing.T) {
	t.Parallel()

	validJSON := `{"client_email":"svc@example.iam.gserviceaccount.com","private_key":"-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----","token_uri":"https://oauth2.googleapis.com/token"}`

	tests := []struct {
		name    string
		cfg     GoogleWorkspaceConfig
		wantErr bool
	}{
		{
			name: "service account json auth valid",
			cfg: GoogleWorkspaceConfig{
				CustomerID:          "C012345",
				DelegatedAdminEmail: "admin@example.com",
				AuthType:            GoogleWorkspaceAuthTypeServiceAccountJSON,
				ServiceAccountJSON:  validJSON,
			},
		},
		{
			name: "adc auth valid",
			cfg: GoogleWorkspaceConfig{
				CustomerID:          "C012345",
				DelegatedAdminEmail: "admin@example.com",
				AuthType:            GoogleWorkspaceAuthTypeADC,
				ServiceAccountEmail: "svc@example.iam.gserviceaccount.com",
			},
		},
		{
			name: "missing customer id",
			cfg: GoogleWorkspaceConfig{
				DelegatedAdminEmail: "admin@example.com",
				AuthType:            GoogleWorkspaceAuthTypeADC,
				ServiceAccountEmail: "svc@example.iam.gserviceaccount.com",
			},
			wantErr: true,
		},
		{
			name: "missing delegated admin",
			cfg: GoogleWorkspaceConfig{
				CustomerID:         "C012345",
				AuthType:           GoogleWorkspaceAuthTypeServiceAccountJSON,
				ServiceAccountJSON: validJSON,
			},
			wantErr: true,
		},
		{
			name: "invalid json auth payload",
			cfg: GoogleWorkspaceConfig{
				CustomerID:          "C012345",
				DelegatedAdminEmail: "admin@example.com",
				AuthType:            GoogleWorkspaceAuthTypeServiceAccountJSON,
				ServiceAccountJSON:  `{"client_email":"missing-private-key"}`,
			},
			wantErr: true,
		},
		{
			name: "adc missing service account email",
			cfg: GoogleWorkspaceConfig{
				CustomerID:          "C012345",
				DelegatedAdminEmail: "admin@example.com",
				AuthType:            GoogleWorkspaceAuthTypeADC,
			},
			wantErr: true,
		},
		{
			name: "invalid auth type",
			cfg: GoogleWorkspaceConfig{
				CustomerID:          "C012345",
				DelegatedAdminEmail: "admin@example.com",
				AuthType:            "invalid",
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.cfg.Validate()
			if tc.wantErr && err == nil {
				t.Fatalf("Validate() error = nil, want error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("Validate() error = %v, want nil", err)
			}
		})
	}
}

func TestMergeGoogleWorkspaceConfig(t *testing.T) {
	t.Parallel()

	existing := GoogleWorkspaceConfig{
		CustomerID:          "C012345",
		PrimaryDomain:       "example.com",
		DelegatedAdminEmail: "admin@example.com",
		AuthType:            GoogleWorkspaceAuthTypeServiceAccountJSON,
		ServiceAccountJSON:  `{"client_email":"svc@example.iam.gserviceaccount.com","private_key":"key"}`,
		DiscoveryEnabled:    true,
	}

	t.Run("service account auth preserves secret on blank update", func(t *testing.T) {
		t.Parallel()
		merged := MergeGoogleWorkspaceConfig(existing, GoogleWorkspaceConfig{
			CustomerID:          "C999999",
			DelegatedAdminEmail: "admin2@example.com",
			AuthType:            GoogleWorkspaceAuthTypeServiceAccountJSON,
			ServiceAccountJSON:  "",
			DiscoveryEnabled:    false,
		})

		if merged.CustomerID != "C999999" {
			t.Fatalf("customer id = %q, want C999999", merged.CustomerID)
		}
		if merged.ServiceAccountJSON == "" {
			t.Fatalf("service account json should be preserved when update is blank")
		}
		if merged.ServiceAccountEmail != "" {
			t.Fatalf("service account email should be empty for service account json auth")
		}
		if merged.DiscoveryEnabled {
			t.Fatalf("discovery enabled should reflect explicit false update")
		}
	})

	t.Run("switching to adc clears json secret", func(t *testing.T) {
		t.Parallel()
		merged := MergeGoogleWorkspaceConfig(existing, GoogleWorkspaceConfig{
			AuthType:            GoogleWorkspaceAuthTypeADC,
			ServiceAccountEmail: "adc-svc@example.iam.gserviceaccount.com",
		})

		if merged.AuthType != GoogleWorkspaceAuthTypeADC {
			t.Fatalf("auth type = %q, want %q", merged.AuthType, GoogleWorkspaceAuthTypeADC)
		}
		if merged.ServiceAccountJSON != "" {
			t.Fatalf("service account json should be cleared for adc auth")
		}
		if merged.ServiceAccountEmail != "adc-svc@example.iam.gserviceaccount.com" {
			t.Fatalf("service account email = %q, want adc value", merged.ServiceAccountEmail)
		}
	})
}
