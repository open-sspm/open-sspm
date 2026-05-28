package configstore

import "testing"

func TestOktaConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  OktaConfig
		wantErr bool
	}{
		{
			name: "polling valid",
			config: OktaConfig{
				Domain: "acme.okta.com",
				Token:  "token",
			},
		},
		{
			name: "event hook push only valid",
			config: OktaConfig{
				Domain:           "acme.okta.com",
				EventInboxMode:   OktaEventInboxModeEventHook,
				EventHookEnabled: true,
				EventHookSecret:  "hook-secret",
			},
		},
		{
			name: "eventbridge push only valid",
			config: OktaConfig{
				Domain:             "acme.okta.com",
				EventInboxMode:     OktaEventInboxModeEventBridge,
				EventBridgeEnabled: true,
				EventBridgeSecret:  "eventbridge-secret",
			},
		},
		{
			name: "hybrid valid",
			config: OktaConfig{
				Domain:           "acme.okta.com",
				Token:            "token",
				EventInboxMode:   OktaEventInboxModeHybrid,
				EventHookEnabled: true,
				EventHookSecret:  "hook-secret",
			},
		},
		{
			name: "missing domain",
			config: OktaConfig{
				Token: "token",
			},
			wantErr: true,
		},
		{
			name: "polling missing token",
			config: OktaConfig{
				Domain: "acme.okta.com",
			},
			wantErr: true,
		},
		{
			name: "enabled event hook missing secret",
			config: OktaConfig{
				Domain:           "acme.okta.com",
				Token:            "token",
				EventHookEnabled: true,
			},
			wantErr: true,
		},
		{
			name: "event hook missing secret",
			config: OktaConfig{
				Domain:           "acme.okta.com",
				EventInboxMode:   OktaEventInboxModeEventHook,
				EventHookEnabled: true,
			},
			wantErr: true,
		},
		{
			name: "event hook receiver disabled",
			config: OktaConfig{
				Domain:          "acme.okta.com",
				EventInboxMode:  OktaEventInboxModeEventHook,
				EventHookSecret: "hook-secret",
			},
			wantErr: true,
		},
		{
			name: "hybrid missing push channel",
			config: OktaConfig{
				Domain:         "acme.okta.com",
				Token:          "token",
				EventInboxMode: OktaEventInboxModeHybrid,
			},
			wantErr: true,
		},
		{
			name: "invalid mode",
			config: OktaConfig{
				Domain:         "acme.okta.com",
				Token:          "token",
				EventInboxMode: "fast",
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

func TestNormalizeOktaDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want string
	}{
		{in: "Acme.Okta.Com", want: "acme.okta.com"},
		{in: "https://Acme.Okta.Com/", want: "acme.okta.com"},
		{in: "https://acme.okta.com/oauth2/default", want: "acme.okta.com"},
		{in: "acme.oktapreview.com/", want: "acme.oktapreview.com"},
	}
	for _, tc := range tests {
		if got := NormalizeOktaDomain(tc.in); got != tc.want {
			t.Fatalf("NormalizeOktaDomain(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestMergeOktaConfig(t *testing.T) {
	t.Parallel()

	existing := OktaConfig{
		Domain:             "old.okta.com",
		Token:              "old-token",
		DiscoveryEnabled:   true,
		EventInboxMode:     OktaEventInboxModeHybrid,
		EventHookEnabled:   true,
		EventHookSecret:    "old-hook",
		EventBridgeEnabled: true,
		EventBridgeSecret:  "old-eventbridge",
	}

	merged := MergeOktaConfig(existing, OktaConfig{
		Domain:             "new.okta.com",
		DiscoveryEnabled:   false,
		EventInboxMode:     OktaEventInboxModeEventHook,
		EventHookEnabled:   true,
		EventBridgeEnabled: false,
	})

	if merged.Domain != "new.okta.com" {
		t.Fatalf("Domain = %q, want new.okta.com", merged.Domain)
	}
	if merged.Token != "old-token" {
		t.Fatalf("Token = %q, want preserved old token", merged.Token)
	}
	if merged.EventHookSecret != "old-hook" {
		t.Fatalf("EventHookSecret = %q, want preserved old hook secret", merged.EventHookSecret)
	}
	if merged.EventBridgeSecret != "old-eventbridge" {
		t.Fatalf("EventBridgeSecret = %q, want preserved old EventBridge secret", merged.EventBridgeSecret)
	}
	if merged.DiscoveryEnabled {
		t.Fatalf("DiscoveryEnabled = true, want false")
	}
	if merged.EventInboxMode != OktaEventInboxModeEventHook {
		t.Fatalf("EventInboxMode = %q, want %q", merged.EventInboxMode, OktaEventInboxModeEventHook)
	}
	if !merged.EventHookEnabled || merged.EventBridgeEnabled {
		t.Fatalf("push flags = hook:%v eventbridge:%v, want hook true eventbridge false", merged.EventHookEnabled, merged.EventBridgeEnabled)
	}
}

func TestMergeOktaConfigPreservesEventInboxModeWhenOmitted(t *testing.T) {
	t.Parallel()

	existing := OktaConfig{
		Domain:           "old.okta.com",
		Token:            "old-token",
		DiscoveryEnabled: true,
		EventInboxMode:   OktaEventInboxModeHybrid,
		EventHookEnabled: true,
		EventHookSecret:  "old-hook",
	}

	merged := MergeOktaConfig(existing, OktaConfig{
		Domain:           "new.okta.com",
		DiscoveryEnabled: true,
		EventHookEnabled: true,
	})

	if merged.EventInboxMode != OktaEventInboxModeHybrid {
		t.Fatalf("EventInboxMode = %q, want preserved hybrid", merged.EventInboxMode)
	}
}

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
