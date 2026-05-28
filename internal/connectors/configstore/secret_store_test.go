package configstore

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestSecretCipherRoundTrip(t *testing.T) {
	t.Parallel()

	cipher, err := NewSecretCipher([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewSecretCipher() error = %v", err)
	}

	ciphertext, nonce, version, err := cipher.Encrypt(KindOkta, secretNameToken, "secret-token")
	if err != nil {
		t.Fatalf("Encrypt() error = %v", err)
	}

	plaintext, err := cipher.Decrypt(KindOkta, secretNameToken, ciphertext, nonce, version)
	if err != nil {
		t.Fatalf("Decrypt() error = %v", err)
	}
	if plaintext != "secret-token" {
		t.Fatalf("Decrypt() = %q, want %q", plaintext, "secret-token")
	}
}

func TestSecretCipherRejectsAADMismatch(t *testing.T) {
	t.Parallel()

	cipher, err := NewSecretCipher([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewSecretCipher() error = %v", err)
	}

	ciphertext, nonce, version, err := cipher.Encrypt(KindOkta, secretNameToken, "secret-token")
	if err != nil {
		t.Fatalf("Encrypt() error = %v", err)
	}

	if _, err := cipher.Decrypt(KindGitHub, secretNameToken, ciphertext, nonce, version); err == nil {
		t.Fatalf("Decrypt() expected AAD mismatch error")
	}
}

func TestSplitAndResolveConfigRoundTrip(t *testing.T) {
	t.Parallel()

	validServiceAccountJSON := `{"client_email":"svc@example.iam.gserviceaccount.com","private_key":"-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----","token_uri":"https://oauth2.googleapis.com/token"}`

	tests := []struct {
		name        string
		kind        string
		cfg         any
		wantSecrets SecretValues
	}{
		{
			name: "okta",
			kind: KindOkta,
			cfg: OktaConfig{
				Domain:             "acme.okta.com",
				Token:              "okta-token",
				DiscoveryEnabled:   true,
				EventInboxMode:     OktaEventInboxModeHybrid,
				EventHookEnabled:   true,
				EventHookSecret:    "event-hook-secret",
				EventBridgeEnabled: true,
				EventBridgeSecret:  "eventbridge-secret",
			},
			wantSecrets: SecretValues{
				secretNameToken:             "okta-token",
				secretNameEventHookSecret:   "event-hook-secret",
				secretNameEventBridgeSecret: "eventbridge-secret",
			},
		},
		{
			name: "github",
			kind: KindGitHub,
			cfg: GitHubConfig{
				Token:       "github-token",
				Org:         "acme",
				APIBase:     "https://api.github.com",
				Enterprise:  "enterprise",
				SCIMEnabled: true,
			},
			wantSecrets: SecretValues{secretNameToken: "github-token"},
		},
		{
			name: "datadog",
			kind: KindDatadog,
			cfg: DatadogConfig{
				APIKey: "api-key",
				AppKey: "app-key",
				Site:   "datadoghq.com",
			},
			wantSecrets: SecretValues{
				secretNameAPIKey: "api-key",
				secretNameAppKey: "app-key",
			},
		},
		{
			name: "aws access key",
			kind: KindAWSIdentityCenter,
			cfg: AWSIdentityCenterConfig{
				Region:          "eu-west-1",
				Name:            "prod",
				AuthType:        AWSIdentityCenterAuthTypeAccessKey,
				AccessKeyID:     "AKIAEXAMPLE",
				SecretAccessKey: "secret-access-key",
				SessionToken:    "session-token",
			},
			wantSecrets: SecretValues{
				secretNameSecretAccessKey: "secret-access-key",
				secretNameSessionToken:    "session-token",
			},
		},
		{
			name: "entra",
			kind: KindEntra,
			cfg: EntraConfig{
				TenantID:         "tenant-id",
				ClientID:         "client-id",
				ClientSecret:     "client-secret",
				DiscoveryEnabled: true,
			},
			wantSecrets: SecretValues{secretNameClientSecret: "client-secret"},
		},
		{
			name: "google workspace",
			kind: KindGoogleWorkspace,
			cfg: GoogleWorkspaceConfig{
				CustomerID:          "C012345",
				DelegatedAdminEmail: "admin@example.com",
				AuthType:            GoogleWorkspaceAuthTypeServiceAccountJSON,
				ServiceAccountJSON:  validServiceAccountJSON,
				DiscoveryEnabled:    true,
			},
			wantSecrets: SecretValues{secretNameServiceAccountJSON: validServiceAccountJSON},
		},
		{
			name: "vault approle",
			kind: KindVault,
			cfg: VaultConfig{
				Address:         "https://vault.example.com",
				AuthType:        VaultAuthTypeAppRole,
				AppRoleRoleID:   "role-id",
				AppRoleSecretID: "secret-id",
				ScanAuthRoles:   true,
			},
			wantSecrets: SecretValues{secretNameAppRoleSecretID: "secret-id"},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			publicCfg, secretValues, err := SplitConfig(tc.kind, tc.cfg)
			if err != nil {
				t.Fatalf("SplitConfig() error = %v", err)
			}
			if !reflect.DeepEqual(secretValues, tc.wantSecrets) {
				t.Fatalf("SplitConfig() secrets = %#v, want %#v", secretValues, tc.wantSecrets)
			}

			publicRaw, err := EncodeConfig(publicCfg)
			if err != nil {
				t.Fatalf("EncodeConfig() error = %v", err)
			}
			for _, secretValue := range tc.wantSecrets {
				if secretValue != "" && strings.Contains(string(publicRaw), secretValue) {
					t.Fatalf("public config unexpectedly contains secret value %q", secretValue)
				}
			}

			resolvedRaw, err := ResolveConfig(tc.kind, publicRaw, secretValues)
			if err != nil {
				t.Fatalf("ResolveConfig() error = %v", err)
			}

			got, err := decodeTypedConfig(tc.kind, resolvedRaw)
			if err != nil {
				t.Fatalf("decodeTypedConfig() error = %v", err)
			}
			want, err := decodeTypedConfig(tc.kind, mustEncodeConfig(t, tc.cfg))
			if err != nil {
				t.Fatalf("decodeTypedConfig(want) error = %v", err)
			}
			got = normalizeTypedConfig(tc.kind, got)
			want = normalizeTypedConfig(tc.kind, want)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("ResolveConfig() round-trip mismatch:\n got: %#v\nwant: %#v", got, want)
			}
		})
	}
}

func TestResolveConfigWithSecretPresenceBuildsValidGoogleWorkspaceConfig(t *testing.T) {
	t.Parallel()

	publicRaw, err := EncodeConfig(GoogleWorkspaceConfig{
		CustomerID:          "C012345",
		DelegatedAdminEmail: "admin@example.com",
		AuthType:            GoogleWorkspaceAuthTypeServiceAccountJSON,
	})
	if err != nil {
		t.Fatalf("EncodeConfig() error = %v", err)
	}

	resolvedRaw, err := ResolveConfigWithSecretPresence(KindGoogleWorkspace, publicRaw, map[string]bool{
		secretNameServiceAccountJSON: true,
	})
	if err != nil {
		t.Fatalf("ResolveConfigWithSecretPresence() error = %v", err)
	}

	cfg, err := DecodeGoogleWorkspaceConfig(resolvedRaw)
	if err != nil {
		t.Fatalf("DecodeGoogleWorkspaceConfig() error = %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func mustEncodeConfig(t *testing.T, cfg any) []byte {
	t.Helper()

	raw, err := EncodeConfig(cfg)
	if err != nil {
		t.Fatalf("EncodeConfig() error = %v", err)
	}
	return raw
}

func normalizeTypedConfig(kind string, cfg any) any {
	switch normalizeKind(kind) {
	case KindOkta:
		return cfg.(OktaConfig).Normalized()
	case KindGitHub:
		return cfg.(GitHubConfig).Normalized()
	case KindDatadog:
		return cfg.(DatadogConfig).Normalized()
	case KindAWSIdentityCenter:
		return cfg.(AWSIdentityCenterConfig).Normalized()
	case KindEntra:
		return cfg.(EntraConfig).Normalized()
	case KindGoogleWorkspace:
		return cfg.(GoogleWorkspaceConfig).Normalized()
	case KindVault:
		return cfg.(VaultConfig).Normalized()
	default:
		return cfg
	}
}

const demoConnectorSecretKeyBase64 = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="

func TestDemoSeedConnectorSecretsDecrypt(t *testing.T) {
	t.Parallel()

	key, err := base64.StdEncoding.DecodeString(demoConnectorSecretKeyBase64)
	if err != nil {
		t.Fatalf("DecodeString() error = %v", err)
	}
	cipher, err := NewSecretCipher(key)
	if err != nil {
		t.Fatalf("NewSecretCipher() error = %v", err)
	}

	tests := []struct {
		name      string
		path      string
		kind      string
		secret    string
		plaintext string
	}{
		{name: "okta token", path: "demo/data/001_seed_demo.sql", kind: KindOkta, secret: secretNameToken, plaintext: "demo_okta_token"},
		{name: "github token", path: "demo/data/001_seed_demo.sql", kind: KindGitHub, secret: secretNameToken, plaintext: "demo_github_token"},
		{name: "datadog api key", path: "demo/data/001_seed_demo.sql", kind: KindDatadog, secret: secretNameAPIKey, plaintext: "demo_datadog_api_key"},
		{name: "datadog app key", path: "demo/data/001_seed_demo.sql", kind: KindDatadog, secret: secretNameAppKey, plaintext: "demo_datadog_app_key"},
		{name: "entra client secret", path: "demo/data/002_seed_demo_expanded.sql", kind: KindEntra, secret: secretNameClientSecret, plaintext: "demo_entra_client_secret"},
		{name: "google workspace service account json", path: "demo/data/003_seed_demo_connectors_and_discovery.sql", kind: KindGoogleWorkspace, secret: secretNameServiceAccountJSON, plaintext: `{"client_email":"demo-open-sspm@demo-project.iam.gserviceaccount.com","private_key":"-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----","token_uri":"https://oauth2.googleapis.com/token"}`},
		{name: "vault token", path: "demo/data/003_seed_demo_connectors_and_discovery.sql", kind: KindVault, secret: secretNameToken, plaintext: "demo_vault_token"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ciphertext, nonce, version := loadDemoSeedConnectorSecretFixture(t, tc.path, tc.kind, tc.secret)
			plaintext, err := cipher.Decrypt(tc.kind, tc.secret, ciphertext, nonce, version)
			if err != nil {
				t.Fatalf("Decrypt() error = %v", err)
			}
			if plaintext != tc.plaintext {
				t.Fatalf("Decrypt() = %q, want %q", plaintext, tc.plaintext)
			}
		})
	}
}

func loadDemoSeedConnectorSecretFixture(t *testing.T, relPath, kind, secretName string) ([]byte, []byte, int16) {
	t.Helper()

	path := filepath.Join(repoRootFromCurrentFile(t), relPath)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", path, err)
	}

	pattern := fmt.Sprintf(
		`'%s'\s*,\s*'%s'\s*,\s*decode\('([0-9A-Fa-f]+)', 'hex'\)\s*,\s*decode\('([0-9A-Fa-f]+)', 'hex'\)\s*,\s*([0-9]+)\s*,`,
		regexp.QuoteMeta(kind),
		regexp.QuoteMeta(secretName),
	)
	match := regexp.MustCompile(pattern).FindSubmatch(raw)
	if match == nil {
		t.Fatalf("fixture %s/%s not found in %s", kind, secretName, relPath)
	}

	ciphertext, err := hex.DecodeString(string(match[1]))
	if err != nil {
		t.Fatalf("DecodeString(ciphertext) error = %v", err)
	}
	nonce, err := hex.DecodeString(string(match[2]))
	if err != nil {
		t.Fatalf("DecodeString(nonce) error = %v", err)
	}
	version64, err := strconv.ParseInt(string(match[3]), 10, 16)
	if err != nil {
		t.Fatalf("ParseInt(version) error = %v", err)
	}
	return ciphertext, nonce, int16(version64)
}

func repoRootFromCurrentFile(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}
