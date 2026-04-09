package datadog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	datadogsdk "github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	datadogv2 "github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestMapDatadogAccountUsesFallbacksAndDetectsServiceAccounts(t *testing.T) {
	t.Parallel()

	lastLogin := time.Date(2026, 4, 8, 10, 30, 0, 0, time.UTC)

	user := datadogv2.NewUser()
	user.SetId("u-1")
	userAttrs := datadogv2.NewUserAttributes()
	userAttrs.SetName("Alice Example")
	userAttrs.SetDisabled(false)
	userAttrs.SetLastLoginTime(lastLogin)
	user.SetAttributes(*userAttrs)

	mappedUser, err := mapDatadogAccount(*user)
	if err != nil {
		t.Fatalf("mapDatadogAccount(user): %v", err)
	}
	if mappedUser.ExternalID != "u-1" {
		t.Fatalf("user external id = %q, want %q", mappedUser.ExternalID, "u-1")
	}
	if mappedUser.DisplayName != "Alice Example" {
		t.Fatalf("user display name = %q, want %q", mappedUser.DisplayName, "Alice Example")
	}
	if mappedUser.Status != "Active" {
		t.Fatalf("user status = %q, want %q", mappedUser.Status, "Active")
	}
	if mappedUser.AccountKind != registry.AccountKindHuman {
		t.Fatalf("user account kind = %q, want %q", mappedUser.AccountKind, registry.AccountKindHuman)
	}
	if mappedUser.EntityCategory != registry.EntityCategoryUser {
		t.Fatalf("user entity category = %q, want %q", mappedUser.EntityCategory, registry.EntityCategoryUser)
	}
	if mappedUser.LastLoginAt == nil || !mappedUser.LastLoginAt.Equal(lastLogin) {
		t.Fatalf("user last login = %v, want %v", mappedUser.LastLoginAt, lastLogin)
	}

	serviceAccount := datadogv2.NewUser()
	serviceAccount.SetId("sa-1")
	serviceAttrs := datadogv2.NewUserAttributes()
	serviceAttrs.SetServiceAccount(true)
	serviceAttrs.SetHandle("robot@example.com")
	serviceAttrs.SetDisabled(true)
	serviceAccount.SetAttributes(*serviceAttrs)

	mappedServiceAccount, err := mapDatadogAccount(*serviceAccount)
	if err != nil {
		t.Fatalf("mapDatadogAccount(service account): %v", err)
	}
	if mappedServiceAccount.ExternalID != "service_account:sa-1" {
		t.Fatalf("service external id = %q, want %q", mappedServiceAccount.ExternalID, "service_account:sa-1")
	}
	if mappedServiceAccount.Email != "robot@example.com" {
		t.Fatalf("service email = %q, want %q", mappedServiceAccount.Email, "robot@example.com")
	}
	if mappedServiceAccount.DisplayName != "robot@example.com" {
		t.Fatalf("service display name = %q, want %q", mappedServiceAccount.DisplayName, "robot@example.com")
	}
	if mappedServiceAccount.Status != "Inactive" {
		t.Fatalf("service status = %q, want %q", mappedServiceAccount.Status, "Inactive")
	}
	if mappedServiceAccount.AccountKind != registry.AccountKindService {
		t.Fatalf("service account kind = %q, want %q", mappedServiceAccount.AccountKind, registry.AccountKindService)
	}
	if mappedServiceAccount.EntityCategory != registry.EntityCategoryServiceAccount {
		t.Fatalf("service entity category = %q, want %q", mappedServiceAccount.EntityCategory, registry.EntityCategoryServiceAccount)
	}
}

func TestNewSDKAdapterConfiguresHTTPTimeoutAndRetry(t *testing.T) {
	t.Parallel()

	adapter, err := newSDKAdapter(configstore.DatadogConfig{
		APIKey: "api-key",
		AppKey: "app-key",
		Site:   "datadoghq.com",
	})
	if err != nil {
		t.Fatalf("newSDKAdapter(): %v", err)
	}
	if adapter.client == nil || adapter.client.Cfg == nil {
		t.Fatal("expected SDK client configuration")
	}
	if adapter.client.Cfg.HTTPClient == nil {
		t.Fatal("expected configured HTTP client")
	}
	if got := adapter.client.Cfg.HTTPClient.Timeout; got != datadogHTTPTimeout {
		t.Fatalf("HTTPClient.Timeout = %v, want %v", got, datadogHTTPTimeout)
	}
	if !adapter.client.Cfg.RetryConfiguration.EnableRetry {
		t.Fatal("RetryConfiguration.EnableRetry = false, want true")
	}
	if got := adapter.client.Cfg.RetryConfiguration.MaxRetries; got != datadogMaxRetries {
		t.Fatalf("RetryConfiguration.MaxRetries = %d, want %d", got, datadogMaxRetries)
	}
	if got := adapter.client.Cfg.RetryConfiguration.HTTPRetryTimeout; got != datadogHTTPTimeout {
		t.Fatalf("RetryConfiguration.HTTPRetryTimeout = %v, want %v", got, datadogHTTPTimeout)
	}
}

func TestSDKAdapterAPIErrorsIncludeBodyMessageAndResponseDetails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		prefix string
		call   func(context.Context, *sdkAdapter) error
	}{
		{
			name:   "accounts",
			path:   "/api/v2/users",
			prefix: "list datadog accounts",
			call: func(ctx context.Context, adapter *sdkAdapter) error {
				_, err := adapter.ListAccounts(ctx)
				return err
			},
		},
		{
			name:   "roles",
			path:   "/api/v2/roles",
			prefix: "list datadog roles",
			call: func(ctx context.Context, adapter *sdkAdapter) error {
				_, err := adapter.ListRoles(ctx)
				return err
			},
		},
		{
			name:   "role members",
			path:   "/api/v2/roles/role-1/users",
			prefix: "list datadog role members",
			call: func(ctx context.Context, adapter *sdkAdapter) error {
				_, err := adapter.ListRoleMembers(ctx, "role-1")
				return err
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			adapter := newTestSDKAdapter(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != tc.path {
					t.Fatalf("path = %q, want %q", r.URL.Path, tc.path)
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Request-Id", "req-123")
				w.Header().Set("X-RateLimit-Remaining", "9")
				w.Header().Set("X-RateLimit-Limit", "10")
				w.Header().Set("X-RateLimit-Reset", "30")
				w.Header().Set("Retry-After", "5")
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"errors":["invalid API key"]}`))
			})

			err := tc.call(context.Background(), adapter)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tc.prefix) {
				t.Fatalf("error %q missing prefix %q", err.Error(), tc.prefix)
			}
			if !strings.Contains(err.Error(), "403 Forbidden") {
				t.Fatalf("error %q missing HTTP status", err.Error())
			}
			if !strings.Contains(err.Error(), "invalid API key") {
				t.Fatalf("error %q missing Datadog body message", err.Error())
			}
			if !strings.Contains(err.Error(), "request_id=req-123") {
				t.Fatalf("error %q missing request id details", err.Error())
			}
			if !strings.Contains(err.Error(), "rate_remaining=9") {
				t.Fatalf("error %q missing rate remaining details", err.Error())
			}
			if !strings.Contains(err.Error(), "rate_limit=10") {
				t.Fatalf("error %q missing rate limit details", err.Error())
			}
			if !strings.Contains(err.Error(), "rate_reset=30") {
				t.Fatalf("error %q missing rate reset details", err.Error())
			}
			if !strings.Contains(err.Error(), "retry_after=5") {
				t.Fatalf("error %q missing retry-after details", err.Error())
			}
			if !strings.Contains(err.Error(), "url=http://") || !strings.Contains(err.Error(), tc.path) {
				t.Fatalf("error %q missing request URL details for %q", err.Error(), tc.path)
			}
		})
	}
}

func TestDatadogAPIErrorMessageTruncatesPlainTextFallback(t *testing.T) {
	t.Parallel()

	message := datadogAPIErrorMessage([]byte(strings.Repeat("x", 350)))
	if len(message) != 303 {
		t.Fatalf("len(message) = %d, want 303", len(message))
	}
	if !strings.HasSuffix(message, "...") {
		t.Fatalf("message = %q, want truncated suffix", message)
	}
}

func TestDatadogAPIErrorMessageDropsHTMLFallback(t *testing.T) {
	t.Parallel()

	if message := datadogAPIErrorMessage([]byte("<html><body>gateway error</body></html>")); message != "" {
		t.Fatalf("message = %q, want empty string for HTML fallback", message)
	}
}

func TestSDKAdapterListAccounts(t *testing.T) {
	t.Parallel()

	var calls int
	adapter := newTestSDKAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("DD-API-KEY") != "api-key" {
			t.Fatalf("DD-API-KEY = %q, want api-key", r.Header.Get("DD-API-KEY"))
		}
		if r.Header.Get("DD-APPLICATION-KEY") != "app-key" {
			t.Fatalf("DD-APPLICATION-KEY = %q, want app-key", r.Header.Get("DD-APPLICATION-KEY"))
		}
		if r.URL.Path != "/api/v2/users" {
			t.Fatalf("path = %q, want /api/v2/users", r.URL.Path)
		}
		if got := r.URL.Query().Get("page[number]"); got != "0" {
			t.Fatalf("page[number] = %q, want 0", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[` +
			`{"id":"u-1","attributes":{"handle":"alice@example.com","status":"Active"}},` +
			`{"id":"sa-1","attributes":{"service_account":true,"email":"ci@example.com","name":"CI Service Account","status":"Active"}}` +
			`]}`))
	})

	accounts, err := adapter.ListAccounts(context.Background())
	if err != nil {
		t.Fatalf("ListAccounts(): %v", err)
	}
	if len(accounts) != 2 {
		t.Fatalf("len(accounts) = %d, want 2", len(accounts))
	}
	if calls != 1 {
		t.Fatalf("requests = %d, want 1", calls)
	}
	if accounts[0].ExternalID != "u-1" || accounts[0].EntityCategory != registry.EntityCategoryUser {
		t.Fatalf("unexpected user account: %#v", accounts[0])
	}
	if accounts[1].ExternalID != "service_account:sa-1" || accounts[1].EntityCategory != registry.EntityCategoryServiceAccount {
		t.Fatalf("unexpected service account: %#v", accounts[1])
	}
}

func TestSDKAdapterListAccountsPaginatesUntilShortPage(t *testing.T) {
	t.Parallel()

	var calls int
	adapter := newTestSDKAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/api/v2/users" {
			t.Fatalf("path = %q, want /api/v2/users", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		switch got := r.URL.Query().Get("page[number]"); got {
		case "0":
			_, _ = w.Write([]byte(datadogUsersResponseJSON(0, datadogPageSize)))
		case "1":
			_, _ = w.Write([]byte(datadogUsersResponseJSON(datadogPageSize, 1)))
		default:
			t.Fatalf("page[number] = %q, want 0 or 1", got)
		}
	})

	accounts, err := adapter.ListAccounts(context.Background())
	if err != nil {
		t.Fatalf("ListAccounts(): %v", err)
	}
	if len(accounts) != datadogPageSize+1 {
		t.Fatalf("len(accounts) = %d, want %d", len(accounts), datadogPageSize+1)
	}
	if calls != 2 {
		t.Fatalf("requests = %d, want 2", calls)
	}
	if accounts[0].ExternalID != "u-0" {
		t.Fatalf("accounts[0].ExternalID = %q, want %q", accounts[0].ExternalID, "u-0")
	}
	if accounts[len(accounts)-1].ExternalID != "u-100" {
		t.Fatalf("last external id = %q, want %q", accounts[len(accounts)-1].ExternalID, "u-100")
	}
}

func TestSDKAdapterListRoles(t *testing.T) {
	t.Parallel()

	adapter := newTestSDKAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v2/roles" {
			t.Fatalf("path = %q, want /api/v2/roles", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		switch got := r.URL.Query().Get("page[number]"); got {
		case "0":
			_, _ = w.Write([]byte(`{"data":[{"id":"role-1","type":"roles","attributes":{"name":"Admin"}}]}`))
		case "1":
			_, _ = w.Write([]byte(`{"data":[]}`))
		default:
			t.Fatalf("page[number] = %q, want 0 or 1", got)
		}
	})

	roles, err := adapter.ListRoles(context.Background())
	if err != nil {
		t.Fatalf("ListRoles(): %v", err)
	}
	if len(roles) != 1 {
		t.Fatalf("len(roles) = %d, want 1", len(roles))
	}
	if roles[0].ID != "role-1" || roles[0].Name != "Admin" {
		t.Fatalf("unexpected role: %#v", roles[0])
	}
}

func TestSDKAdapterListRoleMembersCanonicalizesServiceAccounts(t *testing.T) {
	t.Parallel()

	adapter := newTestSDKAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v2/roles/role-1/users" {
			t.Fatalf("path = %q, want /api/v2/roles/role-1/users", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		switch got := r.URL.Query().Get("page[number]"); got {
		case "0":
			_, _ = w.Write([]byte(`{"data":[` +
				`{"id":"u-1","attributes":{"handle":"alice@example.com"}},` +
				`{"id":"sa-1","attributes":{"service_account":true,"email":"ci@example.com"}}` +
				`]}`))
		case "1":
			_, _ = w.Write([]byte(`{"data":[]}`))
		default:
			t.Fatalf("page[number] = %q, want 0 or 1", got)
		}
	})

	memberIDs, err := adapter.ListRoleMembers(context.Background(), "role-1")
	if err != nil {
		t.Fatalf("ListRoleMembers(): %v", err)
	}
	if len(memberIDs) != 2 {
		t.Fatalf("len(memberIDs) = %d, want 2", len(memberIDs))
	}
	if memberIDs[0] != "u-1" {
		t.Fatalf("memberIDs[0] = %q, want %q", memberIDs[0], "u-1")
	}
	if memberIDs[1] != "service_account:sa-1" {
		t.Fatalf("memberIDs[1] = %q, want %q", memberIDs[1], "service_account:sa-1")
	}
}

func TestMapDatadogRoleProducesStableJSON(t *testing.T) {
	t.Parallel()

	role := datadogv2.NewRole(datadogv2.ROLESTYPE_ROLES)
	role.SetId("role-1")
	attrs := datadogv2.NewRoleAttributes()
	attrs.SetName("Admin")
	role.SetAttributes(*attrs)

	mapped, err := mapDatadogRole(*role)
	if err != nil {
		t.Fatalf("mapDatadogRole(): %v", err)
	}
	var payload map[string]string
	if err := json.Unmarshal(mapped.RawJSON, &payload); err != nil {
		t.Fatalf("json.Unmarshal(role raw json): %v", err)
	}
	if payload["name"] != "Admin" {
		t.Fatalf("role raw json name = %q, want %q", payload["name"], "Admin")
	}
}

func newTestSDKAdapter(t *testing.T, handler http.HandlerFunc) *sdkAdapter {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	cfg := datadogsdk.NewConfiguration()
	cfg.HTTPClient = server.Client()
	cfg.Servers = datadogsdk.ServerConfigurations{
		{URL: server.URL},
		{URL: server.URL},
		{URL: server.URL},
	}

	return &sdkAdapter{
		client: datadogsdk.NewAPIClient(cfg),
		site:   "datadoghq.com",
		apiKey: "api-key",
		appKey: "app-key",
	}
}

func datadogUsersResponseJSON(start, count int) string {
	var body strings.Builder
	body.WriteString(`{"data":[`)
	for i := 0; i < count; i++ {
		if i > 0 {
			body.WriteByte(',')
		}
		id := start + i
		body.WriteString(`{"id":"u-`)
		body.WriteString(strconv.Itoa(id))
		body.WriteString(`","attributes":{"handle":"user-`)
		body.WriteString(strconv.Itoa(id))
		body.WriteString(`@example.com","status":"Active"}}`)
	}
	body.WriteString(`]}`)
	return body.String()
}
