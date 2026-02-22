package googleworkspace

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"golang.org/x/oauth2"
)

func TestNormalizeScopes(t *testing.T) {
	t.Parallel()

	got := normalizeScopes([]string{" scope.a ", "scope.b", "scope.a", ""})
	if len(got) != 2 {
		t.Fatalf("len(normalizeScopes()) = %d, want 2", len(got))
	}
	if got[0] != "scope.a" || got[1] != "scope.b" {
		t.Fatalf("normalizeScopes() = %#v, want [scope.a scope.b]", got)
	}
}

func TestParseGoogleTime(t *testing.T) {
	t.Parallel()

	rfc := "2026-02-20T10:00:00Z"
	if got := parseGoogleTime(rfc); got.Format(time.RFC3339) != rfc {
		t.Fatalf("parseGoogleTime(%q) = %q, want %q", rfc, got.Format(time.RFC3339), rfc)
	}

	unixMS := "1768884000000"
	if got := parseGoogleTime(unixMS); got.IsZero() {
		t.Fatalf("parseGoogleTime(%q) returned zero time", unixMS)
	}

	if got := parseGoogleTime("not-a-time"); !got.IsZero() {
		t.Fatalf("parseGoogleTime(invalid) should return zero")
	}
}

func TestListUsersPaginates(t *testing.T) {
	t.Parallel()

	var tokenCalls atomic.Int32
	var usersCalls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			tokenCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"access_token":"access-token","expires_in":3600}`)
		case "/admin/directory/v1/users":
			usersCalls.Add(1)
			if got := strings.TrimSpace(r.Header.Get("Authorization")); got != "Bearer access-token" {
				t.Fatalf("authorization header = %q, want %q", got, "Bearer access-token")
			}
			switch strings.TrimSpace(r.URL.Query().Get("pageToken")) {
			case "":
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"users":[{"id":"u-1","primaryEmail":"Alice@example.com","name":{"fullName":"Alice A"}}],"nextPageToken":"p2"}`)
			case "p2":
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"users":[{"id":"u-2","primaryEmail":"Bob@example.com","name":{"fullName":"Bob B"}}]}`)
			default:
				t.Fatalf("unexpected pageToken %q", r.URL.Query().Get("pageToken"))
			}
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClientWithOptions(testServiceAccountConfig(t, server.URL+"/token"), ClientOptions{
		HTTPClient:       server.Client(),
		DirectoryBaseURL: server.URL + "/admin/directory/v1",
		TokenURL:         server.URL + "/token",
	})
	if err != nil {
		t.Fatalf("NewClientWithOptions() error = %v", err)
	}

	users, err := client.ListUsers(context.Background(), "C0123")
	if err != nil {
		t.Fatalf("ListUsers() error = %v", err)
	}
	if len(users) != 2 {
		t.Fatalf("len(users) = %d, want 2", len(users))
	}
	if users[0].ID != "u-1" || users[1].ID != "u-2" {
		t.Fatalf("unexpected user ids: %#v", []string{users[0].ID, users[1].ID})
	}
	if tokenCalls.Load() != 1 {
		t.Fatalf("token calls = %d, want 1", tokenCalls.Load())
	}
	if usersCalls.Load() != 2 {
		t.Fatalf("users calls = %d, want 2", usersCalls.Load())
	}
}

func TestDoAuthorizedJSONRequestRetries429(t *testing.T) {
	t.Parallel()

	var apiCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"access_token":"access-token","expires_in":3600}`)
		case "/retry":
			call := apiCalls.Add(1)
			if call == 1 {
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = io.WriteString(w, `{"error":"rate_limited"}`)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"ok":true}`)
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClientWithOptions(testServiceAccountConfig(t, server.URL+"/token"), ClientOptions{
		HTTPClient: server.Client(),
		TokenURL:   server.URL + "/token",
	})
	if err != nil {
		t.Fatalf("NewClientWithOptions() error = %v", err)
	}

	body, status, err := client.doAuthorizedJSONRequest(context.Background(), http.MethodGet, server.URL+"/retry", nil)
	if err != nil {
		t.Fatalf("doAuthorizedJSONRequest() error = %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("status = %d, want %d", status, http.StatusOK)
	}
	if strings.TrimSpace(string(body)) != `{"ok":true}` {
		t.Fatalf("response body = %q, want %q", strings.TrimSpace(string(body)), `{"ok":true}`)
	}
	if apiCalls.Load() != 2 {
		t.Fatalf("api calls = %d, want 2", apiCalls.Load())
	}
}

func TestSignedAssertionADCUsesSignJWT(t *testing.T) {
	t.Parallel()

	var authHeader string
	var claimsPayload string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, ":signJwt") {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		authHeader = strings.TrimSpace(r.Header.Get("Authorization"))
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		var req struct {
			Payload string `json:"payload"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("decode signJwt request: %v", err)
		}
		claimsPayload = req.Payload
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"signedJwt":"signed-jwt-value"}`)
	}))
	defer server.Close()

	cfg := configstore.GoogleWorkspaceConfig{
		CustomerID:          "C0123",
		DelegatedAdminEmail: "admin@example.com",
		AuthType:            configstore.GoogleWorkspaceAuthTypeADC,
		ServiceAccountEmail: "svc-account@example.iam.gserviceaccount.com",
	}
	client, err := NewClientWithOptions(cfg, ClientOptions{
		HTTPClient:            server.Client(),
		TokenURL:              "https://oauth2.googleapis.com/token",
		IAMCredentialsBaseURL: server.URL,
		ADCTokenSource: staticTokenSource{
			token: &oauth2.Token{AccessToken: "adc-access-token", Expiry: time.Now().Add(10 * time.Minute)},
		},
	})
	if err != nil {
		t.Fatalf("NewClientWithOptions() error = %v", err)
	}

	signedJWT, err := client.signedAssertion(context.Background())
	if err != nil {
		t.Fatalf("signedAssertion() error = %v", err)
	}
	if signedJWT != "signed-jwt-value" {
		t.Fatalf("signedAssertion() = %q, want %q", signedJWT, "signed-jwt-value")
	}
	if authHeader != "Bearer adc-access-token" {
		t.Fatalf("authorization header = %q, want %q", authHeader, "Bearer adc-access-token")
	}

	var claims map[string]any
	if err := json.Unmarshal([]byte(claimsPayload), &claims); err != nil {
		t.Fatalf("decode IAM claims payload: %v", err)
	}
	if got := strings.TrimSpace(toString(claims["sub"])); got != "admin@example.com" {
		t.Fatalf("claims[sub] = %q, want %q", got, "admin@example.com")
	}
	if got := strings.TrimSpace(toString(claims["iss"])); got != "svc-account@example.iam.gserviceaccount.com" {
		t.Fatalf("claims[iss] = %q, want %q", got, "svc-account@example.iam.gserviceaccount.com")
	}
}

func TestFetchAccessTokenFailsOnExchangeError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/token" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"error":"internal"}`)
	}))
	defer server.Close()

	client, err := NewClientWithOptions(testServiceAccountConfig(t, server.URL+"/token"), ClientOptions{
		HTTPClient: server.Client(),
		TokenURL:   server.URL + "/token",
	})
	if err != nil {
		t.Fatalf("NewClientWithOptions() error = %v", err)
	}

	_, _, err = client.fetchAccessToken(context.Background())
	if err == nil {
		t.Fatalf("fetchAccessToken() expected error")
	}
	if !strings.Contains(err.Error(), "google oauth token exchange failed") {
		t.Fatalf("fetchAccessToken() error = %q, want exchange failure", err.Error())
	}
}

func TestListGroupMembersReturnsEmptyOnNotFound(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"access_token":"access-token","expires_in":3600}`)
		case "/admin/directory/v1/groups/group-1/members":
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"error":"not_found"}`)
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClientWithOptions(testServiceAccountConfig(t, server.URL+"/token"), ClientOptions{
		HTTPClient:       server.Client(),
		DirectoryBaseURL: server.URL + "/admin/directory/v1",
		TokenURL:         server.URL + "/token",
	})
	if err != nil {
		t.Fatalf("NewClientWithOptions() error = %v", err)
	}

	members, err := client.ListGroupMembers(context.Background(), "group-1")
	if err != nil {
		t.Fatalf("ListGroupMembers() error = %v", err)
	}
	if len(members) != 0 {
		t.Fatalf("len(members) = %d, want 0", len(members))
	}
}

type staticTokenSource struct {
	token *oauth2.Token
	err   error
}

func (s staticTokenSource) Token() (*oauth2.Token, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.token == nil {
		return &oauth2.Token{}, nil
	}
	return s.token, nil
}

func testServiceAccountConfig(t *testing.T, tokenURL string) configstore.GoogleWorkspaceConfig {
	t.Helper()

	return configstore.GoogleWorkspaceConfig{
		CustomerID:          "C0123",
		DelegatedAdminEmail: "admin@example.com",
		AuthType:            configstore.GoogleWorkspaceAuthTypeServiceAccountJSON,
		ServiceAccountJSON:  testServiceAccountJSON(t, tokenURL),
	}
}

func testServiceAccountJSON(t *testing.T, tokenURL string) string {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey() error = %v", err)
	}
	pkcs8Bytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatalf("x509.MarshalPKCS8PrivateKey() error = %v", err)
	}
	privatePEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8Bytes})

	payload, err := json.Marshal(map[string]string{
		"client_email": "svc-account@example.iam.gserviceaccount.com",
		"private_key":  string(privatePEM),
		"token_uri":    tokenURL,
	})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return string(payload)
}

func toString(v any) string {
	s, _ := v.(string)
	return s
}
