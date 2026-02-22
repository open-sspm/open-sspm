package entra

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestListUsersPaging(t *testing.T) {
	t.Parallel()

	var tokenRequests int
	var userRequests int

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/oauth2/v2.0/token"):
			tokenRequests++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"access_token":"tkn","expires_in":3600,"token_type":"Bearer"}`))
			return
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/users"):
			userRequests++
			w.Header().Set("Content-Type", "application/json")
			page := r.URL.Query().Get("page")
			if page == "2" {
				_, _ = w.Write([]byte(`{"value":[{"id":"u2","displayName":"Two","mail":"two@example.com"}]}`))
				return
			}

			next := srv.URL + "/graph/v1.0/users?page=2"
			resp := map[string]any{
				"value": []map[string]any{
					{"id": "u1", "displayName": "One", "mail": "one@example.com"},
				},
				"@odata.nextLink": next,
			}
			_ = json.NewEncoder(w).Encode(resp)
			return
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c, err := NewWithOptions("tenant", "client", "secret", Options{
		AuthorityBaseURL: srv.URL,
		GraphBaseURL:     srv.URL + "/graph/v1.0",
	})
	if err != nil {
		t.Fatalf("NewWithOptions: %v", err)
	}

	users, err := c.ListUsers(context.Background())
	if err != nil {
		t.Fatalf("ListUsers: %v", err)
	}
	if len(users) != 2 {
		t.Fatalf("len(users)=%d want 2", len(users))
	}
	if tokenRequests != 1 {
		t.Fatalf("tokenRequests=%d want 1", tokenRequests)
	}
	if userRequests != 2 {
		t.Fatalf("userRequests=%d want 2", userRequests)
	}
}

func TestNormalizeGUID(t *testing.T) {
	t.Parallel()

	if got := normalizeGUID("{ABC}"); got != "abc" {
		t.Fatalf("normalizeGUID = %q want %q", got, "abc")
	}
	if got := normalizeGUID("  "); got != "" {
		t.Fatalf("normalizeGUID = %q want empty", got)
	}
}

func TestListApplicationsOwnersAndServicePrincipals(t *testing.T) {
	t.Parallel()

	var tokenRequests int
	var applicationRequests int
	var servicePrincipalRequests int
	var groupRequests int
	var appOwnerRequests int
	var spOwnerRequests int

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/oauth2/v2.0/token"):
			tokenRequests++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"access_token":"tkn","expires_in":3600,"token_type":"Bearer"}`))
			return
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/applications"):
			if strings.Contains(r.URL.Path, "/owners") {
				appOwnerRequests++
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"value":[{"id":"owner-user-1","@odata.type":"#microsoft.graph.user","displayName":"Owner One","mail":"owner1@example.com","userPrincipalName":"owner1@example.com"}]}`))
				return
			}
			applicationRequests++
			w.Header().Set("Content-Type", "application/json")
			page := r.URL.Query().Get("page")
			if page == "2" {
				_, _ = w.Write([]byte(`{"value":[{"id":"app-2","appId":"client-app-2","displayName":"App Two","publisherDomain":"apps.contoso.com","verifiedPublisher":{"displayName":""},"createdDateTime":"2026-01-01T00:00:00Z","passwordCredentials":[],"keyCredentials":[]}]}`))
				return
			}
			next := srv.URL + "/graph/v1.0/applications?page=2"
			_, _ = w.Write([]byte(`{"value":[{"id":"app-1","appId":"client-app-1","displayName":"App One","publisherDomain":"one.example.com","verifiedPublisher":{"displayName":"Publisher One"},"createdDateTime":"2025-01-01T00:00:00Z","passwordCredentials":[{"keyId":"pwd-1","displayName":"Secret One","startDateTime":"2025-01-01T00:00:00Z","endDateTime":"2026-01-01T00:00:00Z"}],"keyCredentials":[{"keyId":"cert-1","displayName":"Cert One","type":"AsymmetricX509Cert","usage":"Verify","startDateTime":"2025-01-01T00:00:00Z","endDateTime":"2027-01-01T00:00:00Z"}]}],"@odata.nextLink":"` + next + `"}`))
			return
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/servicePrincipals"):
			if strings.Contains(r.URL.Path, "/owners") {
				spOwnerRequests++
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"value":[{"id":"owner-sp-1","@odata.type":"#microsoft.graph.servicePrincipal","displayName":"SP Owner","appId":"owner-app-id"}]}`))
				return
			}
			servicePrincipalRequests++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"value":[{"id":"sp-1","appId":"client-app-1","displayName":"Service Principal One","publisherName":"Publisher One","accountEnabled":true,"servicePrincipalType":"Application","createdDateTime":"2025-01-02T00:00:00Z","passwordCredentials":[],"keyCredentials":[]}]}`))
			return
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/groups"):
			groupRequests++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"value":[{"id":"group-1","displayName":"Engineering","mail":"engineering@example.com","mailEnabled":true,"securityEnabled":true,"groupTypes":[]} ]}`))
			return
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c, err := NewWithOptions("tenant", "client", "secret", Options{
		AuthorityBaseURL: srv.URL,
		GraphBaseURL:     srv.URL + "/graph/v1.0",
	})
	if err != nil {
		t.Fatalf("NewWithOptions: %v", err)
	}

	apps, err := c.ListApplications(context.Background())
	if err != nil {
		t.Fatalf("ListApplications: %v", err)
	}
	if len(apps) != 2 {
		t.Fatalf("len(apps)=%d want 2", len(apps))
	}
	if len(apps[0].RawJSON) == 0 {
		t.Fatalf("expected app raw json")
	}
	if apps[0].VerifiedPublisher.DisplayName != "Publisher One" {
		t.Fatalf("unexpected verified publisher %q", apps[0].VerifiedPublisher.DisplayName)
	}
	if apps[1].PublisherDomain != "apps.contoso.com" {
		t.Fatalf("unexpected publisher domain %q", apps[1].PublisherDomain)
	}

	servicePrincipals, err := c.ListServicePrincipals(context.Background())
	if err != nil {
		t.Fatalf("ListServicePrincipals: %v", err)
	}
	if len(servicePrincipals) != 1 {
		t.Fatalf("len(servicePrincipals)=%d want 1", len(servicePrincipals))
	}
	if servicePrincipals[0].DisplayName != "Service Principal One" {
		t.Fatalf("unexpected service principal name %q", servicePrincipals[0].DisplayName)
	}
	if servicePrincipals[0].PublisherName != "Publisher One" {
		t.Fatalf("unexpected service principal publisher %q", servicePrincipals[0].PublisherName)
	}

	groups, err := c.ListGroups(context.Background())
	if err != nil {
		t.Fatalf("ListGroups: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("len(groups)=%d want 1", len(groups))
	}
	if groups[0].DisplayName != "Engineering" {
		t.Fatalf("unexpected group name %q", groups[0].DisplayName)
	}

	appOwners, err := c.ListApplicationOwners(context.Background(), "app-1")
	if err != nil {
		t.Fatalf("ListApplicationOwners: %v", err)
	}
	if len(appOwners) != 1 {
		t.Fatalf("len(appOwners)=%d want 1", len(appOwners))
	}
	if appOwners[0].ODataType == "" {
		t.Fatalf("expected owner type from response")
	}

	spOwners, err := c.ListServicePrincipalOwners(context.Background(), "sp-1")
	if err != nil {
		t.Fatalf("ListServicePrincipalOwners: %v", err)
	}
	if len(spOwners) != 1 {
		t.Fatalf("len(spOwners)=%d want 1", len(spOwners))
	}

	if tokenRequests != 1 {
		t.Fatalf("tokenRequests=%d want 1", tokenRequests)
	}
	if applicationRequests != 2 {
		t.Fatalf("applicationRequests=%d want 2", applicationRequests)
	}
	if servicePrincipalRequests != 1 {
		t.Fatalf("servicePrincipalRequests=%d want 1", servicePrincipalRequests)
	}
	if groupRequests != 1 {
		t.Fatalf("groupRequests=%d want 1", groupRequests)
	}
	if appOwnerRequests != 1 {
		t.Fatalf("appOwnerRequests=%d want 1", appOwnerRequests)
	}
	if spOwnerRequests != 1 {
		t.Fatalf("spOwnerRequests=%d want 1", spOwnerRequests)
	}
}

func TestListDirectoryAudits(t *testing.T) {
	t.Parallel()

	var sawFilter bool
	var sawTop bool

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/oauth2/v2.0/token"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"access_token":"tkn","expires_in":3600,"token_type":"Bearer"}`))
			return
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/auditLogs/directoryAudits"):
			if strings.Contains(r.URL.Query().Get("$filter"), "activityDateTime ge") {
				sawFilter = true
			}
			if r.URL.Query().Get("$top") == directoryAuditsTop {
				sawTop = true
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"value":[{"id":"event-1","category":"ApplicationManagement","result":"success","activityDisplayName":"Add application password credential","activityDateTime":"2026-02-01T10:00:00Z","initiatedBy":{"user":{"id":"user-1","displayName":"Alice","userPrincipalName":"alice@example.com"}},"targetResources":[{"id":"app-1","displayName":"App One","type":"Application"}]}]}`))
			return
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c, err := NewWithOptions("tenant", "client", "secret", Options{
		AuthorityBaseURL: srv.URL,
		GraphBaseURL:     srv.URL + "/graph/v1.0",
	})
	if err != nil {
		t.Fatalf("NewWithOptions: %v", err)
	}

	since := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	events, err := c.ListDirectoryAudits(context.Background(), &since)
	if err != nil {
		t.Fatalf("ListDirectoryAudits: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events)=%d want 1", len(events))
	}
	if events[0].ActivityDisplayName != "Add application password credential" {
		t.Fatalf("unexpected activity display name %q", events[0].ActivityDisplayName)
	}
	if len(events[0].RawJSON) == 0 {
		t.Fatalf("expected raw json")
	}
	if !sawFilter {
		t.Fatalf("expected since filter in request")
	}
	if !sawTop {
		t.Fatalf("expected $top=%s in request", directoryAuditsTop)
	}
}

func TestListDirectoryAuditsLargeResponseBody(t *testing.T) {
	t.Parallel()

	largePayload := map[string]any{
		"value": []map[string]any{
			{
				"id":                  "event-1",
				"category":            "ApplicationManagement",
				"result":              "success",
				"activityDisplayName": "Large payload event",
				"activityDateTime":    "2026-02-01T10:00:00Z",
				"initiatedBy": map[string]any{
					"user": map[string]any{
						"id":                "user-1",
						"displayName":       "Alice",
						"userPrincipalName": "alice@example.com",
					},
				},
				"targetResources": []map[string]any{
					{"id": "app-1", "displayName": "App One", "type": "Application"},
				},
				// Ensure response body exceeds maxErrorBodySize to catch truncation on success paths.
				"padding": strings.Repeat("x", maxErrorBodySize+512),
			},
		},
	}

	encodedLargePayload, err := json.Marshal(largePayload)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/oauth2/v2.0/token"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"access_token":"tkn","expires_in":3600,"token_type":"Bearer"}`))
			return
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/auditLogs/directoryAudits"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(encodedLargePayload)
			return
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c, err := NewWithOptions("tenant", "client", "secret", Options{
		AuthorityBaseURL: srv.URL,
		GraphBaseURL:     srv.URL + "/graph/v1.0",
	})
	if err != nil {
		t.Fatalf("NewWithOptions: %v", err)
	}

	events, err := c.ListDirectoryAudits(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListDirectoryAudits: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events)=%d want 1", len(events))
	}
	if events[0].ActivityDisplayName != "Large payload event" {
		t.Fatalf("unexpected activity display name %q", events[0].ActivityDisplayName)
	}
}

func TestListOAuth2PermissionGrants(t *testing.T) {
	t.Parallel()

	var sawUnsupportedSelect bool
	var grantRequests int

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/oauth2/v2.0/token"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"access_token":"tkn","expires_in":3600,"token_type":"Bearer"}`))
			return
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/oauth2PermissionGrants"):
			grantRequests++
			if strings.Contains(r.URL.Query().Get("$select"), "createdDateTime") {
				sawUnsupportedSelect = true
				http.Error(w, "unsupported select field", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"value":[{"id":"grant-1","clientId":"sp-1","consentType":"Principal","principalId":"user-1","resourceId":"api-1","scope":"User.Read Files.Read"}]}`))
			return
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c, err := NewWithOptions("tenant", "client", "secret", Options{
		AuthorityBaseURL: srv.URL,
		GraphBaseURL:     srv.URL + "/graph/v1.0",
	})
	if err != nil {
		t.Fatalf("NewWithOptions: %v", err)
	}

	grants, err := c.ListOAuth2PermissionGrants(context.Background())
	if err != nil {
		t.Fatalf("ListOAuth2PermissionGrants: %v", err)
	}
	if len(grants) != 1 {
		t.Fatalf("len(grants)=%d want 1", len(grants))
	}
	if grants[0].ID != "grant-1" {
		t.Fatalf("unexpected grant id %q", grants[0].ID)
	}
	if sawUnsupportedSelect {
		t.Fatalf("request included unsupported createdDateTime select field")
	}
	if grantRequests != 1 {
		t.Fatalf("grantRequests=%d want 1", grantRequests)
	}
}

func TestGraphURL(t *testing.T) {
	t.Parallel()

	c := &Client{graphBaseURL: "https://example.com/graph/v1.0"}
	got, err := c.graphURL("/users", url.Values{"$top": []string{"1"}})
	if err != nil {
		t.Fatalf("graphURL: %v", err)
	}
	if got != "https://example.com/graph/v1.0/users?%24top=1" {
		t.Fatalf("graphURL=%q", got)
	}
}
