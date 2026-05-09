package entra

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	abstractions "github.com/microsoft/kiota-abstractions-go"
	absauth "github.com/microsoft/kiota-abstractions-go/authentication"
	absser "github.com/microsoft/kiota-abstractions-go/serialization"
	msgraphsdkgo "github.com/microsoftgraph/msgraph-sdk-go"
)

func newGraphTestClient(t *testing.T, srv *httptest.Server) *Client {
	t.Helper()

	client, err := newTestClient(srv.URL+"/graph/v1.0", srv.Client())
	if err != nil {
		t.Fatalf("newTestClient() error = %v", err)
	}
	return client
}

func newTestClient(graphBaseURL string, httpClient *http.Client) (*Client, error) {
	adapter, err := newStaticTokenRequestAdapter(graphBaseURL, httpClient)
	if err != nil {
		return nil, err
	}
	return newClientFromAdapter(adapter)
}

func newStaticTokenRequestAdapter(graphBaseURL string, httpClient *http.Client) (abstractions.RequestAdapter, error) {
	graphBaseURL = strings.TrimRight(strings.TrimSpace(graphBaseURL), "/")
	if graphBaseURL == "" {
		return nil, errors.New("entra graph base url is required")
	}

	parsed, err := url.Parse(graphBaseURL)
	if err != nil {
		return nil, err
	}
	validator, err := absauth.NewAllowedHostsValidatorErrorCheck([]string{parsed.Hostname()})
	if err != nil {
		return nil, err
	}

	tokenProvider := &entraStaticAccessTokenProvider{
		token:     "test-token",
		validator: validator,
	}
	authProvider := absauth.NewBaseBearerTokenAuthenticationProvider(tokenProvider)
	adapter, err := msgraphsdkgo.NewGraphRequestAdapterWithParseNodeFactoryAndSerializationWriterFactoryAndHttpClient(authProvider, nil, nil, httpClient)
	if err != nil {
		return nil, err
	}
	adapter.SetBaseUrl(graphBaseURL)
	return adapter, nil
}

func assertTestBearer(t *testing.T, r *http.Request) {
	t.Helper()

	if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
		t.Fatalf("Authorization=%q want %q", got, "Bearer test-token")
	}
}

func TestListUsersPaging(t *testing.T) {
	t.Parallel()

	var userRequests int

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertTestBearer(t, r)

		switch {
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/users"):
			userRequests++
			w.Header().Set("Content-Type", "application/json")
			if r.URL.Query().Get("page") == "2" {
				_, _ = w.Write([]byte(`{"value":[{"id":"u2","displayName":"Two","mail":"two@example.com"}]}`))
				return
			}

			next := srv.URL + "/graph/v1.0/users?page=2"
			_ = json.NewEncoder(w).Encode(map[string]any{
				"value": []map[string]any{
					{"id": "u1", "displayName": "One", "mail": "one@example.com"},
				},
				"@odata.nextLink": next,
			})
			return
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	users, err := newGraphTestClient(t, srv).ListUsers(context.Background())
	if err != nil {
		t.Fatalf("ListUsers() error = %v", err)
	}
	if len(users) != 2 {
		t.Fatalf("len(users)=%d want 2", len(users))
	}
	if entityID(users[0]) != "u1" || stringValue(users[0].GetDisplayName()) != "One" {
		t.Fatalf("unexpected first user id=%q display_name=%q", entityID(users[0]), stringValue(users[0].GetDisplayName()))
	}
	if entityID(users[1]) != "u2" || stringValue(users[1].GetDisplayName()) != "Two" {
		t.Fatalf("unexpected second user id=%q display_name=%q", entityID(users[1]), stringValue(users[1].GetDisplayName()))
	}
	if len(mustSerializeSDKModel(t, users[1])) == 0 {
		t.Fatalf("expected serialized user payload")
	}
	if userRequests != 2 {
		t.Fatalf("userRequests=%d want 2", userRequests)
	}
}

func TestDeltaUsersPagingRemovedAndDeltaLink(t *testing.T) {
	t.Parallel()

	var userRequests int
	var sawSelect bool
	var sawTop bool

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertTestBearer(t, r)

		switch {
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/users/delta"):
			userRequests++
			w.Header().Set("Content-Type", "application/json")
			if r.URL.Query().Get("page") == "2" {
				delta := srv.URL + "/graph/v1.0/users/delta?$deltatoken=done"
				_, _ = w.Write([]byte(`{"value":[{"id":"u2","displayName":"Two","mail":"two@example.com"}],"@odata.deltaLink":"` + delta + `"}`))
				return
			}

			if strings.Contains(r.URL.Query().Get("$select"), "displayName") {
				sawSelect = true
			}
			if r.URL.Query().Has("$top") {
				sawTop = true
			}
			next := srv.URL + "/graph/v1.0/users/delta?page=2"
			_ = json.NewEncoder(w).Encode(map[string]any{
				"value": []map[string]any{
					{"id": "u1", "displayName": "One", "mail": "one@example.com"},
					{"id": "u-deleted", "@removed": map[string]string{"reason": "deleted"}},
					{"id": "u-changed", "@removed": map[string]string{"reason": "changed"}},
				},
				"@odata.nextLink": next,
			})
			return
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	result, err := newGraphTestClient(t, srv).DeltaUsers(context.Background(), "")
	if err != nil {
		t.Fatalf("DeltaUsers() error = %v", err)
	}
	if len(result.Items) != 2 {
		t.Fatalf("len(result.Items)=%d want 2", len(result.Items))
	}
	if entityID(result.Items[0]) != "u1" || entityID(result.Items[1]) != "u2" {
		t.Fatalf("unexpected users: %q %q", entityID(result.Items[0]), entityID(result.Items[1]))
	}
	if len(result.RemovedIDs) != 2 || result.RemovedIDs[0] != "u-deleted" || result.RemovedIDs[1] != "u-changed" {
		t.Fatalf("RemovedIDs=%v want [u-deleted u-changed]", result.RemovedIDs)
	}
	if !strings.Contains(result.DeltaLink, "$deltatoken=done") {
		t.Fatalf("DeltaLink=%q want final delta token", result.DeltaLink)
	}
	if userRequests != 2 {
		t.Fatalf("userRequests=%d want 2", userRequests)
	}
	if !sawSelect {
		t.Fatalf("expected initial delta request to include select")
	}
	if sawTop {
		t.Fatalf("expected initial delta request to omit unsupported $top")
	}
}

func TestDeltaUsersResumeUsesStoredDeltaLink(t *testing.T) {
	t.Parallel()

	var sawResumeToken bool

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertTestBearer(t, r)

		switch {
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/users/delta"):
			if r.URL.Query().Get("$deltatoken") == "abc" {
				sawResumeToken = true
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"value":[{"id":"u1","displayName":"One"}],"@odata.deltaLink":"` + srv.URL + `/graph/v1.0/users/delta?$deltatoken=next"}`))
			return
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	resume := srv.URL + "/graph/v1.0/users/delta?$deltatoken=abc"
	result, err := newGraphTestClient(t, srv).DeltaUsers(context.Background(), resume)
	if err != nil {
		t.Fatalf("DeltaUsers(resume) error = %v", err)
	}
	if len(result.Items) != 1 || entityID(result.Items[0]) != "u1" {
		t.Fatalf("unexpected resumed items: %+v", result.Items)
	}
	if !sawResumeToken {
		t.Fatalf("expected stored delta link to be used directly")
	}
}

func TestDeltaUsersExpiredCursor(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertTestBearer(t, r)

		switch {
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/users/delta"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusGone)
			_, _ = w.Write([]byte(`{"error":{"code":"SyncStateNotFound","message":"syncState not found"}}`))
			return
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	_, err := newGraphTestClient(t, srv).DeltaUsers(context.Background(), srv.URL+"/graph/v1.0/users/delta?$deltatoken=stale")
	if !errors.Is(err, ErrDeltaCursorExpired) {
		t.Fatalf("DeltaUsers() error = %v, want ErrDeltaCursorExpired", err)
	}
}

func TestLookupUsersByIDsUsesGetByIDsAndIgnoresNonUsers(t *testing.T) {
	t.Parallel()

	var lookupRequests int
	var requestBody struct {
		IDs   []string `json:"ids"`
		Types []string `json:"types"`
	}

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertTestBearer(t, r)

		switch {
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/directoryObjects/getByIds"):
			lookupRequests++
			if r.Method != http.MethodPost {
				t.Fatalf("method=%s want POST", r.Method)
			}
			if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
				t.Fatalf("Decode(requestBody) error = %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"value":[{"@odata.type":"#microsoft.graph.user","id":"u1","displayName":"One","userPrincipalName":"one@example.com"},{"@odata.type":"#microsoft.graph.group","id":"g1","displayName":"Group One"},{"@odata.type":"#microsoft.graph.user","id":"u2","displayName":"Two"}]}`))
			return
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	users, err := newGraphTestClient(t, srv).LookupUsersByIDs(context.Background(), []string{" u1 ", "", "u2", "u1"})
	if err != nil {
		t.Fatalf("LookupUsersByIDs() error = %v", err)
	}
	if len(users) != 2 {
		t.Fatalf("len(users)=%d want 2", len(users))
	}
	if entityID(users[0]) != "u1" || stringValue(users[0].GetDisplayName()) != "One" {
		t.Fatalf("unexpected first user id=%q display_name=%q", entityID(users[0]), stringValue(users[0].GetDisplayName()))
	}
	if entityID(users[1]) != "u2" || stringValue(users[1].GetDisplayName()) != "Two" {
		t.Fatalf("unexpected second user id=%q display_name=%q", entityID(users[1]), stringValue(users[1].GetDisplayName()))
	}
	if len(mustSerializeSDKModel(t, users[1])) == 0 {
		t.Fatalf("expected serialized user payload")
	}
	if lookupRequests != 1 {
		t.Fatalf("lookupRequests=%d want 1", lookupRequests)
	}
	if len(requestBody.Types) != 1 || requestBody.Types[0] != "user" {
		t.Fatalf("types=%v want [user]", requestBody.Types)
	}
	if len(requestBody.IDs) != 2 || requestBody.IDs[0] != "u1" || requestBody.IDs[1] != "u2" {
		t.Fatalf("ids=%v want [u1 u2]", requestBody.IDs)
	}
}

func TestLookupUsersByIDsChunksLargeRequests(t *testing.T) {
	t.Parallel()

	var lookupRequests int
	chunkSizes := make([]int, 0, 2)

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertTestBearer(t, r)

		switch {
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/directoryObjects/getByIds"):
			lookupRequests++
			var req struct {
				IDs []string `json:"ids"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("Decode(request body) error = %v", err)
			}
			chunkSizes = append(chunkSizes, len(req.IDs))

			users := make([]map[string]any, 0, len(req.IDs))
			for _, id := range req.IDs {
				users = append(users, map[string]any{
					"@odata.type": "#microsoft.graph.user",
					"id":          id,
				})
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"value": users})
			return
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	ids := make([]string, 0, entraUserBatchSize+1)
	for i := 0; i < entraUserBatchSize+1; i++ {
		ids = append(ids, "user-"+strconv.Itoa(i))
	}

	users, err := newGraphTestClient(t, srv).LookupUsersByIDs(context.Background(), ids)
	if err != nil {
		t.Fatalf("LookupUsersByIDs() error = %v", err)
	}
	if len(users) != len(ids) {
		t.Fatalf("len(users)=%d want %d", len(users), len(ids))
	}
	if lookupRequests != 2 {
		t.Fatalf("lookupRequests=%d want 2", lookupRequests)
	}
	if len(chunkSizes) != 2 || chunkSizes[0] != entraUserBatchSize || chunkSizes[1] != 1 {
		t.Fatalf("chunkSizes=%v want [%d 1]", chunkSizes, entraUserBatchSize)
	}
}

func TestNormalizeGUID(t *testing.T) {
	t.Parallel()

	if got := normalizeGUID("{ABC}"); got != "abc" {
		t.Fatalf("normalizeGUID()=%q want %q", got, "abc")
	}
	if got := normalizeGUID("  "); got != "" {
		t.Fatalf("normalizeGUID()=%q want empty", got)
	}
}

func TestListApplicationsOwnersAndServicePrincipals(t *testing.T) {
	t.Parallel()

	const appRoleID = "77777777-7777-7777-7777-777777777777"
	const passwordKeyID = "88888888-8888-8888-8888-888888888888"
	const certificateKeyID = "99999999-9999-9999-9999-999999999999"

	var applicationRequests int
	var servicePrincipalRequests int
	var groupRequests int
	var appOwnerRequests int
	var spOwnerRequests int
	var sawServicePrincipalVerifiedPublisherSelect bool

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertTestBearer(t, r)

		switch {
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/applications/") && strings.Contains(r.URL.Path, "/owners"):
			appOwnerRequests++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"value":[{"id":"owner-user-1","@odata.type":"#microsoft.graph.user","displayName":"Owner One","mail":"owner1@example.com","userPrincipalName":"owner1@example.com"}]}`))
			return
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/applications"):
			applicationRequests++
			w.Header().Set("Content-Type", "application/json")
			if r.URL.Query().Get("page") == "2" {
				_, _ = w.Write([]byte(`{"value":[{"id":"app-2","appId":"client-app-2","displayName":"App Two","publisherDomain":"apps.contoso.com","verifiedPublisher":{"displayName":""},"createdDateTime":"2026-01-01T00:00:00Z","passwordCredentials":[],"keyCredentials":[]}]}`))
				return
			}
			next := srv.URL + "/graph/v1.0/applications?page=2"
			_, _ = w.Write([]byte(`{"value":[{"id":"app-1","appId":"client-app-1","displayName":"App One","publisherDomain":"one.example.com","verifiedPublisher":{"displayName":"Publisher One"},"createdDateTime":"2025-01-01T00:00:00Z","passwordCredentials":[{"keyId":"` + passwordKeyID + `","displayName":"Secret One","startDateTime":"2025-01-01T00:00:00Z","endDateTime":"2026-01-01T00:00:00Z"}],"keyCredentials":[{"keyId":"` + certificateKeyID + `","displayName":"Cert One","type":"AsymmetricX509Cert","usage":"Verify","startDateTime":"2025-01-01T00:00:00Z","endDateTime":"2027-01-01T00:00:00Z"}]}],"@odata.nextLink":"` + next + `"}`))
			return
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/servicePrincipals/") && strings.Contains(r.URL.Path, "/owners"):
			spOwnerRequests++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"value":[{"id":"owner-sp-1","@odata.type":"#microsoft.graph.servicePrincipal","displayName":"SP Owner","appId":"owner-app-id"}]}`))
			return
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/servicePrincipals"):
			servicePrincipalRequests++
			if strings.Contains(r.URL.Query().Get("$select"), "verifiedPublisher") {
				sawServicePrincipalVerifiedPublisherSelect = true
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"value":[{"id":"sp-1","appId":"client-app-1","displayName":"Service Principal One","publisherName":"Publisher One","verifiedPublisher":{"displayName":"Publisher One"},"accountEnabled":true,"servicePrincipalType":"Application","createdDateTime":"2025-01-02T00:00:00Z","appRoles":[{"id":"` + appRoleID + `","displayName":"Reader","value":"Reader","isEnabled":true,"allowedMemberTypes":["User"]}],"passwordCredentials":[],"keyCredentials":[]}]}`))
			return
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/groups"):
			groupRequests++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"value":[{"id":"group-1","displayName":"Engineering","mail":"engineering@example.com","mailEnabled":true,"securityEnabled":true,"groupTypes":[]}]}`))
			return
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	client := newGraphTestClient(t, srv)

	apps, err := client.ListApplications(context.Background())
	if err != nil {
		t.Fatalf("ListApplications() error = %v", err)
	}
	if len(apps) != 2 {
		t.Fatalf("len(apps)=%d want 2", len(apps))
	}
	if len(mustSerializeSDKModel(t, apps[0])) == 0 {
		t.Fatalf("expected serialized app payload")
	}
	if got := stringValue(apps[0].GetVerifiedPublisher().GetDisplayName()); got != "Publisher One" {
		t.Fatalf("verifiedPublisher.displayName=%q want %q", got, "Publisher One")
	}
	if got := stringValue(apps[1].GetPublisherDomain()); got != "apps.contoso.com" {
		t.Fatalf("publisherDomain=%q want %q", got, "apps.contoso.com")
	}

	servicePrincipals, err := client.ListServicePrincipals(context.Background())
	if err != nil {
		t.Fatalf("ListServicePrincipals() error = %v", err)
	}
	if len(servicePrincipals) != 1 {
		t.Fatalf("len(servicePrincipals)=%d want 1", len(servicePrincipals))
	}
	if got := stringValue(servicePrincipals[0].GetDisplayName()); got != "Service Principal One" {
		t.Fatalf("displayName=%q want %q", got, "Service Principal One")
	}
	if got := stringValue(servicePrincipals[0].GetVerifiedPublisher().GetDisplayName()); got != "Publisher One" {
		t.Fatalf("verifiedPublisher.displayName=%q want %q", got, "Publisher One")
	}
	if roles := servicePrincipals[0].GetAppRoles(); len(roles) != 1 || stringValue(roles[0].GetDisplayName()) != "Reader" {
		t.Fatalf("unexpected service principal app roles count=%d", len(roles))
	}

	groups, err := client.ListGroups(context.Background())
	if err != nil {
		t.Fatalf("ListGroups() error = %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("len(groups)=%d want 1", len(groups))
	}
	if got := stringValue(groups[0].GetDisplayName()); got != "Engineering" {
		t.Fatalf("displayName=%q want %q", got, "Engineering")
	}

	appOwners, err := client.ListApplicationOwners(context.Background(), "app-1")
	if err != nil {
		t.Fatalf("ListApplicationOwners() error = %v", err)
	}
	if len(appOwners) != 1 {
		t.Fatalf("len(appOwners)=%d want 1", len(appOwners))
	}
	if stringValue(appOwners[0].GetOdataType()) == "" {
		t.Fatalf("expected owner @odata.type")
	}

	spOwners, err := client.ListServicePrincipalOwners(context.Background(), "sp-1")
	if err != nil {
		t.Fatalf("ListServicePrincipalOwners() error = %v", err)
	}
	if len(spOwners) != 1 {
		t.Fatalf("len(spOwners)=%d want 1", len(spOwners))
	}

	if applicationRequests != 2 {
		t.Fatalf("applicationRequests=%d want 2", applicationRequests)
	}
	if servicePrincipalRequests != 1 {
		t.Fatalf("servicePrincipalRequests=%d want 1", servicePrincipalRequests)
	}
	if !sawServicePrincipalVerifiedPublisherSelect {
		t.Fatalf("expected verifiedPublisher in service principal select")
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

func TestListServicePrincipalsPaging(t *testing.T) {
	t.Parallel()

	var servicePrincipalRequests int

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertTestBearer(t, r)

		switch {
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/servicePrincipals"):
			servicePrincipalRequests++
			w.Header().Set("Content-Type", "application/json")
			if r.URL.Query().Get("page") == "2" {
				_, _ = w.Write([]byte(`{"value":[{"id":"sp-2","appId":"client-app-2","displayName":"Service Principal Two","accountEnabled":false}]}`))
				return
			}

			next := srv.URL + "/graph/v1.0/servicePrincipals?page=2"
			_, _ = w.Write([]byte(`{"value":[{"id":"sp-1","appId":"client-app-1","displayName":"Service Principal One","accountEnabled":true}],"@odata.nextLink":"` + next + `"}`))
			return
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	servicePrincipals, err := newGraphTestClient(t, srv).ListServicePrincipals(context.Background())
	if err != nil {
		t.Fatalf("ListServicePrincipals() error = %v", err)
	}
	if len(servicePrincipals) != 2 {
		t.Fatalf("len(servicePrincipals)=%d want 2", len(servicePrincipals))
	}
	if entityID(servicePrincipals[0]) != "sp-1" || entityID(servicePrincipals[1]) != "sp-2" {
		t.Fatalf("unexpected service principal ids [%q %q]", entityID(servicePrincipals[0]), entityID(servicePrincipals[1]))
	}
	if servicePrincipalRequests != 2 {
		t.Fatalf("servicePrincipalRequests=%d want 2", servicePrincipalRequests)
	}
}

func TestListGroupsPaging(t *testing.T) {
	t.Parallel()

	var groupRequests int

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertTestBearer(t, r)

		switch {
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/groups"):
			groupRequests++
			w.Header().Set("Content-Type", "application/json")
			if r.URL.Query().Get("page") == "2" {
				_, _ = w.Write([]byte(`{"value":[{"id":"group-2","displayName":"Security"}]}`))
				return
			}

			next := srv.URL + "/graph/v1.0/groups?page=2"
			_, _ = w.Write([]byte(`{"value":[{"id":"group-1","displayName":"Engineering"}],"@odata.nextLink":"` + next + `"}`))
			return
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	groups, err := newGraphTestClient(t, srv).ListGroups(context.Background())
	if err != nil {
		t.Fatalf("ListGroups() error = %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("len(groups)=%d want 2", len(groups))
	}
	if entityID(groups[0]) != "group-1" || entityID(groups[1]) != "group-2" {
		t.Fatalf("unexpected group ids [%q %q]", entityID(groups[0]), entityID(groups[1]))
	}
	if groupRequests != 2 {
		t.Fatalf("groupRequests=%d want 2", groupRequests)
	}
}

func TestListDirectoryAudits(t *testing.T) {
	t.Parallel()

	var sawFilter bool
	var sawOrder bool
	var sawTop bool

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertTestBearer(t, r)

		switch {
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/auditLogs/directoryAudits"):
			if strings.Contains(r.URL.Query().Get("$filter"), "activityDateTime ge") {
				sawFilter = true
			}
			if r.URL.Query().Get("$orderby") == "activityDateTime desc" {
				sawOrder = true
			}
			if r.URL.Query().Get("$top") == strconv.Itoa(int(directoryAuditsTop)) {
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

	since := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	events, err := newGraphTestClient(t, srv).ListDirectoryAudits(context.Background(), &since)
	if err != nil {
		t.Fatalf("ListDirectoryAudits() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events)=%d want 1", len(events))
	}
	if got := stringValue(events[0].GetActivityDisplayName()); got != "Add application password credential" {
		t.Fatalf("activityDisplayName=%q want %q", got, "Add application password credential")
	}
	if len(mustSerializeSDKModel(t, events[0])) == 0 {
		t.Fatalf("expected serialized audit payload")
	}
	if !sawFilter {
		t.Fatalf("expected since filter in request")
	}
	if !sawOrder {
		t.Fatalf("expected order by in request")
	}
	if !sawTop {
		t.Fatalf("expected $top=%d in request", directoryAuditsTop)
	}
}

func TestListDirectoryAuditsLargeResponseBody(t *testing.T) {
	t.Parallel()

	payload, err := json.Marshal(map[string]any{
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
				"padding": strings.Repeat("x", 8192),
			},
		},
	})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertTestBearer(t, r)

		switch {
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/auditLogs/directoryAudits"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(payload)
			return
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	events, err := newGraphTestClient(t, srv).ListDirectoryAudits(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListDirectoryAudits() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events)=%d want 1", len(events))
	}
	if got := stringValue(events[0].GetActivityDisplayName()); got != "Large payload event" {
		t.Fatalf("activityDisplayName=%q want %q", got, "Large payload event")
	}
}

func TestListSignIns(t *testing.T) {
	t.Parallel()

	var sawFilter bool
	var sawOrder bool
	var sawTop bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertTestBearer(t, r)

		switch {
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/auditLogs/signIns"):
			if strings.Contains(r.URL.Query().Get("$filter"), "createdDateTime ge") {
				sawFilter = true
			}
			if r.URL.Query().Get("$orderby") == "createdDateTime desc" {
				sawOrder = true
			}
			if r.URL.Query().Get("$top") == strconv.Itoa(int(defaultPageSize)) {
				sawTop = true
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"value":[{"id":"signin-1","createdDateTime":"2026-02-01T10:00:00Z","appId":"client-app-1","appDisplayName":"App One","resourceDisplayName":"Graph","userId":"user-1","userDisplayName":"Alice","userPrincipalName":"alice@example.com"}]}`))
			return
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	since := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	signIns, err := newGraphTestClient(t, srv).ListSignIns(context.Background(), &since)
	if err != nil {
		t.Fatalf("ListSignIns() error = %v", err)
	}
	if len(signIns) != 1 {
		t.Fatalf("len(signIns)=%d want 1", len(signIns))
	}
	if got := stringValue(signIns[0].GetAppDisplayName()); got != "App One" {
		t.Fatalf("appDisplayName=%q want %q", got, "App One")
	}
	if !sawFilter {
		t.Fatalf("expected since filter in request")
	}
	if !sawOrder {
		t.Fatalf("expected order by in request")
	}
	if !sawTop {
		t.Fatalf("expected $top=%d in request", defaultPageSize)
	}
}

func TestListOAuth2PermissionGrants(t *testing.T) {
	t.Parallel()

	var sawUnsupportedSelect bool
	var grantRequests int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertTestBearer(t, r)

		switch {
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

	grants, err := newGraphTestClient(t, srv).ListOAuth2PermissionGrants(context.Background())
	if err != nil {
		t.Fatalf("ListOAuth2PermissionGrants() error = %v", err)
	}
	if len(grants) != 1 {
		t.Fatalf("len(grants)=%d want 1", len(grants))
	}
	if got := entityID(grants[0]); got != "grant-1" {
		t.Fatalf("grant id=%q want %q", got, "grant-1")
	}
	if sawUnsupportedSelect {
		t.Fatalf("request included unsupported createdDateTime select field")
	}
	if grantRequests != 1 {
		t.Fatalf("grantRequests=%d want 1", grantRequests)
	}
}

func TestListServicePrincipalAssignedToPaging(t *testing.T) {
	t.Parallel()

	const firstPrincipalID = "22222222-2222-2222-2222-222222222222"
	const secondPrincipalID = "33333333-3333-3333-3333-333333333333"
	const firstRoleID = "77777777-7777-7777-7777-777777777777"
	const secondRoleID = "88888888-8888-8888-8888-888888888888"
	const resourceID = "11111111-1111-1111-1111-111111111111"

	var assignmentRequests int

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertTestBearer(t, r)

		switch {
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/servicePrincipals/sp-1/appRoleAssignedTo"):
			assignmentRequests++
			w.Header().Set("Content-Type", "application/json")
			if r.URL.Query().Get("page") == "2" {
				_, _ = w.Write([]byte(`{"value":[{"id":"assign-2","appRoleId":"` + secondRoleID + `","principalDisplayName":"Engineering","principalId":"` + secondPrincipalID + `","principalType":"Group","resourceDisplayName":"App One","resourceId":"` + resourceID + `"}]}`))
				return
			}
			next := srv.URL + "/graph/v1.0/servicePrincipals/sp-1/appRoleAssignedTo?page=2"
			_, _ = w.Write([]byte(`{"value":[{"id":"assign-1","appRoleId":"` + firstRoleID + `","principalDisplayName":"Alice","principalId":"` + firstPrincipalID + `","principalType":"User","resourceDisplayName":"App One","resourceId":"` + resourceID + `"}],"@odata.nextLink":"` + next + `"}`))
			return
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	assignments, err := newGraphTestClient(t, srv).ListServicePrincipalAssignedTo(context.Background(), "sp-1")
	if err != nil {
		t.Fatalf("ListServicePrincipalAssignedTo() error = %v", err)
	}
	if len(assignments) != 2 {
		t.Fatalf("len(assignments)=%d want 2", len(assignments))
	}
	if got := uuidValueString(assignments[0].GetPrincipalId()); got != firstPrincipalID {
		t.Fatalf("first principal id=%q want %q", got, firstPrincipalID)
	}
	if got := uuidValueString(assignments[1].GetPrincipalId()); got != secondPrincipalID {
		t.Fatalf("second principal id=%q want %q", got, secondPrincipalID)
	}
	if len(mustSerializeSDKModel(t, assignments[0])) == 0 {
		t.Fatalf("expected serialized assignment payload")
	}
	if assignmentRequests != 2 {
		t.Fatalf("assignmentRequests=%d want 2", assignmentRequests)
	}
}

func TestListGroupMembersTransitiveMembersAndDirectoryRoleAssignments(t *testing.T) {
	t.Parallel()

	var directGroupMemberRequests int
	var groupMemberRequests int
	var directoryRoleRequests int
	var directoryRoleAssignmentRequests int

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertTestBearer(t, r)

		switch {
		case strings.Contains(r.URL.Path, "/groups/group-1/members/"):
			directGroupMemberRequests++
			w.Header().Set("Content-Type", "application/json")
			if r.URL.Query().Get("page") == "2" {
				_, _ = w.Write([]byte(`{"value":[{"id":"user-2","displayName":"Bob","userPrincipalName":"bob@example.com"}]}`))
				return
			}
			next := srv.URL + "/graph/v1.0/groups/group-1/members/graph.user?page=2"
			_, _ = w.Write([]byte(`{"value":[{"id":"user-1","displayName":"Alice","mail":"alice@example.com","userPrincipalName":"alice@example.com"}],"@odata.nextLink":"` + next + `"}`))
			return
		case strings.Contains(r.URL.Path, "/groups/group-1/transitiveMembers/"):
			groupMemberRequests++
			if got := r.Header.Get("ConsistencyLevel"); got != "eventual" {
				t.Fatalf("ConsistencyLevel=%q want %q", got, "eventual")
			}
			w.Header().Set("Content-Type", "application/json")
			if r.URL.Query().Get("page") == "2" {
				_, _ = w.Write([]byte(`{"value":[{"id":"user-2","displayName":"Bob","userPrincipalName":"bob@example.com"}]}`))
				return
			}
			if got := r.URL.Query().Get("$count"); got != "true" {
				t.Fatalf("$count=%q want %q", got, "true")
			}
			if got := r.URL.Query().Get("$top"); got != "999" {
				t.Fatalf("$top=%q want %q", got, "999")
			}
			next := srv.URL + "/graph/v1.0/groups/group-1/transitiveMembers/graph.user?page=2"
			_, _ = w.Write([]byte(`{"value":[{"id":"user-1","displayName":"Alice","mail":"alice@example.com","userPrincipalName":"alice@example.com"}],"@odata.nextLink":"` + next + `"}`))
			return
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/roleManagement/directory/roleDefinitions"):
			directoryRoleRequests++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"value":[{"id":"role-def-1","displayName":"Global Administrator","templateId":"tmpl-1","isBuiltIn":true},{"id":"role-def-2","displayName":"Scoped Custom Role","isBuiltIn":false}]}`))
			return
		case strings.HasPrefix(r.URL.Path, "/graph/v1.0/roleManagement/directory/roleAssignments"):
			directoryRoleAssignmentRequests++
			if got := r.URL.Query().Get("$expand"); got != "principal" {
				http.Error(w, "missing expand", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"value":[{"id":"assign-1","principalId":"user-3","roleDefinitionId":"role-def-1","directoryScopeId":"/","principal":{"id":"user-3","displayName":"Carol","@odata.type":"#microsoft.graph.user"}},{"id":"assign-2","principalId":"group-1","roleDefinitionId":"role-def-2","directoryScopeId":"/administrativeUnits/au-1","principal":{"id":"group-1","displayName":"Privileged Ops","@odata.type":"#microsoft.graph.group"}}]}`))
			return
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	client := newGraphTestClient(t, srv)

	directGroupMembers, err := client.ListGroupUserMembers(context.Background(), "group-1")
	if err != nil {
		t.Fatalf("ListGroupUserMembers() error = %v", err)
	}
	if len(directGroupMembers) != 2 {
		t.Fatalf("len(directGroupMembers)=%d want 2", len(directGroupMembers))
	}
	if entityID(directGroupMembers[0]) != "user-1" || entityID(directGroupMembers[1]) != "user-2" {
		t.Fatalf("unexpected direct group member ids [%q %q]", entityID(directGroupMembers[0]), entityID(directGroupMembers[1]))
	}

	groupMembers, err := client.ListGroupTransitiveUserMembers(context.Background(), "group-1")
	if err != nil {
		t.Fatalf("ListGroupTransitiveUserMembers() error = %v", err)
	}
	if len(groupMembers) != 2 {
		t.Fatalf("len(groupMembers)=%d want 2", len(groupMembers))
	}
	if entityID(groupMembers[0]) != "user-1" || entityID(groupMembers[1]) != "user-2" {
		t.Fatalf("unexpected group member ids [%q %q]", entityID(groupMembers[0]), entityID(groupMembers[1]))
	}

	roles, err := client.ListDirectoryRoles(context.Background())
	if err != nil {
		t.Fatalf("ListDirectoryRoles() error = %v", err)
	}
	if len(roles) != 2 {
		t.Fatalf("len(roles)=%d want 2", len(roles))
	}
	if got := stringValue(roles[0].GetTemplateId()); got != "tmpl-1" {
		t.Fatalf("first templateId=%q want %q", got, "tmpl-1")
	}
	if entityID(roles[0]) != "role-def-1" || entityID(roles[1]) != "role-def-2" || stringValue(roles[1].GetTemplateId()) != "" {
		t.Fatalf("unexpected role definitions ids=[%q %q] template2=%q", entityID(roles[0]), entityID(roles[1]), stringValue(roles[1].GetTemplateId()))
	}

	assignments, err := client.ListDirectoryRoleAssignments(context.Background())
	if err != nil {
		t.Fatalf("ListDirectoryRoleAssignments() error = %v", err)
	}
	if len(assignments) != 2 {
		t.Fatalf("len(assignments)=%d want 2", len(assignments))
	}
	if got := stringValue(assignments[0].GetPrincipalId()); got != "user-3" {
		t.Fatalf("first principalId=%q want %q", got, "user-3")
	}
	if got := stringValue(assignments[0].GetPrincipal().GetOdataType()); got != "#microsoft.graph.user" {
		t.Fatalf("first principal @odata.type=%q want %q", got, "#microsoft.graph.user")
	}
	if got := stringValue(assignments[1].GetPrincipalId()); got != "group-1" {
		t.Fatalf("second principalId=%q want %q", got, "group-1")
	}
	if got := entraDirectoryObjectDisplayName(assignments[1].GetPrincipal()); got != "Privileged Ops" {
		t.Fatalf("second principal displayName=%q want %q", got, "Privileged Ops")
	}
	if directGroupMemberRequests != 2 {
		t.Fatalf("directGroupMemberRequests=%d want 2", directGroupMemberRequests)
	}
	if groupMemberRequests != 2 {
		t.Fatalf("groupMemberRequests=%d want 2", groupMemberRequests)
	}
	if directoryRoleRequests != 1 {
		t.Fatalf("directoryRoleRequests=%d want 1", directoryRoleRequests)
	}
	if directoryRoleAssignmentRequests != 1 {
		t.Fatalf("directoryRoleAssignmentRequests=%d want 1", directoryRoleAssignmentRequests)
	}
}

type failingParsable struct{}

func (failingParsable) GetFieldDeserializers() map[string]func(absser.ParseNode) error {
	return nil
}

func (failingParsable) Serialize(absser.SerializationWriter) error {
	return errors.New("boom")
}

func TestSerializeSDKModelReturnsSerializationError(t *testing.T) {
	t.Parallel()

	_, err := serializeSDKModel(failingParsable{})
	if err == nil {
		t.Fatal("expected serializeSDKModel() error")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("serializeSDKModel() error = %v, want wrapped serialization error", err)
	}
}
