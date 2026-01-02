package entra

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
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
