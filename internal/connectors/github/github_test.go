package github

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewSetsHTTPTimeout(t *testing.T) {
	t.Parallel()

	c, err := New("https://api.github.com", "token")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.HTTP == nil {
		t.Fatalf("expected HTTP client to be set")
	}
	if c.HTTP.Timeout <= 0 {
		t.Fatalf("expected non-zero HTTP timeout")
	}
}

func TestClientTimesOutOnSlowServer(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}))
	t.Cleanup(srv.Close)

	c, err := New(srv.URL, "token")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c.HTTP.Timeout = 50 * time.Millisecond

	_, err = c.ListTeams(context.Background(), "acme")
	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("expected timeout error, got %T: %v", err, err)
	}
}

func TestClientRetriesOn429(t *testing.T) {
	t.Parallel()

	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			w.Header().Set("Retry-After", "0")
			http.Error(w, `{"message":"rate limited"}`, http.StatusTooManyRequests)
			return
		}
		if r.URL.Path != "/orgs/acme/teams" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}))
	t.Cleanup(srv.Close)

	c, err := New(srv.URL, "token")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	teams, err := c.ListTeams(context.Background(), "acme")
	if err != nil {
		t.Fatalf("ListTeams: %v", err)
	}
	if len(teams) != 0 {
		t.Fatalf("expected 0 teams, got %d", len(teams))
	}
	if got := atomic.LoadInt32(&calls); got < 2 {
		t.Fatalf("expected retries, got %d calls", got)
	}
}

func TestClientRetriesGraphQLOn502(t *testing.T) {
	t.Parallel()

	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/graphql" {
			http.NotFound(w, r)
			return
		}

		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			w.Header().Set("Retry-After", "0")
			http.Error(w, "bad gateway", http.StatusBadGateway)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"data":{"organization":{"membersWithRole":{"edges":[],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}}`)
	}))
	t.Cleanup(srv.Close)

	c, err := New(srv.URL+"/api/v3", "token")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	members, err := c.listOrgMembersGraphQL(context.Background(), "acme")
	if err != nil {
		t.Fatalf("listOrgMembersGraphQL: %v", err)
	}
	if len(members) != 0 {
		t.Fatalf("expected 0 members, got %d", len(members))
	}
	if got := atomic.LoadInt32(&calls); got < 2 {
		t.Fatalf("expected retries, got %d calls", got)
	}
}

func TestProgrammaticGovernanceEndpoints(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/orgs/acme/repos":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":1,"name":"repo-one","full_name":"acme/repo-one","private":false,"archived":false,"disabled":false,"default_branch":"main","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-02T00:00:00Z","pushed_at":"2026-01-03T00:00:00Z"}]`))
			return
		case "/repos/acme/repo-one/keys":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":11,"key":"ssh-ed25519 AAAA1234567890 key@example","title":"deploy","read_only":true,"verified":true,"added_by":"alice","created_at":"2026-01-05T00:00:00Z","last_used":"2026-01-06T00:00:00Z"}]`))
			return
		case "/orgs/acme/personal-access-token-requests":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":701,"token_id":9001,"token_name":"prod automation","status":"pending_review","owner":{"login":"octo-bot","id":41},"repository_selection":"all","permissions":{"contents":"write"},"created_at":"2026-01-07T00:00:00Z","token_expires_at":"2026-04-01T00:00:00Z","reviewer":{"login":"security-admin","id":51}}]`))
			return
		case "/orgs/acme/personal-access-tokens":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":9001,"name":"prod automation","status":"active","owner":{"login":"octo-bot","id":41},"repository_selection":"all","permissions":{"contents":"write"},"created_at":"2026-01-07T00:00:00Z","updated_at":"2026-01-08T00:00:00Z","last_used_at":"2026-01-09T00:00:00Z","expires_at":"2026-04-01T00:00:00Z"}]`))
			return
		case "/orgs/acme/installations":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"installations":[{"id":101,"app_id":2001,"app_slug":"ops-app","app_name":"Ops App","account":{"login":"acme","type":"Organization","id":999},"repository_selection":"all","permissions":{"contents":"read"},"created_at":"2026-01-10T00:00:00Z","updated_at":"2026-01-11T00:00:00Z"}]}`))
			return
		case "/orgs/acme/audit-log":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"_document_id":"doc-1","action":"repo.deploy_key.create","actor":"alice","@timestamp":"2026-01-12T00:00:00Z","repo":"acme/repo-one","deploy_key_id":"11"}]`))
			return
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	c, err := New(srv.URL, "token")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	repos, err := c.ListOrgRepos(context.Background(), "acme")
	if err != nil {
		t.Fatalf("ListOrgRepos: %v", err)
	}
	if len(repos) != 1 || repos[0].FullName != "acme/repo-one" {
		t.Fatalf("unexpected repos result: %+v", repos)
	}

	keys, err := c.ListRepoDeployKeys(context.Background(), "acme", "repo-one")
	if err != nil {
		t.Fatalf("ListRepoDeployKeys: %v", err)
	}
	if len(keys) != 1 || keys[0].AddedBy != "alice" {
		t.Fatalf("unexpected keys result: %+v", keys)
	}

	patRequests, err := c.ListOrgPersonalAccessTokenRequests(context.Background(), "acme")
	if err != nil {
		t.Fatalf("ListOrgPersonalAccessTokenRequests: %v", err)
	}
	if len(patRequests) != 1 || patRequests[0].TokenID != 9001 || patRequests[0].OwnerLogin != "octo-bot" {
		t.Fatalf("unexpected pat requests result: %+v", patRequests)
	}

	pats, err := c.ListOrgPersonalAccessTokens(context.Background(), "acme")
	if err != nil {
		t.Fatalf("ListOrgPersonalAccessTokens: %v", err)
	}
	if len(pats) != 1 || pats[0].ID != 9001 || pats[0].Status != "active" {
		t.Fatalf("unexpected pats result: %+v", pats)
	}

	installations, err := c.ListOrgInstallations(context.Background(), "acme")
	if err != nil {
		t.Fatalf("ListOrgInstallations: %v", err)
	}
	if len(installations) != 1 || installations[0].AppSlug != "ops-app" {
		t.Fatalf("unexpected installations result: %+v", installations)
	}

	events, err := c.ListOrgAuditLog(context.Background(), "acme")
	if err != nil {
		t.Fatalf("ListOrgAuditLog: %v", err)
	}
	if len(events) != 1 || events[0].Action != "repo.deploy_key.create" {
		t.Fatalf("unexpected audit events result: %+v", events)
	}
}

func TestProgrammaticGovernanceUnavailable(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/orgs/acme/installations", "/orgs/acme/audit-log", "/orgs/acme/personal-access-token-requests", "/orgs/acme/personal-access-tokens":
			http.Error(w, `{"message":"forbidden"}`, http.StatusForbidden)
			return
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	c, err := New(srv.URL, "token")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := c.ListOrgInstallations(context.Background(), "acme"); !errors.Is(err, ErrDatasetUnavailable) {
		t.Fatalf("ListOrgInstallations error=%v, want ErrDatasetUnavailable", err)
	}

	if _, err := c.ListOrgAuditLog(context.Background(), "acme"); !errors.Is(err, ErrDatasetUnavailable) {
		t.Fatalf("ListOrgAuditLog error=%v, want ErrDatasetUnavailable", err)
	}

	if _, err := c.ListOrgPersonalAccessTokenRequests(context.Background(), "acme"); !errors.Is(err, ErrDatasetUnavailable) {
		t.Fatalf("ListOrgPersonalAccessTokenRequests error=%v, want ErrDatasetUnavailable", err)
	}

	if _, err := c.ListOrgPersonalAccessTokens(context.Background(), "acme"); !errors.Is(err, ErrDatasetUnavailable) {
		t.Fatalf("ListOrgPersonalAccessTokens error=%v, want ErrDatasetUnavailable", err)
	}
}
