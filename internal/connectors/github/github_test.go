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
