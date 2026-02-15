package github

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestBuildGitHubPATRequestCredentialRows(t *testing.T) {
	t.Parallel()

	rows := buildGitHubPATRequestCredentialRows("acme", []PersonalAccessTokenRequest{
		{
			ID:                  701,
			TokenID:             9001,
			TokenName:           "prod automation",
			Status:              "pending_review",
			OwnerLogin:          "octo-bot",
			OwnerID:             41,
			RepositorySelection: "all",
			Permissions:         map[string]string{"contents": "write"},
			CreatedAtRaw:        "2026-01-07T00:00:00Z",
			ExpiresAtRaw:        "2026-04-01T00:00:00Z",
			ReviewerLogin:       "security-admin",
			ReviewerID:          51,
		},
	})
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	row := rows[0]
	if row.CredentialKind != "github_pat_request" {
		t.Fatalf("credential kind=%q, want github_pat_request", row.CredentialKind)
	}
	if row.Status != "pending_approval" {
		t.Fatalf("status=%q, want pending_approval", row.Status)
	}
	if row.CreatedByKind != "github_user" || row.CreatedByExternalID != "octo-bot" {
		t.Fatalf("unexpected creator attribution: kind=%q external_id=%q", row.CreatedByKind, row.CreatedByExternalID)
	}
	if row.ApprovedByKind != "github_user" || row.ApprovedByExternalID != "security-admin" {
		t.Fatalf("unexpected approver attribution: kind=%q external_id=%q", row.ApprovedByKind, row.ApprovedByExternalID)
	}

	var scope map[string]any
	if err := json.Unmarshal(row.ScopeJSON, &scope); err != nil {
		t.Fatalf("scope json unmarshal: %v", err)
	}
	if got, _ := scope["organization"].(string); got != "acme" {
		t.Fatalf("scope organization=%v, want acme", scope["organization"])
	}
}

func TestBuildGitHubPATCredentialRowsStatus(t *testing.T) {
	t.Parallel()

	rows := buildGitHubPATCredentialRows("acme", []PersonalAccessToken{
		{
			ID:           9001,
			Name:         "prod automation",
			OwnerLogin:   "octo-bot",
			ExpiresAtRaw: "2020-01-01T00:00:00Z",
		},
		{
			ID:         9002,
			Name:       "revoked token",
			OwnerLogin: "octo-bot",
			Revoked:    true,
		},
	})
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].Status != "expired" {
		t.Fatalf("row[0] status=%q, want expired", rows[0].Status)
	}
	if rows[1].Status != "revoked" {
		t.Fatalf("row[1] status=%q, want revoked", rows[1].Status)
	}
}

func TestBuildGitHubAuditEventRowsPATRequest(t *testing.T) {
	t.Parallel()

	rows := buildGitHubAuditEventRows("acme", []AuditLogEvent{
		{
			DocumentID:   "doc-701",
			Action:       "org.personal_access_token_request.approve",
			Actor:        "security-admin",
			CreatedAtRaw: "2026-01-07T00:00:00Z",
			RequestID:    "701",
		},
	})
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	row := rows[0]
	if row.CredentialKind != "github_pat_request" {
		t.Fatalf("credential kind=%q, want github_pat_request", row.CredentialKind)
	}
	if row.CredentialExternalID != "701" {
		t.Fatalf("credential external id=%q, want 701", row.CredentialExternalID)
	}
	if row.TargetKind != "organization" || row.TargetExternalID != "acme" {
		t.Fatalf("unexpected target: kind=%q external_id=%q", row.TargetKind, row.TargetExternalID)
	}
}

func TestSyncProgrammaticAccessFailsWhenDatasetUnavailable(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/orgs/acme/repos":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[]`))
		case "/orgs/acme/personal-access-token-requests":
			http.Error(w, `{"message":"forbidden"}`, http.StatusForbidden)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	client, err := New(srv.URL, "token")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	integration := NewGitHubIntegration(client, "acme", "", 1, false)
	_, err = integration.syncProgrammaticAccess(context.Background(), nil, func(registry.Event) {}, 42)
	if err == nil {
		t.Fatalf("syncProgrammaticAccess() error = nil, want non-nil")
	}
	if !errors.Is(err, ErrDatasetUnavailable) {
		t.Fatalf("syncProgrammaticAccess() error = %v, want wrapped ErrDatasetUnavailable", err)
	}

	var syncErr *programmaticSyncError
	if !errors.As(err, &syncErr) {
		t.Fatalf("syncProgrammaticAccess() error = %T, want *programmaticSyncError", err)
	}
	if syncErr.kind != registry.SyncErrorKindAPI {
		t.Fatalf("syncProgrammaticAccess() error kind = %q, want %q", syncErr.kind, registry.SyncErrorKindAPI)
	}
}
