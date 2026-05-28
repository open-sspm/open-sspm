package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	connregistry "github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type connectorHealthTestDefinition struct {
	kind        string
	displayName string
}

func (d connectorHealthTestDefinition) Kind() string        { return d.kind }
func (d connectorHealthTestDefinition) DisplayName() string { return d.displayName }
func (d connectorHealthTestDefinition) Role() connregistry.IntegrationRole {
	return connregistry.RoleApp
}
func (d connectorHealthTestDefinition) DecodeConfig([]byte) (any, error)              { return nil, nil }
func (d connectorHealthTestDefinition) ValidateConfig(any) error                      { return nil }
func (d connectorHealthTestDefinition) IsConfigured(any) bool                         { return true }
func (d connectorHealthTestDefinition) SourceName(any) string                         { return "" }
func (d connectorHealthTestDefinition) MetricsProvider() connregistry.MetricsProvider { return nil }
func (d connectorHealthTestDefinition) NewIntegration(any) (connregistry.Integration, error) {
	return nil, nil
}

func TestSizeConnectorHealthErrorMessage(t *testing.T) {
	t.Run("empty message", func(t *testing.T) {
		preview, full, previewTruncated, fullTruncated := sizeConnectorHealthErrorMessage("   ")
		if preview != "" || full != "" {
			t.Fatalf("expected empty preview/full, got preview=%q full=%q", preview, full)
		}
		if previewTruncated || fullTruncated {
			t.Fatalf("expected no truncation flags, got preview=%v full=%v", previewTruncated, fullTruncated)
		}
	})

	t.Run("preview truncation only", func(t *testing.T) {
		message := strings.Repeat("a", connectorHealthErrorPreviewRunes+5)
		preview, full, previewTruncated, fullTruncated := sizeConnectorHealthErrorMessage(message)

		if utf8.RuneCountInString(preview) != connectorHealthErrorPreviewRunes {
			t.Fatalf("preview rune count = %d, want %d", utf8.RuneCountInString(preview), connectorHealthErrorPreviewRunes)
		}
		if utf8.RuneCountInString(full) != connectorHealthErrorPreviewRunes+5 {
			t.Fatalf("full rune count = %d, want %d", utf8.RuneCountInString(full), connectorHealthErrorPreviewRunes+5)
		}
		if !previewTruncated {
			t.Fatalf("previewTruncated = false, want true")
		}
		if fullTruncated {
			t.Fatalf("fullTruncated = true, want false")
		}
	})

	t.Run("full truncation", func(t *testing.T) {
		message := strings.Repeat("b", connectorHealthErrorFullRunes+17)
		preview, full, previewTruncated, fullTruncated := sizeConnectorHealthErrorMessage(message)

		if utf8.RuneCountInString(full) != connectorHealthErrorFullRunes {
			t.Fatalf("full rune count = %d, want %d", utf8.RuneCountInString(full), connectorHealthErrorFullRunes)
		}
		if utf8.RuneCountInString(preview) != connectorHealthErrorPreviewRunes {
			t.Fatalf("preview rune count = %d, want %d", utf8.RuneCountInString(preview), connectorHealthErrorPreviewRunes)
		}
		if !previewTruncated {
			t.Fatalf("previewTruncated = false, want true")
		}
		if !fullTruncated {
			t.Fatalf("fullTruncated = false, want true")
		}
	})

	t.Run("preserves utf8 and line breaks", func(t *testing.T) {
		message := "line 1\n" + strings.Repeat("界", connectorHealthErrorPreviewRunes+1)
		preview, full, previewTruncated, fullTruncated := sizeConnectorHealthErrorMessage(message)

		if !utf8.ValidString(preview) {
			t.Fatalf("preview is not valid UTF-8")
		}
		if !utf8.ValidString(full) {
			t.Fatalf("full is not valid UTF-8")
		}
		if !strings.Contains(full, "\n") {
			t.Fatalf("expected line break to be preserved in full message")
		}
		if !previewTruncated {
			t.Fatalf("previewTruncated = false, want true")
		}
		if fullTruncated {
			t.Fatalf("fullTruncated = true, want false")
		}
	})
}

func TestConnectorHealthErrorDetailsURL(t *testing.T) {
	url := connectorHealthErrorDetailsURL(
		[]string{"github", "github"},
		[]connregistry.RunMode{connregistry.RunModeFull, connregistry.RunModeDiscovery},
		"acme org",
		"GitHub",
	)
	if !strings.HasPrefix(url, "/settings/connector-health/errors?") {
		t.Fatalf("unexpected url prefix: %q", url)
	}
	if !strings.Contains(url, "source_kind=github") {
		t.Fatalf("url missing source_kind: %q", url)
	}
	if !strings.Contains(url, "run_mode=full") {
		t.Fatalf("url missing full run_mode: %q", url)
	}
	if !strings.Contains(url, "run_mode=discovery") {
		t.Fatalf("url missing discovery run_mode: %q", url)
	}
	if !strings.Contains(url, "source_name=acme+org") {
		t.Fatalf("url missing encoded source_name: %q", url)
	}
	if !strings.Contains(url, "connector_name=GitHub") {
		t.Fatalf("url missing connector_name: %q", url)
	}
}

func TestConnectorHealthLanes_IncludeDiscoveryWhenEnabled(t *testing.T) {
	cfg := config.Config{
		SyncDiscoveryEnabled:  true,
		SyncInterval:          15 * time.Minute,
		SyncDiscoveryInterval: 30 * time.Minute,
	}
	state := connregistry.ConnectorState{
		Definition: connectorHealthTestDefinition{kind: configstore.KindGoogleWorkspace, displayName: "Google Workspace"},
		Config: configstore.GoogleWorkspaceConfig{
			DiscoveryEnabled: true,
		},
	}

	lanes := connectorHealthLanes(cfg, state)
	if len(lanes) != 2 {
		t.Fatalf("lane count = %d, want 2", len(lanes))
	}
	if lanes[0].syncKind != "google_workspace" || lanes[0].runMode != connregistry.RunModeFull ||
		lanes[1].syncKind != "google_workspace" || lanes[1].runMode != connregistry.RunModeDiscovery {
		t.Fatalf("lanes = %#v", lanes)
	}
}

func TestConnectorHealthRequestedRollupKeys_IncludeVault(t *testing.T) {
	cfg := config.Config{
		SyncDiscoveryEnabled:  true,
		SyncInterval:          15 * time.Minute,
		SyncDiscoveryInterval: 30 * time.Minute,
	}
	states := []connregistry.ConnectorState{
		{
			Definition: connectorHealthTestDefinition{kind: configstore.KindVault, displayName: "Vault"},
			Configured: true,
			SourceName: "prod-vault",
		},
		{
			Definition: connectorHealthTestDefinition{kind: configstore.KindGoogleWorkspace, displayName: "Google Workspace"},
			Configured: true,
			SourceName: "C0123",
			Config: configstore.GoogleWorkspaceConfig{
				DiscoveryEnabled: true,
			},
		},
	}

	keys := connectorHealthRequestedRollupKeys(cfg, states)
	if len(keys) != 3 {
		t.Fatalf("key count = %d, want 3", len(keys))
	}

	want := map[syncRollupKey]struct{}{
		{kind: configstore.KindVault, name: "prod-vault", mode: string(connregistry.RunModeFull)}:           {},
		{kind: configstore.KindGoogleWorkspace, name: "C0123", mode: string(connregistry.RunModeFull)}:      {},
		{kind: configstore.KindGoogleWorkspace, name: "C0123", mode: string(connregistry.RunModeDiscovery)}: {},
	}
	for _, key := range keys {
		if _, ok := want[key]; !ok {
			t.Fatalf("unexpected key = %#v", key)
		}
		delete(want, key)
	}
	if len(want) != 0 {
		t.Fatalf("missing keys = %#v", want)
	}
}

type stubConnectorHealthSyncRunner struct {
	err error
}

func (r stubConnectorHealthSyncRunner) RunOnce(context.Context) error {
	return r.err
}

func TestHandleConnectorHealthSyncHTMXReturnsPanelFragment(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, _ *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertCommandSearchConnectorConfig(t, ctx, h.Pool, configstore.KindGitHub, true, configstore.GitHubConfig{
			Org:   "acme",
			Token: "github-token",
		})
		h.Syncer = stubConnectorHealthSyncRunner{}

		c, rec := newConnectorHealthSyncFormContext(url.Values{
			"connector_kind": {configstore.KindGitHub},
			"source_name":    {"acme"},
		})
		c.Request().Header.Set("HX-Request", "true")
		c.Request().Header.Set("HX-Target", "connector-health-panel")

		if err := h.HandleConnectorHealthSync(c); err != nil {
			t.Fatalf("HandleConnectorHealthSync(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		if got := rec.Header().Get("HX-Redirect"); got != "" {
			t.Fatalf("HX-Redirect = %q, want empty", got)
		}
		trigger := rec.Header().Get("HX-Trigger")
		if !strings.Contains(trigger, `"osspm:toast"`) || !strings.Contains(trigger, `"osspm:connector-health-changed"`) {
			t.Fatalf("HX-Trigger = %q, want toast and connector-health event", trigger)
		}
		body := rec.Body.String()
		if !strings.Contains(body, `id="connector-health-panel"`) {
			t.Fatalf("body missing connector-health panel: %s", body)
		}
		if strings.Contains(body, `<body`) {
			t.Fatalf("body rendered full page instead of panel fragment")
		}
	})
}

func newConnectorHealthSyncFormContext(values url.Values) (*echo.Context, *httptest.ResponseRecorder) {
	req := httptest.NewRequest(http.MethodPost, "http://example.com/settings/connector-health/sync", strings.NewReader(values.Encode()))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationForm)
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	c.SetPath("/settings/connector-health/sync")
	return c, rec
}
