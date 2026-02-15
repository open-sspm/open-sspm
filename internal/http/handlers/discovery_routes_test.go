package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v5"
	oktaconnector "github.com/open-sspm/open-sspm/internal/connectors/okta"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

func TestHandleDiscoveryApps_HTMXTargetReturnsFragmentAndVary(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)
	ensureSaaSDiscoverySchema(t, harness.ctx, harness.tx)

	now := time.Now().UTC()
	appID := insertDiscoveryAppSeed(t, harness, discoveryAppSeed{
		CanonicalKey: "domain:example.com",
		DisplayName:  "Example App",
		SourceKind:   "okta",
		SourceName:   "acme.okta.com",
		SourceAppID:  "okta-example",
		ObservedAt:   now,
	})
	if appID <= 0 {
		t.Fatalf("insertDiscoveryAppSeed() returned invalid id: %d", appID)
	}

	c, rec := newTestContext(http.MethodGet, "/discovery/apps")
	c.Request().Header.Set("HX-Request", "true")
	c.Request().Header.Set("HX-Target", "discovery-apps-results")

	if err := harness.handlers.HandleDiscoveryApps(c); err != nil {
		t.Fatalf("HandleDiscoveryApps() error = %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	if !strings.Contains(body, `id="discovery-apps-results"`) {
		t.Fatalf("response missing discovery HTMX fragment root: %q", body)
	}
	if strings.Contains(strings.ToLower(body), "<!doctype html>") {
		t.Fatalf("response unexpectedly contains full document shell")
	}

	vary := parseVaryHeader(rec.Header().Get("Vary"))
	if vary["hx-request"] != 1 {
		t.Fatalf("Vary header missing hx-request: %v", vary)
	}
	if vary["hx-target"] != 1 {
		t.Fatalf("Vary header missing hx-target: %v", vary)
	}
}

func TestHandleDiscoveryApps_AllConfiguredExcludesUnconfiguredSources(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)
	ensureSaaSDiscoverySchema(t, harness.ctx, harness.tx)
	configureDiscoveryOktaSource(t, harness, "acme.okta.com")

	now := time.Now().UTC()
	insertDiscoveryAppSeed(t, harness, discoveryAppSeed{
		CanonicalKey: "domain:configured.example.com",
		DisplayName:  "Configured Source App",
		SourceKind:   "okta",
		SourceName:   "acme.okta.com",
		SourceAppID:  "okta-configured",
		ObservedAt:   now,
	})
	insertDiscoveryAppSeed(t, harness, discoveryAppSeed{
		CanonicalKey: "domain:legacy.example.com",
		DisplayName:  "Legacy Source App",
		SourceKind:   "okta",
		SourceName:   "legacy.okta.com",
		SourceAppID:  "okta-legacy",
		ObservedAt:   now,
	})

	c, rec := newTestContext(http.MethodGet, "/discovery/apps")
	if err := harness.handlers.HandleDiscoveryApps(c); err != nil {
		t.Fatalf("HandleDiscoveryApps() error = %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "Configured Source App") {
		t.Fatalf("response missing configured app row: %q", body)
	}
	if strings.Contains(body, "Legacy Source App") {
		t.Fatalf("response unexpectedly includes unconfigured source app: %q", body)
	}
}

func TestHandleDiscoveryApps_AppCellSuppressesDuplicateVendorAndMissingDomain(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)
	ensureSaaSDiscoverySchema(t, harness.ctx, harness.tx)
	configureDiscoveryOktaSource(t, harness, "acme.okta.com")

	now := time.Now().UTC()
	insertDiscoveryAppSeed(t, harness, discoveryAppSeed{
		CanonicalKey:     "name:irs-account:okta",
		DisplayName:      "IRS account",
		PrimaryDomain:    "",
		PrimaryDomainSet: true,
		VendorName:       "IRS account",
		VendorNameSet:    true,
		SourceKind:       "okta",
		SourceName:       "acme.okta.com",
		SourceAppID:      "okta-irs-account",
		ObservedAt:       now,
	})

	c, rec := newTestContext(http.MethodGet, "/discovery/apps")
	if err := harness.handlers.HandleDiscoveryApps(c); err != nil {
		t.Fatalf("HandleDiscoveryApps() error = %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "IRS account") {
		t.Fatalf("response missing seeded app row: %q", body)
	}
	if strings.Contains(body, `class="osspm-cell-secondary osspm-token"`) {
		t.Fatalf("response unexpectedly renders domain token for empty domain: %q", body)
	}
	if strings.Contains(body, `<span class="osspm-cell-secondary osspm-truncate" title="IRS account">IRS account</span>`) {
		t.Fatalf("response unexpectedly renders duplicate vendor row: %q", body)
	}
}

func TestHandleDiscoveryHotspots_EmptyDomainDoesNotRenderEmptySubline(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)
	ensureSaaSDiscoverySchema(t, harness.ctx, harness.tx)
	configureDiscoveryOktaSource(t, harness, "acme.okta.com")

	now := time.Now().UTC()
	appID := insertDiscoveryAppSeed(t, harness, discoveryAppSeed{
		CanonicalKey:     "name:domainless-hotspot:okta",
		DisplayName:      "Domainless Hotspot",
		PrimaryDomain:    "",
		PrimaryDomainSet: true,
		VendorName:       "Vendor",
		VendorNameSet:    true,
		SourceKind:       "okta",
		SourceName:       "acme.okta.com",
		SourceAppID:      "okta-domainless-hotspot",
		ObservedAt:       now,
	})

	if _, err := harness.tx.Exec(harness.ctx, `
		INSERT INTO saas_app_governance_overrides (saas_app_id, business_criticality, data_classification, notes)
		VALUES ($1, 'high', 'restricted', '')
		ON CONFLICT (saas_app_id) DO UPDATE
		SET business_criticality = EXCLUDED.business_criticality,
		    data_classification = EXCLUDED.data_classification,
		    notes = EXCLUDED.notes,
		    updated_at = now()
	`, appID); err != nil {
		t.Fatalf("seed governance override: %v", err)
	}

	if _, err := harness.tx.Exec(harness.ctx, `
		INSERT INTO saas_app_events (
			saas_app_id, source_kind, source_name, signal_kind, event_external_id,
			source_app_id, source_app_name, source_app_domain,
			actor_external_id, actor_email, actor_display_name,
			observed_at, scopes_json, raw_json, seen_in_run_id, seen_at, last_observed_run_id, last_observed_at
		) VALUES (
			$1, 'okta', 'acme.okta.com', 'oauth_grant', $2,
			'okta-domainless-hotspot', 'Domainless Hotspot', '',
			'actor-1', 'actor@example.com', 'Actor One',
			$3, '["application.readwrite.all"]'::jsonb, '{}'::jsonb, $4, now(), $4, now()
		)
	`, appID, fmt.Sprintf("evt-domainless-%d", now.UnixNano()), now, harness.runID); err != nil {
		t.Fatalf("seed privileged event: %v", err)
	}

	c, rec := newTestContext(http.MethodGet, "/discovery/hotspots")
	if err := harness.handlers.HandleDiscoveryHotspots(c); err != nil {
		t.Fatalf("HandleDiscoveryHotspots() error = %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "Domainless Hotspot") {
		t.Fatalf("response missing hotspot row: %q", body)
	}
	if strings.Contains(body, `class="text-xs text-muted-foreground"></div>`) {
		t.Fatalf("response unexpectedly renders empty hotspot app metadata subline: %q", body)
	}
}

func TestHandleDiscoveryAppShow_MissingDomainAndVendorShowNotAvailable(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)
	ensureSaaSDiscoverySchema(t, harness.ctx, harness.tx)

	now := time.Now().UTC()
	appID := insertDiscoveryAppSeed(t, harness, discoveryAppSeed{
		CanonicalKey:     "name:missing-metadata-app:okta",
		DisplayName:      "Missing Metadata App",
		PrimaryDomain:    "",
		PrimaryDomainSet: true,
		VendorName:       "",
		VendorNameSet:    true,
		SourceKind:       "okta",
		SourceName:       "acme.okta.com",
		SourceAppID:      "okta-missing-metadata",
		ObservedAt:       now,
	})

	c, rec := newTestContext(http.MethodGet, discoveryAppHref(appID))
	c.SetPathValues(echo.PathValues{{Name: "id", Value: fmt.Sprintf("%d", appID)}})
	if err := harness.handlers.HandleDiscoveryAppShow(c); err != nil {
		t.Fatalf("HandleDiscoveryAppShow() error = %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "Missing Metadata App") {
		t.Fatalf("response missing app content: %q", body)
	}
	if strings.Count(body, "Not available") < 2 {
		t.Fatalf("expected at least two Not available placeholders for domain/vendor: %q", body)
	}
}

func TestHandleDiscoveryAppShow_BlankTopActorIdentityDoesNotError(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)
	ensureSaaSDiscoverySchema(t, harness.ctx, harness.tx)

	now := time.Now().UTC()
	appID := insertDiscoveryAppSeed(t, harness, discoveryAppSeed{
		CanonicalKey: "domain:blank-actor.example.com",
		DisplayName:  "Blank Actor App",
		SourceKind:   "okta",
		SourceName:   "acme.okta.com",
		SourceAppID:  "okta-blank-actor",
		ObservedAt:   now,
	})

	if _, err := harness.tx.Exec(harness.ctx, `
		INSERT INTO saas_app_events (
			saas_app_id, source_kind, source_name, signal_kind, event_external_id,
			source_app_id, source_app_name, source_app_domain,
			actor_external_id, actor_email, actor_display_name,
			observed_at, scopes_json, raw_json, seen_in_run_id, seen_at, last_observed_run_id, last_observed_at
		) VALUES (
			$1, 'okta', 'acme.okta.com', 'idp_sso', $2,
			'okta-blank-actor', 'Blank Actor App', 'blank-actor.example.com',
			'', '', '',
			$3, '[]'::jsonb, '{}'::jsonb, $4, now(), $4, $3
		)
	`, appID, fmt.Sprintf("evt-blank-actor-%d", now.UnixNano()), now, harness.runID); err != nil {
		t.Fatalf("seed blank-actor discovery event: %v", err)
	}

	c, rec := newTestContext(http.MethodGet, discoveryAppHref(appID))
	c.SetPathValues(echo.PathValues{{Name: "id", Value: fmt.Sprintf("%d", appID)}})
	if err := harness.handlers.HandleDiscoveryAppShow(c); err != nil {
		t.Fatalf("HandleDiscoveryAppShow() error = %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "Blank Actor App") {
		t.Fatalf("response missing app content: %q", body)
	}
	if strings.Contains(body, "<h2>Bindings</h2>") {
		t.Fatalf("response unexpectedly contains bindings section: %q", body)
	}
}

func TestDiscoveryExpiryUsesStalenessWindow(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)
	ensureSaaSDiscoverySchema(t, harness.ctx, harness.tx)

	now := time.Now().UTC()
	oldAppID := insertDiscoveryAppSeed(t, harness, discoveryAppSeed{
		CanonicalKey: "domain:old.example.com",
		DisplayName:  "Old Evidence App",
		SourceKind:   "okta",
		SourceName:   "acme.okta.com",
		SourceAppID:  "okta-old",
		ObservedAt:   now.Add(-40 * 24 * time.Hour),
	})
	recentAppID := insertDiscoveryAppSeed(t, harness, discoveryAppSeed{
		CanonicalKey: "domain:recent.example.com",
		DisplayName:  "Recent Evidence App",
		SourceKind:   "okta",
		SourceName:   "acme.okta.com",
		SourceAppID:  "okta-recent",
		ObservedAt:   now.Add(-2 * 24 * time.Hour),
	})

	newRunID, err := harness.q.CreateSyncRun(harness.ctx, gen.CreateSyncRunParams{
		SourceKind: "okta",
		SourceName: "acme.okta.com",
	})
	if err != nil {
		t.Fatalf("CreateSyncRun() for expiry check error = %v", err)
	}

	insertEvent := func(appID int64, eventID string, observedAt time.Time) {
		t.Helper()
		if _, err := harness.tx.Exec(harness.ctx, `
			INSERT INTO saas_app_events (
				saas_app_id, source_kind, source_name, signal_kind, event_external_id,
				source_app_id, source_app_name, source_app_domain,
				actor_external_id, actor_email, actor_display_name,
				observed_at, scopes_json, raw_json, seen_in_run_id, seen_at, last_observed_run_id, last_observed_at
			) VALUES (
				$1, 'okta', 'acme.okta.com', 'idp_sso', $2,
				$3, $4, 'example.com',
				'actor', 'actor@example.com', 'Actor',
				$5, '[]'::jsonb, '{}'::jsonb, $6, now(), $6, $5
			)
		`, appID, eventID, eventID, eventID, observedAt, harness.runID); err != nil {
			t.Fatalf("insert discovery event %q: %v", eventID, err)
		}
	}

	insertEvent(oldAppID, "evt-old", now.Add(-40*24*time.Hour))
	insertEvent(recentAppID, "evt-recent", now.Add(-2*24*time.Hour))

	expiredSources, err := harness.q.ExpireSaaSAppSourcesNotSeenInRunBySource(harness.ctx, gen.ExpireSaaSAppSourcesNotSeenInRunBySourceParams{
		ExpiredRunID: newRunID,
		SourceKind:   "okta",
		SourceName:   "acme.okta.com",
	})
	if err != nil {
		t.Fatalf("ExpireSaaSAppSourcesNotSeenInRunBySource() error = %v", err)
	}
	if expiredSources != 1 {
		t.Fatalf("expired source rows = %d, want 1", expiredSources)
	}

	expiredEvents, err := harness.q.ExpireSaaSAppEventsNotSeenInRunBySource(harness.ctx, gen.ExpireSaaSAppEventsNotSeenInRunBySourceParams{
		ExpiredRunID: newRunID,
		SourceKind:   "okta",
		SourceName:   "acme.okta.com",
	})
	if err != nil {
		t.Fatalf("ExpireSaaSAppEventsNotSeenInRunBySource() error = %v", err)
	}
	if expiredEvents != 1 {
		t.Fatalf("expired event rows = %d, want 1", expiredEvents)
	}
}

func TestHandleDiscoveryHotspots_DBBackedRiskOrdering(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)
	ensureSaaSDiscoverySchema(t, harness.ctx, harness.tx)
	configureDiscoveryOktaSource(t, harness, "acme.okta.com")

	now := time.Now().UTC()
	highAppID := insertDiscoveryAppSeed(t, harness, discoveryAppSeed{
		CanonicalKey: "domain:high.example.com",
		DisplayName:  "High Risk App",
		SourceKind:   "okta",
		SourceName:   "acme.okta.com",
		SourceAppID:  "okta-high",
		ObservedAt:   now.Add(-2 * time.Minute),
	})
	criticalAppID := insertDiscoveryAppSeed(t, harness, discoveryAppSeed{
		CanonicalKey: "domain:critical.example.com",
		DisplayName:  "Critical Risk App",
		SourceKind:   "okta",
		SourceName:   "acme.okta.com",
		SourceAppID:  "okta-critical",
		ObservedAt:   now.Add(-1 * time.Minute),
	})

	for _, appID := range []int64{highAppID, criticalAppID} {
		if _, err := harness.tx.Exec(harness.ctx, `
			INSERT INTO saas_app_governance_overrides (saas_app_id, business_criticality, data_classification, notes)
			VALUES ($1, 'high', 'restricted', '')
			ON CONFLICT (saas_app_id) DO UPDATE
			SET business_criticality = EXCLUDED.business_criticality,
			    data_classification = EXCLUDED.data_classification,
			    notes = EXCLUDED.notes,
			    updated_at = now()
		`, appID); err != nil {
			t.Fatalf("seed governance override for app %d: %v", appID, err)
		}
	}

	if _, err := harness.tx.Exec(harness.ctx, `
		INSERT INTO saas_app_events (
			saas_app_id, source_kind, source_name, signal_kind, event_external_id,
			source_app_id, source_app_name, source_app_domain,
			actor_external_id, actor_email, actor_display_name,
			observed_at, scopes_json, raw_json, seen_in_run_id, seen_at, last_observed_run_id, last_observed_at
		) VALUES (
			$1, 'okta', 'acme.okta.com', 'oauth_grant', $2,
			'okta-critical', 'Critical Risk App', 'critical.example.com',
			'actor-critical', 'critical@example.com', 'Critical User',
			$3, '["application.readwrite.all"]'::jsonb, '{}'::jsonb, $4, now(), $4, now()
		)
	`, criticalAppID, fmt.Sprintf("evt-critical-%d", now.UnixNano()), now, harness.runID); err != nil {
		t.Fatalf("seed privileged event: %v", err)
	}

	c, rec := newTestContext(http.MethodGet, "/discovery/hotspots")
	if err := harness.handlers.HandleDiscoveryHotspots(c); err != nil {
		t.Fatalf("HandleDiscoveryHotspots() error = %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	criticalPos := strings.Index(body, "Critical Risk App")
	highPos := strings.Index(body, "High Risk App")
	if criticalPos == -1 || highPos == -1 {
		t.Fatalf("response missing expected hotspot rows: %q", body)
	}
	if criticalPos > highPos {
		t.Fatalf("hotspots not ordered by descending risk score: criticalPos=%d highPos=%d", criticalPos, highPos)
	}
}

func configureDiscoveryOktaSource(t *testing.T, harness *programmaticAccessRouteHarness, domain string) {
	t.Helper()

	if err := harness.handlers.Registry.Register(oktaconnector.NewDefinition(1)); err != nil {
		t.Fatalf("register okta connector definition: %v", err)
	}
	configJSON, err := json.Marshal(map[string]any{
		"domain": domain,
		"token":  "test-token",
	})
	if err != nil {
		t.Fatalf("marshal okta config: %v", err)
	}
	if _, err := harness.tx.Exec(harness.ctx, `
		INSERT INTO connector_configs (kind, enabled, config)
		VALUES ('okta', true, $1::jsonb)
		ON CONFLICT (kind) DO UPDATE
		SET enabled = EXCLUDED.enabled, config = EXCLUDED.config, updated_at = now()
	`, configJSON); err != nil {
		t.Fatalf("configure okta connector: %v", err)
	}
}

type discoveryAppSeed struct {
	CanonicalKey     string
	DisplayName      string
	PrimaryDomain    string
	PrimaryDomainSet bool
	VendorName       string
	VendorNameSet    bool
	SourceKind       string
	SourceName       string
	SourceAppID      string
	ObservedAt       time.Time
}

func insertDiscoveryAppSeed(t *testing.T, harness *programmaticAccessRouteHarness, seed discoveryAppSeed) int64 {
	t.Helper()

	observedAt := seed.ObservedAt.UTC()
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	primaryDomain := strings.TrimSpace(seed.PrimaryDomain)
	if !seed.PrimaryDomainSet {
		primaryDomain = "example.com"
	}
	vendorName := strings.TrimSpace(seed.VendorName)
	if !seed.VendorNameSet {
		vendorName = "Example"
	}

	var appID int64
	if err := harness.tx.QueryRow(harness.ctx, `
		INSERT INTO saas_apps (
			canonical_key,
			display_name,
			primary_domain,
			vendor_name,
			first_seen_at,
			last_seen_at,
			created_at,
			updated_at
		) VALUES ($1, $2, $3, $4, $5, $5, now(), now())
		RETURNING id
	`, seed.CanonicalKey, seed.DisplayName, primaryDomain, vendorName, observedAt).Scan(&appID); err != nil {
		t.Fatalf("insert saas app: %v", err)
	}

	if _, err := harness.tx.Exec(harness.ctx, `
		INSERT INTO saas_app_sources (
			saas_app_id, source_kind, source_name, source_app_id, source_app_name, source_app_domain,
			seen_in_run_id, seen_at, last_observed_run_id, last_observed_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'example.com', $6, $7, $6, $7, now(), now())
	`, appID, seed.SourceKind, seed.SourceName, seed.SourceAppID, seed.DisplayName, harness.runID, observedAt); err != nil {
		t.Fatalf("insert saas app source: %v", err)
	}

	return appID
}

func ensureSaaSDiscoverySchema(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()

	exists, err := relationExists(ctx, tx, "saas_apps")
	if err != nil {
		t.Fatalf("check saas_apps relation: %v", err)
	}
	if exists {
		return
	}

	migrationSQL, err := readFirstFile(
		filepath.Join("db", "migrations", "000022_saas_discovery.up.sql"),
		filepath.Join("..", "..", "..", "db", "migrations", "000022_saas_discovery.up.sql"),
	)
	if err != nil {
		t.Skipf("skipping DB-backed discovery route test: discovery migration not found: %v", err)
	}
	if _, err := tx.Exec(ctx, migrationSQL); err != nil {
		t.Skipf("skipping DB-backed discovery route test: applying discovery migration failed: %v", err)
	}
}
