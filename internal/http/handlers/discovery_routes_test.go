package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/auth"
	oktaconnector "github.com/open-sspm/open-sspm/internal/connectors/okta"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/authn"
)

func TestHandleDiscoveryMutationsRequireAdmin(t *testing.T) {
	viewer := auth.Principal{UserID: 42, Role: auth.RoleViewer}

	t.Run("governance viewer forbidden", func(t *testing.T) {
		c, rec := newTestContext(http.MethodPost, "/discovery/apps/1/governance")
		c.SetPathValues(echo.PathValues{{Name: "id", Value: "1"}})
		c.Set(authn.ContextKeyPrincipal, viewer)

		h := &Handlers{}
		if err := h.HandleDiscoveryAppGovernancePost(c); err != nil {
			t.Fatalf("HandleDiscoveryAppGovernancePost() error = %v", err)
		}
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
		}
	})

	t.Run("binding viewer forbidden", func(t *testing.T) {
		c, rec := newTestContext(http.MethodPost, "/discovery/apps/1/binding")
		c.SetPathValues(echo.PathValues{{Name: "id", Value: "1"}})
		c.Set(authn.ContextKeyPrincipal, viewer)

		h := &Handlers{}
		if err := h.HandleDiscoveryAppBindingPost(c); err != nil {
			t.Fatalf("HandleDiscoveryAppBindingPost() error = %v", err)
		}
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
		}
	})

	t.Run("binding clear viewer forbidden", func(t *testing.T) {
		c, rec := newTestContext(http.MethodPost, "/discovery/apps/1/binding/clear")
		c.SetPathValues(echo.PathValues{{Name: "id", Value: "1"}})
		c.Set(authn.ContextKeyPrincipal, viewer)

		h := &Handlers{}
		if err := h.HandleDiscoveryAppBindingClearPost(c); err != nil {
			t.Fatalf("HandleDiscoveryAppBindingClearPost() error = %v", err)
		}
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
		}
	})
}

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
	if !strings.Contains(rec.Body.String(), "Blank Actor App") {
		t.Fatalf("response missing app content: %q", rec.Body.String())
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

func TestHandleDiscoveryMutations_DBBackedAdminWrites(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)
	ensureSaaSDiscoverySchema(t, harness.ctx, harness.tx)

	now := time.Now().UTC()
	appID := insertDiscoveryAppSeed(t, harness, discoveryAppSeed{
		CanonicalKey: "domain:bind.example.com",
		DisplayName:  "Binding App",
		SourceKind:   "okta",
		SourceName:   "acme.okta.com",
		SourceAppID:  "okta-bind",
		ObservedAt:   now,
	})

	const adminID int64 = 910001
	if _, err := harness.tx.Exec(harness.ctx, `
		INSERT INTO auth_users (id, email, password_hash, role, is_active, created_at, updated_at)
		VALUES ($1, $2, 'hash', 'admin', true, now(), now())
		ON CONFLICT (id) DO NOTHING
	`, adminID, fmt.Sprintf("admin-%d@example.com", adminID)); err != nil {
		t.Fatalf("seed admin user: %v", err)
	}

	owner, err := harness.q.CreateIdentity(harness.ctx, gen.CreateIdentityParams{
		Kind:         "human",
		DisplayName:  "Owner One",
		PrimaryEmail: "owner@example.com",
	})
	if err != nil {
		t.Fatalf("create owner identity: %v", err)
	}

	if err := harness.q.UpsertSaaSAppBinding(harness.ctx, gen.UpsertSaaSAppBindingParams{
		SaasAppID:           appID,
		ConnectorKind:       "datadog",
		ConnectorSourceName: harness.sourceName,
		BindingSource:       "auto",
		Confidence:          0.8,
		IsPrimary:           false,
		CreatedByAuthUserID: pgtype.Int8{},
	}); err != nil {
		t.Fatalf("seed auto binding: %v", err)
	}
	if _, err := harness.q.RecomputePrimarySaaSAppBindingBySaaSAppID(harness.ctx, appID); err != nil {
		t.Fatalf("seed auto binding primary recompute: %v", err)
	}

	govValues := url.Values{
		"owner_identity_id":    []string{fmt.Sprintf("%d", owner.ID)},
		"business_criticality": []string{"high"},
		"data_classification":  []string{"restricted"},
		"notes":                []string{"Owner confirmed"},
	}
	govCtx, govRec := newFormContext(http.MethodPost, discoveryAppHref(appID)+"/governance", govValues)
	govCtx.SetPathValues(echo.PathValues{{Name: "id", Value: fmt.Sprintf("%d", appID)}})
	govCtx.Set(authn.ContextKeyPrincipal, auth.Principal{UserID: adminID, Role: auth.RoleAdmin})
	if err := harness.handlers.HandleDiscoveryAppGovernancePost(govCtx); err != nil {
		t.Fatalf("HandleDiscoveryAppGovernancePost() error = %v", err)
	}
	if govRec.Code != http.StatusSeeOther {
		t.Fatalf("governance status = %d, want %d", govRec.Code, http.StatusSeeOther)
	}

	governance, err := harness.q.GetSaaSAppGovernanceViewBySaaSAppID(harness.ctx, appID)
	if err != nil {
		t.Fatalf("GetSaaSAppGovernanceViewBySaaSAppID() error = %v", err)
	}
	if !governance.OwnerIdentityID.Valid || governance.OwnerIdentityID.Int64 != owner.ID {
		t.Fatalf("owner identity = %#v, want %d", governance.OwnerIdentityID, owner.ID)
	}
	if governance.BusinessCriticality != "high" {
		t.Fatalf("business criticality = %q, want high", governance.BusinessCriticality)
	}
	if governance.DataClassification != "restricted" {
		t.Fatalf("data classification = %q, want restricted", governance.DataClassification)
	}

	bindValues := url.Values{
		"binding_target": []string{"github::" + harness.sourceName},
	}
	bindCtx, bindRec := newFormContext(http.MethodPost, discoveryAppHref(appID)+"/binding", bindValues)
	bindCtx.SetPathValues(echo.PathValues{{Name: "id", Value: fmt.Sprintf("%d", appID)}})
	bindCtx.Set(authn.ContextKeyPrincipal, auth.Principal{UserID: adminID, Role: auth.RoleAdmin})
	if err := harness.handlers.HandleDiscoveryAppBindingPost(bindCtx); err != nil {
		t.Fatalf("HandleDiscoveryAppBindingPost() error = %v", err)
	}
	if bindRec.Code != http.StatusSeeOther {
		t.Fatalf("binding status = %d, want %d", bindRec.Code, http.StatusSeeOther)
	}

	bindings, err := harness.q.ListSaaSAppBindingsBySaaSAppID(harness.ctx, appID)
	if err != nil {
		t.Fatalf("ListSaaSAppBindingsBySaaSAppID() error = %v", err)
	}
	manualPrimary := false
	for _, binding := range bindings {
		if binding.BindingSource == "manual" && binding.ConnectorKind == "github" && binding.ConnectorSourceName == harness.sourceName && binding.IsPrimary {
			manualPrimary = true
		}
	}
	if !manualPrimary {
		t.Fatalf("expected manual github binding to be primary, bindings=%#v", bindings)
	}

	clearCtx, clearRec := newFormContext(http.MethodPost, discoveryAppHref(appID)+"/binding/clear", url.Values{})
	clearCtx.SetPathValues(echo.PathValues{{Name: "id", Value: fmt.Sprintf("%d", appID)}})
	clearCtx.Set(authn.ContextKeyPrincipal, auth.Principal{UserID: adminID, Role: auth.RoleAdmin})
	if err := harness.handlers.HandleDiscoveryAppBindingClearPost(clearCtx); err != nil {
		t.Fatalf("HandleDiscoveryAppBindingClearPost() error = %v", err)
	}
	if clearRec.Code != http.StatusSeeOther {
		t.Fatalf("binding clear status = %d, want %d", clearRec.Code, http.StatusSeeOther)
	}

	bindings, err = harness.q.ListSaaSAppBindingsBySaaSAppID(harness.ctx, appID)
	if err != nil {
		t.Fatalf("ListSaaSAppBindingsBySaaSAppID() error = %v", err)
	}
	hasManual := false
	autoPrimary := false
	for _, binding := range bindings {
		if binding.BindingSource == "manual" {
			hasManual = true
		}
		if binding.BindingSource == "auto" && binding.IsPrimary {
			autoPrimary = true
		}
	}
	if hasManual {
		t.Fatalf("expected manual bindings to be cleared, bindings=%#v", bindings)
	}
	if !autoPrimary {
		t.Fatalf("expected auto binding to be restored as primary, bindings=%#v", bindings)
	}
}

func TestUpsertSaaSAppBinding_DoesNotDowngradeManualBinding(t *testing.T) {
	harness := newProgrammaticAccessRouteHarness(t)
	ensureSaaSDiscoverySchema(t, harness.ctx, harness.tx)

	appID := insertDiscoveryAppSeed(t, harness, discoveryAppSeed{
		CanonicalKey: "domain:manual-preserve.example.com",
		DisplayName:  "Manual Preserve App",
		SourceKind:   "okta",
		SourceName:   "acme.okta.com",
		SourceAppID:  "okta-manual-preserve",
		ObservedAt:   time.Now().UTC(),
	})

	if err := harness.q.UpsertSaaSAppBinding(harness.ctx, gen.UpsertSaaSAppBindingParams{
		SaasAppID:           appID,
		ConnectorKind:       "github",
		ConnectorSourceName: harness.sourceName,
		BindingSource:       "manual",
		Confidence:          1,
		IsPrimary:           true,
		CreatedByAuthUserID: pgtype.Int8{},
	}); err != nil {
		t.Fatalf("seed manual binding: %v", err)
	}

	if err := harness.q.UpsertSaaSAppBinding(harness.ctx, gen.UpsertSaaSAppBindingParams{
		SaasAppID:           appID,
		ConnectorKind:       "github",
		ConnectorSourceName: harness.sourceName,
		BindingSource:       "auto",
		Confidence:          0.8,
		IsPrimary:           false,
		CreatedByAuthUserID: pgtype.Int8{},
	}); err != nil {
		t.Fatalf("seed conflicting auto binding: %v", err)
	}

	bindings, err := harness.q.ListSaaSAppBindingsBySaaSAppID(harness.ctx, appID)
	if err != nil {
		t.Fatalf("ListSaaSAppBindingsBySaaSAppID() error = %v", err)
	}
	if len(bindings) != 1 {
		t.Fatalf("len(bindings) = %d, want 1", len(bindings))
	}

	binding := bindings[0]
	if binding.BindingSource != "manual" {
		t.Fatalf("binding source = %q, want manual", binding.BindingSource)
	}
	if binding.Confidence != 1 {
		t.Fatalf("binding confidence = %v, want 1", binding.Confidence)
	}
	if !binding.IsPrimary {
		t.Fatalf("binding primary = %v, want true", binding.IsPrimary)
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
	CanonicalKey string
	DisplayName  string
	SourceKind   string
	SourceName   string
	SourceAppID  string
	ObservedAt   time.Time
}

func insertDiscoveryAppSeed(t *testing.T, harness *programmaticAccessRouteHarness, seed discoveryAppSeed) int64 {
	t.Helper()

	observedAt := seed.ObservedAt.UTC()
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
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
		) VALUES ($1, $2, 'example.com', 'Example', $3, $3, now(), now())
		RETURNING id
	`, seed.CanonicalKey, seed.DisplayName, observedAt).Scan(&appID); err != nil {
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

func newFormContext(method, target string, values url.Values) (*echo.Context, *httptest.ResponseRecorder) {
	body := strings.NewReader(values.Encode())
	e := echo.New()
	req := httptest.NewRequest(method, target, body)
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationForm)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	return c, rec
}
