package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

func TestHandleOktaEventHookVerify(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertEventInboxConfig(t, ctx, pool)

		c, rec := newOktaIngestContext(http.MethodGet, "http://example.com/ingest/okta/events", "")
		c.Request().Header.Set("x-okta-verification-challenge", "challenge-123")
		c.Request().Header.Set(echo.HeaderAuthorization, "hook-secret")

		if err := h.HandleOktaEventHookVerify(c); err != nil {
			t.Fatalf("HandleOktaEventHookVerify(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
		if got := strings.TrimSpace(rec.Body.String()); got != `{"verification":"challenge-123"}` {
			t.Fatalf("body = %s, want verification JSON", got)
		}
	})
}

func TestHandleOktaEventHookVerifyAllowsMissingAuthorization(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertEventInboxConfig(t, ctx, pool)

		c, rec := newOktaIngestContext(http.MethodGet, "http://example.com/ingest/okta/events", "")
		c.Request().Header.Set("x-okta-verification-challenge", "challenge-123")

		if err := h.HandleOktaEventHookVerify(c); err != nil {
			t.Fatalf("HandleOktaEventHookVerify(): %v", err)
		}
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}
	})
}

func TestHandleOktaEventHookVerifyMissingAuthorizationWithoutConfigIsUnauthorized(t *testing.T) {
	withCommandSearchTestDatabase(t, func(_ context.Context, _ *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		c, _ := newOktaIngestContext(http.MethodGet, "http://example.com/ingest/okta/events", "")
		c.Request().Header.Set("x-okta-verification-challenge", "challenge-123")

		err := h.HandleOktaEventHookVerify(c)
		httpErr, ok := err.(*echo.HTTPError)
		if !ok {
			t.Fatalf("error = %T %[1]v, want *echo.HTTPError", err)
		}
		if httpErr.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want %d", httpErr.Code, http.StatusUnauthorized)
		}
	})
}

func TestHandleOktaEventHookPostQueuesEvents(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertEventInboxConfig(t, ctx, pool)

		body := `{
			"eventId": "delivery-1",
			"data": {
				"events": [{
					"uuid": "evt-hook-1",
					"eventType": "user.authentication.sso",
					"published": "2026-01-01T12:00:00Z",
					"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
					"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
				}]
			}
		}`
		c, rec := newOktaIngestContext(http.MethodPost, "http://example.com/ingest/okta/events", body)
		c.Request().Header.Set(echo.HeaderAuthorization, "hook-secret")

		if err := h.HandleOktaEventHookPost(c); err != nil {
			t.Fatalf("HandleOktaEventHookPost(): %v", err)
		}
		if rec.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusNoContent, rec.Body.String())
		}

		assertGenericEventInboxRow(t, ctx, pool, "okta", "acme.okta.com", "event_hook", "provider:evt-hook-1")
	})
}

func TestHandleOktaEventHookPostDropsNonDiscoveryEventsBeforeStorage(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertEventInboxConfig(t, ctx, pool)

		body := `{
			"eventId": "delivery-policy",
			"data": {
				"events": [{
					"uuid": "evt-policy-1",
					"eventType": "policy.lifecycle.update",
					"published": "2026-01-01T12:00:00Z",
					"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
					"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
				}]
			}
		}`
		c, rec := newOktaIngestContext(http.MethodPost, "http://example.com/ingest/okta/events", body)
		c.Request().Header.Set(echo.HeaderAuthorization, "hook-secret")

		if err := h.HandleOktaEventHookPost(c); err != nil {
			t.Fatalf("HandleOktaEventHookPost(): %v", err)
		}
		if rec.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusNoContent, rec.Body.String())
		}

		assertNoGenericEventInboxRow(t, ctx, pool, "provider:evt-policy-1")
	})
}

func TestHandleOktaEventHookPostQueuesStateRefreshEvents(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertEventInboxConfig(t, ctx, pool)

		body := `{
			"eventId": "delivery-user-refresh",
			"data": {
				"events": [{
					"uuid": "evt-user-refresh-1",
					"eventType": "user.lifecycle.deactivate",
					"published": "2026-01-01T12:00:00Z",
					"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"}
				}]
			}
		}`
		c, rec := newOktaIngestContext(http.MethodPost, "http://example.com/ingest/okta/events", body)
		c.Request().Header.Set(echo.HeaderAuthorization, "hook-secret")

		if err := h.HandleOktaEventHookPost(c); err != nil {
			t.Fatalf("HandleOktaEventHookPost(): %v", err)
		}
		if rec.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusNoContent, rec.Body.String())
		}

		assertGenericEventInboxRow(t, ctx, pool, "okta", "acme.okta.com", "event_hook", "provider:evt-user-refresh-1")
	})
}

func TestHandleOktaEventHookPostQueuesEventsWhenDiscoveryPollingDisabled(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertEventInboxConfigWith(t, ctx, pool, true, configstore.OktaConfig{
			Domain:           "acme.okta.com",
			Token:            "okta-token",
			DiscoveryEnabled: false,
			EventInboxMode:   configstore.OktaEventInboxModeHybrid,
			EventHookEnabled: true,
			EventHookSecret:  "hook-secret",
		})

		body := `{
			"eventId": "delivery-user-refresh",
			"data": {
				"events": [{
					"uuid": "evt-user-refresh-discovery-off",
					"eventType": "user.lifecycle.deactivate",
					"published": "2026-01-01T12:00:00Z",
					"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"}
				}]
			}
		}`
		c, rec := newOktaIngestContext(http.MethodPost, "http://example.com/ingest/okta/events", body)
		c.Request().Header.Set(echo.HeaderAuthorization, "hook-secret")

		if err := h.HandleOktaEventHookPost(c); err != nil {
			t.Fatalf("HandleOktaEventHookPost(): %v", err)
		}
		if rec.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusNoContent, rec.Body.String())
		}

		assertGenericEventInboxRow(t, ctx, pool, "okta", "acme.okta.com", "event_hook", "provider:evt-user-refresh-discovery-off")
	})
}

func TestHandleOktaEventHookPostRejectsDisabledIngestConfigurations(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(context.Context, *pgxpool.Pool, *Handlers)
		secret string
	}{
		{
			name: "global ingest disabled",
			setup: func(ctx context.Context, pool *pgxpool.Pool, h *Handlers) {
				upsertEventInboxConfig(t, ctx, pool)
				h.Cfg.EventInboxEnabled = false
			},
			secret: "hook-secret",
		},
		{
			name: "connector disabled",
			setup: func(ctx context.Context, pool *pgxpool.Pool, _ *Handlers) {
				upsertEventInboxConfigWith(t, ctx, pool, false, configstore.OktaConfig{
					Domain:           "acme.okta.com",
					Token:            "okta-token",
					DiscoveryEnabled: true,
					EventInboxMode:   configstore.OktaEventInboxModeHybrid,
					EventHookEnabled: true,
					EventHookSecret:  "hook-secret",
				})
			},
			secret: "hook-secret",
		},
		{
			name: "mode does not enable event hook",
			setup: func(ctx context.Context, pool *pgxpool.Pool, _ *Handlers) {
				upsertEventInboxConfigWith(t, ctx, pool, true, configstore.OktaConfig{
					Domain:           "acme.okta.com",
					Token:            "okta-token",
					DiscoveryEnabled: true,
					EventInboxMode:   configstore.OktaEventInboxModePolling,
					EventHookEnabled: true,
					EventHookSecret:  "hook-secret",
				})
			},
			secret: "hook-secret",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
				tc.setup(ctx, pool, h)

				c, _ := newOktaIngestContext(http.MethodPost, "http://example.com/ingest/okta/events", `{"data":{"events":[]}}`)
				c.Request().Header.Set(echo.HeaderAuthorization, tc.secret)

				err := h.HandleOktaEventHookPost(c)
				httpErr, ok := err.(*echo.HTTPError)
				if !ok {
					t.Fatalf("error = %T %[1]v, want *echo.HTTPError", err)
				}
				if httpErr.Code != http.StatusForbidden {
					t.Fatalf("status = %d, want %d", httpErr.Code, http.StatusForbidden)
				}
			})
		})
	}
}

func TestHandleOktaEventBridgePostQueuesEvent(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertEventInboxConfig(t, ctx, pool)

		body := `{
			"id": "eventbridge-delivery-1",
			"detail-type": "SystemLog",
			"source": "aws.partner/okta.com/acme/event-stream",
			"detail": {
				"uuid": "evt-eventbridge-1",
				"eventType": "app.oauth2.signon",
				"published": "2026-01-01T12:00:00Z",
				"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
				"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://oidc.example.com", "displayName": "OIDC App"}]
			}
		}`
		c, rec := newOktaIngestContext(http.MethodPost, "http://example.com/ingest/okta/eventbridge", body)
		c.Request().Header.Set(echo.HeaderAuthorization, "eventbridge-secret")

		if err := h.HandleOktaEventBridgePost(c); err != nil {
			t.Fatalf("HandleOktaEventBridgePost(): %v", err)
		}
		if rec.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusNoContent, rec.Body.String())
		}

		assertGenericEventInboxRow(t, ctx, pool, "okta", "acme.okta.com", "eventbridge", "provider:evt-eventbridge-1")
	})
}

func TestHandleOktaEventBridgePostRejectsUnexpectedSource(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertEventInboxConfig(t, ctx, pool)

		body := `{
			"id": "eventbridge-delivery-1",
			"detail-type": "SystemLog",
			"source": "custom.example",
			"detail": {
				"uuid": "evt-eventbridge-1",
				"eventType": "app.oauth2.signon",
				"published": "2026-01-01T12:00:00Z",
				"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
				"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://oidc.example.com", "displayName": "OIDC App"}]
			}
		}`
		c, _ := newOktaIngestContext(http.MethodPost, "http://example.com/ingest/okta/eventbridge", body)
		c.Request().Header.Set(echo.HeaderAuthorization, "eventbridge-secret")

		err := h.HandleOktaEventBridgePost(c)
		httpErr, ok := err.(*echo.HTTPError)
		if !ok {
			t.Fatalf("error = %T %[1]v, want *echo.HTTPError", err)
		}
		if httpErr.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", httpErr.Code, http.StatusBadRequest)
		}
	})
}

func TestHandleOktaEventHookPostRejectsBadAuthorization(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertEventInboxConfig(t, ctx, pool)

		c, _ := newOktaIngestContext(http.MethodPost, "http://example.com/ingest/okta/events", `{"data":{"events":[]}}`)
		c.Request().Header.Set(echo.HeaderAuthorization, "wrong")

		err := h.HandleOktaEventHookPost(c)
		httpErr, ok := err.(*echo.HTTPError)
		if !ok {
			t.Fatalf("error = %T %[1]v, want *echo.HTTPError", err)
		}
		if httpErr.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want %d", httpErr.Code, http.StatusForbidden)
		}
	})
}

func TestHandleOktaEventHookPostRejectsMissingAuthorization(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertEventInboxConfig(t, ctx, pool)

		c, _ := newOktaIngestContext(http.MethodPost, "http://example.com/ingest/okta/events", `{"data":{"events":[]}}`)

		err := h.HandleOktaEventHookPost(c)
		httpErr, ok := err.(*echo.HTTPError)
		if !ok {
			t.Fatalf("error = %T %[1]v, want *echo.HTTPError", err)
		}
		if httpErr.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want %d", httpErr.Code, http.StatusUnauthorized)
		}
	})
}

func TestHandleOktaEventHookPostRequiresJSONContentType(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertEventInboxConfig(t, ctx, pool)

		c, _ := newOktaIngestContext(http.MethodPost, "http://example.com/ingest/okta/events", `{"data":{"events":[]}}`)
		c.Request().Header.Del(echo.HeaderContentType)
		c.Request().Header.Set(echo.HeaderAuthorization, "hook-secret")

		err := h.HandleOktaEventHookPost(c)
		httpErr, ok := err.(*echo.HTTPError)
		if !ok {
			t.Fatalf("error = %T %[1]v, want *echo.HTTPError", err)
		}
		if httpErr.Code != http.StatusUnsupportedMediaType {
			t.Fatalf("status = %d, want %d", httpErr.Code, http.StatusUnsupportedMediaType)
		}
	})
}

func TestHandleOktaEventHookVerifyRejectsMissingChallenge(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertEventInboxConfig(t, ctx, pool)

		c, _ := newOktaIngestContext(http.MethodGet, "http://example.com/ingest/okta/events", "")
		c.Request().Header.Set(echo.HeaderAuthorization, "hook-secret")

		err := h.HandleOktaEventHookVerify(c)
		httpErr, ok := err.(*echo.HTTPError)
		if !ok {
			t.Fatalf("error = %T %[1]v, want *echo.HTTPError", err)
		}
		if httpErr.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", httpErr.Code, http.StatusBadRequest)
		}
	})
}

func upsertEventInboxConfig(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()

	upsertEventInboxConfigWith(t, ctx, pool, true, configstore.OktaConfig{
		Domain:             "acme.okta.com",
		Token:              "okta-token",
		DiscoveryEnabled:   true,
		EventInboxMode:     configstore.OktaEventInboxModeHybrid,
		EventHookEnabled:   true,
		EventHookSecret:    "hook-secret",
		EventBridgeEnabled: true,
		EventBridgeSecret:  "eventbridge-secret",
	})
}

func upsertEventInboxConfigWith(t *testing.T, ctx context.Context, pool *pgxpool.Pool, enabled bool, cfg configstore.OktaConfig) {
	t.Helper()

	upsertCommandSearchConnectorConfig(t, ctx, pool, configstore.KindOkta, enabled, cfg)
}

func newOktaIngestContext(method, target, body string) (*echo.Context, *httptest.ResponseRecorder) {
	e := echo.New()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	return e.NewContext(req, rec), rec
}

func assertGenericEventInboxRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sourceKind, sourceName, channel, dedupeKey string) {
	t.Helper()

	var gotStatus string
	if err := pool.QueryRow(ctx, `
		SELECT status::text
		FROM event_inbox
		WHERE source_kind = $1
		  AND source_name = $2
		  AND channel = $3
		  AND dedupe_key = $4
	`, sourceKind, sourceName, channel, dedupeKey).Scan(&gotStatus); err != nil {
		t.Fatalf("select event_inbox row: %v", err)
	}
	if gotStatus != "queued" {
		t.Fatalf("generic event_inbox status = %q, want queued", gotStatus)
	}
}

func assertNoGenericEventInboxRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, dedupeKey string) {
	t.Helper()

	var count int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM event_inbox
		WHERE dedupe_key = $1
	`, dedupeKey).Scan(&count); err != nil {
		t.Fatalf("count event_inbox rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("event_inbox rows for %q = %d, want 0", dedupeKey, count)
	}
}
