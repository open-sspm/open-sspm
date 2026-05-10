package handlers

import (
	"context"
	"errors"
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
		upsertOktaPushIngestConfig(t, ctx, pool)

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
		upsertOktaPushIngestConfig(t, ctx, pool)

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
		upsertOktaPushIngestConfig(t, ctx, pool)
		queue := &stubOktaPushInboxQueue{}
		h.OktaPushInboxQueue = queue

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

		assertOktaPushInboxRow(t, ctx, pool, "acme.okta.com", "event_hook", "delivery-1", "evt-hook-1", "user.authentication.sso")
		if len(queue.ids) != 1 || queue.ids[0] <= 0 {
			t.Fatalf("queued redis ids = %#v, want one persisted row id", queue.ids)
		}
	})
}

func TestHandleOktaEventHookPostIgnoresQueueEnqueueFailure(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertOktaPushIngestConfig(t, ctx, pool)
		queue := &stubOktaPushInboxQueue{err: errors.New("redis unavailable")}
		h.OktaPushInboxQueue = queue

		body := `{
			"eventId": "delivery-enqueue-error",
			"data": {
				"events": [{
					"uuid": "evt-enqueue-error",
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
		assertOktaPushInboxRow(t, ctx, pool, "acme.okta.com", "event_hook", "delivery-enqueue-error", "evt-enqueue-error", "user.authentication.sso")
		if len(queue.ids) != 1 || queue.ids[0] <= 0 {
			t.Fatalf("queued redis ids = %#v, want one persisted row id", queue.ids)
		}
	})
}

func TestEnqueueOktaPushInboxRowsUsesIndependentContext(t *testing.T) {
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	var gotErr error
	queue := &stubOktaPushInboxQueue{
		onEnqueue: func(ctx context.Context) {
			cancelRequest()
			gotErr = ctx.Err()
		},
	}
	h := &Handlers{OktaPushInboxQueue: queue}

	h.enqueueOktaPushInboxRows(requestCtx, []int64{42})

	if gotErr != nil {
		t.Fatalf("enqueue context error after request cancel = %v, want nil", gotErr)
	}
	if len(queue.ids) != 1 || queue.ids[0] != 42 {
		t.Fatalf("queued ids = %#v, want [42]", queue.ids)
	}
}

func TestHandleOktaEventHookPostDropsNonDiscoveryEventsBeforeStorage(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertOktaPushIngestConfig(t, ctx, pool)

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

		assertNoOktaPushInboxRow(t, ctx, pool, "evt-policy-1")
	})
}

func TestHandleOktaEventHookPostQueuesStateRefreshEvents(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertOktaPushIngestConfig(t, ctx, pool)

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

		assertOktaPushInboxRow(t, ctx, pool, "acme.okta.com", "event_hook", "delivery-user-refresh", "evt-user-refresh-1", "user.lifecycle.deactivate")
	})
}

func TestHandleOktaEventHookPostRejectsDisabledIngestConfigurations(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(context.Context, *pgxpool.Pool, *Handlers)
		secret string
	}{
		{
			name: "global discovery disabled",
			setup: func(ctx context.Context, pool *pgxpool.Pool, h *Handlers) {
				upsertOktaPushIngestConfig(t, ctx, pool)
				h.Cfg.SyncDiscoveryEnabled = false
			},
			secret: "hook-secret",
		},
		{
			name: "connector disabled",
			setup: func(ctx context.Context, pool *pgxpool.Pool, _ *Handlers) {
				upsertOktaPushIngestConfigWith(t, ctx, pool, false, configstore.OktaConfig{
					Domain:              "acme.okta.com",
					Token:               "okta-token",
					DiscoveryEnabled:    true,
					DiscoveryIngestMode: configstore.OktaDiscoveryIngestModeHybrid,
					EventHookEnabled:    true,
					EventHookSecret:     "hook-secret",
				})
			},
			secret: "hook-secret",
		},
		{
			name: "discovery disabled",
			setup: func(ctx context.Context, pool *pgxpool.Pool, _ *Handlers) {
				upsertOktaPushIngestConfigWith(t, ctx, pool, true, configstore.OktaConfig{
					Domain:              "acme.okta.com",
					Token:               "okta-token",
					DiscoveryEnabled:    false,
					DiscoveryIngestMode: configstore.OktaDiscoveryIngestModeHybrid,
					EventHookEnabled:    true,
					EventHookSecret:     "hook-secret",
				})
			},
			secret: "hook-secret",
		},
		{
			name: "mode does not enable event hook",
			setup: func(ctx context.Context, pool *pgxpool.Pool, _ *Handlers) {
				upsertOktaPushIngestConfigWith(t, ctx, pool, true, configstore.OktaConfig{
					Domain:              "acme.okta.com",
					Token:               "okta-token",
					DiscoveryEnabled:    true,
					DiscoveryIngestMode: configstore.OktaDiscoveryIngestModePolling,
					EventHookEnabled:    true,
					EventHookSecret:     "hook-secret",
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
		upsertOktaPushIngestConfig(t, ctx, pool)

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

		assertOktaPushInboxRow(t, ctx, pool, "acme.okta.com", "eventbridge", "eventbridge-delivery-1", "evt-eventbridge-1", "app.oauth2.signon")
	})
}

func TestHandleOktaEventBridgePostRejectsUnexpectedSource(t *testing.T) {
	withCommandSearchTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, h *Handlers) {
		upsertOktaPushIngestConfig(t, ctx, pool)

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
		upsertOktaPushIngestConfig(t, ctx, pool)

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
		upsertOktaPushIngestConfig(t, ctx, pool)

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
		upsertOktaPushIngestConfig(t, ctx, pool)

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
		upsertOktaPushIngestConfig(t, ctx, pool)

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

func upsertOktaPushIngestConfig(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()

	upsertOktaPushIngestConfigWith(t, ctx, pool, true, configstore.OktaConfig{
		Domain:              "acme.okta.com",
		Token:               "okta-token",
		DiscoveryEnabled:    true,
		DiscoveryIngestMode: configstore.OktaDiscoveryIngestModeHybrid,
		EventHookEnabled:    true,
		EventHookSecret:     "hook-secret",
		EventBridgeEnabled:  true,
		EventBridgeSecret:   "eventbridge-secret",
	})
}

func upsertOktaPushIngestConfigWith(t *testing.T, ctx context.Context, pool *pgxpool.Pool, enabled bool, cfg configstore.OktaConfig) {
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

func assertOktaPushInboxRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sourceName, channel, deliveryExternalID, eventExternalID, eventType string) {
	t.Helper()

	var gotChannel, gotDeliveryExternalID, gotEventType, gotStatus string
	if err := pool.QueryRow(ctx, `
		SELECT channel, delivery_external_id, event_type, status
		FROM okta_push_inbox
		WHERE source_name = $1
		  AND event_external_id = $2
	`, sourceName, eventExternalID).Scan(&gotChannel, &gotDeliveryExternalID, &gotEventType, &gotStatus); err != nil {
		t.Fatalf("select okta_push_inbox row: %v", err)
	}
	if gotChannel != channel {
		t.Fatalf("channel = %q, want %q", gotChannel, channel)
	}
	if gotDeliveryExternalID != deliveryExternalID {
		t.Fatalf("delivery_external_id = %q, want %q", gotDeliveryExternalID, deliveryExternalID)
	}
	if gotEventType != eventType {
		t.Fatalf("event_type = %q, want %q", gotEventType, eventType)
	}
	if gotStatus != "queued" {
		t.Fatalf("status = %q, want queued", gotStatus)
	}
}

func assertNoOktaPushInboxRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventExternalID string) {
	t.Helper()

	var count int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM okta_push_inbox
		WHERE event_external_id = $1
	`, eventExternalID).Scan(&count); err != nil {
		t.Fatalf("count okta_push_inbox rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("okta_push_inbox rows for %q = %d, want 0", eventExternalID, count)
	}
}

type stubOktaPushInboxQueue struct {
	ids       []int64
	err       error
	onEnqueue func(context.Context)
}

func (q *stubOktaPushInboxQueue) Enqueue(ctx context.Context, ids []int64) error {
	if q.onEnqueue != nil {
		q.onEnqueue(ctx)
	}
	q.ids = append(q.ids, ids...)
	return q.err
}
