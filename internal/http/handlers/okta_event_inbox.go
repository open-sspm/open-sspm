package handlers

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	oktaconnector "github.com/open-sspm/open-sspm/internal/connectors/okta"
	genericinbox "github.com/open-sspm/open-sspm/internal/ingest/inbox"
	"github.com/open-sspm/open-sspm/internal/metrics"
	"github.com/open-sspm/open-sspm/internal/records"
)

const (
	eventInboxStatusQueued  = "queued"
	eventInboxStatusIgnored = "ignored"
)

type oktaEventHookEnvelope struct {
	EventID string `json:"eventId"`
	Data    struct {
		Events []json.RawMessage `json:"events"`
	} `json:"data"`
}

type oktaEventBridgeEnvelope struct {
	ID         string          `json:"id"`
	DetailType string          `json:"detail-type"`
	Source     string          `json:"source"`
	Detail     json.RawMessage `json:"detail"`
}

func (h *Handlers) HandleOktaEventHookVerify(c *echo.Context) error {
	challenge := strings.TrimSpace(c.Request().Header.Get("x-okta-verification-challenge"))
	if challenge == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "missing Okta verification challenge")
	}
	if _, err := h.resolveOktaEventHookVerificationSource(c.Request().Context(), c.Request().Header.Get(echo.HeaderAuthorization)); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]string{"verification": challenge})
}

func (h *Handlers) HandleOktaEventHookPost(c *echo.Context) error {
	if err := requireOktaEventInboxJSON(c); err != nil {
		return err
	}
	sourceName, err := h.resolveOktaEventInboxSource(c.Request().Context(), oktaconnector.PushChannelEventHook, c.Request().Header.Get(echo.HeaderAuthorization))
	if err != nil {
		return err
	}

	var envelope oktaEventHookEnvelope
	if err := json.NewDecoder(c.Request().Body).Decode(&envelope); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid Okta Event Hook JSON")
	}
	if envelope.Data.Events == nil {
		return echo.NewHTTPError(http.StatusBadRequest, "missing Okta Event Hook events")
	}
	if len(envelope.Data.Events) == 0 {
		return c.NoContent(http.StatusNoContent)
	}

	deliveries, ignored, err := oktaEventInboxDeliveriesFromRawEvents(sourceName, oktaconnector.PushChannelEventHook, envelope.EventID, envelope.Data.Events)
	if err != nil {
		return err
	}
	if ignored > 0 {
		metrics.EventInboxDeliveriesReceivedTotal.WithLabelValues(configstore.KindOkta, sourceName, oktaconnector.PushChannelEventHook, eventInboxStatusIgnored).Add(float64(ignored))
	}
	queued := len(deliveries)
	if queued == 0 {
		return c.NoContent(http.StatusNoContent)
	}
	if err := h.enqueueOktaEventInboxDeliveries(c.Request().Context(), deliveries); err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "could not persist Okta Event Hook delivery")
	}
	metrics.EventInboxDeliveriesReceivedTotal.WithLabelValues(configstore.KindOkta, sourceName, oktaconnector.PushChannelEventHook, eventInboxStatusQueued).Add(float64(queued))
	return c.NoContent(http.StatusNoContent)
}

func (h *Handlers) HandleOktaEventBridgePost(c *echo.Context) error {
	if err := requireOktaEventInboxJSON(c); err != nil {
		return err
	}
	sourceName, err := h.resolveOktaEventInboxSource(c.Request().Context(), oktaconnector.PushChannelEventBridge, c.Request().Header.Get(echo.HeaderAuthorization))
	if err != nil {
		return err
	}

	var envelope oktaEventBridgeEnvelope
	if err := json.NewDecoder(c.Request().Body).Decode(&envelope); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid Okta EventBridge JSON")
	}
	if strings.TrimSpace(envelope.DetailType) != "SystemLog" {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid Okta EventBridge detail type")
	}
	if !strings.HasPrefix(strings.TrimSpace(envelope.Source), "aws.partner/okta.com/") {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid Okta EventBridge source")
	}
	if len(envelope.Detail) == 0 {
		return echo.NewHTTPError(http.StatusBadRequest, "missing Okta EventBridge detail")
	}

	deliveries, ignored, err := oktaEventInboxDeliveriesFromRawEvents(sourceName, oktaconnector.PushChannelEventBridge, envelope.ID, []json.RawMessage{envelope.Detail})
	if err != nil {
		return err
	}
	if ignored > 0 {
		metrics.EventInboxDeliveriesReceivedTotal.WithLabelValues(configstore.KindOkta, sourceName, oktaconnector.PushChannelEventBridge, eventInboxStatusIgnored).Add(float64(ignored))
	}
	queued := len(deliveries)
	if queued == 0 {
		return c.NoContent(http.StatusNoContent)
	}
	if err := h.enqueueOktaEventInboxDeliveries(c.Request().Context(), deliveries); err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "could not persist Okta EventBridge delivery")
	}
	metrics.EventInboxDeliveriesReceivedTotal.WithLabelValues(configstore.KindOkta, sourceName, oktaconnector.PushChannelEventBridge, eventInboxStatusQueued).Add(float64(queued))
	return c.NoContent(http.StatusNoContent)
}

func (h *Handlers) enqueueOktaEventInboxDeliveries(ctx context.Context, deliveries []genericinbox.Delivery) error {
	if h == nil || h.Q == nil || len(deliveries) == 0 {
		return nil
	}
	store := genericinbox.NewStore(h.Q)
	for _, delivery := range deliveries {
		if _, err := store.Enqueue(ctx, delivery); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handlers) resolveOktaEventHookVerificationSource(ctx context.Context, authorization string) (string, error) {
	if strings.TrimSpace(authorization) != "" {
		return h.resolveOktaEventInboxSource(ctx, oktaconnector.PushChannelEventHook, authorization)
	}
	candidates, err := h.oktaEventInboxSecretCandidates(ctx, oktaconnector.PushChannelEventHook)
	if err != nil {
		if httpErr, ok := err.(*echo.HTTPError); ok && httpErr.Code == http.StatusForbidden {
			return "", echo.NewHTTPError(http.StatusUnauthorized, "missing Okta event inbox authorization")
		}
		return "", err
	}
	if len(candidates) != 1 {
		return "", echo.NewHTTPError(http.StatusUnauthorized, "missing Okta event inbox authorization")
	}
	return candidates[0].sourceName, nil
}

func (h *Handlers) resolveOktaEventInboxSource(ctx context.Context, channel, authorization string) (string, error) {
	authorization = strings.TrimSpace(authorization)
	if authorization == "" {
		return "", echo.NewHTTPError(http.StatusUnauthorized, "missing Okta event inbox authorization")
	}
	candidates, err := h.oktaEventInboxSecretCandidates(ctx, channel)
	if err != nil {
		return "", err
	}
	var sourceName string
	matches := 0
	for _, candidate := range candidates {
		if constantTimeEqualString(authorization, candidate.secret) {
			matches++
			sourceName = candidate.sourceName
		}
	}
	switch matches {
	case 0:
		return "", echo.NewHTTPError(http.StatusForbidden, "invalid Okta event inbox authorization")
	case 1:
		return sourceName, nil
	default:
		return "", echo.NewHTTPError(http.StatusServiceUnavailable, "Okta event inbox authorization matches multiple sources")
	}
}

type oktaEventInboxSecretCandidate struct {
	sourceName string
	secret     string
}

func (h *Handlers) oktaEventInboxSecretCandidates(ctx context.Context, channel string) ([]oktaEventInboxSecretCandidate, error) {
	if !oktaconnector.IsPushChannel(channel) {
		return nil, echo.NewHTTPError(http.StatusBadRequest, "unknown Okta event inbox channel")
	}
	if !h.Cfg.EventInboxEnabled {
		return nil, echo.NewHTTPError(http.StatusForbidden, "Okta event inbox is not enabled")
	}

	store := h.connectorConfigStore()
	rows, secretRowsByKind, err := store.ListConnectorConfigsWithSecretRows(ctx)
	if err != nil {
		return nil, echo.NewHTTPError(http.StatusServiceUnavailable, "could not load Okta event inbox configuration")
	}

	candidates := make([]oktaEventInboxSecretCandidate, 0, 1)
	for _, row := range rows {
		if !strings.EqualFold(strings.TrimSpace(row.Kind), configstore.KindOkta) {
			continue
		}
		if !row.Enabled {
			continue
		}
		resolved, err := store.ResolveConnectorConfigRowWithSecretRows(row, secretRowsByKind[configstore.KindOkta])
		if err != nil {
			return nil, echo.NewHTTPError(http.StatusServiceUnavailable, "could not resolve Okta event inbox configuration")
		}
		cfg, err := configstore.DecodeOktaConfig(resolved.ResolvedConfig)
		if err != nil {
			return nil, echo.NewHTTPError(http.StatusServiceUnavailable, "could not decode Okta event inbox configuration")
		}
		cfg = cfg.Normalized()
		if !oktaEventInboxChannelAllowed(cfg, channel) {
			continue
		}
		sourceName := strings.TrimSpace(cfg.Domain)
		if sourceName == "" {
			continue
		}
		secret := ""
		switch channel {
		case oktaconnector.PushChannelEventHook:
			secret = cfg.EventHookSecret
		case oktaconnector.PushChannelEventBridge:
			secret = cfg.EventBridgeSecret
		}
		if strings.TrimSpace(secret) == "" {
			continue
		}
		candidates = append(candidates, oktaEventInboxSecretCandidate{
			sourceName: sourceName,
			secret:     strings.TrimSpace(secret),
		})
	}
	if len(candidates) == 0 {
		return nil, echo.NewHTTPError(http.StatusForbidden, "Okta event inbox is not enabled")
	}
	for i, candidate := range candidates {
		for j := i + 1; j < len(candidates); j++ {
			if constantTimeEqualString(candidate.secret, candidates[j].secret) {
				return nil, echo.NewHTTPError(http.StatusServiceUnavailable, "Okta event inbox secrets must be unique")
			}
		}
	}
	return candidates, nil
}

func oktaEventInboxChannelAllowed(cfg configstore.OktaConfig, channel string) bool {
	switch channel {
	case oktaconnector.PushChannelEventHook:
		return cfg.EventHookEnabled &&
			(cfg.EventInboxMode == configstore.OktaEventInboxModeEventHook ||
				cfg.EventInboxMode == configstore.OktaEventInboxModeHybrid)
	case oktaconnector.PushChannelEventBridge:
		return cfg.EventBridgeEnabled &&
			(cfg.EventInboxMode == configstore.OktaEventInboxModeEventBridge ||
				cfg.EventInboxMode == configstore.OktaEventInboxModeHybrid)
	default:
		return false
	}
}

func oktaEventInboxDeliveriesFromRawEvents(sourceName, channel, deliveryExternalID string, rawEvents []json.RawMessage) ([]genericinbox.Delivery, int, error) {
	deliveries := make([]genericinbox.Delivery, 0, len(rawEvents))
	ignored := 0
	for idx, raw := range rawEvents {
		event, err := oktaconnector.MapSystemLogEventJSON(raw)
		if err != nil {
			return nil, 0, echo.NewHTTPError(http.StatusBadRequest, "invalid Okta System Log event")
		}
		if !oktaconnector.ShouldIngestEventInboxEvent(event) {
			ignored++
			continue
		}
		deliveries = append(deliveries, genericinbox.Delivery{
			Source:          records.SourceRef{Kind: configstore.KindOkta, Name: sourceName},
			Channel:         channel,
			ExternalEventID: strings.TrimSpace(event.ID),
			DedupeKey:       "provider:" + strings.TrimSpace(event.ID),
			RawBody:         []byte(raw),
			DecodedSummary: map[string]any{
				"delivery_external_id": strings.TrimSpace(deliveryExternalID),
				"event_type":           strings.TrimSpace(event.EventType),
				"event_index":          idx,
				"published_at":         event.Published.UTC().Format(time.RFC3339),
			},
		})
	}
	return deliveries, ignored, nil
}

func constantTimeEqualString(a, b string) bool {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	if a == "" || b == "" {
		return false
	}
	// Hash first so the comparison always runs over equal-length inputs.
	aHash := sha256.Sum256([]byte(a))
	bHash := sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(aHash[:], bHash[:]) == 1
}

func requireOktaEventInboxJSON(c *echo.Context) error {
	contentType := strings.TrimSpace(c.Request().Header.Get(echo.HeaderContentType))
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil || !strings.EqualFold(mediaType, echo.MIMEApplicationJSON) {
		return echo.NewHTTPError(http.StatusUnsupportedMediaType, "Okta event inbox requires application/json")
	}
	return nil
}
