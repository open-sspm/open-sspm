package handlers

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	oktaconnector "github.com/open-sspm/open-sspm/internal/connectors/okta"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	genericinbox "github.com/open-sspm/open-sspm/internal/ingest/inbox"
	"github.com/open-sspm/open-sspm/internal/metrics"
	"github.com/open-sspm/open-sspm/internal/records"
)

const (
	oktaPushChannelEventHook   = "event_hook"
	oktaPushChannelEventBridge = "eventbridge"

	oktaPushStatusQueued  = "queued"
	oktaPushStatusIgnored = "ignored"

	oktaPushInboxEnqueueTimeout = 2 * time.Second
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
	if err := requireOktaIngestJSON(c); err != nil {
		return err
	}
	sourceName, err := h.resolveOktaPushSource(c.Request().Context(), oktaPushChannelEventHook, c.Request().Header.Get(echo.HeaderAuthorization))
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

	params, genericDeliveries, ignored, err := oktaPushInboxParamsFromRawEvents(sourceName, oktaPushChannelEventHook, envelope.EventID, envelope.Data.Events)
	if err != nil {
		return err
	}
	if ignored > 0 {
		metrics.OktaPushEventsReceivedTotal.WithLabelValues(sourceName, oktaPushChannelEventHook, oktaPushStatusIgnored).Add(float64(ignored))
	}
	queued := len(params.EventExternalIds)
	if queued == 0 {
		return c.NoContent(http.StatusNoContent)
	}
	if err := h.enqueueGenericOktaInboxDeliveries(c.Request().Context(), genericDeliveries); err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "could not persist generic Okta Event Hook delivery")
	}
	ids, err := h.Q.UpsertOktaPushInboxEventsBulk(c.Request().Context(), params)
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "could not persist Okta Event Hook delivery")
	}
	h.enqueueOktaPushInboxRows(c.Request().Context(), ids)
	metrics.OktaPushEventsReceivedTotal.WithLabelValues(sourceName, oktaPushChannelEventHook, oktaPushStatusQueued).Add(float64(queued))
	return c.NoContent(http.StatusNoContent)
}

func (h *Handlers) HandleOktaEventBridgePost(c *echo.Context) error {
	if err := requireOktaIngestJSON(c); err != nil {
		return err
	}
	sourceName, err := h.resolveOktaPushSource(c.Request().Context(), oktaPushChannelEventBridge, c.Request().Header.Get(echo.HeaderAuthorization))
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

	params, genericDeliveries, ignored, err := oktaPushInboxParamsFromRawEvents(sourceName, oktaPushChannelEventBridge, envelope.ID, []json.RawMessage{envelope.Detail})
	if err != nil {
		return err
	}
	if ignored > 0 {
		metrics.OktaPushEventsReceivedTotal.WithLabelValues(sourceName, oktaPushChannelEventBridge, oktaPushStatusIgnored).Add(float64(ignored))
	}
	queued := len(params.EventExternalIds)
	if queued == 0 {
		return c.NoContent(http.StatusNoContent)
	}
	if err := h.enqueueGenericOktaInboxDeliveries(c.Request().Context(), genericDeliveries); err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "could not persist generic Okta EventBridge delivery")
	}
	ids, err := h.Q.UpsertOktaPushInboxEventsBulk(c.Request().Context(), params)
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "could not persist Okta EventBridge delivery")
	}
	h.enqueueOktaPushInboxRows(c.Request().Context(), ids)
	metrics.OktaPushEventsReceivedTotal.WithLabelValues(sourceName, oktaPushChannelEventBridge, oktaPushStatusQueued).Add(float64(queued))
	return c.NoContent(http.StatusNoContent)
}

func (h *Handlers) enqueueGenericOktaInboxDeliveries(ctx context.Context, deliveries []genericinbox.Delivery) error {
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

func (h *Handlers) enqueueOktaPushInboxRows(ctx context.Context, ids []int64) {
	if h == nil || h.OktaPushInboxQueue == nil || len(ids) == 0 {
		return
	}
	baseCtx := context.Background()
	if ctx != nil {
		baseCtx = context.WithoutCancel(ctx)
	}
	enqueueCtx, cancel := context.WithTimeout(baseCtx, oktaPushInboxEnqueueTimeout)
	defer cancel()
	if err := h.OktaPushInboxQueue.Enqueue(enqueueCtx, ids); err != nil {
		slog.Warn("Okta push inbox redis enqueue failed; Postgres poller will recover", "error", err, "rows", len(ids))
	}
}

func (h *Handlers) resolveOktaEventHookVerificationSource(ctx context.Context, authorization string) (string, error) {
	if strings.TrimSpace(authorization) != "" {
		return h.resolveOktaPushSource(ctx, oktaPushChannelEventHook, authorization)
	}
	candidates, err := h.oktaPushSecretCandidates(ctx, oktaPushChannelEventHook)
	if err != nil {
		if httpErr, ok := err.(*echo.HTTPError); ok && httpErr.Code == http.StatusForbidden {
			return "", echo.NewHTTPError(http.StatusUnauthorized, "missing Okta ingest authorization")
		}
		return "", err
	}
	if len(candidates) != 1 {
		return "", echo.NewHTTPError(http.StatusUnauthorized, "missing Okta ingest authorization")
	}
	return candidates[0].sourceName, nil
}

func (h *Handlers) resolveOktaPushSource(ctx context.Context, channel, authorization string) (string, error) {
	authorization = strings.TrimSpace(authorization)
	if authorization == "" {
		return "", echo.NewHTTPError(http.StatusUnauthorized, "missing Okta ingest authorization")
	}
	candidates, err := h.oktaPushSecretCandidates(ctx, channel)
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
		return "", echo.NewHTTPError(http.StatusForbidden, "invalid Okta ingest authorization")
	case 1:
		return sourceName, nil
	default:
		return "", echo.NewHTTPError(http.StatusServiceUnavailable, "Okta ingest authorization matches multiple sources")
	}
}

type oktaPushSecretCandidate struct {
	sourceName string
	secret     string
}

func (h *Handlers) oktaPushSecretCandidates(ctx context.Context, channel string) ([]oktaPushSecretCandidate, error) {
	if !oktaPushKnownChannel(channel) {
		return nil, echo.NewHTTPError(http.StatusBadRequest, "unknown Okta ingest channel")
	}
	if !h.Cfg.SyncDiscoveryEnabled {
		return nil, echo.NewHTTPError(http.StatusForbidden, "Okta ingest is not enabled")
	}

	store := h.connectorConfigStore()
	rows, secretRowsByKind, err := store.ListConnectorConfigsWithSecretRows(ctx)
	if err != nil {
		return nil, echo.NewHTTPError(http.StatusServiceUnavailable, "could not load Okta ingest configuration")
	}

	candidates := make([]oktaPushSecretCandidate, 0, 1)
	for _, row := range rows {
		if !strings.EqualFold(strings.TrimSpace(row.Kind), configstore.KindOkta) {
			continue
		}
		if !row.Enabled {
			continue
		}
		resolved, err := store.ResolveConnectorConfigRowWithSecretRows(row, secretRowsByKind[configstore.KindOkta])
		if err != nil {
			return nil, echo.NewHTTPError(http.StatusServiceUnavailable, "could not resolve Okta ingest configuration")
		}
		cfg, err := configstore.DecodeOktaConfig(resolved.ResolvedConfig)
		if err != nil {
			return nil, echo.NewHTTPError(http.StatusServiceUnavailable, "could not decode Okta ingest configuration")
		}
		cfg = cfg.Normalized()
		if !cfg.DiscoveryEnabled || !oktaPushChannelAllowed(cfg, channel) {
			continue
		}
		sourceName := strings.TrimSpace(cfg.Domain)
		if sourceName == "" {
			continue
		}
		secret := ""
		switch channel {
		case oktaPushChannelEventHook:
			secret = cfg.EventHookSecret
		case oktaPushChannelEventBridge:
			secret = cfg.EventBridgeSecret
		}
		if strings.TrimSpace(secret) == "" {
			continue
		}
		candidates = append(candidates, oktaPushSecretCandidate{
			sourceName: sourceName,
			secret:     strings.TrimSpace(secret),
		})
	}
	if len(candidates) == 0 {
		return nil, echo.NewHTTPError(http.StatusForbidden, "Okta ingest is not enabled")
	}
	for i, candidate := range candidates {
		for j := i + 1; j < len(candidates); j++ {
			if constantTimeEqualString(candidate.secret, candidates[j].secret) {
				return nil, echo.NewHTTPError(http.StatusServiceUnavailable, "Okta ingest secrets must be unique")
			}
		}
	}
	return candidates, nil
}

func oktaPushKnownChannel(channel string) bool {
	return channel == oktaPushChannelEventHook || channel == oktaPushChannelEventBridge
}

func oktaPushChannelAllowed(cfg configstore.OktaConfig, channel string) bool {
	switch channel {
	case oktaPushChannelEventHook:
		return cfg.EventHookEnabled &&
			(cfg.DiscoveryIngestMode == configstore.OktaDiscoveryIngestModeEventHook ||
				cfg.DiscoveryIngestMode == configstore.OktaDiscoveryIngestModeHybrid)
	case oktaPushChannelEventBridge:
		return cfg.EventBridgeEnabled &&
			(cfg.DiscoveryIngestMode == configstore.OktaDiscoveryIngestModeEventBridge ||
				cfg.DiscoveryIngestMode == configstore.OktaDiscoveryIngestModeHybrid)
	default:
		return false
	}
}

func oktaPushInboxParamsFromRawEvents(sourceName, channel, deliveryExternalID string, rawEvents []json.RawMessage) (gen.UpsertOktaPushInboxEventsBulkParams, []genericinbox.Delivery, int, error) {
	params := gen.UpsertOktaPushInboxEventsBulkParams{
		SourceName:          sourceName,
		Channel:             channel,
		DeliveryExternalIds: make([]string, 0, len(rawEvents)),
		EventExternalIds:    make([]string, 0, len(rawEvents)),
		EventTypes:          make([]string, 0, len(rawEvents)),
		EventIndexes:        make([]int32, 0, len(rawEvents)),
		PublishedAts:        make([]pgtype.Timestamptz, 0, len(rawEvents)),
		RawJsons:            make([][]byte, 0, len(rawEvents)),
	}
	genericDeliveries := make([]genericinbox.Delivery, 0, len(rawEvents))
	ignored := 0
	for idx, raw := range rawEvents {
		event, err := oktaconnector.MapSystemLogEventJSON(raw)
		if err != nil {
			return gen.UpsertOktaPushInboxEventsBulkParams{}, nil, 0, echo.NewHTTPError(http.StatusBadRequest, "invalid Okta System Log event")
		}
		if !oktaconnector.ShouldIngestPushEvent(event) {
			ignored++
			continue
		}
		params.DeliveryExternalIds = append(params.DeliveryExternalIds, strings.TrimSpace(deliveryExternalID))
		params.EventExternalIds = append(params.EventExternalIds, strings.TrimSpace(event.ID))
		params.EventTypes = append(params.EventTypes, strings.TrimSpace(event.EventType))
		params.EventIndexes = append(params.EventIndexes, int32(idx))
		params.PublishedAts = append(params.PublishedAts, timestamptzFromTime(event.Published))
		params.RawJsons = append(params.RawJsons, []byte(raw))
		genericDeliveries = append(genericDeliveries, genericinbox.Delivery{
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
	return params, genericDeliveries, ignored, nil
}

func timestamptzFromTime(t time.Time) pgtype.Timestamptz {
	if t.IsZero() {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: t.UTC(), Valid: true}
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

func requireOktaIngestJSON(c *echo.Context) error {
	contentType := strings.TrimSpace(c.Request().Header.Get(echo.HeaderContentType))
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil || !strings.EqualFold(mediaType, echo.MIMEApplicationJSON) {
		return echo.NewHTTPError(http.StatusUnsupportedMediaType, "Okta ingest requires application/json")
	}
	return nil
}
