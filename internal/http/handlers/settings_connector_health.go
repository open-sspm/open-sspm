package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	connregistry "github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
	"github.com/open-sspm/open-sspm/internal/sync"
)

const (
	connectorHealthDetailsRunLimit   int32 = 5
	connectorHealthErrorPreviewRunes       = 320
	connectorHealthErrorFullRunes          = 20000
)

// HandleConnectorHealth renders connector health under Settings.
func (h *Handlers) HandleConnectorHealth(c *echo.Context) error {
	ctx := c.Request().Context()
	layout, _, err := h.LayoutData(ctx, c, "Connector Health")
	if err != nil {
		return h.RenderError(c, err)
	}

	var states []connregistry.ConnectorState
	if h.Registry != nil {
		states, err = h.Registry.LoadStates(ctx, h.Q)
		if err != nil {
			return h.RenderError(c, err)
		}
	}

	data, err := buildConnectorHealthViewData(h.Cfg, h.Q, ctx, states, h.Syncer != nil)
	if err != nil {
		return h.RenderError(c, err)
	}
	data.Layout = layout
	return h.RenderComponent(c, views.SettingsConnectorHealthPage(data))
}

// HandleConnectorHealthSync queues a manual sync for a single connector.
func (h *Handlers) HandleConnectorHealthSync(c *echo.Context) error {
	if c.Request().Method != http.MethodPost {
		return c.NoContent(http.StatusMethodNotAllowed)
	}
	addVary(c, "HX-Request")

	if h.Syncer == nil {
		return h.redirectConnectorHealthWithToast(c, viewmodels.ToastViewData{
			Category:    "warning",
			Title:       "Sync unavailable",
			Description: "Manual sync is not configured on this server.",
		})
	}

	connectorKind := NormalizeConnectorKind(c.FormValue("connector_kind"))
	sourceName := strings.TrimSpace(c.FormValue("source_name"))
	if !IsKnownConnectorKind(connectorKind) || sourceName == "" {
		return h.redirectConnectorHealthWithToast(c, viewmodels.ToastViewData{
			Category:    "error",
			Title:       "Invalid connector",
			Description: "Connector kind and source name are required.",
		})
	}

	if strings.EqualFold(connectorKind, configstore.KindVault) {
		return h.redirectConnectorHealthWithToast(c, viewmodels.ToastViewData{
			Category:    "warning",
			Title:       "Sync unavailable",
			Description: "The selected connector does not support sync.",
		})
	}

	if h.Registry == nil || h.Q == nil {
		return h.redirectConnectorHealthWithToast(c, viewmodels.ToastViewData{
			Category:    "error",
			Title:       "Connector health unavailable",
			Description: "Connector state could not be loaded.",
		})
	}

	states, err := h.Registry.LoadStates(c.Request().Context(), h.Q)
	if err != nil {
		return h.RenderError(c, err)
	}

	var selected *connregistry.ConnectorState
	for idx := range states {
		state := &states[idx]
		kind := NormalizeConnectorKind(state.Definition.Kind())
		name := strings.TrimSpace(state.SourceName)
		if kind == connectorKind && strings.EqualFold(name, sourceName) {
			selected = state
			break
		}
	}
	if selected == nil {
		return h.redirectConnectorHealthWithToast(c, viewmodels.ToastViewData{
			Category:    "error",
			Title:       "Connector not found",
			Description: "The selected connector source could not be resolved.",
		})
	}
	if !selected.Configured || !selected.Enabled || strings.TrimSpace(selected.SourceName) == "" {
		return h.redirectConnectorHealthWithToast(c, viewmodels.ToastViewData{
			Category:    "warning",
			Title:       "Sync unavailable",
			Description: "Enable and configure the connector before triggering sync.",
		})
	}

	triggerCtx := sync.WithConnectorScope(sync.WithForcedSync(c.Request().Context()), connectorKind, sourceName)
	switch err := h.Syncer.RunOnce(triggerCtx); {
	case err == nil, errors.Is(err, sync.ErrSyncQueued):
		return h.redirectConnectorHealthWithToast(c, viewmodels.ToastViewData{
			Category:    "success",
			Title:       "Sync queued",
			Description: sourceDiagnosticLabel(connectorKind, sourceName),
		})
	case errors.Is(err, sync.ErrSyncAlreadyRunning):
		return h.redirectConnectorHealthWithToast(c, viewmodels.ToastViewData{
			Category:    "warning",
			Title:       "Sync already running",
			Description: "A sync is already in progress. Try again shortly.",
		})
	case errors.Is(err, sync.ErrNoEnabledConnectors), errors.Is(err, sync.ErrNoConnectorsDue):
		return h.redirectConnectorHealthWithToast(c, viewmodels.ToastViewData{
			Category:    "warning",
			Title:       "Sync unavailable",
			Description: "No eligible connector work was found for this request.",
		})
	default:
		return h.redirectConnectorHealthWithToast(c, viewmodels.ToastViewData{
			Category:    "error",
			Title:       "Sync failed to queue",
			Description: "Check server logs for details.",
		})
	}
}

func (h *Handlers) redirectConnectorHealthWithToast(c *echo.Context, toast viewmodels.ToastViewData) error {
	setFlashToast(c, toast)
	redirectURL := "/settings/connector-health"
	if isHX(c) {
		setHXRedirect(c, redirectURL)
		return c.NoContent(http.StatusOK)
	}
	return c.Redirect(http.StatusSeeOther, redirectURL)
}

// HandleConnectorHealthErrorDetails renders the latest non-success sync runs for a connector source.
func (h *Handlers) HandleConnectorHealthErrorDetails(c *echo.Context) error {
	if c.Request().Method != http.MethodGet {
		return c.NoContent(http.StatusMethodNotAllowed)
	}
	addVary(c, "HX-Request", "HX-Target")

	sourceKind := strings.ToLower(strings.TrimSpace(c.QueryParam("source_kind")))
	sourceName := strings.TrimSpace(c.QueryParam("source_name"))
	connectorName := strings.TrimSpace(c.QueryParam("connector_name"))

	if sourceKind == "" || sourceName == "" {
		return c.String(http.StatusBadRequest, "source_kind and source_name are required")
	}
	if h.Q == nil {
		return c.String(http.StatusServiceUnavailable, "connector health unavailable")
	}
	if connectorName == "" {
		connectorName = sourceKind
	}

	rows, err := h.Q.ListRecentNonSuccessSyncRunsBySource(c.Request().Context(), gen.ListRecentNonSuccessSyncRunsBySourceParams{
		SourceKind: sourceKind,
		SourceName: sourceName,
		Limit:      connectorHealthDetailsRunLimit,
	})
	if err != nil {
		return h.RenderError(c, err)
	}

	now := time.Now()
	viewRows := make([]viewmodels.ConnectorHealthErrorDetailsRow, 0, len(rows))
	for idx, row := range rows {
		message := strings.TrimSpace(row.Message)
		preview, full, previewTruncated, fullTruncated := sizeConnectorHealthErrorMessage(message)

		runKey := fmt.Sprintf("connector-health-run-%d-%d", row.ID, idx)
		viewRows = append(viewRows, viewmodels.ConnectorHealthErrorDetailsRow{
			RowID:             runKey,
			RunID:             row.ID,
			StatusLabel:       connectorHealthRunStatusLabel(row.Status),
			StatusClass:       connectorHealthRunStatusClass(row.Status),
			FinishedAtLabel:   formatAge(now, row.FinishedAt.Time),
			FinishedAtTitle:   row.FinishedAt.Time.UTC().Format("Jan 2, 2006 3:04 PM UTC"),
			ErrorKind:         fallbackDash(strings.TrimSpace(row.ErrorKind)),
			MessagePreview:    preview,
			MessageFull:       full,
			PreviewTruncated:  previewTruncated,
			FullTextTruncated: fullTruncated,
			HasMessage:        full != "",
			ExpandControlID:   runKey + "-expand",
			ExpandContentID:   runKey + "-details",
		})
	}

	data := viewmodels.ConnectorHealthErrorDetailsDialogViewData{
		DialogID:      connectorHealthErrorDialogID(sourceKind, sourceName),
		ConnectorName: connectorName,
		SourceKind:    sourceKind,
		SourceName:    sourceName,
		Rows:          viewRows,
		HasRows:       len(viewRows) > 0,
	}

	return h.RenderComponent(c, views.ConnectorHealthErrorDetailsDialog(data))
}

type syncRollupKey struct {
	kind string
	name string
}

func buildConnectorHealthViewData(cfg config.Config, q *gen.Queries, ctx context.Context, states []connregistry.ConnectorState, canTriggerSync bool) (viewmodels.ConnectorHealthViewData, error) {
	now := time.Now()
	data := viewmodels.ConnectorHealthViewData{
		LookbackLabel: "7d",
	}
	if q == nil || len(states) == 0 {
		data.SummaryLabel = "Connector health unavailable"
		return data, nil
	}

	requested := make([]syncRollupKey, 0, len(states))
	for _, st := range states {
		kind := strings.ToLower(strings.TrimSpace(st.Definition.Kind()))
		if !st.Configured {
			continue
		}
		if strings.EqualFold(kind, configstore.KindVault) {
			continue
		}
		sourceName := strings.TrimSpace(st.SourceName)
		if sourceName == "" {
			continue
		}
		syncKind := connectorSyncKind(kind)
		if syncKind == "" {
			continue
		}
		requested = append(requested, syncRollupKey{kind: syncKind, name: sourceName})
	}

	rollupByKey := make(map[syncRollupKey]syncRunRollup, len(requested))
	if len(requested) > 0 {
		sourceKinds := make([]string, 0, len(requested))
		sourceNames := make([]string, 0, len(requested))
		for _, key := range requested {
			sourceKinds = append(sourceKinds, key.kind)
			sourceNames = append(sourceNames, key.name)
		}

		rows, err := q.GetSyncRunRollupsForSources(ctx, gen.GetSyncRunRollupsForSourcesParams{
			SourceKinds: sourceKinds,
			SourceNames: sourceNames,
		})
		if err != nil {
			return viewmodels.ConnectorHealthViewData{}, err
		}
		for _, row := range rows {
			key := syncRollupKey{kind: row.SourceKind, name: row.SourceName}
			rollupByKey[key] = syncRunRollupFromRow(row)
		}
	}

	items := make([]viewmodels.ConnectorHealthItem, 0, len(states))
	var (
		enabledTotal        int
		healthyCount        int
		degradedCount       int
		staleCount          int
		neverSyncedCount    int
		needsAttentionCount int
	)

	for _, st := range states {
		kind := strings.TrimSpace(st.Definition.Kind())
		displayName := strings.TrimSpace(st.Definition.DisplayName())
		if displayName == "" {
			displayName = kind
		}
		sourceName := strings.TrimSpace(st.SourceName)

		syncable := !strings.EqualFold(kind, configstore.KindVault)
		syncKind := connectorSyncKind(kind)
		expectedInterval := expectedIntervalForSyncKind(cfg, syncKind)

		var rollup syncRunRollup
		canViewDetails := syncable && st.Configured && sourceName != "" && syncKind != ""
		detailsURL := ""
		if canViewDetails {
			rollup = rollupByKey[syncRollupKey{kind: syncKind, name: sourceName}]
			detailsURL = connectorHealthErrorDetailsURL(syncKind, sourceName, displayName)
		}

		res := connectorHealth(connectorHealthInput{
			syncable:         syncable,
			configured:       st.Configured,
			enabled:          st.Enabled,
			expectedInterval: expectedInterval,
			now:              now,
			rollup:           rollup,
		})

		items = append(items, viewmodels.ConnectorHealthItem{
			Kind:             kind,
			Name:             displayName,
			SourceKind:       syncKind,
			SourceName:       sourceName,
			StatusLabel:      res.statusLabel,
			StatusClass:      res.statusClass,
			LastSuccessLabel: res.lastSuccessLabel,
			LastRunLabel:     res.lastRunLabel,
			SuccessRate7d:    res.successRate7d,
			AvgDuration7d:    res.avgDuration7d,
			DetailsURL:       detailsURL,
			CanViewDetails:   canViewDetails,
			CanTriggerSync:   canTriggerSync && syncable && st.Configured && st.Enabled && sourceName != "" && IsKnownConnectorKind(kind),
		})

		if res.countsAsEnabled {
			enabledTotal++
			switch res.status {
			case connectorHealthHealthy:
				healthyCount++
			case connectorHealthDegraded:
				degradedCount++
			case connectorHealthStale:
				staleCount++
			case connectorHealthNeverSynced:
				neverSyncedCount++
			}
			if res.needsAttention {
				needsAttentionCount++
			}
		}
	}

	data.Items = items

	if enabledTotal == 0 {
		data.SummaryLabel = "0 enabled"
		return data, nil
	}

	data.SummaryLabel = fmt.Sprintf("%d enabled · %d healthy · %d need attention", enabledTotal, healthyCount, needsAttentionCount)

	if staleCount+neverSyncedCount > 0 {
		data.ShowWarning = true
		data.WarningDestructive = true
		data.WarningMessage = "Some enabled connectors have not successfully synced within the expected window. Data from those connectors may be stale."
	}
	if degradedCount > 0 && !data.ShowWarning {
		data.ShowWarning = true
		data.WarningDestructive = false
		data.WarningMessage = "Some enabled connectors are failing. Data may be incomplete until the next successful sync."
	}

	return data, nil
}

func connectorSyncKind(connectorKind string) string {
	connectorKind = strings.ToLower(strings.TrimSpace(connectorKind))
	switch connectorKind {
	case configstore.KindAWSIdentityCenter:
		return "aws"
	default:
		return connectorKind
	}
}

func expectedIntervalForSyncKind(cfg config.Config, syncKind string) time.Duration {
	syncKind = strings.ToLower(strings.TrimSpace(syncKind))

	if cfg.SyncInterval <= 0 {
		cfg.SyncInterval = 15 * time.Minute
	}

	switch syncKind {
	case "okta":
		if cfg.SyncOktaInterval > 0 {
			return cfg.SyncOktaInterval
		}
	case "entra":
		if cfg.SyncEntraInterval > 0 {
			return cfg.SyncEntraInterval
		}
	case "github":
		if cfg.SyncGitHubInterval > 0 {
			return cfg.SyncGitHubInterval
		}
	case "datadog":
		if cfg.SyncDatadogInterval > 0 {
			return cfg.SyncDatadogInterval
		}
	case "aws":
		if cfg.SyncAWSInterval > 0 {
			return cfg.SyncAWSInterval
		}
	}

	return cfg.SyncInterval
}

func syncRunRollupFromRow(row gen.GetSyncRunRollupsForSourcesRow) syncRunRollup {
	var rollup syncRunRollup

	if row.LastRunStatus.Valid {
		rollup.lastRunStatus = strings.TrimSpace(row.LastRunStatus.String)
	}
	if row.LastRunErrorKind.Valid {
		rollup.lastRunErrorKind = strings.TrimSpace(row.LastRunErrorKind.String)
	}
	if row.LastRunFinishedAt.Valid {
		t := row.LastRunFinishedAt.Time
		rollup.lastRunFinishedAt = &t
	}
	if row.LastSuccessAt.Valid {
		t := row.LastSuccessAt.Time
		rollup.lastSuccessAt = &t
	}

	rollup.finishedCount7d = row.FinishedCount7d
	rollup.successCount7d = row.SuccessCount7d
	if row.AvgSuccessDurationMs7d.Valid {
		d := time.Duration(row.AvgSuccessDurationMs7d.Float64 * float64(time.Millisecond))
		rollup.avgSuccessDuration7d = &d
	}

	return rollup
}

func connectorHealthErrorDetailsURL(sourceKind, sourceName, connectorName string) string {
	values := url.Values{}
	values.Set("source_kind", strings.TrimSpace(sourceKind))
	values.Set("source_name", strings.TrimSpace(sourceName))
	if connectorName = strings.TrimSpace(connectorName); connectorName != "" {
		values.Set("connector_name", connectorName)
	}
	return "/settings/connector-health/errors?" + values.Encode()
}

func connectorHealthRunStatusLabel(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "success":
		return "Success"
	case "error":
		return "Error"
	case "canceled":
		return "Canceled"
	case "running":
		return "Running"
	default:
		status = strings.TrimSpace(status)
		if status == "" {
			return "Unknown"
		}
		return status
	}
}

func connectorHealthRunStatusClass(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "success":
		return badgeClassSuccess()
	case "error":
		return badgeClassDanger()
	case "canceled":
		return badgeClassWarning()
	default:
		return badgeClassNeutral()
	}
}

func sizeConnectorHealthErrorMessage(message string) (preview string, full string, previewTruncated bool, fullTruncated bool) {
	message = strings.TrimSpace(message)
	if message == "" {
		return "", "", false, false
	}

	full, fullTruncated = truncateRunes(message, connectorHealthErrorFullRunes)
	preview, previewTruncated = truncateRunes(full, connectorHealthErrorPreviewRunes)
	return preview, full, previewTruncated, fullTruncated
}

func truncateRunes(value string, max int) (string, bool) {
	if max <= 0 {
		return "", value != ""
	}
	runes := []rune(value)
	if len(runes) <= max {
		return value, false
	}
	return string(runes[:max]), true
}

func connectorHealthErrorDialogID(sourceKind, sourceName string) string {
	return "connector-health-errors-" + sanitizeDialogIDPart(sourceKind) + "-" + sanitizeDialogIDPart(sourceName)
}

func sanitizeDialogIDPart(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return "na"
	}

	var b strings.Builder
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	s := strings.Trim(b.String(), "-")
	if s == "" {
		return "na"
	}
	return s
}
