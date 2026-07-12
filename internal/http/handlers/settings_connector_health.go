package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	connregistry "github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/events"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
	"github.com/open-sspm/open-sspm/internal/sync"
)

const (
	connectorHealthDetailsRunLimit   int32 = 5
	connectorHealthErrorPreviewRunes int   = 320
	connectorHealthErrorFullRunes    int   = 20000
)

// HandleConnectorHealth renders connector health under Settings.
func (h *Handlers) HandleConnectorHealth(c *echo.Context) error {
	addVary(c, "HX-Request", "HX-Target")

	data, err := h.buildConnectorHealthViewDataForRequest(c)
	if err != nil {
		return h.RenderError(c, err)
	}
	if isHX(c) && !isHXBoosted(c) && isHXTarget(c, "connector-health-panel") {
		return h.RenderComponent(c, views.SettingsConnectorHealthPanel(data))
	}
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
	setResponseToast(c, toast)
	redirectURL := "/settings/connector-health"
	if isHX(c) {
		addHXTrigger(c, events.ConnectorHealthChanged, map[string]string{"status": normalizeToastCategory(toast.Category)})
		data, err := h.buildConnectorHealthViewDataForRequest(c)
		if err != nil {
			return h.RenderError(c, err)
		}
		return h.RenderComponent(c, views.SettingsConnectorHealthPanel(data))
	}
	return c.Redirect(http.StatusSeeOther, redirectURL)
}

func (h *Handlers) buildConnectorHealthViewDataForRequest(c *echo.Context) (viewmodels.ConnectorHealthViewData, error) {
	ctx := c.Request().Context()
	layout, _, err := h.LayoutData(ctx, c, "Connector Health")
	if err != nil {
		return viewmodels.ConnectorHealthViewData{}, err
	}

	var states []connregistry.ConnectorState
	if h.Registry != nil {
		states, err = h.Registry.LoadStates(ctx, h.Q)
		if err != nil {
			return viewmodels.ConnectorHealthViewData{}, err
		}
	}

	data, err := buildConnectorHealthViewData(h.Cfg, h.Q, ctx, states, h.Syncer != nil)
	if err != nil {
		return viewmodels.ConnectorHealthViewData{}, err
	}
	data.Layout = layout
	return data, nil
}

// HandleConnectorHealthErrorDetails renders the latest non-success sync runs for a connector source.
func (h *Handlers) HandleConnectorHealthErrorDetails(c *echo.Context) error {
	if c.Request().Method != http.MethodGet {
		return c.NoContent(http.StatusMethodNotAllowed)
	}
	addVary(c, "HX-Request", "HX-Target")

	sourceKinds := normalizeConnectorHealthSourceKinds(c.QueryParams()["source_kind"])
	runModes := normalizeConnectorHealthRunModes(c.QueryParams()["run_mode"])
	sourceName := strings.TrimSpace(c.QueryParam("source_name"))
	connectorName := strings.TrimSpace(c.QueryParam("connector_name"))

	if len(sourceKinds) == 0 || sourceName == "" {
		return c.String(http.StatusBadRequest, "source_kind, run_mode, and source_name are required")
	}
	if len(runModes) != len(sourceKinds) {
		return c.String(http.StatusBadRequest, "source_kind and run_mode counts must match")
	}
	if h.Q == nil {
		return c.String(http.StatusServiceUnavailable, "connector health unavailable")
	}
	if connectorName == "" {
		connectorName = sourceKinds[0]
	}

	type connectorHealthErrorRun struct {
		sourceKind string
		runMode    string
		row        gen.ListRecentNonSuccessSyncRunsBySourceRow
	}

	allRows := make([]connectorHealthErrorRun, 0, len(sourceKinds)*int(connectorHealthDetailsRunLimit))
	for idx, sourceKind := range sourceKinds {
		runMode := runModes[idx]
		rows, err := h.Q.ListRecentNonSuccessSyncRunsBySource(c.Request().Context(), gen.ListRecentNonSuccessSyncRunsBySourceParams{
			SourceKind: sourceKind,
			SourceName: sourceName,
			RunMode:    runMode,
			Limit:      connectorHealthDetailsRunLimit,
		})
		if err != nil {
			return h.RenderError(c, err)
		}
		for _, row := range rows {
			allRows = append(allRows, connectorHealthErrorRun{
				sourceKind: sourceKind,
				runMode:    runMode,
				row:        row,
			})
		}
	}
	sort.Slice(allRows, func(i, j int) bool {
		return allRows[i].row.FinishedAt.Time.After(allRows[j].row.FinishedAt.Time)
	})
	if len(allRows) > int(connectorHealthDetailsRunLimit) {
		allRows = allRows[:connectorHealthDetailsRunLimit]
	}

	now := time.Now()
	viewRows := make([]viewmodels.ConnectorHealthErrorDetailsRow, 0, len(allRows))
	for idx, run := range allRows {
		row := run.row
		message := strings.TrimSpace(row.Message)
		preview, full, previewTruncated, fullTruncated := sizeConnectorHealthErrorMessage(message)

		runKey := fmt.Sprintf("connector-health-run-%s-%d-%d", sanitizeDialogIDPart(run.sourceKind), row.ID, idx)
		viewRows = append(viewRows, viewmodels.ConnectorHealthErrorDetailsRow{
			RowID:             runKey,
			RunID:             row.ID,
			LaneLabel:         connectorHealthLaneLabel(run.runMode),
			StatusLabel:       connectorHealthRunStatusLabel(row.Status),
			StatusClass:       connectorHealthRunStatusClass(row.Status),
			FinishedAt:        relativeWithTitleDisplay(now, row.FinishedAt, "", ""),
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
		DialogID:      connectorHealthErrorDialogID(strings.Join(sourceKinds, "-"), sourceName),
		ConnectorName: connectorName,
		SourceKind:    strings.Join(sourceKinds, ", "),
		SourceName:    sourceName,
		Rows:          viewRows,
		HasRows:       len(viewRows) > 0,
	}

	return h.RenderComponent(c, views.ConnectorHealthErrorDetailsDialog(data))
}

type syncRollupKey struct {
	kind string
	name string
	mode string
}

type connectorHealthLane struct {
	label            string
	syncKind         string
	runMode          connregistry.RunMode
	expectedInterval time.Duration
}

type connectorHealthLaneResult struct {
	lane   connectorHealthLane
	result connectorHealthResult
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

	requested := connectorHealthRequestedRollupKeys(cfg, states)

	rollupByKey := make(map[syncRollupKey]syncRunRollup, len(requested))
	if len(requested) > 0 {
		sourceKinds := make([]string, 0, len(requested))
		sourceNames := make([]string, 0, len(requested))
		runModes := make([]string, 0, len(requested))
		for _, key := range requested {
			sourceKinds = append(sourceKinds, key.kind)
			sourceNames = append(sourceNames, key.name)
			runModes = append(runModes, key.mode)
		}

		rows, err := q.GetSyncRunRollupsForSources(ctx, gen.GetSyncRunRollupsForSourcesParams{
			SourceKinds: sourceKinds,
			SourceNames: sourceNames,
			RunModes:    runModes,
		})
		if err != nil {
			return viewmodels.ConnectorHealthViewData{}, err
		}
		for _, row := range rows {
			key := syncRollupKey{kind: row.SourceKind, name: row.SourceName, mode: row.RunMode}
			rollupByKey[key] = syncRunRollupFromRow(row)
		}
	}

	items := make([]viewmodels.ConnectorHealthItem, 0, len(states))
	var (
		enabledTotal        int
		healthyCount        int
		degradedCount       int
		stuckCount          int
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

		syncable := IsKnownConnectorKind(kind)
		var (
			res            connectorHealthResult
			canViewDetails bool
			detailsURL     string
			laneResults    []connectorHealthLaneResult
		)
		if syncable && st.Configured && sourceName != "" {
			lanes := connectorHealthLanes(cfg, st)
			laneResults = make([]connectorHealthLaneResult, 0, len(lanes))
			sourceKinds := make([]string, 0, len(lanes))
			runModes := make([]connregistry.RunMode, 0, len(lanes))
			for _, lane := range lanes {
				rollup := rollupByKey[syncRollupKey{kind: lane.syncKind, name: sourceName, mode: string(lane.runMode)}]
				laneResults = append(laneResults, connectorHealthLaneResult{
					lane: lane,
					result: connectorHealth(connectorHealthInput{
						syncable:         syncable,
						configured:       st.Configured,
						enabled:          st.Enabled,
						expectedInterval: lane.expectedInterval,
						now:              now,
						rollup:           rollup,
					}),
				})
				sourceKinds = append(sourceKinds, lane.syncKind)
				runModes = append(runModes, lane.runMode)
			}
			if len(laneResults) > 0 {
				res = combineConnectorHealthLaneResults(laneResults)
				canViewDetails = true
				detailsURL = connectorHealthErrorDetailsURL(sourceKinds, runModes, sourceName, displayName)
			}
		}
		if !canViewDetails {
			res = connectorHealth(connectorHealthInput{
				syncable:         syncable,
				configured:       st.Configured,
				enabled:          st.Enabled,
				expectedInterval: expectedIntervalForSyncKind(cfg, connectorSyncKind(kind)),
				now:              now,
			})
		}

		item := viewmodels.ConnectorHealthItem{
			Kind:             kind,
			Name:             displayName,
			SourceKind:       connectorSyncKind(kind),
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
			Lanes:            buildConnectorHealthItemLanes(laneResults),
		}
		items = append(items, item)

		if res.countsAsEnabled {
			enabledTotal++
			switch res.status {
			case connectorHealthHealthy:
				healthyCount++
			case connectorHealthDegraded:
				degradedCount++
			case connectorHealthStuck:
				stuckCount++
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
	data.NeedsAttentionCount = needsAttentionCount

	if enabledTotal == 0 {
		data.SummaryLabel = "0 enabled"
		return data, nil
	}

	data.SummaryLabel = fmt.Sprintf("%d enabled · %d healthy · %d need attention", enabledTotal, healthyCount, needsAttentionCount)

	if stuckCount > 0 {
		data.ShowWarning = true
		data.WarningDestructive = true
		data.WarningMessage = "Some enabled connectors have syncs stuck in a running state. Health and freshness indicators may be misleading until those runs are resolved."
	} else if staleCount+neverSyncedCount > 0 {
		data.ShowWarning = true
		data.WarningDestructive = true
		data.WarningMessage = "Some enabled connectors have not successfully synced within the expected window. Data from those connectors may be stale."
	} else if degradedCount > 0 {
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
	if row.OldestRunningStartedAt.Valid {
		t := row.OldestRunningStartedAt.Time
		rollup.oldestRunningStartedAt = &t
	}

	rollup.runningCount = row.RunningCount
	rollup.finishedCount7d = row.FinishedCount7d
	rollup.successCount7d = row.SuccessCount7d
	if row.AvgSuccessDurationMs7d.Valid {
		d := time.Duration(row.AvgSuccessDurationMs7d.Float64 * float64(time.Millisecond))
		rollup.avgSuccessDuration7d = &d
	}

	return rollup
}

func connectorHealthLanes(cfg config.Config, st connregistry.ConnectorState) []connectorHealthLane {
	kind := strings.ToLower(strings.TrimSpace(st.Definition.Kind()))
	fullSyncKind := connectorSyncKind(kind)
	if fullSyncKind == "" {
		return nil
	}

	lanes := []connectorHealthLane{{
		label:            "Full",
		syncKind:         fullSyncKind,
		runMode:          connregistry.RunModeFull,
		expectedInterval: expectedIntervalForSyncKind(cfg, fullSyncKind),
	}}
	if discoverySyncKind := connectorDiscoverySyncKind(cfg, st); discoverySyncKind != "" {
		lanes = append(lanes, connectorHealthLane{
			label:            "Discovery",
			syncKind:         discoverySyncKind,
			runMode:          connregistry.RunModeDiscovery,
			expectedInterval: expectedIntervalForDiscoverySync(cfg),
		})
	}
	return lanes
}

func connectorHealthRequestedRollupKeys(cfg config.Config, states []connregistry.ConnectorState) []syncRollupKey {
	requested := make([]syncRollupKey, 0, len(states))
	requestedSet := make(map[syncRollupKey]struct{}, len(states)*2)
	for _, st := range states {
		if !st.Configured {
			continue
		}

		sourceName := strings.TrimSpace(st.SourceName)
		if sourceName == "" {
			continue
		}

		for _, lane := range connectorHealthLanes(cfg, st) {
			key := syncRollupKey{kind: lane.syncKind, name: sourceName}
			key.mode = string(lane.runMode)
			if _, exists := requestedSet[key]; exists {
				continue
			}
			requestedSet[key] = struct{}{}
			requested = append(requested, key)
		}
	}
	return requested
}

func connectorDiscoverySyncKind(cfg config.Config, st connregistry.ConnectorState) string {
	if !cfg.SyncDiscoveryEnabled {
		return ""
	}

	kind := strings.ToLower(strings.TrimSpace(st.Definition.Kind()))
	switch cfg := st.Config.(type) {
	case configstore.OktaConfig:
		if cfg.DiscoveryEnabled {
			return kind
		}
	case configstore.EntraConfig:
		if cfg.DiscoveryEnabled {
			return kind
		}
	case configstore.GoogleWorkspaceConfig:
		if cfg.DiscoveryEnabled {
			return kind
		}
	}
	return ""
}

func expectedIntervalForDiscoverySync(cfg config.Config) time.Duration {
	if cfg.SyncDiscoveryInterval > 0 {
		return cfg.SyncDiscoveryInterval
	}
	return 15 * time.Minute
}

func combineConnectorHealthLaneResults(laneResults []connectorHealthLaneResult) connectorHealthResult {
	if len(laneResults) == 0 {
		return connectorHealthResult{}
	}
	if len(laneResults) == 1 {
		return laneResults[0].result
	}

	combined := laneResults[0].result
	for _, laneResult := range laneResults[1:] {
		if connectorHealthSeverity(laneResult.result.status) > connectorHealthSeverity(combined.status) {
			combined = laneResult.result
		}
	}
	combined.lastSuccessLabel = joinConnectorHealthLaneLabels(laneResults, func(laneResult connectorHealthLaneResult) string {
		return laneResult.result.lastSuccessLabel
	})
	combined.lastRunLabel = joinConnectorHealthLaneLabels(laneResults, func(laneResult connectorHealthLaneResult) string {
		return laneResult.result.lastRunLabel
	})
	combined.successRate7d = joinConnectorHealthLaneLabels(laneResults, func(laneResult connectorHealthLaneResult) string {
		return laneResult.result.successRate7d
	})
	combined.avgDuration7d = joinConnectorHealthLaneLabels(laneResults, func(laneResult connectorHealthLaneResult) string {
		return laneResult.result.avgDuration7d
	})
	combined.needsAttention = false
	combined.countsAsEnabled = false
	for _, laneResult := range laneResults {
		if laneResult.result.needsAttention {
			combined.needsAttention = true
		}
		if laneResult.result.countsAsEnabled {
			combined.countsAsEnabled = true
		}
	}
	return combined
}

func joinConnectorHealthLaneLabels(laneResults []connectorHealthLaneResult, value func(connectorHealthLaneResult) string) string {
	parts := make([]string, 0, len(laneResults))
	for _, laneResult := range laneResults {
		label := strings.TrimSpace(value(laneResult))
		if label == "" {
			label = "—"
		}
		parts = append(parts, laneResult.lane.label+" "+label)
	}
	if len(parts) == 0 {
		return "—"
	}
	return strings.Join(parts, " · ")
}

func buildConnectorHealthItemLanes(laneResults []connectorHealthLaneResult) []viewmodels.ConnectorHealthItemLane {
	if len(laneResults) <= 1 {
		return nil
	}
	lanes := make([]viewmodels.ConnectorHealthItemLane, 0, len(laneResults))
	for _, lr := range laneResults {
		lanes = append(lanes, viewmodels.ConnectorHealthItemLane{
			Label:       lr.lane.label,
			LastSuccess: valueOrDash(lr.result.lastSuccessLabel),
			LastRun:     valueOrDash(lr.result.lastRunLabel),
			SuccessRate: valueOrDash(lr.result.successRate7d),
			AvgDuration: valueOrDash(lr.result.avgDuration7d),
		})
	}
	return lanes
}

func valueOrDash(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "—"
	}
	return s
}

func connectorHealthSeverity(status connectorHealthStatus) int {
	switch status {
	case connectorHealthStuck:
		return 5
	case connectorHealthStale:
		return 4
	case connectorHealthNeverSynced:
		return 3
	case connectorHealthDegraded:
		return 2
	case connectorHealthHealthy:
		return 1
	default:
		return 0
	}
}

func connectorHealthErrorDetailsURL(sourceKinds []string, runModes []connregistry.RunMode, sourceName, connectorName string) string {
	values := url.Values{}
	normalizedKinds := normalizeConnectorHealthSourceKinds(sourceKinds)
	for idx, sourceKind := range normalizedKinds {
		values.Add("source_kind", sourceKind)
		mode := connregistry.RunModeFull
		if idx < len(runModes) {
			mode = runModes[idx].Normalize()
		}
		values.Add("run_mode", string(mode))
	}
	values.Set("source_name", strings.TrimSpace(sourceName))
	if connectorName = strings.TrimSpace(connectorName); connectorName != "" {
		values.Set("connector_name", connectorName)
	}
	return "/settings/connector-health/errors?" + values.Encode()
}

func normalizeConnectorHealthSourceKinds(sourceKinds []string) []string {
	normalized := make([]string, 0, len(sourceKinds))
	for _, sourceKind := range sourceKinds {
		sourceKind = strings.ToLower(strings.TrimSpace(sourceKind))
		if sourceKind == "" {
			continue
		}
		normalized = append(normalized, sourceKind)
	}
	return normalized
}

func normalizeConnectorHealthRunModes(runModes []string) []string {
	normalized := make([]string, 0, len(runModes))
	for _, runMode := range runModes {
		mode := connregistry.RunMode(strings.ToLower(strings.TrimSpace(runMode))).Normalize()
		normalized = append(normalized, string(mode))
	}
	return normalized
}

func connectorHealthLaneLabel(runMode string) string {
	switch connregistry.RunMode(strings.ToLower(strings.TrimSpace(runMode))).Normalize() {
	case connregistry.RunModeDiscovery:
		return "Discovery"
	case connregistry.RunModeTail:
		return "Tail"
	case connregistry.RunModeEventInbox:
		return "Event inbox"
	default:
		return "Full"
	}
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
