package handlers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	connregistry "github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

// HandleConnectorHealth renders connector health under Settings.
func (h *Handlers) HandleConnectorHealth(c echo.Context) error {
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

	data, err := buildConnectorHealthViewData(h.Cfg, h.Q, ctx, states)
	if err != nil {
		return h.RenderError(c, err)
	}
	data.Layout = layout
	return h.RenderComponent(c, views.SettingsConnectorHealthPage(data))
}

type syncRollupKey struct {
	kind string
	name string
}

func buildConnectorHealthViewData(cfg config.Config, q *gen.Queries, ctx context.Context, states []connregistry.ConnectorState) (viewmodels.ConnectorHealthViewData, error) {
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

		syncable := !strings.EqualFold(kind, configstore.KindVault)
		syncKind := connectorSyncKind(kind)
		expectedInterval := expectedIntervalForSyncKind(cfg, syncKind)

		var rollup syncRunRollup
		if syncable && st.Configured && strings.TrimSpace(st.SourceName) != "" && syncKind != "" {
			rollup = rollupByKey[syncRollupKey{kind: syncKind, name: strings.TrimSpace(st.SourceName)}]
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
			StatusLabel:      res.statusLabel,
			StatusClass:      res.statusClass,
			LastSuccessLabel: res.lastSuccessLabel,
			LastRunLabel:     res.lastRunLabel,
			SuccessRate7d:    res.successRate7d,
			AvgDuration7d:    res.avgDuration7d,
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
