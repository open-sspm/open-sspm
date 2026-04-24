package handlers

import (
	"context"
	"math"
	"strings"

	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func (h *Handlers) buildIdentitiesOverviewMap(ctx context.Context, sourcePairs []viewmodels.ProgrammaticSourceOption, identityCount int64) (viewmodels.OverviewMapGraph, error) {
	graph := newOverviewMapGraph(identityCount)

	if len(sourcePairs) == 0 {
		return graph, nil
	}

	configuredKinds, configuredNames := identityConfiguredSourcePairs(sourcePairs)
	sourceRows, err := h.Q.ListDashboardSourceAccountSummaries(ctx, gen.ListDashboardSourceAccountSummariesParams{
		ConfiguredSourceKinds: configuredKinds,
		ConfiguredSourceNames: configuredNames,
	})
	if err != nil {
		return graph, err
	}

	bucketRows, err := h.Q.ListDashboardPrivilegedAccessBuckets(ctx, gen.ListDashboardPrivilegedAccessBucketsParams{
		ConfiguredSourceKinds: configuredKinds,
		ConfiguredSourceNames: configuredNames,
		BucketLimit:           2,
	})
	if err != nil {
		return graph, err
	}

	return assembleOverviewMapGraph(graph, sourceRows, bucketRows, identitiesSourceHref)
}

func (h *Handlers) buildIdentityShowOverviewMap(ctx context.Context, identityID int64) (viewmodels.OverviewMapGraph, error) {
	graph := newOverviewMapGraph(0)

	sourceRows, err := h.Q.ListIdentitySourceSummaries(ctx, identityID)
	if err != nil {
		return graph, err
	}

	if len(sourceRows) == 0 {
		return graph, nil
	}

	for _, row := range sourceRows {
		graph.AccountCount += row.AccountCount
	}

	totalSources := len(sourceRows)
	graph.Sources = make([]viewmodels.OverviewMapSourceNode, 0, totalSources)
	for idx, row := range sourceRows {
		x, y := overviewMapNodePosition(graph.CenterX, graph.CenterY, idx, totalSources)
		kind := strings.TrimSpace(row.SourceKind)
		sourceName := strings.TrimSpace(row.SourceName)

		graph.Sources = append(graph.Sources, viewmodels.OverviewMapSourceNode{
			Kind:                  kind,
			SourceName:            sourceName,
			Label:                 sourcePrimaryLabel(kind),
			Href:                  identitiesSourceHref(kind),
			X:                     x,
			Y:                     y,
			AccountCount:          row.AccountCount,
			ManagedAccountCount:   managedAccountCount(row.AccountCount, row.HasAuthoritative),
			UnmanagedAccountCount: unmanagedAccountCount(row.AccountCount, row.HasAuthoritative),
			CoveragePercent:       accountCoveragePercent(row.AccountCount, row.HasAuthoritative),
			Tone:                  dashboardGraphTone(kind),
		})
	}

	return graph, nil
}

func assembleOverviewMapGraph(graph viewmodels.OverviewMapGraph, sourceRows []gen.ListDashboardSourceAccountSummariesRow, bucketRows []gen.ListDashboardPrivilegedAccessBucketsRow, sourceHref func(string) string) (viewmodels.OverviewMapGraph, error) {
	bucketsBySource := make(map[string][]viewmodels.OverviewMapBucket)
	for _, row := range bucketRows {
		label := strings.TrimSpace(row.BucketLabel)
		if label == "" {
			continue
		}
		key := overviewMapSourceKey(row.SourceKind, row.SourceName)
		bucketsBySource[key] = append(bucketsBySource[key], viewmodels.OverviewMapBucket{
			Label:            label,
			Severity:         strings.TrimSpace(row.Severity),
			AffectedCount:    row.AffectedCount,
			EntitlementCount: row.EntitlementCount,
		})
	}

	totalSources := len(sourceRows)
	if totalSources == 0 {
		return graph, nil
	}

	graph.Sources = make([]viewmodels.OverviewMapSourceNode, 0, totalSources)
	for idx, row := range sourceRows {
		x, y := overviewMapNodePosition(graph.CenterX, graph.CenterY, idx, totalSources)
		kind := strings.TrimSpace(row.SourceKind)
		sourceName := strings.TrimSpace(row.SourceName)

		graph.AccountCount += row.AccountCount
		graph.Sources = append(graph.Sources, viewmodels.OverviewMapSourceNode{
			Kind:                  kind,
			SourceName:            sourceName,
			Label:                 sourcePrimaryLabel(kind),
			Href:                  sourceHref(kind),
			X:                     x,
			Y:                     y,
			IdentityCount:         row.IdentityCount,
			AccountCount:          row.AccountCount,
			ManagedAccountCount:   row.ManagedAccountCount,
			UnmanagedAccountCount: row.UnmanagedAccountCount,
			CoveragePercent:       overviewMapPercent(row.ManagedAccountCount, row.AccountCount),
			Tone:                  dashboardGraphTone(kind),
			Buckets:               bucketsBySource[overviewMapSourceKey(kind, sourceName)],
		})
	}

	return graph, nil
}

func newOverviewMapGraph(identityCount int64) viewmodels.OverviewMapGraph {
	return viewmodels.OverviewMapGraph{
		CenterX:       viewmodels.ClampOverviewMapPercent(50),
		CenterY:       viewmodels.ClampOverviewMapPercent(50),
		IdentityCount: identityCount,
	}
}

func overviewMapNodePosition(centerX, centerY, index, total int) (int, int) {
	const (
		radius   = 37.0
		minNodeX = 16
		maxNodeX = 84
		minNodeY = 22
		maxNodeY = 78
	)

	angle := -math.Pi / 2
	if total > 1 {
		angle += (2 * math.Pi * float64(index)) / float64(total)
	}
	x := clampInt(int(math.Round(float64(centerX)+radius*math.Cos(angle))), minNodeX, maxNodeX)
	y := clampInt(int(math.Round(float64(centerY)+radius*math.Sin(angle))), minNodeY, maxNodeY)
	return viewmodels.ClampOverviewMapPercent(x), viewmodels.ClampOverviewMapPercent(y)
}

func overviewMapSourceKey(sourceKind, sourceName string) string {
	return strings.ToLower(strings.TrimSpace(sourceKind)) + "\x00" + strings.ToLower(strings.TrimSpace(sourceName))
}

func overviewMapPercent(numerator, denominator int64) int {
	if denominator <= 0 || numerator <= 0 {
		return 0
	}
	percent := int((numerator * 100) / denominator)
	return viewmodels.ClampOverviewMapPercent(percent)
}

func identitiesSourceHref(kind string) string {
	kind = NormalizeConnectorKind(kind)
	return "/identities?source_kind=" + kind
}

func managedAccountCount(accountCount int64, hasAuthoritative bool) int64 {
	if hasAuthoritative {
		return accountCount
	}
	return 0
}

func unmanagedAccountCount(accountCount int64, hasAuthoritative bool) int64 {
	if hasAuthoritative {
		return 0
	}
	return accountCount
}

func accountCoveragePercent(accountCount int64, hasAuthoritative bool) int {
	if accountCount <= 0 {
		return 0
	}
	if hasAuthoritative {
		return 100
	}
	return 0
}
