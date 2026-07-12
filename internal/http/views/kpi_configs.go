package views

import (
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

// DashboardAttentionCards turns the dashboard's cross-product action queues
// into ordinary deep links. The destination pages own filtering and remain
// usable without HTMX.
func DashboardAttentionCards(data viewmodels.DashboardViewData) []KPICard {
	credentials := data.CredentialsAttention
	connectors := data.ConnectorsNeedingAttention

	connectorCaption := "all enabled connectors healthy"
	if connectors > 0 {
		connectorCaption = "enabled connectors need attention"
	}

	return []KPICard{
		{
			Label:      "Credentials to rotate",
			Metric:     FormatInt64(credentials.Total),
			MetricTone: kpiToneFromCount(credentials.Total, "danger"),
			Caption:    "need rotation or approval",
			Breakdown: []KPIBreakdownSegment{
				{Label: "critical", Count: credentials.Critical, Tone: "critical"},
				{Label: "high", Count: credentials.High, Tone: "warn"},
			},
			Href: "/credentials?risk_level=critical%2Chigh",
		},
		{
			Label:      "Apps to review",
			Metric:     FormatInt64(data.UnreviewedDiscoveryApps),
			MetricTone: kpiToneFromCount(data.UnreviewedDiscoveryApps, "warn"),
			Caption:    "discovered apps are unreviewed",
			Trend:      "Open the discovery review queue",
			Href:       "/discovery/apps?review_state=unreviewed",
		},
		{
			Label:      "Suspended identities",
			Metric:     FormatInt64(data.SuspendedHumanIdentities),
			MetricTone: kpiToneFromCount(data.SuspendedHumanIdentities, "warn"),
			Caption:    "human identities to review",
			Trend:      "Confirm access and offboarding state",
			Href:       "/identities?identity_type=human&status=suspended",
		},
		{
			Label:      "Connector health",
			Metric:     FormatInt64(connectors),
			MetricTone: kpiToneFromCount(connectors, "danger"),
			Caption:    connectorCaption,
			Trend:      "Check freshness and sync failures",
			Href:       "/settings/connector-health",
		},
	}
}

// CredentialsKPICards builds the three KPI cards above the credentials list.
func CredentialsKPICards(data viewmodels.CredentialsViewData) []KPICard {
	q := data.Query
	s := data.Summary

	inScopeActive := credentialsActiveSegmentSelected(q)
	needsActionActive := q.RiskLevel == "critical,high"
	pendingActive := q.Status == "pending_approval"
	needsActionCount := s.Critical + s.High + s.Warning

	return []KPICard{
		{
			Label:      "Needs action",
			Metric:     FormatInt64(needsActionCount),
			MetricTone: kpiToneFromCount(needsActionCount, "danger"),
			Caption:    "credentials need a rotation or approval",
			Breakdown: []KPIBreakdownSegment{
				{Label: "critical", Count: s.Critical, Tone: "critical"},
				{Label: "high", Count: s.High, Tone: "warn"},
				{Label: "warning", Count: s.Warning, Tone: "neutral"},
			},
			Href:     q.SegmentCritical().Href(),
			HxTarget: "#credentials-results",
			Active:   needsActionActive,
		},
		{
			Label:    "In scope",
			Metric:   FormatInt64(s.Total),
			Caption:  credentialsInScopeCaption(s.AssetCount),
			Trend:    credentialsInScopeTrend(s.Total, s.Active),
			Href:     q.SegmentActive().Href(),
			HxTarget: "#credentials-results",
			Active:   inScopeActive,
		},
		{
			Label:      "Pending approval",
			Metric:     FormatInt64(s.PendingApproval),
			MetricTone: kpiToneFromCount(s.PendingApproval, "warn"),
			Caption:    credentialsPendingCaption(s.PendingApproval),
			Trend:      credentialsPendingTrend(s.PendingApproval),
			Href:       q.SegmentPendingApproval().Href(),
			HxTarget:   "#credentials-results",
			Active:     pendingActive,
		},
	}
}

func credentialsInScopeCaption(assetCount int64) string {
	if assetCount > 0 {
		return "across " + FormatInt64(assetCount) + " assets"
	}
	return "across configured assets"
}

func credentialsInScopeTrend(total, active int64) string {
	if total > active {
		return FormatInt64(total-active) + " expired"
	}
	return ""
}

func credentialsPendingCaption(count int64) string {
	if count == 0 {
		return "everything's clear"
	}
	return "awaiting review"
}

func credentialsPendingTrend(count int64) string {
	if count == 0 {
		return "down from last check"
	}
	return ""
}

func kpiToneFromCount(count int64, tone string) string {
	if count <= 0 {
		return ""
	}
	return tone
}
