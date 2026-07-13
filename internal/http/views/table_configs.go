package views

import (
	"strings"

	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func ResourceAccessTableConfig(data viewmodels.ResourceShowViewData) TableFrameConfig {
	hasFilters := strings.TrimSpace(data.Query.Q) != "" || data.Query.FilterCount() > 0
	emptyKind := TableEmptyInitial
	emptyDescription := "No entitlements are currently associated with this resource."
	if hasFilters {
		emptyKind = TableEmptyFiltered
		emptyDescription = "No entitlements match the current search and filters."
	}
	return TableFrameConfig{
		ID:               "resource-access-frame",
		ResultsID:        "resource-access-results",
		Title:            "Access",
		Variant:          TableVariantDetail,
		Caption:          "People and accounts with access to this resource.",
		LoadingLabel:     "Updating access…",
		ResultNoun:       "access records",
		Total:            data.MatchingRows,
		ShowingFrom:      data.ShowingFrom,
		ShowingTo:        data.ShowingTo,
		HasRows:          data.HasRows,
		HasFilters:       hasFilters,
		ClearHref:        data.Query.BasePath,
		ClearLabel:       "Clear filters",
		ClearTarget:      "resource-access-frame",
		EmptyKind:        emptyKind,
		EmptyTitle:       "No access found",
		EmptyDescription: emptyDescription,
		Page:             data.Page,
		TotalPages:       data.TotalPages,
		PageHref:         ResourceAccessPageHref(data.Query),
	}
}

func FindingsRulesTableConfig(data viewmodels.FindingsRulesetViewData) TableFrameConfig {
	hasFilters := strings.TrimSpace(data.Query.Q) != "" || data.Query.FilterCount() > 0
	emptyKind := TableEmptyInitial
	emptyDescription := "This ruleset has no active rules."
	if hasFilters {
		emptyKind = TableEmptyFiltered
		emptyDescription = "No rules match the current search and filters."
	}
	return TableFrameConfig{
		ID:               "rules-card",
		ResultsID:        "rules-results",
		Title:            "Rules",
		Description:      "Search, filter, and review the current results for this ruleset.",
		Variant:          TableVariantInventory,
		Caption:          "Rules in this ruleset with severity, monitoring, and current status.",
		LoadingLabel:     "Updating rules…",
		ResultNoun:       "rules",
		Total:            int64(len(data.Rules)),
		HasRows:          data.HasRules,
		HasFilters:       hasFilters,
		ClearHref:        data.Query.BasePath,
		ClearLabel:       "Clear filters",
		ClearTarget:      "rules-card",
		EmptyKind:        emptyKind,
		EmptyTitle:       "No rules found",
		EmptyDescription: emptyDescription,
	}
}

func AppAssetsTableConfig(data viewmodels.AppAssetsViewData) TableFrameConfig {
	hasFilters := data.Query.HasFilters()
	emptyKind := TableEmptyInitial
	if hasFilters {
		emptyKind = TableEmptyFiltered
	}
	cfg := TableFrameConfig{
		ResultsID:        "app-assets-results",
		Variant:          TableVariantInventory,
		Caption:          "Programmatic app assets with type, source, owner counts, credential counts, and last-seen time.",
		LoadingLabel:     "Updating app assets…",
		ResultNoun:       "app assets",
		Total:            data.TotalCount,
		ShowingFrom:      data.ShowingFrom,
		ShowingTo:        data.ShowingTo,
		HasRows:          data.HasItems,
		HasFilters:       hasFilters,
		ClearHref:        "/app-assets",
		ClearTarget:      "app-assets-results",
		EmptyKind:        emptyKind,
		EmptyTitle:       "No app assets found",
		EmptyDescription: data.EmptyStateMsg,
		Page:             data.Page,
		TotalPages:       data.TotalPages,
		PageHref:         AppAssetsPageHref(data.Query),
	}
	if data.Layout.IsAdmin && !hasFilters {
		cfg.EmptyActionHref = "/settings/connectors"
		cfg.EmptyActionLabel = "Configure connectors"
	}
	return cfg
}

func NonHumanIdentitiesTableConfig(data viewmodels.NonHumanIdentitiesViewData) TableFrameConfig {
	hasFilters := data.Query.HasFilters()
	emptyKind := TableEmptyInitial
	if hasFilters {
		emptyKind = TableEmptyFiltered
	}
	return TableFrameConfig{
		ResultsID:        "non-human-identities-inventory",
		Variant:          TableVariantInventory,
		Caption:          "Non-human principals with ownership, credential risk, and activity.",
		LoadingLabel:     "Updating non-human identities…",
		ResultNoun:       "non-human identities",
		Total:            data.TotalCount,
		ShowingFrom:      data.ShowingFrom,
		ShowingTo:        data.ShowingTo,
		HasRows:          data.HasItems,
		HasFilters:       hasFilters,
		ClearHref:        "/non-human-identities",
		ClearTarget:      "non-human-identities-inventory",
		EmptyKind:        emptyKind,
		EmptyTitle:       "No non-human identities",
		EmptyDescription: data.EmptyStateMsg,
		Page:             data.Page,
		TotalPages:       data.TotalPages,
		PageHref:         NonHumanIdentitiesPageHref(data.Query),
	}
}

func ConnectorsTableConfig() TableFrameConfig {
	return TableFrameConfig{
		ResultsID:        "connectors-table-results",
		Variant:          TableVariantOperational,
		Caption:          "Available connectors, configuration status, enabled state, and identity source settings.",
		LoadingLabel:     "Updating connectors…",
		ResultNoun:       "connectors",
		Total:            7,
		HasRows:          true,
		EmptyKind:        TableEmptyInitial,
		EmptyTitle:       "No connectors available",
		EmptyDescription: "No connector types are available in this build.",
	}
}

func ConnectorHealthTableConfig(data viewmodels.ConnectorHealthViewData) TableFrameConfig {
	return TableFrameConfig{
		ID:               "connector-health-frame",
		ResultsID:        "connector-health-results",
		Title:            "Connector health",
		Description:      data.SummaryLabel,
		Variant:          TableVariantOperational,
		Caption:          "Connector health, recent successful runs, success rates, durations, and actions.",
		LoadingLabel:     "Updating connector health…",
		ResultNoun:       "connectors",
		Total:            int64(len(data.Items)),
		HasRows:          len(data.Items) > 0,
		EmptyKind:        TableEmptyInitial,
		EmptyTitle:       "No connector health data",
		EmptyDescription: "Enable a connector to begin collecting health information.",
	}
}

func ResourceAccessColumns() []TableColumnConfig {
	return []TableColumnConfig{
		{Key: "identity", Label: "Identity", Priority: TableColumnPrimary, SortKey: "identity"},
		{Key: "account", Label: "Account", Priority: TableColumnPrimary, SortKey: "account"},
		{Key: "permission", Label: "Permission", Priority: TableColumnSecondary, SortKey: "permission"},
		{Key: "kind", Label: "Kind", Priority: TableColumnSecondary, SortKey: "kind"},
		{Key: "link", Label: "Link", Priority: TableColumnOptional},
	}
}

func ResourceAccessSortConfig(data viewmodels.ResourceShowViewData) TableSortConfig {
	return TableSortConfig{
		ActiveKey: data.Query.SortBy,
		Direction: data.Query.SortDir,
		TargetID:  "resource-access-frame",
		HrefForSort: func(key, direction string) string {
			return data.Query.WithSort(key, direction).Href()
		},
	}
}

func FindingsRulesColumns() []TableColumnConfig {
	return []TableColumnConfig{
		{Key: "rule", Label: "Rule", Priority: TableColumnPrimary},
		{Key: "severity", Label: "Severity", Priority: TableColumnSecondary},
		{Key: "monitoring", Label: "Monitoring", Priority: TableColumnSecondary},
		{Key: "status", Label: "Status", Priority: TableColumnPrimary},
	}
}

func AppAssetsColumns() []TableColumnConfig {
	return []TableColumnConfig{
		{Key: "asset", Label: "Asset", Priority: TableColumnPrimary, Class: "w-[55%]"},
		{Key: "owners", Label: "Owners", Priority: TableColumnSecondary, Align: TableColumnAlignEnd, Class: "w-[12%]"},
		{Key: "credentials", Label: "Credentials", Priority: TableColumnSecondary, Align: TableColumnAlignEnd, Class: "w-[15%]"},
		{Key: "last_seen", Label: "Last seen", Priority: TableColumnSecondary, Align: TableColumnAlignEnd, Class: "w-[18%]"},
	}
}

func NonHumanIdentitiesColumns() []TableColumnConfig {
	return []TableColumnConfig{
		{Key: "principal", Label: "Principal", Priority: TableColumnPrimary},
		{Key: "owner", Label: "Owner", Priority: TableColumnSecondary},
		{Key: "risk", Label: "Risk", Priority: TableColumnPrimary},
		{Key: "activity", Label: "Activity", Priority: TableColumnSecondary},
		{Key: "links", Label: "Links", Priority: TableColumnOptional, Align: TableColumnAlignEnd},
	}
}

func ConnectorsColumns() []TableColumnConfig {
	return []TableColumnConfig{
		{Key: "connector", Label: "Connector", Priority: TableColumnPrimary},
		{Key: "configured", Label: "Configured", Priority: TableColumnSecondary},
		{Key: "enabled", Label: "Enabled", Priority: TableColumnPrimary},
		{Key: "identity_source", Label: "Identity source", Priority: TableColumnSecondary},
		{Key: "actions", Label: "Actions", Priority: TableColumnPrimary, Align: TableColumnAlignEnd},
	}
}

func ConnectorHealthColumns(lookback string) []TableColumnConfig {
	return []TableColumnConfig{
		{Key: "connector", Label: "Connector", Priority: TableColumnPrimary},
		{Key: "health", Label: "Health", Priority: TableColumnPrimary},
		{Key: "last_success", Label: "Last success", Priority: TableColumnSecondary},
		{Key: "last_run", Label: "Last run", Priority: TableColumnSecondary},
		{Key: "success_rate", Label: lookback + " success", Priority: TableColumnOptional},
		{Key: "duration", Label: lookback + " avg (success)", Priority: TableColumnOptional},
		{Key: "actions", Label: "Actions", Priority: TableColumnPrimary, Align: TableColumnAlignEnd},
	}
}
