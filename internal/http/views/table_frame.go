package views

import "strings"

type TableVariant string

const (
	TableVariantInventory   TableVariant = "inventory"
	TableVariantOperational TableVariant = "operational"
	TableVariantDetail      TableVariant = "detail"
)

type TableEmptyKind string

const (
	TableEmptyInitial  TableEmptyKind = "initial"
	TableEmptyFiltered TableEmptyKind = "filtered"
)

type TableFrameConfig struct {
	ID          string
	ResultsID   string
	Title       string
	Description string
	Variant     TableVariant

	Caption      string
	LoadingLabel string
	ResultNoun   string
	Total        int64
	ShowingFrom  int
	ShowingTo    int
	HasRows      bool
	HasFilters   bool
	ClearHref    string
	ClearLabel   string
	ClearTarget  string

	EmptyKind        TableEmptyKind
	EmptyTitle       string
	EmptyDescription string
	EmptyActionHref  string
	EmptyActionLabel string

	Page       int
	TotalPages int
	PageHref   func(int) string
}

type TableColumnPriority string

const (
	TableColumnPrimary   TableColumnPriority = "primary"
	TableColumnSecondary TableColumnPriority = "secondary"
	TableColumnOptional  TableColumnPriority = "optional"
)

type TableColumnAlign string

const (
	TableColumnAlignStart TableColumnAlign = "start"
	TableColumnAlignEnd   TableColumnAlign = "end"
)

type TableColumnConfig struct {
	Key      string
	Label    string
	Priority TableColumnPriority
	Align    TableColumnAlign
	Class    string
	SortKey  string
}

type TableSortConfig struct {
	ActiveKey   string
	Direction   string
	TargetID    string
	HrefForSort func(key, direction string) string
}

func TableFrameClass(cfg TableFrameConfig) string {
	variant := strings.TrimSpace(string(cfg.Variant))
	if variant == "" {
		variant = string(TableVariantInventory)
	}
	return "osspm-table-frame osspm-table-frame-" + variant
}

func TableColumnClass(column TableColumnConfig) string {
	classes := []string{"osspm-table-column", "osspm-table-column-" + string(column.Priority)}
	if column.Align == TableColumnAlignEnd {
		classes = append(classes, "osspm-num")
	}
	if strings.TrimSpace(column.Class) != "" {
		classes = append(classes, column.Class)
	}
	return strings.Join(classes, " ")
}

func TableResultSummary(cfg TableFrameConfig) string {
	noun := strings.TrimSpace(cfg.ResultNoun)
	if noun == "" {
		noun = "results"
	}
	if cfg.Total <= 0 {
		return "No matching " + noun
	}
	if cfg.ShowingFrom > 0 && cfg.ShowingTo >= cfg.ShowingFrom {
		return "Showing " + FormatInt(cfg.ShowingFrom) + "–" + FormatInt(cfg.ShowingTo) + " of " + FormatInt64(cfg.Total)
	}
	return FormatInt64(cfg.Total) + " " + noun
}

func TableSortAria(column TableColumnConfig, sort TableSortConfig) string {
	if column.SortKey == "" || column.SortKey != sort.ActiveKey {
		return "none"
	}
	if strings.EqualFold(sort.Direction, "desc") {
		return "descending"
	}
	return "ascending"
}

func TableNextSortDirection(column TableColumnConfig, sort TableSortConfig) string {
	if column.SortKey == sort.ActiveKey && !strings.EqualFold(sort.Direction, "desc") {
		return "desc"
	}
	return "asc"
}

func TableSortHref(column TableColumnConfig, sort TableSortConfig) string {
	if column.SortKey == "" || sort.HrefForSort == nil {
		return ""
	}
	return sort.HrefForSort(column.SortKey, TableNextSortDirection(column, sort))
}
