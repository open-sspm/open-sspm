package views

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/open-sspm/open-sspm/internal/http/querystate"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestTableResultSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  TableFrameConfig
		want string
	}{
		{name: "empty", cfg: TableFrameConfig{ResultNoun: "rules"}, want: "No matching rules"},
		{name: "total", cfg: TableFrameConfig{ResultNoun: "rules", Total: 24}, want: "24 rules"},
		{name: "range", cfg: TableFrameConfig{ResultNoun: "records", Total: 86, ShowingFrom: 26, ShowingTo: 50}, want: "Showing 26–50 of 86"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := TableResultSummary(test.cfg); got != test.want {
				t.Fatalf("TableResultSummary() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestAppAssetsTableConfigTreatsSearchAsFilteredState(t *testing.T) {
	t.Parallel()

	cfg := AppAssetsTableConfig(viewmodels.AppAssetsViewData{
		Query: querystate.AppAssetsQuery{Q: "missing asset"},
	})
	if !cfg.HasFilters || cfg.EmptyKind != TableEmptyFiltered || cfg.ClearHref != "/app-assets" {
		t.Fatalf("AppAssetsTableConfig() = %#v", cfg)
	}
}

func TestSortableTableHeaderExposesCurrentDirectionAndNextTransition(t *testing.T) {
	t.Parallel()

	column := TableColumnConfig{Label: "Identity", Priority: TableColumnPrimary, SortKey: "identity"}
	sort := TableSortConfig{
		ActiveKey: "identity",
		Direction: "asc",
		TargetID:  "access-frame",
		HrefForSort: func(key, direction string) string {
			return "/resources/demo?sort_by=" + key + "&sort_dir=" + direction
		},
	}

	var body bytes.Buffer
	if err := TableColumnHeader(column, sort).Render(context.Background(), &body); err != nil {
		t.Fatalf("render sortable header: %v", err)
	}
	html := body.String()
	for _, want := range []string{
		`aria-sort="ascending"`,
		`sort_by=identity`,
		`sort_dir=desc`,
		`hx-target="#access-frame"`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("sortable header missing %q: %s", want, html)
		}
	}
}

func TestTableFrameDistinguishesFilteredEmptyState(t *testing.T) {
	t.Parallel()

	var body bytes.Buffer
	if err := TableResults(TableFrameConfig{
		ResultsID:        "rules-results",
		ResultNoun:       "rules",
		EmptyKind:        TableEmptyFiltered,
		EmptyTitle:       "No rules found",
		EmptyDescription: "No rules match the current filters.",
	}).Render(context.Background(), &body); err != nil {
		t.Fatalf("render table results: %v", err)
	}
	html := body.String()
	if !strings.Contains(html, `data-table-empty-kind="filtered"`) || !strings.Contains(html, "No rules match the current filters") {
		t.Fatalf("filtered empty state is not represented: %s", html)
	}
}
