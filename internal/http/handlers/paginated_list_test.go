package handlers

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestPaginatedListStatePageData(t *testing.T) {
	t.Parallel()

	layout := viewmodels.LayoutData{Title: "Test Page"}

	t.Run("zero results clamp to first page and preserve empty state href", func(t *testing.T) {
		t.Parallel()

		pagination := newPaginatedListState(0, 9, 20)
		data := pagination.PageData(layout, 0, "No rows", "/settings/connectors")

		if data.Page != 1 || data.TotalPages != 1 {
			t.Fatalf("page data = (%d, %d), want (1, 1)", data.Page, data.TotalPages)
		}
		if data.ShowingCount != 0 || data.ShowingFrom != 0 || data.ShowingTo != 0 || data.TotalCount != 0 {
			t.Fatalf("unexpected zero-result counts: %+v", data)
		}
		if data.EmptyStateHref != "/settings/connectors" {
			t.Fatalf("EmptyStateHref = %q, want /settings/connectors", data.EmptyStateHref)
		}
	})

	t.Run("clamps requested page to last page", func(t *testing.T) {
		t.Parallel()

		pagination := newPaginatedListState(25, 9, 10)
		data := pagination.PageData(layout, 5, "No rows", "")

		if data.Page != 3 || data.TotalPages != 3 {
			t.Fatalf("page data = (%d, %d), want (3, 3)", data.Page, data.TotalPages)
		}
		if pagination.Offset() != 20 {
			t.Fatalf("Offset() = %d, want 20", pagination.Offset())
		}
		if data.ShowingFrom != 21 || data.ShowingTo != 25 {
			t.Fatalf("showing range = (%d, %d), want (21, 25)", data.ShowingFrom, data.ShowingTo)
		}
	})

	t.Run("computes showing range on partially filled last page", func(t *testing.T) {
		t.Parallel()

		pagination := newPaginatedListState(45, 3, 20)
		data := pagination.PageData(layout, 5, "No rows", "")

		if data.ShowingCount != 5 {
			t.Fatalf("ShowingCount = %d, want 5", data.ShowingCount)
		}
		if data.ShowingFrom != 41 || data.ShowingTo != 45 {
			t.Fatalf("showing range = (%d, %d), want (41, 45)", data.ShowingFrom, data.ShowingTo)
		}
	})
}
