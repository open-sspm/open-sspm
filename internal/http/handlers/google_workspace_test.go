package handlers

import (
	"net/http"
	"testing"
)

func TestGoogleWorkspacePageParameterParsing(t *testing.T) {
	t.Parallel()

	t.Run("uses provided positive page number", func(t *testing.T) {
		t.Parallel()

		c, _ := newTestContext(http.MethodGet, "/google-workspace/users?page=3&q=alice")
		if got := parsePageParam(c); got != 3 {
			t.Fatalf("parsePageParam() = %d, want %d", got, 3)
		}
	})

	t.Run("falls back to first page on invalid input", func(t *testing.T) {
		t.Parallel()

		c, _ := newTestContext(http.MethodGet, "/google-workspace/users?page=bad")
		if got := parsePageParam(c); got != 1 {
			t.Fatalf("parsePageParam() = %d, want %d", got, 1)
		}
	})

	t.Run("falls back to first page on zero page", func(t *testing.T) {
		t.Parallel()

		c, _ := newTestContext(http.MethodGet, "/google-workspace/users?page=0")
		if got := parsePageParam(c); got != 1 {
			t.Fatalf("parsePageParam() = %d, want %d", got, 1)
		}
	})
}

func TestGoogleWorkspacePaginationContracts(t *testing.T) {
	t.Parallel()

	t.Run("computes page and offset for middle page", func(t *testing.T) {
		t.Parallel()

		page, totalPages, offset := paginate(45, 2, 20)
		if page != 2 || totalPages != 3 || offset != 20 {
			t.Fatalf("paginate() = (%d, %d, %d), want (2, 3, 20)", page, totalPages, offset)
		}
	})

	t.Run("clamps page to last page", func(t *testing.T) {
		t.Parallel()

		page, totalPages, offset := paginate(12, 9, 10)
		if page != 2 || totalPages != 2 || offset != 10 {
			t.Fatalf("paginate() = (%d, %d, %d), want (2, 2, 10)", page, totalPages, offset)
		}
	})

	t.Run("computes showing range bounded by total count", func(t *testing.T) {
		t.Parallel()

		showingFrom, showingTo := showingRange(45, 40, 10)
		if showingFrom != 41 || showingTo != 45 {
			t.Fatalf("showingRange() = (%d, %d), want (41, 45)", showingFrom, showingTo)
		}
	})
}

func TestGoogleWorkspaceUnavailableMessage(t *testing.T) {
	t.Parallel()

	if got := connectorUnavailableMessage("Google Workspace", true, false); got != "Google Workspace sync is disabled. Enable it in Connectors." {
		t.Fatalf("connectorUnavailableMessage(disabled) = %q", got)
	}
	if got := connectorUnavailableMessage("Google Workspace", false, false); got != "Google Workspace is not configured yet. Add credentials in Connectors." {
		t.Fatalf("connectorUnavailableMessage(unconfigured) = %q", got)
	}
}
