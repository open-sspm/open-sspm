package views

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestCommandPaletteDialogReusesServerBackedSearch(t *testing.T) {
	var body bytes.Buffer
	err := CommandPaletteDialog(viewmodels.CommandSearchViewData{
		Placeholder: "Search identities and apps… (⌘K / Ctrl+K)",
		AriaLabel:   "Search identities and apps",
	}).Render(context.Background(), &body)
	if err != nil {
		t.Fatalf("render command palette dialog: %v", err)
	}

	html := body.String()
	if !strings.Contains(html, `hx-get="/command/search"`) {
		t.Fatalf("dialog should render command search input: %s", html)
	}
	if !strings.Contains(html, `data-command-root`) {
		t.Fatalf("dialog should keep command search root for HTMX swaps: %s", html)
	}
	if !strings.Contains(html, `class="command-palette-hints"`) {
		t.Fatalf("dialog should render keyboard hints footer: %s", html)
	}
}
