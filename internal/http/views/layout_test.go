package views

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestLayoutRendersResponsiveSidebarContract(t *testing.T) {
	t.Parallel()

	var body bytes.Buffer
	if err := Layout(viewmodels.LayoutData{Title: "Test"}).Render(context.Background(), &body); err != nil {
		t.Fatalf("render layout: %v", err)
	}

	html := body.String()
	for _, want := range []string{
		`data-breakpoint="1024"`,
		`data-sidebar-mobile-close`,
		`aria-label="Close navigation"`,
		`data-sidebar-content`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("layout should contain %s", want)
		}
	}

	contentIndex := strings.Index(html, `data-sidebar-content`)
	skipLinkIndex := strings.Index(html, `href="#main"`)
	if contentIndex < 0 || skipLinkIndex < contentIndex {
		t.Fatal("skip link should render inside the inert sidebar content subtree")
	}
}
