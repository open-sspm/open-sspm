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

func TestLayoutsDisableGlobalViewTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		render func(*bytes.Buffer) error
	}{
		{
			name: "authenticated",
			render: func(body *bytes.Buffer) error {
				return Layout(viewmodels.LayoutData{Title: "Test"}).Render(context.Background(), body)
			},
		},
		{
			name: "public",
			render: func(body *bytes.Buffer) error {
				return PublicLayout("Test", "", nil).Render(context.Background(), body)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body bytes.Buffer
			if err := tt.render(&body); err != nil {
				t.Fatalf("render layout: %v", err)
			}

			html := body.String()
			if !strings.Contains(html, `globalViewTransitions&#34;:false`) {
				t.Fatal("layout should disable global HTMX view transitions")
			}
			if strings.Contains(html, `globalViewTransitions&#34;:true`) {
				t.Fatal("layout should not enable global HTMX view transitions")
			}
		})
	}
}
