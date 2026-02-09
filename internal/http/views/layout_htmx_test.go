package views

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestLayoutEnablesGlobalHTMXBoost(t *testing.T) {
	t.Parallel()

	html := renderViewComponent(t, Layout(viewmodels.LayoutData{
		Title:     "Dashboard",
		CSRFToken: "csrf-token-123",
	}))

	assertContains(t, html, `hx-boost="true"`)
	assertContains(t, html, `X-CSRF-Token`)
	assertContains(t, html, `csrf-token-123`)
}

func TestLayoutLogoutFormOptsOutOfHTMXBoost(t *testing.T) {
	t.Parallel()

	html := renderViewComponent(t, Layout(viewmodels.LayoutData{
		Title:     "Dashboard",
		CSRFToken: "csrf-token-123",
		UserEmail: "admin@example.com",
	}))

	assertContains(t, html, `form method="post" action="/logout" hx-boost="false"`)
}
