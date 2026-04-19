package views

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestGlobalViewRowShowsInactiveStatusLabel(t *testing.T) {
	t.Parallel()

	var body bytes.Buffer
	err := GlobalViewRow(viewmodels.GlobalViewAppCard{
		Name:        "GitHub",
		StatusLabel: "Invalid config",
		StatusClass: "badge bg-rose-100 text-rose-800",
	}, false).Render(context.Background(), &body)
	if err != nil {
		t.Fatalf("render global view row: %v", err)
	}

	html := body.String()
	if !strings.Contains(html, "Invalid config") {
		t.Fatalf("inactive row should render its status label: %s", html)
	}
	if strings.Contains(html, "Not reporting") {
		t.Fatalf("inactive row should not collapse to generic placeholder: %s", html)
	}
}
