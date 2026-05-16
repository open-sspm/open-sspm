package views

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestPageHeaderWithoutDescriptionCollapsesWhenEmpty(t *testing.T) {
	t.Parallel()

	var body bytes.Buffer
	if err := PageHeader("").Render(context.Background(), &body); err != nil {
		t.Fatalf("render page header: %v", err)
	}

	html := body.String()
	if !strings.Contains(html, "empty:hidden") {
		t.Fatalf("empty page header should render a collapsible action row: %s", html)
	}
	if strings.Contains(html, "md:flex-row") {
		t.Fatalf("empty page header should not render the outer spacing wrapper: %s", html)
	}
}
