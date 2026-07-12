package views

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestMutationFormAttrsDropsRepeatsAndDisablesSubmitControls(t *testing.T) {
	attrs := MutationFormAttrs()

	if got := attrs["hx-sync"]; got != "this:drop" {
		t.Fatalf("hx-sync = %q, want this:drop", got)
	}
	wantDisabled := "find button[type='submit']:not(:disabled), find input[type='submit']:not(:disabled)"
	if got := attrs["hx-disabled-elt"]; got != wantDisabled {
		t.Fatalf("hx-disabled-elt = %q, want %q", got, wantDisabled)
	}
}

func TestScopedMutationAttrsUsesOneSharedResourceLock(t *testing.T) {
	attrs := ScopedMutationAttrs("connector-row-okta", "input[type='checkbox']")

	if got := attrs["hx-sync"]; got != "#connector-row-okta:drop" {
		t.Fatalf("hx-sync = %q, want row drop lock", got)
	}
	if got := attrs["hx-disabled-elt"]; got != "#connector-row-okta input[type='checkbox']:not(:disabled)" {
		t.Fatalf("hx-disabled-elt = %q, want active row switches", got)
	}
}

func TestScopedMutationAttrsRequiresScopeAndControls(t *testing.T) {
	tests := []struct {
		name     string
		scope    string
		controls string
	}{
		{name: "missing scope", controls: "button"},
		{name: "missing controls", scope: "panel"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("ScopedMutationAttrs should reject an incomplete scope")
				}
			}()
			ScopedMutationAttrs(tt.scope, tt.controls)
		})
	}
}

func TestMutationGuardsRenderOnFormsRowsAndActionGroups(t *testing.T) {
	tests := []struct {
		name      string
		render    func(*bytes.Buffer) error
		fragments []string
	}{
		{
			name: "ordinary form",
			render: func(body *bytes.Buffer) error {
				return LoginForm(viewmodels.LoginViewData{}).Render(context.Background(), body)
			},
			fragments: []string{`hx-sync="this:drop"`, `find button[type=&#39;submit&#39;]:not(:disabled)`},
		},
		{
			name: "shared connector row",
			render: func(body *bytes.Buffer) error {
				return OktaConnectorRow(viewmodels.ConnectorsViewData{}).Render(context.Background(), body)
			},
			fragments: []string{`hx-sync="#connector-row-okta:drop"`, `#connector-row-okta input[type=&#39;checkbox&#39;]:not(:disabled)`},
		},
		{
			name: "candidate action group",
			render: func(body *bytes.Buffer) error {
				return identityResolutionActions("csrf", viewmodels.IdentityResolutionCandidateItem{
					ID:        42,
					CanAccept: true,
					CanReject: true,
				}).Render(context.Background(), body)
			},
			fragments: []string{`id="identity-resolution-actions-42"`, `hx-sync="#identity-resolution-actions-42:drop"`, `#identity-resolution-actions-42 button[type=&#39;submit&#39;]:not(:disabled)`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body bytes.Buffer
			if err := tt.render(&body); err != nil {
				t.Fatalf("render: %v", err)
			}
			html := body.String()
			for _, fragment := range tt.fragments {
				if !strings.Contains(html, fragment) {
					t.Fatalf("rendered mutation control missing %q: %s", fragment, html)
				}
			}
		})
	}
}
