package accessgraph

import "testing"

func TestParseCanonicalResourceRef(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		in           string
		wantKind     string
		wantExternal string
		wantOK       bool
	}{
		{name: "empty", in: "", wantOK: false},
		{name: "noColon", in: "github_repo", wantOK: false},
		{name: "missingKind", in: ":owner/repo", wantOK: false},
		{name: "missingExternalID", in: "github_repo:", wantOK: false},
		{name: "basic", in: "github_repo:owner/repo", wantKind: "github_repo", wantExternal: "owner/repo", wantOK: true},
		{name: "whitespace", in: "  GitHub_Repo : owner/repo  ", wantKind: "github_repo", wantExternal: "owner/repo", wantOK: true},
		{name: "externalIDWithColon", in: "aws_account:arn:aws:iam::123:role/Admin", wantKind: "aws_account", wantExternal: "arn:aws:iam::123:role/Admin", wantOK: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotKind, gotExternal, gotOK := ParseCanonicalResourceRef(tt.in)
			if gotOK != tt.wantOK {
				t.Fatalf("ok=%v, want %v (kind=%q external=%q)", gotOK, tt.wantOK, gotKind, gotExternal)
			}
			if !tt.wantOK {
				return
			}
			if gotKind != tt.wantKind {
				t.Fatalf("kind=%q, want %q", gotKind, tt.wantKind)
			}
			if gotExternal != tt.wantExternal {
				t.Fatalf("external=%q, want %q", gotExternal, tt.wantExternal)
			}
		})
	}
}

func TestBuildResourceHref(t *testing.T) {
	t.Parallel()

	got := BuildResourceHref("github", "my-org", "github_repo", "owner/repo")
	want := "/resources/github/my-org/github_repo/owner/repo"
	if got != want {
		t.Fatalf("href=%q, want %q", got, want)
	}
}

func TestBuildResourceHrefFromResourceRef(t *testing.T) {
	t.Parallel()

	got := BuildResourceHrefFromResourceRef("datadog", "datadoghq.com", "datadog_role:abc123")
	want := "/resources/datadog/datadoghq.com/datadog_role/abc123"
	if got != want {
		t.Fatalf("href=%q, want %q", got, want)
	}
}

func TestDisplayResourceLabel(t *testing.T) {
	t.Parallel()

	t.Run("datadog role", func(t *testing.T) {
		t.Parallel()

		raw := []byte(`{"role_id":"abc123","role_name":"Admin"}`)
		got := DisplayResourceLabel("datadog_role:abc123", raw)
		if got != "Admin" {
			t.Fatalf("label=%q, want %q", got, "Admin")
		}
	})

	t.Run("entra service principal", func(t *testing.T) {
		t.Parallel()

		raw := []byte(`{"service_principal_id":"sp-123","service_principal_name":"Zendesk"}`)
		got := DisplayResourceLabel("entra_service_principal:sp-123", raw)
		if got != "Zendesk" {
			t.Fatalf("label=%q, want %q", got, "Zendesk")
		}
	})

	t.Run("entra directory role", func(t *testing.T) {
		t.Parallel()

		raw := []byte(`{"role_definition_id":"role-def-1","role_template_id":"tmpl-1","role_display_name":"Global Administrator"}`)
		got := DisplayResourceLabel("entra_directory_role:tmpl-1", raw)
		if got != "Global Administrator" {
			t.Fatalf("label=%q, want %q", got, "Global Administrator")
		}
	})

	t.Run("entra custom directory role fallback id", func(t *testing.T) {
		t.Parallel()

		raw := []byte(`{"role_definition_id":"role-def-2"}`)
		got := DisplayResourceLabel("entra_directory_role:role-def-2", raw)
		if got != "role-def-2" {
			t.Fatalf("label=%q, want %q", got, "role-def-2")
		}
	})
}

func TestDisplayEntitlementPermission(t *testing.T) {
	t.Parallel()

	t.Run("entra app role keeps readable label", func(t *testing.T) {
		t.Parallel()

		raw := []byte(`{"role_name":"Agent","app_role_id":"role-1"}`)
		got := DisplayEntitlementPermission("entra_app_role", "Agent (role-1)", raw)
		if got != "Agent (role-1)" {
			t.Fatalf("label=%q, want %q", got, "Agent (role-1)")
		}
	})

	t.Run("non-entra permission falls back to stored value", func(t *testing.T) {
		t.Parallel()

		got := DisplayEntitlementPermission("github_team_repo_permission", "admin", nil)
		if got != "admin" {
			t.Fatalf("label=%q, want %q", got, "admin")
		}
	})
}
