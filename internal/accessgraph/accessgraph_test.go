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

	raw := []byte(`{"role_id":"abc123","role_name":"Admin"}`)
	got := DisplayResourceLabel("datadog_role:abc123", raw)
	if got != "Admin" {
		t.Fatalf("label=%q, want %q", got, "Admin")
	}
}
