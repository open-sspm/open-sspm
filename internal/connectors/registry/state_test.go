package registry

import "testing"

func TestConnectorBrowseUsersHref(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		kind string
		want string
	}{
		{name: "okta", kind: "okta", want: "/accounts/okta"},
		{name: "entra", kind: "entra", want: "/accounts/entra"},
		{name: "google workspace", kind: "google_workspace", want: "/accounts/google-workspace"},
		{name: "github", kind: "github", want: "/accounts/github"},
		{name: "datadog", kind: "datadog", want: "/accounts/datadog"},
		{name: "aws identity center", kind: "aws_identity_center", want: "/accounts/aws"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := connectorBrowseUsersHref(tc.kind); got != tc.want {
				t.Fatalf("connectorBrowseUsersHref(%q) = %q, want %q", tc.kind, got, tc.want)
			}
		})
	}
}

func TestConnectorUnmanagedHref(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		kind       string
		sourceName string
		want       string
	}{
		{name: "okta", kind: "okta", want: "/assigned-apps"},
		{name: "entra", kind: "entra", want: "/accounts/unlinked/entra"},
		{name: "google workspace", kind: "google_workspace", want: "/accounts/unlinked/google-workspace"},
		{name: "github", kind: "github", sourceName: "acme", want: "/accounts/unlinked/github/acme"},
		{name: "datadog", kind: "datadog", sourceName: "datadoghq.com", want: "/accounts/unlinked/datadog/datadoghq.com"},
		{name: "aws identity center", kind: "aws_identity_center", want: "/accounts/unlinked/aws"},
		{name: "github without source name", kind: "github", want: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := connectorUnmanagedHref(tc.kind, tc.sourceName); got != tc.want {
				t.Fatalf("connectorUnmanagedHref(%q, %q) = %q, want %q", tc.kind, tc.sourceName, got, tc.want)
			}
		})
	}
}

func TestConnectorSecondaryLabel(t *testing.T) {
	t.Parallel()

	if got := connectorSecondaryLabel("okta"); got != "Browse apps" {
		t.Fatalf("connectorSecondaryLabel(okta) = %q, want %q", got, "Browse apps")
	}

	for _, kind := range []string{"entra", "google_workspace", "github", "datadog", "aws_identity_center"} {
		kind := kind
		t.Run(kind, func(t *testing.T) {
			t.Parallel()

			if got := connectorSecondaryLabel(kind); got != "Unlinked" {
				t.Fatalf("connectorSecondaryLabel(%q) = %q, want %q", kind, got, "Unlinked")
			}
		})
	}
}
