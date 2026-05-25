package views

import "testing"

func TestConnectorDialogHref(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"/settings/connectors?open=github":              "/settings/connectors/github/dialog",
		"/settings/connectors?open=google_workspace":    "/settings/connectors/google_workspace/dialog",
		"/settings/connectors?open=aws_identity_center": "/settings/connectors/aws_identity_center/dialog",
		"/settings/connectors?open=unknown":             "",
		"/settings/connectors":                          "",
		"/settings/users?open=add":                      "",
	}

	for href, want := range tests {
		if got := ConnectorDialogHref(href); got != want {
			t.Fatalf("ConnectorDialogHref(%q) = %q, want %q", href, got, want)
		}
	}
}
