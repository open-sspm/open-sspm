package connectordisplay

import "testing"

func TestKindLabel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		kind string
		want string
	}{
		{kind: "aws", want: "AWS Identity Center"},
		{kind: "aws_identity_center", want: "AWS Identity Center"},
		{kind: "entra", want: "Microsoft Entra"},
		{kind: "pagerduty", want: "PagerDuty"},
		{kind: "custom_source", want: "Custom Source"},
		{kind: "", want: "—"},
	}

	for _, tt := range tests {
		t.Run(tt.kind, func(t *testing.T) {
			t.Parallel()
			if got := KindLabel(tt.kind); got != tt.want {
				t.Fatalf("KindLabel(%q) = %q, want %q", tt.kind, got, tt.want)
			}
		})
	}
}

func TestScopeLabel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		kind string
		want string
	}{
		{kind: "okta", want: "Org URL"},
		{kind: "datadog", want: "Site"},
		{kind: "vault", want: "Vault"},
		{kind: "google_workspace", want: "Customer"},
		{kind: "aws_identity_center", want: "Instance"},
		{kind: "custom_source", want: "Source"},
	}

	for _, tt := range tests {
		t.Run(tt.kind, func(t *testing.T) {
			t.Parallel()
			if got := ScopeLabel(tt.kind); got != tt.want {
				t.Fatalf("ScopeLabel(%q) = %q, want %q", tt.kind, got, tt.want)
			}
		})
	}
}
