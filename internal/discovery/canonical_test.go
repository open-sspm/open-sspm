package discovery

import "testing"

func TestCanonicalKey(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input CanonicalInput
		want  string
	}{
		{
			name: "domain etld plus one",
			input: CanonicalInput{
				SourceKind:   "okta",
				SourceName:   "acme.okta.com",
				SourceAppID:  "00o123",
				SourceDomain: "https://sub.prod.example.co.uk/path",
			},
			want: "domain:example.co.uk",
		},
		{
			name: "entra app id fallback",
			input: CanonicalInput{
				SourceKind:  "entra",
				SourceAppID: "11111111-2222-3333-4444-555555555555",
			},
			want: "entra_appid:11111111-2222-3333-4444-555555555555",
		},
		{
			name: "okta source app fallback",
			input: CanonicalInput{
				SourceKind:  "okta",
				SourceName:  "dev-123.okta.com",
				SourceAppID: "00o8xv2",
			},
			want: "okta_app:dev-123.okta.com:00o8xv2",
		},
		{
			name: "name fallback",
			input: CanonicalInput{
				SourceKind:    "entra",
				SourceAppName: "My Awesome SaaS",
			},
			want: "name:my-awesome-saas:entra",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := CanonicalKey(tc.input); got != tc.want {
				t.Fatalf("CanonicalKey() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBuildMetadata(t *testing.T) {
	t.Parallel()

	t.Run("infers vendor from domain", func(t *testing.T) {
		t.Parallel()

		meta := BuildMetadata(CanonicalInput{
			SourceKind:    "entra",
			SourceAppName: "Payroll Tool",
			SourceDomain:  "payroll.acme.com",
		})

		if meta.CanonicalKey != "domain:acme.com" {
			t.Fatalf("CanonicalKey = %q", meta.CanonicalKey)
		}
		if meta.DisplayName != "Payroll Tool" {
			t.Fatalf("DisplayName = %q", meta.DisplayName)
		}
		if meta.Domain != "acme.com" {
			t.Fatalf("Domain = %q", meta.Domain)
		}
		if meta.VendorName != "Acme" {
			t.Fatalf("VendorName = %q", meta.VendorName)
		}
	})

	t.Run("keeps vendor empty without hints", func(t *testing.T) {
		t.Parallel()

		meta := BuildMetadata(CanonicalInput{
			SourceKind:    "entra",
			SourceAppName: "Payroll Tool",
		})
		if meta.VendorName != "" {
			t.Fatalf("VendorName = %q, want empty", meta.VendorName)
		}
	})

	t.Run("uses provided vendor hint without affecting canonical key", func(t *testing.T) {
		t.Parallel()

		meta := BuildMetadata(CanonicalInput{
			SourceKind:       "entra",
			SourceAppName:    "Payroll Tool",
			SourceVendorName: "Contoso",
		})
		if meta.VendorName != "Contoso" {
			t.Fatalf("VendorName = %q, want %q", meta.VendorName, "Contoso")
		}
		if meta.CanonicalKey != "name:payroll-tool:entra" {
			t.Fatalf("CanonicalKey = %q", meta.CanonicalKey)
		}
	})
}
