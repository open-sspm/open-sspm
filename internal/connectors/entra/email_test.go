package entra

import "testing"

func TestPreferredEmail(t *testing.T) {
	tests := []struct {
		name string
		user User
		want string
	}{
		{
			name: "prefer_mail",
			user: mustParseUser(t, `{"id":"user-1","mail":"Alice@example.com","userPrincipalName":"alice@corp.example.com"}`),
			want: "Alice@example.com",
		},
		{
			name: "fallback_to_upn",
			user: mustParseUser(t, `{"id":"user-2","userPrincipalName":"bob@corp.example.com"}`),
			want: "bob@corp.example.com",
		},
		{
			name: "guest_prefers_other_mails_over_upn",
			user: mustParseUser(t, `{"id":"user-3","userType":"Guest","userPrincipalName":"bob_example.com#EXT#@tenant.onmicrosoft.com","otherMails":["bob@example.com"]}`),
			want: "bob@example.com",
		},
		{
			name: "fallback_to_other_mails",
			user: mustParseUser(t, `{"id":"user-4","otherMails":["","carol@example.com"]}`),
			want: "carol@example.com",
		},
		{
			name: "fallback_to_proxy_addresses",
			user: mustParseUser(t, `{"id":"user-5","proxyAddresses":["smtp:dave@example.com"]}`),
			want: "dave@example.com",
		},
		{
			name: "missing_email",
			user: mustParseUser(t, `{"id":"user-6","mail":"","userPrincipalName":"not-an-email"}`),
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := preferredEmail(tt.user)
			if got != tt.want {
				t.Fatalf("preferredEmail() = %q, want %q", got, tt.want)
			}
		})
	}
}
