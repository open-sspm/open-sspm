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
			user: User{Mail: "Alice@example.com", UserPrincipalName: "alice@corp.example.com"},
			want: "Alice@example.com",
		},
		{
			name: "fallback_to_upn",
			user: User{UserPrincipalName: "bob@corp.example.com"},
			want: "bob@corp.example.com",
		},
		{
			name: "guest_prefers_other_mails_over_upn",
			user: User{UserType: "Guest", UserPrincipalName: "bob_example.com#EXT#@tenant.onmicrosoft.com", OtherMails: []string{"bob@example.com"}},
			want: "bob@example.com",
		},
		{
			name: "fallback_to_other_mails",
			user: User{OtherMails: []string{"", "carol@example.com"}},
			want: "carol@example.com",
		},
		{
			name: "fallback_to_proxy_addresses",
			user: User{ProxyAddresses: []string{"smtp:dave@example.com"}},
			want: "dave@example.com",
		},
		{
			name: "missing_email",
			user: User{Mail: "", UserPrincipalName: "not-an-email"},
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
