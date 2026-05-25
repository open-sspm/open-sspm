package auth

import "testing"

func TestIsValidEmailRejectsDisplayNameForms(t *testing.T) {
	tests := []struct {
		name  string
		email string
		want  bool
	}{
		{name: "simple lowercase", email: "person@example.com", want: true},
		{name: "trims and lowercases", email: " Person@Example.com ", want: true},
		{name: "display name", email: "Person <person@example.com>", want: false},
		{name: "missing domain dot", email: "person@example", want: false},
		{name: "empty", email: "", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidEmail(tt.email); got != tt.want {
				t.Fatalf("IsValidEmail(%q) = %v, want %v", tt.email, got, tt.want)
			}
		})
	}
}
