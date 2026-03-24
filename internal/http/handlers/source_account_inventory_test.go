package handlers

import "testing"

func TestSourceAccountInventoryDisplayName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		displayName string
		email       string
		externalID  string
		want        string
	}{
		{
			name:        "prefers display name",
			displayName: " Alice Example ",
			email:       "alice@example.com",
			externalID:  "alice-123",
			want:        "Alice Example",
		},
		{
			name:       "falls back to email",
			email:      " alice@example.com ",
			externalID: "alice-123",
			want:       "alice@example.com",
		},
		{
			name:       "falls back to external id",
			externalID: " alice-123 ",
			want:       "alice-123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := sourceAccountInventoryDisplayName(tt.displayName, tt.email, tt.externalID)
			if got != tt.want {
				t.Fatalf("sourceAccountInventoryDisplayName() = %q, want %q", got, tt.want)
			}
		})
	}
}
