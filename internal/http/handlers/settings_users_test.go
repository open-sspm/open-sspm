package handlers

import "testing"

func TestSettingsUserUpdateSuccessTitle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		changeRole     bool
		changePassword bool
		want           string
	}{
		{
			name:           "password only",
			changeRole:     false,
			changePassword: true,
			want:           "Password updated",
		},
		{
			name:           "group only",
			changeRole:     true,
			changePassword: false,
			want:           "Group updated",
		},
		{
			name:           "group and password",
			changeRole:     true,
			changePassword: true,
			want:           "User updated",
		},
		{
			name:           "no flags defaults to generic title",
			changeRole:     false,
			changePassword: false,
			want:           "User updated",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := settingsUserUpdateSuccessTitle(tc.changeRole, tc.changePassword)
			if got != tc.want {
				t.Fatalf("settingsUserUpdateSuccessTitle(%v, %v) = %q, want %q", tc.changeRole, tc.changePassword, got, tc.want)
			}
		})
	}
}
