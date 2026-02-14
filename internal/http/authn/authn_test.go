package authn

import "testing"

func TestSanitizeNext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: ""},
		{name: "whitespace", in: "   ", want: ""},
		{name: "root", in: "/", want: ""},
		{name: "ok_path", in: "/findings", want: "/findings"},
		{name: "ok_path_query", in: "/findings?foo=bar", want: "/findings?foo=bar"},
		{name: "ok_root_query", in: "/?foo=bar", want: "/?foo=bar"},
		{name: "absolute_url", in: "https://evil.example/", want: ""},
		{name: "protocol_relative", in: "//evil.example/", want: ""},
		{name: "triple_slash", in: "///evil.example/", want: ""},
		{name: "backslash", in: "/\\evil.example/", want: ""},
		{name: "encoded_slash", in: "/%2f%2fevil.example/", want: ""},
		{name: "encoded_backslash", in: "/%5cevil.example/", want: ""},
		{name: "login_path", in: "/login", want: ""},
		{name: "login_subpath", in: "/login/reset", want: ""},
		{name: "newline", in: "/\n/evil", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := SanitizeNext(tt.in); got != tt.want {
				t.Fatalf("SanitizeNext(%q)=%q; want %q", tt.in, got, tt.want)
			}
		})
	}
}
