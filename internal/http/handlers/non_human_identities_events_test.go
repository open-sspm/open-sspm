package handlers

import (
	"net/http"
	"testing"
)

func TestNonHumanIdentitiesRefererInfoAcceptsSameHostnameWithPortDifferences(t *testing.T) {
	tests := []struct {
		name        string
		requestHost string
		referer     string
	}{
		{
			name:        "request host includes port",
			requestHost: "example.com:443",
			referer:     "https://example.com/non-human-identities?freshness_state=stale",
		},
		{
			name:        "referer includes port",
			requestHost: "example.com",
			referer:     "https://example.com:8443/non-human-identities?freshness_state=stale",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := newTestContext(http.MethodGet, "http://example.com/non-human-identities")
			c.Request().Host = tt.requestHost
			c.Request().Header.Set("Referer", tt.referer)

			info, ok := nonHumanIdentitiesRefererInfo(c)
			if !ok {
				t.Fatalf("nonHumanIdentitiesRefererInfo() ok = false, want true")
			}
			if info.path != "/non-human-identities" {
				t.Fatalf("path = %q, want %q", info.path, "/non-human-identities")
			}
		})
	}
}

func TestNonHumanIdentitiesRefererInfoRejectsDifferentHostname(t *testing.T) {
	c, _ := newTestContext(http.MethodGet, "http://example.com/non-human-identities")
	c.Request().Header.Set("Referer", "http://other.example.com/non-human-identities")

	if _, ok := nonHumanIdentitiesRefererInfo(c); ok {
		t.Fatalf("nonHumanIdentitiesRefererInfo() ok = true, want false")
	}
}
