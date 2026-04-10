package handlers

import (
	"net/http"
	"testing"
)

func TestNonHumanAccessRefererInfoAcceptsSameHostnameWithPortDifferences(t *testing.T) {
	tests := []struct {
		name        string
		requestHost string
		referer     string
	}{
		{
			name:        "request host includes port",
			requestHost: "example.com:443",
			referer:     "https://example.com/non-human-access?freshness_state=stale",
		},
		{
			name:        "referer includes port",
			requestHost: "example.com",
			referer:     "https://example.com:8443/non-human-access?freshness_state=stale",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := newTestContext(http.MethodGet, "http://example.com/non-human-access")
			c.Request().Host = tt.requestHost
			c.Request().Header.Set("Referer", tt.referer)

			info, ok := nonHumanAccessRefererInfo(c)
			if !ok {
				t.Fatalf("nonHumanAccessRefererInfo() ok = false, want true")
			}
			if info.path != "/non-human-access" {
				t.Fatalf("path = %q, want %q", info.path, "/non-human-access")
			}
		})
	}
}

func TestNonHumanAccessRefererInfoRejectsDifferentHostname(t *testing.T) {
	c, _ := newTestContext(http.MethodGet, "http://example.com/non-human-access")
	c.Request().Header.Set("Referer", "http://other.example.com/non-human-access")

	if _, ok := nonHumanAccessRefererInfo(c); ok {
		t.Fatalf("nonHumanAccessRefererInfo() ok = true, want false")
	}
}
