package datadog

import (
	"errors"
	"net/url"
	"strings"
)

const defaultSite = "datadoghq.com"

// Config holds the configuration for the Datadog connector.
type Config struct {
	APIKey string `json:"api_key"`
	AppKey string `json:"app_key"`
	Site   string `json:"site"`
}

// Normalized returns a copy of the config with trimmed whitespace and defaults applied.
func (c Config) Normalized() Config {
	out := c
	out.APIKey = strings.TrimSpace(out.APIKey)
	out.AppKey = strings.TrimSpace(out.AppKey)
	out.Site = normalizeSite(out.Site)
	if out.Site == "" {
		out.Site = defaultSite
	}
	return out
}

// APIBaseURL returns the Datadog API base URL for the configured site.
func (c Config) APIBaseURL() string {
	site := normalizeSite(c.Site)
	if site == "" {
		site = defaultSite
	}
	return "https://api." + site
}

// Validate returns an error if the config is invalid.
func (c Config) Validate() error {
	c = c.Normalized()
	if c.APIKey == "" {
		return errors.New("Datadog API key is required")
	}
	if c.AppKey == "" {
		return errors.New("Datadog app key is required")
	}
	if c.Site == "" {
		return errors.New("Datadog site is required")
	}
	return nil
}

func normalizeSite(raw string) string {
	site := strings.TrimSpace(raw)
	if site == "" {
		return ""
	}
	if strings.Contains(site, "://") {
		if u, err := url.Parse(site); err == nil && u.Host != "" {
			site = u.Host
		}
	}
	site = strings.Trim(site, "/")
	site = strings.TrimPrefix(site, "api.")
	return site
}
