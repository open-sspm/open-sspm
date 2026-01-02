package okta

import (
	"errors"
	"strings"
)

// Config holds the configuration for the Okta connector.
type Config struct {
	Domain string `json:"domain"`
	Token  string `json:"token"`
}

// Normalized returns a copy of the config with trimmed whitespace.
func (c Config) Normalized() Config {
	out := c
	out.Domain = strings.TrimSpace(out.Domain)
	out.Token = strings.TrimSpace(out.Token)
	return out
}

// BaseURL returns the full API base URL from the domain.
func (c Config) BaseURL() string {
	base := strings.TrimSpace(c.Domain)
	if base == "" {
		return ""
	}
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "https://" + base
	}
	return strings.TrimRight(base, "/")
}

// Validate returns an error if the config is invalid.
func (c Config) Validate() error {
	c = c.Normalized()
	if c.Domain == "" {
		return errors.New("Okta domain is required")
	}
	if c.Token == "" {
		return errors.New("Okta token is required")
	}
	return nil
}

// Merge returns a new config by merging an update into an existing config.
// Token is only updated if the update value is non-empty.
func Merge(existing Config, update Config) Config {
	merged := existing
	merged.Domain = strings.TrimSpace(update.Domain)
	if token := strings.TrimSpace(update.Token); token != "" {
		merged.Token = token
	}
	return merged
}
