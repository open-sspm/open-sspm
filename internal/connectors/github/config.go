package github

import (
	"errors"
	"strings"
)

const defaultAPIBase = "https://api.github.com"

// Config holds the configuration for the GitHub connector.
type Config struct {
	Token      string `json:"token"`
	Org        string `json:"org"`
	APIBase    string `json:"api_base"`
	Enterprise string `json:"enterprise"`
	// SCIMEnabled toggles whether to use the SCIM API for identity/email resolution.
	SCIMEnabled bool `json:"scim_enabled"`
}

// Normalized returns a copy of the config with trimmed whitespace and defaults applied.
func (c Config) Normalized() Config {
	out := c
	out.Token = strings.TrimSpace(out.Token)
	out.Org = strings.TrimSpace(out.Org)
	out.APIBase = strings.TrimSpace(out.APIBase)
	out.Enterprise = strings.TrimSpace(out.Enterprise)
	if out.APIBase == "" {
		out.APIBase = defaultAPIBase
	}
	out.APIBase = strings.TrimRight(out.APIBase, "/")
	return out
}

// Validate returns an error if the config is invalid.
func (c Config) Validate() error {
	c = c.Normalized()
	if c.Token == "" {
		return errors.New("GitHub token is required")
	}
	if c.Org == "" {
		return errors.New("GitHub org is required")
	}
	if c.APIBase == "" {
		return errors.New("GitHub API base is required")
	}
	return nil
}

// Merge returns a new config by merging an update into an existing config.
// Token is only updated if the update value is non-empty.
func Merge(existing Config, update Config) Config {
	merged := existing
	merged.Org = strings.TrimSpace(update.Org)
	merged.APIBase = strings.TrimSpace(update.APIBase)
	merged.Enterprise = strings.TrimSpace(update.Enterprise)
	merged.SCIMEnabled = update.SCIMEnabled
	if token := strings.TrimSpace(update.Token); token != "" {
		merged.Token = token
	}
	return merged
}
