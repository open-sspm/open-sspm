package aws

import (
	"errors"
	"strings"
)

// Config holds the configuration for the AWS Identity Center connector.
type Config struct {
	Region          string `json:"region"`
	Name            string `json:"name"`
	InstanceARN     string `json:"instance_arn"`
	IdentityStoreID string `json:"identity_store_id"`
}

// Normalized returns a copy of the config with trimmed whitespace.
func (c Config) Normalized() Config {
	out := c
	out.Region = strings.TrimSpace(out.Region)
	out.Name = strings.TrimSpace(out.Name)
	out.InstanceARN = strings.TrimSpace(out.InstanceARN)
	out.IdentityStoreID = strings.TrimSpace(out.IdentityStoreID)
	return out
}

// Validate returns an error if the config is invalid.
func (c Config) Validate() error {
	c = c.Normalized()
	if c.Region == "" {
		return errors.New("AWS Identity Center region is required")
	}
	return nil
}

// Merge returns a new config by merging an update into an existing config.
func Merge(existing Config, update Config) Config {
	merged := existing
	merged.Region = strings.TrimSpace(update.Region)
	merged.Name = strings.TrimSpace(update.Name)
	merged.InstanceARN = strings.TrimSpace(update.InstanceARN)
	merged.IdentityStoreID = strings.TrimSpace(update.IdentityStoreID)
	return merged
}
