package aws

import (
	"context"
	"strings"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

type Definition struct{}

func (d *Definition) Kind() string {
	return configstore.KindAWSIdentityCenter
}

func (d *Definition) DisplayName() string {
	return "AWS Identity Center"
}

func (d *Definition) Role() registry.IntegrationRole {
	return registry.RoleApp
}

func (d *Definition) DecodeConfig(raw []byte) (any, error) {
	return registry.DecodeNormalizedConfig(raw, configstore.DecodeAWSIdentityCenterConfig, configstore.AWSIdentityCenterConfig.Normalized)
}

func (d *Definition) ValidateConfig(cfg any) error {
	return cfg.(configstore.AWSIdentityCenterConfig).Validate()
}

func (d *Definition) IsConfigured(cfg any) bool {
	c := cfg.(configstore.AWSIdentityCenterConfig)
	if c.Region == "" {
		return false
	}
	switch c.AuthType {
	case configstore.AWSIdentityCenterAuthTypeDefaultChain:
		return true
	case configstore.AWSIdentityCenterAuthTypeAccessKey:
		return c.AccessKeyID != "" && c.SecretAccessKey != ""
	default:
		return false
	}
}

func (d *Definition) SourceName(cfg any) string {
	c := cfg.(configstore.AWSIdentityCenterConfig)
	if c.Name != "" {
		return c.Name
	}
	return c.Region
}

func (d *Definition) MetricsProvider() registry.MetricsProvider {
	return registry.NewUserSourceMetricsProvider("aws")
}

func (d *Definition) NewIntegration(cfg any) (registry.Integration, error) {
	c := cfg.(configstore.AWSIdentityCenterConfig)
	// Note: NewIntegration is synchronous but aws.New requires context.
	// We can use context.Background() here as client creation usually doesn't block long
	// or we should change NewIntegration to take context.
	// aws.New calls LoadDefaultConfig which might make network calls (IMDS).
	// Using context.Background() is acceptable for now.
	client, err := New(context.Background(), Options{
		Region:          c.Region,
		InstanceArn:     c.InstanceARN,
		IdentityStoreID: c.IdentityStoreID,
		AuthType:        c.AuthType,
		AccessKeyID:     c.AccessKeyID,
		SecretAccessKey: c.SecretAccessKey,
		SessionToken:    c.SessionToken,
	})
	if err != nil {
		return nil, err
	}
	sourceName := c.Name
	if strings.TrimSpace(sourceName) == "" {
		sourceName = c.Region
	}
	return NewAWSIntegration(client, sourceName), nil
}
