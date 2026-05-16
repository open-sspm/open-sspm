package records

import (
	"errors"
	"strings"
)

type ResourcePayload interface {
	ResourceName() ResourceName
	SchemaVersion() int
	Validate() error
	ToEnvelope() ResourceEnvelope
}

type ProviderFields map[string]any

type ResourceEnvelope struct {
	Resource      ResourceName   `json:"resource"`
	SchemaVersion int            `json:"schema_version"`
	ExternalID    string         `json:"external_id"`
	DisplayName   string         `json:"display_name,omitempty"`
	Provider      ProviderFields `json:"provider"`
	Attributes    map[string]any `json:"attributes"`
	Raw           map[string]any `json:"raw,omitempty"`
}

type ResourceRef struct {
	Resource    ResourceName `json:"resource"`
	ExternalID  string       `json:"external_id"`
	DisplayName string       `json:"display_name,omitempty"`
}

type IdentityPayload struct {
	ExternalID    string
	Email         string
	DisplayName   string
	Status        string
	IsPrivileged  bool
	Groups        []ResourceRef
	ProviderAttrs map[string]any
	Raw           map[string]any
}

func (p IdentityPayload) ResourceName() ResourceName { return ResourceIdentity }
func (p IdentityPayload) SchemaVersion() int         { return 1 }

func (p IdentityPayload) Validate() error {
	if strings.TrimSpace(p.ExternalID) == "" {
		return errors.New("identity external_id is required")
	}
	return nil
}

func (p IdentityPayload) ToEnvelope() ResourceEnvelope {
	attrs := copyMap(p.ProviderAttrs)
	attrs["email"] = strings.TrimSpace(p.Email)
	attrs["status"] = strings.TrimSpace(p.Status)
	attrs["is_privileged"] = p.IsPrivileged
	if len(p.Groups) > 0 {
		attrs["groups"] = p.Groups
	}
	return ResourceEnvelope{
		Resource:      p.ResourceName(),
		SchemaVersion: p.SchemaVersion(),
		ExternalID:    strings.TrimSpace(p.ExternalID),
		DisplayName:   strings.TrimSpace(p.DisplayName),
		Provider:      ProviderFields{},
		Attributes:    attrs,
		Raw:           copyMap(p.Raw),
	}
}

func copyMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		if strings.TrimSpace(k) == "" {
			continue
		}
		out[k] = v
	}
	return out
}
