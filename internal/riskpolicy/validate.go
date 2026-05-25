package riskpolicy

import (
	"errors"
	"fmt"
	"strings"

	osspecv2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/spec/v2"
)

func validatePolicyPack(name string, pack PolicyPack) error {
	var errs []error
	if pack.Metadata.ID == "" {
		errs = append(errs, errors.New("metadata.id is required"))
	}
	if pack.Metadata.Version == "" {
		errs = append(errs, errors.New("metadata.version is required"))
	}
	if !validDomain(pack.Metadata.Domain) {
		errs = append(errs, fmt.Errorf("metadata.domain %q is not supported", pack.Metadata.Domain))
	}
	if strings.TrimSpace(pack.Inputs.Schema) == "" {
		errs = append(errs, errors.New("inputs.schema is required"))
	}
	if pack.Policy.Engine != osspecv2.CheckEngine_REGO {
		errs = append(errs, fmt.Errorf("policy.engine %q is not supported", pack.Policy.Engine))
	}
	if strings.TrimSpace(pack.Policy.Package) == "" {
		errs = append(errs, errors.New("policy.package is required"))
	}
	if strings.TrimSpace(pack.Policy.Query) == "" {
		errs = append(errs, errors.New("policy.query is required"))
	}
	if strings.TrimSpace(pack.Policy.Rego) == "" {
		errs = append(errs, errors.New("policy.rego is required"))
	}

	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("%s: validate policy: %w", name, err)
	}
	return nil
}

func validDomain(domain Domain) bool {
	switch domain {
	case DomainCredential, DomainSaaS, DomainIdentity:
		return true
	default:
		return false
	}
}

func validBusinessCriticality(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "low", "medium", "high", "critical":
		return true
	default:
		return false
	}
}

func validDataClassification(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "public", "internal", "confidential", "restricted":
		return true
	default:
		return false
	}
}
