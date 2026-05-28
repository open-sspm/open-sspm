package evaluator

import osspecv2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/spec/v2"

const Kind = "opensspm.entity_policy_pack"

type Domain = osspecv2.EntityPolicyDomain

const (
	DomainCredential Domain = osspecv2.EntityPolicyDomain_CREDENTIAL
	DomainSaaS       Domain = osspecv2.EntityPolicyDomain_SAAS
	DomainIdentity   Domain = osspecv2.EntityPolicyDomain_IDENTITY
)

type PolicyPack = osspecv2.EntityPolicyPack
type PolicyMetadata = osspecv2.EntityPolicyMetadata
type Inputs = osspecv2.EntityPolicyInputs
type RegoPolicy = osspecv2.RegoPolicy
