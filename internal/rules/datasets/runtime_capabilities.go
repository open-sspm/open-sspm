package datasets

import (
	"sort"

	runtimev2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/runtime/v2"
)

const normalizedDatasetVersion = 1

var (
	oktaCapabilitiesV2 = []string{
		"okta:apps",
		"okta:apps/admin-console-settings",
		"okta:authenticators",
		"okta:brands/signin-page",
		"okta:log-streams",
		"okta:policies/app-signin",
		"okta:policies/password",
		"okta:policies/sign-on",
	}

	normalizedCapabilities = []string{
		"normalized:entitlement_assignments",
		"normalized:identities",
	}
)

func RuntimeCapabilities(p RouterProvider) []runtimev2.DatasetRef {
	out := make([]runtimev2.DatasetRef, 0, len(oktaCapabilitiesV2)+len(normalizedCapabilities))

	if p.Okta != nil {
		for _, ds := range oktaCapabilitiesV2 {
			out = append(out, runtimev2.DatasetRef{Dataset: ds, Version: 1})
		}
	}
	if p.Normalized != nil {
		for _, ds := range normalizedCapabilities {
			out = append(out, runtimev2.DatasetRef{Dataset: ds, Version: normalizedDatasetVersion})
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Dataset != out[j].Dataset {
			return out[i].Dataset < out[j].Dataset
		}
		return out[i].Version < out[j].Version
	})
	return out
}
