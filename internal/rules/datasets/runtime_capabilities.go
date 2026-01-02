package datasets

import (
	"sort"

	runtimev1 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/runtime/v1"
)

var (
	oktaCapabilitiesV1 = []string{
		"okta:apps",
		"okta:apps/admin-console-settings",
		"okta:authenticators",
		"okta:brands/signin-page",
		"okta:log-streams",
		"okta:policies/app-signin",
		"okta:policies/password",
		"okta:policies/sign-on",
	}

	normalizedCapabilitiesV1 = []string{
		"normalized:entitlement_assignments",
		"normalized:identities",
	}
)

func RuntimeCapabilities(p RouterProvider) []runtimev1.DatasetRef {
	out := make([]runtimev1.DatasetRef, 0, len(oktaCapabilitiesV1)+len(normalizedCapabilitiesV1))

	if p.Okta != nil {
		for _, ds := range oktaCapabilitiesV1 {
			out = append(out, runtimev1.DatasetRef{Dataset: ds, Version: 1})
		}
	}
	if p.Normalized != nil {
		for _, ds := range normalizedCapabilitiesV1 {
			out = append(out, runtimev1.DatasetRef{Dataset: ds, Version: 1})
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
