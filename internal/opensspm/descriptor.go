package opensspm

import (
	specv2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/spec/v2"
)

func DescriptorV2() (specv2.Descriptor, error) {
	return specv2.GeneratedDescriptor, nil
}
