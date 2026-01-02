package opensspm

import (
	"fmt"
	"sync"

	specv1 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/spec/v1"
	"github.com/open-sspm/open-sspm/internal/opensspm/specassets"
)

var (
	descriptorV1Once sync.Once
	descriptorV1     specv1.DescriptorV1
	descriptorV1Err  error
)

func DescriptorV1() (specv1.DescriptorV1, error) {
	descriptorV1Once.Do(func() {
		descriptorV1, descriptorV1Err = specv1.ParseDescriptorV1(specassets.DescriptorV1JSON())
		if descriptorV1Err != nil {
			descriptorV1Err = fmt.Errorf("parse embedded Open SSPM descriptor.v1.json: %w", descriptorV1Err)
		}
	})

	return descriptorV1, descriptorV1Err
}
