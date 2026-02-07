package opensspm

import (
	"encoding/json"
	"fmt"
	"sync"

	specv2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/spec/v2"
	"github.com/open-sspm/open-sspm/internal/opensspm/specassets"
	"gopkg.in/yaml.v3"
)

var (
	descriptorV2Once sync.Once
	descriptorV2     specv2.Descriptor
	descriptorV2Err  error
)

func DescriptorV2() (specv2.Descriptor, error) {
	descriptorV2Once.Do(func() {
		var v any
		if err := yaml.Unmarshal(specassets.DescriptorV2YAML(), &v); err != nil {
			descriptorV2Err = fmt.Errorf("parse embedded Open SSPM descriptor.v2.yaml: %w", err)
			return
		}

		b, err := json.Marshal(v)
		if err != nil {
			descriptorV2Err = fmt.Errorf("marshal embedded Open SSPM descriptor.v2.yaml: %w", err)
			return
		}

		descriptorV2, descriptorV2Err = specv2.ParseDescriptor(b)
		if descriptorV2Err != nil {
			descriptorV2Err = fmt.Errorf("decode embedded Open SSPM descriptor.v2.yaml: %w", descriptorV2Err)
		}
	})

	return descriptorV2, descriptorV2Err
}
