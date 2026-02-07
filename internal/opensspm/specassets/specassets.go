package specassets

import (
	"encoding/json"
	"fmt"
	"time"

	_ "embed"
)

type Lock struct {
	UpstreamRepo            string    `json:"upstream_repo"`
	UpstreamRef             string    `json:"upstream_ref"`
	UpstreamCommit          string    `json:"upstream_commit,omitempty"`
	DescriptorHash          string    `json:"descriptor_hash"`
	DescriptorHashAlgorithm string    `json:"descriptor_hash_algorithm"`
	UpdatedAt               time.Time `json:"updated_at"`
}

var (
	//go:embed descriptor.v2.yaml
	descriptorV2YAML []byte

	//go:embed spec.lock.json
	lockJSON []byte
)

func DescriptorV2YAML() []byte {
	return descriptorV2YAML
}

func Lockfile() (Lock, error) {
	var l Lock
	if err := json.Unmarshal(lockJSON, &l); err != nil {
		return Lock{}, fmt.Errorf("parse Open SSPM spec lockfile: %w", err)
	}
	return l, nil
}
