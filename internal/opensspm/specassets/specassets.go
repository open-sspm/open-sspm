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
	//go:embed descriptor.v1.json
	descriptorV1JSON []byte

	//go:embed requirements.json
	requirementsJSON []byte

	//go:embed spec.lock.json
	lockJSON []byte
)

func DescriptorV1JSON() []byte {
	return descriptorV1JSON
}

func RequirementsJSON() []byte {
	return requirementsJSON
}

func Lockfile() (Lock, error) {
	var l Lock
	if err := json.Unmarshal(lockJSON, &l); err != nil {
		return Lock{}, fmt.Errorf("parse Open SSPM spec lockfile: %w", err)
	}
	return l, nil
}
