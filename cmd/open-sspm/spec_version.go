package main

import (
	"encoding/json"
	"fmt"

	"github.com/open-sspm/open-sspm/internal/opensspm/specassets"
	"github.com/spf13/cobra"
)

var specVersionJSON bool

var specVersionCmd = &cobra.Command{
	Use:   "spec-version",
	Short: "Print the pinned Open SSPM spec version + descriptor hash.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		lock, err := specassets.Lockfile()
		if err != nil {
			return err
		}

		if specVersionJSON {
			b, err := json.MarshalIndent(lock, "", "  ")
			if err != nil {
				return err
			}
			fmt.Println(string(b))
			return nil
		}

		fmt.Printf(
			"open-sspm-spec repo=%s ref=%s commit=%s descriptor_hash=%s updated_at=%s\n",
			lock.UpstreamRepo,
			lock.UpstreamRef,
			lock.UpstreamCommit,
			lock.DescriptorHash,
			lock.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
		)
		return nil
	},
}

func init() {
	specVersionCmd.Flags().BoolVar(&specVersionJSON, "json", false, "Print spec lockfile as JSON")
}
