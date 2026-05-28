package main

import (
	"fmt"

	"github.com/open-sspm/open-sspm/internal/evaluator"
	"github.com/spf13/cobra"
)

var validatePolicyPacksCmd = &cobra.Command{
	Use:   "validate-policy-packs",
	Short: "Validate embedded evaluator policy packs without touching the DB.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		registry, err := evaluator.LoadBuiltin()
		if err != nil {
			return err
		}

		fmt.Printf("validated %d policy packs\n", registry.PackCount())
		return nil
	},
}
