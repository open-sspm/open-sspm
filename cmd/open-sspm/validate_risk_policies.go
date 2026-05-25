package main

import (
	"fmt"

	"github.com/open-sspm/open-sspm/internal/riskpolicy"
	"github.com/spf13/cobra"
)

var validateRiskPoliciesCmd = &cobra.Command{
	Use:   "validate-risk-policies",
	Short: "Validate embedded risk policy packs without touching the DB.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		registry, err := riskpolicy.LoadBuiltin()
		if err != nil {
			return err
		}

		fmt.Printf("validated %d risk policy packs (%d Rego policies)\n", registry.PackCount(), registry.RegoPolicyCount())
		return nil
	},
}
