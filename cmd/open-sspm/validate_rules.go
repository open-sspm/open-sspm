package main

import (
	"fmt"
	"github.com/spf13/cobra"
)

var validateRulesCmd = &cobra.Command{
	Use:   "validate-rules",
	Short: "Validate embedded + runtime rulesets without touching the DB.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		loaded, err := loadRulesFromOpenSSPMDescriptor()
		if err != nil {
			return err
		}

		var ruleCount int
		for _, ls := range loaded {
			ruleCount += len(ls.Rules)
		}

		fmt.Printf("validated %d rulesets (%d rules)\n", len(loaded), ruleCount)
		return nil
	},
}
