package main

import "github.com/spf13/cobra"

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Run the background full sync loop.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runWorker()
	},
}

func runWorker() error {
	return runWorkerHost(fullSyncLane())
}
