package main

import "github.com/spf13/cobra"

var workerTailCmd = &cobra.Command{
	Use:   "worker-tail",
	Short: "Run the background incremental tail sync loop.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runWorkerTail()
	},
}

func runWorkerTail() error {
	return runWorkerHost(tailSyncLane())
}
