package main

import "github.com/spf13/cobra"

var workerDiscoveryCmd = &cobra.Command{
	Use:   "worker-discovery",
	Short: "Run the background SaaS discovery sync loop.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runWorkerDiscovery()
	},
}

func runWorkerDiscovery() error {
	return runWorkerHost(discoverySyncLane())
}
