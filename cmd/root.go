package cmd

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "dumper",
	Short: "Dumper is a application to do backups for dbs and files to a remote cloud storage",
}

func Execute() error {
	if err := rootCmd.Execute(); err != nil {
		return err
	}

	return nil
}
