package cmd

import (
	"github.com/breathbath/go_utils/utils/io"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "dumper",
	Short: "Dumper is a application to do backups for dbs and files to a remote cloud storage",
}

func Execute() error {
	io.OutputInfo("", "Version: %s", Version)

	initImportDumps()
	initVersion()
	initDumper()
	if err := rootCmd.Execute(); err != nil {
		return err
	}

	return nil
}
