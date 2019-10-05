package cmd

import (
	"encoding/json"
	"github.com/breathbath/dumper/config"
	"github.com/breathbath/dumper/exec"
	"github.com/breathbath/go_utils/utils/io"
	"github.com/spf13/cobra"
)

var connNamesToImport *[]string

func init() {
	connNamesToImport = importDumpsCmd.Flags().StringSlice("conns", []string{}, "list of conn names like db1,db2")
	rootCmd.AddCommand(importDumpsCmd)
}

var importDumpsCmd = &cobra.Command{
	Use:   "import_dumps",
	Short: "Import dumps",
	RunE: func(cmd *cobra.Command, args []string) error {
		io.OutputInfo("", "Db conn names will be used to import: %v", *connNamesToImport)
		cmd.SilenceUsage = true
		cmd.SilenceErrors = true
		io.OutputInfo("", "Starting importing dumps")

		confs, err := config.ParseConfig()
		if err != nil {
			return err
		}

		importer := exec.MysqlImportExecutor{}
		var importConf exec.ImportConfig
		var lastErr error
		for _, conf := range confs {
			if conf.Kind != "import_dumps" {
				continue
			}

			err := json.Unmarshal([]byte(*conf.Context), &importConf)
			if err != nil {
				lastErr = err
				io.OutputError(err, "", "Failed to parse config: %v", err)
				continue
			}

			err = importer.Execute(conf, importConf, *connNamesToImport)
			if err != nil {
				lastErr = err
				io.OutputError(err, "", "Failed to execute importer: %v", err)
				continue
			}
		}

		if lastErr == nil {
			io.OutputInfo("", "Success")
		}

		return lastErr
	},
}
