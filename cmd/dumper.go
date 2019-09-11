package cmd

import (
	"github.com/breathbath/dumper/config"
	"github.com/breathbath/dumper/exec"
	"github.com/breathbath/go_utils/utils/env"
	"github.com/breathbath/go_utils/utils/errs"
	"github.com/breathbath/go_utils/utils/io"
	"github.com/robfig/cron"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(dumpCmd)
}

var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "Execute all dumps",
	RunE: func(cmd *cobra.Command, args []string) error {
		io.OutputInfo("", "Starting dump executor")

		confs, err := config.ParseConfig()
		if err != nil {
			return err
		}

		c := cron.New()

		ers := errs.NewErrorContainer()

		for _, conf := range confs {
			router := exec.Router{
				Executors: map[string]exec.Executor{
					"mysql": exec.MysqlDumpExecutor{},
					"tar": exec.TarExecutor{},
				},
				GeneralConfig: conf,
			}

			if env.ReadEnvBool("RUN_ON_STARTUP", false) {
				io.OutputInfo("", "Will run '%s'", conf.Name)
				err := router.RunErr()
				ers.AddError(err)
			}

			io.OutputInfo("", "Will schedule '%s' to run '%s'", conf.Name, conf.Period)

			err = c.AddJob(conf.Period, router)
			if err != nil {
				ers.AddError(err)
				continue
			}
		}

		err = ers.Result(" ")
		if err != nil {
			return err
		}

		c.Run()

		return nil
	},
}
