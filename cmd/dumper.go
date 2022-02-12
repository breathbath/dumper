package cmd

import (
	"github.com/breathbath/dumper/config"
	"github.com/breathbath/dumper/exec"
	"github.com/breathbath/dumper/yand"
	"github.com/breathbath/go_utils/utils/env"
	"github.com/breathbath/go_utils/utils/errs"
	"github.com/breathbath/go_utils/utils/io"
	"github.com/robfig/cron"
	"github.com/spf13/cobra"
)

func initDumper() {
	rootCmd.AddCommand(dumpCmd)
}

var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "Start cronjob to trigger dumps periodically",
	Long: "Start cronjob to trigger dumps periodically according to the config " +
		"defined in CONFIG_PATH env var, to trigger immediately do `RUN_ON_STARTUP=true ./dumper dump`",
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.SilenceUsage = true
		cmd.SilenceErrors = true
		io.OutputInfo("", "Starting dump executor")

		confs, err := config.ParseConfig()
		if err != nil {
			return err
		}

		c := cron.New()

		ers := errs.NewErrorContainer()

		for _, conf := range confs {
			if conf.Period == "" {
				io.OutputWarning("", "config '%s' has empty execution period, will skip it", conf.Name)
				continue
			}

			cfg := yand.NewConfigFromEnvs()
			yandexUploader := yand.NewService(cfg)

			uploaders := map[string]exec.Uploader{
				yand.YandexUploader: yandexUploader,
			}
			router := exec.Router{
				Executors: map[string]exec.Executor{
					"mysql": exec.MysqlDumpExecutor{
						Uploaders: uploaders,
					},
					"tar": exec.TarExecutor{
						Uploaders: uploaders,
					},
				},
				GeneralConfig: conf,
			}
			if env.ReadEnvBool("RUN_ON_STARTUP", false) {
				io.OutputInfo("", "Will run '%s'", conf.Name)
				err = router.RunErr()
				ers.AddError(err)
			}

			if err == nil {
				io.OutputInfo("", "Will schedule '%s' to run '%s'", conf.Name, conf.Period)
				err = c.AddJob(conf.Period, router)
				if err != nil {
					ers.AddError(err)
					continue
				}
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
