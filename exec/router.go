package exec

import (
	"fmt"

	"github.com/breathbath/dumper/config"
	"github.com/breathbath/go_utils/utils/io"
)

type Router struct {
	Executors     map[string]Executor
	GeneralConfig *config.Config
}

func (r Router) RunErr() error {
	e, ok := r.Executors[r.GeneralConfig.Kind]
	if !ok {
		return fmt.Errorf("no executor registered for '%s'", r.GeneralConfig.Kind)
	}

	execConfig, err := e.GetValidConfig(r.GeneralConfig)
	if err != nil {
		return err
	}

	err = e.Execute(r.GeneralConfig, execConfig)
	if err != nil {
		return err
	}

	return nil
}

func (r Router) Run() {
	err := r.RunErr()
	if err != nil {
		io.OutputError(err, "", "")
	}
}
