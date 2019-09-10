package exec

import (
	"fmt"
	"github.com/breathbath/dumper/config"
	"github.com/breathbath/go_utils/utils/io"
)

type Router struct {
	Executors map[string] Executor
	GeneralConfig config.Config
}

func (r Router) Run() {
	e, ok := r.Executors[r.GeneralConfig.Kind]
	if !ok {
		io.OutputError(fmt.Errorf("No executor registered for '%s'", r.GeneralConfig.Kind), "", "")
		return
	}

	execConfig, err := e.GetValidConfig(r.GeneralConfig)
	if err != nil {
		io.OutputError(err, "", "")
		return
	}

	err = e.Execute(r.GeneralConfig, execConfig)
	if err != nil {
		io.OutputError(err, "", "")
		return
	}
}
