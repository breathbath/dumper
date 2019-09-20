package exec

import (
	"fmt"
	io2 "github.com/breathbath/go_utils/utils/io"
	"io"
	"os"
	exec2 "os/exec"
)

type CmdExec struct {
	successWriter io.Writer
	errorWriter   io.Writer
	envs          []string
}

func (e CmdExec) Execute(format string, args ...interface{}) error {
	command := fmt.Sprintf(format, args...)

	cmd := exec2.Command("/bin/sh", "-c", command)
	cmd.Stdout = e.successWriter
	cmd.Stderr = e.errorWriter
	cmd.Env = os.Environ()
	if len(e.envs) > 0 {
		cmd.Env = append(cmd.Env, e.envs...)
	}

	io2.OutputInfo("", "Will run %s", command)

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("Command failed \"%s\", %v", command, err)
	}

	return nil
}
