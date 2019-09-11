package exec

import (
	"fmt"
	io2 "github.com/breathbath/go_utils/utils/io"
	"io"
	exec2 "os/exec"
)

type CmdExec struct {
	successWriter io.Writer
	errorWriter io.Writer
}

func (e CmdExec) Execute(name, format string, args ...interface{}) error {
	command := fmt.Sprintf(format, args...)

	if name == "" {
		name = command
	}

	cmd := exec2.Command("/bin/sh", "-c", command)
	cmd.Stdout = e.successWriter
	cmd.Stderr = e.errorWriter

	io2.OutputInfo("", "Will run %s", name)

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("Command failed \"%s\", %v", name, err)
	}

	return nil
}
