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

func (e CmdExec) Execute(format string, args ...interface{}) error {
	command := fmt.Sprintf(format, args...)

	cmd := exec2.Command("/bin/sh", "-c", command)
	cmd.Stdout = e.successWriter
	cmd.Stderr = e.errorWriter

	io2.OutputInfo("", "Will run %s -c %s", "/bin/sh", command)

	err := cmd.Run()
	if err != nil {
		errMsg := fmt.Sprintf("Command failed \"%s\", %v", command, err)
		_, err = e.errorWriter.Write([]byte(errMsg))
		if err != nil {
			return err
		}
		return nil
	}

	return nil
}
