package cli

import (
	"fmt"
	"github.com/breathbath/go_utils/utils/env"
	io2 "github.com/breathbath/go_utils/utils/io"
	"io"
	"os"
	exec2 "os/exec"
	"regexp"
	"strings"
)

func EscapeQuotes(inpt string) string {
	inpt = strings.Replace(inpt, `"`, `\\"`, -1)
	inpt = strings.Replace(inpt, "`", "\\`", -1)

	return inpt
}

func GetEnvOrValue(rawValue string) string {
	rgx := regexp.MustCompile(`\${(\w+)}`)
	items := rgx.FindAllStringSubmatch(rawValue, -1)
	for _, vars := range items {
		envValue := env.ReadEnv(vars[1], "")
		rawValue = strings.Replace(rawValue, vars[0], envValue, -1)
	}

	return rawValue
}

type CmdExec struct {
	SuccessWriter io.Writer
	ErrorWriter   io.Writer
	Envs          []string
}

func (e CmdExec) Execute(format string, args ...interface{}) error {
	command := fmt.Sprintf(format, args...)

	cmd := exec2.Command("/bin/bash", "-c", command)
	cmd.Stdout = e.SuccessWriter
	cmd.Stderr = e.ErrorWriter
	cmd.Env = os.Environ()
	if len(e.Envs) > 0 {
		cmd.Env = append(cmd.Env, e.Envs...)
	}

	io2.OutputInfo("", "Will run %s", command)

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("Command failed \"%s\", %v", command, err)
	}

	return nil
}
