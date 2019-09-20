package exec

import (
	"encoding/json"
	"fmt"
	"github.com/breathbath/dumper/config"
	"github.com/breathbath/go_utils/utils/errs"
	"github.com/breathbath/go_utils/utils/fs"
	"github.com/breathbath/go_utils/utils/io"
	"gopkg.in/validator.v2"
	"os/exec"
	"path/filepath"
	"time"
)

type TarConfig struct {
	Paths      []string `json:"paths",validate:"min=1"`
	OutputPath string   `json:"outputPath"`
	TarBin     string   `json:"gzipBin"`
}

type TarExecutor struct {
}

func (te TarExecutor) GetValidConfig(generalConfig config.Config) (interface{}, error) {
	var gConfig TarConfig
	err := json.Unmarshal([]byte(*generalConfig.Context), &gConfig)
	if err != nil {
		return gConfig, err
	}

	if gConfig.TarBin == "" {
		gConfig.TarBin = "tar"
	}

	if ers := validator.Validate(gConfig); ers != nil {
		return nil, ers
	}

	_, err = exec.LookPath(gConfig.TarBin)
	if err != nil {
		return nil, err
	}

	return gConfig, err
}

func (te TarExecutor) Execute(generalConfig config.Config, execConfig interface{}) error {
	tarConfig, ok := execConfig.(TarConfig)
	if !ok {
		return fmt.Errorf("Wrong config format for gzip dumper")
	}

	var err error
	if !filepath.IsAbs(tarConfig.OutputPath) {
		tarConfig.OutputPath, err = filepath.Abs(tarConfig.OutputPath)
		if err != nil {
			return err
		}
	}

	nowSuffix := time.Now().UTC().Format("02.01.2006.15.04.05.000")

	ers := errs.NewErrorContainer()
	for _, path := range tarConfig.Paths {
		lastFolderName := filepath.Base(path)
		fileName := fmt.Sprintf("%s_%s.tar.gz", lastFolderName, nowSuffix)
		if tarConfig.OutputPath != "" && !fs.FileExists(tarConfig.OutputPath) {
			err := fs.MkDir(tarConfig.OutputPath)
			if err != nil {
				ers.AddError(fmt.Errorf("Cannot create directory %s: %v", tarConfig.OutputPath, err))
				continue
			}
		}

		fullFileName := filepath.Join(tarConfig.OutputPath, fileName)

		io.OutputInfo("", "Making latest %s dump of %s to %s", tarConfig.TarBin, path, fullFileName)
		cgexec := CmdExec{
			successWriter: NewStdSuccessWriter(),
			errorWriter:   NewStdErrorWriter(),
		}

		err := cgexec.Execute(
			"%s -czf %s %s",
			tarConfig.TarBin,
			fullFileName,
			path,
		)
		ers.AddError(err)
	}

	return ers.Result(" ")
}
