package exec

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/breathbath/dumper/cli"
	"github.com/breathbath/dumper/config"
	"github.com/breathbath/go_utils/v3/pkg/errs"
	"github.com/breathbath/go_utils/v3/pkg/fs"
	"github.com/breathbath/go_utils/v3/pkg/io"
	validation "github.com/go-ozzo/ozzo-validation"
)

type TarConfig struct {
	Paths      []string     `json:"paths"`
	OutputPath string       `json:"outputPath"`
	TarBin     string       `json:"gzipBin"`
	Upload     *UploaderCfg `json:"upload"`
}

func (tc *TarConfig) Validate() error {
	return validation.ValidateStruct(tc,
		validation.Field(&tc.Paths, validation.Required, validation.Length(1, -1)),
	)
}

type TarExecutor struct {
	Uploaders map[string]Uploader
	UploadHelper
}

func (te TarExecutor) GetValidConfig(generalConfig *config.Config) (interface{}, error) {
	gConfig := new(TarConfig)
	err := json.Unmarshal([]byte(*generalConfig.Context), &gConfig)
	if err != nil {
		return gConfig, err
	}

	if gConfig.TarBin == "" {
		gConfig.TarBin = "tar"
	}

	_, err = exec.LookPath(gConfig.TarBin)
	if err != nil {
		return nil, err
	}

	err = gConfig.Validate()
	if err != nil {
		return nil, err
	}

	err = te.validateConfig(gConfig.Upload, te.Uploaders)
	if err != nil {
		return nil, err
	}

	return gConfig, err
}

func (te TarExecutor) Execute(generalConfig *config.Config, execConfig interface{}) error {
	tarConfig, ok := execConfig.(*TarConfig)
	if !ok {
		return fmt.Errorf("wrong config format for gzip dumper")
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
		fileName := fmt.Sprintf("%s_%s.tar%s", lastFolderName, nowSuffix, GzExt)
		if tarConfig.OutputPath != "" && !fs.FileExists(tarConfig.OutputPath) {
			err = fs.MkDir(tarConfig.OutputPath)
			if err != nil {
				ers.AddError(fmt.Errorf("cannot create directory %s: %v", tarConfig.OutputPath, err))
				continue
			}
		}

		fullFileName := filepath.Join(tarConfig.OutputPath, fileName)

		io.OutputInfo("", "archiving from %s to %s", path, fullFileName)
		cgexec := cli.CmdExec{
			SuccessWriter: cli.NewStdSuccessWriter(),
			ErrorWriter:   cli.NewStdErrorWriter(),
		}

		err = cgexec.Execute(
			"%s -czf %s %s",
			tarConfig.TarBin,
			fullFileName,
			path,
		)

		if err != nil {
			ers.AddError(err)
			continue
		}

		io.OutputInfo("", "successfully archived %s to %s", path, fullFileName)

		err = te.uploadIfNeeded(fullFileName, tarConfig.Upload, te.Uploaders)
		if err != nil {
			ers.AddError(err)
			continue
		}
	}

	err = ers.Result(" ")
	if err != nil {
		return err
	}

	return nil
}
