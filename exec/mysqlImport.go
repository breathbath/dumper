package exec

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/breathbath/dumper/cli"
	"github.com/breathbath/dumper/db"
	errs2 "github.com/breathbath/go_utils/v3/pkg/errs"
	"github.com/breathbath/go_utils/v3/pkg/fs"
	"github.com/breathbath/go_utils/v3/pkg/io"
	validation "github.com/go-ozzo/ozzo-validation"
)

type ImportConfig struct {
	Conns           map[string]*db.ConnConfig `json:"dbConn"`
	DumpsFolderName string                    `json:"dumpsFolderName"`
	IsGzipped       bool                      `json:"isGzipped,omitempty"`
	TempFolderPath  string                    `json:"tempFolderPath,omitempty"`
}

func (ic ImportConfig) Validate() error {
	const maxConnsCount = 10
	const minConnsCount = 1
	fields := []*validation.FieldRules{
		validation.Field(&ic.Conns, validation.Length(minConnsCount, maxConnsCount)),
		validation.Field(&ic.DumpsFolderName, validation.Required),
	}

	return validation.ValidateStruct(&ic, fields...)
}

type MysqlImportExecutor struct {
}

func (mie MysqlImportExecutor) Execute(
	conf *ImportConfig,
	connNamesToImport []string,
) error {
	var latestFile os.FileInfo
	lastFileTimestamp := time.Time{}
	var fileTime time.Time

	err := filepath.Walk(conf.DumpsFolderName, func(path string, info os.FileInfo, err error) error {
		var e error
		regx := regexp.MustCompile(`^\d{2}\.\d{2}\.\d{4}\.\d{2}\.\d{2}\.\d{2}\.\d{3}`)
		timestampStr := regx.FindString(info.Name())
		if timestampStr == "" {
			return nil
		}
		fileTime, e = time.Parse("02.01.2006.15.04.05.000", timestampStr)
		if e != nil {
			io.OutputWarning("", "Cannot parse %q as time str: %v", timestampStr, e)
			return nil
		}

		if fileTime.After(lastFileTimestamp) {
			lastFileTimestamp = fileTime
			latestFile = info
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("dump dir read failure %v", err)
	}

	if latestFile == nil {
		io.OutputWarning("", "Didn't find any dump file")
		return nil
	}

	fullFilePath := filepath.Join(conf.DumpsFolderName, latestFile.Name())
	io.OutputInfo("", "Selected file '%s' to import", fullFilePath)
	sqlFilePath := fullFilePath
	if conf.IsGzipped {
		if conf.DumpsFolderName == "" {
			conf.DumpsFolderName = "/tmp"
		}
		err = mie.unarchiveFile(conf.TempFolderPath, latestFile.Name(), fullFilePath)
		if err != nil {
			return err
		}
	}

	ers := errs2.NewErrorContainer()
	for connName, dbConnConf := range conf.Conns {
		err = mie.importDump(connNamesToImport, connName, sqlFilePath, dbConnConf)
		if err != nil {
			ers.AddError(err)
		}
	}

	return ers.Result(" ")
}

func (mie MysqlImportExecutor) unarchiveFile(tempFolderPath, latestFileName, fullFilePath string) error {
	destFilePathGzipped := filepath.Join(tempFolderPath, latestFileName)
	sqlFilePath := filepath.Join(tempFolderPath, latestFileName+".sql")

	err := fs.CopyFile(fullFilePath, destFilePathGzipped)
	defer fs.RmFile(destFilePathGzipped)
	if err != nil {
		return err
	}

	cmdExec := cli.CmdExec{
		SuccessWriter: cli.NewStdSuccessWriter(),
		ErrorWriter:   cli.NewStdErrorWriter(),
	}
	err = cmdExec.Execute(`gzip -d -c %s > %s`, destFilePathGzipped, sqlFilePath)
	defer fs.RmFile(sqlFilePath)
	if err != nil {
		return err
	}

	io.OutputInfo("", "Extracted %s to %s", sqlFilePath, destFilePathGzipped)

	return nil
}

func (mie MysqlImportExecutor) importDump(connNamesToImport []string, connName, sqlFilePath string, dbConnConf *db.ConnConfig) error {
	if len(connNamesToImport) > 0 && !mie.connNameInList(connName, connNamesToImport) {
		return nil
	}
	db.PrepareDBConnConfig(dbConnConf)

	err := db.ExecMysql(dbConnConf, `DROP DATABASE IF EXISTS `+dbConnConf.DBName, false)
	if err != nil {
		return err
	}

	err = db.ExecMysql(dbConnConf, "CREATE DATABASE "+dbConnConf.DBName, false)
	if err != nil {
		return err
	}

	err = db.ImportDumpFromFileToDB(dbConnConf, sqlFilePath)
	if err != nil {
		return err
	}

	return nil
}

func (mie MysqlImportExecutor) connNameInList(connNameToFind string, conns []string) bool {
	for _, connNameFromList := range conns {
		if connNameFromList == connNameToFind {
			return true
		}
	}

	return false
}
