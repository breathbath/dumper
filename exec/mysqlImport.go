package exec

import (
	"fmt"
	"github.com/breathbath/dumper/cli"
	"github.com/breathbath/dumper/config"
	"github.com/breathbath/dumper/db"
	errs2 "github.com/breathbath/go_utils/utils/errs"
	"github.com/breathbath/go_utils/utils/fs"
	"github.com/breathbath/go_utils/utils/io"
	validation "github.com/go-ozzo/ozzo-validation"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

type ImportConfig struct {
	Conns           map[string]db.DbConn `json:"dbConn"`
	DumpsFolderName string               `json:"dumpsFolderName"`
	IsGzipped       bool                 `json:"isGzipped,omitempty"`
	TempFolderPath  string               `json:"tempFolderPath,omitempty"`
}

func (ic ImportConfig) Validate() error {
	fields := []*validation.FieldRules{
		validation.Field(&ic.Conns, validation.Length(1, 10)),
		validation.Field(&ic.DumpsFolderName, validation.Required),
	}

	return validation.ValidateStruct(&ic, fields...)
}

type MysqlImportExecutor struct {
}

func (mie MysqlImportExecutor) Execute(generalConfig config.Config, conf ImportConfig, connNamesToImport []string) error {
	files, err := fs.ReadFilesInDirectory(conf.DumpsFolderName)
	if err != nil {
		return fmt.Errorf("Dump dir read failure %v", err)
	}

	var latestFile os.FileInfo
	lastFileTimestamp := time.Time{}
	for _, file := range files {
		regx := regexp.MustCompile(`^\d{2}\.\d{2}\.\d{4}\.\d{2}\.\d{2}\.\d{2}\.\d{3}`)
		timestampStr := regx.FindString(file.Name())
		if timestampStr == "" {
			continue
		}
		fileTime, err := time.Parse("02.01.2006.15.04.05.000", timestampStr)
		if err != nil {
			io.OutputWarning("", "Cannot parse %s as time str", timestampStr)
			continue
		}

		if fileTime.After(lastFileTimestamp) {
			lastFileTimestamp = fileTime
			latestFile = file
		}
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
		destFilePathGzipped := filepath.Join(conf.TempFolderPath, latestFile.Name())
		sqlFilePath = filepath.Join(conf.TempFolderPath, latestFile.Name()+".sql")

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
	}

	ers := errs2.NewErrorContainer()
	for connName, dbConnConf := range conf.Conns {
		if len(connNamesToImport) > 0 && !mie.connNameInList(connName, connNamesToImport) {
			continue
		}
		dbConnConf = db.PrepareDbConnConfig(dbConnConf)

		err := db.ExecMysql(dbConnConf, `DROP DATABASE IF EXISTS `+dbConnConf.DbName, false)
		if err != nil {
			ers.AddError(err)
			continue
		}

		err = db.ExecMysql(dbConnConf, "CREATE DATABASE " + dbConnConf.DbName, false)
		if err != nil {
			ers.AddError(err)
			continue
		}

		err = db.ImportDumpFromFileToDb(dbConnConf, sqlFilePath)
		if err != nil {
			ers.AddError(err)
			continue
		}
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
