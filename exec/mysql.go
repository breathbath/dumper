package exec

import (
	"encoding/json"
	"fmt"
	"github.com/breathbath/dumper/config"
	"github.com/breathbath/go_utils/utils/fs"
	"github.com/breathbath/go_utils/utils/io"
	"gopkg.in/validator.v2"
	"os/exec"
	"path/filepath"
	"time"
)

type Config struct {
	Host       string `json:"host"`
	Port       int    `json:"port"`
	DbUser     string `json:"user"`
	Pass       string `json:"pass"`
	DbName     string `json:"dbname",validate:"nonzero"`
	DumpBin    string `json:"mysqldumpBin",validate:"nonzero"`
	OutputPath string `json:"outputPath"`
}

type MysqlDumpExecutor struct {
}

func (mde MysqlDumpExecutor) GetValidConfig(generalConfig config.Config) (interface{}, error) {
	var dbConf Config
	err := json.Unmarshal([]byte(*generalConfig.Context), &dbConf)
	if err != nil {
		return Config{}, err
	}

	if dbConf.Host == "" {
		dbConf.Host = "localhost"
	}

	if dbConf.Port == 0 {
		dbConf.Port = 3306
	}

	if dbConf.DumpBin == "" {
		dbConf.DumpBin = "mysqldump"
	}

	if errs := validator.Validate(dbConf); errs != nil {
		return nil, errs
	}

	_, err = exec.LookPath(dbConf.DumpBin)
	if err != nil {
		return nil, err
	}

	return dbConf, err
}

func (mde MysqlDumpExecutor) Execute(generalConfig config.Config, execConfig interface{}) error {
	dbConfig, ok := execConfig.(Config)
	if !ok {
		return fmt.Errorf("Wrong config format for mysql dumper")
	}

	nowSuffix := time.Now().UTC().Format("02.01.2006.15.04.05.000")
	fileName := fmt.Sprintf("%s_%s.sql.gz", dbConfig.DbName, nowSuffix)
	if dbConfig.OutputPath != "" && !fs.FileExists(dbConfig.OutputPath) {
		err := fs.MkDir(dbConfig.OutputPath)
		if err != nil {
			return fmt.Errorf("Cannot create directory %s: %v", dbConfig.OutputPath, err)
		}
	}
	fullFileName := filepath.Join(dbConfig.OutputPath, fileName)

	io.OutputInfo("", "Making latest db dump of %s to %s", dbConfig.DbName, fullFileName)
	cmdExec := CmdExec{
		successWriter: NewStdSuccessWriter(),
		errorWriter:   NewStdErrorWriter(),
	}
	err := cmdExec.Execute(
		"%s -u%s -p%s -h%s -P%d %s | gzip -9 > %s",
		dbConfig.DumpBin,
		dbConfig.DbUser,
		dbConfig.Pass,
		dbConfig.Host,
		dbConfig.Port,
		dbConfig.DbName,
		fullFileName,
	)
	if err != nil {
		return err
	}

	return nil
}
