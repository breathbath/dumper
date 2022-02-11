package exec

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/breathbath/dumper/cli"
	"github.com/breathbath/dumper/config"
	"github.com/breathbath/dumper/db"
	"github.com/breathbath/go_utils/utils/env"
	"github.com/breathbath/go_utils/utils/errs"
	"github.com/breathbath/go_utils/utils/fs"
	"github.com/breathbath/go_utils/utils/io"
	validation "github.com/go-ozzo/ozzo-validation"
	"os"
	"path/filepath"
	"time"
)

type Clean func()

type MysqlConfig struct {
	SourceDb         db.DbConn `json:"sourceDb"`
	TargetDb         db.DbConn `json:"targetDb,omitempty"`
	MysqlDumpVersion string    `json:"mysqlDumpVersion"`
	BeforeDump       []string  `json:"beforeDump,omitempty"`
	OutputPath       string    `json:"outputPath"`
	Dumps            []db.Dump `json:"dumps,omitempty"`
	IsGzipped        bool      `json:"isGzipped,omitempty"`
	CleanTargetDb    bool      `json:"cleanTargetDb,omitempty"`
	TmpPath          string    `json:"tmpPath"`
}

func (mc MysqlConfig) Validate() error {
	fields := []*validation.FieldRules{
		validation.Field(&mc.SourceDb, validation.Required),
		validation.Field(&mc.SourceDb),
		validation.Field(&mc.OutputPath, validation.Required),
	}
	if mc.TargetDb.DbName != "" {
		fields = append(fields, validation.Field(&mc.TargetDb))
	}

	return validation.ValidateStruct(&mc, fields...)
}

type MysqlDumpExecutor struct {
}

func (mde MysqlDumpExecutor) GetValidConfig(generalConfig config.Config) (interface{}, error) {
	var dbConf MysqlConfig
	err := json.Unmarshal([]byte(*generalConfig.Context), &dbConf)
	if err != nil {
		return MysqlConfig{}, fmt.Errorf("Config parsing failed: %v", err)
	}

	err = dbConf.Validate()

	return dbConf, err
}

func (mde MysqlDumpExecutor) Execute(generalConfig config.Config, execConfig interface{}) error {
	var err error

	dbConfig, ok := execConfig.(MysqlConfig)
	if !ok {
		return fmt.Errorf("Wrong config format for mysql dumper")
	}

	err = mde.prepareOutputPath(dbConfig)
	if err != nil {
		return err
	}

	dbConfig, err = mde.prepareDbConfig(dbConfig)
	if err != nil {
		return err
	}

	scriptsPath, err := env.ReadEnvOrError("SCRIPTS_PATH")
	if err != nil {
		return err
	}

	defer func() {
		err := mde.cleanup(dbConfig, scriptsPath)
		if err != nil {
			io.OutputError(err, "", "")
		}
	}()

	if dbConfig.TargetDb.DbName != "" && len(dbConfig.BeforeDump) > 0 {
		var filePath string
		var err error
		var cl Clean
		if len(dbConfig.Dumps) > 1 {
			filePath, cl, err = mde.exportDumpsToFile(
				dbConfig,
				dbConfig.SourceDb,
			)
			if cl != nil {
				defer cl()
			}
		} else {
			dump := db.Dump{}
			if len(dbConfig.Dumps) > 0 {
				dump = dbConfig.Dumps[0]
			}

			filePath, err = mde.exportDumpToFile(
				dbConfig,
				dump,
				dbConfig.SourceDb,
			)
		}
		if err != nil {
			return err
		}

		err = db.ImportDumpFromFileToDb(dbConfig.TargetDb, filePath)
		if err != nil {
			return err
		}

		err = db.SanitizeTargetDb(dbConfig.TargetDb, dbConfig.BeforeDump)
		if err != nil {
			return err
		}

		var cl2 Clean
		if len(dbConfig.Dumps) > 1 {
			filePath, cl2, err = mde.exportDumpsToFile(
				dbConfig,
				dbConfig.TargetDb,
			)
			if cl2 != nil {
				defer cl2()
			}
		} else {
			dump := db.Dump{}
			if len(dbConfig.Dumps) > 0 {
				dump = dbConfig.Dumps[0]
			}
			filePath, err = mde.exportDumpToFile(
				dbConfig,
				dump,
				dbConfig.TargetDb,
			)
		}

		if err != nil {
			return err
		}

		return nil
	}

	if dbConfig.TargetDb.DbName != "" && len(dbConfig.BeforeDump) == 0 {
		io.OutputWarning("", "Target db value is ignored since before dump field is empty")
	}

	if dbConfig.TargetDb.DbName == "" && len(dbConfig.BeforeDump) > 0 {
		io.OutputWarning("", "Before dump field is ignored since target db value is empty. Dumper doesn't sanitize source db")
	}

	var cl Clean
	if len(dbConfig.Dumps) > 1 {
		_, cl, err = mde.exportDumpsToFile(
			dbConfig,
			dbConfig.SourceDb,
		)
		if cl != nil {
			defer cl()
		}
	} else {
		dump := db.Dump{}
		if len(dbConfig.Dumps) > 0 {
			dump = dbConfig.Dumps[0]
		}
		_, err = mde.exportDumpToFile(
			dbConfig,
			dump,
			dbConfig.SourceDb,
		)
	}

	if err != nil {
		return err
	}

	return nil
}

func (mde MysqlDumpExecutor) prepareOutputPath(cfg MysqlConfig) error {
	return fs.MkDir(cfg.OutputPath)
}

func (mde MysqlDumpExecutor) runGzip(filePath string) error {
	io.OutputInfo("", "Will gzip %s", filePath)

	cmdExec := cli.CmdExec{
		SuccessWriter: cli.NewStdSuccessWriter(),
		ErrorWriter:   cli.NewStdErrorWriter(),
		Envs:          []string{},
	}

	return cmdExec.Execute(fmt.Sprintf("gzip -v -9 %s", filePath))
}

func (mde MysqlDumpExecutor) prepareDbConfig(cfg MysqlConfig) (MysqlConfig, error) {
	cfg.SourceDb = db.PrepareDbConnConfig(cfg.SourceDb)
	if cfg.TargetDb.DbName != "" {
		cfg.TargetDb = db.PrepareDbConnConfig(cfg.TargetDb)
	}

	cfg.OutputPath = cli.GetEnvOrValue(cfg.OutputPath)

	var err error
	if !filepath.IsAbs(cfg.OutputPath) {
		cfg.OutputPath, err = filepath.Abs(cfg.OutputPath)
	}

	cfg.MysqlDumpVersion = cli.GetEnvOrValue(cfg.MysqlDumpVersion)

	return cfg, err
}

func (mde MysqlDumpExecutor) cleanDb(cfg db.DbConn, scriptsPath string) error {
	io.OutputInfo("", "Will clean target db '%s'", cfg.DbName)

	cmdExec := cli.CmdExec{
		SuccessWriter: cli.NewStdSuccessWriter(),
		ErrorWriter:   cli.NewStdErrorWriter(),
		Envs: []string{
			"MUSER=" + cfg.User,
			"MPASS=" + cfg.Password,
			"MPORT=" + cfg.Port,
			"MHOST=" + cfg.Host,
			"MDB=" + cfg.DbName,
		},
	}

	return cmdExec.Execute(filepath.Join(scriptsPath, "cleanDb.sh"))
}

func (mde MysqlDumpExecutor) cleanup(cfg MysqlConfig, scriptsPath string) error {
	var err error

	if cfg.CleanTargetDb {
		io.OutputInfo("", "Will run cleanup")
		err = mde.cleanDb(cfg.TargetDb, scriptsPath)
	}

	return err
}

func (mde MysqlDumpExecutor) generateDumpFilesPath(prefix, dbName, outputPath string) string {
	fileName := fmt.Sprintf("%s_%s.sql", prefix, dbName)
	return fs.JoinPath(outputPath, fileName)
}

func (mde MysqlDumpExecutor) generateFullPaths(tempDirPath, outputDirPath, dbName string) (tempFilePath, outputFilePath string) {
	prefix := time.Now().UTC().Format("02.01.2006.15.04.05.000")
	if tempDirPath == "" {
		tempDirPath = os.TempDir()
	}

	tempFilePath = mde.generateDumpFilesPath(prefix, dbName, tempDirPath)
	outputFilePath = mde.generateDumpFilesPath(prefix, dbName, outputDirPath)

	return
}

func (mde MysqlDumpExecutor) exportDumpsToFile(dbConf MysqlConfig, dbConn db.DbConn) (filePath string, cl Clean, err error) {
	if dbConf.OutputPath == "" {
		return "", nil, errors.New("output dir path should not be empty")
	}

	io.OutputInfo("", "Will execute dump scripts for db '%s'", dbConn.DbName)

	tempFilePath, outputFilePath := mde.generateFullPaths(dbConf.TmpPath, dbConf.OutputPath, dbConn.DbName)

	ers := errs.NewErrorContainer()
	for _, dump := range dbConf.Dumps {
		pipedOutput := fmt.Sprintf(">> %s", tempFilePath)
		err := db.ExecMysqlDump(dbConn, pipedOutput, dbConf.MysqlDumpVersion, dump)
		ers.AddError(err)
	}

	rmTempFilePath := func() {
		io.OutputInfo("", "Will remove '%s'", tempFilePath)
		fs.RmFile(tempFilePath)
	}

	err = ers.Result("")
	if err != nil {
		return tempFilePath, rmTempFilePath, err
	}

	io.OutputInfo("", "Dumped db '%s' to %s", dbConn.DbName, tempFilePath)
	if !dbConf.IsGzipped {
		err := os.Rename(tempFilePath, outputFilePath)
		if err != nil {
			return "", nil, err
		}

		return outputFilePath, nil, nil
	}

	outputFilePath += ".gz"
	cmd := fmt.Sprintf("cat %s | gzip -9 > %s", tempFilePath, outputFilePath)
	cmdExec := cli.CmdExec{
		SuccessWriter: cli.NewStdSuccessWriter(),
		ErrorWriter:   cli.NewStdErrorWriter(),
	}

	err = cmdExec.Execute(cmd)

	return outputFilePath, rmTempFilePath, err
}

func (mde MysqlDumpExecutor) exportDumpToFile(dbConf MysqlConfig, dump db.Dump, dbConn db.DbConn) (filePath string, err error) {
	if dbConf.OutputPath == "" {
		return "", errors.New("output dir path should not be empty")
	}

	io.OutputInfo("", "Will execute dump scripts for db '%s'", dbConn.DbName)

	tempFilePath, outputFilePath := mde.generateFullPaths(dbConf.TmpPath, dbConf.OutputPath, dbConf.SourceDb.DbName)

	pipedOutput := ""
	if dbConf.IsGzipped {
		tempFilePath += ".gz"
		outputFilePath += ".gz"
		pipedOutput = fmt.Sprintf("| gzip -9 > %s", tempFilePath)
	}

	err = db.ExecMysqlDump(dbConn, pipedOutput, dbConf.MysqlDumpVersion, dump)
	if err != nil {
		return "", err
	}

	io.OutputInfo("", "Dumped db '%s' to %s", dbConn.DbName, tempFilePath)

	err = os.Rename(tempFilePath, outputFilePath)
	if err != nil {
		return "", err
	}
	io.OutputInfo("", "Moved db dump %s to %s", dbConn.DbName, outputFilePath)

	return outputFilePath, nil
}
