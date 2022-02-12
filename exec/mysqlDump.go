package exec

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/breathbath/dumper/cli"
	"github.com/breathbath/dumper/config"
	"github.com/breathbath/dumper/db"
	"github.com/breathbath/go_utils/utils/env"
	"github.com/breathbath/go_utils/utils/errs"
	"github.com/breathbath/go_utils/utils/fs"
	"github.com/breathbath/go_utils/utils/io"
	validation "github.com/go-ozzo/ozzo-validation"
)

type Clean func()

const (
	GzExt = ".gz"
)

type MysqlConfig struct {
	SourceDB         *db.ConnConfig `json:"sourceDb"`
	TargetDB         *db.ConnConfig `json:"targetDb,omitempty"`
	MysqlDumpVersion string         `json:"mysqlDumpVersion"`
	BeforeDump       []string       `json:"beforeDump,omitempty"`
	OutputPath       string         `json:"outputPath"`
	Dumps            []*db.Dump     `json:"dumps,omitempty"`
	IsGzipped        bool           `json:"isGzipped,omitempty"`
	CleanTargetDB    bool           `json:"cleanTargetDb,omitempty"`
	TmpPath          string         `json:"tmpPath"`
	Upload           *UploaderCfg   `json:"upload"`
}

func (mc *MysqlConfig) Validate() error {
	fields := []*validation.FieldRules{
		validation.Field(&mc.SourceDB, validation.Required),
		validation.Field(&mc.SourceDB),
		validation.Field(&mc.OutputPath, validation.Required),
	}
	if mc.TargetDB != nil && mc.TargetDB.DBName != "" {
		fields = append(fields, validation.Field(&mc.TargetDB))
	}

	return validation.ValidateStruct(mc, fields...)
}

type MysqlDumpExecutor struct {
	Uploaders map[string]Uploader
	UploadHelper
}

func (mde MysqlDumpExecutor) GetValidConfig(generalConfig *config.Config) (interface{}, error) {
	dbConf := new(MysqlConfig)
	err := json.Unmarshal(*generalConfig.Context, dbConf)
	if err != nil {
		return MysqlConfig{}, fmt.Errorf("config parsing failed: %v", err)
	}

	err = dbConf.Validate()
	if err != nil {
		return nil, err
	}

	err = mde.validateConfig(dbConf.Upload, mde.Uploaders)
	if err != nil {
		return nil, err
	}

	return dbConf, nil
}

func (mde MysqlDumpExecutor) Execute(generalConfig *config.Config, execConfig interface{}) error {
	var err error

	dbConfig, ok := execConfig.(*MysqlConfig)
	if !ok {
		return fmt.Errorf("wrong config format for mysql dumper")
	}

	err = mde.prepareOutputPath(dbConfig)
	if err != nil {
		return err
	}

	err = mde.prepareDBConfig(dbConfig)
	if err != nil {
		return err
	}

	scriptsPath, err := env.ReadEnvOrError("SCRIPTS_PATH")
	if err != nil {
		return err
	}

	defer func() {
		e := mde.cleanup(dbConfig, scriptsPath)
		if e != nil {
			io.OutputError(e, "", "")
		}
	}()

	if dbConfig.TargetDB != nil && dbConfig.TargetDB.DBName != "" && len(dbConfig.BeforeDump) > 0 {
		var targetFilePath string
		targetFilePath, err = mde.dumpPrepared(dbConfig)
		if err != nil {
			return err
		}

		err = mde.uploadIfNeeded(targetFilePath, dbConfig.Upload, mde.Uploaders)
		if err != nil {
			return err
		}

		return nil
	}

	mde.validateBeforeDumpConfig(dbConfig)

	targetFilePath, err := mde.dumpByConfig(dbConfig, dbConfig.SourceDB)
	if err != nil {
		return err
	}

	err = mde.uploadIfNeeded(targetFilePath, dbConfig.Upload, mde.Uploaders)
	if err != nil {
		return err
	}

	return nil
}

func (mde MysqlDumpExecutor) validateBeforeDumpConfig(dbConfig *MysqlConfig) {
	if dbConfig.TargetDB != nil && dbConfig.TargetDB.DBName != "" && len(dbConfig.BeforeDump) == 0 {
		io.OutputWarning("", "Target db value is ignored since before dump field is empty")
	}

	if dbConfig.TargetDB != nil && dbConfig.TargetDB.DBName == "" && len(dbConfig.BeforeDump) > 0 {
		io.OutputWarning("", "Before dump field is ignored since target db value is empty. Dumper doesn't sanitize source db")
	}
}

func (mde MysqlDumpExecutor) dumpPrepared(dbConfig *MysqlConfig) (targetFilePath string, err error) {
	filePath, err := mde.dumpByConfig(dbConfig, dbConfig.SourceDB)
	if err != nil {
		return "", err
	}

	err = db.ImportDumpFromFileToDB(dbConfig.TargetDB, filePath)
	if err != nil {
		return "", err
	}

	err = db.SanitizeTargetDB(dbConfig.TargetDB, dbConfig.BeforeDump)
	if err != nil {
		return "", err
	}

	targetFilePath, err = mde.dumpByConfig(dbConfig, dbConfig.TargetDB)

	if err != nil {
		return targetFilePath, err
	}

	return targetFilePath, nil
}

func (mde MysqlDumpExecutor) dumpByConfig(dbConfig *MysqlConfig, dbConn *db.ConnConfig) (dumpFilePath string, err error) {
	var cl Clean
	if len(dbConfig.Dumps) > 1 {
		dumpFilePath, cl, err = mde.exportDumpsToFile(
			dbConfig,
			dbConn,
		)
		if cl != nil {
			defer cl()
		}
		return dumpFilePath, err
	}

	var dump *db.Dump
	if len(dbConfig.Dumps) > 0 {
		dump = dbConfig.Dumps[0]
	}
	dumpFilePath, err = mde.exportDumpToFile(
		dbConfig,
		dump,
		dbConn,
	)

	return dumpFilePath, err
}

func (mde MysqlDumpExecutor) prepareOutputPath(cfg *MysqlConfig) error {
	return fs.MkDir(cfg.OutputPath)
}

func (mde MysqlDumpExecutor) prepareDBConfig(cfg *MysqlConfig) error {
	db.PrepareDBConnConfig(cfg.SourceDB)
	if cfg.TargetDB != nil && cfg.TargetDB.DBName != "" {
		db.PrepareDBConnConfig(cfg.TargetDB)
	}

	cfg.OutputPath = cli.GetEnvOrValue(cfg.OutputPath)

	var err error
	if !filepath.IsAbs(cfg.OutputPath) {
		cfg.OutputPath, err = filepath.Abs(cfg.OutputPath)
		if err != nil {
			return err
		}
	}

	cfg.MysqlDumpVersion = cli.GetEnvOrValue(cfg.MysqlDumpVersion)

	return nil
}

func (mde MysqlDumpExecutor) cleanDB(cfg *db.ConnConfig, scriptsPath string) error {
	io.OutputInfo("", "Will clean target db '%s'", cfg.DBName)

	cmdExec := cli.CmdExec{
		SuccessWriter: cli.NewStdSuccessWriter(),
		ErrorWriter:   cli.NewStdErrorWriter(),
		Envs: []string{
			"MUSER=" + cfg.User,
			"MPASS=" + cfg.Password,
			"MPORT=" + cfg.Port,
			"MHOST=" + cfg.Host,
			"MDB=" + cfg.DBName,
		},
	}

	return cmdExec.Execute(filepath.Join(scriptsPath, "cleanDB.sh"))
}

func (mde MysqlDumpExecutor) cleanup(cfg *MysqlConfig, scriptsPath string) error {
	var err error

	if cfg.CleanTargetDB {
		io.OutputInfo("", "Will run cleanup")
		err = mde.cleanDB(cfg.TargetDB, scriptsPath)
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

func (mde MysqlDumpExecutor) exportDumpsToFile(dbConf *MysqlConfig, dbConn *db.ConnConfig) (filePath string, cl Clean, err error) {
	if dbConf.OutputPath == "" {
		return "", nil, errors.New("output dir path should not be empty")
	}

	io.OutputInfo("", "Will execute dump scripts for db '%s'", dbConn.DBName)

	tempFilePath, outputFilePath := mde.generateFullPaths(dbConf.TmpPath, dbConf.OutputPath, dbConn.DBName)

	ers := errs.NewErrorContainer()
	for _, dump := range dbConf.Dumps {
		pipedOutput := fmt.Sprintf(">> %s", tempFilePath)
		err = db.ExecMysqlDump(dbConn, pipedOutput, dbConf.MysqlDumpVersion, dump)
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

	io.OutputInfo("", "Dumped db '%s' to %s", dbConn.DBName, tempFilePath)
	if !dbConf.IsGzipped {
		err = os.Rename(tempFilePath, outputFilePath)
		if err != nil {
			return "", nil, err
		}

		return outputFilePath, nil, nil
	}

	outputFilePath += GzExt
	cmd := fmt.Sprintf("cat %s | gzip -9 > %s", tempFilePath, outputFilePath)
	cmdExec := cli.CmdExec{
		SuccessWriter: cli.NewStdSuccessWriter(),
		ErrorWriter:   cli.NewStdErrorWriter(),
	}

	err = cmdExec.Execute(cmd)

	return outputFilePath, rmTempFilePath, err
}

func (mde MysqlDumpExecutor) exportDumpToFile(dbConf *MysqlConfig, dump *db.Dump, dbConn *db.ConnConfig) (filePath string, err error) {
	if dbConf.OutputPath == "" {
		return "", errors.New("output dir path should not be empty")
	}

	io.OutputInfo("", "Will execute dump scripts for db '%s'", dbConn.DBName)

	tempFilePath, outputFilePath := mde.generateFullPaths(dbConf.TmpPath, dbConf.OutputPath, dbConf.SourceDB.DBName)

	pipedOutput := ""
	if dbConf.IsGzipped {
		tempFilePath += GzExt
		outputFilePath += GzExt
		pipedOutput = fmt.Sprintf("| gzip -9 > %s", tempFilePath)
	}

	err = db.ExecMysqlDump(dbConn, pipedOutput, dbConf.MysqlDumpVersion, dump)
	if err != nil {
		return "", err
	}

	io.OutputInfo("", "Dumped db '%s' to %s", dbConn.DBName, tempFilePath)

	err = os.Rename(tempFilePath, outputFilePath)
	if err != nil {
		return "", err
	}
	io.OutputInfo("", "Moved db dump %s to %s", dbConn.DBName, outputFilePath)

	return outputFilePath, nil
}
