package exec

import (
	"encoding/json"
	"fmt"
	"github.com/breathbath/dumper/cli"
	"github.com/breathbath/dumper/config"
	"github.com/breathbath/dumper/db"
	"github.com/breathbath/go_utils/utils/env"
	"github.com/breathbath/go_utils/utils/errs"
	"github.com/breathbath/go_utils/utils/fs"
	"github.com/breathbath/go_utils/utils/io"
	validation "github.com/go-ozzo/ozzo-validation"
	"path/filepath"
	"time"
)

type MysqlConfig struct {
	SourceDb         db.DbConn   `json:"sourceDb"`
	TargetDb         db.DbConn   `json:"targetDb,omitempty"`
	MysqlDumpVersion string   `json:"mysqlDumpVersion"`
	BeforeDump       []string `json:"beforeDump,omitempty"`
	OutputPath       string   `json:"outputPath"`
	Dumps            []db.Dump   `json:"dumps,omitempty"`
	IsGzipped        bool     `json:"isGzipped,omitempty"`
	CleanTargetDb    bool     `json:"cleanTargetDb,omitempty"`
	TmpPath          string   `json:"tmpPath"`
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
		filePath, err := mde.exportDumpsToFile(
			dbConfig.MysqlDumpVersion,
			dbConfig.Dumps,
			dbConfig.SourceDb,
			dbConfig.TmpPath,
			dbConfig.IsGzipped,
		)
		if err != nil {
			return err
		}

		defer func() {
			io.OutputInfo("", "Will remove '%s'", filePath)
			fs.RmFile(filePath)
		}()

		err = db.ImportDumpFromFileToDb(dbConfig.TargetDb, filePath)
		if err != nil {
			return err
		}

		err = db.SanitizeTargetDb(dbConfig.TargetDb, dbConfig.BeforeDump)
		if err != nil {
			return err
		}

		_, err = mde.exportDumpsToFile(
			dbConfig.MysqlDumpVersion,
			[]db.Dump{},
			dbConfig.TargetDb,
			dbConfig.OutputPath,
			dbConfig.IsGzipped,
		)
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

	_, err = mde.exportDumpsToFile(dbConfig.MysqlDumpVersion, dbConfig.Dumps, dbConfig.SourceDb, dbConfig.OutputPath, dbConfig.IsGzipped)

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

func (mde MysqlDumpExecutor) generateDumpFilesPath(dbName, outputPath string) string {
	nowSuffix := time.Now().UTC().Format("02.01.2006.15.04.05.000")
	fileName := fmt.Sprintf("%s_%s.sql", nowSuffix, dbName)
	return fs.JoinPath(outputPath, fileName)
}

func (mde MysqlDumpExecutor) exportDumpsToFile(mysqldumpVersion string, dumps []db.Dump, dbConn db.DbConn, outputPath string, isGzipped bool) (filePath string, err error) {
	io.OutputInfo("", "Will execute dump scripts for db '%s'", dbConn.DbName)
	filePath = mde.generateDumpFilesPath(dbConn.DbName, outputPath)
	if len(dumps) < 2 {
		pipedOutput := ""
		dump := db.Dump{}
		if isGzipped {
			filePath += ".gz"
			pipedOutput = fmt.Sprintf("| gzip -9 > %s", filePath)
		}
		if len(dumps) == 1 {
			dump = dumps[0]
		}

		err = db.ExecMysqlDump(dbConn, pipedOutput, mysqldumpVersion, dump)
		if err == nil {
			io.OutputInfo("", "Dumped db '%s' to %s", dbConn.DbName, filePath)
		}

		return
	}

	ers := errs.NewErrorContainer()
	for _, dump := range dumps {
		pipedOutput := fmt.Sprintf(">> %s", filePath)
		err := db.ExecMysqlDump(dbConn, pipedOutput, mysqldumpVersion, dump)
		ers.AddError(err)
	}
	err = ers.Result("")

	if err == nil {
		io.OutputInfo("", "Dumped db '%s' to %s", dbConn.DbName, filePath)
	}

	return
}
