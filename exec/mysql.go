package exec

import (
	"encoding/json"
	"fmt"
	"github.com/breathbath/dumper/config"
	"github.com/breathbath/go_utils/utils/env"
	"github.com/breathbath/go_utils/utils/errs"
	"github.com/breathbath/go_utils/utils/fs"
	"github.com/breathbath/go_utils/utils/io"
	validation "github.com/go-ozzo/ozzo-validation"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type Dump struct {
	IgnoreTables []string `json:"ignoreTables,omitempty"`
	Table        string   `json:"table,omitempty"`
	Where        string   `json:"where,omitempty"`
	Flags        []string `json:"flags,omitempty"`
}

type MysqlConfig struct {
	SourceDb         DbConn   `json:"sourceDb"`
	TargetDb         DbConn   `json:"targetDb,omitempty"`
	MysqlDumpVersion string   `json:"mysqlDumpVersion"`
	BeforeDump       []string `json:"beforeDump,omitempty"`
	OutputPath       string   `json:"outputPath"`
	Dumps            []Dump   `json:"dumps,omitempty"`
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

type DbConn struct {
	User     string `json:"user" validate:"required"`
	Password string `json:"password" validate:"required"`
	Host     string `json:"host"`
	Port     string `json:"port"`
	DbName   string `json:"db" validate:"required"`
}

func (dc DbConn) Validate() error {
	return validation.ValidateStruct(&dc,
		validation.Field(&dc.User, validation.Required),
		validation.Field(&dc.Password, validation.Required),
		validation.Field(&dc.DbName, validation.Required),
	)
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

		err = mde.importDumpFromFileToDb(dbConfig.TargetDb, filePath)
		if err != nil {
			return err
		}

		err = mde.sanitizeTargetDb(dbConfig.TargetDb, dbConfig.BeforeDump)
		if err != nil {
			return err
		}

		_, err = mde.exportDumpsToFile(
			dbConfig.MysqlDumpVersion,
			[]Dump{},
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

	cmdExec := CmdExec{
		successWriter: NewStdSuccessWriter(),
		errorWriter:   NewStdErrorWriter(),
		envs:          []string{},
	}

	return cmdExec.Execute(fmt.Sprintf("gzip -v -9 %s", filePath))
}

func (mde MysqlDumpExecutor) getEnvOrValue(rawValue string) string {
	rgx := regexp.MustCompile(`\${(\w+)}`)
	items := rgx.FindAllStringSubmatch(rawValue, -1)
	for _, vars := range items {
		envValue := env.ReadEnv(vars[1], "")
		rawValue = strings.Replace(rawValue, vars[0], envValue, -1)
	}

	return rawValue
}

func (mde MysqlDumpExecutor) prepareDbConnConfig(connCfg DbConn) DbConn {
	connCfg.DbName = mde.getEnvOrValue(connCfg.DbName)
	connCfg.Host = mde.getEnvOrValue(connCfg.Host)
	connCfg.Port = mde.getEnvOrValue(connCfg.Port)
	connCfg.User = mde.getEnvOrValue(connCfg.User)
	connCfg.Password = mde.getEnvOrValue(connCfg.Password)

	return connCfg
}

func (mde MysqlDumpExecutor) prepareDbConfig(cfg MysqlConfig) (MysqlConfig, error) {
	cfg.SourceDb = mde.prepareDbConnConfig(cfg.SourceDb)
	if cfg.TargetDb.DbName != "" {
		cfg.TargetDb = mde.prepareDbConnConfig(cfg.TargetDb)
	}

	cfg.OutputPath = mde.getEnvOrValue(cfg.OutputPath)

	var err error
	if !filepath.IsAbs(cfg.OutputPath) {
		cfg.OutputPath, err = filepath.Abs(cfg.OutputPath)
	}

	cfg.MysqlDumpVersion = mde.getEnvOrValue(cfg.MysqlDumpVersion)

	return cfg, err
}

func (mde MysqlDumpExecutor) cleanDb(cfg DbConn, scriptsPath string) error {
	io.OutputInfo("", "Will clean target db '%s'", cfg.DbName)

	cmdExec := CmdExec{
		successWriter: NewStdSuccessWriter(),
		errorWriter:   NewStdErrorWriter(),
		envs: []string{
			"MUSER=" + cfg.User,
			"MPASS=" + cfg.Password,
			"MPORT=" + cfg.Port,
			"MHOST=" + cfg.Host,
			"MDB=" + cfg.DbName,
		},
	}

	return cmdExec.Execute(filepath.Join(scriptsPath, "cleanDb.sh"))
}

func (mde MysqlDumpExecutor) execMysqlDump(cfg DbConn, pipeOutput, mysqldumpVersion string, dump Dump) error {
	envs := []string{
		"MUSER=" + cfg.User,
		"MPORT=" + cfg.Port,
		"MHOST=" + cfg.Host,
		"MDB=" + cfg.DbName,
		"MYSQL_PWD=" + cfg.Password,
	}

	statistics := ""
	if strings.HasPrefix(mysqldumpVersion, "8") {
		statistics = " --column-statistics=0"
	}

	where := ""
	if dump.Where != "" {
		where = fmt.Sprintf(` --where="%s"`, dump.Where)
	}

	flagsFlat := ""
	if len(dump.Flags) > 0 {
		flagsFlat = " " + strings.Join(dump.Flags, " ")
	}

	ignoreTablesFlat := ""
	ignoreTables := make([]string, 0, len(dump.IgnoreTables))
	for _, it := range dump.IgnoreTables {
		ignoreTables = append(ignoreTables, fmt.Sprintf("--ignore-table=%s.%s", cfg.DbName, it))
	}
	if len(ignoreTables) > 0 {
		ignoreTablesFlat = fmt.Sprintf(" %s", strings.Join(ignoreTables, " "))
	}

	lockTables := ""
	if where != "" {
		lockTables = " --lock-all-tables"
	}

	cmd := fmt.Sprintf(`set -o pipefail && mysqldump%s -u${MUSER} -P${MPORT} -h${MHOST}%s%s%s%s ${MDB} %s %s`,
		statistics,
		flagsFlat,
		where,
		ignoreTablesFlat,
		lockTables,
		dump.Table,
		pipeOutput,
	)

	cmdExec := CmdExec{
		successWriter: NewStdSuccessWriter(),
		errorWriter:   NewStdErrorWriter(),
		envs:          envs,
	}

	return cmdExec.Execute(cmd)
}

func (mde MysqlDumpExecutor) escapeQuotes(inpt string) string {
	//inpt = strings.Replace(inpt, "'", "\\'", -1)
	inpt = strings.Replace(inpt, `"`, `\\"`, -1)
	inpt = strings.Replace(inpt, "`", "\\`", -1)

	return inpt
}

func (mde MysqlDumpExecutor) cleanup(cfg MysqlConfig, scriptsPath string) error {
	io.OutputInfo("", "Will run cleanup")
	var err error

	if cfg.CleanTargetDb {
		err = mde.cleanDb(cfg.TargetDb, scriptsPath)
	}

	return err
}

func (mde MysqlDumpExecutor) generateDumpFilesPath(dbName, outputPath string) string {
	nowSuffix := time.Now().UTC().Format("02.01.2006.15.04.05.000")
	fileName := fmt.Sprintf("%s_%s.sql", nowSuffix, dbName)
	return fs.JoinPath(outputPath, fileName)
}

func (mde MysqlDumpExecutor) exportDumpsToFile(mysqldumpVersion string, dumps []Dump, dbConn DbConn, outputPath string, isGzipped bool) (filePath string, err error) {
	io.OutputInfo("", "Will execute dump scripts for db '%s'", dbConn.DbName)
	filePath = mde.generateDumpFilesPath(dbConn.DbName, outputPath)
	if len(dumps) < 2 {
		pipedOutput := ""
		dump := Dump{}
		if isGzipped {
			filePath += ".gz"
			pipedOutput = fmt.Sprintf("| gzip -9 >> %s", filePath)
		}
		if len(dumps) == 1 {
			dump = dumps[0]
		}

		err = mde.execMysqlDump(dbConn, pipedOutput, mysqldumpVersion, dump)
		if err == nil {
			io.OutputInfo("", "Dumped db '%s' to %s", dbConn.DbName, filePath)
		}

		return
	}

	ers := errs.NewErrorContainer()
	for _, dump := range dumps {
		pipedOutput := fmt.Sprintf(">> %s", filePath)
		err := mde.execMysqlDump(dbConn, pipedOutput, mysqldumpVersion, dump)
		ers.AddError(err)
	}
	err = ers.Result("")

	if err == nil {
		io.OutputInfo("", "Dumped db '%s' to %s", dbConn.DbName, filePath)
	}

	return
}

func (mde MysqlDumpExecutor) importDumpFromFileToDb(dbConn DbConn, filePath string) (err error) {
	io.OutputInfo("", "Will import '%s' to db '%s'", dbConn.DbName, filePath)

	cmd := fmt.Sprintf(
		`set -o pipefail && cat %s | mysql -u${DB_USER} -p${DB_PASS} -P${DB_PORT} -h${DB_HOST} ${DB_NAME}`,
		filePath,
	)
	cmdExec := CmdExec{
		successWriter: NewStdSuccessWriter(),
		errorWriter:   NewStdErrorWriter(),
		envs: []string{
			"DB_USER=" + dbConn.User,
			"DB_PASS=" + dbConn.Password,
			"DB_PORT=" + dbConn.Port,
			"DB_HOST=" + dbConn.Host,
			"DB_NAME=" + dbConn.DbName,
		},
	}

	return cmdExec.Execute(cmd)
}

func (mde MysqlDumpExecutor) sanitizeTargetDb(dbConn DbConn, scriptsToRun []string) error {
	if dbConn.DbName == "" || len(scriptsToRun) == 0 {
		return nil
	}

	io.OutputInfo("", "Will sanitize db '%s'", dbConn.DbName)

	ers := errs.NewErrorContainer()
	for _, q := range scriptsToRun {
		q = mde.escapeQuotes(q)
		cmd := fmt.Sprintf(`echo "%s" | mysql -u${MUSER} -P${MPORT} -h${MHOST} ${MDB}`, q)
		cmdExec := CmdExec{
			successWriter: NewStdSuccessWriter(),
			errorWriter:   NewStdErrorWriter(),
			envs: []string{
				"MUSER=" + dbConn.User,
				"MPORT=" + dbConn.Port,
				"MHOST=" + dbConn.Host,
				"MDB=" + dbConn.DbName,
				"MYSQL_PWD=" + dbConn.Password,
			},
		}

		err := cmdExec.Execute(cmd)
		if err != nil {
			ers.AddError(err)
			break
		}
	}

	return ers.Result("")
}
