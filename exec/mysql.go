package exec

import (
	"encoding/json"
	"fmt"
	"github.com/breathbath/dumper/config"
	"github.com/breathbath/go_utils/utils/env"
	"github.com/breathbath/go_utils/utils/errs"
	"github.com/breathbath/go_utils/utils/fs"
	"github.com/breathbath/go_utils/utils/io"
	"gopkg.in/validator.v2"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type Dump struct {
	IgnoreTables []string `json:"ignoreTables"`
	Table        string   `json:"table"`
	Where        string   `json:"where"`
}

type MysqlConfig struct {
	SourceUser       string   `json:"sourceUser",validate:"required"`
	SourcePassword   string   `json:"sourcePassword",validate:"required"`
	SourceHost       string   `json:"sourceHost"`
	SourcePort       string   `json:"sourcePort"`
	SourceDbName     string   `json:"sourceDbName",validate:"required"`
	TargetUser       string   `json:"targetUser,omitempty"`
	TargetPassword   string   `json:"targetPassword,omitempty"`
	TargetHost       string   `json:"targetHost,omitempty"`
	TargetPort       string   `json:"targetPort,omitempty"`
	TargetDbName     string   `json:"targetDbName,omitempty"`
	MysqlDumpVersion string   `json:"mysqlDumpVersion"`
	BeforeDump       []string `json:"beforeDump,omitempty"`
	OutputPath       string   `json:"outputPath",validate:"required"`
	Dumps            []Dump   `json:"dumps",validate:"required"`
	IsGzipped        bool     `json:"isGzipped,omitempty"`
	CleanTargetDb    bool     `json:"cleanTargetDb,omitempty"`
}

type MysqlDumpExecutor struct {
}

func (mde MysqlDumpExecutor) GetValidConfig(generalConfig config.Config) (interface{}, error) {
	var dbConf MysqlConfig
	err := json.Unmarshal([]byte(*generalConfig.Context), &dbConf)
	if err != nil {
		return MysqlConfig{}, err
	}

	if ers := validator.Validate(dbConf); ers != nil {
		return nil, ers
	}

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

	err = mde.cleanTargetDbIfNeeded(dbConfig, scriptsPath)
	if err != nil {
		return err
	}

	err = mde.copySourceToTargetIfNeeded(dbConfig, scriptsPath)
	if err != nil {
		return err
	}

	err = mde.prepareDbCopyIfNeeded(dbConfig)
	if err != nil {
		return err
	}

	err = mde.runDumps(dbConfig)
	defer func() {
		err := mde.cleanup(dbConfig, scriptsPath)
		if err != nil {
			io.OutputError(err, "", "")
		}
	}()

	return err
}

func (mde MysqlDumpExecutor) prepareOutputPath(cfg MysqlConfig) error {
	return fs.MkDir(cfg.OutputPath)
}

func (mde MysqlDumpExecutor) runGzip(cfg MysqlConfig, dumpFile string) error {
	if !cfg.IsGzipped {
		return nil
	}

	io.OutputInfo("", "Will gzip %s", dumpFile)

	cmdExec := CmdExec{
		successWriter: NewStdSuccessWriter(),
		errorWriter:   NewStdErrorWriter(),
		envs: []string{
			"MUSER=" + cfg.TargetUser,
			"MPASS=" + cfg.TargetPassword,
			"MPORT=" + cfg.TargetPort,
			"MHOST=" + cfg.TargetHost,
			"MDB=" + cfg.TargetDbName,
		},
	}

	return cmdExec.Execute(fmt.Sprintf("gzip -v -9 %s", dumpFile))
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

func (mde MysqlDumpExecutor) prepareDbConfig(cfg MysqlConfig) (MysqlConfig, error) {
	cfg.TargetDbName = mde.getEnvOrValue(cfg.TargetDbName)
	cfg.SourceDbName = mde.getEnvOrValue(cfg.SourceDbName)
	cfg.OutputPath = mde.getEnvOrValue(cfg.OutputPath)

	var err error
	if !filepath.IsAbs(cfg.OutputPath) {
		cfg.OutputPath, err = filepath.Abs(cfg.OutputPath)
	}

	cfg.MysqlDumpVersion = mde.getEnvOrValue(cfg.MysqlDumpVersion)
	cfg.TargetHost = mde.getEnvOrValue(cfg.TargetHost)
	cfg.SourceHost = mde.getEnvOrValue(cfg.SourceHost)
	cfg.TargetPort = mde.getEnvOrValue(cfg.TargetPort)
	cfg.SourcePort = mde.getEnvOrValue(cfg.SourcePort)
	cfg.TargetUser = mde.getEnvOrValue(cfg.TargetUser)
	cfg.SourceUser = mde.getEnvOrValue(cfg.SourceUser)
	cfg.TargetPassword = mde.getEnvOrValue(cfg.TargetPassword)
	cfg.SourcePassword = mde.getEnvOrValue(cfg.SourcePassword)

	return cfg, err
}

func (mde MysqlDumpExecutor) cleanTargetDbIfNeeded(cfg MysqlConfig, scriptsPath string) error {
	if cfg.TargetDbName == "" || !cfg.CleanTargetDb {
		return nil
	}

	io.OutputInfo("", "Will clean target db '%s'", cfg.TargetDbName)

	cmdExec := CmdExec{
		successWriter: NewStdSuccessWriter(),
		errorWriter:   NewStdErrorWriter(),
		envs: []string{
			"MUSER=" + cfg.TargetUser,
			"MPASS=" + cfg.TargetPassword,
			"MPORT=" + cfg.TargetPort,
			"MHOST=" + cfg.TargetHost,
			"MDB=" + cfg.TargetDbName,
		},
	}

	return cmdExec.Execute(filepath.Join(scriptsPath, "cleanDb.sh"))
}

func (mde MysqlDumpExecutor) copySourceToTargetIfNeeded(cfg MysqlConfig, scriptsPath string) error {
	if cfg.TargetDbName == "" {
		return nil
	}

	io.OutputInfo("", "Will copy source db '%s' target db '%s'", cfg.SourceDbName, cfg.TargetDbName)

	statistics := ""
	if strings.HasPrefix(cfg.MysqlDumpVersion, "8") {
		statistics = "--column-statistics=0"
	}

	cmd := fmt.Sprintf(`set -o pipefail && mysqldump %s -u${SOURCE_DB_USER} -p${SOURCE_DB_PASS} -P${SOURCE_DB_PORT} -h${SOURCE_DB_HOST} ${SOURCE_DB_NAME} | 
mysql -u${TARGET_DB_USER} -p${TARGET_DB_PASS} -P${TARGET_DB_PORT} -h${TARGET_DB_HOST} ${TARGET_DB_NAME}`,
		statistics,
	)
	cmdExec := CmdExec{
		successWriter: NewStdSuccessWriter(),
		errorWriter:   NewStdErrorWriter(),
		envs: []string{
			"SOURCE_DB_USER=" + cfg.SourceUser,
			"SOURCE_DB_PASS=" + cfg.SourcePassword,
			"SOURCE_DB_PORT=" + cfg.SourcePort,
			"SOURCE_DB_HOST=" + cfg.SourceHost,
			"SOURCE_DB_NAME=" + cfg.SourceDbName,
			"TARGET_DB_USER=" + cfg.TargetUser,
			"TARGET_DB_PASS=" + cfg.TargetPassword,
			"TARGET_DB_PORT=" + cfg.TargetPort,
			"TARGET_DB_HOST=" + cfg.TargetHost,
			"TARGET_DB_NAME=" + cfg.TargetDbName,
		},
	}

	return cmdExec.Execute(cmd)
}

func (mde MysqlDumpExecutor) execMysqlDump(cfg MysqlConfig, pipeOutput string, dump Dump) error {
	envs := []string{}
	dbName := ""
	if cfg.TargetDbName == "" {
		dbName = cfg.SourceDbName
		envs = []string{
			"MUSER=" + cfg.SourceUser,
			"MPORT=" + cfg.SourcePort,
			"MHOST=" + cfg.SourceHost,
			"MDB=" + dbName,
			"MYSQL_PWD=" + cfg.SourcePassword,
		}
	} else {
		dbName = cfg.TargetDbName
		envs = []string{
			"MUSER=" + cfg.TargetUser,
			"MPORT=" + cfg.TargetPort,
			"MHOST=" + cfg.TargetHost,
			"MDB=" + dbName,
			"MYSQL_PWD=" + cfg.TargetPassword,
		}
	}
	statistics := ""
	if strings.HasPrefix(cfg.MysqlDumpVersion, "8") {
		statistics = " --column-statistics=0"
	}

	where := ""
	if dump.Where != "" {
		where = fmt.Sprintf(` --where="%s"`, dump.Where)
	}

	ignoreTablesFlat := ""
	ignoreTables := make([]string, 0, len(dump.IgnoreTables))
	for _, it := range dump.IgnoreTables {
		ignoreTables = append(ignoreTables, fmt.Sprintf("--ignore-table=%s.%s", dbName, it))
	}
	if len(ignoreTables) > 0 {
		ignoreTablesFlat = fmt.Sprintf(" %s", strings.Join(ignoreTables, " "))
	}

	cmd := fmt.Sprintf(`set -o pipefail && mysqldump%s -u${MUSER} -P${MPORT} -h${MHOST}%s%s ${MDB} %s %s`,
		statistics,
		where,
		ignoreTablesFlat,
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

func (mde MysqlDumpExecutor) prepareDbCopyIfNeeded(cfg MysqlConfig) error {
	if cfg.TargetDbName == "" {
		return nil
	}

	if len(cfg.BeforeDump) == 0 {
		return nil
	}

	io.OutputInfo("", "Will prepare target db '%s'", cfg.TargetDbName)

	ers := errs.NewErrorContainer()
	for _, q := range cfg.BeforeDump {
		q = mde.escapeQuotes(q)
		cmd := fmt.Sprintf(`echo "%s" | mysql -u${MUSER} -P${MPORT} -h${MHOST} ${MDB}`, q)
		cmdExec := CmdExec{
			successWriter: NewStdSuccessWriter(),
			errorWriter:   NewStdErrorWriter(),
			envs: []string{
				"MUSER=" + cfg.TargetUser,
				"MPORT=" + cfg.TargetPort,
				"MHOST=" + cfg.TargetHost,
				"MDB=" + cfg.TargetDbName,
				"MYSQL_PWD=" + cfg.TargetPassword,
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

func (mde MysqlDumpExecutor) cleanup(cfg MysqlConfig, scriptsPath string) error {
	if !cfg.CleanTargetDb {
		return nil
	}

	io.OutputInfo("", "Will run cleanup")

	return mde.cleanTargetDbIfNeeded(cfg, scriptsPath)
}

func (mde MysqlDumpExecutor) runDumps(cfg MysqlConfig) error {
	ers := errs.NewErrorContainer()

	dbName := cfg.SourceDbName
	if cfg.TargetDbName != "" {
		dbName = cfg.TargetDbName
	}

	nowSuffix := time.Now().UTC().Format("02.01.2006.15.04.05.000")
	fileName := fmt.Sprintf("%s_%s.sql", nowSuffix, dbName)
	filePath := fs.JoinPath(cfg.OutputPath, fileName)

	io.OutputInfo("", "Will execute dumps")

	if len(cfg.Dumps) < 2 {
		pipedOutput := ""
		dump := Dump{}
		if cfg.IsGzipped {
			filePath += ".gz"
			pipedOutput = fmt.Sprintf("| gzip -9 >> %s", filePath)
		}
		if len(cfg.Dumps) == 1 {
			dump = cfg.Dumps[0]
		}

		err := mde.execMysqlDump(cfg, pipedOutput, dump)
		if err == nil {
			io.OutputInfo("", "Dumped db '%s' to %s", cfg.SourceDbName, filePath)
		}

		return err
	}

	for _, dump := range cfg.Dumps {
		pipedOutput := fmt.Sprintf(">> %s", filePath)
		err := mde.execMysqlDump(cfg, pipedOutput, dump)
		ers.AddError(err)
	}

	if fs.FileExists(filePath) {
		err := mde.runGzip(cfg, filePath)
		ers.AddError(err)
	}

	if ers.Result("") == nil {
		io.OutputInfo("", "Dumped db '%s' to %s", cfg.SourceDbName, filePath)
	}

	return ers.Result(" ")
}
