package db

import (
	"fmt"
	"strings"

	"github.com/breathbath/dumper/cli"
	"github.com/breathbath/go_utils/utils/errs"
	"github.com/breathbath/go_utils/utils/io"
	validation "github.com/go-ozzo/ozzo-validation"
)

type ConnConfig struct {
	User     string `json:"user" validate:"required"`
	Password string `json:"password" validate:"required"`
	Host     string `json:"host"`
	Port     string `json:"port"`
	DBName   string `json:"db" validate:"required"`
}

type Dump struct {
	IgnoreTables []string `json:"ignoreTables,omitempty"`
	Table        string   `json:"table,omitempty"`
	Where        string   `json:"where,omitempty"`
	Flags        []string `json:"flags,omitempty"`
}

func (dc *ConnConfig) Validate() error {
	return validation.ValidateStruct(dc,
		validation.Field(&dc.User, validation.Required),
		validation.Field(&dc.Password, validation.Required),
		validation.Field(&dc.DBName, validation.Required),
	)
}

func ImportDumpFromFileToDB(dbConn *ConnConfig, filePath string) (err error) {
	io.OutputInfo("", "Will import '%s' to db '%s'", dbConn.DBName, filePath)

	cmd := fmt.Sprintf(
		`set -o pipefail && cat %s | mysql -u${DB_USER} -p${DB_PASS} -P${DB_PORT} -h${DB_HOST} ${DB_NAME}`,
		filePath,
	)

	cmdExec := cli.CmdExec{
		SuccessWriter: cli.NewStdSuccessWriter(),
		ErrorWriter:   cli.NewStdErrorWriter(),
		Envs: []string{
			"DB_USER=" + dbConn.User,
			"DB_PASS=" + dbConn.Password,
			"DB_PORT=" + dbConn.Port,
			"DB_HOST=" + dbConn.Host,
			"DB_NAME=" + dbConn.DBName,
		},
	}

	return cmdExec.Execute(cmd)
}

func ExecMysql(dbConn *ConnConfig, sql string, useDBName bool) (err error) {
	io.OutputInfo("", "Will execute '%s' to db '%s'", sql, dbConn.DBName)

	dbName := "${DB_NAME}"
	if !useDBName {
		dbName = ""
	}

	cmd := fmt.Sprintf(
		`mysql -u${DB_USER} -p${DB_PASS} -P${DB_PORT} -h${DB_HOST} %s -e '%s'`,
		dbName,
		sql,
	)

	cmdExec := cli.CmdExec{
		SuccessWriter: cli.NewStdSuccessWriter(),
		ErrorWriter:   cli.NewStdErrorWriter(),
		Envs: []string{
			"DB_USER=" + dbConn.User,
			"DB_PASS=" + dbConn.Password,
			"DB_PORT=" + dbConn.Port,
			"DB_HOST=" + dbConn.Host,
			"DB_NAME=" + dbConn.DBName,
		},
	}

	return cmdExec.Execute(cmd)
}

func SanitizeTargetDB(dbConn *ConnConfig, scriptsToRun []string) error {
	if dbConn.DBName == "" || len(scriptsToRun) == 0 {
		return nil
	}

	io.OutputInfo("", "Will sanitize db '%s'", dbConn.DBName)

	ers := errs.NewErrorContainer()
	for _, q := range scriptsToRun {
		q = cli.EscapeQuotes(q)
		cmd := fmt.Sprintf(`echo %q | mysql -u${MUSER} -P${MPORT} -h${MHOST} -p${MYSQL_PWD} ${MDB}`, q)
		cmdExec := cli.CmdExec{
			SuccessWriter: cli.NewStdSuccessWriter(),
			ErrorWriter:   cli.NewStdErrorWriter(),
			Envs: []string{
				"MUSER=" + dbConn.User,
				"MPORT=" + dbConn.Port,
				"MHOST=" + dbConn.Host,
				"MDB=" + dbConn.DBName,
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

func ExecMysqlDump(cfg *ConnConfig, pipeOutput, mysqldumpVersion string, dump *Dump) error {
	envs := []string{
		"MUSER=" + cfg.User,
		"MPORT=" + cfg.Port,
		"MHOST=" + cfg.Host,
		"MDB=" + cfg.DBName,
		"MYSQL_PWD=" + cfg.Password,
	}

	statistics := ""
	if strings.HasPrefix(mysqldumpVersion, "8") {
		statistics = " --column-statistics=0"
	}

	where := ""
	if dump.Where != "" {
		where = fmt.Sprintf(` --where=%q`, dump.Where)
	}

	flagsFlat := ""
	if len(dump.Flags) > 0 {
		flagsFlat = " " + strings.Join(dump.Flags, " ")
	}

	ignoreTablesFlat := ""
	ignoreTables := make([]string, 0, len(dump.IgnoreTables))
	for _, it := range dump.IgnoreTables {
		ignoreTables = append(ignoreTables, fmt.Sprintf("--ignore-table=%s.%s", cfg.DBName, it))
	}
	if len(ignoreTables) > 0 {
		ignoreTablesFlat = fmt.Sprintf(" %s", strings.Join(ignoreTables, " "))
	}

	lockTables := ""
	if where != "" {
		lockTables = " --lock-all-tables"
	}

	cmd := fmt.Sprintf(`set -o pipefail && mysqldump%s -u${MUSER} -p${MYSQL_PWD} -P${MPORT} -h${MHOST}%s%s%s%s ${MDB} %s %s`,
		statistics,
		flagsFlat,
		where,
		ignoreTablesFlat,
		lockTables,
		dump.Table,
		pipeOutput,
	)

	cmdExec := cli.CmdExec{
		SuccessWriter: cli.NewStdSuccessWriter(),
		ErrorWriter:   cli.NewStdErrorWriter(),
		Envs:          envs,
	}

	return cmdExec.Execute(cmd)
}

func PrepareDBConnConfig(connCfg *ConnConfig) {
	connCfg.DBName = cli.GetEnvOrValue(connCfg.DBName)
	connCfg.Host = cli.GetEnvOrValue(connCfg.Host)
	connCfg.Port = cli.GetEnvOrValue(connCfg.Port)
	connCfg.User = cli.GetEnvOrValue(connCfg.User)
	connCfg.Password = cli.GetEnvOrValue(connCfg.Password)
}
