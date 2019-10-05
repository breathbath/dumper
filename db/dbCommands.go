package db

import (
	"fmt"
	"github.com/breathbath/dumper/cli"
	"github.com/breathbath/go_utils/utils/errs"
	"github.com/breathbath/go_utils/utils/io"
	validation "github.com/go-ozzo/ozzo-validation"
	"strings"
)

type DbConn struct {
	User     string `json:"user" validate:"required"`
	Password string `json:"password" validate:"required"`
	Host     string `json:"host"`
	Port     string `json:"port"`
	DbName   string `json:"db" validate:"required"`
}

type Dump struct {
	IgnoreTables []string `json:"ignoreTables,omitempty"`
	Table        string   `json:"table,omitempty"`
	Where        string   `json:"where,omitempty"`
	Flags        []string `json:"flags,omitempty"`
}

func (dc DbConn) Validate() error {
	return validation.ValidateStruct(&dc,
		validation.Field(&dc.User, validation.Required),
		validation.Field(&dc.Password, validation.Required),
		validation.Field(&dc.DbName, validation.Required),
	)
}

func ImportDumpFromFileToDb(dbConn DbConn, filePath string) (err error) {
	io.OutputInfo("", "Will import '%s' to db '%s'", dbConn.DbName, filePath)

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
			"DB_NAME=" + dbConn.DbName,
		},
	}

	return cmdExec.Execute(cmd)
}

func ExecMysql(dbConn DbConn, sql string, useDbName bool) (err error) {
	io.OutputInfo("", "Will execute '%s' to db '%s'", sql, dbConn.DbName)

	dbName := "${DB_NAME}"
	if !useDbName {
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
			"DB_NAME=" + dbConn.DbName,
		},
	}

	return cmdExec.Execute(cmd)
}

func SanitizeTargetDb(dbConn DbConn, scriptsToRun []string) error {
	if dbConn.DbName == "" || len(scriptsToRun) == 0 {
		return nil
	}

	io.OutputInfo("", "Will sanitize db '%s'", dbConn.DbName)

	ers := errs.NewErrorContainer()
	for _, q := range scriptsToRun {
		q = cli.EscapeQuotes(q)
		cmd := fmt.Sprintf(`echo "%s" | mysql -u${MUSER} -P${MPORT} -h${MHOST} ${MDB}`, q)
		cmdExec := cli.CmdExec{
			SuccessWriter: cli.NewStdSuccessWriter(),
			ErrorWriter:   cli.NewStdErrorWriter(),
			Envs: []string{
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

func ExecMysqlDump(cfg DbConn, pipeOutput, mysqldumpVersion string, dump Dump) error {
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

	cmdExec := cli.CmdExec{
		SuccessWriter: cli.NewStdSuccessWriter(),
		ErrorWriter:   cli.NewStdErrorWriter(),
		Envs:          envs,
	}

	return cmdExec.Execute(cmd)
}

func PrepareDbConnConfig(connCfg DbConn) DbConn {
	connCfg.DbName = cli.GetEnvOrValue(connCfg.DbName)
	connCfg.Host = cli.GetEnvOrValue(connCfg.Host)
	connCfg.Port = cli.GetEnvOrValue(connCfg.Port)
	connCfg.User = cli.GetEnvOrValue(connCfg.User)
	connCfg.Password = cli.GetEnvOrValue(connCfg.Password)

	return connCfg
}
