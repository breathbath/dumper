#!/usr/bin/env bash
set -e
set -o pipefail

# Detect paths`
MYSQL=$(which mysql)
AWK=$(which awk)
GREP=$(which grep)

TABLES=$(MYSQL_PWD=${MPASS} $MYSQL -u ${MUSER} -P${MPORT} -h${MHOST} ${MDB} -e 'show tables' | ${AWK} '{ print $1}' | (${GREP} -v '^Tables' || true) )

for t in ${TABLES}
do
	MYSQL_PWD=${MPASS} $MYSQL -u ${MUSER} -P${MPORT} -h${MHOST} ${MDB} -e "SET FOREIGN_KEY_CHECKS=0; drop table \`${t}\`"
done
echo "Removed all tables from db ${MDB}"
