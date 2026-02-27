package db

import (
	"fmt"
	"strings"

	"database/sql"

	_ "github.com/lib/pq"
)

type PGConnector struct {
	Pool *sql.DB
}

func NewPGConnector(dsn *DSN) (*PGConnector, error) {
	dsnString := fmt.Sprintf("%s://%s:%s@%s:%s/%s",
		dsn.DbType, dsn.Username, dsn.Password, dsn.DbHost,
		dsn.DbPort, dsn.Database)
	p, err := sql.Open("postgres", dsnString)
	return &PGConnector{p}, err
}

func (conn PGConnector) GetPool() *sql.DB {
	return conn.Pool
}

func (conn PGConnector) GetTables(table string, strict bool) string {
	sqlStr := `select table_name from information_schema.tables 
	where table_type = 'BASE TABLE' and table_schema = $1 and table_name %s order by table_name fetch first $3 rows only`
	if !strict && (strings.Contains(table, "%") || strings.Contains(table, "_")) {
		sqlStr = fmt.Sprintf(sqlStr, "like $2")
	} else {
		sqlStr = fmt.Sprintf(sqlStr, "= $2")
	}
	return sqlStr
}

func (conn PGConnector) GetColumns() string {
	return `select column_name, data_type, character_maximum_length 
						from information_schema.columns where table_schema = $1 
						and table_name = $2 order by ordinal_position`
}

func (conn PGConnector) GetStats() string {
	return `select n_live_tup, last_analyze 
						from pg_stat_all_tables where schemaname = $1 
						and relname = $2`

}

func (conn PGConnector) GetIndexes() string {
	return `select i.relname, pi.indisunique, pi.indisvalid, pi.indisprimary --, pi.indisready
			from pg_class i join pg_index pi 
			on i.oid = pi.indexrelid join pg_class t on  t.oid = pi.indrelid join pg_namespace ns on t.relnamespace = ns.oid
			where ns.nspname = $1 and t.relname = $2 order by 1`
}

func (conn PGConnector) GetIndColumns() string {
	return `select a.attname, a.attnum from
			pg_class t join pg_index ix on t.oid = ix.indrelid 
			join pg_class i on i.oid = ix.indexrelid
			join pg_attribute a on  a.attrelid = t.oid and a.attnum = ANY(ix.indkey)
			join pg_namespace ns on t.relnamespace = ns.oid
			where t.relkind = 'r' and ns.nspname  = $1 and i.relname = $2 order by 2`
}

func (conn PGConnector) GetParameter() string {
	return `select name, setting from pg_settings where name = $1`
}

func (conn PGConnector) GetVersionSQL() string {
	return `select version()`
}
