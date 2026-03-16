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
	sqlStr := `select pc.relname, pd.description from pg_class pc 
	join pg_catalog.pg_namespace pn ON pn.oid = pc.relnamespace
	left join pg_description pd on pd.objoid = pc.oid	 
	where pc.relkind = 'r' and pn.nspname = $1 and and pd.objsubid = 0 
	and pc.relname %s order by 1 fetch first $3 rows only`
	if !strict && (strings.Contains(table, "%") || strings.Contains(table, "_")) {
		sqlStr = fmt.Sprintf(sqlStr, "like $2")
	} else {
		sqlStr = fmt.Sprintf(sqlStr, "= $2")
	}
	return sqlStr
}

func (conn PGConnector) GetColumns() string {
	return `select a.attname AS column_name,
    				format_type(a.atttypid, a.atttypmod) AS data_type,
					null as data_length,
    				col_description(a.attrelid, a.attnum) AS column_comment  
					from pg_attribute a
					join pg_namespace pn on pn.oid = c.relnamespace
					join pg_class c on a.attrelid = c.oid
					left join pg_catalog.pg_description d ON d.objoid = c.oid AND d.objsubid = a.attnum 
					where pn.nspname = $1 and c.relname = $2 and a.attnum > 0 and not a.attisdropped
					order by a.attnum`
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

func (conn PGConnector) GetRoCommand() string {
	return `set session characteristics as transaction read only;`
}
