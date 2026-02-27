package db

import (
	"fmt"
	"strings"

	"database/sql"

	_ "github.com/sijms/go-ora/v2"
)

type OraConnector struct {
	pool *sql.DB
}

func NewOraConnector(dsn *DSN) (*OraConnector, error) {
	dsnStr := fmt.Sprintf("%s://%s:%s@%s:%s/%s",
		dsn.DbType, dsn.Username, dsn.Password, dsn.DbHost, dsn.DbPort, dsn.Database)
	p, err := sql.Open("oracle", dsnStr)
	return &OraConnector{p}, err
}

func (conn OraConnector) GetPool() *sql.DB {
	return conn.pool
}

func (conn OraConnector) GetTables(table string, strict bool) string {
	sqlStr := `select table_name from all_tables 
where owner = upper(:1) and table_name %s order by table_name fetch first :3 rows only`
	if !strict && (strings.Contains(table, "%") || strings.Contains(table, "_")) {
		sqlStr = fmt.Sprintf(sqlStr, "like upper(:2)")
	} else {
		sqlStr = fmt.Sprintf(sqlStr, "= upper(:2)")
	}
	return sqlStr
}

func (conn OraConnector) GetColumns() string {
	return `select column_name, data_type, data_length 
						from all_tab_columns where owner = upper(:1) 
						and table_name = upper(:2) order by column_id`
}
func (conn OraConnector) GetStats() string {
	return `select num_rows, last_analyzed from all_tables where owner = upper(:1) 
						and table_name = upper(:2)`
}

func (conn OraConnector) GetIndexes() string {
	return `select ai.index_name,     
			case AI.UNIQUENESS  when 'UNIQUE' then '1' else '0' end, 
			case ai.status when 'VALID' then '1' else '0' end, 
			case ac.constraint_type when 'P' then '1' else '0' END
			--, ai.index_type, ai.num_rows 
			from all_indexes ai left join all_constraints ac on ai.owner = ac.owner and ai.index_name =  ac.constraint_name
			where ai.owner = upper(:1) and ai.table_name = upper(:2)
			order by 1`
}

func (conn OraConnector) GetIndColumns() string {
	return `select aic.column_name, aic.column_position from all_ind_columns aic 
			where aic.index_owner = upper(:1) and aic.index_name = upper(:2) order by 2`
}

func (conn OraConnector) GetParameter() string {
	return `select name, value from v$parameter where name = :1`
}

func (conn OraConnector) GetVersionSQL() string {
	return `select banner from v$version`
}
