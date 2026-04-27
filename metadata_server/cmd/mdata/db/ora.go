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
	sqlStr := `select atn.table_name, atc.comments from all_tables atn, all_tab_comments atc
				where atn.owner = atc.owner and atn.table_name = atc.table_name 
				and atn.owner = upper(:1) and atn.table_name %s order by atn.table_name fetch first :3 rows only`
	if !strict && (strings.Contains(table, "%") || strings.Contains(table, "_")) {
		sqlStr = fmt.Sprintf(sqlStr, "like upper(:2)")
	} else {
		sqlStr = fmt.Sprintf(sqlStr, "= upper(:2)")
	}
	return sqlStr
}

func (conn OraConnector) GetColumns() string {
	return `select atc1.column_name, atc1.data_type, atc1.data_length, atc2.comments 
						from all_tab_columns atc1, all_tab_comments atc2
						where atc1.owner(+) = atc2.owner AND atc1.table_name(+) = atc2.table_name
						and atc1.owner = upper(:1) 
						and atc1.table_name = upper(:2) order by atc1.column_id`
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

func (conn OraConnector) GetRoCommand() string {
	return `set transaction read only;`
}

func (conn OraConnector) GetForeignKeys() string {
	return `SELECT c.constraint_name,
		c.owner AS from_schema, c.table_name AS from_table, cc.column_name AS from_column,
		r.owner AS to_schema, r.table_name AS to_table, rc.column_name AS to_column
	FROM all_constraints c
	JOIN all_cons_columns cc ON cc.constraint_name = c.constraint_name AND cc.owner = c.owner
	JOIN all_constraints r ON r.constraint_name = c.r_constraint_name AND r.owner = c.r_owner
	JOIN all_cons_columns rc ON rc.constraint_name = r.constraint_name AND rc.owner = r.owner
		AND rc.position = cc.position
	WHERE c.constraint_type = 'R'
	AND ((c.owner = upper(:1) AND c.table_name = upper(:2)) OR (r.owner = upper(:3) AND r.table_name = upper(:4)))`
}
