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
	left join pg_description pd on pd.objoid = pc.oid and pd.objsubid = 0 
	where pc.relkind = 'r' and pn.nspname = $1
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
					join pg_class c on a.attrelid = c.oid
					join pg_namespace pn on pn.oid = c.relnamespace
					left join pg_catalog.pg_description d on d.objoid = c.oid and d.objsubid = a.attnum 
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

func (conn PGConnector) GetRelationTreeSQL() string {
	return `WITH RECURSIVE
params(p_schema, p_table, p_depth) AS (
    VALUES ($1::text, $2::text, $3::int)
),
fk_edges AS (
    SELECT tc.table_schema AS from_schema, tc.table_name AS from_table, kcu.column_name AS from_col,
           ccu.table_schema AS to_schema, ccu.table_name AS to_table, ccu.column_name AS to_col,
           tc.constraint_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage ccu
        ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
),
fk_tree(parent_schema, parent_table, child_schema, child_table, constraint_name, from_column, to_column, direction, depth, path) AS (
    SELECT p.p_schema, p.p_table, e.to_schema, e.to_table, e.constraint_name, e.from_col, e.to_col,
           'outgoing'::text, 1,
           ARRAY[p.p_schema||'.'||p.p_table, e.to_schema||'.'||e.to_table]
    FROM params p, fk_edges e
    WHERE e.from_schema = p.p_schema AND e.from_table = p.p_table
    UNION ALL
    SELECT p.p_schema, p.p_table, e.from_schema, e.from_table, e.constraint_name, e.from_col, e.to_col,
           'incoming'::text, 1,
           ARRAY[p.p_schema||'.'||p.p_table, e.from_schema||'.'||e.from_table]
    FROM params p, fk_edges e
    WHERE e.to_schema = p.p_schema AND e.to_table = p.p_table
    UNION ALL
    SELECT t.child_schema, t.child_table, e.to_schema, e.to_table, e.constraint_name, e.from_col, e.to_col,
           'outgoing'::text, t.depth + 1,
           t.path || (e.to_schema||'.'||e.to_table)
    FROM fk_tree t, params p, fk_edges e
    WHERE e.from_schema = t.child_schema AND e.from_table = t.child_table
    AND t.depth < p.p_depth
    AND NOT (e.to_schema||'.'||e.to_table) = ANY(t.path)
    UNION ALL
    SELECT t.child_schema, t.child_table, e.from_schema, e.from_table, e.constraint_name, e.from_col, e.to_col,
           'incoming'::text, t.depth + 1,
           t.path || (e.from_schema||'.'||e.from_table)
    FROM fk_tree t, params p, fk_edges e
    WHERE e.to_schema = t.child_schema AND e.to_table = t.child_table
    AND t.depth < p.p_depth
    AND NOT (e.from_schema||'.'||e.from_table) = ANY(t.path)
)
SELECT parent_schema, parent_table, child_schema, child_table, constraint_name, from_column, to_column, direction, depth
FROM fk_tree ORDER BY depth`
}
