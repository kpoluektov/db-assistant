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
	// Uses pg_catalog instead of information_schema: the information_schema join condition
	// ccu.table_schema = tc.table_schema compares the *referenced* table's schema against
	// the FK table's schema, which silently returns 0 rows on some Managed PostgreSQL
	// configurations even when FK constraints exist.
	// pg_catalog is always accessible and handles multi-column FKs correctly via LATERAL unnest
	// with WITH ORDINALITY to preserve the column-position pairing.
	return `WITH RECURSIVE
params(p_schema, p_table, p_depth) AS (VALUES ($1::text COLLATE "C", $2::text COLLATE "C", $3::int)),
fk_edges AS (
    SELECT
        ns1.nspname::text  AS from_schema,
        t1.relname::text   AS from_table,
        a1.attname::text   AS from_col,
        ns2.nspname::text  AS to_schema,
        t2.relname::text   AS to_table,
        a2.attname::text   AS to_col,
        c.conname::text    AS constraint_name
    FROM pg_catalog.pg_constraint c
    JOIN pg_catalog.pg_class     t1  ON t1.oid = c.conrelid
    JOIN pg_catalog.pg_namespace ns1 ON ns1.oid = t1.relnamespace
    JOIN pg_catalog.pg_class     t2  ON t2.oid = c.confrelid
    JOIN pg_catalog.pg_namespace ns2 ON ns2.oid = t2.relnamespace
    JOIN LATERAL unnest(c.conkey)  WITH ORDINALITY AS fkcol(attnum, ord) ON true
    JOIN LATERAL unnest(c.confkey) WITH ORDINALITY AS pkcol(attnum, ord) ON fkcol.ord = pkcol.ord
    JOIN pg_catalog.pg_attribute a1 ON a1.attrelid = c.conrelid  AND a1.attnum = fkcol.attnum
    JOIN pg_catalog.pg_attribute a2 ON a2.attrelid = c.confrelid AND a2.attnum = pkcol.attnum
    WHERE c.contype = 'f'
),
dirs(dir) AS (VALUES ('outgoing'::text), ('incoming'::text)),
fk_tree(parent_schema, parent_table, child_schema, child_table, constraint_name, from_column, to_column, direction, depth, path) AS (
    SELECT p.p_schema, p.p_table,
           CASE d.dir WHEN 'outgoing' THEN e.to_schema   ELSE e.from_schema END,
           CASE d.dir WHEN 'outgoing' THEN e.to_table    ELSE e.from_table  END,
           e.constraint_name, e.from_col, e.to_col, d.dir, 1,
           ARRAY[p.p_schema||'.'||p.p_table,
                 CASE d.dir WHEN 'outgoing' THEN e.to_schema||'.'||e.to_table
                            ELSE e.from_schema||'.'||e.from_table END]
    FROM params p, fk_edges e, dirs d
    WHERE (d.dir = 'outgoing' AND e.from_schema = p.p_schema AND e.from_table = p.p_table)
       OR (d.dir = 'incoming' AND e.to_schema   = p.p_schema AND e.to_table   = p.p_table)
    UNION ALL
    SELECT t.child_schema, t.child_table,
           CASE d.dir WHEN 'outgoing' THEN e.to_schema   ELSE e.from_schema END,
           CASE d.dir WHEN 'outgoing' THEN e.to_table    ELSE e.from_table  END,
           e.constraint_name, e.from_col, e.to_col, d.dir, t.depth + 1,
           t.path || CASE d.dir WHEN 'outgoing' THEN e.to_schema||'.'||e.to_table
                                ELSE e.from_schema||'.'||e.from_table END
    FROM fk_tree t, params p, fk_edges e, dirs d
    WHERE t.depth < p.p_depth
    AND ((d.dir = 'outgoing' AND e.from_schema = t.child_schema AND e.from_table = t.child_table
          AND NOT (e.to_schema||'.'||e.to_table)     = ANY(t.path))
      OR (d.dir = 'incoming' AND e.to_schema   = t.child_schema AND e.to_table   = t.child_table
          AND NOT (e.from_schema||'.'||e.from_table) = ANY(t.path)))
)
SELECT parent_schema, parent_table, child_schema, child_table, constraint_name, from_column, to_column, direction, depth
FROM fk_tree ORDER BY depth`
}
