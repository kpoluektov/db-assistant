package db

import (
	"database/sql"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/lefinal/nulls"
)

type DSN struct {
	DbType   string
	Username string
	Password string
	DbHost   string
	DbPort   string
	Database string
	CAPath   string
}

type Table struct {
	Name        string         `json:"name"`
	Description sql.NullString `json:"description,omitempty"`
	Columns     *[]Column      `json:"columns,omitempty"`
}

type Column struct {
	Name        string         `json:"name"`
	Type        string         `json:"type"`
	Length      nulls.Int      `json:"length,omitempty"`
	Description sql.NullString `json:"description,omitempty"`
}

type Stats struct {
	NumRows      nulls.Int  `json:"numrows,omitempty"`
	LastAnalized nulls.Time `json:"lastanalyzed,omitempty"`
}

type IndexColumn struct {
	Name     string    `json:"name"`
	Position nulls.Int `json:"position"`
}
type Index struct {
	Name         string         `json:"name"`
	Uniquenes    bool           `json:unique`
	Validity     bool           `json: valid`
	IsPrimaryKey bool           `json: is_pk`
	Columns      *[]IndexColumn `json:"columns,omitempty"`
}
type Parameter struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type ForeignKey struct {
	ConstraintName string `json:"constraint_name"`
	FromSchema     string `json:"from_schema"`
	FromTable      string `json:"from_table"`
	FromColumn     string `json:"from_column"`
	ToSchema       string `json:"to_schema"`
	ToTable        string `json:"to_table"`
	ToColumn       string `json:"to_column"`
}

type RelationEdge struct {
	ConstraintName string        `json:"constraint_name"`
	FromColumn     string        `json:"from_column"`
	ToColumn       string        `json:"to_column"`
	Direction      string        `json:"direction"`
	Node           *RelationNode `json:"node"`
}

type RelationNode struct {
	Schema    string          `json:"schema"`
	Table     string          `json:"table"`
	Relations []*RelationEdge `json:"relations,omitempty"`
}

type Connector interface {
	GetPool() *sql.DB
	GetTables(table string, strict bool) string
	GetColumns() string
	GetStats() string
	GetIndexes() string
	GetIndColumns() string
	GetParameter() string
	GetVersionSQL() string
	GetRoCommand() string
	GetForeignKeys() string
}

func InitPool(dsn *DSN) (Connector, error) {
	var connector Connector
	var err error
	switch dsn.DbType {
	case "postgres":
		connector, err = NewPGConnector(dsn)
	case "oracle":
		connector, err = NewOraConnector(dsn)
	case "mysql":
		connector, err = NewMySQLConnector(dsn)
	default:
		err = errors.New("Unknown connection type")
	}

	if err != nil {
		log.Printf("Unable to create connection pool: %v\n", err)
	}
	return connector, err
}

type Connection struct {
	status         bool
	connector      Connector
	createdAt      time.Time
	lastActivityAt time.Time
	id             string
	version        string
}

func (c *Connection) close() {
	log.Printf("Closing connection %s", c.id)
	c.connector.GetPool().Close()
	c.status = false
}

func (conn *Connection) GetStatus() bool {
	return conn.status
}

func (conn *Connection) CurVersion() string {
	return conn.version
}

func (conn *Connection) Check() error {
	var err error
	if conn.status {
		err = conn.connector.GetPool().Ping()
	} else {
		err = errors.New("No active connection")
	}
	return err
}

func (conn *Connection) GetTables(schema string, table string, size int, strict bool) ([]Table, error) {
	tables := []Table{}

	sqlStr := conn.connector.GetTables(table, strict)
	//log.Printf("sqlStr is %s", sqlStr)
	rows, err := conn.connector.GetPool().Query(sqlStr, schema, table, size)
	if err != nil {
		log.Printf("Query failed: %v\n", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var tName string
		var tDescription sql.NullString
		err = rows.Scan(&tName, &tDescription)
		if err != nil {
			break
		}
		if !strict {
			tables = append(tables, Table{tName, tDescription, nil})
		} else {
			cols, _ := conn.getColumns(schema, table)
			tables = append(tables, Table{tName, tDescription, &cols})
		}
	}
	if err != nil {
		log.Printf("QueryRow failed: %v\n", err)
	}
	return tables, err
}

func (conn *Connection) getColumns(schema string, table string) ([]Column, error) {
	columns := []Column{}
	sqlStr := conn.connector.GetColumns()
	rows, err := conn.connector.GetPool().Query(sqlStr, schema, table)
	if err != nil {
		log.Printf("Rows queuing failed: %v\n", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var cName, cType string
		var сDescription sql.NullString
		var cLength nulls.Int
		err = rows.Scan(&cName, &cType, &cLength, &сDescription)
		if err != nil {
			break
		}
		columns = append(columns, Column{cName, cType, cLength, сDescription})
	}
	if err != nil {
		log.Printf("Rows quering failed: %v\n", err)
	}
	return columns, err
}

func (conn *Connection) GetStats(schema string, table string) (Stats, error) {
	var numRows nulls.Int
	var lastAnalized nulls.Time
	sqlStr := conn.connector.GetStats()
	rows, err := conn.connector.GetPool().Query(sqlStr, schema, table)
	if err != nil {
		return Stats{numRows, lastAnalized}, err
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&numRows, &lastAnalized)
	}
	if err != nil {
		log.Printf("Stats querying failed: %v\n", err)
	}
	return Stats{numRows, lastAnalized}, err
}

func (conn *Connection) GetIndexes(schema string, table string) ([]Index, error) {
	indexes := []Index{}

	sqlStr := conn.connector.GetIndexes()
	//log.Printf("sqlStr is %s", sqlStr)
	rows, err := conn.connector.GetPool().Query(sqlStr, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var iName string
		var iUnique, iValid, iIsPk bool
		err = rows.Scan(&iName, &iUnique, &iValid, &iIsPk)
		if err != nil {
			break
		}
		var cols, _ = conn.getIndColumns(schema, iName)
		indexes = append(indexes, Index{iName, iUnique, iValid, iIsPk, &cols})
	}
	if err != nil {
		log.Printf("QueryRow failed: %v\n", err)
	}
	return indexes, err
}

func (conn *Connection) getIndColumns(schema string, index string) ([]IndexColumn, error) {
	columns := []IndexColumn{}
	sqlStr := conn.connector.GetIndColumns()
	rows, err := conn.connector.GetPool().Query(sqlStr, schema, index)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var cName string
		var cPosition nulls.Int
		err = rows.Scan(&cName, &cPosition)
		if err != nil {
			break
		}
		columns = append(columns, IndexColumn{cName, cPosition})
	}
	if err != nil {
		log.Printf("Rows quering failed: %v\n", err)
	}
	return columns, err
}

func (conn *Connection) GetParameter(pName string) ([]Parameter, error) {
	params := []Parameter{}
	sqlStr := conn.connector.GetParameter()
	rows, err := conn.connector.GetPool().Query(sqlStr, pName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var cName string
		var cValue string
		err = rows.Scan(&cName, &cValue)
		if err != nil {
			break
		}
		params = append(params, Parameter{cName, cValue})
	}
	if err != nil {
		log.Printf("Rows quering failed: %v\n", err)
	}
	return params, err
}

func (conn *Connection) GetWideResult(pSQL string) ([]map[string]any, error) {
	finalSQL := conn.connector.GetRoCommand() + pSQL
	rows, err := conn.connector.GetPool().Query(finalSQL)
	if err != nil {
		log.Printf("Rows queuing failed: %v\n", err)
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var results []map[string]any
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			return nil, err
		}

		m := make(map[string]any)
		for i, colName := range columns {
			m[colName] = values[i]
		}
		results = append(results, m)
	}
	if err = rows.Err(); err != nil {
		log.Printf("Rows quering failed: %v\n", err)
		return nil, err
	}
	return results, err

}

func (conn *Connection) fetchForeignKeys(schema, table string) ([]ForeignKey, error) {
	fks := []ForeignKey{}
	sqlStr := conn.connector.GetForeignKeys()
	if sqlStr == "" {
		return fks, nil
	}
	rows, err := conn.connector.GetPool().Query(sqlStr, schema, table, schema, table)
	if err != nil {
		log.Printf("Foreign keys query failed: %v\n", err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var fk ForeignKey
		err = rows.Scan(&fk.ConstraintName, &fk.FromSchema, &fk.FromTable, &fk.FromColumn,
			&fk.ToSchema, &fk.ToTable, &fk.ToColumn)
		if err != nil {
			break
		}
		fks = append(fks, fk)
	}
	if err != nil {
		log.Printf("Foreign keys scanning failed: %v\n", err)
	}
	return fks, err
}

func (conn *Connection) GetRelationTree(schema, table string, maxDepth int) (*RelationNode, error) {
	if maxDepth > 5 {
		maxDepth = 5
	}
	visited := map[string]bool{strings.ToLower(schema) + "." + strings.ToLower(table): true}
	root := &RelationNode{Schema: schema, Table: table}

	type queueItem struct {
		node  *RelationNode
		depth int
	}
	queue := []queueItem{{node: root, depth: 0}}

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]
		if item.depth >= maxDepth {
			continue
		}

		fks, err := conn.fetchForeignKeys(item.node.Schema, item.node.Table)
		if err != nil {
			return nil, err
		}

		for _, fk := range fks {
			var childSchema, childTable, direction string
			if strings.EqualFold(fk.FromSchema, item.node.Schema) && strings.EqualFold(fk.FromTable, item.node.Table) {
				childSchema, childTable = fk.ToSchema, fk.ToTable
				direction = "outgoing"
			} else {
				childSchema, childTable = fk.FromSchema, fk.FromTable
				direction = "incoming"
			}

			childKey := strings.ToLower(childSchema) + "." + strings.ToLower(childTable)
			if visited[childKey] {
				continue
			}
			visited[childKey] = true

			childNode := &RelationNode{Schema: childSchema, Table: childTable}
			item.node.Relations = append(item.node.Relations, &RelationEdge{
				ConstraintName: fk.ConstraintName,
				FromColumn:     fk.FromColumn,
				ToColumn:       fk.ToColumn,
				Direction:      direction,
				Node:           childNode,
			})
			queue = append(queue, queueItem{node: childNode, depth: item.depth + 1})
		}
	}

	return root, nil
}

/* not a connection member */
func GetVersion(connector Connector) string {
	version := "not defined"
	sqlStr := connector.GetVersionSQL()
	rows, err := connector.GetPool().Query(sqlStr)
	if err != nil {
		log.Printf("Version queuing failed: %v\n", err)
		return version
	}
	defer rows.Close()
	if err != nil {
		log.Printf("Version quering failed: %v\n", err)
	}
	return version
}
