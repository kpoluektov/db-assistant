package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInitPool_UnknownType(t *testing.T) {
	conn, err := InitPool(&DSN{DbType: "clickhouse"})
	assert.Error(t, err)
	assert.Nil(t, conn)
	assert.Contains(t, err.Error(), "Unknown connection type")
}

func TestInitPool_PostgresOpen(t *testing.T) {
	conn, err := InitPool(&DSN{
		DbType:   "postgres",
		Username: "u",
		Password: "p",
		DbHost:   "localhost",
		DbPort:   "5432",
		Database: "testdb",
	})
	assert.NoError(t, err)
	assert.NotNil(t, conn)
	assert.NotNil(t, conn.GetPool())
	conn.GetPool().Close()
}

func TestInitPool_MySQLOpen_RejectedByDriver(t *testing.T) {
	// NewMySQLConnector builds "mysql://user:pass@host:port/db" which the MySQL driver
	// does not accept in the non-TLS path (expects "user:pass@tcp(host:port)/db").
	// InitPool returns a MySQLConnector with a nil pool and an error.
	conn, err := InitPool(&DSN{
		DbType:   "mysql",
		Username: "u",
		Password: "p",
		DbHost:   "localhost",
		DbPort:   "3306",
		Database: "testdb",
	})
	assert.Error(t, err)
	assert.NotNil(t, conn)
	assert.Nil(t, conn.GetPool())
}

func TestInitPool_OracleOpen(t *testing.T) {
	conn, err := InitPool(&DSN{
		DbType:   "oracle",
		Username: "u",
		Password: "p",
		DbHost:   "localhost",
		DbPort:   "1521",
		Database: "XE",
	})
	assert.NoError(t, err)
	assert.NotNil(t, conn)
	assert.NotNil(t, conn.GetPool())
	conn.GetPool().Close()
}

func TestInitPool_PostgresOpen_EmptyFields(t *testing.T) {
	conn, err := InitPool(&DSN{
		DbType: "postgres",
	})
	assert.NoError(t, err)
	assert.NotNil(t, conn)
	conn.GetPool().Close()
}

func TestConnection_GetStatus(t *testing.T) {
	conn := &Connection{status: true}
	assert.True(t, conn.GetStatus())

	conn = &Connection{status: false}
	assert.False(t, conn.GetStatus())
}

func TestConnection_CurVersion(t *testing.T) {
	conn := &Connection{version: "PostgreSQL 16.0"}
	assert.Equal(t, "PostgreSQL 16.0", conn.CurVersion())

	conn = &Connection{}
	assert.Equal(t, "", conn.CurVersion())
}

func TestConnection_Check_NoActiveConnection(t *testing.T) {
	conn := &Connection{status: false}
	err := conn.Check()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "No active connection")
}

func TestAssembleRelationTree_NilEdges(t *testing.T) {
	root := assembleRelationTree("public", "users", nil)
	assert.Equal(t, "public", root.Schema)
	assert.Equal(t, "users", root.Table)
	assert.Nil(t, root.Relations)
}

func TestAssembleRelationTree_EmptyEdges(t *testing.T) {
	root := assembleRelationTree("s1", "t1", []treeEdgeRow{})
	assert.Equal(t, "s1", root.Schema)
	assert.Equal(t, "t1", root.Table)
	assert.Nil(t, root.Relations)
}

func TestAssembleRelationTree_SingleOutgoing(t *testing.T) {
	edges := []treeEdgeRow{
		{
			parentSchema:   "public",
			parentTable:    "users",
			childSchema:    "public",
			childTable:     "orders",
			constraintName: "fk_orders_user",
			fromColumn:     "id",
			toColumn:       "user_id",
			direction:      "outgoing",
			depth:          1,
		},
	}
	root := assembleRelationTree("public", "users", edges)
	assert.Equal(t, "users", root.Table)
	assert.Len(t, root.Relations, 1)

	rel := root.Relations[0]
	assert.Equal(t, "orders", rel.Node.Table)
	assert.Equal(t, "public", rel.Node.Schema)
	assert.Equal(t, "outgoing", rel.Direction)
	assert.Equal(t, "fk_orders_user", rel.ConstraintName)
	assert.Equal(t, "id", rel.FromColumn)
	assert.Equal(t, "user_id", rel.ToColumn)
	assert.Nil(t, rel.Node.Relations)
}

func TestAssembleRelationTree_SingleIncoming(t *testing.T) {
	edges := []treeEdgeRow{
		{
			parentSchema:   "public",
			parentTable:    "users",
			childSchema:    "public",
			childTable:     "orders",
			constraintName: "fk_orders_user",
			fromColumn:     "user_id",
			toColumn:       "id",
			direction:      "incoming",
			depth:          1,
		},
	}
	root := assembleRelationTree("public", "users", edges)
	assert.Len(t, root.Relations, 1)
	rel := root.Relations[0]
	assert.Equal(t, "orders", rel.Node.Table)
	assert.Equal(t, "incoming", rel.Direction)
	assert.Equal(t, "user_id", rel.FromColumn)
	assert.Equal(t, "id", rel.ToColumn)
}

func TestAssembleRelationTree_MultipleEdgesFromRoot(t *testing.T) {
	edges := []treeEdgeRow{
		{
			parentSchema: "public", parentTable: "users",
			childSchema: "public", childTable: "orders",
			constraintName: "fk_orders_user", direction: "outgoing", depth: 1,
		},
		{
			parentSchema: "public", parentTable: "users",
			childSchema: "public", childTable: "profiles",
			constraintName: "fk_profiles_user", direction: "outgoing", depth: 1,
		},
	}
	root := assembleRelationTree("public", "users", edges)
	assert.Len(t, root.Relations, 2)
	childTables := map[string]bool{}
	for _, r := range root.Relations {
		childTables[r.Node.Table] = true
	}
	assert.True(t, childTables["orders"])
	assert.True(t, childTables["profiles"])
}

func TestAssembleRelationTree_TwoLevelsDeep(t *testing.T) {
	edges := []treeEdgeRow{
		{
			parentSchema: "public", parentTable: "users",
			childSchema: "public", childTable: "orders",
			constraintName: "fk1", direction: "outgoing", depth: 1,
		},
		{
			parentSchema: "public", parentTable: "orders",
			childSchema: "public", childTable: "items",
			constraintName: "fk2", direction: "outgoing", depth: 2,
		},
	}
	root := assembleRelationTree("public", "users", edges)
	assert.Len(t, root.Relations, 1)

	ordersNode := root.Relations[0].Node
	assert.Equal(t, "orders", ordersNode.Table)
	assert.Len(t, ordersNode.Relations, 1)

	itemsNode := ordersNode.Relations[0].Node
	assert.Equal(t, "items", itemsNode.Table)
	assert.Nil(t, itemsNode.Relations)
}

func TestAssembleRelationTree_SkipsOrphanEdge(t *testing.T) {
	edges := []treeEdgeRow{
		{
			parentSchema: "public", parentTable: "non_existent",
			childSchema: "public", childTable: "some_table",
			constraintName: "fk_orphan", direction: "outgoing", depth: 1,
		},
	}
	root := assembleRelationTree("public", "users", edges)
	assert.Nil(t, root.Relations)
}

func TestAssembleRelationTree_CaseInsensitiveMatching(t *testing.T) {
	edges := []treeEdgeRow{
		{
			parentSchema: "PUBLIC", parentTable: "USERS",
			childSchema: "public", childTable: "orders",
			constraintName: "fk", direction: "outgoing", depth: 1,
		},
	}
	root := assembleRelationTree("public", "users", edges)
	assert.Len(t, root.Relations, 1)
	assert.Equal(t, "orders", root.Relations[0].Node.Table)
}

func TestAssembleRelationTree_MultipleFKsBetweenSameTables(t *testing.T) {
	edges := []treeEdgeRow{
		{
			parentSchema: "public", parentTable: "users",
			childSchema: "public", childTable: "addresses",
			constraintName: "fk_primary_addr", fromColumn: "id", toColumn: "primary_addr_id",
			direction: "outgoing", depth: 1,
		},
		{
			parentSchema: "public", parentTable: "users",
			childSchema: "public", childTable: "addresses",
			constraintName: "fk_billing_addr", fromColumn: "id", toColumn: "billing_addr_id",
			direction: "outgoing", depth: 1,
		},
	}
	root := assembleRelationTree("public", "users", edges)
	assert.Len(t, root.Relations, 2)
	assert.Equal(t, "addresses", root.Relations[0].Node.Table)
	assert.Equal(t, "addresses", root.Relations[1].Node.Table)
}

func TestAssembleRelationTree_DifferentSchema(t *testing.T) {
	edges := []treeEdgeRow{
		{
			parentSchema: "public", parentTable: "users",
			childSchema: "audit", childTable: "user_changes",
			constraintName: "fk_audit", direction: "outgoing", depth: 1,
		},
	}
	root := assembleRelationTree("public", "users", edges)
	assert.Len(t, root.Relations, 1)
	assert.Equal(t, "audit", root.Relations[0].Node.Schema)
	assert.Equal(t, "user_changes", root.Relations[0].Node.Table)
}

func TestPGConnector_GetPool(t *testing.T) {
	conn, err := InitPool(&DSN{DbType: "postgres"})
	assert.NoError(t, err)
	pg, ok := conn.(*PGConnector)
	assert.True(t, ok)
	assert.NotNil(t, pg.GetPool())
	pg.GetPool().Close()
}

func TestMySQLConnector_GetPool(t *testing.T) {
	conn := &MySQLConnector{}
	assert.Nil(t, conn.GetPool())
}

func TestOraConnector_GetPool(t *testing.T) {
	conn, err := InitPool(&DSN{DbType: "oracle"})
	assert.NoError(t, err)
	oc, ok := conn.(*OraConnector)
	assert.True(t, ok)
	assert.NotNil(t, oc.GetPool())
	oc.GetPool().Close()
}

func TestPGConnector_GetTablesSQL(t *testing.T) {
	conn := &PGConnector{}
	sql := conn.GetTables("test", true)
	assert.Contains(t, sql, "= $2")

	sql = conn.GetTables("test%", false)
	assert.Contains(t, sql, "like $2")

	sql = conn.GetTables("test_", false)
	assert.Contains(t, sql, "like $2")
}

func TestMySQLConnector_GetTablesSQL(t *testing.T) {
	conn := &MySQLConnector{}
	sql := conn.GetTables("test", true)
	assert.Contains(t, sql, "= ?")

	sql = conn.GetTables("test%", false)
	assert.Contains(t, sql, "like ?")
}

func TestOraConnector_GetTablesSQL(t *testing.T) {
	conn := &OraConnector{}
	sql := conn.GetTables("test", true)
	assert.Contains(t, sql, "= upper(:2)")

	sql = conn.GetTables("test%", false)
	assert.Contains(t, sql, "like upper(:2)")
}

func TestPGConnector_GetColumnsSQL(t *testing.T) {
	conn := &PGConnector{}
	sql := conn.GetColumns()
	assert.Contains(t, sql, "$1")
	assert.Contains(t, sql, "$2")
	assert.Contains(t, sql, "attname")
}

func TestMySQLConnector_GetColumnsSQL(t *testing.T) {
	conn := &MySQLConnector{}
	sql := conn.GetColumns()
	assert.Contains(t, sql, "?")
	assert.Contains(t, sql, "column_name")
}

func TestOraConnector_GetColumnsSQL(t *testing.T) {
	conn := &OraConnector{}
	sql := conn.GetColumns()
	assert.Contains(t, sql, ":1")
	assert.Contains(t, sql, ":2")
}

func TestPGConnector_GetStatsSQL(t *testing.T) {
	conn := &PGConnector{}
	sql := conn.GetStats()
	assert.Contains(t, sql, "n_live_tup")
}

func TestMySQLConnector_GetStatsSQL(t *testing.T) {
	conn := &MySQLConnector{}
	sql := conn.GetStats()
	assert.Contains(t, sql, "table_rows")
}

func TestOraConnector_GetStatsSQL(t *testing.T) {
	conn := &OraConnector{}
	sql := conn.GetStats()
	assert.Contains(t, sql, "num_rows")
}

func TestPGConnector_GetIndexesSQL(t *testing.T) {
	conn := &PGConnector{}
	sql := conn.GetIndexes()
	assert.Contains(t, sql, "indisunique")
}

func TestMySQLConnector_GetIndexesSQL(t *testing.T) {
	conn := &MySQLConnector{}
	sql := conn.GetIndexes()
	assert.Contains(t, sql, "non_unique")
}

func TestOraConnector_GetIndexesSQL(t *testing.T) {
	conn := &OraConnector{}
	sql := conn.GetIndexes()
	assert.Contains(t, sql, "UNIQUENESS")
}

func TestPGConnector_GetIndColumnsSQL(t *testing.T) {
	conn := &PGConnector{}
	sql := conn.GetIndColumns()
	assert.Contains(t, sql, "attname")
	assert.Contains(t, sql, "indkey")
}

func TestMySQLConnector_GetIndColumnsSQL(t *testing.T) {
	conn := &MySQLConnector{}
	sql := conn.GetIndColumns()
	assert.Contains(t, sql, "seq_in_index")
}

func TestOraConnector_GetIndColumnsSQL(t *testing.T) {
	conn := &OraConnector{}
	sql := conn.GetIndColumns()
	assert.Contains(t, sql, "column_position")
}

func TestPGConnector_GetParameterSQL(t *testing.T) {
	conn := &PGConnector{}
	sql := conn.GetParameter()
	assert.Contains(t, sql, "pg_settings")
}

func TestMySQLConnector_GetParameterSQL(t *testing.T) {
	conn := &MySQLConnector{}
	sql := conn.GetParameter()
	assert.Contains(t, sql, "show variables")
}

func TestOraConnector_GetParameterSQL(t *testing.T) {
	conn := &OraConnector{}
	sql := conn.GetParameter()
	assert.Contains(t, sql, "v$parameter")
}

func TestPGConnector_GetVersionSQL(t *testing.T) {
	conn := &PGConnector{}
	assert.Equal(t, "select version()", conn.GetVersionSQL())
}

func TestMySQLConnector_GetVersionSQL(t *testing.T) {
	conn := &MySQLConnector{}
	assert.Equal(t, "select version()", conn.GetVersionSQL())
}

func TestOraConnector_GetVersionSQL(t *testing.T) {
	conn := &OraConnector{}
	assert.Equal(t, "select banner from v$version", conn.GetVersionSQL())
}

func TestPGConnector_GetRoCommand(t *testing.T) {
	conn := &PGConnector{}
	assert.Contains(t, conn.GetRoCommand(), "read only")
}

func TestMySQLConnector_GetRoCommand(t *testing.T) {
	conn := &MySQLConnector{}
	assert.Contains(t, conn.GetRoCommand(), "read only")
}

func TestOraConnector_GetRoCommand(t *testing.T) {
	conn := &OraConnector{}
	assert.Contains(t, conn.GetRoCommand(), "read only")
}

func TestPGConnector_GetRelationTreeSQL(t *testing.T) {
	conn := &PGConnector{}
	sql := conn.GetRelationTreeSQL()
	assert.Contains(t, sql, "pg_catalog")
	assert.Contains(t, sql, "RECURSIVE")
}

func TestMySQLConnector_GetRelationTreeSQL(t *testing.T) {
	conn := &MySQLConnector{}
	sql := conn.GetRelationTreeSQL()
	assert.Contains(t, sql, "RECURSIVE")
	assert.NotContains(t, sql, "pg_catalog")
}

func TestOraConnector_GetRelationTreeSQL(t *testing.T) {
	conn := &OraConnector{}
	sql := conn.GetRelationTreeSQL()
	// Oracle does not use RECURSIVE keyword; CTE recursion is implicit
	assert.Contains(t, sql, "WITH params")
	assert.Contains(t, sql, "FROM dual")
	assert.Contains(t, sql, "fk_tree")
}
