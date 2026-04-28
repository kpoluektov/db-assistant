package db

import (
	"database/sql"
)

type ClickHouseConnector struct {
	pool *sql.DB
}

func (conn *ClickHouseConnector) GetStatus() string {
	return "OK"
}

func (conn *ClickHouseConnector) Close() {
	conn.pool.Close()
}
