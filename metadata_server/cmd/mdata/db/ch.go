package db

import (
	"database/sql"
	"log"
)

type ClickHouseConnector struct {
	pool *sql.DB
}

func (conn *ClickHouseConnector) GetStatus() string {
	return "OK"
}

func (conn *ClickHouseConnector) test() {
	// print connection properties
	log.Printf("propertise %s", conn.pool.Stats().WaitDuration)

}

func (conn *ClickHouseConnector) Close() {
	conn.pool.Close()
}
