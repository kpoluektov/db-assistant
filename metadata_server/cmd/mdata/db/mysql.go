package db

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"crypto/tls"
	"crypto/x509"
	"database/sql"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
)

type MySQLConnector struct {
	pool *sql.DB
}

func NewMySQLConnector(dsn *DSN) (*MySQLConnector, error) {
	dsnStr := ""
	if len(dsn.CAPath) > 0 {
		tlsConf, err := createTLSConf(dsn.CAPath, "", "")
		if err != nil {
			log.Printf("Error %s when createTLSConf\n", err)
			return nil, err
		}
		err = mysql.RegisterTLSConfig("custom", tlsConf)
		if err != nil {
			log.Printf("Error %s when RegisterTLSConfig\n", err)
			return nil, err
		}

		// connection string (dataSourceName) is slightly different
		dsnStr = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?tls=custom",
			dsn.Username, dsn.Password, dsn.DbHost, dsn.DbPort, dsn.Database)
	} else {
		dsnStr = fmt.Sprintf("%s://%s:%s@%s:%s/%s",
			dsn.DbType, dsn.Username, dsn.Password, dsn.DbHost, dsn.DbPort, dsn.Database)
	}
	// log.Printf("current dsn is %s", dsnStr)
	c, err := sql.Open("mysql", dsnStr)
	return &MySQLConnector{c}, err
}

func (conn MySQLConnector) GetPool() *sql.DB {
	return conn.pool
}

func (conn MySQLConnector) GetTables(table string, strict bool) string {
	sqlStr := `select table_name from information_schema.tables 
	where table_type = 'BASE TABLE' and table_schema = ? and table_name %s order by table_name limit ?`
	if !strict && (strings.Contains(table, "%") || strings.Contains(table, "_")) {
		sqlStr = fmt.Sprintf(sqlStr, "like ?")
	} else {
		sqlStr = fmt.Sprintf(sqlStr, "= ?")
	}
	return sqlStr
}

func (conn MySQLConnector) GetColumns() string {
	return `select column_name, data_type, character_maximum_length 
						from information_schema.columns where table_schema = ?
						and table_name = ? order by ordinal_position`
}

func (conn MySQLConnector) GetStats() string {
	return `select table_rows, null 
						from information_schema.tables where table_schema = ? 
						and table_name = ?`
}

func (conn MySQLConnector) GetIndexes() string {
	return `select distinct index_name, case non_unique when '1' then '0' else '1' end, 
			true, case index_name when 'PRIMARY' then '1' else 0 end 
						from information_schema.statistics where table_schema = ? 
						and table_name = ? order by 1`
}

func (conn MySQLConnector) GetIndColumns() string {
	return `select column_name, seq_in_index 
						from information_schema.statistics where table_schema = ? 
						and index_name = ? order by 2`
}

func createTLSConf(caPath string, clientCertPath string, clientKeyPath string) (*tls.Config, error) {

	rootCertPool := x509.NewCertPool()
	pem, err := os.ReadFile(caPath)
	if err != nil {
		return nil, err
	}
	if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
		return nil, errors.New("Failed to append PEM.")
	}
	clientCert := make([]tls.Certificate, 0, 1)
	if len(clientCertPath) > 0 {
		certs, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
		if err != nil {
			return nil, err
		}
		clientCert = append(clientCert, certs)
	}

	return &tls.Config{
		RootCAs:      rootCertPool,
		Certificates: clientCert,
	}, nil
}

func (conn MySQLConnector) GetParameter() string {
	return `SHOW VARIABLES WHERE Variable_name = ?`
}

func (conn MySQLConnector) GetVersionSQL() string {
	return `select version()`
}
