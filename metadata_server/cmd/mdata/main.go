package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"

	"syscall"
	"time"

	"github.com/alexedwards/scs/v2"
	"github.com/joho/godotenv"
	"golang.org/x/sync/errgroup"
	"test.org/mdata/db"
)

const (
	prefix              = "/metadata/"
	connectionPrefix    = "/connection/"
	connectionOperation = "operation"
	metaSchema          = "schema"
	metaTable           = "table"
	tablesList          = "/tables/"
	statsPrefix         = "/stats/"
	indexPrefix         = "/indexes/"
	parameterPrefix     = "/parameter/"
	parameterName       = "name"
	defaultPort         = "8080"
	defaultAddr         = "127.0.0.1"
	SessionID           = "sessionID"
	sessionLifeTime     = 5 // min
	tableListLimit      = 100
)

var (
	sessionManager    *scs.SessionManager
	connectionManager *db.ConnectionManager
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Print("No .env file loaded")
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		c := make(chan os.Signal, 1) // we need to reserve to buffer size 1, so the notifier are not blocked
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)

		<-c
		cancel()
	}()

	exists := false

	serverPort, exists := os.LookupEnv("MDATA_PORT")
	if !exists {
		serverPort = defaultPort
	}
	serverAddress, exists := os.LookupEnv("MDATA_ADDR")
	if !exists {
		serverAddress = defaultAddr
	}

	// Initialize a new session manager and configure the session lifetime.
	sessionManager = scs.New()
	sessionManager.Lifetime = sessionLifeTime * time.Minute

	// Initialize connection manager
	connectionManager = db.NewConnectionManager(
		*db.NewConnectionStore(),
		1*time.Minute,
		sessionLifeTime*time.Minute,
		sessionLifeTime*2*time.Minute,
	)

	mux := http.NewServeMux()
	mux.Handle("/healthz", healthHandler())
	mux.Handle(fmt.Sprintf("%s{%s}/{%s}", prefix, metaSchema, metaTable), universalHandler(metaTable))        // get meta by table name
	mux.Handle(fmt.Sprintf("%s{%s}/{%s}", tablesList, metaSchema, metaTable), universalHandler(tablesList))   // get table list by scheme and wildcard
	mux.Handle(fmt.Sprintf("%s{%s}", connectionPrefix, connectionOperation), connectHandler())                // connect/disconnect
	mux.Handle(fmt.Sprintf("%s{%s}/{%s}", statsPrefix, metaSchema, metaTable), universalHandler(statsPrefix)) // get statistics by table name
	mux.Handle(fmt.Sprintf("%s{%s}/{%s}", indexPrefix, metaSchema, metaTable), universalHandler(indexPrefix)) // get indexes by table name
	mux.Handle(fmt.Sprintf("%s{%s}", parameterPrefix, parameterName), universalHandler(parameterPrefix))      // get parameter value by name

	httpServer := &http.Server{
		Handler: sessionManager.LoadAndSave(mux),
		Addr:    fmt.Sprintf("%s:%s", serverAddress, serverPort),
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		log.Printf("ListenAndServe %s", httpServer.Addr)
		return httpServer.ListenAndServe()
	})
	g.Go(func() error {
		<-gCtx.Done()
		log.Printf("Stop message received")
		connectionManager.CloseAllConnection()
		return httpServer.Shutdown(context.Background())
	})

	if err := g.Wait(); err != nil {
		fmt.Printf("exit reason: %s \n", err)
	}

	// Wait for an interrupt
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	// Attempt a graceful shutdown
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connectionManager.CloseAllConnection()
	httpServer.Shutdown(ctx)
}
func healthHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			w.WriteHeader(http.StatusAccepted)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func connectHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			if r.PathValue(connectionOperation) == "open" {
				newConnection(w, r)
			} else {
				closeConnection(w, r)
			}
		case "GET":
			if r.PathValue(connectionOperation) == "status" {
				statusConnection(w, r)
			}
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func handleGetTable(w http.ResponseWriter, r *http.Request) {
	getTables(w, r, 1, true)
}

func getTables(w http.ResponseWriter, r *http.Request, numRows int, strict bool) {
	connection, _ := connectionManager.GetConnection(sessionManager.GetString(r.Context(), SessionID))
	err := connection.Check()
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotAcceptable)
	} else {
		schemaName := r.PathValue(metaSchema)
		tableName := r.PathValue(metaTable)
		if len(schemaName) == 0 || len(tableName) == 0 {
			http.Error(w, "Wrong path", http.StatusBadRequest)
		} else {
			tbl, err := connection.GetTables(schemaName, r.PathValue(metaTable), numRows, strict)
			if err != nil {
				http.Error(w, "Table not found", http.StatusNotFound)
			} else {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusAccepted)
				res := make(map[string]any)
				res["tables"] = tbl
				res["schema"] = schemaName
				res["version"] = connection.CurVersion()
				json.NewEncoder(w).Encode(res)
			}
		}
	}
}

func handleGetTableList(w http.ResponseWriter, r *http.Request) {
	getTables(w, r, tableListLimit, false)
}

func newConnection(w http.ResponseWriter, r *http.Request) {
	dsn := &db.DSN{
		DbType:   r.FormValue("dbtype"),
		Username: r.FormValue("username"),
		Password: r.FormValue("password"),
		DbHost:   r.FormValue("dbhost"),
		DbPort:   r.FormValue("dbport"),
		Database: r.FormValue("database"),
		CAPath:   r.FormValue("capath"),
	}

	//	var err error
	if len(dsn.DbHost) == 0 {
		http.Error(w, "Wrong connection params", http.StatusBadRequest)
	} else {
		log.Printf("connecting to %s:%s", dsn.DbHost, dsn.DbPort)
		sessID, err := connectionManager.AddConnection(*dsn)
		// Store a new key and value in the session data.
		sessionManager.Put(r.Context(), SessionID, sessID)
		conn, _ := connectionManager.GetConnection(sessID)
		if err != nil {
			http.Error(w, "Can't create connection", http.StatusBadRequest)
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(conn.GetStatus())
		}
	}
}

func closeConnection(w http.ResponseWriter, r *http.Request) {
	sessID := sessionManager.GetString(r.Context(), SessionID)
	connection, found := connectionManager.GetConnection(sessID)
	if !found || connection.Check() != nil {
		http.Error(w, "No opened connection", http.StatusBadRequest)
	} else {
		connectionManager.RemoveConnection(sessID)
		sessionManager.Destroy(r.Context())
		log.Print("Connection closed ")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode((connection.GetStatus()))
	}
}

func statusConnection(w http.ResponseWriter, r *http.Request) {
	connection, found := connectionManager.GetConnection(sessionManager.GetString(r.Context(), SessionID))
	if !found || connection.Check() != nil {
		http.Error(w, "No opened connection", http.StatusBadRequest)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode((connection.GetStatus()))
	}
}

func universalHandler(goal string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			switch goal {
			case metaTable:
				handleGetTable(w, r)
			case tablesList:
				handleGetTableList(w, r)
			case statsPrefix:
				handleStatsTable(w, r)
			case indexPrefix:
				getIndexes(w, r)
			case parameterPrefix:
				getParameter(w, r)
			default:
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

func handleStatsTable(w http.ResponseWriter, r *http.Request) {
	connection, _ := connectionManager.GetConnection(sessionManager.GetString(r.Context(), SessionID))
	err := connection.Check()
	if err != nil {
		http.Error(w, "No opened connection", http.StatusNotAcceptable)
	} else {

		schemaName := r.PathValue(metaSchema)
		tableName := r.PathValue(metaTable)
		if len(schemaName) == 0 || len(tableName) == 0 {
			http.Error(w, "Wrong path", http.StatusBadRequest)
		} else {
			stats, err := connection.GetStats(schemaName, tableName)
			if err != nil {
				http.Error(w, "Table not found", http.StatusNotFound)
			} else {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusAccepted)
				res := make(map[string]any)
				res["schema"] = schemaName
				res["table"] = tableName
				res["statistic"] = stats
				res["version"] = connection.CurVersion()
				json.NewEncoder(w).Encode(res)
			}
		}
	}
}

func getIndexes(w http.ResponseWriter, r *http.Request) {
	connection, _ := connectionManager.GetConnection(sessionManager.GetString(r.Context(), SessionID))
	err := connection.Check()
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotAcceptable)
	} else {
		schemaName := r.PathValue(metaSchema)
		tableName := r.PathValue(metaTable)
		if len(schemaName) == 0 || len(tableName) == 0 {
			http.Error(w, "Wrong path", http.StatusBadRequest)
		} else {
			index, err := connection.GetIndexes(schemaName, r.PathValue(metaTable))
			if err != nil {
				http.Error(w, "Table not found", http.StatusNotFound)
			} else {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusAccepted)
				res := make(map[string]any)
				res["indexes"] = index
				res["schema"] = schemaName
				res["version"] = connection.CurVersion()
				json.NewEncoder(w).Encode(res)
			}
		}
	}
}

func getParameter(w http.ResponseWriter, r *http.Request) {
	connection, _ := connectionManager.GetConnection(sessionManager.GetString(r.Context(), SessionID))
	err := connection.Check()
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotAcceptable)
	} else {
		parameterName := r.PathValue(parameterName)
		if len(parameterName) == 0 {
			http.Error(w, "Wrong path", http.StatusBadRequest)
		} else {
			parameter, err := connection.GetParameter(parameterName)
			if err != nil {
				http.Error(w, "Parameter not found", http.StatusNotFound)
			} else {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusAccepted)
				res := make(map[string]any)
				res["parameter"] = parameter
				res["version"] = connection.CurVersion()
				json.NewEncoder(w).Encode(res)
			}
		}
	}
}
