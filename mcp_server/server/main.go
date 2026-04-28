package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Print("No .env file loaded")
	}
}

const (
	CONNECTION_PREFIX  = "connection"
	CONNECT_COMMAND    = "open"
	CLOSE_COMMAND      = "close"
	STATS_PREFIX       = "stats"
	INDEX_PREFIX       = "indexes"
	META_PREFIX        = "metadata"
	TABLE_LIST_PREFIX  = "tables"
	CONTENT_TYPE       = "application/x-www-form-urlencoded"
	SessionID          = "session"
	TABLE_NAME_STR     = "tableName"
	SCHEMA_NAME_STR    = "schemaName"
	PARAMETER_PREFIX      = "parameter"
	PARAMETER_NAME_STR    = "parameterName"
	RELATIONSHIPS_PREFIX  = "relationships"
	WIDE_SQL_PREFIX       = "sql"

	CONFIG_META_URL_STR    = "META_URL"
	CONFIG_METADATA_HOST   = "MDATA_HOST"
	CONFIG_METADATA_PORT   = "MDATA_PORT"
	CONFIG_METADATA_USER   = "MDATA_USER"
	CONFIG_METADATA_PASS   = "MDATA_PASS"
	CONFIG_METADATA_TYPE   = "MDATA_TYPE"
	CONFIG_METADATA_BASE   = "MDATA_BASE"
	CONFIG_METADATA_CAPATH = "MDATA_CAPATH"
	CONFIG_MCP_PORT        = "MCP_PORT"
	CONFIG_MCP_HOST        = "MCP_HOST"
	GET_VERB               = "GET"
)

var (
	metaURL    = os.Getenv(CONFIG_META_URL_STR)
	metaHost   = os.Getenv(CONFIG_METADATA_HOST)
	metaPort   = os.Getenv(CONFIG_METADATA_PORT)
	metaUser   = os.Getenv(CONFIG_METADATA_USER)
	metaPass   = os.Getenv(CONFIG_METADATA_PASS)
	metaType   = os.Getenv(CONFIG_METADATA_TYPE)
	metaBase   = os.Getenv(CONFIG_METADATA_BASE)
	metaCAPath = os.Getenv(CONFIG_METADATA_CAPATH)
	mcpPort    = os.Getenv(CONFIG_MCP_PORT)
	mcpHost    = os.Getenv(CONFIG_MCP_HOST)
)

func main() {

	var sseMode, _ = strconv.ParseBool(os.Getenv("SSE_MODE"))
	loadEnvs()
	if metaURL == "" {
		log.Fatal("No meta URL provided")
	}

	// Create MCP server
	mcpServer := server.NewMCPServer(
		"Metadata",
		"1.0.0",
		server.WithToolCapabilities(false),
	)
	statTool := mcp.NewTool("get_statistics",
		mcp.WithDescription("Retrieve table statistics"),
		mcp.WithString(TABLE_NAME_STR,
			mcp.Required(),
			mcp.Description("table name to retrieve statistics for"),
		),
		mcp.WithString(SCHEMA_NAME_STR,
			mcp.Required(),
			mcp.Description("schema name where table exists"),
		),
	)
	metaTool := mcp.NewTool("get_metadata",
		mcp.WithDescription("Retrieve table metadata"),
		mcp.WithString(TABLE_NAME_STR,
			mcp.Required(),
			mcp.Description("table name to retrieve metadata for"),
		),
		mcp.WithString(SCHEMA_NAME_STR,
			mcp.Required(),
			mcp.Description("schema name where table exists"),
		),
	)
	listTool := mcp.NewTool("get_table_list",
		mcp.WithDescription("Retrieve list of tables by given schema and wildcard"),
		mcp.WithString(TABLE_NAME_STR,
			mcp.Required(),
			mcp.Description("table name to retrieve metadata for"),
		),
		mcp.WithString(SCHEMA_NAME_STR,
			mcp.Required(),
			mcp.Description("schema name where table exists"),
		),
	)
	indexTool := mcp.NewTool("get_indexes",
		mcp.WithDescription("Retrieve indexes information on table_name"),
		mcp.WithString(TABLE_NAME_STR,
			mcp.Required(),
			mcp.Description("table name to retrieve indexes information for"),
		),
		mcp.WithString(SCHEMA_NAME_STR,
			mcp.Required(),
			mcp.Description("schema name where index exists"),
		),
	)
	parameterTool := mcp.NewTool("get_db_parameters",
		mcp.WithDescription("Retrieve information about database settings like pg_settings/v$parameters"),
		mcp.WithString(PARAMETER_NAME_STR,
			mcp.Required(),
			mcp.Description("table name to retrieve indexes information for"),
		),
	)
	wideSQLTool := mcp.NewTool("run_wide_sql",
		mcp.WithDescription("Run wide SQL"),
		mcp.WithString(WIDE_SQL_PREFIX,
			mcp.Required(),
			mcp.Description("SQL to execute"),
		),
	)
	relationshipsTool := mcp.NewTool("get_relationships",
		mcp.WithDescription("Retrieve FK dependency tree for a table showing which tables reference it and which tables it references, traversed up to the given depth"),
		mcp.WithString(TABLE_NAME_STR,
			mcp.Required(),
			mcp.Description("table name to retrieve FK relationships for"),
		),
		mcp.WithString(SCHEMA_NAME_STR,
			mcp.Required(),
			mcp.Description("schema name where table exists"),
		),
		mcp.WithNumber("depth",
			mcp.Description("maximum traversal depth 1-5 (default 5)"),
		),
	)

	// Add tool handler
	mcpServer.AddTool(statTool, getTableStatsHandler)
	mcpServer.AddTool(metaTool, getTableMetaHandler)
	mcpServer.AddTool(listTool, getTableListHandler)
	mcpServer.AddTool(indexTool, getIndexesHandler)
	mcpServer.AddTool(parameterTool, getParameterHandler)
	mcpServer.AddTool(wideSQLTool, getWideSQLHandler)
	mcpServer.AddTool(relationshipsTool, getRelationshipsHandler)

	// Start the stdio server
	// Run server in appropriate mode
	if sseMode {
		// Create and start SSE server
		sseServer := server.NewSSEServer(mcpServer, server.WithBaseURL(fmt.Sprintf("http://%s:%s", mcpHost, mcpPort)))
		log.Printf(fmt.Sprintf("Starting SSE server on localhost:%s", mcpPort))
		if err := sseServer.Start(fmt.Sprintf(":%s", mcpPort)); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	} else {
		// Run as stdio server
		if err := server.ServeStdio(mcpServer); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	}
}

func loadEnvs() {
	metaURL = os.Getenv(CONFIG_META_URL_STR)
	metaHost = os.Getenv(CONFIG_METADATA_HOST)
	metaPort = os.Getenv(CONFIG_METADATA_PORT)
	metaUser = os.Getenv(CONFIG_METADATA_USER)
	metaPass = os.Getenv(CONFIG_METADATA_PASS)
	metaType = os.Getenv(CONFIG_METADATA_TYPE)
	metaBase = os.Getenv(CONFIG_METADATA_BASE)
	mcpPort = os.Getenv(CONFIG_MCP_PORT)
	mcpHost = os.Getenv(CONFIG_MCP_HOST)
	metaCAPath = os.Getenv(CONFIG_METADATA_CAPATH)
}

func connectMetaServer() (string, error) {
	formData := url.Values{}
	formData.Set("username", metaUser)
	formData.Set("password", metaPass)
	formData.Set("dbhost", metaHost)
	formData.Set("dbport", metaPort)
	formData.Set("dbtype", metaType)
	formData.Set("database", metaBase)
	if len(metaCAPath) > 0 {
		formData.Set("capath", metaCAPath)
	}
	connectResp, err := http.Post(
		fmt.Sprintf("%s/%s/%s", metaURL, CONNECTION_PREFIX, CONNECT_COMMAND),
		CONTENT_TYPE,
		strings.NewReader(formData.Encode()),
	)
	if err != nil {
		return "", fmt.Errorf("connect metadata server: %w", err)
	}
	defer connectResp.Body.Close()
	if connectResp.StatusCode != http.StatusAccepted {
		return "", fmt.Errorf("connect metadata server: status %d", connectResp.StatusCode)
	}
	var sessID string
	for _, c := range connectResp.Cookies() {
		if c.Name == SessionID {
			sessID = c.Value
		}
	}
	bodyBytes, err := io.ReadAll(connectResp.Body)
	if err != nil {
		return "", fmt.Errorf("connect metadata server: read response: %w", err)
	}
	if !strings.Contains(string(bodyBytes), "true") {
		return "", errors.New("cannot connect to metadata server")
	}
	log.Printf("Connected to %s", metaURL)
	return sessID, nil
}

func createSessionCookie(sID string) *http.Cookie {
	expire := time.Now().Add(24 * time.Hour) // Cookie expires in 24 hours
	return &http.Cookie{
		Name:    SessionID,
		Value:   sID,
		Expires: expire,
		MaxAge:  int(expire.Sub(time.Now()).Seconds()), // MaxAge in seconds
	}
}
func createRequest(method string, url string, body io.Reader, sID string) (*http.Request, error) {
	r, err := http.NewRequest(method, url, body)
	r.AddCookie(createSessionCookie(sID))
	r.Header.Set("Content-Type", CONTENT_TYPE)
	return r, err
}

func disconnectMetaServer(sessID string) {
	client := &http.Client{}
	req, err := createRequest("POST", fmt.Sprintf("%s/%s/%s", metaURL, CONNECTION_PREFIX, CLOSE_COMMAND), strings.NewReader(""), sessID)
	if err != nil {
		log.Printf("disconnect: create request: %v", err)
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("disconnect: %v", err)
		return
	}
	resp.Body.Close()
}

func makeRequestOnTable(path string, request mcp.CallToolRequest) (string, error) {
	tableName, ok := request.GetArguments()[TABLE_NAME_STR].(string)
	if !ok {
		return "", errors.New("Table name must be a string")
	}
	schemaName, ok := request.GetArguments()[SCHEMA_NAME_STR].(string)
	if !ok {
		return "", errors.New("Schema name must be a string")
	}
	log.Printf("looking for %s for table %s in schema %s", path, tableName, schemaName)
	return makeWideRequest(
		fmt.Sprintf("%s/%s/%s/%s", metaURL, path, url.QueryEscape(schemaName), url.QueryEscape(tableName)),
		GET_VERB,
		nil,
	)
}

func makeWideRequest(path string, verb string, body io.Reader) (string, error) {
	//create connection
	sessID, err := connectMetaServer()
	if err != nil {
		return "", err
	}
	client := &http.Client{}
	req, err := createRequest(verb, path, body, sessID)
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("get page error: %w", err)
	}
	defer resp.Body.Close()
	var bodyString = ""
	if resp.StatusCode >= http.StatusOK && resp.StatusCode <= http.StatusAccepted {
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", fmt.Errorf("read response: %w", err)
		}
		bodyString = string(bodyBytes)
	} else {
		bodyString = "Can't parse response"
	}
	log.Print(bodyString)
	disconnectMetaServer(sessID)
	return bodyString, nil
}

func getHandler(ctx context.Context, request mcp.CallToolRequest, prefix string) (*mcp.CallToolResult, error) {
	resp, err := makeRequestOnTable(prefix, request)
	if err != nil {
		return nil, fmt.Errorf("get page error: %w", err)
	}
	return mcp.NewToolResultText(resp), nil
}

func getTableStatsHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return getHandler(ctx, request, STATS_PREFIX)
}

func getTableMetaHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return getHandler(ctx, request, META_PREFIX)
}

func getTableListHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return getHandler(ctx, request, TABLE_LIST_PREFIX)
}

func getIndexesHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return getHandler(ctx, request, INDEX_PREFIX)
}

func getParameterHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	parameterName, ok := request.GetArguments()[PARAMETER_NAME_STR].(string)
	if !ok {
		return nil, errors.New("Parameter name must be a string")
	}
	resp, err :=
		makeWideRequest(fmt.Sprintf("%s/%s/%s", metaURL, PARAMETER_PREFIX, url.QueryEscape(parameterName)), GET_VERB, nil)
	if err != nil {
		return nil, fmt.Errorf("get page error: %w", err)
	}
	return mcp.NewToolResultText(resp), nil
}

func getRelationshipsHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	tableName, ok := request.GetArguments()[TABLE_NAME_STR].(string)
	if !ok {
		return nil, errors.New("Table name must be a string")
	}
	schemaName, ok := request.GetArguments()[SCHEMA_NAME_STR].(string)
	if !ok {
		return nil, errors.New("Schema name must be a string")
	}
	reqURL := fmt.Sprintf("%s/%s/%s/%s", metaURL, RELATIONSHIPS_PREFIX, url.QueryEscape(schemaName), url.QueryEscape(tableName))
	if depth, ok := request.GetArguments()["depth"].(float64); ok && depth >= 1 && depth <= 5 {
		reqURL += fmt.Sprintf("?depth=%d", int(depth))
	}
	log.Printf("getting relationships for table %s in schema %s", tableName, schemaName)
	resp, err := makeWideRequest(reqURL, GET_VERB, nil)
	if err != nil {
		return nil, fmt.Errorf("get relationships error: %w", err)
	}
	return mcp.NewToolResultText(resp), nil
}

func getWideSQLHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {

	sql := request.GetArguments()[WIDE_SQL_PREFIX].(string)
	data := url.Values{}
	data.Set("sql", sql)
	log.Printf("executing %s", sql)
	resp, err := makeWideRequest(fmt.Sprintf("%s/%s", metaURL, WIDE_SQL_PREFIX), "POST", strings.NewReader(data.Encode()))
	if err != nil {
		return nil, fmt.Errorf("post error: %w", err)
	}
	return mcp.NewToolResultText(resp), nil
}
