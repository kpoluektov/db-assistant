# mcp_server

Go [MCP (Model Context Protocol)](https://modelcontextprotocol.io) server that bridges the OpenAI Agents SDK to `metadata_server`. Exposes database metadata and SQL execution as MCP tools over SSE transport.

## How it works

On each tool call the server:
1. Opens a connection to `metadata_server` (`POST /connection/open`) using the configured database credentials
2. Calls the appropriate REST endpoint
3. Closes the connection (`POST /connection/close`)
4. Returns the result as a tool response

## MCP tools

| Tool | Maps to | Description |
|------|---------|-------------|
| `get_metadata` | `GET /metadata/{schema}/{table}` | Columns, types, comments |
| `get_table_list` | `GET /tables/{schema}/{table}` | Table list (wildcard supported) |
| `get_statistics` | `GET /stats/{schema}/{table}` | Table row count and statistics |
| `get_indexes` | `GET /indexes/{schema}/{table}` | Table indexes |
| `get_db_parameters` | `GET /parameter/{name}` | Database parameter value |
| `get_relationships` | `GET /relationships/{schema}/{table}?depth=N` | FK dependency tree (depth 1–5) |
| `run_wide_sql` | `POST /sql` | Execute a read-only SQL query |

## Environment variables

| Variable | Description |
|----------|-------------|
| `META_URL` | Base URL of `metadata_server`, e.g. `http://web:8080` |
| `MDATA_HOST` | Database host |
| `MDATA_PORT` | Database port |
| `MDATA_USER` | Database user |
| `MDATA_PASS` | Database password |
| `MDATA_TYPE` | Database type: `postgres` / `mysql` / `oracle` / `clickhouse` |
| `MDATA_BASE` | Database name / SID |
| `MCP_PORT` | Port the MCP server listens on (default `8081`) |
| `MCP_HOST` | Hostname used in the SSE base URL advertised to clients |
| `SSE_MODE` | `true` — SSE transport; `false` — stdio transport |
| `MDATA_CAPATH` | *(optional)* Path to CA certificate for TLS database connections |

## Transport modes

**SSE mode** (`SSE_MODE=true`) — HTTP server with Server-Sent Events. Used by the OpenAI Agents SDK (`MCPServerSse`). The server advertises itself at `http://<MCP_HOST>:<MCP_PORT>`.

**stdio mode** (`SSE_MODE=false`) — communicates over stdin/stdout. Useful for local testing with the MCP CLI.

## Build

```bash
# Docker image
docker build -t mcp_server:0.7 .

# Standalone (requires Go 1.23+)
cd server
go build -o mcp_server .
```

## Run with Docker

```bash
docker run -d -p 8081:8081 --env-file .env mcp_server:0.7
```

`.env` example:

```env
META_URL=http://host.docker.internal:8080
MDATA_HOST=rc1b-xxx.mdb.yandexcloud.net
MDATA_PORT=5432
MDATA_USER=myuser
MDATA_PASS=secret
MDATA_TYPE=postgres
MDATA_BASE=mydb
MCP_PORT=8081
MCP_HOST=localhost
SSE_MODE=true
```

## Run standalone

```bash
META_URL=http://localhost:8080 \
MDATA_HOST=localhost MDATA_PORT=5432 \
MDATA_USER=myuser MDATA_PASS=secret \
MDATA_TYPE=postgres MDATA_BASE=mydb \
MCP_PORT=8081 MCP_HOST=localhost SSE_MODE=true \
./mcp_server
```

## Testing with MCP Inspector

```bash
npx @modelcontextprotocol/inspector http://localhost:8081/sse
```
