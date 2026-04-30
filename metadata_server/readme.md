# metadata_server

Go REST API that extracts metadata from relational databases and exposes it over HTTP. Used by `mcp_server` as a backend for MCP tool calls; can also be queried directly for debugging.

## Supported databases

| Database | `dbtype` value |
|----------|---------------|
| PostgreSQL / Yandex Managed PostgreSQL | `postgres` |
| MySQL / Yandex Managed MySQL | `mysql` |
| Oracle DB | `oracle` |
| ClickHouse | `clickhouse` |

Required privileges:
- **PostgreSQL** — access to `pg_catalog`, `information_schema`
- **MySQL** — access to `information_schema`, `performance_schema`
- **Oracle** — `SELECT` on `all_tables`, `all_tab_columns`, `all_indexes`, `all_ind_columns`, `all_constraints`, `all_cons_columns`, `v$parameter`

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MDATA_ADDR` | `127.0.0.1` | Bind address |
| `MDATA_PORT` | `8080` | Listen port |

## Session model

Each caller must first open a connection (`POST /connection/open`) and receive a `session` cookie. That cookie must be passed with every subsequent request. Sessions expire after **5 minutes** of inactivity; the connection manager closes idle connections automatically.

## API reference

All responses are `application/json`. Successful responses return HTTP `202 Accepted`.

### Health check

```
GET /healthz
```

Returns `202` when the server is running.

---

### Connection management

#### Open connection

```
POST /connection/open
Content-Type: application/x-www-form-urlencoded

dbtype=postgres&dbhost=...&dbport=5432&username=...&password=...&database=...
```

Optional: `capath=<path>` — path to a CA certificate file (for TLS, e.g. Yandex Managed MySQL).

Returns a `session` cookie and a JSON status object. Pass the cookie on all subsequent calls.

#### Close connection

```
POST /connection/close
Cookie: session=<token>
```

#### Connection status

```
GET /connection/status
Cookie: session=<token>
```

---

### Metadata

#### Table metadata (columns, types, descriptions)

```
GET /metadata/{schema}/{table}
Cookie: session=<token>
```

Response:
```json
{
  "schema": "public",
  "version": "PostgreSQL 16.2",
  "tables": [
    {
      "name": "orders",
      "description": "...",
      "columns": [
        { "name": "id", "type": "integer", "description": "" }
      ]
    }
  ]
}
```

#### Table list

```
GET /tables/{schema}/{wildcard}
Cookie: session=<token>
```

`wildcard` supports `%` and `_` (SQL LIKE syntax). Returns up to 100 tables.

Response:
```json
{
  "schema": "public",
  "version": "PostgreSQL 16.2",
  "tables": [
    { "name": "orders" },
    { "name": "order_items" }
  ]
}
```

#### Table statistics

```
GET /stats/{schema}/{table}
Cookie: session=<token>
```

Response:
```json
{
  "schema": "public",
  "table": "orders",
  "version": "PostgreSQL 16.2",
  "statistic": { ... }
}
```

#### Indexes

```
GET /indexes/{schema}/{table}
Cookie: session=<token>
```

Response:
```json
{
  "schema": "public",
  "version": "PostgreSQL 16.2",
  "indexes": [
    {
      "name": "orders_pkey",
      "unique": true,
      "is_pk": true,
      "columns": [{ "name": "id" }]
    }
  ]
}
```

#### Foreign key relationship tree

```
GET /relationships/{schema}/{table}?depth=N
Cookie: session=<token>
```

`depth` — traversal depth, 1–5 (default 5).

Response:
```json
{
  "schema": "public",
  "table": "orders",
  "version": "PostgreSQL 16.2",
  "tree": {
    "relations": [
      {
        "direction": "outgoing",
        "from_column": "user_id",
        "to_column": "id",
        "node": { "schema": "public", "table": "users" }
      },
      {
        "direction": "incoming",
        "from_column": "order_id",
        "to_column": "id",
        "node": { "schema": "public", "table": "order_items" }
      }
    ]
  }
}
```

#### Database parameter

```
GET /parameter/{name}
Cookie: session=<token>
```

Example: `/parameter/work_mem`

Response:
```json
{
  "version": "PostgreSQL 16.2",
  "parameter": { "name": "work_mem", "value": "4MB" }
}
```

---

### SQL execution

```
POST /sql
Content-Type: application/x-www-form-urlencoded
Cookie: session=<token>

sql=SELECT+id%2C+name+FROM+public.orders+LIMIT+10
```

Executes the query in a **read-only** transaction. Returns a JSON array of row objects:

```json
[
  { "id": 1, "name": "Order A" },
  { "id": 2, "name": "Order B" }
]
```

---

## Example session (curl)

```bash
export DB_PASS=secret

# 1. Open connection — save the session cookie
curl -c /tmp/cookies.txt \
     -d 'dbtype=postgres' \
     -d 'dbhost=rc1b-xxx.mdb.yandexcloud.net' \
     -d 'dbport=5432' \
     -d 'username=myuser' \
     -d "password=$DB_PASS" \
     -d 'database=mydb' \
     -X POST http://localhost:8080/connection/open

# 2. Get table metadata
curl -b /tmp/cookies.txt \
     http://localhost:8080/metadata/public/orders

# 3. Get table list (all tables in public schema)
curl -b /tmp/cookies.txt \
     "http://localhost:8080/tables/public/%25"

# 4. Get indexes
curl -b /tmp/cookies.txt \
     http://localhost:8080/indexes/public/orders

# 5. Get FK relationship tree (depth 2)
curl -b /tmp/cookies.txt \
     "http://localhost:8080/relationships/public/orders?depth=2"

# 6. Run a SQL query
curl -b /tmp/cookies.txt \
     -d 'sql=SELECT id, name FROM public.orders LIMIT 5' \
     -X POST http://localhost:8080/sql

# 7. Close connection
curl -b /tmp/cookies.txt \
     -X POST http://localhost:8080/connection/close
```

## Build

```bash
# Docker image
docker build -t metadata_server:0.7 .

# Standalone (requires Go 1.23+)
cd cmd/mdata
go build -o metadata_server .
```

## Run standalone

```bash
MDATA_ADDR=0.0.0.0 MDATA_PORT=8080 ./metadata_server
```
