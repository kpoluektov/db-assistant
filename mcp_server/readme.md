## Metadata MCP server for Metadata rest service. 

publishes two tools 
- get_statistics - maps to /stat/ metadata server path
- get_metadata - maps to /metadata/ metadata server path 

To build the image 
```sh
docker build -t mcp_metadata_server:0.2 .
```

To start the server run
```sh
docker run -itd -p 8081:8081 --env-file server/.env  mcp_metadata_server:0.2
```

.env example
```sh
META_URL=http://127.0.0.1:8080
MDATA_HOST=rc1a-8ugcgj0tw5eh0z5q.mdb.yandexcloud.net
MDATA_PORT=6432
MDATA_USER=user
MDATA_PASS=secret_password
MDATA_TYPE=postgres
MDATA_BASE=dbname
MCP_PORT=8081
MSP_HOST=84.252.143.105
SSE_MODE=true
```

