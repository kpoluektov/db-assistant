## DBMS Metadata rest service. 

supports 
- PostgreSQL/MySQL via information_schema.tables/columns
- Oracle via all_tables/all_tab_columns
so appropriate rights needs to be granted. 

To build the image 
```sh
docker build -t metadata_server:0.2 .
```

To start the server run
```sh
docker run -itd -p 8080:8080 -e MDATA_ADDR="0.0.0.0" -e MDATA_PORT=8080 metadata_server:0.2
```

Access sequence could looks like this

export PG_PASSWORD=

### create db session
```sh
curl -v -d 'username=pol' \
   -d "password=$PG_PASSWORD" \
   -d 'dbhost=rc1a-8ugcgj0tw5eh0z5q.mdb.yandexcloud.net' \
   -d 'dbport=6432' \
   -d 'dbtype=postgres' \
   -d 'database=pol' \
    --request POST "http://127.0.0.1:8080/connection/open"
```

returns cookie like "session=K6eAvSYsRyLBdGyOhcekonXMSifovb0mRcdk0_5Stec". The cookie needs to be passed all the next request

### check db session status
```sh
curl  --cookie "session=K6eAvSYsRyLBdGyOhcekonXMSifovb0mRcdk0_5Stec" http://127.0.0.1:8080/connection/status
```

### get statistics for table `my_table` in schema `public`
```sh
curl --cookie "session=K6eAvSYsRyLBdGyOhcekonXMSifovb0mRcdk0_5Stec" "http://127.0.0.1:8080/stats/public/my_table"    
```

### get metadata for table `my_table` in schema `public`
```sh
curl --cookie "session=K6eAvSYsRyLBdGyOhcekonXMSifovb0mRcdk0_5Stec" "http://127.0.0.1:8080/metadata/public/my_table"    
```

### close current db session
```sh
curl  --cookie "session=K6eAvSYsRyLBdGyOhcekonXMSifovb0mRcdk0_5Stec" --request POST "http://84.252.143.105:8080/connection/close"
```
