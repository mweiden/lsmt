# lsmt

Experimental log-structured merge tree database written in Rust.

## Features

- Async storage abstraction with local or S3 backends
- Sharded write-ahead logs and in-memory tables for parallel ingestion
- Column-oriented SSTable placeholders with bloom filters and zone maps
- Basic SQL parsing via [`sqlparser`]
- Dockerfile and docker-compose for containerized deployment

## Development

```bash
cargo test            # run unit tests
cargo run             # start the HTTP query service on port 8080
```

## Example

With the server running you can insert and query data over HTTP:

```bash
# add a key/value pair
curl -X POST localhost:8080/query -d "INSERT INTO kv VALUES ('hello','world')"

# fetch the previously inserted value
curl -X POST localhost:8080/query -d "SELECT value FROM kv WHERE key = 'hello'"
```

The project is a scaffold and many components are left for future work.
