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
cargo test
```

The project is a scaffold and many components are left for future work.
