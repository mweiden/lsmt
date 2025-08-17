# lsmt

Toy/experimental clone of [Apache Cassandra](https://en.wikipedia.org/wiki/Apache_Cassandra) written in Rust.

## Features

- REST API for querying
- Async storage abstraction with local or S3 backends
- Sharded write-ahead logs and in-memory tables for parallel ingestion
- Column-oriented SSTable placeholders with bloom filters and zone maps
- Dockerfile and docker-compose for containerized deployment

## Query Syntax

The built-in SQL engine understands a small subset of SQL:

- `INSERT` of a `key`/`value` pair into a table
- `UPDATE` and `DELETE` statements targeting a single key
- `SELECT` with optional `WHERE` filters, `ORDER BY`, `GROUP BY`,
  `DISTINCT`, simple aggregate functions (`COUNT`, `MIN`, `MAX`, `SUM`)
  and `LIMIT`
- Table management statements such as `CREATE TABLE`, `DROP TABLE` and
  `SHOW TABLES`

Note on creating [partition and clustering keys](https://cassandra.apache.org/doc/4.0/cassandra/data_modeling/intro.html#partitions):
the first column in `PRIMARY KEY(...)` will be the partition key, subsequent columns will be indexed as clustering keys.

So for the example `id` will be the partition key and `c` will be a clustering key:

```
CREATE TABLE t (
   id int,
   c text,
   k int,
   v text,
   PRIMARY KEY (id,c)
);
```

## Development

```bash
cargo test            # run unit tests
cargo run             # start the HTTP query service on port 8080
```

## Storage Backends

The server supports both local filesystem storage and Amazon S3.

### Local

Local storage is the default. Specify a directory with `--data-dir`:

```bash
cargo run -- --data-dir ./data
```

### S3

To use S3, set AWS credentials in the environment and provide the bucket
name:

```bash
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
  cargo run -- --storage s3 --bucket my-bucket
```

`AWS_REGION` controls the region (default `us-east-1`).

## Example

With the server running you can insert and query data over HTTP:

```bash
# add a key/value pair
curl -X POST localhost:8080/query -d "INSERT INTO kv VALUES ('hello','world')"
# => {"op":"INSERT","unit":"row","count":1}

# fetch the previously inserted value
curl -X POST localhost:8080/query -d "SELECT value FROM kv WHERE key = 'hello'"
# ["value" comes back as JSON]
# => [{"value":"world"}]
```

## Docker Compose Cluster

The provided `docker-compose.yml` starts a three-node cluster using local
storage with a replication factor of two.

Start the cluster:

```bash
docker compose up
```

Insert via one node and query from the others:

```bash
curl -X POST localhost:8080/query -d "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))"
curl -X POST localhost:8080/query -d "INSERT INTO kv VALUES ('hello','world')"
# => {"op":"INSERT","unit":"row","count":1}
curl -X POST localhost:8081/query -d "SELECT value FROM id WHERE key = 'hello'"
curl -X POST localhost:8082/query -d "SELECT value FROM id WHERE key = 'hello'"
```

The project is a scaffold and many components are left for future work.
