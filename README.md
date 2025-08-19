# Cass
[![Test Status](https://github.com/mweiden/cass/actions/workflows/ci.yml/badge.svg)](https://github.com/mweiden/cass/actions/workflows/ci.yml) [![codecov](https://codecov.io/gh/mweiden/cass/branch/main/graph/badge.svg)](https://codecov.io/gh/mweiden/cass)

Toy/experimental clone of [Apache Cassandra](https://en.wikipedia.org/wiki/Apache_Cassandra) written in Rust, mostly using [OpenAI Codex](https://chatgpt.com/codex).

## Features

- **HTTP API:** with basic SQL syntax
- **Data Structure:** Stores data in a [log-structured merge tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree)
- **Storage:** Column-oriented SSTable placeholders with bloom filters and zone maps to speed up queries; persist to local or S3 AWS backends
- **Durability / Recovery:** Sharded write-ahead logs for durability and in-memory tables for parallel ingestion
- **Deployment:** Dockerfile and docker-compose for containerized deployment and local testing
- **Scalability:** Horizontally scalable
- **Gossip:** Cluster membership and liveness detection via gossip with health checks
- **Logging:** HTTP requests logged in common log format

## Design tradeoffs

Like Cassandra itself, `cass` is an [AP system](https://en.wikipedia.org/wiki/CAP_theorem):

- **Consistency:** Consistency is relaxed, last-write-wins conflict resolution
- **Availability:** always writable, tunably consistent, fault-tolerant through replication
- **Partition tolerance:** will continue to work even if parts of the cluster cannot communicate

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

### Contributing

Before submitting changes, ensure the code is formatted and tests pass:

```bash
cargo fmt
cargo test
```

The project uses idiomatic Rust patterns with small, focused functions. See the
module-level comments in `src/` for a high-level overview of the architecture.

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


## Example / Docker Compose Cluster

With the server running you can insert and query data over HTTP. The provided `docker-compose.yml` starts a five-node cluster using local
storage with a replication factor of three where you can try this out.

Start the cluster:

```bash
docker compose up
```

Insert via one node and query from the others:

```bash
curl -X POST localhost:8080/query -d "CREATE TABLE kv (id TEXT, val TEXT, PRIMARY KEY(id))"
curl -X POST localhost:8080/query -d "INSERT INTO kv VALUES ('hello','world')"
# => {"op":"INSERT","unit":"row","count":1}
curl -X POST localhost:8081/query -d "SELECT val FROM kv WHERE id = 'hello'"
# You can run the following requests to illustrate that each node proxies requests to the replicas owning the parition key
curl -X POST localhost:8082/query -d "SELECT val FROM kv WHERE id = 'hello'"
curl -X POST localhost:8083/query -d "SELECT val FROM kv WHERE id = 'hello'"
curl -X POST localhost:8084/query -d "SELECT val FROM kv WHERE id = 'hello'"
```

## Maintenance Endpoints

The server exposes a couple of helper endpoints useful during testing:

- `POST /flush` instructs every node in the cluster to flush its in-memory
  memtable to an on-disk SSTable.
- `POST /flip` toggles the health status of the node receiving the request,
  switching it between healthy and unhealthy. The node's `node_health`
  Prometheus gauge reports `1` when healthy and `0` when unhealthy.

## Monitoring

Each node exposes Prometheus metrics at `/metrics`. The provided
`docker-compose.yml` also starts Prometheus and Grafana. After running

```bash
docker compose up
```

visit <http://localhost:3000> and sign in with the default
`admin`/`admin` credentials. The Grafana instance is preconfigured with the
Prometheus data source so you can explore metrics such as HTTP request
counts, peer health, RAM and CPU usage, and SSTable disk usage.

There is also a preconfigured dashboard with basic metrics from all instances. Screenshot below:

<img width="1257" height="821" alt="Screenshot 2025-08-17 at 11 48 28 PM" src="https://github.com/user-attachments/assets/cbaf71aa-c726-4c6a-a1eb-422060aecd0a" />


