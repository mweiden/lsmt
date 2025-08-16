# AGENTS

This file provides guidance for contributors working with this repository.

## Project Overview

- Experimental log-structured merge tree database written in Rust.
- Supports async storage backends (local filesystem or S3) and a basic SQL layer.
- Docker tooling is available for containerized development and testing.

## Code Style

- Format Rust code using `cargo fmt` before committing.
- Prefer idiomatic Rust patterns and keep functions small and focused.

## Testing

- Run `cargo test` and ensure all tests pass before submitting changes.

## Documentation

- Update relevant documentation when modifying behavior or adding features.

## Code Navigation

- `src/main.rs` starts the HTTP query service.
- `src/lib.rs` exposes the `Database` type tying together core modules.
- `src/memtable.rs` handles in-memory writes before flushing to disk.
- `src/sstable.rs` implements on-disk sorted string tables.
- `src/wal.rs` provides a write-ahead log for durability.
- `src/query.rs` parses and executes a small subset of SQL.
- `src/storage/` contains filesystem and S3 storage backends.
- `tests/` includes unit and integration tests demonstrating usage.
