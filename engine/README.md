# SlateDB Engine

SlateDB is a single-process Rust database engine with:

- A handwritten lexer and recursive-descent parser
- Page-based on-disk storage
- Fixed-schema heap tables
- One-column B+ tree indexes for equality lookup
- Autocommit plus explicit `BEGIN` / `COMMIT` / `ROLLBACK`
- Physical page-image WAL recovery
- CLI `repl`, `exec`, and `bench` entry points

## Commands

```powershell
cargo run -- repl --data .\target\demo.sdb --debug-ast
cargo run -- exec --data .\target\demo.sdb --query "SELECT * FROM users;" --format json
cargo run -- inspect --data .\target\demo.sdb
cargo run -- bench --data .\target\bench.sdb
```

## Supported SQL

```sql
CREATE TABLE users (id INT, name TEXT);
INSERT INTO users VALUES (1, 'Ana');
SELECT * FROM users WHERE id = 1;
SELECT * FROM users WHERE id = 1 LIMIT 1;
DELETE FROM users WHERE id = 1;
CREATE INDEX idx_users_id ON users(id);
EXPLAIN SELECT * FROM users WHERE id = 1;
BEGIN;
COMMIT;
ROLLBACK;
.exit
```

## Storage Model

- Page `0` stores the database header.
- Catalog metadata is serialized into catalog pages.
- Heap pages use a slotted-page layout and tombstones for deletes.
- Index pages are stored as a page-backed B+ tree.
- The WAL stores committed page after-images and is replayed on startup.

## Current Limits

- `INT` and `TEXT` only
- No `NULL`
- Equality filters only
- `LIMIT` on `SELECT`
- One table per query
- No joins, `UPDATE`, `ORDER BY`, or `GROUP BY`
- Single-process, single-writer only
