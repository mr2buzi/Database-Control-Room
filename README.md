# SlateDB

Rust storage engine + desktop database workbench.

SlateDB is a single-process relational database prototype written in Rust, paired with an Electron + React workbench for inspecting query plans, execution stats, schema state, and result rows from the real engine.

## What It Includes

Engine:

- handwritten SQL lexer and recursive-descent parser
- page-based heap storage
- catalog metadata for tables and indexes
- one-column B+ tree indexes
- equality and range-filter query planner
- transactional execution with `BEGIN`, `COMMIT`, `ROLLBACK`
- physical WAL crash recovery

Workbench:

- Electron + React desktop UI
- query editor
- planner and execution trace view
- schema explorer
- result table
- query history

Architecture:

```text
React UI
  -> Electron IPC
  -> Rust CLI engine
  -> page file + WAL
```

Tech:

- Rust
- React
- Electron
- TypeScript

## Why This Project Is Interesting

The goal of this project was to build something deeper than a typical CRUD application. Many student "database" projects stop at serializing objects to disk. SlateDB implements the pieces that make it behave like a database system:

- parsed SQL
- page-backed storage
- stable row identifiers
- index lookups
- planner decisions
- WAL-backed recovery

The desktop workbench makes those internals visible without introducing an API server or remote deployment layer.

## Core Design

### Parser

SlateDB uses a handwritten lexer and recursive-descent parser for a constrained SQL subset.

Supported statements:

- `CREATE TABLE`
- `INSERT`
- `SELECT`
- `DELETE`
- `CREATE INDEX`
- `EXPLAIN`
- `BEGIN`, `COMMIT`, `ROLLBACK`
- `.exit` in REPL mode

### Storage

- Page size: `4096` bytes
- Page `0`: database header
- Catalog: serialized table and index metadata
- Heap: slotted pages for row storage
- Deletes: tombstones, not physical compaction
- `RowId`: `{ page_id: u32, slot_id: u16 }`

### Row Format

Each row stores:

- deleted flag
- column count
- typed values

Value support in v1:

- `INT`
- `TEXT`

### Indexing

SlateDB implements one-column B+ tree indexes for point lookups and range scans.

Current scope:

- `INT` and `TEXT` keys
- equality and single-bound range predicates
- leaf pages map key -> one or more `RowId`s

### Planner

The planner is intentionally simple:

- if the query has an equality or single-bound range predicate on an indexed column with a matching type, use the index
- otherwise fall back to a sequential heap scan

This keeps `EXPLAIN` meaningful and makes the scan-vs-index tradeoff easy to demonstrate.

### Transactions and Recovery

The engine is single-process and single-writer.

Transaction model:

- outside `BEGIN`, statements run in autocommit mode
- inside a transaction, dirty pages stay in memory until commit
- `ROLLBACK` discards uncommitted dirty pages

WAL model:

- physical page-image logging
- on commit, the engine writes committed page images to the WAL and flushes them
- recovery scans the WAL, keeps committed transactions, replays page images, and truncates the WAL

This is simpler than a more advanced redo/undo scheme, but it keeps the recovery model straightforward and easy to reason about.

## Frontend

The workbench is a local interface for inspecting the database, not the database itself.

Runtime flow:

1. SQL is entered in the query editor.
2. React sends the query to Electron through the preload bridge.
3. Electron runs the Rust engine locally.
4. The engine returns JSON with AST, plan, stats, and rows.
5. The workbench renders the result and updates the schema and history panels.

The frontend therefore reflects real engine behavior rather than simulated planner output.

## Supported SQL

```sql
CREATE TABLE users (id INT, name TEXT, tier TEXT);
INSERT INTO users VALUES (1, 'Ana', 'free');
INSERT INTO users VALUES (2, 'Jay', 'pro');
SELECT id, name, tier FROM users WHERE id = 2 LIMIT 1;
SELECT id, name, tier FROM users WHERE id >= 2 LIMIT 2;
CREATE INDEX idx_users_id ON users(id);
EXPLAIN SELECT id, name, tier FROM users WHERE id = 2 LIMIT 1;
BEGIN; INSERT INTO users VALUES (3, 'Mia', 'pro'); ROLLBACK;
```

## Scope Limits

These limits are intentional:

- `INT` and `TEXT` only
- no `NULL`
- single-column equality and single-bound range predicates only
- `LIMIT` on `SELECT`
- one table per query
- no joins
- no `UPDATE`
- single-process only

## How To Run

Prerequisites:

- Node.js and npm
- Rust toolchain with `cargo`
- Windows PowerShell for the desktop launcher

Install dependencies:

```powershell
npm install
```

Desktop workbench:

```powershell
npm run desktop
```

This is the main demo path. It starts the frontend, launches Electron, builds the Rust engine if needed, seeds a local demo database, and connects the UI to the real engine.

Frontend only:

```powershell
npm run dev
```

This uses a browser fallback mode for UI iteration.

Production build:

```powershell
npm run build
```

Engine only:

```powershell
cd engine
cargo run -- repl --data .\target\demo.sdb --debug-ast
cargo run -- exec --data .\target\demo.sdb --query "SELECT * FROM users;" --format json
cargo run -- inspect --data .\target\demo.sdb
cargo test
```

Benchmark entrypoint:

```powershell
cd engine
cargo run -- bench --data .\target\bench.sdb
```

## Demo Flow

Recommended interview demo flow:

1. Launch `npm run desktop`
2. Run:

```sql
SELECT id, name, tier
FROM users
WHERE id = 2
LIMIT 1;
```

3. Show the planner and output panels, then point out indexed lookup
4. Run:

```sql
EXPLAIN SELECT id, name, tier
FROM users
WHERE id >= 2
LIMIT 2;
```

5. Show the AST and planner payload
6. Run a transaction demo:

```sql
BEGIN;
INSERT INTO users VALUES (5, 'Rina', 'pro');
ROLLBACK;
SELECT id, name, tier
FROM users
WHERE id = 5
LIMIT 1;
```

7. Walk through how the WAL and rollback model support that behavior

## Testing

The engine tests cover:

- parser correctness
- persistence across restart
- delete tombstones
- planner and index behavior
- `LIMIT`
- multi-statement execution
- schema inspection
- transaction rollback and commit behavior
- WAL recovery

Run:

```powershell
cd engine
cargo test
```

## What To Improve Next

The next feature to add would be `UPDATE`, which would make row mutation and index maintenance a more complete part of the engine story.

Other useful next steps:

- `UPDATE`
- richer schema introspection in the UI
- better benchmark output
- compaction / vacuum
- cleaner desktop packaging

## Files To Start With

For a quick walkthrough, these are the best starting points:

- [src/App.tsx](c:/Users/User/Documents/project%202/src/App.tsx)
- [src/queryEngine.ts](c:/Users/User/Documents/project%202/src/queryEngine.ts)
- [electron/main.cjs](c:/Users/User/Documents/project%202/electron/main.cjs)
- [electron/slatedb-runtime.cjs](c:/Users/User/Documents/project%202/electron/slatedb-runtime.cjs)
- [engine/src/executor.rs](c:/Users/User/Documents/project%202/engine/src/executor.rs)
- [engine/src/parser.rs](c:/Users/User/Documents/project%202/engine/src/parser.rs)
- [engine/src/storage/pager.rs](c:/Users/User/Documents/project%202/engine/src/storage/pager.rs)
- [engine/src/index/btree.rs](c:/Users/User/Documents/project%202/engine/src/index/btree.rs)

## Resume Version

Built a Rust storage engine and desktop database workbench with handwritten SQL parsing, page-based heap storage, B+ tree indexing, transactional execution, and WAL-backed crash recovery, with planner and execution details exposed through an Electron UI.
