mod support;

use slatedb::executor::OutputFormat;

use support::{exec, open_db, TestDb};

#[test]
fn explain_reports_index_usage() {
    let test_db = TestDb::new("planner-index");
    let mut db = open_db(&test_db.path);
    exec(&mut db, "CREATE TABLE users (id INT, name TEXT);", OutputFormat::Table);
    exec(&mut db, "INSERT INTO users VALUES (1, 'Ana');", OutputFormat::Table);
    exec(&mut db, "INSERT INTO users VALUES (2, 'Jay');", OutputFormat::Table);
    exec(
        &mut db,
        "CREATE INDEX idx_users_id ON users(id);",
        OutputFormat::Table,
    );

    let explain = exec(
        &mut db,
        "EXPLAIN SELECT id, name FROM users WHERE id = 2;",
        OutputFormat::Json,
    );
    assert!(explain.contains("\"used_index\":\"idx_users_id\""));

    let select = exec(
        &mut db,
        "SELECT id, name FROM users WHERE id = 2;",
        OutputFormat::Json,
    );
    assert!(select.contains("\"used_index\":true"));
}

#[test]
fn planner_uses_index_range_scan_for_indexed_inequality() {
    let test_db = TestDb::new("planner-range");
    let mut db = open_db(&test_db.path);
    exec(&mut db, "CREATE TABLE users (id INT, name TEXT);", OutputFormat::Table);
    exec(&mut db, "INSERT INTO users VALUES (1, 'Ana');", OutputFormat::Table);
    exec(&mut db, "INSERT INTO users VALUES (2, 'Jay');", OutputFormat::Table);
    exec(&mut db, "INSERT INTO users VALUES (3, 'Mia');", OutputFormat::Table);
    exec(
        &mut db,
        "CREATE INDEX idx_users_id ON users(id);",
        OutputFormat::Table,
    );

    let explain = exec(
        &mut db,
        "EXPLAIN SELECT id, name FROM users WHERE id >= 2 LIMIT 2;",
        OutputFormat::Json,
    );
    assert!(explain.contains("\"description\":\"EXPLAIN index range scan via idx_users_id\""));
    assert!(explain.contains("\"op\":\">=\""));

    let select = exec(
        &mut db,
        "SELECT id, name FROM users WHERE id >= 2 LIMIT 2;",
        OutputFormat::Json,
    );
    assert!(select.contains("\"kind\":\"IndexRangeScan\""));
    assert!(select.contains("\"used_index\":\"idx_users_id\""));
}

#[test]
fn planner_falls_back_to_scan_when_unindexed() {
    let test_db = TestDb::new("planner-scan");
    let mut db = open_db(&test_db.path);
    exec(&mut db, "CREATE TABLE users (id INT, name TEXT);", OutputFormat::Table);
    exec(&mut db, "INSERT INTO users VALUES (1, 'Ana');", OutputFormat::Table);

    let select = exec(
        &mut db,
        "SELECT id, name FROM users WHERE name = 'Ana';",
        OutputFormat::Json,
    );
    assert!(select.contains("\"used_index\":false"));
    assert!(select.contains("\"rows_read\":1"));
}

#[test]
fn planner_falls_back_to_scan_for_unindexed_range() {
    let test_db = TestDb::new("planner-range-scan");
    let mut db = open_db(&test_db.path);
    exec(&mut db, "CREATE TABLE users (id INT, name TEXT);", OutputFormat::Table);
    exec(&mut db, "INSERT INTO users VALUES (1, 'Ana');", OutputFormat::Table);
    exec(&mut db, "INSERT INTO users VALUES (2, 'Jay');", OutputFormat::Table);

    let select = exec(
        &mut db,
        "SELECT id, name FROM users WHERE name >= 'A';",
        OutputFormat::Json,
    );
    assert!(select.contains("\"kind\":\"SeqScan\""));
    assert!(select.contains("\"used_index\":false"));
}
